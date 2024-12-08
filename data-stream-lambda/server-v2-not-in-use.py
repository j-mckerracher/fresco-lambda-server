import json
import os
from contextlib import contextmanager
import datetime
import psycopg2.pool
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import base64
import zlib
from typing import Tuple, Dict, Any
import time
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import asyncio
from dataclasses import dataclass

# Update these constants for better performance
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.9)
DEFAULT_BATCH_SIZE = 5000
DEFAULT_ROW_LIMIT = 1000000
MAX_PUBLISHER_THREADS = 10  # Increased from 8 to handle more concurrent publishes
COMPRESSION_LEVEL = 1
PARALLEL_QUERIES = 12  # Increased from 8 to process more data in parallel
QUEUE_SIZE = 100  # Increased queue size for better buffering
MAX_EXECUTION_TIME = 55

# Initialize the PostgreSQL connection pool
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=10,
        maxconn=30,
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME']
    )
    print("Enhanced database connection pool created successfully.")
except Exception as e:
    print(f"Error creating database connection pool: {e}")
    db_pool = None

# Initialize AWS IoT client
try:
    iot_endpoint = os.environ['IOT_ENDPOINT']
    iot_topic = os.environ['IOT_TOPIC']
    region = os.environ.get('REGION', 'us-east-1')
    iot_client = boto3.client('iot-data', region_name=region, endpoint_url=f"https://{iot_endpoint}")
    print("AWS IoT Data client initialized successfully.")
except Exception as e:
    print(f"Error initializing AWS IoT Data client: {e}")
    iot_client = None


@dataclass
class QueryPartition:
    start_offset: int
    end_offset: int
    query: str
    batch_size: int


def create_response(
        transfer_id: str,
        row_count: int = 0,
        chunk_count: int = 0,
        schema_info: Dict = None,
        time_elapsed: float = 0,
        total_rows: int = 0,
        published_messages: int = 0,
        status: str = "COMPLETED",
        error: str = None
) -> Dict[str, Any]:
    """Create response matching AppSync schema"""
    return {
        "transferId": transfer_id,
        "metadata": {
            "rowCount": row_count,
            "chunkCount": chunk_count,
            "schema": json.dumps(schema_info) if schema_info else None,
            "isComplete": status == "COMPLETED",
            "timeElapsed": time_elapsed,
            "totalRows": total_rows,
            "publishedMessageCount": published_messages,
            "processingStatus": status,
            "error": error
        }
    }


async def get_schema(query: str) -> Tuple[pa.Schema, Dict]:
    """Get Arrow schema and schema info dict"""
    with get_db_connection() as conn:  # Use regular context manager
        with conn.cursor() as cursor:
            cursor.execute("SET work_mem = '256MB'")
            cursor.execute(f"SELECT * FROM ({query}) t LIMIT 0")

            fields = []
            schema_info = {}

            for desc in cursor.description:
                name = desc[0]
                pg_type = desc[1]

                if pg_type in (1184, 1114):  # TIMESTAMPTZ or TIMESTAMP
                    field_type = pa.timestamp('us', tz='UTC')
                    schema_info[name] = "timestamp"
                elif pg_type == 701 or name == 'value_gpu':  # FLOAT8
                    field_type = pa.float64()
                    schema_info[name] = "float"
                elif pg_type == 20:  # BIGINT
                    field_type = pa.int64()
                    schema_info[name] = "integer"
                elif pg_type == 23:  # INTEGER
                    field_type = pa.int32()
                    schema_info[name] = "integer"
                else:
                    field_type = pa.string()
                    schema_info[name] = "string"

                fields.append(pa.field(name, field_type))

    return pa.schema(fields), schema_info


class OptimizedBatchProcessor:
    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.field_converters = self._create_field_converters()
        self.arrays = [[] for _ in range(len(self.schema))]
        self.preview_sent = False  # Track if we've shown the preview
        self.preview_size = 5  # Number of rows to preview

    def _create_field_converters(self):
        # Create closure for each type only once
        def make_timestamp_converter():
            return lambda x: x

        def make_float_converter():
            return lambda x: float(x) if x is not None else 0.0

        def make_int_converter():
            return lambda x: int(x) if x is not None else 0

        def make_str_converter():
            return lambda x: str(x) if x is not None else ''

        converters = []
        for field in self.schema:
            if pa.types.is_timestamp(field.type):
                converters.append(make_timestamp_converter())
            elif field.name == 'value_gpu' or pa.types.is_floating(field.type):
                converters.append(make_float_converter())
            elif pa.types.is_integer(field.type):
                converters.append(make_int_converter())
            else:
                converters.append(make_str_converter())
        return converters

    def process_batch(self, rows: list) -> bytes:
        # Show preview of first batch if not shown yet
        if not self.preview_sent and rows:
            self._show_preview(rows)
            self.preview_sent = True

        # Clear arrays for reuse
        for arr in self.arrays:
            arr.clear()

        # Process rows in bulk
        for row in rows:
            for i, (value, converter) in enumerate(zip(row, self.field_converters)):
                self.arrays[i].append(converter(value))

        # Create Arrow arrays efficiently
        arrow_arrays = [pa.array(col, type=field.type)
                        for col, field in zip(self.arrays, self.schema)]

        # Optimize record batch creation
        record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)

        return zlib.compress(sink.getvalue().to_pybytes(), level=COMPRESSION_LEVEL)

    def _show_preview(self, rows: list):
        """Display a preview of the data being processed"""
        preview_rows = rows[:self.preview_size]

        # Get column names from schema
        columns = [field.name for field in self.schema]

        print("\n=== Data Preview ===")
        print(f"Showing first {len(preview_rows)} rows of {len(rows)} in this batch")
        print("\nColumns:", columns)

        # Create a formatted table
        row_format = "{:<3} " + " | ".join(["{:<20}"] * len(columns))

        # Print header
        print("\n" + row_format.format("idx", *columns))
        print("-" * (3 + (23 * len(columns))))

        # Print rows
        for idx, row in enumerate(preview_rows):
            # Format each value to be display-friendly
            formatted_row = []
            for val in row:
                if val is None:
                    formatted_row.append("NULL")
                elif isinstance(val, (datetime.datetime, datetime.date)):
                    formatted_row.append(str(val))
                else:
                    formatted_row.append(str(val)[:20])  # Truncate long values

            print(row_format.format(idx, *formatted_row))
        print("\n=== End Preview ===\n")


class FastParallelPublisher:
    def __init__(self, iot_client, topic: str, transfer_id: str):
        self.iot_client = iot_client
        self.topic = topic
        self.transfer_id = transfer_id
        self.sequence_number = 0
        self.sequence_lock = threading.Lock()
        self.publish_queue = Queue(maxsize=QUEUE_SIZE)
        self.executor = ThreadPoolExecutor(max_workers=MAX_PUBLISHER_THREADS)
        self.publish_count = 0
        self.is_running = True
        self.error = None
        self.max_chunk_size = int((MAX_MQTT_PAYLOAD_SIZE * 0.6) - 2048)
        self.futures = [self.executor.submit(self._publish_worker)
                        for _ in range(MAX_PUBLISHER_THREADS)]
        self.rows_published = 0  # Track total rows published
        self.preview_shown = False  # Add flag for preview
        self.preview_rows = []  # Store preview rows

    def _estimate_message_size(self, data: bytes, metadata: dict) -> int:
        """Estimate final message size including base64 encoding and JSON structure"""
        # Base64 encoding increases size by approximately 4/3
        encoded_size = len(data) * 4 // 3
        # Add estimated metadata and JSON structure overhead
        metadata_size = len(json.dumps(metadata))
        return encoded_size + metadata_size + 100  # Extra buffer for JSON formatting

    def _chunk_data(self, data: bytes) -> list:
        """Split data into appropriately sized chunks"""
        chunks = []
        for i in range(0, len(data), self.max_chunk_size):
            chunk = data[i:i + self.max_chunk_size]
            # Create test metadata to verify size
            test_metadata = {
                'transfer_id': self.transfer_id,
                'sequence': 0,
                'partition': 0,
                'chunk_index': 0,
                'total_chunks': 1,
                'final': False,
                'timestamp': int(time.time() * 1000)
            }

            # If estimated size is too large, reduce chunk size and retry
            while self._estimate_message_size(chunk, test_metadata) >= MAX_MQTT_PAYLOAD_SIZE:
                chunk = chunk[:int(len(chunk) * 0.8)]  # Reduce by 20%

            chunks.append(chunk)
        return chunks

    def _publish_worker(self):
        while self.is_running:
            try:
                batch_item = self.publish_queue.get(timeout=0.1)
                if batch_item is None:
                    break

                data, is_final, partition_id, row_count = batch_item
                self.rows_published += row_count

                # Log publishing progress
                if self.rows_published % 10000 == 0:  # Log every 10k rows
                    print(f"Progress: Published {self.rows_published:,} rows so far...")

                chunks = self._chunk_data(data)
                total_chunks = len(chunks)

                for chunk_index, chunk in enumerate(chunks):
                    sequence = self._get_next_sequence()

                    metadata = {
                        'transfer_id': self.transfer_id,
                        'sequence': sequence,
                        'partition': partition_id,
                        'chunk_index': chunk_index,
                        'total_chunks': total_chunks,
                        'final': is_final and chunk_index == total_chunks - 1,
                        'timestamp': int(time.time() * 1000),
                        'row_count': row_count
                    }

                    message = {
                        'type': 'arrow_data',
                        'metadata': metadata,
                        'data': base64.b64encode(chunk).decode('utf-8')
                    }

                    # Add debug logging - log first message and every 100th message
                    if sequence == 1 or sequence % 100 == 0:
                        # Create a safe version of the message for logging (truncate data field)
                        log_message = message.copy()
                        log_message['data'] = log_message['data'][:100] + '...' if log_message['data'] else None
                        print(f"Publishing message (sequence {sequence}):", json.dumps(log_message, indent=2))

                    for attempt in range(3):
                        try:
                            payload = json.dumps(message)
                            if len(payload.encode('utf-8')) > MAX_MQTT_PAYLOAD_SIZE:
                                raise Exception(f"Payload size exceeds limit")

                            self.iot_client.publish(
                                topic=self.topic,
                                qos=1,
                                payload=payload
                            )
                            self.publish_count += 1
                            break
                        except Exception as e:
                            if attempt == 2:
                                print(f"Failed to publish after {attempt + 1} attempts: {str(e)}")
                                self.error = e
                                self.is_running = False
                                raise
                            time.sleep(0.5 * (attempt + 1))

                self.publish_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                print(f"Publisher worker error: {str(e)}")
                self.error = e
                self.is_running = False
                break

    def _get_next_sequence(self):
        with self.sequence_lock:
            self.sequence_number += 1
            return self.sequence_number

    def publish(self, data: bytes, is_final: bool = False, partition_id: int = 0,
                row_count: int = 0):  # Add row_count parameter
        if not self.is_running:
            raise self.error if self.error else Exception("Publisher stopped")
        self.publish_queue.put((data, is_final, partition_id, row_count))  # Include row_count

    def wait_completion(self):
        self.is_running = False
        for _ in range(MAX_PUBLISHER_THREADS):
            self.publish_queue.put(None)
        self.executor.shutdown(wait=True)
        if self.error:
            raise self.error
        return self.publish_count


@contextmanager
def get_db_connection():
    """Get database connection from pool"""
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)


async def process_partition(partition: QueryPartition, publisher: FastParallelPublisher,
                            batch_processor: OptimizedBatchProcessor) -> int:
    rows_processed = 0

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Set work_mem and enable parallel workers
            cursor.execute("""
                SET work_mem = '256MB';
                SET max_parallel_workers_per_gather = 4;
                SET parallel_tuple_cost = 0.1;
                SET parallel_setup_cost = 0.1;
            """)

            # Create a CTE with row numbers for efficient partitioning
            partition_query = f"""
                WITH numbered_rows AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num
                    FROM ({partition.query}) base_query
                )
                SELECT * FROM numbered_rows
                WHERE row_num > {partition.start_offset}
                  AND row_num <= {partition.end_offset}
            """

            cursor.execute(partition_query)

            while True:
                rows = cursor.fetchmany(partition.batch_size)
                if not rows:
                    break

                batch_data = batch_processor.process_batch(rows)
                is_final = len(rows) < partition.batch_size
                publisher.publish(batch_data, is_final, partition.start_offset, len(rows))
                rows_processed += len(rows)

                if rows_processed % 50000 == 0:
                    print(f"Progress: Processed {rows_processed:,} rows in partition {partition.start_offset}")

    return rows_processed


async def _lambda_handler(event, context):
    start_time = time.time()
    publisher = None
    total_rows = 0
    transfer_id = None
    schema_info = None

    try:
        args = event['arguments']
        transfer_id = args['transferId']
        query = args['query']
        row_limit = min(args.get('rowLimit', DEFAULT_ROW_LIMIT), DEFAULT_ROW_LIMIT)
        batch_size = min(args.get('batchSize', DEFAULT_BATCH_SIZE), DEFAULT_BATCH_SIZE)

        # Get estimated row count first
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Use EXPLAIN to get row estimate
                cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
                explain_result = cursor.fetchone()[0]
                estimated_rows = explain_result[0]['Plan']['Plan Rows']

                # Adjust parallel queries based on estimated rows
                actual_parallel_queries = min(
                    PARALLEL_QUERIES,
                    max(1, int(estimated_rows / 10000))  # 1 partition per 10k rows
                )

        schema, schema_info = await get_schema(query)
        publisher = FastParallelPublisher(iot_client, iot_topic, transfer_id)
        batch_processor = OptimizedBatchProcessor(schema)

        # Create dynamic partitions based on estimated rows
        partitions = [QueryPartition(
            start_offset=i * (row_limit // actual_parallel_queries),
            end_offset=min((i + 1) * (row_limit // actual_parallel_queries), row_limit),
            query=query,
            batch_size=batch_size
        ) for i in range(actual_parallel_queries)]

        tasks = [process_partition(p, publisher, batch_processor) for p in partitions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle results
        errors = [r for r in results if isinstance(r, Exception)]
        successful_results = [r for r in results if isinstance(r, int)]
        total_rows = sum(successful_results)

        # Detailed processing results
        execution_time = time.time() - start_time
        rows_per_second = int(total_rows / execution_time) if execution_time > 0 else 0
        print(f"\nProcessing Results:")
        print(f"✓ Total rows processed: {total_rows:,}")
        print(f"✓ Processing time: {execution_time:.2f} seconds")
        print(f"✓ Processing rate: {rows_per_second:,} rows/second")
        print(f"✓ Published messages: {publisher.publish_count if publisher else 0}")
        if errors:
            print(f"⚠ Errors encountered: {len(errors)}")
            for i, error in enumerate(errors, 1):
                print(f"  Error {i}: {str(error)}")

        if errors and total_rows == 0:
            raise errors[0]

        status = "COMPLETED" if total_rows > 0 and not errors else "PARTIAL"
        published_messages = publisher.publish_count if publisher else 0

        return create_response(
            transfer_id=transfer_id,
            row_count=total_rows,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows,
            published_messages=published_messages,
            status=status
        )

    except Exception as e:
        error_message = str(e)
        error_type = "TIMEOUT_ERROR" if "timeout" in error_message.lower() else "ERROR"

        execution_time = time.time() - start_time
        print(f"\nError occurred after {execution_time:.2f} seconds:")
        print(f"✗ Error type: {error_type}")
        print(f"✗ Error message: {error_message}")
        print(f"✗ Rows processed before error: {total_rows:,}")

        return create_response(
            transfer_id=transfer_id,
            row_count=total_rows,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows,
            published_messages=publisher.publish_count if publisher else 0,
            status=error_type,
            error=error_message
        )

    finally:
        if publisher:
            try:
                publisher.wait_completion()
            except Exception as e:
                print(f"Error during publisher cleanup: {str(e)}")


def lambda_handler(event, context):
    return asyncio.run(_lambda_handler(event, context))