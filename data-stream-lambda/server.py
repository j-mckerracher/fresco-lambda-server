import json
import os
from contextlib import contextmanager
import psutil
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import base64
import zlib
from typing import List, Tuple, Dict, Any
import time
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import asyncio
from dataclasses import dataclass
from collections import defaultdict

# Optimized Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024   # 128 KB (AWS IoT limit)
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.7)  # Increased buffer size
DEFAULT_BATCH_SIZE = 2000  # Increased from 1000
DEFAULT_ROW_LIMIT = 1000000  # Increased to 1M rows
MAX_TOTAL_TIME = 58  # Maximum total execution time
MAX_PROCESSING_TIME = 45  # Increased processing window
MAX_PUBLISHING_TIME = 10  # Optimized publishing window
MAX_CLEANUP_TIME = 3
MAX_PUBLISHER_THREADS = 12  # Doubled from 6
COMPRESSION_LEVEL = 4  # Fast compression
PARALLEL_QUERIES = 4  # Number of parallel query executors
MAX_MEMORY_THRESHOLD = 2400
QUEUE_SIZE = min(100, int(psutil.virtual_memory().available / (1024 * 1024 * 10)))  # 10MB per queued item

# Initialize the PostgreSQL connection pool with more connections
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=10,
        maxconn=30,  # Increased pool size
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


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)


def check_memory():
    usage = psutil.Process().memory_info().rss / (1024 * 1024)
    if usage > MAX_MEMORY_THRESHOLD:
        raise MemoryError(f"Memory usage ({usage}MB) exceeded threshold")


def create_response(status, transfer_id, row_count=0, chunk_count=0, schema_info=None, time_elapsed=0, total_rows=0, published_messages=0, error=None):
    """Create a standardized response object for the Lambda function"""
    return {
        'transferId': transfer_id,
        'metadata': {
            'rowCount': int(row_count),
            'chunkCount': int(chunk_count),
            'schema': schema_info,
            'isComplete': status == "COMPLETED",
            'timeElapsed': int(time_elapsed),  # Convert to integer
            'totalRows': int(total_rows),
            'publishedMessageCount': int(published_messages),
            'processingStatus': status,
            'error': str(error) if error else None
        }
    }


class OptimizedBatchProcessor:
    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.field_converters = self._create_field_converters()

    def _create_field_converters(self):
        def make_float_converter():
            def converter(x):
                if x is None:
                    return 0.0
                # Handle both string and numeric inputs
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return 0.0
            return converter

        def make_int_converter():
            def converter(x):
                if x is None:
                    return 0
                try:
                    return int(x)
                except (TypeError, ValueError):
                    return 0
            return converter

        def make_str_converter():
            def converter(x):
                return '' if x is None else str(x)
            return converter

        def make_datetime_converter():
            def converter(x):
                # Pass datetime objects directly to Arrow
                # Arrow will handle the conversion internally
                return x
            return converter

        converters = []
        for field in self.schema:
            if pa.types.is_timestamp(field.type):
                converters.append(make_datetime_converter())
            elif field.name == 'value_gpu' or pa.types.is_floating(field.type):
                converters.append(make_float_converter())
            elif pa.types.is_integer(field.type):
                converters.append(make_int_converter())
            else:
                converters.append(make_str_converter())
        return converters

    def process_batch(self, rows: list) -> bytes:
        try:
            check_memory()
            # Pre-allocate arrays for each column
            arrays = [[] for _ in range(len(self.schema))]

            # Single-pass row processing with optimized conversion
            for row in rows:
                for i, (value, converter) in enumerate(zip(row, self.field_converters)):
                    arrays[i].append(converter(value))

            # Bulk conversion to Arrow arrays
            arrow_arrays = [
                pa.array(column_data, type=field.type)
                for column_data, field in zip(arrays, self.schema)
            ]

            # Create record batch and serialize
            record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, record_batch.schema) as writer:
                writer.write_batch(record_batch)

            # Compress the data
            data = sink.getvalue().to_pybytes()
            compressed_data = zlib.compress(data, level=COMPRESSION_LEVEL)
            return compressed_data

        except Exception as e:
            print(f"Error processing batch: {str(e)}")
            raise


class EnhancedParallelPublisher:
    def __init__(self, iot_client, topic, transfer_id, num_threads=MAX_PUBLISHER_THREADS):
        self.iot_client = iot_client
        self.topic = topic
        self.transfer_id = transfer_id
        self.max_chunk_size = MAX_DATA_SIZE
        self.sequence_number = 0
        self.sequence_lock = threading.Lock()
        self.publish_queue = Queue(maxsize=QUEUE_SIZE)
        self.error = None
        self.is_running = True
        self.executor = ThreadPoolExecutor(max_workers=num_threads)
        self.futures = []
        self.publish_count = 0
        self.publish_count_lock = threading.Lock()
        self.batch_timestamps = defaultdict(float)

        # Start publisher threads
        for _ in range(num_threads):
            future = self.executor.submit(self._publish_worker)
            self.futures.append(future)

        print(f"EnhancedParallelPublisher initialized with {num_threads} threads")

    def get_next_sequence(self):
        with self.sequence_lock:
            self.sequence_number += 1
            return self.sequence_number

    def _chunk_data(self, data: bytes) -> List[bytes]:
        """Split data into chunks that will fit within IoT payload limits after encoding"""
        chunks = []
        chunk_size = MAX_DATA_SIZE

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            # Estimate final message size including metadata and base64 encoding
            encoded_size = len(base64.b64encode(chunk)) + 500  # 500 bytes buffer for metadata

            # If estimated size is too large, reduce chunk size and try again
            while encoded_size > MAX_MQTT_PAYLOAD_SIZE and chunk_size > 1024:
                chunk_size = int(chunk_size * 0.8)  # Reduce by 20%
                chunk = data[i:i + chunk_size]
                encoded_size = len(base64.b64encode(chunk)) + 500

            chunks.append(chunk)

        return chunks

    def _publish_batch(self, batch_item: Tuple[bytes, bool, int]):
        batch_data, is_final, partition_id = batch_item
        sequence = self.get_next_sequence()

        # Split data into appropriately sized chunks
        chunks = self._chunk_data(batch_data)
        total_chunks = len(chunks)

        for chunk_number, chunk_data in enumerate(chunks, start=1):
            is_final_sequence = is_final and (chunk_number == total_chunks)

            # Calculate actual message size for logging
            encoded_data = base64.b64encode(chunk_data).decode('utf-8')

            message = {
                'type': 'arrow_data',
                'metadata': {
                    'transfer_id': self.transfer_id,
                    'sequence_number': sequence,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'chunk_size': len(chunk_data),
                    'format': 'arrow_stream_ipc_compressed',
                    'compression': 'zlib',
                    'compression_level': COMPRESSION_LEVEL,
                    'partition_id': partition_id,
                    'timestamp': int(time.time() * 1000),
                    'is_final_sequence': is_final_sequence
                },
                'data': encoded_data
            }

            message_json = json.dumps(message)
            message_size = len(message_json.encode('utf-8'))

            if message_size > MAX_MQTT_PAYLOAD_SIZE:
                print(f"Warning: Message size ({message_size} bytes) exceeds limit after chunking")
                continue

            # Implement exponential backoff for publishing
            max_retries = 3
            retry_delay = 0.1

            for attempt in range(max_retries):
                try:
                    self.iot_client.publish(
                        topic=self.topic,
                        qos=1,
                        payload=message_json
                    )
                    with self.publish_count_lock:
                        self.publish_count += 1
                    break
                except Exception as e:
                    if "RequestEntityTooLargeException" in str(e):
                        print(f"Message size too large ({message_size} bytes), skipping chunk")
                        break
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(retry_delay * (2 ** attempt))

    def _publish_worker(self):
        while self.is_running:
            try:
                batch_item = self.publish_queue.get(timeout=0.1)
                if batch_item is None:
                    break

                self._publish_batch(batch_item)
                self.publish_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                self.error = e
                self.is_running = False
                break

    def publish(self, batch_data: bytes, is_final: bool = False, partition_id: int = 0):
        if self.error:
            raise self.error
        self.publish_queue.put((batch_data, is_final, partition_id))

    def wait_completion(self, timeout=None):
        try:
            wait_start = time.time()

            # Process remaining queue items
            while not self.publish_queue.empty():
                if timeout and time.time() - wait_start > timeout:
                    break
                time.sleep(0.1)

            # Signal threads to stop
            self.is_running = False
            for _ in range(len(self.futures)):
                self.publish_queue.put(None)

            # Wait for executor shutdown
            self.executor.shutdown(wait=True)

            if self.error:
                raise self.error

            return self.publish_count

        except Exception as e:
            print(f"Error during publisher shutdown: {str(e)}")
            raise
        finally:
            if not self.executor._shutdown:
                self.executor.shutdown(wait=False)


class ParallelQueryExecutor:
    def __init__(self, base_query: str, schema: pa.Schema, num_partitions: int = PARALLEL_QUERIES):
        self.base_query = base_query
        self.schema = schema
        self.num_partitions = num_partitions
        self.batch_processor = OptimizedBatchProcessor(schema)

    def create_partition_query(self, partition: QueryPartition) -> str:
        return f"""
        WITH numbered_rows AS (
            SELECT ROW_NUMBER() OVER () as rn, t.*
            FROM ({self.base_query}) t
        )
        SELECT * FROM numbered_rows 
        WHERE rn > {partition.start_offset} AND rn <= {partition.end_offset}
        """


def is_query_safe(query):
    """Validates the SQL query for safety"""
    print("Validating SQL query for safety.")
    disallowed_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE',
        'TRUNCATE', 'EXECUTE', 'GRANT', 'REVOKE', 'REPLACE'
    ]
    parsed = sqlparse.parse(query)
    if not parsed:
        print("Failed to parse SQL query.")
        return False

    tokens = [token for token in parsed[0].flatten()]
    token_values = [token.value.upper() for token in tokens if not token.is_whitespace]

    for keyword in disallowed_keywords:
        if keyword.upper() in token_values:
            print(f"Disallowed keyword '{keyword}' found in query.")
            return False

    if ';' in query:
        print("Semicolon detected in query.")
        return False

    print("SQL query is considered safe.")
    return True


def lambda_handler(event, context):
    """Synchronous wrapper for async _lambda_handler"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_lambda_handler(event, context))
    finally:
        try:
            loop.close()
        except Exception as e:
            print(f"Error closing event loop: {e}")


async def _lambda_handler(event, context):
    """
    Optimized Lambda handler for high-throughput data streaming
    """
    print("Received event:", json.dumps(event))
    start_time = time.time()
    publisher = None
    schema_info = None
    processing_status = "ERROR"
    total_rows_processed = 0
    transfer_id = None

    try:
        # Extract arguments
        arguments = event['arguments']
        transfer_id = arguments['transferId']
        query = arguments['query']
        row_limit = min(arguments.get('rowLimit', DEFAULT_ROW_LIMIT), DEFAULT_ROW_LIMIT)  # Enforce maximum limit
        batch_size = min(arguments.get('batchSize', DEFAULT_BATCH_SIZE), 5000)  # Cap batch size

        print(f"Starting parallel query execution with transfer ID: {transfer_id}")
        print(f"Query: {query}")
        print(f"Row limit: {row_limit}, Batch size: {batch_size}")

        # Validate query
        if not is_query_safe(query):
            raise Exception('Unsafe SQL query.')

        if db_pool is None:
            raise Exception('Database connection pool is not initialized.')

        # Modify the base query to include the row limit
        limited_query = f"""
        WITH limited_results AS (
            SELECT * FROM ({query}) base_query
            LIMIT {row_limit}
        )
        SELECT * FROM limited_results
        """

        # Get initial schema and row count
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                # Set optimized query parameters
                cursor.execute("""
                    SET work_mem = '256MB';
                    SET maintenance_work_mem = '256MB';
                    SET random_page_cost = 1.0;
                    SET effective_cache_size = '4GB';
                """)

                # Get schema
                cursor.execute(f"SELECT * FROM ({limited_query}) t LIMIT 0")
                if cursor.description is None:
                    return create_response(
                        status="COMPLETED",
                        transfer_id=transfer_id,
                        row_count=0,
                        chunk_count=0,
                        schema_info={},
                        time_elapsed=time.time() - start_time
                    )

                # Create schema
                fields = []
                for desc in cursor.description:
                    name = desc[0]
                    if desc[1] == 1184 or desc[1] == 1114:  # TIMESTAMPTZ or TIMESTAMP
                        field_type = pa.timestamp('us', tz='UTC')
                    elif desc[1] == 701 or name == 'value_gpu':  # FLOAT8/DOUBLE
                        field_type = pa.float64()
                    elif desc[1] == 20:  # BIGINT
                        field_type = pa.int64()
                    elif desc[1] == 25:  # TEXT
                        field_type = pa.string()
                    else:
                        field_type = pa.string()
                    fields.append(pa.field(name, field_type))

                schema = pa.schema(fields)
                schema_info = {field.name: str(field.type) for field in schema}

                # Get approximate row count for the limited query
                cursor.execute(f"EXPLAIN (FORMAT JSON) {limited_query}")
                plan = cursor.fetchone()[0]
                estimated_rows = min(plan[0]['Plan']['Plan Rows'], row_limit)

        finally:
            db_pool.putconn(conn)

        # Calculate partition sizes based on row_limit
        rows_per_partition = max(1000, min(estimated_rows, row_limit) // PARALLEL_QUERIES)
        partitions = []
        remaining_rows = row_limit
        offset = 0

        # Create partitions that respect the row limit
        while remaining_rows > 0 and len(partitions) < PARALLEL_QUERIES:
            partition_size = min(rows_per_partition, remaining_rows)
            partitions.append(QueryPartition(
                start_offset=offset,
                end_offset=offset + partition_size,
                query=limited_query,  # Use the query with LIMIT clause
                batch_size=batch_size
            ))
            offset += partition_size
            remaining_rows -= partition_size

        # Initialize parallel processing components
        publisher = EnhancedParallelPublisher(iot_client, iot_topic, transfer_id)
        batch_processor = OptimizedBatchProcessor(schema)

        # Execute partitions in parallel
        tasks = []
        for partition in partitions:
            task = asyncio.create_task(process_partition(
                partition,
                publisher,
                batch_processor
            ))
            tasks.append(task)

        # Wait for all partitions to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and handle any errors
        partition_errors = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Error in partition {i}: {str(result)}")
                partition_errors.append(result)
            else:
                total_rows_processed += result

        if partition_errors and total_rows_processed == 0:
            raise partition_errors[0]

        processing_status = "COMPLETED" if total_rows_processed > 0 else "PARTIAL"
        if total_rows_processed >= row_limit:
            processing_status = "COMPLETED"

        execution_time = time.time() - start_time
        print(f"Query execution completed in {execution_time:.2f}s. "
              f"Processed {total_rows_processed:,} rows.")

        return create_response(
            status=processing_status,
            transfer_id=transfer_id,
            row_count=total_rows_processed,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows_processed,
            published_messages=publisher.publish_count if publisher else 0
        )

    except Exception as e:
        print(f"Error during execution: {str(e)}")
        return create_response(
            status="ERROR",
            transfer_id=transfer_id,
            row_count=total_rows_processed,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=time.time() - start_time,
            total_rows=total_rows_processed,
            published_messages=publisher.publish_count if publisher else 0,
            error=str(e)
        )

    finally:
        if publisher:
            try:
                cleanup_time = min(MAX_CLEANUP_TIME,
                                   MAX_TOTAL_TIME - (time.time() - start_time))
                if cleanup_time > 0:
                    publisher.wait_completion(timeout=cleanup_time)
            except Exception as e:
                print(f"Error during publisher cleanup: {str(e)}")


async def process_partition(partition, publisher, batch_processor):
    """Helper function to process a single partition"""
    rows_processed = 0
    max_rows = partition.end_offset - partition.start_offset

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            try:
                # Optimize connection for this partition
                cursor.execute("""
                    SET work_mem = '256MB';
                    SET enable_parallel_hash = on;
                    SET enable_partitionwise_join = on;
                    SET random_page_cost = 1.0;
                """)

                # Execute partitioned query with explicit LIMIT
                partition_query = f"""
                WITH numbered_rows AS (
                    SELECT ROW_NUMBER() OVER () as rn, t.*
                    FROM ({partition.query}) t
                )
                SELECT * FROM numbered_rows 
                WHERE rn > {partition.start_offset} AND rn <= {partition.end_offset}
                LIMIT {max_rows}  -- Explicit limit for this partition
                """
                cursor.execute(partition_query)

                while rows_processed < max_rows:
                    check_memory()
                    rows = cursor.fetchmany(min(partition.batch_size, max_rows - rows_processed))
                    if not rows:
                        break

                    batch_data = batch_processor.process_batch(rows)
                    check_memory()
                    is_final = len(rows) < partition.batch_size or rows_processed + len(rows) >= max_rows
                    publisher.publish(batch_data, is_final, partition.start_offset)

                    rows_processed += len(rows)

                return rows_processed

            except Exception as e:
                print(f"Error processing partition: {str(e)}")
                raise