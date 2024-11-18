import os
import asyncpg
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import base64
import lz4.frame
import orjson
from typing import Tuple, Dict, Any
import time
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import asyncio
from dataclasses import dataclass

# Optimized constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.9)
DEFAULT_BATCH_SIZE = 20000  # Increased from 5000
DEFAULT_ROW_LIMIT = 1000000
MAX_PUBLISHER_THREADS = 16  # Increased from 10
PARALLEL_QUERIES = 16  # Increased from 12
QUEUE_SIZE = 200  # Increased from 100
MAX_EXECUTION_TIME = 55

print("Starting application with optimized constants:")
print(f"MAX_MQTT_PAYLOAD_SIZE={MAX_MQTT_PAYLOAD_SIZE}, MAX_DATA_SIZE={MAX_DATA_SIZE}, "
      f"DEFAULT_BATCH_SIZE={DEFAULT_BATCH_SIZE}, DEFAULT_ROW_LIMIT={DEFAULT_ROW_LIMIT}, "
      f"MAX_PUBLISHER_THREADS={MAX_PUBLISHER_THREADS}, PARALLEL_QUERIES={PARALLEL_QUERIES}, "
      f"QUEUE_SIZE={QUEUE_SIZE}, MAX_EXECUTION_TIME={MAX_EXECUTION_TIME}")


# Database connection pool (now using asyncpg)
async def init_db_pool():
    print("Initializing asyncpg database connection pool...")
    try:
        pool = await asyncpg.create_pool(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            database=os.environ['DB_NAME'],
            min_size=10,
            max_size=30,
            command_timeout=25,
            statement_cache_size=0
        )
        print("asyncpg database connection pool initialized successfully.")
        return pool
    except Exception as e:
        print(f"Error initializing asyncpg database connection pool: {e}")
        raise


# Initialize AWS IoT client
print("Initializing AWS IoT Data client...")
try:
    iot_endpoint = os.environ['IOT_ENDPOINT']
    iot_topic = os.environ['IOT_TOPIC']
    region = os.environ.get('REGION', 'us-east-1')
    iot_client = boto3.client('iot-data', region_name=region, endpoint_url=f"https://{iot_endpoint}")
    print(
        f"AWS IoT Data client initialized successfully with endpoint '{iot_endpoint}', topic '{iot_topic}', region '{region}'.")
except Exception as e:
    print(f"Error initializing AWS IoT Data client: {e}")
    iot_client = None  # Handle this in the handler


@dataclass
class QueryPartition:
    start_offset: int
    end_offset: int
    query: str
    batch_size: int


def create_response(**kwargs) -> Dict[str, Any]:
    """Create response using orjson for faster serialization"""
    transfer_id = kwargs.get('transfer_id')
    row_count = kwargs.get('row_count', 0)
    chunk_count = kwargs.get('chunk_count', 0)
    schema_info = kwargs.get('schema_info')
    time_elapsed = kwargs.get('time_elapsed', 0)
    total_rows = kwargs.get('total_rows', 0)
    published_messages = kwargs.get('published_messages', 0)
    status = kwargs.get('status', "COMPLETED")
    error = kwargs.get('error')

    print(f"Creating response: transfer_id={transfer_id}, row_count={row_count}, chunk_count={chunk_count}, "
          f"schema_info={schema_info}, time_elapsed={time_elapsed:.2f}s, total_rows={total_rows}, "
          f"published_messages={published_messages}, status={status}, error={error}")

    return {
        "transferId": transfer_id,
        "metadata": {
            "rowCount": row_count,
            "chunkCount": chunk_count,
            "schema": schema_info,
            "isComplete": status == "COMPLETED",
            "timeElapsed": time_elapsed,
            "totalRows": total_rows,
            "publishedMessageCount": published_messages,
            "processingStatus": status,
            "error": error
        }
    }


class OptimizedBatchProcessor:
    def __init__(self, schema: pa.Schema):
        print(f"Initializing OptimizedBatchProcessor with schema: {schema}")
        self.schema = schema
        self.field_converters = self._create_field_converters()
        self.arrays = [[] for _ in range(len(self.schema))]
        print(f"Initialized OptimizedBatchProcessor with {len(self.schema)} fields.")

    def _create_field_converters(self):
        print("Creating field converters based on schema...")
        converters = []
        for field in self.schema:
            # Store field type to avoid closure issues
            field_type = field.type
            field_name = field.name

            if pa.types.is_timestamp(field_type):
                # Directly use the datetime object, no conversion needed
                converters.append(lambda x, _ft=field_type: x)
                print(f"Field '{field_name}': using identity converter for timestamp.")
            elif field_name == 'value_gpu' or pa.types.is_floating(field_type):
                converters.append(lambda x, _ft=field_type: float(x) if x is not None else 0.0)
                print(f"Field '{field_name}': using float converter.")
            elif pa.types.is_integer(field_type):
                converters.append(lambda x, _ft=field_type: int(x) if x is not None else 0)
                print(f"Field '{field_name}': using integer converter.")
            else:
                converters.append(lambda x, _ft=field_type: str(x) if x is not None else '')
                print(f"Field '{field_name}': using string converter.")
        return converters

    def process_batch(self, rows: list) -> bytes:
        print(f"Processing batch of {len(rows)} rows.")
        # Clear arrays for reuse
        for arr in self.arrays:
            arr.clear()

        try:
            # Process rows in bulk
            for row in rows:
                for i, (value, converter) in enumerate(zip(row, self.field_converters)):
                    try:
                        converted_value = converter(value)
                        self.arrays[i].append(converted_value)
                    except Exception as e:
                        print(f"Error converting value in column {i}: {value}, Error: {e}")
                        raise

            print("Creating Arrow arrays...")
            # Create Arrow arrays efficiently
            arrow_arrays = []
            for i, (arr, field) in enumerate(zip(self.arrays, self.schema)):
                try:
                    arrow_arrays.append(pa.array(arr, type=field.type))
                except Exception as e:
                    print(f"Error creating Arrow array for column {i} ({field.name}): {e}")
                    print(f"Sample values: {arr[:5]}")
                    raise

            record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)
            print(f"Created RecordBatch with {len(record_batch)} rows.")

            # Use faster compression
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, record_batch.schema) as writer:
                writer.write_batch(record_batch)

            compressed_data = lz4.frame.compress(sink.getvalue().to_pybytes(), compression_level=3)
            print(f"Compressed batch data: original size={len(sink.getvalue())} bytes, "
                  f"compressed size={len(compressed_data)} bytes.")
            return compressed_data

        except Exception as e:
            print(f"Error in process_batch: {e}")
            # Print detailed debug information
            print(f"Schema: {self.schema}")
            if len(rows) > 0:
                print(f"First row sample: {rows[0]}")
                print(f"First row types: {[type(v) for v in rows[0]]}")
            raise


class FastParallelPublisher:
    def __init__(self, iot_client, topic: str, transfer_id: str):
        print(f"Initializing FastParallelPublisher with topic='{topic}', transfer_id='{transfer_id}'.")
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
        self.max_chunk_size = int(MAX_MQTT_PAYLOAD_SIZE * 0.6)
        print(f"Max chunk size set to {self.max_chunk_size} bytes.")
        self.futures = [self.executor.submit(self._publish_worker) for _ in range(MAX_PUBLISHER_THREADS)]
        print(f"Started {MAX_PUBLISHER_THREADS} publisher threads.")
        self.rows_published = 0

    async def _publish_message(self, message: dict, attempt: int = 0):
        try:
            payload = orjson.dumps(message)
            if len(payload) > MAX_MQTT_PAYLOAD_SIZE:
                raise ValueError("Payload size exceeds limit")

            print(f"Publishing message with sequence={message['metadata']['sequence']}, attempt={attempt + 1}.")
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.iot_client.publish(
                    topic=self.topic,
                    qos=1,
                    payload=payload
                )
            )
            self.publish_count += 1
            print(f"Successfully published message with sequence={message['metadata']['sequence']}.")
        except Exception as e:
            print(
                f"Error publishing message with sequence={message['metadata']['sequence']} on attempt {attempt + 1}: {e}")
            if attempt < 2:
                wait_time = 0.5 * (attempt + 1)
                print(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                await self._publish_message(message, attempt + 1)
            else:
                print(
                    f"Failed to publish message with sequence={message['metadata']['sequence']} after {attempt + 1} attempts.")
                raise

    def _publish_worker(self):
        thread_name = threading.current_thread().name
        print(f"Publisher worker '{thread_name}' started.")
        while self.is_running:
            try:
                batch_item = self.publish_queue.get(timeout=0.1)
                if batch_item is None:
                    print(f"Publisher worker '{thread_name}' received shutdown signal.")
                    break

                data, is_final, partition_id, row_count = batch_item
                self.rows_published += row_count
                print(f"Publisher worker '{thread_name}' processing batch: partition_id={partition_id}, "
                      f"row_count={row_count}, is_final={is_final}.")

                # Split into smaller chunks
                chunks = [data[i:i + self.max_chunk_size] for i in range(0, len(data), self.max_chunk_size)]
                total_chunks = len(chunks)
                print(f"Batch split into {total_chunks} chunks.")

                for chunk_index, chunk in enumerate(chunks):
                    with self.sequence_lock:
                        self.sequence_number += 1
                        sequence = self.sequence_number
                        print(f"Assigned sequence number {sequence} to chunk {chunk_index + 1}/{total_chunks}.")

                    message = {
                        'type': 'arrow_data',
                        'metadata': {
                            'transfer_id': self.transfer_id,
                            'sequence': sequence,
                            'partition': partition_id,
                            'chunk_index': chunk_index,
                            'total_chunks': total_chunks,
                            'final': is_final and chunk_index == total_chunks - 1,
                            'timestamp': int(time.time() * 1000),
                            'row_count': row_count
                        },
                        'data': base64.b64encode(chunk).decode('utf-8')
                    }
                    print(
                        f"Publisher worker '{thread_name}' enqueuing message with sequence={sequence} for publishing.")

                    try:
                        asyncio.run(self._publish_message(message))
                    except Exception as e:
                        print(
                            f"Publisher worker '{thread_name}' failed to publish message with sequence={sequence}: {e}")
                        self.error = e
                        self.is_running = False
                        break

                self.publish_queue.task_done()
                print(f"Publisher worker '{thread_name}' completed processing batch.")

            except Empty:
                continue
            except Exception as e:
                print(f"Publisher worker '{thread_name}' encountered error: {e}")
                self.error = e
                self.is_running = False
                break
        print(f"Publisher worker '{thread_name}' exiting.")

    def publish(self, data: bytes, is_final: bool = False, partition_id: int = 0, row_count: int = 0):
        if not self.is_running:
            error_msg = f"Cannot publish, publisher is stopped. Error: {self.error}"
            print(error_msg)
            raise self.error if self.error else Exception("Publisher stopped")
        print(
            f"Enqueuing data for publishing: partition_id={partition_id}, row_count={row_count}, is_final={is_final}.")
        self.publish_queue.put((data, is_final, partition_id, row_count))

    def wait_completion(self):
        print("Waiting for publisher to complete...")
        self.publish_queue.join()
        self.is_running = False
        for _ in range(MAX_PUBLISHER_THREADS):
            self.publish_queue.put(None)
        print("Shutdown signals sent to publisher threads.")

        try:
            for i, future in enumerate(self.futures):
                future.result()
                print(f"Publisher thread {i + 1} has completed.")
        except Exception as e:
            print(f"Error during publisher threads execution: {e}")
            raise

        if self.error:
            print(f"Publisher encountered an error: {self.error}")
            raise self.error
        print(f"Total published messages: {self.publish_count}")
        return self.publish_count


async def process_partition(partition: QueryPartition, publisher: FastParallelPublisher,
                            batch_processor: OptimizedBatchProcessor, pool) -> int:
    print(f"Processing partition: start_offset={partition.start_offset}, end_offset={partition.end_offset}")
    rows_processed = 0

    async with pool.acquire() as conn:
        print(f"Acquired database connection for partition {partition.start_offset}-{partition.end_offset}.")
        # Set optimized session parameters
        print("Setting optimized PostgreSQL session parameters.")
        await conn.execute("""
            SET LOCAL work_mem = '256MB';
            SET LOCAL max_parallel_workers_per_gather = 4;
            SET LOCAL parallel_tuple_cost = 0.1;
            SET LOCAL parallel_setup_cost = 0.1;
            SET LOCAL enable_partitionwise_join = on;
            SET LOCAL enable_partitionwise_aggregate = on;
        """)
        print("Optimized PostgreSQL session parameters set.")

        # Modify the partition query to handle timestamps correctly
        partition_query = f"""
            WITH numbered_rows AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER () as row_num
                FROM ({partition.query}) base_query
            )
            SELECT * FROM numbered_rows
            WHERE row_num > {partition.start_offset}
              AND row_num <= {partition.end_offset}
        """
        print(f"Executing partition query for rows {partition.start_offset + 1} to {partition.end_offset}.")

        try:
            # First get column names from the query
            column_names = []
            async with conn.transaction():
                # Get the first record to extract column names
                first_record = await conn.fetchrow(f"SELECT * FROM ({partition.query}) t LIMIT 1")
                if first_record:
                    column_names = first_record.keys()
                    print(f"Retrieved column names: {column_names}")

            if not column_names:
                raise ValueError("No columns found in query result")

            async with conn.transaction():
                # Use fetch instead of cursor to get all columns
                batch_rows = []
                current_batch_size = 0

                # Fetch in smaller batches to maintain memory efficiency
                async for record in conn.cursor(partition_query):
                    # Convert record to list of values based on column names
                    row_values = [record[col_name] for col_name in column_names]
                    batch_rows.append(row_values)
                    current_batch_size += 1

                    # Process batch when it reaches the desired size
                    if current_batch_size >= partition.batch_size:
                        print(
                            f"Processing batch of {len(batch_rows)} rows from partition {partition.start_offset}-{partition.end_offset}")
                        if batch_rows:  # Ensure we have rows to process
                            batch_data = batch_processor.process_batch(batch_rows)
                            is_final = False  # Not final since we're still processing
                            publisher.publish(batch_data, is_final, partition.start_offset, len(batch_rows))
                            rows_processed += len(batch_rows)
                            print(f"Published batch: {rows_processed} rows processed so far in partition")

                        # Reset batch
                        batch_rows = []
                        current_batch_size = 0

                # Process any remaining rows in the final batch
                if batch_rows:
                    print(f"Processing final batch of {len(batch_rows)} rows from partition")
                    batch_data = batch_processor.process_batch(batch_rows)
                    is_final = True  # This is the final batch for this partition
                    publisher.publish(batch_data, is_final, partition.start_offset, len(batch_rows))
                    rows_processed += len(batch_rows)
                    print(f"Published final batch: {rows_processed} total rows processed in partition")

        except Exception as e:
            print(f"Error processing partition {partition.start_offset}-{partition.end_offset}: {str(e)}")
            print(f"Error type: {type(e)}")
            print(f"Error details: {e}")
            if batch_rows:
                print(f"Sample row from error batch: {batch_rows[0]}")
            print(f"Column names: {column_names if 'column_names' in locals() else 'Not retrieved'}")
            if 'record' in locals():
                print(f"Last record keys: {record.keys() if record else 'No record'}")
                print(f"Last record values: {dict(record) if record else 'No record'}")
            raise

    print(
        f"Completed processing partition {partition.start_offset}-{partition.end_offset}. Total rows processed: {rows_processed}")
    return rows_processed


async def get_schema(pool, query: str) -> Tuple[pa.Schema, Dict]:
    print(f"Retrieving schema for query: {query}")
    async with pool.acquire() as conn:
        print("Acquired database connection for schema retrieval.")
        # Get column information
        columns = await conn.fetch(f"SELECT * FROM ({query}) t LIMIT 0")
        print("Executed schema retrieval query.")

        fields = []
        schema_info = {}

        for column in columns:
            name = column.name
            pg_type = column.type_oid  # asyncpg uses 'type_oid' for the column type

            print(f"Processing column '{name}' with PostgreSQL type OID {pg_type}.")

            if pg_type in (1184, 1114):  # TIMESTAMPTZ or TIMESTAMP
                field_type = pa.timestamp('us', tz='UTC')
                schema_info[name] = "timestamp"
                print(f"Column '{name}' mapped to Arrow timestamp.")
            elif pg_type == 701 or name == 'value_gpu':  # FLOAT8
                field_type = pa.float64()
                schema_info[name] = "float"
                print(f"Column '{name}' mapped to Arrow float64.")
            elif pg_type == 20:  # BIGINT
                field_type = pa.int64()
                schema_info[name] = "integer"
                print(f"Column '{name}' mapped to Arrow int64.")
            elif pg_type == 23:  # INTEGER
                field_type = pa.int32()
                schema_info[name] = "integer"
                print(f"Column '{name}' mapped to Arrow int32.")
            else:
                field_type = pa.string()
                schema_info[name] = "string"
                print(f"Column '{name}' mapped to Arrow string.")

            fields.append(pa.field(name, field_type))

    schema = pa.schema(fields)
    print(f"Retrieved Arrow schema: {schema}")
    return schema, schema_info


async def _lambda_handler(event, context):
    print(f"Lambda handler invoked with event: {event}")
    start_time = time.time()
    publisher = None
    total_rows = 0
    transfer_id = None
    schema_info = None
    pool = None

    try:
        args = event['arguments']
        transfer_id = args['transferId']
        query = args['query']
        row_limit = min(args.get('rowLimit', DEFAULT_ROW_LIMIT), DEFAULT_ROW_LIMIT)
        batch_size = min(args.get('batchSize', DEFAULT_BATCH_SIZE), DEFAULT_BATCH_SIZE)
        print(
            f"Received arguments: transfer_id={transfer_id}, query='{query}', row_limit={row_limit}, batch_size={batch_size}")

        # Initialize connection pool
        print("Initializing database connection pool...")
        pool = await init_db_pool()

        # Get schema information
        print("Retrieving schema information...")
        schema, schema_info = await get_schema(pool, query)
        print("Schema retrieval complete.")

        # Initialize publisher and batch processor
        print("Initializing FastParallelPublisher and OptimizedBatchProcessor...")
        publisher = FastParallelPublisher(iot_client, iot_topic, transfer_id)
        batch_processor = OptimizedBatchProcessor(schema)
        print("Publisher and Batch Processor initialized.")

        # Create optimized partitions
        partition_size = max(50000, row_limit // PARALLEL_QUERIES)
        print(f"Partitioning data into chunks of size {partition_size} rows.")
        partitions = [
            QueryPartition(
                start_offset=i * partition_size,
                end_offset=min((i + 1) * partition_size, row_limit),
                query=query,
                batch_size=batch_size
            ) for i in range(PARALLEL_QUERIES)
        ]
        print(f"Total partitions created: {len(partitions)}")

        # Process partitions concurrently
        print("Starting partition processing tasks.")
        tasks = [process_partition(p, publisher, batch_processor, pool) for p in partitions]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Partition processing tasks completed.")

        # Handle results
        errors = [r for r in results if isinstance(r, Exception)]
        successful_results = [r for r in results if isinstance(r, int)]
        total_rows = sum(successful_results)
        if errors:
            print(f"Encountered errors in partitions: {errors}")
        else:
            print("All partitions processed successfully.")

        # Wait for publisher to complete
        print("Waiting for publisher to complete...")
        publisher.wait_completion()
        print("Publisher has completed.")

        # Calculate execution time
        execution_time = time.time() - start_time
        status = "COMPLETED" if total_rows > 0 and not errors else "PARTIAL"
        print(f"Execution time: {execution_time:.2f} seconds, Status: {status}")

        # Create and return response
        response = create_response(
            transfer_id=transfer_id,
            row_count=total_rows,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows,
            published_messages=publisher.publish_count if publisher else 0,
            status=status
        )
        print(f"Response created: {response}")
        return response

    except Exception as e:
        execution_time = time.time() - start_time
        print(f"Exception in lambda handler: {e}")
        response = create_response(
            transfer_id=transfer_id,
            row_count=total_rows,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows,
            published_messages=publisher.publish_count if publisher else 0,
            status="ERROR",
            error=str(e)
        )
        print(f"Error response created: {response}")
        return response

    finally:
        if publisher:
            try:
                print("Ensuring publisher has completed...")
                publisher.wait_completion()
                print("Publisher cleanup complete.")
            except Exception as e:
                print(f"Error during publisher cleanup: {e}")
        if pool:
            print("Closing database connection pool...")
            await pool.close()
            print("Database connection pool closed.")


def lambda_handler(event, context):
    print("lambda_handler invoked.")
    return asyncio.run(_lambda_handler(event, context))
