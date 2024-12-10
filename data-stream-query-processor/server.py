import json
import os
import psycopg2.pool
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import zlib
import time
from contextlib import contextmanager
from typing import List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import asyncio

# Constants
COMPRESSION_LEVEL = 1
MAX_SQS_MESSAGE_SIZE = 256 * 1024
MAX_PAYLOAD_SIZE = int(MAX_SQS_MESSAGE_SIZE * 0.7)
MAX_THREADS = 8
BATCH_SIZE = 1000
QUEUE_SIZE = 50
MAX_DB_RETRIES = 3
DB_RETRY_DELAY = 1

print("Starting application with configuration:")
print(f"MAX_PAYLOAD_SIZE: {MAX_PAYLOAD_SIZE:,} bytes")
print(f"BATCH_SIZE: {BATCH_SIZE}")
print(f"MAX_THREADS: {MAX_THREADS}")
print(f"QUEUE_SIZE: {QUEUE_SIZE}")
print(f"DB_RETRY_DELAY: {DB_RETRY_DELAY}s")
print("-" * 50)

# Initialize PostgreSQL connection pool
try:
    print("Initializing database connection pool...")
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME']
    )
    print(f"Database pool created successfully (min={2}, max={10})")
except Exception as e:
    print(f"CRITICAL: Database pool initialization failed: {str(e)}")
    db_pool = None

# Initialize SQS client
try:
    print("Initializing AWS SQS client...")
    sqs = boto3.client('sqs')
    OUTPUT_QUEUE_URL = os.environ['OUTPUT_QUEUE_URL']
    print(f"SQS client initialized successfully. Queue URL: {OUTPUT_QUEUE_URL}")
except Exception as e:
    print(f"CRITICAL: SQS client initialization failed: {str(e)}")
    sqs = None


@contextmanager
def get_db_connection():
    """Get database connection from pool with retries"""
    conn = None
    start_time = time.time()
    for attempt in range(MAX_DB_RETRIES):
        try:
            if db_pool is None:
                raise Exception("Database pool not initialized")

            print(f"Attempting to get database connection (attempt {attempt + 1}/{MAX_DB_RETRIES})...")
            conn = db_pool.getconn()
            if conn:
                print(f"Successfully acquired database connection after {time.time() - start_time:.2f}s")
                yield conn
                return
        except Exception as e:
            last_error = e
            if attempt < MAX_DB_RETRIES - 1:
                print(f"WARNING: Database connection attempt {attempt + 1} failed: {str(e)}")
                print(f"Waiting {DB_RETRY_DELAY}s before retry...")
                time.sleep(DB_RETRY_DELAY)
            continue
        finally:
            if conn:
                try:
                    db_pool.putconn(conn)
                    print("Successfully returned connection to pool")
                except Exception as e:
                    print(f"ERROR: Failed to return connection to pool: {str(e)}")

    print(f"CRITICAL: All database connection attempts failed after {time.time() - start_time:.2f}s")
    raise Exception(f"Failed to get database connection after {MAX_DB_RETRIES} attempts: {str(last_error)}")


class ParallelBatchProcessor:
    """Handles parallel processing of data batches with Arrow conversion"""

    def __init__(self, schema_info: dict):
        print("\nInitializing ParallelBatchProcessor...")
        print(f"Schema info: {json.dumps(schema_info, indent=2)}")
        self.schema = self._create_schema(schema_info)
        self.field_converters = self._create_field_converters()
        self.arrays = [[] for _ in range(len(self.schema))]
        self.preview_sent = False
        self.batch_queue = Queue(maxsize=QUEUE_SIZE)
        self.sqs_queue = Queue(maxsize=QUEUE_SIZE)
        self.error = None
        self.processed_rows = 0
        self.lock = threading.Lock()
        self.max_chunk_size = int(MAX_PAYLOAD_SIZE * 0.9)
        print(f"ParallelBatchProcessor initialized with {len(self.schema)} fields")
        print(f"Maximum chunk size: {self.max_chunk_size:,} bytes")

    def _show_preview(self, rows: List[tuple], message_info: Dict):
        """Show a preview of the first row for debugging"""
        if rows and not self.preview_sent:
            print("\nData Preview:")
            print("First row field values:")
            for field, value in zip(self.schema, rows[0]):
                print(f"- {field.name}: {value}")
            self.preview_sent = True

    def _chunk_data(self, data: bytes) -> List[bytes]:
        """Split data into appropriately sized chunks for SQS"""
        if len(data) <= self.max_chunk_size:
            return [data]

        chunks = []
        for i in range(0, len(data), self.max_chunk_size):
            chunk = data[i:i + self.max_chunk_size]
            chunks.append(chunk)
        return chunks

    def process_rows(self, rows: List[tuple], message_info: Dict) -> Tuple[List[bytes], int]:
        """Process a batch of rows into Arrow format with size validation"""
        batch_start_time = time.time()
        print(f"\nProcessing batch of {len(rows)} rows...")

        if not self.preview_sent and rows:
            self._show_preview(rows, message_info)

        # Clear arrays for reuse
        for arr in self.arrays:
            arr.clear()

        # Process rows in bulk
        for row in rows:
            for i, (value, converter) in enumerate(zip(row, self.field_converters)):
                try:
                    converted_value = converter(value)
                    self.arrays[i].append(converted_value)
                except Exception as e:
                    print(f"Warning: Error converting value at index {i}: {str(e)}")
                    self.arrays[i].append(None)

        # Create Arrow arrays efficiently
        arrow_arrays = []
        for i, (array_data, field) in enumerate(zip(self.arrays, self.schema)):
            try:
                arrow_arrays.append(pa.array(array_data, type=field.type))
            except Exception as e:
                print(f"Error creating Arrow array for field {field.name}: {str(e)}")
                # Create array of nulls as fallback
                arrow_arrays.append(pa.array([None] * len(array_data), type=field.type))

        try:
            record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)
            print(f"Created Arrow record batch with {len(rows)} rows")

            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, record_batch.schema) as writer:
                writer.write_batch(record_batch)

            uncompressed_size = len(sink.getvalue())
            compressed_data = zlib.compress(sink.getvalue().to_pybytes(), level=COMPRESSION_LEVEL)
            compressed_size = len(compressed_data)
            compression_ratio = (1 - compressed_size / uncompressed_size) * 100

            print(f"Data compression stats:")
            print(f"- Uncompressed size: {uncompressed_size:,} bytes")
            print(f"- Compressed size: {compressed_size:,} bytes")
            print(f"- Compression ratio: {compression_ratio:.1f}%")

            chunks = self._chunk_data(compressed_data)
            print(f"Split data into {len(chunks)} chunks")

            with self.lock:
                self.processed_rows += len(rows)
                print(f"Total rows processed so far: {self.processed_rows:,}")

            batch_time = time.time() - batch_start_time
            print(f"Batch processing completed in {batch_time:.2f} seconds")
            print(f"Processing speed: {len(rows) / batch_time:.1f} rows/second")

            return chunks, len(rows)

        except Exception as e:
            print(f"ERROR: Failed to process batch: {str(e)}")
            raise

    def _create_schema(self, schema_info: dict) -> pa.Schema:
        """Create Arrow schema from schema info dictionary"""
        print("\nCreating Arrow schema...")
        fields = []
        for name, type_str in schema_info.items():
            print(f"Field: {name}, Type: {type_str}")
            if type_str == "timestamp":
                field_type = pa.timestamp('us', tz='UTC')
            elif type_str == "float":
                field_type = pa.float64()
            elif type_str == "integer":
                field_type = pa.int64()
            else:
                field_type = pa.string()
            fields.append(pa.field(name, field_type))
        schema = pa.schema(fields)
        print("Arrow schema created successfully")
        return schema

    def _create_field_converters(self):
        """Create field converters with proper timestamp handling"""
        converters = []

        for field in self.schema:
            field_type = field.type

            if pa.types.is_timestamp(field_type):
                def timestamp_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        if isinstance(value, str):
                            # Parse ISO format timestamp string to datetime
                            from datetime import datetime
                            import pytz
                            # Remove timezone if present and parse
                            if '+' in value:
                                dt_str = value.split('+')[0]
                            else:
                                dt_str = value
                            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                            # Add UTC timezone
                            dt = pytz.UTC.localize(dt)
                            # Convert to microseconds since epoch
                            return int(dt.timestamp() * 1000000)
                        return value
                    except Exception as e:
                        print(f"Warning: Could not convert '{value}' to timestamp for {field_name}: {str(e)}")
                        return None

                converters.append(timestamp_converter)

            elif pa.types.is_floating(field_type):
                def float_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        # Handle various string formats
                        if isinstance(value, str):
                            # Remove any non-numeric characters except decimal point and minus
                            clean_value = ''.join(c for c in value if c.isdigit() or c in '.-')
                            return float(clean_value) if clean_value else None
                        return float(value)
                    except Exception as e:
                        print(f"Warning: Could not convert '{value}' to float for {field_name}: {str(e)}")
                        return None

                converters.append(float_converter)

            elif pa.types.is_integer(field_type):
                def int_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        # Handle various string formats
                        if isinstance(value, str):
                            # Remove any non-numeric characters except minus
                            clean_value = ''.join(c for c in value if c.isdigit() or c == '-')
                            return int(clean_value) if clean_value else None
                        return int(value)
                    except Exception as e:
                        print(f"Warning: Could not convert '{value}' to integer for {field_name}: {str(e)}")
                        return None

                converters.append(int_converter)

            else:  # String/default converter
                def string_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        return str(value)
                    except Exception as e:
                        print(f"Warning: Could not convert value to string for {field_name}: {str(e)}")
                        return None

                converters.append(string_converter)

        return converters


async def process_partition(message: Dict, processor: ParallelBatchProcessor) -> int:
    """Process a single partition of data with connection error handling"""
    partition_start_time = time.time()
    print(f"\nStarting partition processing:")
    print(f"Transfer ID: {message['transfer_id']}")
    print(f"Partition ID: {message['partition_id']}")
    print(f"Order: {message['order']}")
    print(f"Offset range: {message['start_offset']} - {message['end_offset']}")

    rows_processed = 0
    sequence = 0

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                print("\nOptimizing database connection...")
                cursor.execute("""
                    SET work_mem = '128MB';
                    SET max_parallel_workers_per_gather = 2;
                    SET parallel_tuple_cost = 0.1;
                    SET parallel_setup_cost = 0.1;
                """)

                print("Executing partition query...")
                partition_query = f"""
                    WITH numbered_rows AS (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num
                        FROM ({message['query']}) base_query
                    )
                    SELECT * FROM numbered_rows
                    WHERE row_num > {message['start_offset']}
                      AND row_num <= {message['end_offset']}
                """

                query_start_time = time.time()
                cursor.execute(partition_query)
                print(f"Query execution started after {time.time() - query_start_time:.2f}s")

                while True:
                    batch_start = time.time()
                    rows = cursor.fetchmany(BATCH_SIZE)
                    if not rows:
                        print("No more rows to fetch")
                        break

                    print(f"\nProcessing batch of {len(rows)} rows...")
                    chunks, row_count = processor.process_rows(rows, message)
                    total_chunks = len(chunks)

                    print(f"Sending {total_chunks} chunks to SQS...")
                    for chunk_index, chunk_data in enumerate(chunks):
                        sequence += 1
                        metadata = {
                            'transfer_id': message['transfer_id'],
                            'partition_id': message['partition_id'],
                            'order': message['order'],
                            'sequence': sequence,
                            'chunk_index': chunk_index,
                            'total_chunks': total_chunks,
                            'row_count': row_count if chunk_index == total_chunks - 1 else 0,
                            'timestamp': int(time.time() * 1000),
                            'is_final': (not rows or len(rows) < BATCH_SIZE) and chunk_index == total_chunks - 1,
                            'estimated_size': message.get('estimated_size')
                        }

                        message_body = {
                            **metadata,
                            'data': chunk_data.hex()
                        }

                        message_size = len(json.dumps(message_body).encode('utf-8'))
                        print(f"Chunk {chunk_index + 1}/{total_chunks}:")
                        print(f"- Message size: {message_size:,} bytes")
                        print(f"- Sequence: {sequence}")

                        if message_size > MAX_SQS_MESSAGE_SIZE:
                            raise ValueError(
                                f"Message size {message_size:,} exceeds SQS limit of {MAX_SQS_MESSAGE_SIZE:,}")

                        try:
                            sqs.send_message(
                                QueueUrl=OUTPUT_QUEUE_URL,
                                MessageBody=json.dumps(message_body),
                                MessageAttributes={
                                    'transfer_id': {
                                        'DataType': 'String',
                                        'StringValue': str(metadata['transfer_id'])
                                    },
                                    'order': {
                                        'DataType': 'Number',
                                        'StringValue': str(metadata['order'])
                                    }
                                }
                            )

                            if chunk_index == total_chunks - 1:
                                rows_processed += row_count

                        except Exception as e:
                            print(f"ERROR: Failed to send message to SQS: {str(e)}")
                            raise

                    batch_time = time.time() - batch_start
                    print(f"Batch completed in {batch_time:.2f}s ({len(rows) / batch_time:.1f} rows/second)")

    except Exception as e:
        print(f"ERROR: Failed to process partition {message['partition_id']}: {str(e)}")
        raise

    total_time = time.time() - partition_start_time
    print(f"\nPartition processing completed:")
    print(f"- Total rows processed: {rows_processed:,}")
    print(f"- Total time: {total_time:.2f}s")
    print(f"- Average speed: {rows_processed / total_time:.1f} rows/second")

    return rows_processed


async def process_messages(messages: List[Dict]) -> Dict[str, Any]:
    """Process messages with improved error handling"""
    start_time = time.time()
    print(f"\nStarting message processing...")
    print(f"Number of messages to process: {len(messages)}")

    total_rows = 0

    try:
        if not messages:
            print("No messages to process")
            return {'statusCode': 200}

        if db_pool is None:
            raise Exception("Database connection pool not available")

        first_message = messages[0]
        print(f"\nInitializing processor with schema from first message:")
        print(f"Transfer ID: {first_message['transfer_id']}")
        processor = ParallelBatchProcessor(first_message['schema_info'])

        max_concurrent = min(len(messages), 5)
        print(f"Processing messages in batches of {max_concurrent}")

        for i in range(0, len(messages), max_concurrent):
            batch = messages[i:i + max_concurrent]
            print(f"\nProcessing batch {i // max_concurrent + 1}:")
            print(f"- Batch size: {len(batch)}")

            batch_start = time.time()
            tasks = [process_partition(message, processor) for message in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            errors = [r for r in results if isinstance(r, Exception)]
            successful_results = [r for r in results if isinstance(r, int)]
            batch_rows = sum(successful_results)
            total_rows += batch_rows

            batch_time = time.time() - batch_start
            print(f"\nBatch {i // max_concurrent + 1} completed:")
            print(f"- Processed {batch_rows:,} rows in {batch_time:.2f}s")
            print(f"- Speed: {batch_rows / batch_time:.1f} rows/second")

            if errors:
                print(f"\nERRORS in batch {i // max_concurrent + 1}:")
                for idx, error in enumerate(errors, 1):
                    print(f"Error {idx}: {str(error)}")
                if not successful_results:
                    raise errors[0]

        execution_time = time.time() - start_time
        print(f"\nProcessing summary:")
        print(f"- Total execution time: {execution_time:.2f}s")
        print(f"- Total rows processed: {total_rows:,}")
        print(f"- Average speed: {total_rows / execution_time:.1f} rows/second")
        print("Processing completed successfully")

        return {'statusCode': 200}

    except Exception as e:
        error_time = time.time() - start_time
        print(f"\nCRITICAL: Message processing failed after {error_time:.2f}s")
        print(f"Error details: {str(e)}")
        print(f"Rows processed before error: {total_rows:,}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def calculate_optimal_batch_size() -> int:
    """Calculate optimal SQS batch size based on performance metrics"""
    # Assuming ~1000 rows can be processed per second (based on logs)
    # And average message contains ~1300 rows (based on logs)
    # Target processing time of 8 seconds (2 second buffer)
    TARGET_PROCESSING_TIME = 8
    ROWS_PER_SECOND = 1000
    AVG_ROWS_PER_MESSAGE = 1300

    max_rows = TARGET_PROCESSING_TIME * ROWS_PER_SECOND
    return max(1, min(10, int(max_rows / AVG_ROWS_PER_MESSAGE)))


def lambda_handler(event, context):
    """Optimized Lambda handler with message limiting"""
    handler_start_time = time.time()
    print("\n=== Lambda Handler Started ===")
    print(f"Lambda function: {context.function_name}")
    print(f"Request ID: {context.aws_request_id}")

    # Calculate optimal batch size
    optimal_batch = calculate_optimal_batch_size()
    print(f"Calculated optimal batch size: {optimal_batch} messages")

    # Limit number of messages to process
    records = event.get('Records', [])[:optimal_batch]
    print(f"Processing {len(records)} out of {len(event.get('Records', []))} received messages")

    messages = []
    total_rows_estimate = 0

    for idx, record in enumerate(records, 1):
        try:
            message = json.loads(record['body'])
            row_count = message['end_offset'] - message['start_offset']

            # Early exit if estimated processing time would exceed target
            if (total_rows_estimate + row_count) / 1000 > 8:  # Assuming 1000 rows/second
                print(f"Stopping at {idx} messages to maintain performance target")
                break

            messages.append(message)
            total_rows_estimate += row_count

            print(f"Added message {idx} (estimated {row_count:,} rows)")
            print(f"Cumulative estimated rows: {total_rows_estimate:,}")

        except Exception as e:
            print(f"ERROR: Failed to parse message {idx}: {str(e)}")
            continue

    print(f"\nProcessing {len(messages)} messages")
    print(f"Estimated total rows: {total_rows_estimate:,}")
    print(f"Estimated processing time: {total_rows_estimate / 1000:.1f}s")

    result = asyncio.run(process_messages(messages))

    handler_time = time.time() - handler_start_time
    print("\n=== Lambda Handler Completed ===")
    print(f"Total execution time: {handler_time:.2f}s")
    print(f"Processing speed: {total_rows_estimate / handler_time:.1f} rows/second")

    return result