import re
import json
import math
import os
import psycopg2.pool
import pyarrow as pa
import pyarrow.ipc as ipc
import zlib
import time
from contextlib import contextmanager
from typing import List, Tuple, Dict, Any
import threading
from queue import Queue
import asyncio
import aioboto3
from botocore.config import Config


# Constants
COMPRESSION_LEVEL = 1
MAX_SQS_MESSAGE_SIZE = 256 * 1024
MAX_PAYLOAD_SIZE = int(MAX_SQS_MESSAGE_SIZE * 0.7)
MAX_THREADS = 8
QUEUE_SIZE = 50
MAX_DB_RETRIES = 3
DB_RETRY_DELAY = 1
OUTPUT_QUEUE_URL = os.getenv('OUTPUT_QUEUE_URL')

print("Starting application with configuration:")
print(f"MAX_PAYLOAD_SIZE: {MAX_PAYLOAD_SIZE:,} bytes")
print(f"MAX_THREADS: {MAX_THREADS}")
print(f"QUEUE_SIZE: {QUEUE_SIZE}")
print(f"DB_RETRY_DELAY: {DB_RETRY_DELAY}s")
print("-" * 50)

# Initialize PostgreSQL connection pool
try:
    print("Initializing database connection pool...")
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=20,
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME'],
        options='-c statement_timeout=25000',
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )
    print(f"Database pool created successfully (min={2}, max={10})")
except Exception as e:
    print(f"CRITICAL: Database pool initialization failed: {str(e)}")
    db_pool = None


boto3_session = aioboto3.Session()
config = Config(
    retries=dict(
        max_attempts=3
    )
)


def calculate_optimal_batch_size(query_text: str = '') -> int:
    """Calculate optimal batch size based on available time and query complexity"""
    # Reduced base batch size for better timeout handling
    base_batch_size = 50  # Reduced from 6 to 4

    if not query_text:
        return base_batch_size

    if 'GROUP BY' in query_text.upper() or 'ORDER BY' in query_text.upper():
        return max(base_batch_size // 2, 25)

    if query_text.upper().count('JOIN') > 1:
        return max(base_batch_size // 2, 25)

    return base_batch_size


class ProgressTracker:
    def __init__(self):
        self.processed_rows = 0
        self.processed_messages = 0
        self.start_time = time.time()
        self._lock = asyncio.Lock()

    async def update(self, rows: int, messages: int = 1):
        async with self._lock:
            self.processed_rows += rows
            self.processed_messages += messages

    def get_stats(self):
        duration = time.time() - self.start_time
        return {
            'processed_rows': self.processed_rows,
            'processed_messages': self.processed_messages,
            'duration_seconds': duration,
            'rows_per_second': self.processed_rows / duration if duration > 0 else 0
        }


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
                break
        except Exception as e:
            print(f"WARNING: Database connection attempt {attempt + 1} failed: {str(e)}")
            if conn:
                try:
                    db_pool.putconn(conn)
                except Exception as put_error:
                    print(f"Error returning connection to pool: {str(put_error)}")
                conn = None

            if attempt < MAX_DB_RETRIES - 1:
                print(f"Waiting {DB_RETRY_DELAY}s before retry...")
                time.sleep(DB_RETRY_DELAY)
            else:
                print(f"CRITICAL: All database connection attempts failed after {time.time() - start_time:.2f}s")
                raise
        finally:
            if conn:
                try:
                    db_pool.putconn(conn)
                    print("Successfully returned connection to pool")
                except Exception as e:(
                        print(f"ERROR: Failed to return connection to pool: {str(e)}"))


class ParallelBatchProcessor:
    """Handles parallel processing of data batches with Arrow conversion and chunk tracking"""

    def __init__(self, schema_info: dict, expected_total_rows: int = None):
        print("\nInitializing ParallelBatchProcessor...")
        self.schema = self._create_schema(schema_info)
        self.field_converters = self._create_field_converters()
        self.arrays = [[] for _ in range(len(self.schema))]
        self.preview_sent = False
        self.batch_queue = Queue(maxsize=QUEUE_SIZE)
        self.sqs_queue = Queue(maxsize=QUEUE_SIZE)
        self.error = None
        self.processed_rows = 0
        self.total_chunks_processed = 0
        self.expected_total_rows = expected_total_rows
        self.lock = threading.Lock()
        self.max_chunk_size = int(MAX_PAYLOAD_SIZE * 0.9)
        self.start_time = time.time()
        self.timeout = 25  # 25 second timeout

        # New tracking attributes
        self.batches_processed = 0
        self.total_chunks_expected = None
        if expected_total_rows:
            avg_rows_per_chunk = 1000
            estimated_chunks = math.ceil(expected_total_rows / avg_rows_per_chunk)
            self.total_chunks_expected = estimated_chunks
            print(f"Expecting approximately {estimated_chunks:,} chunks based on {expected_total_rows:,} rows")

        print(f"ParallelBatchProcessor initialized with {len(self.schema)} fields")
        print(f"Maximum chunk size: {self.max_chunk_size:,} bytes")

    def check_timeout(self, context: str = ""):
        """Check if we're approaching timeout"""
        elapsed = time.time() - self.start_time
        if elapsed > self.timeout:
            raise TimeoutError(f"Operation timed out during {context}. Elapsed: {elapsed:.2f}s")

    def process_rows(self, rows: List[tuple], message_info: Dict) -> Tuple[List[bytes], int]:
        """
        Process a batch of rows into Arrow format with optimized performance and timeout handling.

        Args:
            rows: List of database row tuples
            message_info: Dictionary containing message metadata

        Returns:
            Tuple containing list of compressed data chunks and row count

        Raises:
            TimeoutError: If processing time approaches Lambda timeout
        """
        start_time = time.time()
        batch_start_time = time.time()
        print(f"\nProcessing batch {self.batches_processed + 1}")
        print(f"Chunks processed so far: {self.total_chunks_processed:,}")

        def check_time():
            """Check if we're approaching timeout"""
            if time.time() - start_time > 20:  # Leave 10s buffer
                raise TimeoutError("Approaching timeout limit")

        if not self.preview_sent and rows:
            self._show_preview(rows, message_info)

        # Clear arrays for reuse
        for arr in self.arrays:
            arr.clear()

        # Process rows based on batch size
        MIN_ROWS_FOR_SUBBATCH = 100
        if len(rows) < MIN_ROWS_FOR_SUBBATCH:
            # Direct processing for small batches
            try:
                print(f"Processing {len(rows)} rows directly")
                for row in rows:
                    for j, (value, converter) in enumerate(zip(row, self.field_converters)):
                        try:
                            converted_value = converter(value)
                            self.arrays[j].append(converted_value)
                        except Exception as e:
                            print(f"Warning: Error converting value at index {j}: {str(e)}")
                            self.arrays[j].append(None)

                    # Periodic timeout check every 100 rows
                    if len(self.arrays[0]) % 100 == 0:
                        check_time()

            except Exception as e:
                print(f"Error during row conversion: {str(e)}")
                raise

        else:
            # Use sub-batches for larger datasets
            sub_batch_size = max(MIN_ROWS_FOR_SUBBATCH // 2, 50)
            print(f"Processing {len(rows)} rows in sub-batches of {sub_batch_size}")

            for i in range(0, len(rows), sub_batch_size):
                check_time()  # Check time before each sub-batch
                sub_batch_start = time.time()

                # Process sub-batch of rows
                sub_rows = rows[i:i + sub_batch_size]
                print(f"Processing sub-batch {i // sub_batch_size + 1}/{math.ceil(len(rows) / sub_batch_size)}")

                for row in sub_rows:
                    for j, (value, converter) in enumerate(zip(row, self.field_converters)):
                        try:
                            converted_value = converter(value)
                            self.arrays[j].append(converted_value)
                        except Exception as e:
                            print(f"Warning: Error converting value at index {j}: {str(e)}")
                            self.arrays[j].append(None)

                sub_batch_time = time.time() - sub_batch_start
                print(f"Sub-batch processed in {sub_batch_time:.3f}s")

        try:
            print("Creating Arrow arrays...")
            check_time()
            arrow_arrays = []

            # Create all arrays in a single pass
            for i, (array_data, field) in enumerate(zip(self.arrays, self.schema)):
                try:
                    print(f"Creating array for field '{field.name}'...")
                    arrow_arrays.append(pa.array(array_data, type=field.type))
                except Exception as e:
                    print(f"Error creating Arrow array for field {field.name}: {str(e)}")
                    arrow_arrays.append(pa.array([None] * len(array_data), type=field.type))

            print("Creating record batch...")
            check_time()
            record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)
            print(f"Created Arrow record batch with {len(rows)} rows")

            print("Compressing data...")
            check_time()
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

            print("Splitting data into chunks...")
            check_time()
            chunks = self._chunk_data(compressed_data)

            with self.lock:
                self.total_chunks_processed += len(chunks)
                self.processed_rows += len(rows)
                self.batches_processed += 1

                progress = self.get_progress()
                print("\nProgress Update:")
                print(f"- Total chunks processed: {progress['chunks_processed']:,}")
                print(f"- Total rows processed: {progress['rows_processed']:,}")
                print(f"- Total batches processed: {progress['batches_processed']:,}")
                if progress['estimated_completion']:
                    print(f"- Estimated completion: {progress['estimated_completion']}")

            batch_time = time.time() - batch_start_time
            print(f"Batch processing completed in {batch_time:.2f} seconds")
            print(f"Processing speed: {len(rows) / batch_time:.1f} rows/second")

            return chunks, len(rows)

        except TimeoutError:
            print(f"WARNING: Processing timed out after {time.time() - start_time:.2f}s")
            raise
        except Exception as e:
            print(f"ERROR: Failed to process batch: {str(e)}")
            raise

    def get_progress(self) -> Dict[str, Any]:
        """Get current processing progress statistics"""
        with self.lock:
            stats = {
                'chunks_processed': self.total_chunks_processed,
                'rows_processed': self.processed_rows,
                'batches_processed': self.batches_processed,
                'estimated_completion': None
            }

            if self.total_chunks_expected:
                completion_percentage = (self.total_chunks_processed / self.total_chunks_expected) * 100
                stats['estimated_completion'] = f"{completion_percentage:.1f}%"

            return stats

    def mark_partition_complete(self, partition_id: str):
        """Mark a partition as complete and update tracking"""
        with self.lock:
            print(f"\nPartition {partition_id} completed:")
            print(f"- Total chunks in this partition: {self.total_chunks_processed:,}")
            print(f"- Total rows in this partition: {self.processed_rows:,}")

            if self.total_chunks_expected:
                remaining_chunks = self.total_chunks_expected - self.total_chunks_processed
                print(f"- Estimated remaining chunks: {remaining_chunks:,}")

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

    def _create_schema(self, schema_info: dict) -> pa.Schema:
        """Create Arrow schema from schema info dictionary"""
        print("\nCreating Arrow schema...")
        fields = []
        for name, type_str in schema_info.items():
            # print(f"Field: {name}, Type: {type_str}")
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
        timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})(?:\+\d{2}:?\d{2})?')

        converters = []
        for field in self.schema:
            field_type = field.type

            if pa.types.is_timestamp(field_type):
                def timestamp_converter(value, pattern=timestamp_pattern):
                    if value is None:
                        return None
                    try:
                        if isinstance(value, str):
                            # Extract datetime part without timezone
                            match = pattern.match(value)
                            if match:
                                from datetime import datetime
                                dt = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
                                return int(dt.timestamp() * 1000000)
                        return value
                    except Exception:
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


async def process_partition(message: Dict, processor: ParallelBatchProcessor) -> Dict[str, Any]:
    """
    Process a single partition of data with enhanced progress tracking.
    Returns statistics about the processing results.
    """
    partition_start_time = time.time()
    tracker = ProgressTracker()

    print(f"\nStarting partition processing:")
    print(f"Transfer ID: {message['transfer_id']}")
    print(f"Partition ID: {message['partition_id']}")
    print(f"Order: {message['order']}")
    print(f"Offset range: {message['start_offset']} - {message['end_offset']}")
    print(f"Expected rows: {message.get('total_rows', 'Unknown')}")

    # Calculate optimal batch size based on query
    batch_size = calculate_optimal_batch_size(message['query'])
    print(f"Calculated optimal batch size: {batch_size}")

    sequence = 0
    total_rows_processed = 0

    try:
        with get_db_connection() as conn:
            # Set session parameters using a separate cursor
            with conn.cursor() as setup_cursor:
                print("Setting session parameters...")
                params = [
                    "work_mem = '256MB'",
                    "statement_timeout = '25s'"
                ]

                for param in params:
                    setup_cursor.execute(f"SET LOCAL {param}")
                conn.commit()
                print("Session parameters set successfully")

            cursor_name = f'partition_cursor_{message["partition_id"]}'
            print(f"Creating cursor: {cursor_name}")

            with conn.cursor(name=cursor_name) as cursor:
                partition_query = f"""
                    SELECT *
                    FROM ({message['query']}) base_query
                    OFFSET {message['start_offset']}
                    LIMIT {batch_size}
                """

                print(f"Executing partition query:\n{partition_query}")
                query_start_time = time.time()

                cursor.execute(partition_query)
                print(f"Query execution started after {time.time() - query_start_time:.2f}s")

                while True:
                    batch_start = time.time()
                    rows = cursor.fetchmany(batch_size)

                    if not rows:
                        print("No more rows to fetch")
                        break

                    print(f"\nProcessing batch of {len(rows)} rows...")
                    chunks, row_count = processor.process_rows(rows, message)
                    total_chunks = len(chunks)

                    # Get current progress stats before sending messages
                    progress_stats = processor.get_progress()
                    print(f"\nCurrent progress:")
                    print(f"- Chunks processed: {progress_stats['chunks_processed']:,}")
                    if progress_stats['estimated_completion']:
                        print(f"- Overall completion: {progress_stats['estimated_completion']}")

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
                            'is_final': (not rows or len(rows) < batch_size) and chunk_index == total_chunks - 1,
                            'estimated_size': message.get('estimated_size'),
                            'progress': progress_stats['estimated_completion']
                        }

                        message_body = {
                            **metadata,
                            'data': chunk_data.hex()
                        }

                        try:
                            print(f"Sending message to SQS. Order # {metadata['order']}")
                            await send_sqs_message(
                                message_body=message_body,
                                queue_url=OUTPUT_QUEUE_URL,
                                message_attributes={
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
                                total_rows_processed += row_count
                                await tracker.update(row_count)

                        except Exception as e:
                            print(f"ERROR: Failed to send message to SQS: {str(e)}")
                            raise

                    batch_time = time.time() - batch_start
                    print(f"Batch completed in {batch_time:.2f}s ({len(rows) / batch_time:.1f} rows/second)")

                    if time.time() - partition_start_time > 25:
                        print("Approaching Lambda timeout, saving progress...")
                        break

        # Mark partition as complete
        processor.mark_partition_complete(message['partition_id'])

    except Exception as e:
        print(f"ERROR: Failed to process partition {message['partition_id']}: {str(e)}")
        raise

    total_time = time.time() - partition_start_time
    stats = {
        'partition_id': message['partition_id'],
        'total_rows_processed': total_rows_processed,
        'total_time': total_time,
        'average_speed': total_rows_processed / total_time if total_time > 0 else 0,
        'sequences_generated': sequence,
        'progress': processor.get_progress(),
        **tracker.get_stats()
    }

    print(f"\nPartition processing completed:")
    print(f"- Total rows processed: {total_rows_processed:,}")
    print(f"- Total time: {total_time:.2f}s")
    print(f"- Average speed: {stats['average_speed']:.1f} rows/second")
    print(f"- Overall progress: {stats['progress'].get('estimated_completion', 'Unknown')}")

    return stats


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

            # Fixed result handling - look for dict results instead of int
            errors = [r for r in results if isinstance(r, Exception)]
            successful_results = [r for r in results if isinstance(r, dict)]

            if successful_results:
                batch_rows = sum(r['total_rows_processed'] for r in successful_results)
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


async def send_sqs_message(message_body: Dict, queue_url: str, message_attributes: Dict) -> None:
    """Send message to SQS using async client"""
    async with boto3_session.client('sqs', config=config) as sqs:
        try:
            await sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body),
                MessageAttributes=message_attributes
            )
        except Exception as e:
            print(f"Error sending SQS message: {str(e)}")
            raise


def lambda_handler(event, context):
    """
    Non-async wrapper for the async Lambda handler implementation.
    """
    print(f"Incoming event: {event}")
    return asyncio.get_event_loop().run_until_complete(_async_handler(event, context))


async def _async_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Async implementation of the Lambda handler.
    """
    handler_start_time = time.time()
    print("\n=== Lambda Handler Started ===")
    print(f"Lambda function: {context.function_name}")
    print(f"Request ID: {context.aws_request_id}")
    print(f"Time remaining: {context.get_remaining_time_in_millis() / 1000:.2f}s")

    # Initialize metrics
    metrics = {
        'messages_received': len(event.get('Records', [])),
        'messages_processed': 0,
        'total_rows_processed': 0,
        'errors': [],
        'processing_time': 0
    }

    try:
        if not event.get('Records'):
            print("No messages to process")
            return create_response(200, metrics)

        # Verify database connection pool
        if db_pool is None:
            raise RuntimeError("Database connection pool not initialized")

        records = event.get('Records', [])
        messages = []
        total_rows_estimate = 0

        # Parse and validate messages
        for idx, record in enumerate(records, 1):
            try:
                message = json.loads(record['body'])
                validate_message(message)
                messages.append(message)
                row_count = message['end_offset'] - message['start_offset']
                total_rows_estimate += row_count
                print(f"Added message {idx} (estimated {row_count:,} rows)")
                print(f"Cumulative estimated rows: {total_rows_estimate:,}")
            except (json.JSONDecodeError, ValueError) as e:
                error_msg = f"Failed to parse message {idx}: {str(e)}"
                print(f"ERROR: {error_msg}")
                metrics['errors'].append(error_msg)
                continue

        if not messages:
            return create_response(200, metrics)

        # Initialize processor and start processing
        print(f"\nInitializing processor with schema from first message:")
        print(f"Transfer ID: {messages[0]['transfer_id']}")

        # Process each message
        try:
            results = await process_messages(messages)

            # Update metrics
            metrics.update({
                'messages_processed': len(messages),
                'processing_time': time.time() - handler_start_time,
                'status': 'SUCCESS'
            })

            return create_response(results.get('statusCode', 200), metrics)

        except Exception as e:
            error_message = f"Processing failed: {str(e)}"
            print(f"ERROR: {error_message}")
            metrics['errors'].append(error_message)
            return create_response(500, metrics)

    except Exception as e:
        error_message = str(e)
        print(f"CRITICAL ERROR: {error_message}")
        metrics['errors'].append(error_message)
        return create_response(
            500,
            {
                **metrics,
                'status': 'ERROR',
                'error': error_message,
                'processing_time': time.time() - handler_start_time
            }
        )


def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized API response"""
    return {
        'statusCode': status_code,
        'body': json.dumps(body),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


def validate_message(message: Dict[str, Any]) -> None:
    """Validate required message fields"""
    required_fields = ['transfer_id', 'partition_id', 'order', 'query',
                       'start_offset', 'end_offset', 'schema_info']

    missing_fields = [field for field in required_fields if field not in message]
    if missing_fields:
        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")


def estimate_processing_time(row_count: int) -> float:
    """Estimate processing time based on row count"""
    # Based on observed processing speed of ~1000 rows/second
    return (row_count / 1000) * 1.2  # Add 20% buffer


async def process_message_batch(messages: List[Dict], processor: ParallelBatchProcessor) -> List[Dict]:
    """Process a batch of messages in parallel with improved concurrency control"""
    max_concurrent = min(len(messages), 5)
    results = []

    for i in range(0, len(messages), max_concurrent):
        batch = messages[i:i + max_concurrent]
        print(f"\nProcessing batch {i // max_concurrent + 1}:")
        print(f"- Batch size: {len(batch)}")

        batch_start = time.time()
        tasks = [process_partition(message, processor) for message in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        results.extend(batch_results)

        batch_time = time.time() - batch_start
        successful_results = [r for r in batch_results if isinstance(r, dict)]

        if successful_results:
            total_rows = sum(r['total_rows_processed'] for r in successful_results)
            print(f"\nBatch {i // max_concurrent + 1} completed:")
            print(f"- Processed {total_rows:,} rows in {batch_time:.2f}s")
            print(f"- Speed: {total_rows / batch_time:.1f} rows/second")

        errors = [r for r in batch_results if isinstance(r, Exception)]
        if errors:
            print(f"\nERRORS in batch {i // max_concurrent + 1}:")
            for idx, error in enumerate(errors, 1):
                print(f"Error {idx}: {str(error)}")

    return results


def log_performance_metrics(metrics: Dict[str, Any], context: Any) -> None:
    """Log detailed performance metrics"""
    time_remaining = context.get_remaining_time_in_millis() / 1000
    print("\n=== Performance Metrics ===")
    print(f"Messages received: {metrics['messages_received']}")
    print(f"Messages processed: {metrics['messages_processed']}")
    print(f"Total rows processed: {metrics['total_rows_processed']:,}")
    print(f"Processing time: {metrics['processing_time']:.2f}s")
    print(f"Time remaining: {time_remaining:.2f}s")

    if metrics['errors']:
        print("\nErrors encountered:")
        for error in metrics['errors']:
            print(f"- {error}")

    if metrics['total_rows_processed'] > 0 and metrics['processing_time'] > 0:
        rows_per_second = metrics['total_rows_processed'] / metrics['processing_time']
        print(f"\nProcessing speed: {rows_per_second:.1f} rows/second")