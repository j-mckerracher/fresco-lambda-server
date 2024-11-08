import json
import os
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import base64
from typing import List, Tuple
import time
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.7)
DEFAULT_BATCH_SIZE = 1000  # Increased for better throughput
DEFAULT_ROW_LIMIT = 80000
MAX_TOTAL_TIME = 55  # Maximum total execution time
MAX_PROCESSING_TIME = 35  # Processing window
MAX_PUBLISHING_TIME = 15  # Publishing window
MAX_CLEANUP_TIME = 3
MAX_PUBLISHER_THREADS = 6  # Increased publisher threads

# Initialize the PostgreSQL connection pool with more connections
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=20,
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME']
    )
    print("Database connection pool created successfully.")
except Exception as e:
    print(f"Error creating database connection pool: {e}")
    db_pool = None

# Initialize AWS IoT client with retry configuration
try:
    iot_endpoint = os.environ['IOT_ENDPOINT']
    iot_topic = os.environ['IOT_TOPIC']
    region = os.environ.get('REGION', 'us-east-1')

    iot_client = boto3.client(
        'iot-data',
        region_name=region,
        endpoint_url=f"https://{iot_endpoint}"
    )
    print("AWS IoT Data client initialized successfully.")
except Exception as e:
    print(f"Error initializing AWS IoT Data client: {e}")
    iot_client = None


class ParallelBatchPublisher:
    def __init__(self, iot_client, topic, transfer_id, num_threads=MAX_PUBLISHER_THREADS):
        self.iot_client = iot_client
        self.topic = topic
        self.transfer_id = transfer_id
        self.max_chunk_size = MAX_DATA_SIZE
        self.sequence_number = 0
        self.sequence_lock = threading.Lock()
        self.publish_queue = Queue()
        self.error = None
        self.is_running = True
        self.executor = ThreadPoolExecutor(max_workers=num_threads)
        self.futures = []
        self.publish_count = 0
        self.publish_count_lock = threading.Lock()

        # Start publisher threads
        for _ in range(num_threads):
            future = self.executor.submit(self._publish_worker)
            self.futures.append(future)

        print(f"ParallelBatchPublisher initialized with {num_threads} threads")

    def get_next_sequence(self):
        with self.sequence_lock:
            self.sequence_number += 1
            return self.sequence_number

    def _publish_batch(self, batch_item: Tuple[bytes, bool]):
        if self.iot_client is None:
            raise Exception("IoT client not initialized")

        batch_data, is_final = batch_item
        sequence = self.get_next_sequence()
        chunks = chunk_arrow_data(batch_data, self.max_chunk_size)
        total_chunks = len(chunks)

        for chunk_number, chunk_data in enumerate(chunks, start=1):
            is_final_sequence = is_final and (chunk_number == total_chunks)

            message = {
                'type': 'arrow_data',
                'metadata': {
                    'transfer_id': self.transfer_id,
                    'sequence_number': sequence,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'chunk_size': len(chunk_data),
                    'format': 'arrow_stream_ipc',
                    'timestamp': int(time.time() * 1000),
                    'is_final_sequence': is_final_sequence
                },
                'data': base64.b64encode(chunk_data).decode('utf-8')
            }

            # Implement exponential backoff for publishing
            max_retries = 2
            retry_delay = 0.1
            last_exception = None

            for attempt in range(max_retries):
                try:
                    self.iot_client.publish(
                        topic=self.topic,
                        qos=1,
                        payload=json.dumps(message)
                    )
                    with self.publish_count_lock:
                        self.publish_count += 1
                    break
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (2 ** attempt))
                    else:
                        raise last_exception

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

    def publish(self, batch_data: bytes, is_final: bool = False):
        if self.error:
            raise self.error
        self.publish_queue.put((batch_data, is_final))

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


def process_batch(rows: list, schema: pa.Schema) -> bytes:
    """Optimized batch processing with better error handling"""
    try:
        arrays = []
        for i, field in enumerate(schema):
            array_data = [row[i] if row[i] is not None else None for row in rows]

            if field.name == 'value_gpu':
                arrays.append(pa.array(array_data, type=pa.float64()))
            elif pa.types.is_timestamp(field.type):
                arrays.append(pa.array(array_data, type=field.type))
            else:
                arrays.append(pa.array(array_data, type=field.type))

        record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)
        return sink.getvalue().to_pybytes()
    except Exception as e:
        print(f"Error processing batch: {str(e)}")
        raise


def create_record_batch(rows: list, schema: pa.Schema) -> pa.RecordBatch:
    """
    Convert a list of rows to a PyArrow RecordBatch with proper null handling.
    """
    arrays = []
    for i, field in enumerate(schema):
        array_data = [row[i] if row[i] is not None else None for row in rows]
        try:
            # Handle different types appropriately
            if field.name == 'value_gpu':
                arrays.append(pa.array([0.0 if x is None else float(x) for x in array_data],
                                       type=pa.float64()))
            elif pa.types.is_timestamp(field.type):
                arrays.append(pa.array(array_data, type=field.type))
            elif pa.types.is_string(field.type):
                arrays.append(pa.array(['' if x is None else str(x) for x in array_data]))
            elif pa.types.is_integer(field.type):
                arrays.append(pa.array([0 if x is None else x for x in array_data],
                                       type=field.type))
            elif pa.types.is_floating(field.type):
                arrays.append(pa.array([0.0 if x is None else float(x) for x in array_data],
                                       type=field.type))
            else:
                arrays.append(pa.array(array_data, type=field.type))
        except Exception as e:
            print(f"Error creating array for field {field.name} of type {field.type}: {str(e)}")
            print(f"Sample data: {array_data[:5]}")
            raise

    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def serialize_arrow_batch(record_batch: pa.RecordBatch) -> bytes:
    """
    Serialize a single record batch to bytes using Arrow IPC format.
    """
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, record_batch.schema)
    writer.write_batch(record_batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def chunk_arrow_data(data: bytes, max_chunk_size: int) -> List[bytes]:
    """
    Split data into chunks that fit within the max_chunk_size.
    """
    chunks = []
    data_size = len(data)
    start = 0

    while start < data_size:
        end = min(start + max_chunk_size, data_size)
        chunk = data[start:end]
        chunks.append(chunk)
        start = end

    return chunks


class BatchPublisher:
    def __init__(self, iot_client, topic, transfer_id, max_chunk_size=MAX_DATA_SIZE):
        self.iot_client = iot_client
        self.topic = topic
        self.transfer_id = transfer_id
        self.max_chunk_size = max_chunk_size
        self.sequence_number = 0
        self.publish_queue = Queue()
        self.publish_thread = threading.Thread(target=self._publish_worker, daemon=True)
        self.is_running = True
        self.error = None
        self.last_publish_time = time.time()
        self.publish_thread.start()
        print(f"BatchPublisher initialized for transfer {transfer_id}")

    def _publish_batch(self, batch_item: Tuple[bytes, bool]):
        batch_data, is_final = batch_item
        self.sequence_number += 1
        chunks = chunk_arrow_data(batch_data, self.max_chunk_size)
        total_chunks = len(chunks)

        batch_start_time = time.time()
        print(f"Publishing batch {self.sequence_number} with {total_chunks} chunks (final: {is_final})")

        for chunk_number, chunk_data in enumerate(chunks, start=1):
            if time.time() - batch_start_time > MAX_PUBLISH_TIME_PER_BATCH:
                raise Exception(f"Batch publish timeout exceeded ({MAX_PUBLISH_TIME_PER_BATCH}s)")

            is_final_sequence = is_final and (chunk_number == total_chunks)

            message = {
                'type': 'arrow_data',
                'metadata': {
                    'transfer_id': self.transfer_id,
                    'sequence_number': self.sequence_number,
                    'chunk_number': chunk_number,
                    'total_chunks': total_chunks,
                    'chunk_size': len(chunk_data),
                    'format': 'arrow_stream_ipc',
                    'timestamp': int(time.time() * 1000),
                    'is_final_sequence': is_final_sequence
                },
                'data': base64.b64encode(chunk_data).decode('utf-8')
            }

            try:
                response = self.iot_client.publish(
                    topic=self.topic,
                    qos=1,
                    payload=json.dumps(message)
                )
                print(f"Published sequence {self.sequence_number}, chunk {chunk_number}/{total_chunks} "
                      f"(is_final_sequence: {is_final_sequence})")
            except Exception as e:
                print(f"Error publishing chunk {chunk_number}: {str(e)}")
                raise

    def _publish_worker(self):
        print("Publisher worker thread started")
        while self.is_running:
            try:
                try:
                    batch_item = self.publish_queue.get(timeout=0.5)  # Reduced timeout
                except Empty:
                    continue

                if batch_item is None:
                    print("Received shutdown signal")
                    break

                try:
                    self._publish_batch(batch_item)
                    self.last_publish_time = time.time()
                    self.publish_queue.task_done()
                except Exception as e:
                    print(f"Error publishing batch: {str(e)}")
                    self.error = e
                    self.is_running = False
                    break

            except Exception as e:
                print(f"Error in publish worker: {str(e)}")
                self.error = e
                self.is_running = False
                break

        print("Publisher worker thread ending")

    def publish(self, batch_data: bytes, is_final: bool = False):
        if self.error:
            raise self.error
        self.publish_queue.put((batch_data, is_final))

    def wait_completion(self, timeout=None, start_time=None):
        """
        Wait for publisher completion with absolute timeout based on Lambda start time.
        """
        print("Checking for remaining messages to publish...")
        try:
            remaining_time = timeout
            if start_time:
                elapsed = time.time() - start_time
                remaining_time = max(0.5, min(timeout, MAX_PUBLISHING_TIME, 57 - elapsed))
                print(f"Will wait up to {remaining_time:.1f}s for remaining messages")

            messages_remaining = self.publish_queue.qsize()
            if messages_remaining > 0:
                print(f"Found {messages_remaining} messages still queued")

            wait_start = time.time()
            while not self.publish_queue.empty():
                if time.time() - wait_start > remaining_time:
                    messages_left = self.publish_queue.qsize()
                    print(
                        f"Publishing window closed after {remaining_time:.1f}s with {messages_left} messages remaining")
                    break
                time.sleep(0.1)

            self.is_running = False
            self.publish_queue.put(None)  # Signal to stop the worker thread

            if remaining_time > 0:
                self.publish_thread.join(timeout=remaining_time)
            else:
                self.publish_thread.join(timeout=0.5)

            if self.error:
                raise self.error

            final_messages = self.publish_queue.qsize()
            if final_messages > 0:
                print(f"Publishing phase completed with {final_messages} unpublished messages")
            else:
                print("All messages published successfully")

        except Exception as e:
            print(f"Error during publisher shutdown: {str(e)}")
            raise


def process_rows(rows: list, schema: pa.Schema) -> bytes:
    """
    Process a group of rows into an Arrow Stream IPC format with error handling.
    """
    try:
        record_batch = create_record_batch(rows, schema)
        return serialize_arrow_batch(record_batch)
    except Exception as e:
        print(f"Error processing rows: {str(e)}")
        if rows:
            print("Sample row data:", rows[0])
            print("Schema:")
            for field in schema:
                print(f"{field.name}: {field.type}")
        raise


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


def add_limit_if_needed(query, row_limit=DEFAULT_ROW_LIMIT):
    """Appends a LIMIT clause if needed"""
    print("Adding LIMIT clause if needed.")
    if 'LIMIT' not in query.upper():
        print(f"Adding: LIMIT {row_limit}")
        return f"{query} LIMIT {row_limit}"
    print("LIMIT clause already present in query.")
    return query


def lambda_handler(event, context):
    """
    Lambda handler with improved timeout handling and status reporting
    """
    print("Received event:", json.dumps(event))
    start_time = time.time()
    conn = None
    cursor = None
    transfer_id = None
    row_count = 0
    total_rows = 0
    publisher = None
    schema_info = None
    processing_status = "ERROR"  # Default status

    def get_remaining_time():
        """Calculate remaining execution time"""
        elapsed = time.time() - start_time
        return max(0, min(
            context.get_remaining_time_in_millis() / 1000 - 1,
            MAX_TOTAL_TIME - elapsed
        ))

    def create_response(status, row_count=0, chunk_count=0, error=None):
        """Create a standardized response object"""
        time_elapsed = time.time() - start_time
        return {
            'transferId': transfer_id,
            'metadata': {
                'rowCount': row_count,
                'chunkCount': chunk_count,
                'schema': schema_info,
                'isComplete': status == "COMPLETED",
                'timeElapsed': round(time_elapsed, 2),
                'totalRows': total_rows,
                'publishedMessageCount': publisher.publish_count if publisher else 0,
                'processingStatus': status,
                'error': str(error) if error else None
            }
        }

    try:
        # Extract arguments
        arguments = event['arguments']
        transfer_id = arguments['transferId']
        query = arguments['query']
        row_limit = arguments.get('rowLimit', DEFAULT_ROW_LIMIT)
        batch_size = arguments.get('batchSize', DEFAULT_BATCH_SIZE)

        print(f"Starting query execution with transfer ID: {transfer_id}")
        print(f"Query: {query}")
        print(f"Row limit: {row_limit}, Batch size: {batch_size}")

        # Validate query
        if not is_query_safe(query):
            raise Exception('Unsafe SQL query.')

        if db_pool is None:
            raise Exception('Database connection pool is not initialized.')

        # Get database connection
        conn = db_pool.getconn()
        conn.autocommit = True

        # Configure statement timeout
        with conn.cursor() as setup_cursor:
            setup_cursor.execute(f"SET statement_timeout = '{MAX_PROCESSING_TIME}s';")

        cursor = conn.cursor()

        # Execute query with limit
        query_with_limit = add_limit_if_needed(query, row_limit)
        print(f"\nExecuting query with limit: {query_with_limit}")
        cursor.execute(query_with_limit)

        if cursor.description is None:
            return create_response("COMPLETED", 0, 0)

        # Create schema
        fields = []
        for desc in cursor.description:
            name = desc[0]
            if desc[1] == 1184:  # TIMESTAMPTZ
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
        print(f"Created schema: {schema_info}")

        # Initial fetch
        first_batch = cursor.fetchmany(batch_size)
        if not first_batch:
            return create_response("COMPLETED", 0, 0)

        publisher = ParallelBatchPublisher(iot_client, iot_topic, transfer_id)
        row_count = len(first_batch)
        total_rows = min(row_limit, cursor.rowcount if cursor.rowcount >= 0 else row_limit)
        batch_buffer = list(first_batch)
        buffer_size = len(first_batch)
        processing_complete = False
        processing_status = "PARTIAL"

        last_progress_time = time.time()

        try:
            # Process first batch if buffer is full
            if buffer_size >= batch_size:
                batch_data = process_batch(batch_buffer, schema)
                publisher.publish(batch_data, False)
                batch_buffer = []
                buffer_size = 0

            # Main processing loop
            while not processing_complete and row_count < row_limit:
                # Check time limits
                remaining_time = get_remaining_time()
                if remaining_time < MAX_PUBLISHING_TIME + MAX_CLEANUP_TIME:
                    print(f"Time limit approaching. Processed {row_count}/{total_rows} rows")
                    processing_complete = True
                    processing_status = "PARTIAL"
                    break

                # Check for publisher errors
                if publisher and publisher.error:
                    raise publisher.error

                # Fetch next batch
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    processing_complete = True
                    processing_status = "COMPLETED"
                    break

                current_batch_size = len(rows)
                row_count += current_batch_size
                batch_buffer.extend(rows)
                buffer_size += current_batch_size

                # Process buffer
                if buffer_size >= batch_size or current_batch_size < batch_size:
                    is_final = processing_complete or (row_count >= row_limit) or (current_batch_size < batch_size)
                    batch_data = process_batch(batch_buffer, schema)
                    publisher.publish(batch_data, is_final)
                    batch_buffer = []
                    buffer_size = 0

                # Progress logging
                current_time = time.time()
                if current_time - last_progress_time >= 1.0:
                    progress = (row_count / total_rows) * 100 if total_rows > 0 else 0
                    elapsed = current_time - start_time
                    rows_per_second = row_count / elapsed if elapsed > 0 else 0
                    queue_size = publisher.publish_queue.qsize() if publisher else 0
                    print(
                        f"Progress: {progress:.1f}% ({row_count}/{total_rows}) - {rows_per_second:.0f} rows/sec - Queue: {queue_size}")
                    last_progress_time = current_time

            # Process remaining buffer
            if batch_buffer:
                batch_data = process_batch(batch_buffer, schema)
                publisher.publish(batch_data, True)

            # Final publishing phase
            remaining_time = get_remaining_time()
            publishing_time = min(MAX_PUBLISHING_TIME, remaining_time - MAX_CLEANUP_TIME)
            print(f"Final publishing phase, allowing up to {publishing_time:.1f}s for remaining messages")
            published_count = publisher.wait_completion(timeout=publishing_time) if publisher else 0

            if row_count >= total_rows and processing_complete:
                processing_status = "COMPLETED"

            return create_response(
                processing_status,
                row_count,
                publisher.sequence_number if publisher else 0
            )

        finally:
            if publisher:
                try:
                    cleanup_time = min(MAX_CLEANUP_TIME, get_remaining_time())
                    print(f"Cleanup phase, allowing up to {cleanup_time:.1f}s for final messages")
                    publisher.wait_completion(timeout=cleanup_time)
                except Exception as e:
                    print(f"Error during publisher cleanup: {str(e)}")

    except Exception as e:
        print(f"Error during execution: {str(e)}")
        return create_response(
            "ERROR",
            row_count if 'row_count' in locals() else 0,
            publisher.sequence_number if publisher and 'publisher' in locals() else 0,
            error=e
        )

    finally:
        cleanup_start = time.time()

        # Clean up resources
        if cursor:
            try:
                cursor.close()
                print("Cursor closed")
            except Exception as e:
                print(f"Error closing cursor: {e}")

        if conn:
            try:
                db_pool.putconn(conn)
                print("Connection returned to pool")
            except Exception as e:
                print(f"Error returning connection to pool: {e}")

        total_time = time.time() - start_time
        cleanup_time = time.time() - cleanup_start
        print(f"Execution completed in {total_time:.2f}s (Cleanup: {cleanup_time:.2f}s)")