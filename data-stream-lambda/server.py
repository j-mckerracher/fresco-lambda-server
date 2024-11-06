import json
import os
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import base64
import uuid
from typing import Iterator, Dict, Any, List, Tuple
import time
import threading
from queue import Queue, Empty
import concurrent.futures

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.7)  # Leave room for metadata and base64 encoding
DEFAULT_BATCH_SIZE = 100  # Reduced batch size for faster processing
DEFAULT_ROW_LIMIT = 1000000  # Default high row limit
MAX_PUBLISH_TIME_PER_BATCH = 0.5  # Maximum time allowed for publishing a single batch (in seconds)
MAX_PROCESSING_TIME = 40  # Maximum time for row processing (in seconds)
MAX_PUBLISHING_TIME = 15  # Maximum time for final publishing (in seconds)
MAX_CLEANUP_TIME = 3  # Maximum time for cleanup (in seconds)

# Initialize the PostgreSQL connection pool
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
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
        print("Waiting for publisher completion")
        try:
            remaining_time = timeout
            if start_time:
                elapsed = time.time() - start_time
                remaining_time = max(0.5, min(timeout, MAX_PUBLISHING_TIME, 57 - elapsed))
                print(f"Remaining time for completion: {remaining_time:.1f}s")

            wait_start = time.time()
            while not self.publish_queue.empty():
                if time.time() - wait_start > remaining_time:
                    print(f"Publisher wait timeout after {remaining_time:.1f}s")
                    break
                time.sleep(0.1)

            self.is_running = False
            self.publish_queue.put(None)

            if remaining_time > 0:
                self.publish_thread.join(timeout=remaining_time)
            else:
                self.publish_thread.join(timeout=0.5)

            if self.error:
                raise self.error

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
    print("Received event:", json.dumps(event))
    start_time = time.time()
    conn = None
    cursor = None

    try:
        arguments = event['arguments']
        query = arguments['query']
        transfer_id = arguments['transferId']
        row_limit = arguments.get('rowLimit', DEFAULT_ROW_LIMIT)
        batch_size = arguments.get('batchSize', DEFAULT_BATCH_SIZE)

        print(f"Starting query execution with transfer ID: {transfer_id}")
        print(f"Query: {query}")
        print(f"Row limit: {row_limit}, Batch size: {batch_size}")

        if not is_query_safe(query):
            raise Exception('Unsafe SQL query.')

        if db_pool is None:
            raise Exception('Database connection pool is not initialized.')

        conn = db_pool.getconn()

        # Set DB statement timeout
        cursor = conn.cursor()
        cursor.execute(f"SET statement_timeout = '{MAX_PROCESSING_TIME}s';")

        # Execute the query
        query_with_limit = add_limit_if_needed(query, row_limit)
        print(f"\nExecuting query with limit: {query_with_limit}")
        cursor.execute(query_with_limit)

        if cursor.description is None:
            raise Exception("Query execution failed - no column description available")

        # Create schema from the cursor description
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

        # Try to fetch first batch
        try:
            first_batch = cursor.fetchmany(batch_size)
            print(f"Fetched first batch size: {len(first_batch) if first_batch else 0}")
        except Exception as e:
            print(f"Error fetching data: {str(e)}")
            raise Exception(f"Data fetch failed: {str(e)}")

        if not first_batch:
            print("Query returned no data")
            return {
                'transferId': transfer_id,
                'metadata': {
                    'rowCount': 0,
                    'chunkCount': 0,
                    'schema': schema_info,
                    'isComplete': True,
                    'timeElapsed': time.time() - start_time
                }
            }

        publisher = None
        row_count = len(first_batch)
        total_rows = min(row_limit, cursor.rowcount if cursor.rowcount >= 0 else row_limit)
        is_final = False

        try:
            publisher = BatchPublisher(iot_client, iot_topic, transfer_id)
            last_progress_time = time.time()

            # Process and publish first batch
            batch_data = process_rows(first_batch, schema)
            is_final = len(first_batch) < batch_size
            publisher.publish(batch_data, is_final)

            # Process remaining batches
            while not is_final and row_count < row_limit:
                elapsed = time.time() - start_time
                if elapsed > MAX_PROCESSING_TIME:
                    print(f"Processing time limit reached ({elapsed:.1f}s). Processed {row_count}/{total_rows} rows")
                    is_final = True
                    break

                rows = cursor.fetchmany(batch_size)
                if not rows:
                    is_final = True
                    break

                current_batch_size = len(rows)
                row_count += current_batch_size

                batch_data = process_rows(rows, schema)
                is_final = is_final or (row_count >= row_limit) or (current_batch_size < batch_size)
                publisher.publish(batch_data, is_final)

                # Print progress every second
                current_time = time.time()
                if current_time - last_progress_time >= 1.0:
                    progress = (row_count / total_rows) * 100
                    elapsed = current_time - start_time
                    rows_per_second = row_count / elapsed if elapsed > 0 else 0
                    print(f"Progress: {progress:.1f}% ({row_count}/{total_rows}) - {rows_per_second:.0f} rows/sec")
                    last_progress_time = current_time

            # Wait for publisher with remaining time limit
            if publisher:
                elapsed = time.time() - start_time
                remaining_time = max(0.5, min(MAX_PUBLISHING_TIME, 57 - elapsed))
                print(f"Final publishing phase, remaining time: {remaining_time:.1f}s")
                publisher.wait_completion(timeout=remaining_time, start_time=start_time)

            print(f"Query completed. Processed {row_count}/{total_rows} rows")
            return {
                'transferId': transfer_id,
                'metadata': {
                    'rowCount': row_count,
                    'totalRows': total_rows,
                    'chunkCount': publisher.sequence_number if publisher else 0,
                    'schema': schema_info,
                    'isComplete': row_count >= total_rows,
                    'timeElapsed': time.time() - start_time
                }
            }

        finally:
            if publisher:
                try:
                    elapsed = time.time() - start_time
                    remaining_time = max(0.5, min(MAX_CLEANUP_TIME, 58 - elapsed))
                    publisher.wait_completion(timeout=remaining_time, start_time=start_time)
                except Exception as e:
                    print(f"Error during publisher cleanup: {str(e)}")

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        import traceback
        print("Full traceback:")
        print(traceback.format_exc())
        raise
    finally:
        cleanup_start = time.time()
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
        print(f"Total execution time: {total_time:.2f}s (Cleanup: {cleanup_time:.2f}s)")