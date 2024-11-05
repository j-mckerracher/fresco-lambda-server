import json
import os
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import base64
import uuid
from typing import Iterator, Dict, Any, List
import time

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.7)  # Leave room for metadata and base64 encoding
DEFAULT_BATCH_SIZE = 500  # Default batch size
DEFAULT_ROW_LIMIT = 1000000  # Set a high default row limit for large datasets

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


def stream_query_results(cursor, batch_size) -> Iterator[List[tuple]]:
    """
    Generator function to stream results from the database in batches.
    """
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def create_record_batch(rows: list, schema: pa.Schema) -> pa.RecordBatch:
    """
    Convert a list of rows to a PyArrow RecordBatch.
    """
    arrays = []
    for i, field in enumerate(schema):
        array_data = [row[i] for row in rows]
        arrays.append(pa.array(array_data, type=field.type))
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


def publish_batch(iot_client: Any, topic: str, batch_data: bytes,
                  transfer_id: str, sequence_number: int, max_chunk_size: int = MAX_DATA_SIZE) -> None:
    """
    Publish Arrow Stream IPC data, splitting into chunks if necessary.
    """
    # Split the batch_data into chunks if it exceeds max_chunk_size
    chunks = chunk_arrow_data(batch_data, max_chunk_size)
    total_chunks = len(chunks)

    print(f"Publishing {total_chunks} chunks for sequence {sequence_number} to topic {topic}.")

    for chunk_number, chunk_data in enumerate(chunks, start=1):
        message = {
            'type': 'arrow_data',
            'metadata': {
                'transfer_id': transfer_id,
                'sequence_number': sequence_number,
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'chunk_size': len(chunk_data),
                'format': 'arrow_stream_ipc',
                'timestamp': int(time.time() * 1000),
                'is_final_sequence': (sequence_number == 1 and chunk_number == total_chunks)  # Added flag
            },
            'data': base64.b64encode(chunk_data).decode('utf-8')
        }

        # Verify message size and print size info
        message_bytes = json.dumps(message).encode('utf-8')
        message_size = len(message_bytes)
        print(f"Message size for chunk {chunk_number}: {message_size} bytes")

        if message_size > MAX_MQTT_PAYLOAD_SIZE:
            raise ValueError(f"Message size ({message_size} bytes) exceeds MQTT limit")

        try:
            # Add a small delay between publishing chunks
            if chunk_number > 1:
                time.sleep(0.1)  # 100ms delay between chunks

            # Publish the chunk
            response = iot_client.publish(
                topic=topic,
                qos=1,
                payload=json.dumps(message)
            )
            print(f"Published sequence {sequence_number}, chunk {chunk_number}/{total_chunks}")
            print(f"IoT publish response: {json.dumps(response, default=str)}")
        except Exception as e:
            print(f"Error publishing chunk {chunk_number}: {str(e)}")
            raise

    print(f"Completed publishing sequence {sequence_number}.")


def process_rows(rows: list, schema: pa.Schema) -> bytes:
    """
    Process a group of rows into an Arrow Stream IPC format.
    """
    record_batch = create_record_batch(rows, schema)
    return serialize_arrow_batch(record_batch)


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    try:
        # Extract parameters from the event
        query = event['arguments']['query']
        transfer_id = event['arguments'].get('transferId', str(uuid.uuid4()))
        row_limit = event['arguments'].get('rowLimit', DEFAULT_ROW_LIMIT)
        batch_size = event['arguments'].get('batchSize', DEFAULT_BATCH_SIZE)

        print(f"Starting query execution with transfer ID: {transfer_id}")
        print(f"Query: {query}")
        print(f"Row limit: {row_limit}, Batch size: {batch_size}")

        if not is_query_safe(query):
            raise Exception('Unsafe SQL query.')

        query_with_limit = add_limit_if_needed(query, row_limit)

        if db_pool is None:
            raise Exception('Database connection pool is not initialized.')

        # Add a small delay before starting query execution
        time.sleep(1)
        print("Starting query execution after delay...")

        conn = db_pool.getconn()
        cursor_name = f"cursor_{uuid.uuid4().hex}"
        cursor = conn.cursor(name=cursor_name)
        cursor.itersize = batch_size

        print(f"Executing query with limit: {query_with_limit}")
        cursor.execute(query_with_limit)

        first_batch = cursor.fetchmany(batch_size)
        if not first_batch:
            print('No data returned from query.')
            return {
                'transferId': transfer_id,
                'metadata': {
                    'rowCount': 0,
                    'chunkCount': 0,
                    'schema': {}
                }
            }

        column_names = [desc[0] for desc in cursor.description]
        print(f"Column names: {column_names}")

        table = pa.Table.from_arrays(
            [pa.array([row[i] for row in first_batch]) for i in range(len(column_names))],
            names=column_names
        )
        schema = table.schema
        schema_info = {field.name: str(field.type) for field in schema}
        print(f"Determined schema: {json.dumps(schema_info)}")

        sequence_number = 1
        row_count = 0

        print(f"Starting to process and publish rows for transfer ID: {transfer_id}")

        # Process and publish the first batch
        row_count += len(first_batch)
        print(f"Processing batch {sequence_number} with {len(first_batch)} rows.")
        batch_data = process_rows(first_batch, schema)
        publish_batch(iot_client, iot_topic, batch_data, transfer_id, sequence_number)

        # If there's only one batch, we're done
        if len(first_batch) < batch_size:
            print("Query complete - only one batch needed.")
            return {
                'transferId': transfer_id,
                'metadata': {
                    'rowCount': row_count,
                    'chunkCount': 1,
                    'schema': schema_info
                }
            }

        # Process and publish remaining batches
        sequence_number += 1
        for batch_rows in stream_query_results(cursor, batch_size):
            row_count += len(batch_rows)
            print(f"Processing batch {sequence_number} with {len(batch_rows)} rows.")
            batch_data = process_rows(batch_rows, schema)
            publish_batch(iot_client, iot_topic, batch_data, transfer_id, sequence_number)
            sequence_number += 1

            # Break if we've hit the row limit
            if row_count >= row_limit:
                print(f"Row limit {row_limit} reached. Stopping query execution.")
                break

        print(f"Completed processing and publishing all rows. Total rows: {row_count}")

        return {
            'transferId': transfer_id,
            'metadata': {
                'rowCount': row_count,
                'chunkCount': sequence_number - 1,
                'schema': schema_info
            }
        }

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
            print("Database cursor closed.")
        if 'conn' in locals():
            db_pool.putconn(conn)
            print("Database connection returned to pool.")


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