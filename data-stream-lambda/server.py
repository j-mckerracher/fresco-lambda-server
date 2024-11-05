import json
import math
import os
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
from psycopg2 import extras
import boto3
import base64
import uuid
from typing import Iterator, Dict, Any, List
import time
import io

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.7)  # Leave room for metadata and base64 encoding
BATCH_SIZE = 500  # Reduced batch size for better chunking
ROW_LIMIT = 100

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


def stream_query_results(cursor) -> Iterator[Dict[str, Any]]:
    """
    Generator function to stream results from the database in batches.
    """
    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            yield row


def create_record_batch(rows: list, schema: pa.Schema) -> pa.RecordBatch:
    """
    Convert a list of rows to a PyArrow RecordBatch.
    """
    arrays = []
    for field in schema:
        array_data = [row[field.name] for row in rows]
        arrays.append(pa.array(array_data))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def serialize_arrow_batch(record_batch: pa.RecordBatch) -> bytes:
    """
    Serialize a single record batch to bytes using Arrow IPC format.
    """
    sink = pa.BufferOutputStream()
    writer = ipc.new_file(sink, record_batch.schema)
    writer.write_batch(record_batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def chunk_arrow_data(data: bytes, max_chunk_size: int) -> List[bytes]:
    """
    Split Arrow data into chunks while preserving Arrow IPC format.
    """
    if len(data) <= max_chunk_size:
        return [data]

    # Read the original Arrow file
    reader = pa.ipc.open_file(pa.py_buffer(data))
    schema = reader.schema
    batches = [batch for batch in reader]

    # Create new chunks with complete Arrow files
    chunks = []
    current_batch_group = []
    current_size = 0

    for batch in batches:
        # Serialize single batch to estimate size
        batch_data = serialize_arrow_batch(batch)
        batch_size = len(batch_data)

        if current_size + batch_size > max_chunk_size and current_batch_group:
            # Create a new Arrow file with current batch group
            sink = pa.BufferOutputStream()
            writer = ipc.new_file(sink, schema)
            for b in current_batch_group:
                writer.write_batch(b)
            writer.close()
            chunks.append(sink.getvalue().to_pybytes())

            current_batch_group = [batch]
            current_size = batch_size
        else:
            current_batch_group.append(batch)
            current_size += batch_size

    # Handle remaining batches
    if current_batch_group:
        sink = pa.BufferOutputStream()
        writer = ipc.new_file(sink, schema)
        for batch in current_batch_group:
            writer.write_batch(batch)
        writer.close()
        chunks.append(sink.getvalue().to_pybytes())

    return chunks


def publish_batch(iot_client: Any, topic: str, batch_data: bytes,
                  transfer_id: str, sequence_number: int, total_chunks: int) -> None:
    """
    Publish Arrow data in chunks while preserving Arrow IPC format.
    """
    data_chunks = chunk_arrow_data(batch_data, MAX_DATA_SIZE)

    for idx, chunk in enumerate(data_chunks, start=1):
        message = {
            'type': 'arrow_data',
            'metadata': {
                'transfer_id': transfer_id,
                'sequence_number': sequence_number,
                'chunk_number': idx,
                'total_chunks': len(data_chunks),
                'chunk_size': len(chunk),
                'format': 'arrow_ipc',
                'timestamp': int(time.time() * 1000)
            },
            'data': base64.b64encode(chunk).decode('utf-8')
        }

        # Verify final message size
        message_bytes = json.dumps(message).encode('utf-8')
        if len(message_bytes) > MAX_MQTT_PAYLOAD_SIZE:
            raise ValueError(f"Message size ({len(message_bytes)} bytes) exceeds MQTT limit")

        iot_client.publish(
            topic=topic,
            qos=1,
            payload=json.dumps(message)
        )
        print(f"Published sequence {sequence_number}, chunk {idx}/{len(data_chunks)}")


def process_rows(rows: list, schema: pa.Schema, transfer_id: str, sequence_number: int) -> bytes:
    """
    Process a group of rows into an Arrow file format.
    """
    record_batch = create_record_batch(rows, schema)
    sink = pa.BufferOutputStream()
    writer = ipc.new_file(sink, schema)
    writer.write_batch(record_batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    try:
        query = event['arguments']['query']
        if not is_query_safe(query):
            raise Exception('Unsafe SQL query.')

        query_with_limit = add_limit_if_needed(query)

        if db_pool is None:
            raise Exception('Database connection pool is not initialized.')

        conn = db_pool.getconn()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        cursor.itersize = BATCH_SIZE  # Set server-side cursor size

        # Execute query with server-side cursor
        cursor.execute(query_with_limit)

        # Get the first batch to determine schema
        first_batch = cursor.fetchmany(BATCH_SIZE)
        if not first_batch:
            raise Exception('No data returned from query.')

        # Create PyArrow schema from first batch
        schema = pa.Schema.from_pandas(pa.Table.from_pylist(first_batch).to_pandas())
        schema_info = {field.name: str(field.type) for field in schema}

        transfer_id = str(uuid.uuid4())
        sequence_number = 1
        row_count = 0
        buffer_rows = []

        # Process rows in smaller batches
        for row in stream_query_results(cursor):
            buffer_rows.append(row)
            row_count += 1

            if len(buffer_rows) >= BATCH_SIZE:
                batch_data = process_rows(buffer_rows, schema, transfer_id, sequence_number)
                publish_batch(iot_client, iot_topic, batch_data, transfer_id,
                              sequence_number, -1)
                sequence_number += 1
                buffer_rows = []

        # Process remaining rows
        if buffer_rows:
            batch_data = process_rows(buffer_rows, schema, transfer_id, sequence_number)
            publish_batch(iot_client, iot_topic, batch_data, transfer_id,
                          sequence_number, sequence_number)

        return {
            'transferId': transfer_id,
            'metadata': {
                'rowCount': row_count,
                'chunkCount': sequence_number,
                'schema': {field.name: str(field.type) for field in schema}
            }
        }

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            db_pool.putconn(conn)


# Helper functions remain unchanged
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

    return True


def add_limit_if_needed(query, row_limit=ROW_LIMIT):
    """Appends a LIMIT clause if needed"""
    print("Adding LIMIT clause if needed.")
    if 'LIMIT' not in query.upper():
        return f"{query} LIMIT {row_limit}"
    return query
