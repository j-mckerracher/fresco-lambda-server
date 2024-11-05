import json
import os
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
from psycopg2 import extras
import boto3
import base64
import uuid
from typing import Iterator, Dict, Any
import time
import math

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.7)  # Leave room for metadata and base64 encoding
BATCH_SIZE = 1000  # Number of rows to process at once
ROW_LIMIT = 1000

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


def chunk_data(data: bytes, max_chunk_size: int) -> list:
    """
    Split data into chunks that will fit within MQTT payload limits after base64 encoding
    """
    # Calculate base64 encoded size
    encoded_size = math.ceil(len(data) * 4 / 3)
    num_chunks = math.ceil(encoded_size / max_chunk_size)
    chunk_size = math.ceil(len(data) / num_chunks)

    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def publish_batch(iot_client: Any, topic: str, batch_data: bytes,
                  transfer_id: str, sequence_number: int, total_chunks: int) -> None:
    """
    Publish a single batch of data to IoT topic with size checking and chunking.
    """
    # Split data into appropriate chunks if needed
    data_chunks = chunk_data(batch_data, MAX_DATA_SIZE)
    total_chunks = len(data_chunks)

    for idx, chunk in enumerate(data_chunks, start=1):
        message = {
            'type': 'arrow_data',
            'metadata': {
                'transfer_id': transfer_id,
                'sequence_number': sequence_number,
                'chunk_number': idx,
                'total_chunks': total_chunks,
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
        print(f"Published sequence {sequence_number}, chunk {idx}/{total_chunks}")


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
        row_count = len(first_batch)

        # Process and publish first batch
        record_batch = create_record_batch(first_batch, schema)
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, schema) as writer:
            writer.write_batch(record_batch)
        batch_data = sink.getvalue().to_pybytes()

        publish_batch(iot_client, iot_topic, batch_data, transfer_id,
                      sequence_number, -1)  # -1 for unknown total chunks initially

        # Stream remaining results
        buffer_rows = []
        for row in stream_query_results(cursor):
            buffer_rows.append(row)
            row_count += 1

            if len(buffer_rows) >= BATCH_SIZE:
                sequence_number += 1
                record_batch = create_record_batch(buffer_rows, schema)
                sink = pa.BufferOutputStream()
                with ipc.new_stream(sink, schema) as writer:
                    writer.write_batch(record_batch)
                batch_data = sink.getvalue().to_pybytes()

                publish_batch(iot_client, iot_topic, batch_data, transfer_id,
                              sequence_number, -1)
                buffer_rows = []

        # Send any remaining rows
        if buffer_rows:
            sequence_number += 1
            record_batch = create_record_batch(buffer_rows, schema)
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, schema) as writer:
                writer.write_batch(record_batch)
            batch_data = sink.getvalue().to_pybytes()

            publish_batch(iot_client, iot_topic, batch_data, transfer_id,
                          sequence_number, sequence_number)

        return {
            'transferId': transfer_id,
            'metadata': {
                'rowCount': row_count,
                'chunkCount': sequence_number,
                'schema': schema_info
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