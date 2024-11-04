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
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
CHUNK_SIZE = int((MAX_MQTT_PAYLOAD_SIZE - 200) * 0.75)  # AWS IoT MQTT payload limit
FETCH_SIZE = 1000  # Number of rows to fetch per batch
ROW_LIMIT = 1000
MAX_WORKERS = 4  # Maximum number of worker threads

print("Starting application...")

# Initialize the PostgreSQL connection pool with optimized settings
try:
    print("Initializing PostgreSQL connection pool...")
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=2,  # Increased minimum connections
        maxconn=10,  # Reduced max connections to prevent DB overload
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME'],
        # Add connection optimization parameters
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )
    print("Database connection pool created successfully.")
    print(f"Connection pool stats - Min connections: {db_pool.minconn}, Max connections: {db_pool.maxconn}")
except Exception as e:
    print(f"Error creating database connection pool: {e}")
    db_pool = None

# Initialize AWS IoT Data client using boto3
try:
    print("Initializing AWS IoT Data client...")
    iot_endpoint = os.environ['IOT_ENDPOINT']
    iot_topic = os.environ['IOT_TOPIC']
    region = os.environ.get('REGION', 'us-east-1')
    print(f"AWS IoT Endpoint: {iot_endpoint}, Topic: {iot_topic}, Region: {region}")
    iot_client = boto3.client('iot-data', region_name=region, endpoint_url=f"https://{iot_endpoint}")
    print("AWS IoT Data client initialized successfully.")
except Exception as e:
    print(f"Error initializing AWS IoT Data client: {e}")
    iot_client = None


def is_query_safe(query: str) -> bool:
    """Validates the SQL query for safety."""
    print(f"Validating SQL query: {query}")
    disallowed_keywords = {
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE',
        'TRUNCATE', 'EXECUTE', 'GRANT', 'REVOKE', 'REPLACE'
    }

    parsed = sqlparse.parse(query)
    if not parsed:
        print("Query parsing failed: No statements found.")
        return False

    tokens = {token.value.upper() for token in parsed[0].flatten()
              if not token.is_whitespace}
    print(f"Parsed tokens: {tokens}")

    is_safe = not (any(keyword in tokens for keyword in disallowed_keywords)
                   or ';' in query)
    print(f"Query safety check: {'Safe' if is_safe else 'Unsafe'}")
    return is_safe


def add_limit_if_needed(query: str, row_limit: int = ROW_LIMIT) -> str:
    """Appends a LIMIT clause if not present."""
    print(f"Adding LIMIT to query if necessary. Current query: {query}")
    if 'LIMIT' not in query.upper():
        query_with_limit = f"{query} LIMIT {row_limit}"
        print(f"LIMIT added to query: {query_with_limit}")
        return query_with_limit
    else:
        print("LIMIT clause already present in query.")
        return query


def create_arrow_batch(rows: list) -> bytes:
    """Convert a batch of rows to Arrow IPC format."""
    print(f"Converting {len(rows)} rows to Arrow IPC format.")
    if not rows:
        print("No rows to convert. Returning empty bytes.")
        return b''

    try:
        table = pa.Table.from_pylist(rows)
        print(f"Arrow Table created with schema: {table.schema}")
        sink = pa.BufferOutputStream()
        with ipc.RecordBatchStreamWriter(sink, table.schema) as writer:
            writer.write_table(table)
        arrow_bytes = sink.getvalue().to_pybytes()
        print(f"Arrow IPC data size: {len(arrow_bytes)} bytes")
        return arrow_bytes
    except Exception as e:
        print(f"Error converting rows to Arrow IPC format: {e}")
        return b''


def publish_chunk(iot_client: Any, topic: str, chunk: bytes,
                  transfer_id: str, sequence_number: int,
                  total_chunks: int) -> None:
    """Publish a single chunk to IoT topic."""
    print(f"Publishing chunk {sequence_number}/{total_chunks} to topic '{topic}'.")
    try:
        message = {
            'type': 'arrow_data',
            'metadata': {
                'transfer_id': transfer_id,
                'sequence_number': sequence_number,
                'total_chunks': total_chunks,
                'chunk_size': len(chunk),
                'format': 'arrow_ipc'
            },
            'data': base64.b64encode(chunk).decode('utf-8')
        }

        payload = json.dumps(message)
        print(f"Publishing payload size: {len(payload)} bytes")
        response = iot_client.publish(
            topic=topic,
            qos=1,
            payload=payload
        )
        print(f"Published chunk {sequence_number}/{total_chunks} successfully. Response: {response}")
    except Exception as e:
        print(f"Error publishing chunk {sequence_number}/{total_chunks}: {e}")


def process_and_publish_batch(
        rows: list,
        transfer_id: str,
        sequence_number: int,
        total_chunks: int,
        executor: ThreadPoolExecutor
) -> None:
    """Process a batch of rows and publish them."""
    print(f"Processing batch #{sequence_number} with {len(rows)} rows.")
    arrow_data = create_arrow_batch(rows)
    print(f"Arrow data length: {len(arrow_data)} bytes")

    # Split arrow data into chunks if needed
    for i in range(0, len(arrow_data), CHUNK_SIZE):
        chunk = arrow_data[i:i + CHUNK_SIZE]
        current_chunk_number = (i // CHUNK_SIZE) + 1
        print(f"Submitting chunk {current_chunk_number} for publishing.")
        executor.submit(
            publish_chunk,
            iot_client,
            iot_topic,
            chunk,
            transfer_id,
            sequence_number,
            total_chunks  # Pass the actual total_chunks
        )
    print(f"Batch #{sequence_number} submitted for publishing.")



def stream_query_results(cursor: Any, transfer_id: str) -> Dict[str, Any]:
    print("Starting to stream query results.")
    batch = []
    total_rows = 0
    sequence_number = 0
    total_chunks = 0  # Initialize total_chunks

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            print(f"Fetching next {FETCH_SIZE} rows from cursor.")
            rows = cursor.fetchmany(FETCH_SIZE)
            if not rows:
                print("No more rows fetched from cursor.")
                # Process final batch if any
                if batch:
                    sequence_number += 1
                    total_chunks += 1
                    print(f"Processing final batch #{sequence_number} with {len(batch)} rows.")
                    process_and_publish_batch(
                        batch, transfer_id, sequence_number, total_chunks, executor
                    )
                break

            batch.extend(rows)
            total_rows += len(rows)
            print(f"Fetched {len(rows)} rows. Total rows so far: {total_rows}")

            # Process batch when it reaches optimal size
            if len(batch) >= FETCH_SIZE:
                sequence_number += 1
                total_chunks += 1
                print(f"Processing batch #{sequence_number} with {len(batch)} rows.")
                process_and_publish_batch(
                    batch, transfer_id, sequence_number, total_chunks, executor
                )
                batch = []

    print(f"Finished streaming query results. Total rows: {total_rows}, Chunks sent: {total_chunks}")
    return {
        'total_rows': total_rows,
        'chunks_sent': total_chunks
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda function handler with streaming capability."""
    print("Lambda handler invoked.")
    print("Received event:", json.dumps(event))

    try:
        query = event['arguments']['query']
        print(f"Extracted query from event: {query}")
    except KeyError:
        print("No query provided in the event.")
        return {'error': 'No query provided.'}

    if not is_query_safe(query):
        print("Unsafe SQL query detected.")
        return {'error': 'Unsafe SQL query.'}

    query_with_limit = add_limit_if_needed(query)
    print(f"Final query to execute: {query_with_limit}")

    if not db_pool:
        print("Database connection pool is not initialized.")
        return {'error': 'Database connection pool is not initialized.'}

    transfer_id = str(uuid.uuid4())
    print(f"Generated transfer ID: {transfer_id}")

    try:
        print("Acquiring database connection from pool...")
        conn = db_pool.getconn()
        print("Database connection acquired.")

        # Use server-side cursor for streaming
        with conn.cursor(
                name='streaming_cursor',
                cursor_factory=extras.RealDictCursor
        ) as cursor:
            cursor.itersize = FETCH_SIZE
            print(f"Executing query: {query_with_limit}")
            cursor.execute(query_with_limit)
            print("Query executed successfully.")

            # Start streaming results
            result_info = stream_query_results(cursor, transfer_id)
            print(
                f"Streaming completed. Total rows: {result_info['total_rows']}, Chunks sent: {result_info['chunks_sent']}")

            return {
                'transferId': transfer_id,
                'metadata': {
                    'rowCount': result_info['total_rows'],
                    'chunkCount': result_info['chunks_sent'],
                    'schema': None  # or provide the actual schema if available
                }
            }

    except Exception as e:
        print(f"Error processing query: {e}")
        return {'error': str(e)}

    finally:
        if 'conn' in locals():
            print("Returning connection to the pool.")
            db_pool.putconn(conn)
            print("Connection returned to the pool.")


# Example usage (for testing purposes)
if __name__ == "__main__":
    test_event = {
        'arguments': {
            'query': 'SELECT * FROM your_table'
        }
    }
    test_context = None
    print("Testing lambda_handler with test_event.")
    response = lambda_handler(test_event, test_context)
    print("Lambda handler response:", response)
