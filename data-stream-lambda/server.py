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
from operator import itemgetter
import concurrent.futures
import threading
import queue
import time

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
CHUNK_SIZE = int((MAX_MQTT_PAYLOAD_SIZE - 200) * 0.75)  # AWS IoT MQTT payload limit
ROW_LIMIT = 1000
PUBLISH_WORKERS = 5  # Number of parallel publishing threads
QUEUE_MAX_SIZE = 100  # Maximum number of chunks in the queue for backpressure

# Initialize the PostgreSQL connection pool as a global variable
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=20,  # Adjust based on expected concurrency and database limits
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME']
    )
    print("Database connection pool created successfully.")
except Exception as e:
    print(f"Error creating database connection pool: {e}")
    db_pool = None  # Handle this in the handler

# Initialize AWS IoT Data client using boto3
try:
    iot_endpoint = os.environ['IOT_ENDPOINT']
    iot_topic = os.environ['IOT_TOPIC']
    region = os.environ.get('REGION', 'us-east-1')
    iot_client = boto3.client('iot-data', region_name=region, endpoint_url=f"https://{iot_endpoint}")
    print("AWS IoT Data client initialized successfully.")
except Exception as e:
    print(f"Error initializing AWS IoT Data client: {e}")
    iot_client = None  # Handle this in the handler


def lambda_handler(event, context):
    """
    Lambda function handler to process GraphQL queries, execute SQL against PostgreSQL,
    serialize the results to Apache Arrow IPC format, publish the data in chunks to AWS IoT,
    and return metadata about the published data.
    """
    print("Received event:", json.dumps(event))

    try:
        # Extract the SQL query from the AppSync event arguments
        query = event['arguments']['query']
        print(f"Extracted query: {query}")
    except KeyError:
        print("No query provided in the arguments.")
        return {
            'error': 'No query provided.'
        }

    # Validate the SQL query
    if not is_query_safe(query):
        print("Unsafe SQL query detected.")
        return {
            'error': 'Unsafe SQL query.'
        }

    # Add LIMIT clause if needed
    query_with_limit = add_limit_if_needed(query)
    print(f"Final query to execute: {query_with_limit}")

    # Connect to the database using the connection pool
    if db_pool is None:
        print("Database connection pool is not initialized.")
        return {
            'error': 'Database connection pool is not initialized.'
        }

    try:
        conn = db_pool.getconn()
        print("Acquired database connection from pool.")
    except Exception as e:
        print(f"Error acquiring database connection: {e}")
        return {
            'error': 'Failed to acquire database connection.'
        }

    # Initialize a queue for backpressure handling
    publish_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)

    # Event to signal publishing completion
    publishing_done = threading.Event()

    # Generate a unique transfer ID for this data transfer
    transfer_id = str(uuid.uuid4())
    print(f"Generated Transfer ID: {transfer_id}")

    try:
        # Start publisher threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=PUBLISH_WORKERS) as executor:
            # Start publisher workers
            futures = []
            for _ in range(PUBLISH_WORKERS):
                futures.append(executor.submit(publisher_worker, iot_client, iot_topic, publish_queue, publishing_done))

            # Execute the query and serialize in chunks
            row_count, schema_info, total_chunks = execute_query_and_serialize_stream(conn, query_with_limit,
                                                                                      publish_queue, transfer_id)

            # After all chunks are queued, signal that publishing is done
            publishing_done.set()

            # Wait for all publisher threads to finish
            concurrent.futures.wait(futures)

    except Exception as e:
        print(f"Error during query execution or publishing: {e}")
        return {
            'error': 'Failed to execute query or publish data.'
        }
    finally:
        # Release the connection back to the pool
        db_pool.putconn(conn)
        print("Released database connection back to pool.")

    if row_count == 0:
        print("No data returned from query.")
        return {
            'error': 'No data returned from query.'
        }

    # Return the transferId and related metadata
    return {
        'transferId': transfer_id,
        'rowCount': row_count,
        'schema': schema_info,
        'totalChunks': total_chunks
    }


def is_query_safe(query):
    """
    Validates the SQL query for safety by checking for disallowed keywords and semicolons.
    Returns True if the query is considered safe, False otherwise.
    """
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


def add_limit_if_needed(query, row_limit=ROW_LIMIT):
    """
    Appends a LIMIT clause to the SQL query if it's not already present.
    """
    print("Adding LIMIT clause if needed.")
    if 'LIMIT' not in query.upper():
        limited_query = f"{query} LIMIT {row_limit}"
        print(f"LIMIT added to query: {limited_query}")
        return limited_query
    else:
        print("Query already contains LIMIT.")
        return query


def execute_query_and_serialize_stream(conn, query, publish_queue, transfer_id):
    """
    Executes the SQL query using a server-side cursor, fetches data in chunks,
    serializes each chunk to Apache Arrow IPC format, and puts serialized chunks into the publish queue.
    Returns total row count, schema information, and total number of chunks.
    """
    print(f"Executing query with streaming: {query}")
    with conn.cursor(name='stream_cursor', cursor_factory=extras.RealDictCursor) as cursor:
        cursor.itersize = ROW_LIMIT  # Number of rows to fetch per batch
        cursor.execute(query)
        total_rows = 0
        schema_info = {}
        total_chunks = 0

        while True:
            rows = cursor.fetchmany(cursor.itersize)
            if not rows:
                break

            batch_size = len(rows)
            total_rows += batch_size
            print(f"Fetched {batch_size} rows. Total rows so far: {total_rows}")

            # Convert list of dicts to PyArrow Table
            table = pa.Table.from_pylist(rows)
            print("Converted rows to PyArrow Table.")

            # Extract schema information once
            if not schema_info:
                schema_info = {
                    field.name: str(field.type)
                    for field in table.schema
                }
                print(f"Schema Information: {schema_info}")

            # Serialize the table to Apache Arrow IPC format
            sink = pa.BufferOutputStream()
            with ipc.RecordBatchStreamWriter(sink, table.schema) as writer:
                writer.write_table(table)
            arrow_data = sink.getvalue().to_pybytes()
            print("Serialized data to Apache Arrow IPC format.")

            # Split serialized data into MQTT-sized chunks
            for i in range(0, len(arrow_data), CHUNK_SIZE):
                chunk = arrow_data[i:i + CHUNK_SIZE]
                chunk_size = len(chunk)
                sequence_number = total_chunks + 1  # Starting from 1
                total_chunks += 1

                # Create message structure
                message = {
                    'type': 'arrow_data',  # Type indicator
                    'metadata': {
                        'transfer_id': transfer_id,  # Shared transfer_id for all chunks
                        'sequence_number': sequence_number,
                        'total_chunks': None,  # Optional: Update after all chunks are determined
                        'chunk_size': chunk_size,
                        'format': 'arrow_ipc'  # Data format
                    },
                    'data': base64.b64encode(chunk).decode('utf-8')
                }

                # Put the chunk into the queue (blocks if queue is full)
                try:
                    publish_queue.put_nowait(message)
                    print(f"Queued chunk {sequence_number}")
                except queue.Full:
                    print("Publish queue is full. Implementing backpressure.")
                    # Implement backpressure: wait until there's space
                    while True:
                        try:
                            publish_queue.put(message, timeout=1)
                            print(f"Queued chunk {sequence_number} after waiting")
                            break
                        except queue.Full:
                            print("Waiting for space in publish queue...")
                            continue

    print(f"Completed fetching and queuing data. Total rows: {total_rows}, Total chunks: {total_chunks}")
    return total_rows, schema_info, total_chunks


def publisher_worker(iot_client, topic, publish_queue, publishing_done_event):
    """
    Worker function to publish chunks from the queue to the IoT topic.
    Continues until publishing_done_event is set and the queue is empty.
    """
    while not publishing_done_event.is_set() or not publish_queue.empty():
        try:
            message = publish_queue.get(timeout=1)
            payload_json = json.dumps(message)
            payload_size = len(payload_json.encode('utf-8'))

            if payload_size > MAX_MQTT_PAYLOAD_SIZE:
                print(f"Payload size {payload_size} exceeds MQTT limit. Skipping chunk.")
                publish_queue.task_done()
                continue  # Or handle accordingly

            # Publish the message
            iot_client.publish(
                topic=topic,
                qos=1,  # At-least-once delivery
                payload=payload_json
            )
            print(f"Published chunk {message['metadata']['sequence_number']}")
            publish_queue.task_done()
        except queue.Empty:
            continue  # Check if publishing is done
        except Exception as e:
            print(f"Error publishing chunk {message.get('metadata', {}).get('sequence_number', 'Unknown')}: {e}")
            publish_queue.task_done()
