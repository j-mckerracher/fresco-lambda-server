import json
import os
import psycopg2
import psycopg2.pool
import sqlparse
import pyarrow as pa
import pyarrow.ipc as ipc
from psycopg2 import extras
import boto3
from botocore.exceptions import ClientError
import base64
import time
import uuid

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024  # 128 KB
CHUNK_SIZE = int((MAX_MQTT_PAYLOAD_SIZE - 200) * 0.75)  # AWS IoT MQTT payload limit
ROW_LIMIT = 1000

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

    try:
        arrow_data, row_count, schema_info = execute_query_and_serialize(conn, query_with_limit)
    except Exception as e:
        print(f"Query execution failed: {e}")
        return {
            'error': 'Query execution failed.'
        }
    finally:
        # Release the connection back to the pool
        db_pool.putconn(conn)
        print("Released database connection back to pool.")

    if not arrow_data:
        print("No data returned from query.")
        return {
            'error': 'No data returned from query.'
        }

    # Generate a unique reference ID for this data transfer
    transfer_id = str(uuid.uuid4())

    # Publish the binary data to AWS IoT MQTT topic in chunks
    if iot_client is None or iot_topic is None:
        print("AWS IoT Data client is not initialized.")
        return {
            'error': 'AWS IoT Data client is not initialized.'
        }

    try:
        chunk_info = publish_in_chunks(iot_client, iot_topic, arrow_data, transfer_id)
        print(f"Published data to IoT topic '{iot_topic}' in chunks.")
    except Exception as e:
        print(f"Failed to publish data in chunks: {e}")
        return {
            'error': 'Failed to publish data to IoT topic.'
        }

    # Return metadata about the published data
    return {
        'status': 'success',
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


def execute_query_and_serialize(conn, query):
    """
    Executes the SQL query, fetches the data, converts it to Apache Arrow IPC format,
    and returns the binary data along with row count and schema information.
    """
    print(f"Executing query: {query}")
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        row_count = len(rows)
        print(f"Query returned {row_count} rows.")

        if rows:
            # Convert list of dicts to PyArrow Table
            table = pa.Table.from_pylist(rows)
            print("Converted rows to PyArrow Table.")

            # Extract schema information
            schema_info = {
                field.name: str(field.type)
                for field in table.schema
            }

            # Serialize the table to Apache Arrow IPC format
            sink = pa.BufferOutputStream()
            writer = ipc.RecordBatchStreamWriter(sink, table.schema)
            writer.write_table(table)
            writer.close()
            arrow_data = sink.getvalue().to_pybytes()
            print("Serialized data to Apache Arrow IPC format.")
            return arrow_data, row_count, schema_info
        else:
            print("No data to serialize.")
            return b'', 0, {}


def publish_in_chunks(iot_client, topic, data, transfer_id):
    """
    Publishes the given data to the specified IoT topic in chunks,
    accounting for Base64 encoding overhead and metadata size.
    """
    total_size = len(data)
    print(f"Total data size: {total_size} bytes.")
    chunk_count = 0
    total_chunks = -(-total_size // CHUNK_SIZE)  # Ceiling division

    for i in range(0, total_size, CHUNK_SIZE):
        chunk = data[i:i + CHUNK_SIZE]
        chunk_size = len(chunk)
        chunk_count += 1

        # Create chunk metadata
        chunk_metadata = {
            'transfer_id': transfer_id,
            'chunk_number': chunk_count,
            'total_chunks': total_chunks,
            'chunk_size': chunk_size
        }

        # Encode the chunk
        encoded_chunk = base64.b64encode(chunk).decode('utf-8')

        # Create the full payload
        payload = {
            'metadata': chunk_metadata,
            'data': encoded_chunk
        }

        # Verify payload size before sending
        payload_json = json.dumps(payload)
        payload_size = len(payload_json.encode('utf-8'))
        print(f"Publishing chunk {chunk_count}/{total_chunks}: {payload_size} bytes (after encoding)")

        if payload_size > MAX_MQTT_PAYLOAD_SIZE:
            print(f"Warning: Payload size {payload_size} exceeds MQTT limit of {MAX_MQTT_PAYLOAD_SIZE}")
            raise Exception(f"Payload size {payload_size} exceeds MQTT limit")

        try:
            iot_client.publish(
                topic=topic,
                qos=1,
                payload=payload_json
            )
            print(f"Successfully published chunk {chunk_count}/{total_chunks}.")
        except ClientError as e:
            print(f"ClientError publishing chunk {chunk_count}: {e}")
            retries = 3
            delay = 1
            for attempt in range(1, retries + 1):
                try:
                    print(f"Retrying chunk {chunk_count}, attempt {attempt}.")
                    time.sleep(delay)
                    iot_client.publish(
                        topic=topic,
                        qos=1,
                        payload=payload_json
                    )
                    print(f"Successfully published chunk {chunk_count} on retry {attempt}.")
                    break
                except ClientError as retry_e:
                    print(f"Retry {attempt} failed for chunk {chunk_count}: {retry_e}")
                    delay *= 2
                    if attempt == retries:
                        raise Exception(f"Failed to publish chunk {chunk_count} after {retries} attempts.")
        except Exception as e:
            print(f"Unexpected error publishing chunk {chunk_count}: {e}")
            raise Exception(f"Failed to publish chunk {chunk_count}: {e}")

    print(f"All {chunk_count} chunks published successfully.")
    return {
        'chunk_count': chunk_count,
        'total_chunks': total_chunks
    }