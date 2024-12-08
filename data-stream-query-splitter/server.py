import json
import os
import datetime
import psycopg2.pool
from dataclasses import dataclass
import boto3
from typing import Dict, Any, List, Tuple
import time
import pyarrow as pa
import asyncio
from contextlib import contextmanager

# Constants
MAX_PARTITION_SIZE = 256 * 1024  # 256 KiB in bytes
DEFAULT_SAMPLE_SIZE = 1000
PARALLEL_QUERIES = 12

required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME', 'SQS_QUEUE_URL']
for var in required_vars:
    if var not in os.environ:
        print(f"Missing required environment variable: {var}")

# Initialize the PostgreSQL connection pool
try:
    print("Connecting to DB.")
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=5,
        maxconn=10,
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

# Initialize SQS client
try:
    sqs = boto3.client('sqs')
    QUEUE_URL = os.environ['SQS_QUEUE_URL']
    print("SQS client initialized successfully.")
except Exception as e:
    print(f"Error initializing SQS client: {e}")
    sqs = None


@dataclass
class QueryPartition:
    partition_id: int
    order: int
    start_offset: int
    end_offset: int
    query: str
    transfer_id: str
    estimated_size: int


def create_api_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create API Gateway response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body)
    }


def create_response_body(
        transfer_id: str,
        partitions_count: int = 0,
        schema_info: Dict = None,
        time_elapsed: float = 0,
        estimated_rows: int = 0,
        messages_sent: int = 0,
        status: str = "COMPLETED",
        error: str = None
) -> Dict[str, Any]:
    """Create response body"""
    return {
        "transferId": transfer_id,
        "metadata": {
            "partitionsCount": partitions_count,
            "schema": json.dumps(schema_info) if schema_info else None,
            "isComplete": status == "COMPLETED",
            "timeElapsed": time_elapsed,
            "estimatedRows": estimated_rows,
            "messagesSent": messages_sent,
            "processingStatus": status,
            "error": error
        }
    }


@contextmanager
def get_db_connection():
    """Get database connection from pool"""
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)


def get_schema(query: str) -> Dict:
    """Get schema info dict for metadata"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM ({query}) t LIMIT 0")
            schema_info = {}

            for desc in cursor.description:
                name = desc[0]
                pg_type = desc[1]

                if pg_type in (1184, 1114):  # TIMESTAMPTZ or TIMESTAMP
                    schema_info[name] = "timestamp"
                elif pg_type == 701 or name == 'value_gpu':  # FLOAT8
                    schema_info[name] = "float"
                elif pg_type in (20, 23):  # BIGINT or INTEGER
                    schema_info[name] = "integer"
                else:
                    schema_info[name] = "string"

    return schema_info


def estimate_row_size(query: str, sample_size: int = DEFAULT_SAMPLE_SIZE) -> Tuple[float, int]:
    """
    Estimate average row size and total rows using a sample
    Returns: (avg_row_size_bytes, estimated_total_rows)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Get total row count estimate
            cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
            explain_result = cursor.fetchone()[0]
            estimated_total_rows = explain_result[0]['Plan']['Plan Rows']

            # Sample some rows to estimate size
            sample_query = f"""
            WITH sample AS (
                SELECT * FROM ({query}) t
                LIMIT {sample_size}
            )
            SELECT pg_column_size(t.*) FROM sample t
            """
            cursor.execute(sample_query)
            sizes = [row[0] for row in cursor.fetchall()]

            if not sizes:
                return 0, 0

            avg_size = sum(sizes) / len(sizes)

            # Add 10% overhead for safety
            avg_size = avg_size * 1.1

            return avg_size, estimated_total_rows


def calculate_partitions(query: str, avg_row_size: float, total_rows: int, row_limit: int = None) -> List[
    QueryPartition]:
    """Calculate query partitions ensuring each returns < 256 KiB"""
    if row_limit:
        total_rows = min(total_rows, row_limit)

    rows_per_partition = int(MAX_PARTITION_SIZE / avg_row_size)
    total_partitions = (total_rows + rows_per_partition - 1) // rows_per_partition

    print(f"Calculated partition details:")
    print(f"- Average row size: {avg_row_size:.2f} bytes")
    print(f"- Rows per partition: {rows_per_partition}")
    print(f"- Total partitions needed: {total_partitions}")
    print(f"- Row limit applied: {row_limit if row_limit else 'None'}")

    partitions = []
    for i in range(total_partitions):
        start_offset = i * rows_per_partition
        end_offset = min((i + 1) * rows_per_partition, total_rows)

        # Skip empty partitions
        if end_offset <= start_offset:
            continue

        # Modify query to include row limit if specified
        partition_query = query
        if row_limit:
            partition_query = f"WITH limited_query AS ({query} LIMIT {row_limit}) SELECT * FROM limited_query"

        partition = QueryPartition(
            partition_id=i,
            order=i + 1,  # 1-based ordering
            start_offset=start_offset,
            end_offset=end_offset,
            query=partition_query,
            transfer_id="",  # Will be set later
            estimated_size=int((end_offset - start_offset) * avg_row_size)
        )
        partitions.append(partition)

    return partitions


def send_to_sqs(partition: QueryPartition):
    """Send partition info to SQS"""
    message = {
        'partition_id': partition.partition_id,
        'order': partition.order,
        'transfer_id': partition.transfer_id,
        'query': partition.query,
        'start_offset': partition.start_offset,
        'end_offset': partition.end_offset,
        'estimated_size': partition.estimated_size
    }

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message),
        MessageAttributes={
            'transfer_id': {
                'DataType': 'String',
                'StringValue': partition.transfer_id
            },
            'order': {
                'DataType': 'Number',
                'StringValue': str(partition.order)
            }
        }
    )


async def _process_request(event_body: Dict[str, Any]):
    print(f"Processing request. Event body: {event_body}")
    start_time = time.time()
    transfer_id = None
    schema_info = None
    messages_sent = 0

    try:
        # Extract arguments from the new request format

        arguments = event_body.get('arguments', {})
        transfer_id = arguments.get('clientId')
        query = arguments.get('query')
        row_limit = arguments.get('rowLimit')

        if not transfer_id or not query:
            raise ValueError("Missing required fields: clientId and query are required in arguments")

        # Get schema info for metadata
        schema_info = get_schema(query)

        # Estimate row size and total rows
        avg_row_size, estimated_total_rows = estimate_row_size(query)
        if avg_row_size == 0:
            raise ValueError("Could not estimate row size - no data returned from sample query")

        # Calculate partitions with row limit
        partitions = calculate_partitions(query, avg_row_size, estimated_total_rows, row_limit)

        # Set transfer ID and send to SQS
        for partition in partitions:
            partition.transfer_id = transfer_id
            send_to_sqs(partition)
            messages_sent += 1

            if messages_sent % 100 == 0:
                print(f"Sent {messages_sent} partition messages...")

        print(f"\nPartitioning complete:")
        print(f"✓ Total partitions: {len(partitions)}")
        print(f"✓ Average partition size: {MAX_PARTITION_SIZE / 1024:.2f} KiB")
        print(f"✓ Total messages sent: {messages_sent}")

        response_body = create_response_body(
            transfer_id=transfer_id,
            partitions_count=len(partitions),
            schema_info=schema_info,
            time_elapsed=time.time() - start_time,
            estimated_rows=min(estimated_total_rows, row_limit) if row_limit else estimated_total_rows,
            messages_sent=messages_sent,
            status="COMPLETED"
        )
        return create_api_response(200, response_body)

    except ValueError as ve:
        error_message = str(ve)
        response_body = create_response_body(
            transfer_id=transfer_id,
            schema_info=schema_info,
            time_elapsed=time.time() - start_time,
            messages_sent=messages_sent,
            status="VALIDATION_ERROR",
            error=error_message
        )
        return create_api_response(400, response_body)

    except Exception as e:
        error_message = str(e)
        error_type = "TIMEOUT_ERROR" if "timeout" in error_message.lower() else "ERROR"

        execution_time = time.time() - start_time
        print(f"\nError occurred after {execution_time:.2f} seconds:")
        print(f"✗ Error type: {error_type}")
        print(f"✗ Error message: {error_message}")

        response_body = create_response_body(
            transfer_id=transfer_id,
            schema_info=schema_info,
            time_elapsed=execution_time,
            messages_sent=messages_sent,
            status=error_type,
            error=error_message
        )
        return create_api_response(500, response_body)


def lambda_handler(event, context):
    if db_pool is None:
        return create_api_response(500, {'error': 'Database connection pool failed to initialize'})
    if sqs is None:
        return create_api_response(500, {'error': 'SQS client failed to initialize'})
    """Main Lambda handler for API Gateway requests"""
    # Handle OPTIONS request for CORS
    print(f"Incoming event: {event}")

    if event.get('httpMethod') == 'OPTIONS':
        return create_api_response(200, {'message': 'CORS preflight request successful'})

    try:
        # Parse request body
        print("Loading event body.")
        body = json.loads(event.get('body', '{}'))

        return asyncio.run(_process_request(body))

    except json.JSONDecodeError:
        return create_api_response(400, {
            'error': 'Invalid JSON in request body'
        })