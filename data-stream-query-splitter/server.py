import json
import os
from dataclasses import dataclass
import boto3
from typing import Dict, Any, List, Tuple
import time
import asyncio

# Constants
MAX_PARTITION_SIZE = 256 * 1024  # 256 KiB in bytes
MAX_EXECUTION_TIME = 2.5  # 2.5 seconds max execution time

# Hardcoded schema based on database analysis
JOB_DATA_SCHEMA = {
    "time": "timestamp",
    "submit_time": "timestamp",
    "start_time": "timestamp",
    "end_time": "timestamp",
    "timelimit": "float",
    "nhosts": "integer",
    "ncores": "integer",
    "value_cpuuser": "float",
    "value_gpu": "float",
    "value_memused": "float",
    "value_memused_minus_diskcache": "float",
    "value_nfs": "float",
    "value_block": "float",
    "exitcode": "string",
    "host_list": "string",
    "username": "string",
    "account": "string",
    "queue": "string",
    "host": "string",
    "jid": "string",
    "unit": "string",
    "jobname": "string"
}

# Hardcoded metrics based on database analysis
AVERAGE_ROW_SIZE_BYTES = 200  # Rounded up from 199.856
ROWS_PER_MONTH = 800000

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


def get_schema(query: str) -> Dict:
    """Return hardcoded schema for job_data table"""
    return JOB_DATA_SCHEMA


def estimate_row_size(query: str) -> Tuple[float, int]:
    """
    Return hardcoded estimates based on analysis of data
    For time-based queries, estimate row count based on date range
    """
    query_lower = query.lower()
    if 'time' in query_lower and '>=' in query_lower and '<' in query_lower:
        try:
            # Extract dates using string operations
            start_date = query_lower.split(">=")[1].split("'")[1][:7]  # Gets YYYY-MM
            end_date = query_lower.split("<")[1].split("'")[1][:7]  # Gets YYYY-MM

            # Calculate number of months
            start_year, start_month = map(int, start_date.split('-'))
            end_year, end_month = map(int, end_date.split('-'))
            months_diff = (end_year - start_year) * 12 + (end_month - start_month)

            # Estimate total rows based on months
            estimated_rows = months_diff * ROWS_PER_MONTH
        except Exception:
            # Fallback to a reasonable default if parsing fails
            estimated_rows = ROWS_PER_MONTH
    else:
        # Default estimate for non-time-based queries
        estimated_rows = ROWS_PER_MONTH

    return AVERAGE_ROW_SIZE_BYTES, estimated_rows


def calculate_partitions(query: str, avg_row_size: float, total_rows: int, row_limit: int = None) -> List[
    QueryPartition]:
    """Calculate query partitions ensuring each returns < 256 KiB"""
    if row_limit:
        total_rows = min(total_rows, row_limit)

    # Use larger partition size to reduce number of partitions
    rows_per_partition = max(int(MAX_PARTITION_SIZE / avg_row_size), 1000)
    total_partitions = (total_rows + rows_per_partition - 1) // rows_per_partition

    partitions = []
    for i in range(total_partitions):
        start_offset = i * rows_per_partition
        end_offset = min((i + 1) * rows_per_partition, total_rows)

        if end_offset <= start_offset:
            continue

        partition_query = f"WITH limited_query AS ({query} LIMIT {row_limit}) SELECT * FROM limited_query" if row_limit else query

        partitions.append(QueryPartition(
            partition_id=i,
            order=i + 1,
            start_offset=start_offset,
            end_offset=end_offset,
            query=partition_query,
            transfer_id="",
            estimated_size=int((end_offset - start_offset) * avg_row_size)
        ))

    return partitions


def send_to_sqs(partition: QueryPartition):
    """Send partition info to SQS with minimal payload"""
    message = {
        'pid': partition.partition_id,
        'o': partition.order,
        'tid': partition.transfer_id,
        'q': partition.query,
        's': partition.start_offset,
        'e': partition.end_offset,
        'sz': partition.estimated_size
    }

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message),
        MessageAttributes={
            'tid': {'DataType': 'String', 'StringValue': partition.transfer_id},
            'o': {'DataType': 'Number', 'StringValue': str(partition.order)}
        }
    )


async def _process_request(event_body: Dict[str, Any]):
    print(f"Processing request. Event body: {event_body}")
    start_time = time.time()
    transfer_id = None
    schema_info = None
    messages_sent = 0

    def check_timeout():
        if time.time() - start_time > MAX_EXECUTION_TIME:
            raise TimeoutError("Function approaching timeout limit")

    try:
        # Extract arguments from the request format
        arguments = event_body.get('arguments', {})
        transfer_id = arguments.get('clientId')
        query = arguments.get('query')
        row_limit = arguments.get('rowLimit')

        if not transfer_id or not query:
            raise ValueError("Missing required fields: clientId and query are required in arguments")

        # Get schema info
        schema_info = get_schema(query)

        check_timeout()
        # Get size and row estimates (now very fast)
        avg_row_size, estimated_total_rows = estimate_row_size(query)

        check_timeout()
        # Calculate partitions
        partitions = calculate_partitions(query, avg_row_size, estimated_total_rows, row_limit)

        check_timeout()
        # Send to SQS
        for partition in partitions:
            partition.transfer_id = transfer_id
            send_to_sqs(partition)
            messages_sent += 1

            if messages_sent % 100 == 0:
                print(f"Sent {messages_sent} partition messages...")
                check_timeout()

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

    except TimeoutError as te:
        error_message = str(te)
        response_body = create_response_body(
            transfer_id=transfer_id,
            schema_info=schema_info,
            time_elapsed=time.time() - start_time,
            messages_sent=messages_sent,
            status="TIMEOUT_ERROR",
            error=error_message
        )
        return create_api_response(408, response_body)

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
    if sqs is None:
        return create_api_response(500, {'error': 'SQS client failed to initialize'})

    print(f"Incoming event: {event}")

    if event.get('httpMethod') == 'OPTIONS':
        return create_api_response(200, {'message': 'CORS preflight request successful'})

    try:
        print("Loading event body.")
        body = json.loads(event.get('body', '{}'))
        return asyncio.run(_process_request(body))
    except json.JSONDecodeError:
        return create_api_response(400, {
            'error': 'Invalid JSON in request body'
        })