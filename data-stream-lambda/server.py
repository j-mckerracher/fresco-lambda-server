import asyncio
import json
import os
import aioboto3
import sqlparse
import time  # Import the time module for timing
from typing import List, Any, Dict
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from sqlparse.tokens import Token
import jwt  # Requires PyJWT library
import datetime
import decimal
import uuid
import traceback
import asyncpg

"""
Required environment variables:
- DB_HOST
- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_PORT
- CHUNK_SIZE
- WEBSOCKET_API_ID
- WEBSOCKET_STAGE
- REGION
- DYNAMODB_TABLE
- JWT_SECRET
- JWT_ISSUER
"""
ROW_LIMIT = 1000000

# Updated: Set a default chunk size that is safe within the payload limit
DEFAULT_CHUNK_SIZE = 10


# Function to parse and validate the SQL query
def is_query_safe(query: str) -> bool:
    """
    Parse the SQL query and ensure it is a safe SELECT statement.
    """
    print("Validating SQL query for safety.")
    parsed = sqlparse.parse(query)
    print(f"Parsed SQL statements count: {len(parsed)}")

    if len(parsed) != 1:
        print("Unsafe query: Multiple SQL statements detected.")
        return False  # Only allow single statements

    stmt = parsed[0]
    stmt_type = stmt.get_type()
    print(f"SQL statement type: {stmt_type}")

    if stmt_type != 'SELECT':
        print("Unsafe query: Only SELECT statements are allowed.")
        return False

    # Define a list of disallowed keywords
    disallowed_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE',
        'TRUNCATE', 'EXECUTE', 'GRANT', 'REVOKE', 'MERGE', 'CALL'
    ]

    for token in stmt.flatten():
        # Check for disallowed keywords
        if token.ttype == Token.Keyword and token.value.upper() in disallowed_keywords:
            print(f"Unsafe query: Disallowed keyword detected - {token.value}")
            return False
        # Prevent use of semicolons to avoid statement chaining
        if token.match(Token.Punctuation, ';'):
            print("Unsafe query: Semicolon detected, which is not allowed.")
            return False

    print("SQL query is deemed safe.")
    return True


# Main Lambda handler
def lambda_handler(event, context):
    start_time = time.perf_counter()  # Record the start time
    try:
        print("Lambda handler invoked.")
        print(f"Incoming event: {json.dumps(event)}")
        request_context = event.get('requestContext', {})

        if 'http' in request_context:
            return asyncio.run(main_handler(event))
        else:
            print("Invalid request to data Lambda: WebSocket event received.")
            return {
                'statusCode': 400,
                'body': 'Invalid request'
            }
    finally:
        end_time = time.perf_counter()  # Record the end time
        elapsed_time = end_time - start_time
        print(f"Lambda execution time: {elapsed_time:.6f} seconds - chunk size = {DEFAULT_CHUNK_SIZE} row limit = {ROW_LIMIT}")


async def main_handler(event) -> Dict[str, Any]:
    conn = None
    try:
        # Extract the routeKey
        route_key = event.get('routeKey', '')
        print(f"Route Key: {route_key}")

        # Split the routeKey into method and path
        try:
            http_method, path = route_key.split(' ', 1)
            print(f"HTTP Method: {http_method}, Path: {path}")
        except ValueError:
            print("Invalid routeKey format.")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Bad Request'})
            }

        # Validate HTTP method
        if http_method.upper() != 'GET':
            print(f"Unsupported HTTP method: {http_method}")
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Method Not Allowed'})
            }

        # Validate path
        if path != '/data':
            print(f"Unsupported path: {path}")
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Not Found'})
            }

        print("Proceeding with data streaming.")
        # Extract clientId from Authorization header (case-insensitive)
        headers = event.get('headers', {})
        # Normalize headers to lowercase for case-insensitive access
        headers_lower = {k.lower(): v for k, v in headers.items()}
        auth_header = headers_lower.get('authorization')
        print(f"Authorization header: {auth_header}")
        if not auth_header:
            print("Authorization header missing.")
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Unauthorized: Missing Authorization header'})
            }

        try:
            token = auth_header.split(" ")[1]  # Assuming "Bearer <token>"
            print(f"Extracted JWT token: {token}")
        except IndexError:
            print("Invalid Authorization header format.")
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Unauthorized: Invalid Authorization header format'})
            }

        try:
            print("Decoding JWT token.")
            decoded_token = jwt.decode(
                token,
                os.environ['JWT_SECRET'],
                algorithms=['HS256'],
                issuer=os.environ['JWT_ISSUER']
            )
            print(f"Decoded JWT token: {decoded_token}")
            client_id = decoded_token.get('clientId')
            print(f"Extracted clientId from token: {client_id}")
            if not client_id:
                print("clientId not found in token.")
                raise ValueError("clientId not found in token")
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError, ValueError) as e:
            print(f"JWT decoding error: {str(e)}")
            return {
                'statusCode': 401,
                'body': json.dumps({'error': f'Unauthorized: {str(e)}'})
            }

        # Get the SQL query from the request
        query_param = event.get('queryStringParameters', {}).get('query')
        print(f"Received query parameter: {query_param}")
        if not query_param:
            print("Query parameter missing.")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing query parameter'})
            }
        # Decode URL-encoded query parameter
        query = unquote_plus(query_param)
        print(f"Decoded SQL query: {query}")

        # Perform SQL sanitization
        if not is_query_safe(query):
            print("Unsafe SQL query detected.")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Unsafe query'})
            }

        # Check if the original query already has a LIMIT clause
        parsed = sqlparse.parse(query)
        stmt = parsed[0]
        has_limit = False
        for token in stmt.flatten():
            if token.ttype == Token.Keyword and token.value.upper() == 'LIMIT':
                has_limit = True
                break

        if not has_limit:
            # Append LIMIT clause to ensure a maximum of ROW_LIMIT rows
            query_with_limit = f"{query} LIMIT {ROW_LIMIT};"
            print(f"SQL query with LIMIT: {query_with_limit}")
        else:
            # Use the original query as is
            query_with_limit = f"{query};"
            print(f"SQL query without appending LIMIT: {query_with_limit}")

        # Retrieve all active connectionIds for the clientId
        print(f"Retrieving active WebSocket connections for clientId: {client_id}")
        connection_ids = await get_client_connections(client_id)
        print(f"Active connection IDs: {connection_ids}")
        if not connection_ids:
            print("No active WebSocket connections found for the client.")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No active WebSocket connections for client'})
            }

        # Configure chunk size
        chunk_size = int(os.environ.get('CHUNK_SIZE', str(DEFAULT_CHUNK_SIZE)))  # Default to 10
        print(f"Configured chunk size: {chunk_size}")

        # Initialize sequence_id
        sequence_id = 0

        # Configure API Gateway endpoint
        endpoint = f"https://{os.environ['WEBSOCKET_API_ID']}.execute-api.{os.environ['REGION']}.amazonaws.com/{os.environ['WEBSOCKET_STAGE']}"
        print(f"API Gateway Management API endpoint: {endpoint}")

        # Create database connection
        print("Establishing database connection.")
        conn = await asyncpg.connect(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            database=os.environ['DB_NAME']
        )
        print("Database connection established.")

        # Create API Gateway management client
        session = aioboto3.Session()
        async with session.client('apigatewaymanagementapi', endpoint_url=endpoint) as apigw_management_api:
            print("API Gateway Management API client created.")

            # Use a server-side cursor for memory efficiency
            async with conn.transaction():
                print(f"Executing query: {query_with_limit}")
                # Initialize chunk_data outside the loop
                chunk_data = []

                # Use cursor with prefetch
                async for record in conn.cursor(
                        query_with_limit,
                        prefetch=chunk_size
                ):
                    try:
                        # Convert record to dict
                        row_dict = dict(record)

                        # Handle special data types
                        for key, value in row_dict.items():
                            if isinstance(value, (datetime.datetime, datetime.date)):
                                row_dict[key] = value.isoformat()
                            elif isinstance(value, decimal.Decimal):
                                row_dict[key] = float(value)
                            elif isinstance(value, uuid.UUID):
                                row_dict[key] = str(value)
                            elif isinstance(value, bytes):
                                row_dict[key] = value.decode('utf-8')

                        chunk_data.append(row_dict)

                        # Check if chunk_size is reached
                        if len(chunk_data) >= chunk_size:
                            message = {
                                'sequence_id': sequence_id,
                                'data': chunk_data
                            }

                            # Serialize message to JSON
                            serialized_message = json.dumps(message, default=str)
                            message_size = len(serialized_message.encode('utf-8'))
                            print(f"Serialized message size: {message_size} bytes")

                            if message_size > 128 * 1024:
                                print(f"Message size {message_size} exceeds the 128 KB limit. Splitting the message.")
                                # Split chunk_data into smaller sub-chunks
                                sub_chunks = split_large_chunk(chunk_data)
                                for sub_chunk in sub_chunks:
                                    sub_message = {
                                        'sequence_id': sequence_id,
                                        'data': sub_chunk
                                    }
                                    await send_message_to_connections(apigw_management_api, connection_ids, sub_message)
                                    print(f"Sent sub-chunk for sequence_id {sequence_id}")
                                    sequence_id += 1
                            else:
                                # Send the message as is
                                await send_message_to_connections(apigw_management_api, connection_ids, message)
                                print(f"Sent message for sequence_id {sequence_id}")
                                sequence_id += 1

                            chunk_data = []  # Reset chunk data

                    except Exception as e:
                        print(f"Error processing record: {e}")
                        print(f"Traceback: {traceback.format_exc()}")
                        continue

                # Send any remaining data
                if chunk_data:
                    message = {
                        'sequence_id': sequence_id,
                        'data': chunk_data
                    }

                    # Serialize message to JSON
                    serialized_message = json.dumps(message, default=str)
                    message_size = len(serialized_message.encode('utf-8'))
                    print(f"Serialized message size: {message_size} bytes")

                    if message_size > 128 * 1024:
                        print("Final message size exceeds 128 KB. Splitting into smaller sub-chunks.")
                        sub_chunks = split_large_chunk(chunk_data)
                        for sub_chunk in sub_chunks:
                            sub_message = {
                                'sequence_id': sequence_id,
                                'data': sub_chunk
                            }
                            await send_message_to_connections(apigw_management_api, connection_ids, sub_message)
                            print(f"Sent final sub-chunk for sequence_id {sequence_id}")
                            sequence_id += 1
                    else:
                        # Send the final message as is
                        await send_message_to_connections(apigw_management_api, connection_ids, message)
                        print(f"Sent final message for sequence_id {sequence_id}")
                        sequence_id += 1

                # Send completion message
                completion_message = {
                    'type': 'completion',
                    'message': 'All data chunks have been sent.'
                }
                print("Sending completion message to all connections")
                await send_message_to_connections(apigw_management_api, connection_ids, completion_message)
                print("Completion message sent to all connections")

        print("Data streaming completed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data sent successfully'})
        }

    except asyncpg.PostgresError as e:
        print(f"Database error: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Database error: {str(e)}'})
        }
    except Exception as e:
        print(f"Unexpected error during data streaming: {str(e)}")
        print(f"Error type: {type(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        if conn:
            print("Closing database connection.")
            await conn.close()
            print("Database connection closed.")


async def remove_connection(connection_id: str):
    """
    Remove a connectionId from the DynamoDB table.
    """
    print(f"Removing connection from DynamoDB: connectionId={connection_id}")
    dynamodb_table = os.environ['DYNAMODB_TABLE']
    region = os.environ['REGION']
    print(f"DynamoDB Table: {dynamodb_table}, Region: {region}")

    session = aioboto3.Session()
    async with session.resource('dynamodb', region_name=region) as dynamodb:
        table = await dynamodb.Table(dynamodb_table)
        print("Deleting item from DynamoDB table.")
        await table.delete_item(
            Key={
                'connectionId': connection_id
            }
        )
        print(f"Connection {connection_id} removed from DynamoDB successfully.")


async def send_message_to_connections(apigw_management_api, connection_ids: List[str], message: Dict[str, Any]) -> None:
    """
    Send a message to multiple WebSocket connections concurrently.
    Handle disconnections and failed sends.
    """
    serialized_message = json.dumps(message, default=str)
    message_size = len(serialized_message.encode('utf-8'))
    print(f"Serialized message size: {message_size} bytes")

    if message_size > 128 * 1024:
        print(f"Cannot send message of size {message_size} bytes as it exceeds the 128 KB limit.")
        # Optionally, implement further splitting or handle the error as needed
        return

    print(f"Sending message of size {message_size} bytes to {len(connection_ids)} connections.")
    tasks = []
    for connection_id in connection_ids:
        print(f"Preparing to send message to connection ID: {connection_id}")
        tasks.append(send_message(apigw_management_api, connection_id, message))
    print("Executing send tasks concurrently.")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print("Send tasks completed. Processing results.")
    # Remove failed connections
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            failed_connection_id = connection_ids[idx]
            print(f"Failed to send message to connection ID: {failed_connection_id}. Removing from active connections.")
            # Remove the connectionId from DynamoDB
            await remove_connection(failed_connection_id)
        else:
            print(f"Message sent successfully to connection ID: {connection_ids[idx]}")


async def send_message(apigw_management_api, connection_id: str, message: Dict[str, Any]) -> None:
    """
    Send a single message to a WebSocket connection.
    """
    print(f"Sending message to connection ID: {connection_id}")
    try:
        await apigw_management_api.post_to_connection(
            Data=json.dumps(message, default=str).encode('utf-8'),
            ConnectionId=connection_id
        )
        print(f"Message sent to connection ID: {connection_id}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"ClientError while sending message to {connection_id}: {error_code}")
        if error_code in ('GoneException', 'ForbiddenException'):
            print(f"Connection {connection_id} is no longer valid. Raising exception to remove connection.")
            raise e
        else:
            print(f"Error sending message to {connection_id}: {e.response['Error']['Message']}")
            raise e
    except Exception as e:
        print(f"Unexpected error while sending message to {connection_id}: {e}")
        raise e


async def get_client_connections(client_id: str) -> List[str]:
    print(f"Retrieving connections for clientId: {client_id}")
    dynamodb_table = os.environ['DYNAMODB_TABLE']
    region = os.environ['REGION']
    print(f"DynamoDB Table: {dynamodb_table}, Region: {region}")

    session = aioboto3.Session()
    async with session.resource('dynamodb', region_name=region) as dynamodb:
        table = await dynamodb.Table(dynamodb_table)
        print("Querying DynamoDB for active connections.")
        try:
            response = await table.query(
                IndexName='ClientIdIndex',
                KeyConditionExpression='clientId = :cid',
                ExpressionAttributeValues={
                    ':cid': client_id
                }
            )
            print("DynamoDB query successful.")
        except Exception as e:
            print(f"DynamoDB query failed: {e}")
            raise e  # Re-raise to be caught by the outer try-except

        items = response.get('Items', [])
        print(f"Number of connections retrieved: {len(items)}")
        connection_ids = [item['connectionId'] for item in items]
        print(f"Connection IDs: {connection_ids}")
        return connection_ids


def split_large_chunk(chunk_data: List[Dict[str, Any]], max_size: int = 128 * 1024) -> List[List[Dict[str, Any]]]:
    """
    Splits chunk_data into smaller sub-chunks such that each sub-chunk's serialized size
    does not exceed max_size bytes.
    """
    sub_chunks = []
    current_sub_chunk = []
    current_size = 0

    for row in chunk_data:
        serialized_row = json.dumps(row, default=str)
        row_size = len(serialized_row.encode('utf-8'))

        if current_size + row_size > max_size:
            if current_sub_chunk:
                sub_chunks.append(current_sub_chunk)
            current_sub_chunk = [row]
            current_size = row_size
        else:
            current_sub_chunk.append(row)
            current_size += row_size

    if current_sub_chunk:
        sub_chunks.append(current_sub_chunk)

    print(f"Split chunk into {len(sub_chunks)} sub-chunks to fit within {max_size} bytes.")
    return sub_chunks