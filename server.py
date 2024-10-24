import asyncio
import json
import os
import aioboto3
import sqlparse
from typing import List, Any, Dict
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from sqlparse.tokens import Token
import jwt  # Requires PyJWT library
import time

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, DatabaseError, SQLAlchemyError

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
    # Check for disallowed tokens
    for token in stmt.flatten():
        if token.ttype in [Token.Keyword.DDL, Token.Keyword.DML]:
            print(f"Unsafe query: Disallowed token detected - {token.value}")
            return False
        if token.match(Token.Punctuation, ';'):
            print("Unsafe query: Semicolon detected, which is not allowed.")
            return False
    print("SQL query is deemed safe.")
    return True


# Main Lambda handler
def lambda_handler(event, context):
    print("Lambda handler invoked.")
    request_context = event.get('requestContext', {})
    if 'routeKey' in request_context:
        # This is a WebSocket event
        print("Detected WebSocket event.")
        route_key = request_context['routeKey']
        print(f"WebSocket route key: {route_key}")
        if route_key == '$connect':
            print("Handling WebSocket $connect event.")
            return handle_connect(event)
        elif route_key == '$disconnect':
            print("Handling WebSocket $disconnect event.")
            return handle_disconnect(event)
        elif route_key == '$default':
            print("Handling WebSocket $default event.")
            return handle_default(event)
        else:
            print(f"Invalid route key: {route_key}")
            return {
                'statusCode': 400,
                'body': 'Invalid route'
            }
    else:
        # This is an HTTP API event
        print("Detected HTTP API event.")
        return asyncio.run(main_handler(event))


def handle_connect(event):
    """
    Handle WebSocket $connect event.
    Extract clientId and store the connectionId and clientId in DynamoDB.
    """
    print("Entered handle_connect function.")
    connection_id = event['requestContext']['connectionId']
    print(f"Connection ID: {connection_id}")

    # Extract Authorization header
    auth_header = event.get('headers', {}).get('Authorization')
    print(f"Authorization header: {auth_header}")
    if not auth_header:
        print("Authorization header missing.")
        return {
            'statusCode': 401,
            'body': 'Unauthorized: Missing Authorization header'
        }

    try:
        token = auth_header.split(" ")[1]  # Assuming "Bearer <token>"
        print(f"Extracted JWT token: {token}")
    except IndexError:
        print("Invalid Authorization header format.")
        return {
            'statusCode': 401,
            'body': 'Unauthorized: Invalid Authorization header format'
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
            'body': f'Unauthorized: {str(e)}'
        }

    # Store connectionId and clientId in DynamoDB
    print(f"Adding connection {connection_id} for client {client_id} to DynamoDB.")
    asyncio.run(add_connection(connection_id, client_id))

    print(f"Connection established: {connection_id} for client: {client_id}")
    return {
        'statusCode': 200,
        'body': 'Connected'
    }


def handle_disconnect(event):
    """
    Handle WebSocket $disconnect event.
    Remove the connectionId from DynamoDB.
    """
    print("Entered handle_disconnect function.")
    connection_id = event['requestContext']['connectionId']
    print(f"Connection ID to disconnect: {connection_id}")

    # Remove connectionId from DynamoDB
    print(f"Removing connection {connection_id} from DynamoDB.")
    asyncio.run(remove_connection(connection_id))

    print(f"Connection disconnected: {connection_id}")
    return {
        'statusCode': 200,
        'body': 'Disconnected'
    }


def handle_default(event):
    """
    Handle WebSocket $default event.
    This can be used to process messages sent by clients.
    """
    print("Entered handle_default function.")
    # For simplicity, we'll just acknowledge the message
    return {
        'statusCode': 200,
        'body': 'Message received'
    }


async def main_handler(event) -> Dict[str, Any]:
    """
    Asynchronous main handler for the Lambda function.
    Processes HTTP GET requests with a SQL query and streams data over WebSocket connections.
    Handles the /ws-url endpoint to provide the WebSocket URL.
    """
    print("Entered main_handler function.")
    engine = None  # Initialize engine variable for cleanup
    try:
        # Extract the HTTP method and path
        http_method = event.get('requestContext', {}).get('http', {}).get('method')
        path = event.get('rawPath', '')
        print(f"HTTP Method: {http_method}, Path: {path}")

        if http_method != 'GET':
            print(f"Unsupported HTTP method: {http_method}")
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Method Not Allowed'})
            }

        if path == '/ws-url':
            # Handle the /ws-url endpoint
            print("Handling /ws-url endpoint.")
            websocket_url = f"wss://{os.environ['WEBSOCKET_API_ID']}.execute-api.{os.environ['REGION']}.amazonaws.com/{os.environ['WEBSOCKET_STAGE']}"
            print(f"Generated WebSocket URL: {websocket_url}")
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'websocket_url': websocket_url})
            }

        # If not /ws-url, proceed with data streaming
        print("Proceeding with data streaming.")
        # Extract clientId from Authorization header
        auth_header = event.get('headers', {}).get('Authorization')
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

        # Append LIMIT clause to ensure maximum of one million rows
        query_with_limit = f"SELECT * FROM ({query}) AS sub LIMIT 1000000"
        print(f"SQL query with LIMIT: {query_with_limit}")

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
        chunk_size = int(os.environ.get('CHUNK_SIZE', '10000'))  # Default to 10,000
        print(f"Configured chunk size: {chunk_size}")

        # Build the database URL
        user = os.environ['DB_USER']
        password = os.environ['DB_PASSWORD']
        host = os.environ['DB_HOST']
        port = os.environ['DB_PORT']
        dbname = os.environ['DB_NAME']
        print("Building database URL.")
        database_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
        print(f"Database URL: {database_url}")

        # Create the async engine
        print("Creating asynchronous database engine.")
        engine = create_async_engine(database_url, echo=False)

        async with engine.connect() as conn:
            print("Connected to the database.")
            # Use stream_results to fetch data in chunks
            print(f"Executing SQL query: {query_with_limit}")
            result = await conn.stream(text(query_with_limit))
            print("SQL query executed successfully.")

            sequence_id = 0

            # Set up the API Gateway Management API client
            endpoint = f"https://{os.environ['WEBSOCKET_API_ID']}.execute-api.{os.environ['REGION']}.amazonaws.com/{os.environ['WEBSOCKET_STAGE']}"
            print(f"API Gateway Management API endpoint: {endpoint}")
            session = aioboto3.Session()
            async with session.client('apigatewaymanagementapi', endpoint_url=endpoint) as apigw_management_api:
                print("API Gateway Management API client created.")
                # Fetch and send data in chunks
                async for chunk in result.partitions(chunk_size):
                    print(f"Fetched chunk with {len(chunk)} rows.")
                    # Prepare the message with sequence_id and data
                    message = {
                        'sequence_id': sequence_id,
                        'data': [dict(row) for row in chunk]
                    }
                    print(f"Prepared message for sequence_id {sequence_id}: {message}")
                    # Send the message over WebSocket connections
                    await send_message_to_connections(apigw_management_api, connection_ids, message)
                    print(f"Sent message for sequence_id {sequence_id} to connections.")
                    sequence_id += 1

                # After all data chunks have been sent, send a completion message
                completion_message = {
                    'type': 'completion',
                    'message': 'All data chunks have been sent.'
                }
                print("Sending completion message to all connections.")
                await send_message_to_connections(apigw_management_api, connection_ids, completion_message)
                print("Completion message sent to all connections.")

        print("Data streaming completed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data sent successfully'})
        }

    except OperationalError as e:
        print(f"Operational error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Database connection failed'})
        }
    except DatabaseError as e:
        print(f"Database error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Database query failed'})
        }
    except SQLAlchemyError as e:
        print(f"SQLAlchemy error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'An unexpected database error occurred'})
        }
    except ValueError as e:
        print(f"Value error: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'An unexpected error occurred'})
        }
    finally:
        if engine:
            print("Disposing of the database engine.")
            await engine.dispose()
            print("Database engine disposed.")


async def send_message_to_connections(apigw_management_api, connection_ids: List[str], message: Dict[str, Any]) -> None:
    """
    Send a message to multiple WebSocket connections concurrently.
    Handle disconnections and failed sends.
    """
    print(f"Sending message to {len(connection_ids)} connections.")
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


async def add_connection(connection_id: str, client_id: str):
    """
    Add a new connectionId and clientId to the DynamoDB table.
    """
    print(f"Adding connection to DynamoDB: connectionId={connection_id}, clientId={client_id}")
    dynamodb_table = os.environ['DYNAMODB_TABLE']
    region = os.environ['REGION']
    ttl_value = int(time.time()) + 3600  # Set TTL to 1 hour from now
    print(f"DynamoDB Table: {dynamodb_table}, Region: {region}, TTL: {ttl_value}")

    async with aioboto3.resource('dynamodb', region_name=region) as dynamodb:
        table = await dynamodb.Table(dynamodb_table)
        print("Putting item into DynamoDB table.")
        await table.put_item(
            Item={
                'connectionId': connection_id,
                'clientId': client_id,
                'ttl': ttl_value
            }
        )
        print("Connection added to DynamoDB successfully.")


async def remove_connection(connection_id: str):
    """
    Remove a connectionId from the DynamoDB table.
    """
    print(f"Removing connection from DynamoDB: connectionId={connection_id}")
    dynamodb_table = os.environ['DYNAMODB_TABLE']
    region = os.environ['REGION']
    print(f"DynamoDB Table: {dynamodb_table}, Region: {region}")

    async with aioboto3.resource('dynamodb', region_name=region) as dynamodb:
        table = await dynamodb.Table(dynamodb_table)
        print("Deleting item from DynamoDB table.")
        await table.delete_item(
            Key={
                'connectionId': connection_id
            }
        )
        print(f"Connection {connection_id} removed from DynamoDB successfully.")


async def get_client_connections(client_id: str) -> List[str]:
    """
    Retrieve all active connectionIds associated with a clientId.
    """
    print(f"Retrieving connections for clientId: {client_id}")
    dynamodb_table = os.environ['DYNAMODB_TABLE']
    region = os.environ['REGION']
    print(f"DynamoDB Table: {dynamodb_table}, Region: {region}")

    async with aioboto3.resource('dynamodb', region_name=region) as dynamodb:
        table = await dynamodb.Table(dynamodb_table)
        print("Querying DynamoDB for active connections.")
        response = await table.query(
            IndexName='ClientIdIndex',
            KeyConditionExpression='clientId = :cid',
            ExpressionAttributeValues={
                ':cid': client_id
            }
        )
        items = response.get('Items', [])
        print(f"Number of connections retrieved: {len(items)}")
        connection_ids = [item['connectionId'] for item in items]
        print(f"Connection IDs: {connection_ids}")
        return connection_ids
