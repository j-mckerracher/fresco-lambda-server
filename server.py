print("v1")
import asyncio
import json
import os
import aioboto3
import sqlparse
import jwt  # PyJWT library
from typing import List, Any, Dict
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from sqlparse.tokens import Token

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, DatabaseError, SQLAlchemyError

"""
Required env vars:
'DB_HOST'
'DB_NAME'
'DB_USER'
'DB_PW'
'DB_PORT'
'JWT_SECRET'
'JWT_ISSUER'
'CHUNK_SIZE'
'WEBSOCKET_API_ID'
'WEBSOCKET_STAGE'
'REGION'
"""


# Function to parse and validate the SQL query
def is_query_safe(query: str) -> bool:
    """
    Parse the SQL query and ensure it is a safe SELECT statement.
    """
    parsed = sqlparse.parse(query)
    if len(parsed) != 1:
        return False  # Only allow single statements
    stmt = parsed[0]
    if stmt.get_type() != 'SELECT':
        return False
    # Check for disallowed tokens
    for token in stmt.flatten():
        if token.ttype in [Token.Keyword.DDL, Token.Keyword.DML]:
            return False
        if token.match(Token.Punctuation, ';'):
            return False
    return True


# Function to extract and validate connection IDs from JWT token
def extract_connection_ids(event) -> List[str]:
    """
    Extract and validate connection IDs from the Authorization header.
    """
    token = event.get('headers', {}).get('Authorization')
    if not token:
        raise ValueError('Missing Authorization token')
    try:
        secret = os.environ['JWT_SECRET']
        issuer = os.environ.get('JWT_ISSUER', 'your-issuer')
        payload = jwt.decode(token, secret, algorithms=['HS256'], issuer=issuer)
        connection_ids = payload.get('connection_ids')
        if not isinstance(connection_ids, list) or not connection_ids:
            raise ValueError('Invalid connection IDs in token')
        # Limit to a maximum of 6 connections
        return connection_ids[:6]
    except jwt.InvalidTokenError as e:
        raise ValueError(f'Invalid token: {e}')


# Main Lambda handler
def lambda_handler(event, context):
    request_context = event.get('requestContext', {})
    if 'routeKey' in request_context:
        # This is a WebSocket event
        route_key = request_context['routeKey']
        if route_key == '$connect':
            return handle_connect(event)
        elif route_key == '$disconnect':
            return handle_disconnect(event)
        elif route_key == '$default':
            return asyncio.run(handle_default(event))
        else:
            return {
                'statusCode': 400,
                'body': 'Invalid route'
            }
    else:
        # This is an HTTP API event
        return asyncio.run(main_handler(event))


def handle_connect(event):
    """
    Handle WebSocket $connect event.
    Store the connection ID for later use.
    """
    connection_id = event['requestContext']['connectionId']
    # Store the connection ID in a database or cache (e.g., DynamoDB)
    # For simplicity, this example does not implement storage
    return {
        'statusCode': 200,
        'body': 'Connected'
    }


def handle_disconnect(event):
    """
    Handle WebSocket $disconnect event.
    Remove the connection ID from storage.
    """
    connection_id = event['requestContext']['connectionId']
    # Remove the connection ID from storage
    return {
        'statusCode': 200,
        'body': 'Disconnected'
    }


async def handle_default(event):
    """
    Handle WebSocket $default event.
    This can be used to process messages sent by clients.
    """
    connection_id = event['requestContext']['connectionId']
    # Process the incoming message if needed
    return {
        'statusCode': 200,
        'body': 'Message received'
    }


async def main_handler(event) -> Dict[str, Any]:
    """
    Asynchronous main handler for the Lambda function.
    """
    engine = None  # Initialize engine variable for cleanup
    try:
        # Get the query parameter
        query_param = event.get('queryStringParameters', {}).get('query')
        if not query_param:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing query parameter'})
            }
        # Decode URL-encoded query parameter
        query = unquote_plus(query_param)

        # Perform SQL sanitization
        if not is_query_safe(query):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Unsafe query'})
            }

        # Append LIMIT clause to ensure maximum of one million rows
        query_with_limit = f"SELECT * FROM ({query}) AS sub LIMIT 1000000"

        # Extract and validate connection IDs from token
        try:
            connection_ids = extract_connection_ids(event)
        except ValueError as e:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': str(e)})
            }

        # Configure chunk size
        chunk_size = int(os.environ.get('CHUNK_SIZE', '10000'))  # Default to 10,000

        # Build the database URL
        user = os.environ['DB_USER']
        password = os.environ['DB_PW']
        host = os.environ['DB_HOST']
        port = os.environ['DB_PORT']
        dbname = os.environ['DB_NAME']

        database_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"

        # Create the async engine
        engine = create_async_engine(database_url, echo=False)

        async with engine.connect() as conn:
            # Use stream_results to fetch data in chunks
            result = await conn.execute(
                text(query_with_limit).execution_options(stream_results=True)
            )
            sequence_id = 0

            # Set up the API Gateway Management API client
            endpoint = f"https://{os.environ['WEBSOCKET_API_ID']}.execute-api.{os.environ['REGION']}.amazonaws.com/{os.environ['WEBSOCKET_STAGE']}"
            session = aioboto3.session.Session()
            async with session.client('apigatewaymanagementapi', endpoint_url=endpoint) as apigw_management_api:

                # Fetch and send data in chunks
                async for chunk in result.partitions(chunk_size):
                    # Prepare the message
                    message = {
                        'sequence_id': sequence_id,
                        'data': [dict(row) for row in chunk]
                    }
                    # Send the message over WebSocket connections
                    await send_message_to_connections(apigw_management_api, connection_ids, message)
                    sequence_id += 1

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
            await engine.dispose()


async def send_message_to_connections(apigw_management_api, connection_ids: List[str], message: Dict[str, Any]) -> None:
    """
    Send a message to multiple WebSocket connections concurrently.
    Handle disconnections and failed sends.
    """
    tasks = []
    for connection_id in connection_ids:
        tasks.append(send_message(apigw_management_api, connection_id, message))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Remove failed connections
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            failed_connection_id = connection_ids[idx]
            print(f"Removing failed connection: {failed_connection_id}")
            # Optionally, remove the connection ID from the list or notify the client
            connection_ids.remove(failed_connection_id)


async def send_message(apigw_management_api, connection_id: str, message: Dict[str, Any]) -> None:
    """
    Send a single message to a WebSocket connection.
    """
    try:
        await apigw_management_api.post_to_connection(
            Data=json.dumps(message, default=str).encode('utf-8'),
            ConnectionId=connection_id
        )
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ('GoneException', 'ForbiddenException'):
            print(f"Connection {connection_id} is no longer valid.")
            raise e
        else:
            print(f"Error sending message to {connection_id}: {e.response['Error']['Message']}")
            raise e
