import asyncio
import json
import os
import aioboto3
import sqlparse
import time
from typing import List, Any, Dict, Optional
from botocore.exceptions import ClientError
from urllib.parse import unquote_plus
from sqlparse.tokens import Token
import jwt
import datetime
import decimal
import uuid
import asyncpg

# Constants
ROW_LIMIT = 1000000
DEFAULT_CHUNK_SIZE = 10
DISALLOWED_SQL_KEYWORDS = {
    'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE',
    'TRUNCATE', 'EXECUTE', 'GRANT', 'REVOKE', 'MERGE', 'CALL'
}


class SQLValidator:
    @staticmethod
    def is_query_safe(query: str) -> bool:
        """Parse the SQL query and ensure it is a safe SELECT statement."""
        print("Validating SQL query for safety.")
        parsed = sqlparse.parse(query)
        print(f"Parsed SQL statements count: {len(parsed)}")

        if len(parsed) != 1:
            print("Unsafe query: Multiple SQL statements detected.")
            return False

        stmt = parsed[0]
        stmt_type = stmt.get_type()
        print(f"SQL statement type: {stmt_type}")

        if stmt_type != 'SELECT':
            print("Unsafe query: Only SELECT statements are allowed.")
            return False

        for token in stmt.flatten():
            if token.ttype == Token.Keyword and token.value.upper() in DISALLOWED_SQL_KEYWORDS:
                print(f"Unsafe query: Disallowed keyword detected - {token.value}")
                return False
            if token.match(Token.Punctuation, ';'):
                print("Unsafe query: Semicolon detected, which is not allowed.")
                return False

        print("SQL query is deemed safe.")
        return True

    @staticmethod
    def add_limit_if_needed(query: str) -> str:
        """Add LIMIT clause if not present in the query."""
        parsed = sqlparse.parse(query)
        stmt = parsed[0]
        has_limit = any(
            token.ttype == Token.Keyword and token.value.upper() == 'LIMIT'
            for token in stmt.flatten()
        )

        if not has_limit:
            return f"{query} LIMIT {ROW_LIMIT};"
        return f"{query};"


class AuthenticationHandler:
    def __init__(self, event: Dict[str, Any]):
        self.event = event
        self.headers = {k.lower(): v for k, v in event.get('headers', {}).items()}

    def extract_token(self) -> Optional[str]:
        """Extract JWT token from Authorization header."""
        auth_header = self.headers.get('authorization')
        if not auth_header:
            raise ValueError("Authorization header missing")

        try:
            return auth_header.split(" ")[1]
        except IndexError:
            raise ValueError("Invalid Authorization header format")

    def validate_token(self) -> str:
        """Validate JWT token and return client_id."""
        token = self.extract_token()
        try:
            decoded_token = jwt.decode(
                token,
                os.environ['JWT_SECRET'],
                algorithms=['HS256'],
                issuer=os.environ['JWT_ISSUER']
            )
            client_id = decoded_token.get('clientId')
            if not client_id:
                raise ValueError("clientId not found in token")
            return client_id
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError) as e:
            raise ValueError(f"Invalid token: {str(e)}")


class WebSocketManager:
    def __init__(self, apigw_management_api):
        self.apigw_management_api = apigw_management_api

    async def remove_connection(self, connection_id: str) -> None:
        """Remove a connectionId from the DynamoDB table."""
        print(f"Removing connection from DynamoDB: connectionId={connection_id}")
        session = aioboto3.Session()
        async with session.resource('dynamodb', region_name=os.environ['REGION']) as dynamodb:
            table = await dynamodb.Table(os.environ['DYNAMODB_TABLE'])
            await table.delete_item(Key={'connectionId': connection_id})
            print(f"Connection {connection_id} removed from DynamoDB successfully.")

    async def send_message_to_connections(self, connection_ids: List[str], message: Dict[str, Any]) -> None:
        """Send a message to multiple WebSocket connections concurrently."""
        serialized_message = json.dumps(message, default=str)
        message_size = len(serialized_message.encode('utf-8'))

        if message_size > 128 * 1024:
            print(f"Cannot send message of size {message_size} bytes as it exceeds the 128 KB limit.")
            return

        print(f"Sending message to {len(connection_ids)} connections.")
        tasks = [self.send_message(conn_id, message) for conn_id in connection_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                await self.remove_connection(connection_ids[idx])

    async def send_message(self, connection_id: str, message: Dict[str, Any]) -> None:
        """Send a single message to a WebSocket connection."""
        try:
            await self.apigw_management_api.post_to_connection(
                Data=json.dumps(message, default=str).encode('utf-8'),
                ConnectionId=connection_id
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ('GoneException', 'ForbiddenException'):
                raise e
            raise e
        except Exception as e:
            raise e


class ConnectionManager:
    @staticmethod
    async def get_client_connections(client_id: str) -> List[str]:
        """Retrieve all active connectionIds for a client."""
        session = aioboto3.Session()
        async with session.resource('dynamodb', region_name=os.environ['REGION']) as dynamodb:
            table = await dynamodb.Table(os.environ['DYNAMODB_TABLE'])
            response = await table.query(
                IndexName='ClientIdIndex',
                KeyConditionExpression='clientId = :cid',
                ExpressionAttributeValues={':cid': client_id}
            )
            return [item['connectionId'] for item in response.get('Items', [])]


class DataProcessor:
    @staticmethod
    def process_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row of data, handling special data types."""
        processed_row = {}
        for key, value in row.items():
            if isinstance(value, (datetime.datetime, datetime.date)):
                processed_row[key] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                processed_row[key] = float(value)
            elif isinstance(value, uuid.UUID):
                processed_row[key] = str(value)
            elif isinstance(value, bytes):
                processed_row[key] = value.decode('utf-8')
            else:
                processed_row[key] = value
        return processed_row

    @staticmethod
    def split_large_chunk(chunk_data: List[Dict[str, Any]], max_size: int = 128 * 1024) -> List[List[Dict[str, Any]]]:
        """Split chunk_data into smaller sub-chunks."""
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

        return sub_chunks


class DataStreamHandler:
    def __init__(self, websocket_manager: WebSocketManager, connection_ids: List[str]):
        self.websocket_manager = websocket_manager
        self.connection_ids = connection_ids
        self.chunk_size = int(os.environ.get('CHUNK_SIZE', str(DEFAULT_CHUNK_SIZE)))
        self.sequence_id = 0

    async def process_and_send_chunk(self, chunk_data: List[Dict[str, Any]]) -> None:
        """Process and send a chunk of data."""
        message = {
            'sequence_id': self.sequence_id,
            'data': chunk_data
        }

        serialized_message = json.dumps(message, default=str)
        message_size = len(serialized_message.encode('utf-8'))

        if message_size > 128 * 1024:
            sub_chunks = DataProcessor.split_large_chunk(chunk_data)
            for sub_chunk in sub_chunks:
                sub_message = {
                    'sequence_id': self.sequence_id,
                    'data': sub_chunk
                }
                await self.websocket_manager.send_message_to_connections(self.connection_ids, sub_message)
                self.sequence_id += 1
        else:
            await self.websocket_manager.send_message_to_connections(self.connection_ids, message)
            self.sequence_id += 1

    async def send_completion_message(self) -> None:
        """Send completion message to all connections."""
        completion_message = {
            'type': 'completion',
            'message': 'All data chunks have been sent.'
        }
        await self.websocket_manager.send_message_to_connections(self.connection_ids, completion_message)


async def stream_data(conn: asyncpg.Connection, query: str, stream_handler: DataStreamHandler) -> None:
    """Stream data from database and send to WebSocket connections."""
    chunk_data = []

    async with conn.transaction():
        async for record in conn.cursor(query, prefetch=stream_handler.chunk_size):
            row_dict = DataProcessor.process_row(dict(record))
            chunk_data.append(row_dict)

            if len(chunk_data) >= stream_handler.chunk_size:
                await stream_handler.process_and_send_chunk(chunk_data)
                chunk_data = []

        if chunk_data:
            await stream_handler.process_and_send_chunk(chunk_data)

        await stream_handler.send_completion_message()


async def main_handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """Main handler for processing incoming requests."""
    conn = None
    try:
        # Validate request
        route_key = event.get('routeKey', '')
        try:
            http_method, path = route_key.split(' ', 1)
        except ValueError:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Bad Request'})}

        if http_method.upper() != 'GET' or path != '/data':
            return {'statusCode': 405, 'body': json.dumps({'error': 'Method Not Allowed'})}

        # Authenticate request
        auth_handler = AuthenticationHandler(event)
        try:
            client_id = auth_handler.validate_token()
        except ValueError as e:
            return {'statusCode': 401, 'body': json.dumps({'error': f'Unauthorized: {str(e)}'})}

        # Get and validate query
        query_param = event.get('queryStringParameters', {}).get('query')
        if not query_param:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Missing query parameter'})}

        query = unquote_plus(query_param)
        if not SQLValidator.is_query_safe(query):
            return {'statusCode': 400, 'body': json.dumps({'error': 'Unsafe query'})}

        query_with_limit = SQLValidator.add_limit_if_needed(query)

        # Get active connections
        connection_ids = await ConnectionManager.get_client_connections(client_id)
        if not connection_ids:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No active WebSocket connections for client'})}

        # Set up API Gateway management
        endpoint = f"https://{os.environ['WEBSOCKET_API_ID']}.execute-api.{os.environ['REGION']}.amazonaws.com/{os.environ['WEBSOCKET_STAGE']}"
        session = aioboto3.Session()

        # Initialize database connection
        conn = await asyncpg.connect(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            database=os.environ['DB_NAME']
        )

        # Stream data
        async with session.client('apigatewaymanagementapi', endpoint_url=endpoint) as apigw_management_api:
            websocket_manager = WebSocketManager(apigw_management_api)
            stream_handler = DataStreamHandler(websocket_manager, connection_ids)
            await stream_data(conn, query_with_limit, stream_handler)

        return {'statusCode': 200, 'body': json.dumps({'message': 'Data sent successfully'})}

    except asyncpg.PostgresError as e:
        print(f"Database error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': f'Database error: {str(e)}'})}
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
    finally:
        if conn:
            await conn.close()


def lambda_handler(event, context):
    start_time = time.perf_counter()
    try:
        request_context = event.get('requestContext', {})
        if 'http' in request_context:
            return asyncio.run(main_handler(event))
        return {'statusCode': 400, 'body': 'Invalid request'}
    finally:
        end_time = time.perf_counter()
        print(f"Lambda execution time: {end_time - start_time:.6f} seconds")