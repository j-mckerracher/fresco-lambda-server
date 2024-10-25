import os
import aioboto3
import jwt
import asyncio
import time  # Ensure time module is imported


def lambda_handler(event, context):
    print("Lambda handler invoked.")
    print(f"Event received: {event}")
    print(f"Context received: {context}")

    request_context = event.get('requestContext', {})
    print(f"Request context: {request_context}")

    if 'routeKey' in request_context:
        route_key = request_context['routeKey']
        print(f"WebSocket route key: {route_key}")

        if route_key == '$connect':
            print("Handling WebSocket $connect route.")
            return handle_connect(event)
        elif route_key == '$disconnect':
            print("Handling WebSocket $disconnect route.")
            return handle_disconnect(event)
        elif route_key == '$default':
            print("Handling WebSocket $default route.")
            return handle_default(event)
        else:
            print(f"Invalid route key: {route_key}")
            return {
                'statusCode': 400,
                'body': 'Invalid route'
            }
    else:
        print("Not a WebSocket request.")
        return {
            'statusCode': 400,
            'body': 'Not a WebSocket request'
        }


def handle_connect(event):
    print("Entered handle_connect function.")
    connection_id = event['requestContext']['connectionId']
    print(f"Connection ID: {connection_id}")

    # Extract clientId and token from query parameters
    query_params = event.get('queryStringParameters', {})
    print(f"Query parameters: {query_params}")
    client_id = query_params.get('clientId')
    token = query_params.get('token')

    if not client_id or not token:
        print("Missing clientId or token.")
        return {
            'statusCode': 401,
            'body': 'Unauthorized: Missing clientId or token'
        }

    # Validate JWT
    try:
        print("Decoding JWT token.")
        decoded_token = jwt.decode(
            token,
            os.environ['JWT_SECRET'],
            algorithms=['HS256'],
            issuer=os.environ['JWT_ISSUER']
        )
        print(f"Decoded JWT token: {decoded_token}")

        if decoded_token.get('clientId') != client_id:
            print("clientId in token does not match query parameter.")
            raise ValueError("clientId does not match token")
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError, ValueError) as e:
        print(f"JWT decoding error: {str(e)}")
        return {
            'statusCode': 401,
            'body': f'Unauthorized: {str(e)}'
        }

    # Store connectionId and clientId in DynamoDB
    print(f"Adding connection {connection_id} for client {client_id} to DynamoDB.")
    asyncio.run(add_connection(connection_id, client_id))

    return {
        'statusCode': 200,
        'body': 'Connected'
    }


def handle_disconnect(event):
    print("Entered handle_disconnect function.")
    connection_id = event['requestContext']['connectionId']
    print(f"Connection ID to disconnect: {connection_id}")

    # Remove connectionId from DynamoDB
    print(f"Removing connection {connection_id} from DynamoDB.")
    asyncio.run(remove_connection(connection_id))

    return {
        'statusCode': 200,
        'body': 'Disconnected'
    }


def handle_default(event):
    print("Entered handle_default function.")
    return {
        'statusCode': 200,
        'body': 'Message received'
    }


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
        print(f"Connection {connection_id} added to DynamoDB successfully.")


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
        print(f"Deleting connection {connection_id} from DynamoDB table.")
        await table.delete_item(
            Key={
                'connectionId': connection_id
            }
        )
        print(f"Connection {connection_id} removed from DynamoDB successfully.")
