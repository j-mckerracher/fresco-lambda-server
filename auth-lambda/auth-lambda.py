import json
import os
import jwt
import time


def lambda_handler(event, context):
    print("Lambda handler invoked.")
    print(f"Received event: {json.dumps(event)}")
    print(f"Received context: {context}")

    # Extract the relevant part of the path, ignoring the stage name
    path = event.get('rawPath', '').replace(f"/{os.environ.get('STAGE', 'prod')}", '')
    method = event.get('requestContext', {}).get('http', {}).get('method', 'GET')
    print(f"Request path: {path}")
    print(f"Request method: {method}")

    if path == '/ws-url' and method == 'GET':
        print("Handling /ws-url GET request.")
        return ws_url_handler(event)
    elif path == '/get-jwt' and method == 'POST':
        print("Handling /get-jwt POST request.")
        return generate_jwt_handler(event)
    else:
        print("Invalid path or method. Returning 404.")
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Not Found'})
        }


def ws_url_handler(event):
    print("Entered ws_url_handler function.")
    websocket_url = f"wss://{os.environ['WEBSOCKET_API_ID']}.execute-api.{os.environ['REGION']}.amazonaws.com/{os.environ['WEBSOCKET_STAGE']}"
    print(f"Generated WebSocket URL: {websocket_url}")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({'websocket_url': websocket_url})
    }


def generate_jwt_handler(event):
    print("Entered generate_jwt_handler function.")
    body = event.get('body', '{}')
    print(f"Received body: {body}")

    try:
        body = json.loads(body)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON body: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid JSON format'})
        }

    client_id = body.get('clientId')
    print(f"Extracted clientId: {client_id}")

    if not client_id:
        print("Missing clientId in request.")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing clientId'})
        }

    token = generate_jwt(client_id)
    print(f"Generated JWT token: {token}")

    return {
        'statusCode': 200,
        'body': json.dumps({'token': token})
    }


def generate_jwt(client_id: str) -> str:
    print(f"Generating JWT for clientId: {client_id}")
    payload = {
        'clientId': client_id,
        'iss': os.environ['JWT_ISSUER'],
        'exp': int(time.time()) + 3600  # Expires in 1 hour
    }
    print(f"JWT payload: {payload}")

    try:
        token = jwt.encode(payload, os.environ['JWT_SECRET'], algorithm='HS256')
        print(f"JWT successfully encoded: {token}")
    except Exception as e:
        print(f"Error encoding JWT: {str(e)}")
        raise

    return token
