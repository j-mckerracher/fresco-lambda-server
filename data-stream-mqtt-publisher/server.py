import json
import os
import boto3
import base64
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from typing import Dict, Any
import zlib

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024
MAX_PUBLISHER_THREADS = 10
MAX_RETRIES = 3

# Initialize clients
try:
    iot_client = boto3.client('iot-data',
                              endpoint_url=f"https://{os.environ['IOT_ENDPOINT']}")
    s3 = boto3.client('s3')
    IOT_TOPIC = os.environ['IOT_TOPIC']
    print("AWS clients initialized successfully")
except Exception as e:
    print(f"Error initializing AWS clients: {e}")
    iot_client = None
    s3 = None


class FastPublisher:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=MAX_PUBLISHER_THREADS)
        self.sequence_number = 0
        self.sequence_lock = threading.Lock()
        self.transfer_stats: Dict[str, Dict] = {}
        self.stats_lock = threading.Lock()
        self.preview_shown = False

    def _get_next_sequence(self, transfer_id: str) -> int:
        with self.sequence_lock:
            if transfer_id not in self.transfer_stats:
                self.transfer_stats[transfer_id] = {
                    'sequence': 0,
                    'published': 0,
                    'bytes_published': 0
                }
            self.transfer_stats[transfer_id]['sequence'] += 1
            return self.transfer_stats[transfer_id]['sequence']

    def _update_stats(self, transfer_id: str, data_size: int):
        with self.stats_lock:
            self.transfer_stats[transfer_id]['published'] += 1
            self.transfer_stats[transfer_id]['bytes_published'] += data_size

    def _show_preview(self, metadata: Dict, data_size: int):
        """Show preview of first message being published"""
        if self.preview_shown:
            return

        print("\n=== Publisher Preview ===")
        print(f"Transfer ID: {metadata['transfer_id']}")
        print(f"Partition ID: {metadata['partition_id']}")
        print(f"Order: {metadata['order']}")
        print(f"Data size: {data_size:,} bytes")
        print(f"IoT Topic: {IOT_TOPIC}")
        print("=== End Preview ===\n")
        self.preview_shown = True

    def _chunk_data(self, data: bytes, metadata: Dict) -> list:
        """Split data into MQTT-compatible chunks"""
        # Create test metadata to verify size
        test_metadata = {
            **metadata,
            'sequence': 0,
            'chunk_index': 0,
            'total_chunks': 1,
            'final': False
        }

        # Calculate max data size accounting for metadata and base64 encoding
        metadata_size = len(json.dumps(test_metadata).encode('utf-8'))
        max_data_size = int((MAX_MQTT_PAYLOAD_SIZE - metadata_size - 100) * 0.75)  # Account for base64 expansion

        chunks = []
        for i in range(0, len(data), max_data_size):
            chunk = data[i:i + max_data_size]
            chunks.append(chunk)

        return chunks

    def _publish_message(self, message: Dict, retry_count: int = 0):
        """Publish message to IoT Core with retries"""
        try:
            payload = json.dumps(message)
            payload_size = len(payload.encode('utf-8'))

            if payload_size > MAX_MQTT_PAYLOAD_SIZE:
                raise ValueError(f"Payload too large: {payload_size} bytes")

            iot_client.publish(
                topic=IOT_TOPIC,
                qos=1,
                payload=payload
            )

            self._update_stats(
                message['metadata']['transfer_id'],
                len(message['data']) * 3 // 4  # Estimate original data size from base64
            )

            # Log every 100th message or first message of each partition
            if (message['metadata']['sequence'] % 100 == 0 or
                    message['metadata']['sequence'] == 1):
                print(f"Published message: transfer={message['metadata']['transfer_id']}, "
                      f"order={message['metadata']['order']}, "
                      f"sequence={message['metadata']['sequence']}, "
                      f"size={payload_size:,} bytes")

        except Exception as e:
            if retry_count < MAX_RETRIES:
                time.sleep(0.1 * (retry_count + 1))
                self._publish_message(message, retry_count + 1)
            else:
                raise Exception(f"Failed to publish after {MAX_RETRIES} attempts: {str(e)}")

    def publish_data(self, data: bytes, metadata: Dict):
        """Process and publish data"""
        # Show preview of first message
        self._show_preview(metadata, len(data))

        # Split data into MQTT-compatible chunks
        chunks = self._chunk_data(data, metadata)
        total_chunks = len(chunks)
        transfer_id = metadata['transfer_id']

        futures = []
        for chunk_index, chunk in enumerate(chunks):
            sequence = self._get_next_sequence(transfer_id)

            # Preserve all metadata and add chunking info
            chunk_metadata = {
                **metadata,  # This includes the 'order' field
                'sequence': sequence,
                'chunk_index': chunk_index,
                'total_chunks': total_chunks,
                'final': metadata.get('is_final', False) and chunk_index == total_chunks - 1,
                'timestamp': int(time.time() * 1000)
            }

            message = {
                'type': 'arrow_data',
                'metadata': chunk_metadata,
                'data': base64.b64encode(chunk).decode('utf-8')
            }

            futures.append(self.executor.submit(self._publish_message, message))

        # Wait for all publishes to complete
        for future in futures:
            future.result()  # This will raise any exceptions that occurred

    def get_stats(self, transfer_id: str) -> Dict:
        """Get publishing statistics for a transfer"""
        with self.stats_lock:
            stats = self.transfer_stats.get(transfer_id, {
                'sequence': 0,
                'published': 0,
                'bytes_published': 0
            }).copy()
            return stats


def lambda_handler(event, context):
    publisher = FastPublisher()
    start_time = time.time()

    try:
        for record in event['Records']:
            message = json.loads(record['body'])

            # Handle data from SQS
            if 's3_reference' in message:
                # Get data from S3
                s3_response = s3.get_object(
                    Bucket=message['s3_reference']['bucket'],
                    Key=message['s3_reference']['key']
                )
                data = s3_response['Body'].read()
            else:
                # Data is directly in the message
                data = bytes.fromhex(message['data'])

            # Publish the data
            publisher.publish_data(data, {
                k: v for k, v in message.items()
                if k != 'data' and k != 's3_reference'
            })

        # Generate detailed completion stats
        execution_time = time.time() - start_time
        stats = {
            transfer_id: publisher.get_stats(transfer_id)
            for transfer_id in publisher.transfer_stats
        }

        print("\nExecution completed successfully:")
        print(f"✓ Time taken: {execution_time:.2f} seconds")
        print(f"✓ Records processed: {len(event['Records'])}")
        for transfer_id, transfer_stats in stats.items():
            print(f"✓ Transfer {transfer_id}:")
            print(f"  - Messages published: {transfer_stats['published']}")
            print(f"  - Total data: {transfer_stats['bytes_published']:,} bytes")

        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully processed all records',
                'record_count': len(event['Records']),
                'execution_time': execution_time,
                'stats': stats
            }
        }

    except Exception as e:
        execution_time = time.time() - start_time
        error_message = str(e)

        print(f"\nError occurred after {execution_time:.2f} seconds:")
        print(f"✗ Error: {error_message}")

        return {
            'statusCode': 500,
            'body': {
                'error': error_message,
                'execution_time': execution_time,
                'stats': {
                    transfer_id: publisher.get_stats(transfer_id)
                    for transfer_id in publisher.transfer_stats
                }
            }
        }
    finally:
        publisher.executor.shutdown(wait=True)