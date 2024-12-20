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
PUBLISH_RATE_LIMIT = 90  # publishes per second
MINIMUM_PUBLISH_INTERVAL = 1.0 / PUBLISH_RATE_LIMIT

print(f"Starting application with configuration:")
print(f"MAX_MQTT_PAYLOAD_SIZE: {MAX_MQTT_PAYLOAD_SIZE:,} bytes")

# Initialize clients
try:
    iot_client = boto3.client('iot-data',
                              endpoint_url=f"https://{os.environ['IOT_ENDPOINT']}")
    s3 = boto3.client('s3')
    IOT_TOPIC = os.environ['IOT_TOPIC']
    print(f"AWS clients initialized successfully:")
    print(f"- IoT Endpoint: {os.environ['IOT_ENDPOINT']}")
    print(f"- IoT Topic: {IOT_TOPIC}")
except Exception as e:
    print(f"CRITICAL ERROR: Failed to initialize AWS clients: {str(e)}")
    print(f"Environment variables available: {list(os.environ.keys())}")
    iot_client = None
    s3 = None


class RateLimitedPublisher:
    def __init__(self):
        print(f"Initializing RateLimitedPublisher")
        print(f"Target rate: {PUBLISH_RATE_LIMIT} messages per second")
        print(f"Minimum interval: {MINIMUM_PUBLISH_INTERVAL:.4f} seconds")

        self.sequence_number = 0
        self.sequence_lock = threading.Lock()
        self.transfer_stats: Dict[str, Dict] = {}
        self.stats_lock = threading.Lock()
        self.last_publish_time = 0
        self.preview_shown = False

    def _get_next_sequence(self, transfer_id: str) -> int:
        with self.sequence_lock:
            if transfer_id not in self.transfer_stats:
                print(f"Initializing stats for new transfer_id: {transfer_id}")
                self.transfer_stats[transfer_id] = {
                    'sequence': 0,
                    'published': 0,
                    'bytes_published': 0,
                    'start_time': time.time()
                }
            self.transfer_stats[transfer_id]['sequence'] += 1
            return self.transfer_stats[transfer_id]['sequence']

    def _update_stats(self, transfer_id: str, data_size: int):
        with self.stats_lock:
            self.transfer_stats[transfer_id]['published'] += 1
            self.transfer_stats[transfer_id]['bytes_published'] += data_size

            stats = self.transfer_stats[transfer_id]
            elapsed_time = time.time() - stats['start_time']
            current_rate = stats['published'] / elapsed_time if elapsed_time > 0 else 0

            if self.transfer_stats[transfer_id]['published'] % 50 == 0:
                print(f"\nTransfer progress - ID: {transfer_id}")
                print(f"- Messages published: {stats['published']}")
                print(f"- Total bytes: {stats['bytes_published']:,}")
                print(f"- Current publish rate: {current_rate:.1f} messages/second")
                print(f"- Elapsed time: {elapsed_time:.1f} seconds")

    def _chunk_data(self, data: bytes, metadata: Dict) -> list:
        """Split data into MQTT-compatible chunks"""
        test_metadata = {
            **metadata,
            'sequence': 0,
            'chunk_index': 0,
            'total_chunks': 1,
            'final': False
        }

        metadata_size = len(json.dumps(test_metadata).encode('utf-8'))
        max_data_size = int((MAX_MQTT_PAYLOAD_SIZE - metadata_size - 100) * 0.75)

        print(f"\nChunking data for transfer {metadata['transfer_id']}:")
        print(f"- Total data size: {len(data):,} bytes")
        print(f"- Metadata size: {metadata_size:,} bytes")
        print(f"- Max chunk size: {max_data_size:,} bytes")

        chunks = []
        for i in range(0, len(data), max_data_size):
            chunk = data[i:i + max_data_size]
            chunks.append(chunk)

        print(f"- Created {len(chunks)} chunks")
        return chunks

    def _enforce_rate_limit(self):
        """Enforce publish rate limit using sleep"""
        current_time = time.time()
        time_since_last_publish = current_time - self.last_publish_time

        if time_since_last_publish < MINIMUM_PUBLISH_INTERVAL:
            sleep_time = MINIMUM_PUBLISH_INTERVAL - time_since_last_publish
            time.sleep(sleep_time)

        self.last_publish_time = time.time()

    def _publish_message(self, message: Dict, retry_count: int = 0):
        """Publish message to IoT Core with rate limiting and retries"""
        try:
            self._enforce_rate_limit()

            payload = json.dumps(message)
            payload_size = len(payload.encode('utf-8'))

            if payload_size > MAX_MQTT_PAYLOAD_SIZE:
                print(f"ERROR: Payload size {payload_size:,} bytes exceeds maximum {MAX_MQTT_PAYLOAD_SIZE:,} bytes")
                raise ValueError(f"Payload too large: {payload_size:,} bytes")

            start_time = time.time()
            iot_client.publish(
                topic=IOT_TOPIC,
                qos=1,
                payload=payload
            )
            publish_time = time.time() - start_time

            self._update_stats(
                message['metadata']['transfer_id'],
                len(message['data']) * 3 // 4
            )

            if (message['metadata']['sequence'] % 50 == 0 or
                    message['metadata']['sequence'] == 1):
                print(f"\nMessage published successfully:")
                print(f"- Transfer ID: {message['metadata']['transfer_id']}")
                print(f"- Sequence: {message['metadata']['sequence']}")
                print(f"- Size: {payload_size:,} bytes")
                print(f"- Publish time: {publish_time:.3f} seconds")

        except Exception as e:
            print(f"\nERROR: Publish failed (attempt {retry_count + 1}/{MAX_RETRIES + 1}):")
            print(f"- Transfer ID: {message['metadata']['transfer_id']}")
            print(f"- Sequence: {message['metadata']['sequence']}")
            print(f"- Error: {str(e)}")

            if retry_count < MAX_RETRIES:
                sleep_time = 0.1 * (retry_count + 1)
                print(f"- Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
                self._publish_message(message, retry_count + 1)
            else:
                raise Exception(f"Failed to publish after {MAX_RETRIES} attempts: {str(e)}")

    def publish_data(self, data: bytes, metadata: Dict):
        """Process and publish data with rate limiting"""
        print(f"\nStarting new data publication:")
        print(f"- Transfer ID: {metadata['transfer_id']}")
        print(f"- Data size: {len(data):,} bytes")

        chunks = self._chunk_data(data, metadata)
        total_chunks = len(chunks)
        transfer_id = metadata['transfer_id']

        print(f"\nStarting publish process:")
        print(f"- Total chunks: {total_chunks}")
        print(f"- Target rate: {PUBLISH_RATE_LIMIT} messages/second")

        for chunk_index, chunk in enumerate(chunks):
            sequence = self._get_next_sequence(transfer_id)

            chunk_metadata = {
                **metadata,
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

            self._publish_message(message)

    def get_stats(self, transfer_id: str) -> Dict:
        """Get publishing statistics for a transfer"""
        with self.stats_lock:
            stats = self.transfer_stats.get(transfer_id, {
                'sequence': 0,
                'published': 0,
                'bytes_published': 0,
                'start_time': time.time()
            }).copy()

            elapsed_time = time.time() - stats['start_time']
            stats['publish_rate'] = stats['published'] / elapsed_time if elapsed_time > 0 else 0

            return stats


def lambda_handler(event, context):
    print("\n=== Lambda Handler Started ===")
    print(f"Event Records: {len(event['Records'])}")

    # Constants
    MAX_BATCH_SIZE = 100
    SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

    start_time = time.time()
    publisher = RateLimitedPublisher()
    sqs = boto3.client('sqs')

    try:
        # Split records into current batch and remaining
        current_batch = event['Records'][:MAX_BATCH_SIZE]
        remaining_records = event['Records'][MAX_BATCH_SIZE:]

        print(f"\nProcessing batch of {len(current_batch)} records")
        print(f"Remaining records to be requeued: {len(remaining_records)}")

        # Process current batch
        for index, record in enumerate(current_batch, 1):
            print(f"\nProcessing record {index}/{len(current_batch)}")
            message = json.loads(record['body'])

            # Get data either from S3 or direct message
            if 's3_reference' in message:
                print(f"Fetching data from S3:")
                print(f"- Bucket: {message['s3_reference']['bucket']}")
                print(f"- Key: {message['s3_reference']['key']}")

                start_fetch = time.time()
                s3_response = s3.get_object(
                    Bucket=message['s3_reference']['bucket'],
                    Key=message['s3_reference']['key']
                )
                data = s3_response['Body'].read()
                fetch_time = time.time() - start_fetch

                print(f"- Fetch completed in {fetch_time:.2f} seconds")
                print(f"- Data size: {len(data):,} bytes")
            else:
                print("Using direct data from message")
                data = bytes.fromhex(message['data'])
                print(f"- Data size: {len(data):,} bytes")

            # Publish the data
            publisher.publish_data(data, {
                k: v for k, v in message.items()
                if k != 'data' and k != 's3_reference'
            })

        # Requeue remaining records if any
        if remaining_records:
            print(f"\nRequeueing {len(remaining_records)} remaining records")
            for record in remaining_records:
                try:
                    sqs.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=record['body'],
                        MessageAttributes=record.get('messageAttributes', {})
                    )
                except Exception as e:
                    print(f"Error requeueing message: {str(e)}")
                    raise

            print(f"Successfully requeued {len(remaining_records)} records")

        # Calculate execution statistics
        execution_time = time.time() - start_time
        stats = {
            transfer_id: publisher.get_stats(transfer_id)
            for transfer_id in publisher.transfer_stats
        }

        # Log execution summary
        print("\n=== Execution Summary ===")
        print(f"✓ Time taken: {execution_time:.2f} seconds")
        print(f"✓ Records processed in this batch: {len(current_batch)}")
        print(f"✓ Records requeued: {len(remaining_records)}")

        for transfer_id, transfer_stats in stats.items():
            print(f"\n✓ Transfer {transfer_id}:")
            print(f"  - Messages published: {transfer_stats['published']}")
            print(f"  - Total data: {transfer_stats['bytes_published']:,} bytes")
            print(
                f"  - Average throughput: {transfer_stats['bytes_published'] / execution_time / 1024 / 1024:.2f} MB/s"
            )

        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully processed batch',
                'records_processed': len(current_batch),
                'records_requeued': len(remaining_records),
                'execution_time': execution_time,
                'stats': stats
            }
        }

    except Exception as e:
        execution_time = time.time() - start_time
        error_message = str(e)

        print("\n=== Execution Failed ===")
        print(f"✗ Error occurred after {execution_time:.2f} seconds:")
        print(f"✗ Error message: {error_message}")

        print("\nPartial stats at time of failure:")
        for transfer_id in publisher.transfer_stats:
            stats = publisher.get_stats(transfer_id)
            print(f"- Transfer {transfer_id}:")
            print(f"  • Messages published: {stats['published']}")
            print(f"  • Total data: {stats['bytes_published']:,} bytes")

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
        print("\n=== Shutting down publisher ===")
        publisher.executor.shutdown(wait=True)
        print("Publisher shutdown complete")