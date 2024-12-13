import json
import os
import time
import base64
import traceback
import zlib
import asyncio
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Tuple, Dict, Any
import boto3
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxTable
import pyarrow as pa
import pyarrow.ipc as ipc
from botocore.exceptions import ClientError
from botocore.config import Config

# Constants
MAX_MQTT_PAYLOAD_SIZE = 128 * 1024
MAX_DATA_SIZE = int(MAX_MQTT_PAYLOAD_SIZE * 0.9)
DEFAULT_BATCH_SIZE = 5000
DEFAULT_ROW_LIMIT = 1000000
MAX_PUBLISHER_THREADS = 10
COMPRESSION_LEVEL = 1
PARALLEL_QUERIES = 12
QUEUE_SIZE = 100
MAX_EXECUTION_TIME = 55

import boto3
import json
import os
from botocore.exceptions import ClientError
from botocore.config import Config
from influxdb_client import InfluxDBClient


def get_secret():
    secret_name = "READONLY-InfluxDB-auth-parameters-uhibo081q6"
    region_name = "us-east-1"
    print(f"Attempting to retrieve secret '{secret_name}' from region '{region_name}'")

    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name,
        config=Config(region_name=region_name)
    )
    print("AWS Secrets Manager client initialized")

    try:
        print("Requesting secret value...")
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        print("Secret retrieved successfully")

        secret_string = get_secret_value_response['SecretString']
        secret_data = json.loads(secret_string)
        print("Secret data parsed successfully")
        return secret_data
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"Failed to retrieve secret. Error code: {error_code}")
        print(f"Error message: {error_message}")
        raise e


# Initialize AWS IoT client
try:
    print("\n=== Initializing AWS IoT Client ===")
    print("Reading environment variables...")
    iot_endpoint = os.environ['IOT_ENDPOINT']
    iot_topic = os.environ['IOT_TOPIC']
    region = os.environ.get('REGION', 'us-east-1')
    print(f"Environment variables retrieved:")
    print(f"  - IOT_ENDPOINT: {iot_endpoint}")
    print(f"  - IOT_TOPIC: {iot_topic}")
    print(f"  - REGION: {region}")

    endpoint_url = f"https://{iot_endpoint}"
    print(f"Initializing IoT client with endpoint URL: {endpoint_url}")

    iot_client = boto3.client('iot-data', region_name=region, endpoint_url=endpoint_url)
    print("AWS IoT Data client initialized successfully.")
except KeyError as e:
    print(f"Missing required environment variable: {e}")
    iot_client = None
except Exception as e:
    print(f"Error initializing AWS IoT Data client: {str(e)}")
    print(f"Exception type: {type(e).__name__}")
    iot_client = None

# Initialize InfluxDB client
try:
    print("\n=== Initializing InfluxDB Client ===")
    print("Retrieving secrets from AWS Secrets Manager...")
    secrets = get_secret()
    password = secrets['password']
    username = 'giGvXg34nFmZsYyV7ed3'

    INFLUXDB_URL = "https://fresco-keeqmd5mu25lsf.us-east-1.timestream-influxdb.amazonaws.com:8086"
    ORG = "fresco"

    print(f"Connecting to InfluxDB:")
    print(f"  - URL: {INFLUXDB_URL}")
    print(f"  - Organization: {ORG}")
    print(f"  - Username: {username}")
    print("  - Password: ********")

    # Create client with custom timeout settings
    influx_client = InfluxDBClient(
        url=INFLUXDB_URL,
        username=username,
        password=password,
        org=ORG,
        timeout=60000,  # Increase from 30000 to 60000
        pool_settings=dict(
            timeout=60.0,  # Increase from 30.0 to 60.0
            connect_timeout=60.0,  # Increase from 30.0 to 60.0
            read_timeout=60.0  # Increase from 30.0 to 60.0
        )
    )

    # Configure query client with specific timeouts
    # query_api = influx_client.query_api(
    #     query_options=dict(
    #         max_retry_time=30_000,  # Maximum retry time in milliseconds
    #         retry_interval=1_000,  # Initial retry interval in milliseconds
    #         max_retries=3  # Maximum number of retries
    #     )
    # )

    print("InfluxDB client initialized successfully with custom timeouts:")
    print("  - Overall timeout: 30 seconds")
    print("  - Connection timeout: 30 seconds")
    print("  - Read timeout: 30 seconds")
    print("  - Max retries: 3")
    print("  - Connection pool size: 25")

except KeyError as e:
    print(f"Missing required secret key: {e}")
    influx_client = None
except Exception as e:
    print(f"Error initializing InfluxDB client: {str(e)}")
    print(f"Exception type: {type(e).__name__}")
    influx_client = None


@dataclass
class QueryPartition:
    start_time: str
    end_time: str
    batch_size: int


def create_response(
        transfer_id: str,
        row_count: int = 0,
        chunk_count: int = 0,
        schema_info: Dict = None,
        time_elapsed: float = 0,
        total_rows: int = 0,
        published_messages: int = 0,
        status: str = "COMPLETED",
        error: str = None
) -> Dict[str, Any]:
    return {
        "transferId": transfer_id,
        "metadata": {
            "rowCount": row_count,
            "chunkCount": chunk_count,
            "schema": json.dumps(schema_info) if schema_info else None,
            "isComplete": status == "COMPLETED",
            "timeElapsed": time_elapsed,
            "totalRows": total_rows,
            "publishedMessageCount": published_messages,
            "processingStatus": status,
            "error": error
        }
    }


async def get_schema(query: str) -> Tuple[pa.Schema, Dict]:
    """Get Arrow schema from InfluxDB query result"""
    print(f"\n=== Starting Schema Inference ===")
    print(f"Original query: {query}")

    query_api = influx_client.query_api()
    print("Query API client initialized")

    # Execute query with limit 1 to get structure
    sample_query = f'{query} |> limit(n: 1)'
    print(f"Executing sample query: {sample_query}")

    try:
        tables = query_api.query(sample_query)
        print(f"Query executed successfully")
        print(f"Number of tables returned: {len(tables)}")

        if not tables:
            print("WARNING: Query returned no data")
            raise ValueError("No data returned from query")

        first_table = tables[0]
        record_count = len(first_table.records)
        print(f"First table contains {record_count} records")

        first_record = first_table.records[0]
        print("Successfully retrieved first record for schema inference")

        fields = []
        schema_info = {}

        # Add time field
        print("\nBuilding schema:")
        print("Adding timestamp field")
        fields.append(pa.field('time', pa.timestamp('ns')))
        schema_info['time'] = 'timestamp'

        # Add other fields based on values
        print("\nProcessing data fields:")
        for key, value in first_record.values.items():
            if key != 'time':
                print(f"\nField: {key}")
                print(f"  Value type: {type(value).__name__}")

                if isinstance(value, float):
                    print("  Inferring as float64")
                    fields.append(pa.field(key, pa.float64()))
                    schema_info[key] = "float"
                elif isinstance(value, int):
                    print("  Inferring as int64")
                    fields.append(pa.field(key, pa.int64()))
                    schema_info[key] = "integer"
                else:
                    print("  Inferring as string")
                    fields.append(pa.field(key, pa.string()))
                    schema_info[key] = "string"

                print(f"  Added to schema with type: {schema_info[key]}")

        final_schema = pa.schema(fields)
        print("\nSchema inference completed successfully")
        print(f"Total fields in schema: {len(fields)}")
        print("Field summary:")
        for field in fields:
            print(f"  - {field.name}: {field.type}")

        return final_schema, schema_info

    except ValueError as e:
        print(f"ValueError during schema inference: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during schema inference: {str(e)}")
        print(f"Exception type: {type(e).__name__}")
        raise


class FastParallelPublisher:
    def __init__(self, iot_client, topic: str, transfer_id: str):
        print(f"\n=== Initializing FastParallelPublisher ===")
        print(f"Topic: {topic}")
        print(f"Transfer ID: {transfer_id}")
        print(f"Max workers: {MAX_PUBLISHER_THREADS}")
        print(f"Queue size: {QUEUE_SIZE}")
        print(f"Max chunk size: {int((MAX_MQTT_PAYLOAD_SIZE * 0.6) - 2048)} bytes")

        self.iot_client = iot_client
        self.topic = topic
        self.transfer_id = transfer_id
        self.sequence_number = 0
        self.sequence_lock = threading.Lock()
        self.publish_queue = Queue(maxsize=QUEUE_SIZE)
        self.executor = ThreadPoolExecutor(max_workers=MAX_PUBLISHER_THREADS)
        self.publish_count = 0
        self.is_running = True
        self.error = None
        self.max_chunk_size = int((MAX_MQTT_PAYLOAD_SIZE * 0.6) - 2048)
        self.futures = [self.executor.submit(self._publish_worker)
                        for _ in range(MAX_PUBLISHER_THREADS)]
        self.rows_published = 0
        self.preview_shown = False
        self.preview_rows = []
        print("Publisher initialized successfully")

    def _estimate_message_size(self, data: bytes, metadata: dict) -> int:
        encoded_size = len(data) * 4 // 3
        metadata_size = len(json.dumps(metadata))
        total_size = encoded_size + metadata_size + 100
        print(f"Estimated message size: {total_size:,} bytes")
        return total_size

    def _chunk_data(self, data: bytes) -> list:
        print(f"\nChunking data of size: {len(data):,} bytes")
        chunks = []
        for i in range(0, len(data), self.max_chunk_size):
            chunk = data[i:i + self.max_chunk_size]
            test_metadata = {
                'transfer_id': self.transfer_id,
                'sequence': 0,
                'partition': 0,
                'chunk_index': 0,
                'total_chunks': 1,
                'final': False,
                'timestamp': int(time.time() * 1000)
            }

            while self._estimate_message_size(chunk, test_metadata) >= MAX_MQTT_PAYLOAD_SIZE:
                original_size = len(chunk)
                chunk = chunk[:int(len(chunk) * 0.8)]
                print(f"Reducing chunk size from {original_size:,} to {len(chunk):,} bytes")

            chunks.append(chunk)

        print(f"Data split into {len(chunks)} chunks")
        return chunks

    def _publish_worker(self):
        print(f"Started publisher worker thread: {threading.current_thread().name}")
        while self.is_running:
            try:
                batch_item = self.publish_queue.get(timeout=0.1)
                if batch_item is None:
                    print(f"Worker {threading.current_thread().name} received shutdown signal")
                    break

                data, is_final, partition_id, row_count = batch_item
                self.rows_published += row_count

                if self.rows_published % 10000 == 0:
                    print(f"Progress: Published {self.rows_published:,} rows")
                    print(f"Queue size: {self.publish_queue.qsize()}")

                chunks = self._chunk_data(data)
                total_chunks = len(chunks)

                for chunk_index, chunk in enumerate(chunks):
                    sequence = self._get_next_sequence()
                    metadata = {
                        'transfer_id': self.transfer_id,
                        'sequence': sequence,
                        'partition': partition_id,
                        'chunk_index': chunk_index,
                        'total_chunks': total_chunks,
                        'final': is_final and chunk_index == total_chunks - 1,
                        'timestamp': int(time.time() * 1000),
                        'row_count': row_count
                    }

                    message = {
                        'type': 'arrow_data',
                        'metadata': metadata,
                        'data': base64.b64encode(chunk).decode('utf-8')
                    }

                    if sequence == 1 or sequence % 100 == 0:
                        log_message = message.copy()
                        log_message['data'] = log_message['data'][:100] + '...'
                        print(f"\nPublishing message {sequence}:")
                        print(f"Metadata: {json.dumps(metadata, indent=2)}")
                        print(f"Data preview: {log_message['data'][:50]}...")

                    for attempt in range(3):
                        try:
                            payload = json.dumps(message)
                            payload_size = len(payload.encode('utf-8'))
                            print(f"Payload size: {payload_size:,} bytes")

                            if payload_size > MAX_MQTT_PAYLOAD_SIZE:
                                raise Exception(
                                    f"Payload size ({payload_size:,} bytes) exceeds limit ({MAX_MQTT_PAYLOAD_SIZE:,} bytes)")

                            self.iot_client.publish(
                                topic=self.topic,
                                qos=1,
                                payload=payload
                            )
                            self.publish_count += 1
                            break
                        except Exception as e:
                            print(f"Publish attempt {attempt + 1} failed: {str(e)}")
                            if attempt == 2:
                                print(f"Failed to publish after all attempts")
                                self.error = e
                                self.is_running = False
                                raise
                            time.sleep(0.5 * (attempt + 1))

                self.publish_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                print(f"Fatal error in worker {threading.current_thread().name}: {str(e)}")
                print(f"Stack trace: {traceback.format_exc()}")
                self.error = e
                self.is_running = False
                break

    def _get_next_sequence(self):
        with self.sequence_lock:
            self.sequence_number += 1
            return self.sequence_number

    def publish(self, data: bytes, is_final: bool = False, partition_id: int = 0, row_count: int = 0):
        if not self.is_running:
            error_msg = str(self.error) if self.error else "Publisher stopped"
            print(f"Cannot publish - publisher not running: {error_msg}")
            raise self.error if self.error else Exception(error_msg)
        print(f"\nQueuing {row_count} rows for partition {partition_id}")
        self.publish_queue.put((data, is_final, partition_id, row_count))

    def wait_completion(self):
        print("\n=== Shutting down publisher ===")
        print(f"Total messages published: {self.publish_count:,}")
        print(f"Total rows processed: {self.rows_published:,}")

        self.is_running = False
        for _ in range(MAX_PUBLISHER_THREADS):
            self.publish_queue.put(None)

        print("Waiting for worker threads to complete...")
        self.executor.shutdown(wait=True)

        if self.error:
            print(f"Publisher completed with error: {str(self.error)}")
            raise self.error

        print("Publisher shutdown complete")
        return self.publish_count


class OptimizedBatchProcessor:
    def __init__(self, schema: pa.Schema):
        print(f"\n=== Initializing OptimizedBatchProcessor ===")
        print(f"Schema fields: {[field.name for field in schema]}")
        print(f"Field types: {[field.type for field in schema]}")

        self.schema = schema
        self.field_converters = self._create_field_converters()
        self.arrays = [[] for _ in range(len(self.schema))]
        self.preview_sent = False
        self.preview_size = 5
        print("Processor initialized successfully")

    def _create_field_converters(self):
        print("\nCreating field converters:")
        converters = []
        for field in self.schema:
            if pa.types.is_timestamp(field.type):
                print(f"  {field.name}: timestamp -> passthrough")
                converters.append(lambda x: x)
            elif field.name == 'value_gpu' or pa.types.is_floating(field.type):
                print(f"  {field.name}: -> float")
                converters.append(lambda x: float(x) if x is not None else 0.0)
            elif pa.types.is_integer(field.type):
                print(f"  {field.name}: -> integer")
                converters.append(lambda x: int(x) if x is not None else 0)
            else:
                print(f"  {field.name}: -> string")
                converters.append(lambda x: str(x) if x is not None else '')
        return converters

    def process_batch(self, records: list) -> bytes:
        print(f"\n=== Processing batch of {len(records)} records ===")

        if not self.preview_sent and records:
            self._show_preview(records)
            self.preview_sent = True

        print("Clearing previous arrays")
        for arr in self.arrays:
            arr.clear()

        print("Converting records to arrays")
        null_counts = [0] * len(self.schema)
        conversion_errors = [0] * len(self.schema)

        for record_idx, record in enumerate(records):
            try:
                values = [record.get_time()]
                values.extend(record.values.get(field.name) for field in self.schema if field.name != 'time')

                for i, (value, converter) in enumerate(zip(values, self.field_converters)):
                    try:
                        if value is None:
                            null_counts[i] += 1
                        converted_value = converter(value)
                        self.arrays[i].append(converted_value)
                    except Exception as e:
                        conversion_errors[i] += 1
                        print(f"Error converting field {self.schema[i].name} at record {record_idx}: {str(e)}")
                        # Use default value based on type
                        self.arrays[i].append(0 if pa.types.is_numeric(self.schema[i].type) else '')

            except Exception as e:
                print(f"Error processing record {record_idx}: {str(e)}")
                continue

        print("\nConversion statistics:")
        for i, field in enumerate(self.schema):
            print(f"  {field.name}:")
            print(f"    Null values: {null_counts[i]}")
            print(f"    Conversion errors: {conversion_errors[i]}")

        print("\nCreating Arrow arrays")
        arrow_arrays = [pa.array(col, type=field.type)
                        for col, field in zip(self.arrays, self.schema)]

        print("Creating record batch")
        record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)

        print("Serializing to IPC format")
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)

        uncompressed_size = sink.getvalue().size
        print(f"Compressing data (Level {COMPRESSION_LEVEL})")
        compressed_data = zlib.compress(sink.getvalue().to_pybytes(), level=COMPRESSION_LEVEL)
        compression_ratio = (1 - len(compressed_data) / uncompressed_size) * 100

        print(f"Batch processing complete:")
        print(f"  Records processed: {len(records)}")
        print(f"  Uncompressed size: {uncompressed_size:,} bytes")
        print(f"  Compressed size: {len(compressed_data):,} bytes")
        print(f"  Compression ratio: {compression_ratio:.1f}%")

        return compressed_data

    def _show_preview(self, records: list):
        preview_records = records[:self.preview_size]
        columns = [field.name for field in self.schema]

        print("\n=== Data Preview ===")
        print(f"Showing first {len(preview_records)} records of {len(records)} in this batch")
        print("\nColumns:", columns)

        row_format = "{:<3} " + " | ".join(["{:<20}"] * len(columns))
        print("\n" + row_format.format("idx", *columns))
        print("-" * (3 + (23 * len(columns))))

        for idx, record in enumerate(preview_records):
            try:
                values = [record.get_time()]
                values.extend(record.values.get(field.name) for field in self.schema if field.name != 'time')
                formatted_values = [str(val)[:20] if val is not None else "NULL" for val in values]
                print(row_format.format(idx, *formatted_values))
            except Exception as e:
                print(f"Error formatting preview record {idx}: {str(e)}")
        print("\n=== End Preview ===\n")


async def process_partition(partition: QueryPartition, publisher: FastParallelPublisher,
                            batch_processor: OptimizedBatchProcessor) -> int:
    print(f"\n=== Processing Partition ===")
    print(f"Start time: {partition.start_time}")
    print(f"End time: {partition.end_time}")
    print(f"Batch size: {partition.batch_size}")

    rows_processed = 0
    query_api = influx_client.query_api()

    partition_query = f'''
    from(bucket: "job_data")
        |> range(start: {partition.start_time}, stop: {partition.end_time})
        |> filter(fn: (r) => r["_measurement"] == "job_metrics")
    '''
    print(f"Executing query:\n{partition_query}")

    try:
        start_time = time.time()
        print("Fetching data from InfluxDB...")
        tables = query_api.query(partition_query)
        print(f"Query execution time: {time.time() - start_time:.2f}s")

        current_batch = []
        batch_count = 0
        total_batch_size = 0

        print("Processing records...")
        for table in tables:
            for record in table.records:
                current_batch.append(record)

                if len(current_batch) >= partition.batch_size:
                    batch_count += 1
                    batch_start = time.time()

                    print(f"\nProcessing batch {batch_count}")
                    batch_data = batch_processor.process_batch(current_batch)
                    batch_size = len(batch_data)
                    total_batch_size += batch_size

                    print(f"Publishing batch (size: {batch_size:,} bytes)")
                    publisher.publish(batch_data, False, int(time.time()), len(current_batch))

                    rows_processed += len(current_batch)
                    batch_time = time.time() - batch_start
                    print(f"Batch processing time: {batch_time:.2f}s")
                    print(f"Processing rate: {len(current_batch) / batch_time:.1f} records/s")

                    current_batch = []

                    if rows_processed % 50000 == 0:
                        avg_batch_size = total_batch_size / batch_count
                        print(f"\nProgress Update:")
                        print(f"  Rows processed: {rows_processed:,}")
                        print(f"  Batches completed: {batch_count}")
                        print(f"  Average batch size: {avg_batch_size:,.0f} bytes")
                        print(f"  Average rows/batch: {rows_processed / batch_count:.1f}")

        if current_batch:
            print("\nProcessing final batch")
            batch_data = batch_processor.process_batch(current_batch)
            print(f"Publishing final batch (size: {len(batch_data):,} bytes)")
            publisher.publish(batch_data, True, int(time.time()), len(current_batch))
            rows_processed += len(current_batch)

        total_time = time.time() - start_time
        print(f"\nPartition processing complete:")
        print(f"  Total rows: {rows_processed:,}")
        print(f"  Total batches: {batch_count + 1}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Overall rate: {rows_processed / total_time:.1f} records/s")

    except Exception as e:
        print(f"\nERROR processing partition {partition.start_time}:")
        print(f"  Error type: {type(e).__name__}")
        print(f"  Error message: {str(e)}")
        print(f"  Stack trace:\n{traceback.format_exc()}")
        raise

    return rows_processed


async def _lambda_handler(event, context):
    start_time = time.time()
    print(f"\n=== Lambda Handler Start ===")
    print(f"Incoming event: {event}")

    try:
        args = event.get('arguments', {})
        if not args:
            raise ValueError("Missing required 'arguments' in request")

        transfer_id = args.get('transferId')
        if not transfer_id:
            raise ValueError("Missing required 'transferId' in arguments")

        query = args.get('query')
        if not query:
            raise ValueError("Missing required 'query' in arguments")

        row_limit = min(args.get('rowLimit', DEFAULT_ROW_LIMIT), DEFAULT_ROW_LIMIT)
        batch_size = min(args.get('batchSize', DEFAULT_BATCH_SIZE), DEFAULT_BATCH_SIZE)

        print(f"\nConfiguration:")
        print(f"Row limit: {row_limit:,}")
        print(f"Batch size: {batch_size:,}")

        if query.lower().strip() == 'select * from job_data;':
            print("Converting simple query to Flux format")
            query = '''
            from(bucket: "job_data")
                |> range(start: 2022-01-01T00:00:00Z)
                |> filter(fn: (r) => r["_measurement"] == "job_metrics")
            '''
        print(f"Final query:\n{query}")

        print("\nInferring schema...")
        schema, schema_info = await get_schema(query)
        print(f"Schema fields: {[f.name for f in schema]}")

        print("\nInitializing publisher and processor...")
        publisher = FastParallelPublisher(iot_client, iot_topic, transfer_id)
        batch_processor = OptimizedBatchProcessor(schema)

        time_ranges = [
            ("2022-01-01T00:00:00Z", "2022-06-30T23:59:59Z"),
            ("2022-07-01T00:00:00Z", "2022-12-31T23:59:59Z"),
            ("2023-01-01T00:00:00Z", "2023-06-30T23:59:59Z"),
            ("2023-07-01T00:00:00Z", "2023-12-31T23:59:59Z"),
            ("2024-01-01T00:00:00Z", "now()")
        ]
        print(f"\nCreating {len(time_ranges)} time-based partitions")

        partitions = [QueryPartition(
            start_time=start,
            end_time=end,
            batch_size=batch_size
        ) for start, end in time_ranges]

        print("\nStarting parallel processing...")
        tasks = [process_partition(p, publisher, batch_processor) for p in partitions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        errors = [r for r in results if isinstance(r, Exception)]
        successful_results = [r for r in results if isinstance(r, int)]
        total_rows = sum(successful_results)

        execution_time = time.time() - start_time
        rows_per_second = int(total_rows / execution_time) if execution_time > 0 else 0
        print(f"\n=== Processing Summary ===")
        print(f"✓ Total rows: {total_rows:,}")
        print(f"✓ Time elapsed: {execution_time:.2f}s")
        print(f"✓ Processing rate: {rows_per_second:,} rows/s")
        print(f"✓ Messages published: {publisher.publish_count if publisher else 0}")

        if errors:
            print(f"\n⚠ Errors ({len(errors)}):")
            for i, error in enumerate(errors, 1):
                print(f"  {i}. {str(error)}")
                print(f"     Type: {type(error).__name__}")

        if errors and total_rows == 0:
            raise errors[0]

        status = "COMPLETED" if total_rows > 0 and not errors else "PARTIAL"
        published_messages = publisher.publish_count if publisher else 0

        response = create_response(
            transfer_id=transfer_id,
            row_count=total_rows,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows,
            published_messages=published_messages,
            status=status
        )
        print("\nResponse:", json.dumps(response, indent=2))
        return response

    except Exception as e:
        error_message = str(e)
        error_type = "TIMEOUT_ERROR" if "timeout" in error_message.lower() else "ERROR"
        execution_time = time.time() - start_time

        print(f"\n=== Error Summary ===")
        print(f"✗ Type: {error_type}")
        print(f"✗ Message: {error_message}")
        print(f"✗ Time elapsed: {execution_time:.2f}s")
        print(f"✗ Rows processed: {total_rows:,}")
        print(f"Stack trace:\n{traceback.format_exc()}")

        response = create_response(
            transfer_id=transfer_id,
            row_count=total_rows,
            chunk_count=publisher.sequence_number if publisher else 0,
            schema_info=schema_info,
            time_elapsed=execution_time,
            total_rows=total_rows,
            published_messages=publisher.publish_count if publisher else 0,
            status=error_type,
            error=error_message
        )
        print("\nError response:", json.dumps(response, indent=2))
        return response

    finally:
        print("\n=== Cleanup ===")
        if publisher:
            try:
                publisher.wait_completion()
                print("Publisher cleanup completed")
            except Exception as e:
                print(f"Publisher cleanup error: {str(e)}")

        influx_client.close()
        print(f"Total execution time: {time.time() - start_time:.2f}s")


def lambda_handler(event, context):
    # Parse the body to get arguments
    if event.get('body'):
        try:
            body = json.loads(event['body'])
            event = body  # Replace event with parsed body
        except json.JSONDecodeError as e:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Invalid JSON in request body: {str(e)}"})
            }

    return asyncio.run(_lambda_handler(event, context))