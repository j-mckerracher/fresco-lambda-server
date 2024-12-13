import json
import os
import time
import zlib
import re
import psycopg2
import boto3
import pyarrow as pa
import pyarrow.ipc as ipc
from typing import List, Dict, Tuple

COMPRESSION_LEVEL = 6  # Balanced between speed and compression
CHUNK_SIZE = 256 * 1024  # 256KB chunks for SQS


class ArrowProcessor:
    def __init__(self, schema):
        self.schema = schema
        self.arrays = [[] for _ in schema]
        self.field_converters = self._create_field_converters()
        self.preview_sent = False
        self.total_chunks_processed = 0
        self.processed_rows = 0
        self.batches_processed = 0

    def _create_field_converters(self):
        """Enhanced field converters with robust type handling"""
        timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})(?:\+\d{2}:?\d{2})?')

        converters = []
        for field in self.schema:
            field_type = field.type

            if pa.types.is_timestamp(field_type):
                def timestamp_converter(value, pattern=timestamp_pattern):
                    if value is None:
                        return None
                    try:
                        if isinstance(value, str):
                            match = pattern.match(value)
                            if match:
                                from datetime import datetime
                                dt = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
                                return int(dt.timestamp() * 1000000)
                        return value
                    except Exception:
                        return None

                converters.append(timestamp_converter)

            elif pa.types.is_floating(field_type):
                def float_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        if isinstance(value, str):
                            clean_value = ''.join(c for c in value if c.isdigit() or c in '.-')
                            return float(clean_value) if clean_value else None
                        return float(value)
                    except Exception as e:
                        print(f"Warning: Could not convert '{value}' to float for {field_name}: {str(e)}")
                        return None

                converters.append(float_converter)

            elif pa.types.is_integer(field_type):
                def int_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        if isinstance(value, str):
                            clean_value = ''.join(c for c in value if c.isdigit() or c == '-')
                            return int(clean_value) if clean_value else None
                        return int(value)
                    except Exception as e:
                        print(f"Warning: Could not convert '{value}' to integer for {field_name}: {str(e)}")
                        return None

                converters.append(int_converter)

            else:  # String/default converter
                def string_converter(value, field_name=field.name):
                    if value is None:
                        return None
                    try:
                        return str(value)
                    except Exception as e:
                        print(f"Warning: Could not convert value to string for {field_name}: {str(e)}")
                        return None

                converters.append(string_converter)

        return converters

    def _chunk_data(self, data: bytes) -> List[bytes]:
        """Split compressed data into SQS-compatible chunks"""
        return [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]

    def get_progress(self) -> Dict:
        """Get current processing progress"""
        return {
            'chunks_processed': self.total_chunks_processed,
            'rows_processed': self.processed_rows,
            'batches_processed': self.batches_processed,
            'estimated_completion': None
        }

    def process_rows(self, rows: List[tuple], message_info: Dict) -> Tuple[List[bytes], int]:
        """Process rows into Arrow format with optimized performance"""
        start_time = time.time()
        print(f"\nProcessing batch {self.batches_processed + 1} with {len(rows)} rows")

        def check_time():
            """Check if approaching timeout"""
            if time.time() - start_time > 20:  # Leave 10s buffer
                raise TimeoutError("Approaching timeout limit")

        # Process rows
        for row in rows:
            for j, (value, converter) in enumerate(zip(row, self.field_converters)):
                try:
                    converted_value = converter(value)
                    self.arrays[j].append(converted_value)
                except Exception as e:
                    print(f"Warning: Error converting value at index {j}: {str(e)}")
                    self.arrays[j].append(None)

            if len(self.arrays[0]) % 100 == 0:
                check_time()

        try:
            # Create Arrow arrays
            check_time()
            arrow_arrays = []
            for i, (array_data, field) in enumerate(zip(self.arrays, self.schema)):
                arrow_arrays.append(pa.array(array_data, type=field.type))

            # Create record batch
            record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)

            # Compress data
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, record_batch.schema) as writer:
                writer.write_batch(record_batch)

            compressed_data = zlib.compress(sink.getvalue().to_pybytes(), level=COMPRESSION_LEVEL)
            chunks = self._chunk_data(compressed_data)

            self.total_chunks_processed += len(chunks)
            self.processed_rows += len(rows)
            self.batches_processed += 1

            print(f"Successfully processed batch {self.batches_processed} - {len(rows)} rows into {len(chunks)} chunks")
            return chunks, len(rows)

        except Exception as e:
            print(f"ERROR: Failed to process batch: {str(e)}")
            raise


def create_arrow_schema(schema_info: Dict) -> pa.Schema:
    """Create PyArrow schema from schema info"""
    fields = []
    for name, dtype in schema_info.items():
        if dtype == "timestamp":
            pa_type = pa.timestamp('us')
        elif dtype == "integer":
            pa_type = pa.int64()
        elif dtype == "float":
            pa_type = pa.float64()
        else:
            pa_type = pa.string()
        fields.append(pa.field(name, pa_type))
    return pa.schema(fields)


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME'],
    )


def send_to_sqs(chunks: List[bytes], metadata: Dict, queue_url: str, is_final: bool = False):
    """Send processed chunks to SQS queue with final message flag"""
    sqs = boto3.client('sqs')

    for i, chunk in enumerate(chunks):
        message = {
            'transfer_id': metadata['transfer_id'],
            'partition_id': metadata['partition_id'],
            'chunk_index': i,
            'total_chunks': len(chunks),
            'data': chunk.hex(),
            'is_final': is_final,
            'sequence': metadata['sequence'] if 'sequence' in metadata else 0,
            'order': metadata.get('order', 0)
        }

        print(f"Sending chunk {i + 1}/{len(chunks)} for partition {metadata['partition_id']}")
        print(f"Message size: {len(message['data'])} bytes")

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )


def process_query_message(message, partition_tracker):
    """Process a single SQS message with partition tracking"""
    try:
        body = json.loads(message['body'])
        print(f"Starting to process partition {body.get('partition_id')} for transfer {body.get('transfer_id')}")

        # Track this partition
        transfer_id = body['transfer_id']
        if transfer_id not in partition_tracker:
            partition_tracker[transfer_id] = {
                'total_partitions': body.get('total_partitions', 0),
                'processed_partitions': set(),
                'current_partition': 0
            }

        # Create Arrow schema from schema info
        schema = create_arrow_schema(body['schema_info'])
        processor = ArrowProcessor(schema)

        # Execute query for this partition - using the query as passed from Lambda 1
        # Just add OFFSET and LIMIT for pagination
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                partition_query = f"""
                {body['query']} 
                OFFSET {body['start_offset']} 
                LIMIT {body['end_offset'] - body['start_offset']}
                """
                print(f"Executing query: {partition_query}")
                print(f"Offset: {body['start_offset']}, Limit: {body['end_offset'] - body['start_offset']}")
                cur.execute(partition_query)
                rows = cur.fetchall()
                print(f"Retrieved {len(rows)} rows from database for partition {body['partition_id']}")

        # Process rows into Arrow format
        chunks, row_count = processor.process_rows(rows, body)
        print("test")

        # Track partition completion
        tracker = partition_tracker[transfer_id]
        tracker['processed_partitions'].add(body['partition_id'])
        tracker['current_partition'] = max(tracker['current_partition'], body['partition_id'])

        total_partitions = body['total_partitions']
        order = body['order']
        is_final = True if order == total_partitions else False

        print(f"Processing complete for partition {body['partition_id']}: {row_count} rows processed into {len(chunks)} chunks")
        if is_final:
            print(f"This is the final partition for transfer {transfer_id}")

        # Send to output queue
        output_queue_url = os.environ['OUTPUT_QUEUE_URL']
        send_to_sqs(chunks, {
            'transfer_id': body['transfer_id'],
            'partition_id': body['partition_id'],
            'sequence': body['partition_id'],
            'order': body['order'],
            'is_final': is_final
        }, output_queue_url, is_final)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'transfer_id': body['transfer_id'],
                'partition_id': body['partition_id'],
                'chunks_processed': len(chunks),
                'rows_processed': row_count,
            })
        }

    except Exception as e:
        print(f"ERROR processing partition {body.get('partition_id')}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'transfer_id': body.get('transfer_id'),
                'partition_id': body.get('partition_id')
            })
        }


def lambda_handler(event, context):
    """Main Lambda handler with partition tracking"""
    print(f"Incoming event: {event}")

    # Initialize partition tracker
    partition_tracker = {}

    responses = []
    for record in event['Records']:
        response = process_query_message(record, partition_tracker)
        responses.append(response)

    print(f"Processed {len(responses)} messages")
    return responses if len(responses) > 1 else responses[0]