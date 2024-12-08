import json
import os
import psycopg2.pool
import pyarrow as pa
import pyarrow.ipc as ipc
import boto3
import zlib
import time
from contextlib import contextmanager
from typing import List, Tuple, Dict, Any, Generator

# Constants
COMPRESSION_LEVEL = 1
MAX_SQS_MESSAGE_SIZE = 256 * 1024
MAX_PAYLOAD_SIZE = int(MAX_SQS_MESSAGE_SIZE * 0.7)  # Leave room for metadata

# Initialize PostgreSQL connection pool
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        database=os.environ['DB_NAME']
    )
    print("Database connection pool created successfully")
except Exception as e:
    print(f"Error creating database connection pool: {e}")
    db_pool = None

# Initialize SQS client
try:
    sqs = boto3.client('sqs')
    OUTPUT_QUEUE_URL = os.environ['OUTPUT_QUEUE_URL']
    print("SQS client initialized successfully")
except Exception as e:
    print(f"Error initializing SQS client: {e}")
    sqs = None


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
    finally:
        if conn:
            db_pool.putconn(conn)


class ArrowBatchProcessor:
    def __init__(self, schema_info: dict):
        self.schema = self._create_schema(schema_info)
        self.field_converters = self._create_field_converters()
        self.pending_rows = []
        self.preview_sent = False

    def _create_schema(self, schema_info: dict):
        fields = []
        for name, type_str in schema_info.items():
            if type_str == "timestamp":
                field_type = pa.timestamp('us', tz='UTC')
            elif type_str == "float":
                field_type = pa.float64()
            elif type_str == "integer":
                field_type = pa.int64()
            else:
                field_type = pa.string()
            fields.append(pa.field(name, field_type))
        return pa.schema(fields)

    def _create_field_converters(self):
        converters = []
        for field in self.schema:
            if pa.types.is_timestamp(field.type):
                converters.append(lambda x: x)
            elif field.name == 'value_gpu' or pa.types.is_floating(field.type):
                converters.append(lambda x: float(x) if x is not None else 0.0)
            elif pa.types.is_integer(field.type):
                converters.append(lambda x: int(x) if x is not None else 0)
            else:
                converters.append(lambda x: str(x) if x is not None else '')
        return converters

    def _show_preview(self, rows: List[tuple], message_info: Dict):
        """Display a preview of the data being processed"""
        if self.preview_sent:
            return

        preview_size = 5
        preview_rows = rows[:preview_size]
        columns = [field.name for field in self.schema]

        print("\n=== Data Processing Preview ===")
        print(f"Transfer ID: {message_info['transfer_id']}")
        print(f"Partition: {message_info['partition_id']} (Order: {message_info['order']})")
        print(f"Showing first {len(preview_rows)} rows of partition")
        print("\nColumns:", columns)

        # Create a formatted table
        row_format = "{:<3} " + " | ".join(["{:<20}"] * len(columns))
        print("\n" + row_format.format("idx", *columns))
        print("-" * (3 + (23 * len(columns))))

        for idx, row in enumerate(preview_rows):
            formatted_row = [
                str(val)[:20] if val is not None else "NULL"
                for val in row
            ]
            print(row_format.format(idx, *formatted_row))

        print("\n=== End Preview ===\n")
        self.preview_sent = True

    def process_rows(self, rows: List[tuple], message_info: Dict) -> Generator[Tuple[bytes, int], None, None]:
        """Process rows and yield compressed arrow data that fits within SQS limits"""
        # Show preview of first batch
        if not self.preview_sent and rows:
            self._show_preview(rows, message_info)

        # Add new rows to pending rows
        self.pending_rows.extend(rows)

        while self.pending_rows:
            # Start with a small batch and increase until we hit size limit
            batch_size = min(100, len(self.pending_rows))
            while True:
                test_batch = self.pending_rows[:batch_size]
                data = self._create_arrow_batch(test_batch)
                compressed_data = zlib.compress(data, level=COMPRESSION_LEVEL)

                if len(compressed_data) > MAX_PAYLOAD_SIZE and batch_size > 1:
                    # Batch too large, try smaller batch
                    batch_size = max(1, batch_size // 2)
                else:
                    # We found a good batch size
                    break

            # Process the batch
            self.pending_rows = self.pending_rows[batch_size:]
            yield compressed_data, batch_size

    def _create_arrow_batch(self, rows: List[tuple]) -> bytes:
        """Create an Arrow batch from rows"""
        arrays = [[] for _ in range(len(self.schema))]

        for row in rows:
            for i, (value, converter) in enumerate(zip(row, self.field_converters)):
                arrays[i].append(converter(value))

        arrow_arrays = [pa.array(col, type=field.type)
                        for col, field in zip(arrays, self.schema)]
        record_batch = pa.RecordBatch.from_arrays(arrow_arrays, schema=self.schema)

        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, record_batch.schema) as writer:
            writer.write_batch(record_batch)

        return sink.getvalue().to_pybytes()

    def get_pending_count(self) -> int:
        """Get count of pending rows"""
        return len(self.pending_rows)


def send_to_sqs(data: bytes, metadata: Dict[str, Any]):
    """Send data to SQS"""
    message_body = {
        **metadata,
        'data': data.hex()  # Convert binary to hex string for JSON compatibility
    }

    try:
        sqs.send_message(
            QueueUrl=OUTPUT_QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageAttributes={
                'transfer_id': {
                    'DataType': 'String',
                    'StringValue': str(metadata['transfer_id'])
                },
                'order': {
                    'DataType': 'Number',
                    'StringValue': str(metadata['order'])
                }
            }
        )
        print(f"Sent message: transfer_id={metadata['transfer_id']}, "
              f"order={metadata['order']}, sequence={metadata['sequence']}, "
              f"size={len(data):,} bytes")
    except Exception as e:
        print(f"Error sending message to SQS: {e}")
        raise


def lambda_handler(event, context):
    try:
        for record in event['Records']:
            message = json.loads(record['body'])
            print(f"\nProcessing partition {message['partition_id']} "
                  f"(Order: {message['order']}) "
                  f"for transfer {message['transfer_id']}")

            processor = ArrowBatchProcessor(message['schema_info'])
            rows_processed = 0

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Optimize query performance
                    cursor.execute("""
                        SET work_mem = '256MB';
                        SET max_parallel_workers_per_gather = 4;
                        SET parallel_tuple_cost = 0.1;
                        SET parallel_setup_cost = 0.1;
                    """)

                    partition_query = f"""
                        WITH numbered_rows AS (
                            SELECT 
                                *,
                                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num
                            FROM ({message['query']}) base_query
                        )
                        SELECT * FROM numbered_rows
                        WHERE row_num > {message['start_offset']}
                          AND row_num <= {message['end_offset']}
                    """

                    cursor.execute(partition_query)
                    sequence = 0

                    while True:
                        rows = cursor.fetchmany(1000)  # Fetch in smaller chunks
                        if not rows and not processor.get_pending_count():
                            break

                        current_rows = rows if rows else []
                        rows_processed += len(current_rows)

                        # Process batches that fit within size limit
                        for compressed_data, row_count in processor.process_rows(current_rows, message):
                            sequence += 1
                            metadata = {
                                'transfer_id': message['transfer_id'],
                                'partition_id': message['partition_id'],
                                'order': message['order'],  # Include order in output
                                'sequence': sequence,
                                'row_count': row_count,
                                'timestamp': int(time.time() * 1000),
                                'is_final': not rows and processor.get_pending_count() == 0,
                                'estimated_size': message.get('estimated_size')
                            }

                            send_to_sqs(compressed_data, metadata)

                        if not rows:  # No more rows to fetch
                            break

            print(f"Completed processing partition {message['partition_id']}: "
                  f"{rows_processed:,} rows processed in {sequence} batches")

        return {'statusCode': 200}

    except Exception as e:
        print(f"Error processing message: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }