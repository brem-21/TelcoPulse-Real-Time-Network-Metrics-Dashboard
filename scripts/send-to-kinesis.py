import boto3
import csv
import json
import time
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KinesisCSVWriter:
    def __init__(self):
        """
        Initialize the Kinesis CSV Writer using environment variables.
        """
        self.stream_name = os.getenv('stream_name')
        self.region_name = os.getenv('region')
        self.kinesis_client = boto3.client(
            'kinesis',
            aws_access_key_id=os.getenv('access_key_id'),
            aws_secret_access_key=os.getenv('secret_access_key'),
            region_name=self.region_name
        )

    def write_csv_to_kinesis(self, csv_file_path: str, partition_key_column: str = None) -> None:
        """
        Read CSV file and write records to Kinesis.

        Args:
            csv_file_path (str): Path to the CSV file
            partition_key_column (str): Column name to use as partition key. If None, uses row number
        """
        try:
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)

                for row_number, row in enumerate(csv_reader, 1):
                    data = json.dumps(row)

                    # Determine partition key
                    raw_key = row.get(partition_key_column, '') if partition_key_column else ''
                    partition_key = raw_key.strip() if isinstance(raw_key, str) else str(row_number)

                    # Validate partition key, fallback if invalid
                    if not partition_key:
                        partition_key = str(row_number)

                    try:
                        response = self.kinesis_client.put_record(
                            StreamName=self.stream_name,
                            Data=data,
                            PartitionKey=partition_key
                        )
                        logger.info(f"Successfully wrote record {row_number} to Kinesis. ShardId: {response['ShardId']}")
                        time.sleep(0.1)
                    except Exception as e:
                        logger.error(f"Error writing record {row_number} to Kinesis: {str(e)}")
                        raise

        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            raise

def main():
    csv_file_path = os.getenv('data_loc')  # Load from .env

    try:
        writer = KinesisCSVWriter()
        writer.write_csv_to_kinesis(csv_file_path, partition_key_column='operator')  # Use 'operator' as key
        logger.info("Successfully processed all records")
    except Exception as e:
        logger.error(f"Failed to process CSV file: {str(e)}")

if __name__ == "__main__":
    main()
