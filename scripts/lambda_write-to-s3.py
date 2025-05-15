import json
import boto3
import base64
import datetime
import os

def lambda_handler(event, context):
    # Initialize AWS clients
    s3_client = boto3.client('s3')
    
    # Get S3 bucket name from environment variable
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    if not bucket_name:
        print("ERROR: S3_BUCKET_NAME environment variable not set")
        return {
            'statusCode': 500,
            'body': 'S3_BUCKET_NAME environment variable not set'
        }
    
    # Debug: Log initial event info
    print(f"Processing event with {len(event.get('Records', []))} records")
    
    if not event.get('Records'):
        print("No Records found in event")
        return {
            'statusCode': 200,
            'body': 'No records to process'
        }

    processed_records = 0
    
    # Process Kinesis records
    try:
        for record in event['Records']:
            # Extract the kinesis data
            if 'kinesis' not in record or 'data' not in record['kinesis']:
                print(f"Record missing kinesis data field: {json.dumps(record)}")
                continue
                
            # Decode Kinesis data
            try:
                payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
                print(f"Successfully decoded payload (first 100 chars): {payload[:100]}...")
                
                # Try parsing as JSON
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError as e:
                    print(f"Not valid JSON, treating as raw text: {str(e)}")
                    data = payload
                    
            except Exception as e:
                print(f"Error decoding data: {str(e)}")
                print(f"Raw data: {record['kinesis']['data']}")
                continue
                
            # Generate a unique S3 key with timestamp
            timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
            s3_key = f"bronze/data-{timestamp}.json"
            
            # Write to S3
            try:
                content = json.dumps(data) if isinstance(data, dict) else data
                response = s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=content,
                    ContentType='application/json' if isinstance(data, dict) else 'text/plain'
                )
                
                # Verify successful write by checking response
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                    print(f"Successfully wrote to s3://{bucket_name}/{s3_key}")
                    processed_records += 1
                else:
                    print(f"Unexpected S3 response: {json.dumps(response)}")
            except Exception as e:
                print(f"Error writing to S3: {str(e)}")
                continue
                
        print(f"Completed processing with {processed_records}/{len(event['Records'])} successful records")
        return {
            'statusCode': 200,
            'body': f'Successfully processed {processed_records} records'
        }
        
    except Exception as e:
        print(f"Fatal error in Lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error processing records: {str(e)}'
        }