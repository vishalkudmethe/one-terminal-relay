import boto3
import config

dynamodb = boto3.client('dynamodb', region_name='ap-south-1')
try:
    response = dynamodb.list_tables()
    print(f"Tables: {response.get('TableNames', [])}")
except Exception as e:
    print(f"Error: {e}")
