import boto3

dynamodb = boto3.client('dynamodb', region_name='ap-south-1')

try:
    print("Creating OneTerminal_Master table...")
    response = dynamodb.create_table(
        TableName='OneTerminal_Master',
        KeySchema=[
            {'AttributeName': 'uId', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'broker_name', 'KeyType': 'RANGE'}  # Sort key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'uId', 'AttributeType': 'S'},
            {'AttributeName': 'broker_name', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print("Table creation initiated. Status:", response['TableDescription']['TableStatus'])
except Exception as e:
    print(f"Error creating table: {e}")
