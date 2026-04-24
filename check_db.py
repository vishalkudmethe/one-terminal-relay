import boto3
import json

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
table = dynamodb.Table('OneTerminal_Master')

print("Fetching MCX:GOLD05JUN2026FUT...")
res = table.get_item(Key={'uId': 'MCX:GOLD05JUN2026FUT', 'broker_name': 'angel'})
print(res.get('Item'))

print("Fetching MCX:GOLD05JUN26FUT...")
res = table.get_item(Key={'uId': 'MCX:GOLD05JUN26FUT', 'broker_name': 'angel'})
print(res.get('Item'))
