import boto3
from botocore.exceptions import ClientError

def migrate_tata():
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table('OneTerminal_Master')
    
    print("🚀 Starting Tata Motors Migration: NSE:TATAMOTORS -> NSE:TMPV (Token 570)")
    
    try:
        # 1. Get existing item
        response = table.get_item(Key={'uId': 'NSE:TATAMOTORS'})
        item = response.get('Item')
        
        if not item:
            print("⚠️ NSE:TATAMOTORS not found in DynamoDB. Checking if NSE:TMPV already exists...")
            check_res = table.get_item(Key={'uId': 'NSE:TMPV'})
            if check_res.get('Item'):
                print("✅ NSE:TMPV already exists. No migration needed.")
            else:
                print("❌ Neither TATAMOTORS nor TMPV found. Please check uId format.")
            return

        # 2. Update fields for new entity
        item['uId'] = 'NSE:TMPV'
        item['symbol'] = 'TMPV'
        item['native_token'] = '570'
        item['company_name'] = 'Tata Motors Passenger Vehicles'
        
        # 3. Create new item
        table.put_item(Item=item)
        print("✅ Created new item: NSE:TMPV")
        
        # 4. Delete old item
        table.delete_item(Key={'uId': 'NSE:TATAMOTORS'})
        print("🗑️ Deleted old item: NSE:TATAMOTORS")
        
        print("🏁 Migration completed successfully.")
        
    except ClientError as e:
        print(f"❌ DynamoDB Error: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")

if __name__ == "__main__":
    migrate_tata()
