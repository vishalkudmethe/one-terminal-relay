import requests
import pyotp
import json

# CREDENTIALS from secrets.dart - TRYING API SECRET AS PRIVATE KEY
API_KEY = "279e0ad8-fb61-41b9-a3fc-222bc63f3079" 
CLIENT_ID = "AACD647104"
PIN = "1010"
TOTP_KEY = "HP3YFFIEHWISFSBQWQIVNHNHIE"
PUBLIC_IP = "13.126.49.124"

def get_angel_token():
    try:
        # 1. Generate TOTP
        totp = pyotp.TOTP(TOTP_KEY).now()
        
        # 2. Login
        url = "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword"
        payload = {
            "clientcode": CLIENT_ID,
            "password": PIN,
            "totp": totp
        }
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-UserType': 'USER',
            'X-SourceID': 'WEB',
            'X-PrivateKey': API_KEY,
            'X-ClientLocalIP': '192.168.1.1',
            'X-ClientPublicIP': PUBLIC_IP, 
            'X-MACAddress': '02:00:00:00:00:00',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        print(f"Attempting login for {CLIENT_ID} with TOTP {totp} and API_SECRET as PrivateKey...")
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        data = response.json()
        
        if data.get("status"):
            feed_token = data["data"]["feedToken"]
            print(f"SUCCESS")
            print(f"FEED_TOKEN={feed_token}")
            print(f"CLIENT_ID={CLIENT_ID}")
            print(f"API_KEY={API_KEY}")
        else:
            print(f"Error: {data}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    get_angel_token()
