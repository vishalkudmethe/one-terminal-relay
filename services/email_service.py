import os
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# AWS SES Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "One Terminal Global <no-reply@oneterminal.io>")

def send_welcome_email(recipient_email, api_key, api_secret):
    """
    Sends the 'Welcome to One Terminal Global' email with API Credentials.
    """
    client = boto3.client('ses', region_name=AWS_REGION)

    subject = "Welcome to One Terminal Global - Institutional Account Approved"
    
    # High-density, professional email body
    html_body = f"""
    <html>
    <head></head>
    <body style="background-color: #0d1117; color: #ffffff; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 40px;">
        <div style="max-width: 600px; margin: auto; border: 1px solid #30363d; border-radius: 12px; padding: 30px; background-color: #161b22;">
            <h1 style="color: #00ffff; border-bottom: 2px solid #30363d; padding-bottom: 10px;">ONE TERMINAL GLOBAL</h1>
            <p style="font-size: 16px; line-height: 1.6;">Congratulations,</p>
            <p style="font-size: 14px; line-height: 1.6;">Your institutional trading account with <b>Alpaca Securities</b> via One Terminal Global has been <b>APPROVED</b>.</p>
            
            <div style="background-color: #0d1117; border: 1px dashed #00ffff; padding: 20px; border-radius: 8px; margin: 25px 0;">
                <p style="margin: 0; color: #8b949e; font-size: 12px; text-transform: uppercase; letter-spacing: 1px;">Trading API Credentials</p>
                <p style="margin: 10px 0 5px 0; font-family: monospace; font-size: 14px;"><strong>API Key ID:</strong><br/>{api_key}</p>
                <p style="margin: 10px 0 0 0; font-family: monospace; font-size: 14px;"><strong>Secret Key:</strong><br/>{api_secret}</p>
            </div>

            <p style="font-size: 12px; color: #8b949e;"><i>IMPORTANT: Please keep these credentials secure. Never share your Secret Key with anyone. Use these keys to 'Sign In' to One Terminal Global on your mobile app.</i></p>
            
            <div style="margin-top: 30px; font-size: 12px; color: #484f58; text-align: center;">
                &copy; 2026 One Terminal Global | Institutional-Grade US Stock Execution
            </div>
        </div>
    </body>
    </html>
    """
    
    try:
        response = client.send_email(
            Destination={'ToAddresses': [recipient_email]},
            Message={
                'Body': {
                    'Html': {'Charset': "UTF-8", 'Data': html_body},
                },
                'Subject': {'Charset': "UTF-8", 'Data': subject},
            },
            Source=SENDER_EMAIL,
        )
        logger.info(f"Email sent successfully. Message ID: {response['MessageId']}")
        return True
    except ClientError as e:
        logger.error(f"AWS SES Error: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending email: {e}")
        return False
