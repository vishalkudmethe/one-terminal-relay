#!/bin/bash
# One Terminal - Cloud Relay Deployment Script for Ubuntu 22.04 LTS

echo "--- Updating System ---"
sudo apt update && sudo apt upgrade -y

echo "--- Installing Python & Dependencies ---"
sudo apt install python3.10 python3.10-venv python3-pip nginx certbot python3-certbot-nginx -y

echo "--- Setup Application Directory ---"
mkdir -p /home/ubuntu/relay
cd /home/ubuntu/relay

# (Copy your requirements.txt and main.py to /home/ubuntu/relay using SCP or git)

echo "--- Create Virtual Environment ---"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

echo "--- Creating Systemd Service ---"
cat <<EOF | sudo tee /etc/systemd/system/oneterminal-relay.service
[Unit]
Description=Gunicorn instance to serve OneTerminal Relay
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/relay
Environment="PATH=/home/ubuntu/relay/venv/bin"
Environment="GATEWAY_SECRET=super-secret-relay-token-change-me"
# Angel One - Relay's own trading account credentials for market data streaming
# Get these from: Angel SmartAPI dashboard -> Your Apps -> Feed Token
Environment="ANGEL_CLIENT_ID=YOUR_ANGEL_CLIENT_ID"
Environment="ANGEL_FEED_TOKEN=YOUR_ANGEL_FEED_TOKEN"
Environment="ANGEL_API_KEY=YOUR_ANGEL_API_KEY"
ExecStart=/home/ubuntu/relay/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000 --workers 4

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl start oneterminal-relay
sudo systemctl enable oneterminal-relay

echo "--- Setup Nginx Proxy ---"
cat <<EOF | sudo tee /etc/nginx/sites-available/relay
server {
    listen 80;
    server_name YOUR_DOMAIN_NAME.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/relay /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx

echo "--- Setup SSL (Run manually after checking DNS) ---"
echo "Run: sudo certbot --nginx -d YOUR_DOMAIN_NAME.com"
