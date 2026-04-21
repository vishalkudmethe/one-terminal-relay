#!/bin/bash
# One Terminal Relay - Deploy Script
# Usage: ./deploy.sh <github-personal-access-token>
# Or: export GITHUB_PAT=your_token && ./deploy.sh

set -e

PAT=${1:-$GITHUB_PAT}

if [ -z "$PAT" ]; then
  echo "ERROR: GitHub PAT required. Usage: ./deploy.sh <token>"
  echo "       Or set GITHUB_PAT env variable."
  exit 1
fi

echo "==> Pulling latest code from GitHub..."
cd /home/ubuntu/relay

# Set remote with token for this pull only
git remote set-url origin "https://${PAT}@github.com/vishalkudmethe/one-terminal-relay.git"
git fetch origin
git reset --hard origin/main

# Restore safe remote (without token)
git remote set-url origin "https://github.com/vishalkudmethe/one-terminal-relay.git"

echo "==> Restarting relay service..."
pm2 restart one-terminal-relay

echo "==> Done. Relay is live."
pm2 logs one-terminal-relay --lines 10 --no-daemon
