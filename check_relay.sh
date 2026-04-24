#!/bin/bash
# Check relay environment and Angel One credentials
echo "=== RELAY PROCESS ==="
ps aux | grep uvicorn | grep -v grep

echo ""
echo "=== RELAY PID ENV (Angel/Gateway creds) ==="
PID=$(ps aux | grep 'uvicorn main:app' | grep -v grep | awk '{print $2}' | head -1)
if [ -n "$PID" ]; then
    cat /proc/$PID/environ 2>/dev/null | tr '\0' '\n' | grep -E 'ANGEL|GATEWAY|FEED|API'
    echo "(PID=$PID)"
else
    echo "Relay process not found!"
fi

echo ""
echo "=== START SCRIPTS ==="
ls -la /home/ubuntu/*.sh 2>/dev/null
ls -la /home/ubuntu/relay/*.sh 2>/dev/null

echo ""
echo "=== RELAY LOG TAIL ==="
tail -30 /home/ubuntu/relay.log 2>/dev/null || tail -30 /home/ubuntu/relay/relay.log 2>/dev/null || journalctl -u oneterminal-relay --no-pager -n 30 2>/dev/null || echo "No log found"
