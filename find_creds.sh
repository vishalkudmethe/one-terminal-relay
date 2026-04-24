#!/bin/bash
echo "=== .env files ==="
find /home/ubuntu -name "*.env" -o -name ".env" 2>/dev/null

echo ""
echo "=== start/run scripts ==="
find /home/ubuntu -name "start*.sh" -o -name "run*.sh" -o -name "*.sh" 2>/dev/null | grep -v check_relay | grep -v deploy

echo ""
echo "=== relay.log last 50 lines ==="
tail -50 /home/ubuntu/relay.log 2>/dev/null

echo ""
echo "=== any stored angel credentials in relay db or config ==="
cat /home/ubuntu/relay/config.py 2>/dev/null | grep -i angel

echo ""
echo "=== sqlite angel data ==="
cd /home/ubuntu/relay && python3 -c "
import sqlite3
try:
    conn = sqlite3.connect('audit_log.db')
    cur = conn.cursor()
    cur.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")
    print('Tables:', cur.fetchall())
except Exception as e:
    print('Error:', e)
" 2>/dev/null

echo ""
echo "=== /proc PID cmdline (how relay was started) ==="
PID=$(ps aux | grep 'uvicorn main:app' | grep -v grep | awk '{print $2}' | head -1)
cat /proc/$PID/cmdline 2>/dev/null | tr '\0' ' '
echo ""
echo "CWD: $(ls -la /proc/$PID/cwd 2>/dev/null)"
