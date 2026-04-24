import sqlite3
try:
    conn = sqlite3.connect('/home/ubuntu/relay/audit_log.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM global_settings")
    rows = cur.fetchall()
    print("=== global_settings ===")
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error: {e}")
