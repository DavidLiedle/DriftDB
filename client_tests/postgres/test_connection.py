#!/usr/bin/env python3
import psycopg2
import sys

try:
    print("Connecting to DriftDB...")
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="driftdb",
        user="driftdb",
        password=""
    )
    conn.autocommit = True
    print("Connected successfully!")

    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"Test query result: {result}")

    cur.close()
    conn.close()
    print("Connection test passed!")

except Exception as e:
    print(f"Connection failed: {e}")
    sys.exit(1)