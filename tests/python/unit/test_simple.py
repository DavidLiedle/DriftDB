#!/usr/bin/env python3
"""Simple test to check authentication and basic query."""

import psycopg2
import sys

try:
    print("Attempting to connect...")
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="driftdb",
        user="driftdb",
        password="driftdb",
        connect_timeout=5
    )
    print("Connected successfully!")

    cursor = conn.cursor()
    print("Executing simple query...")
    cursor.execute("SELECT 1 AS test")
    result = cursor.fetchone()
    print(f"Result: {result}")

    cursor.close()
    conn.close()
    print("Test completed successfully!")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)