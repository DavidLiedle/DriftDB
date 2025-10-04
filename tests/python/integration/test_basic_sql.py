#!/usr/bin/env python3
"""Test basic SQL without automatic transactions."""

import psycopg2
import sys

try:
    print("Connecting to DriftDB...")
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="driftdb",
        user="driftdb",
        password="driftdb",
        connect_timeout=5
    )

    # Disable autocommit to control transactions manually
    conn.autocommit = True
    print("✓ Connected successfully with autocommit enabled")

    cursor = conn.cursor()

    # Test 1: Simple SELECT
    print("\n1. Testing simple SELECT...")
    try:
        cursor.execute("SELECT 1 AS test_column")
        result = cursor.fetchone()
        print(f"✓ SELECT result: {result}")
    except Exception as e:
        print(f"✗ SELECT failed: {e}")

    # Test 2: CREATE TABLE
    print("\n2. Testing CREATE TABLE...")
    try:
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """)
        print("✓ Table created successfully")
    except Exception as e:
        print(f"✗ CREATE TABLE failed: {e}")

    # Test 3: INSERT
    print("\n3. Testing INSERT...")
    try:
        cursor.execute("""
            INSERT INTO test_table (id, name, value)
            VALUES (1, 'test_item', 42.5)
        """)
        print("✓ INSERT successful")
    except Exception as e:
        print(f"✗ INSERT failed: {e}")

    # Test 4: SELECT from table
    print("\n4. Testing SELECT from table...")
    try:
        cursor.execute("SELECT * FROM test_table")
        results = cursor.fetchall()
        print(f"✓ Found {len(results)} rows: {results}")
    except Exception as e:
        print(f"✗ SELECT from table failed: {e}")

    cursor.close()
    conn.close()
    print("\n✓ All tests completed!")

except psycopg2.Error as e:
    print(f"✗ Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    sys.exit(1)