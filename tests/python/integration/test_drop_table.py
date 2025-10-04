#!/usr/bin/env python3
"""Test DROP TABLE functionality."""

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
    conn.autocommit = True
    print("✓ Connected successfully")

    cursor = conn.cursor()

    # Test 1: Create a test table
    print("\n1. Creating test table...")
    try:
        cursor.execute("""
            CREATE TABLE drop_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """)
        print("✓ Table created successfully")
    except Exception as e:
        print(f"✗ CREATE TABLE failed: {e}")
        sys.exit(1)

    # Test 2: Insert some data
    print("\n2. Inserting test data...")
    try:
        cursor.execute("""
            INSERT INTO drop_test (id, name, value)
            VALUES (1, 'test_item', 42.5)
        """)
        print("✓ Data inserted successfully")
    except Exception as e:
        print(f"✗ INSERT failed: {e}")

    # Test 3: Verify table exists
    print("\n3. Verifying table exists...")
    try:
        cursor.execute("SELECT * FROM drop_test")
        results = cursor.fetchall()
        print(f"✓ Table exists with {len(results)} rows: {results}")
    except Exception as e:
        print(f"✗ SELECT failed: {e}")

    # Test 4: Drop the table
    print("\n4. Dropping the table...")
    try:
        cursor.execute("DROP TABLE drop_test")
        print("✓ Table dropped successfully")
    except Exception as e:
        print(f"✗ DROP TABLE failed: {e}")
        sys.exit(1)

    # Test 5: Verify table no longer exists
    print("\n5. Verifying table is gone...")
    try:
        cursor.execute("SELECT * FROM drop_test")
        results = cursor.fetchall()
        print(f"✗ Table still exists with {len(results)} rows")
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"✓ Table no longer exists (expected error): {e}")

    # Test 6: Try to drop non-existent table
    print("\n6. Testing DROP TABLE on non-existent table...")
    try:
        cursor.execute("DROP TABLE non_existent_table")
        print("✗ DROP TABLE should have failed on non-existent table")
    except psycopg2.Error as e:
        print(f"✓ Expected error for non-existent table: {e}")

    # Test 7: Create and drop multiple tables
    print("\n7. Testing multiple table drops...")
    try:
        cursor.execute("CREATE TABLE test1 (id INTEGER PRIMARY KEY)")
        cursor.execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY)")
        cursor.execute("CREATE TABLE test3 (id INTEGER PRIMARY KEY)")
        print("✓ Created 3 test tables")

        cursor.execute("DROP TABLE test2")
        print("✓ Dropped test2")

        # Verify test1 and test3 still exist
        cursor.execute("SELECT * FROM test1")
        cursor.execute("SELECT * FROM test3")
        print("✓ test1 and test3 still exist")

        # Clean up
        cursor.execute("DROP TABLE test1")
        cursor.execute("DROP TABLE test3")
        print("✓ Cleaned up remaining tables")
    except Exception as e:
        print(f"✗ Multiple table test failed: {e}")

    cursor.close()
    conn.close()
    print("\n✓ All DROP TABLE tests completed successfully!")

except psycopg2.Error as e:
    print(f"✗ Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    sys.exit(1)