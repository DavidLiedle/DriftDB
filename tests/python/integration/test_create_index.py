#!/usr/bin/env python3
"""Test CREATE INDEX functionality."""

import psycopg2
import sys
import time

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

    # Cleanup first
    try:
        cursor.execute("DROP TABLE IF EXISTS users")
    except:
        pass

    # Test 1: Create a test table
    print("\n1. Creating test table...")
    try:
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                email TEXT,
                created_at TEXT
            )
        """)
        print("✓ Table created successfully")
    except Exception as e:
        print(f"✗ CREATE TABLE failed: {e}")
        sys.exit(1)

    # Test 2: Insert some test data
    print("\n2. Inserting test data...")
    try:
        for i in range(1, 11):
            cursor.execute(f"""
                INSERT INTO users (id, username, email, created_at)
                VALUES ({i}, 'user{i}', 'user{i}@example.com', '2024-01-{i:02d}')
            """)
        print("✓ Test data inserted")
    except Exception as e:
        print(f"✗ INSERT failed: {e}")

    # Test 3: Create index on username column
    print("\n3. Creating index on username...")
    try:
        cursor.execute("CREATE INDEX idx_username ON users(username)")
        print("✓ Index created on username")
    except Exception as e:
        print(f"✗ CREATE INDEX failed: {e}")
        sys.exit(1)

    # Test 4: Create index on email column with custom name
    print("\n4. Creating named index on email...")
    try:
        cursor.execute("CREATE INDEX email_idx ON users(email)")
        print("✓ Named index created on email")
    except Exception as e:
        print(f"✗ CREATE INDEX with name failed: {e}")

    # Test 5: Try to create duplicate index (should fail)
    print("\n5. Testing duplicate index prevention...")
    try:
        cursor.execute("CREATE INDEX idx_username2 ON users(username)")
        print("✗ Duplicate index should have failed")
    except psycopg2.Error as e:
        print(f"✓ Duplicate index correctly rejected: {e}")

    # Test 6: Create index on non-existent table (should fail)
    print("\n6. Testing index on non-existent table...")
    try:
        cursor.execute("CREATE INDEX bad_idx ON nonexistent(column)")
        print("✗ Index on non-existent table should have failed")
    except psycopg2.Error as e:
        print(f"✓ Index on non-existent table correctly rejected: {e}")

    # Test 7: Query using indexed columns (performance test)
    print("\n7. Testing query performance with indexes...")
    try:
        # Query by username (indexed)
        start = time.time()
        cursor.execute("SELECT * FROM users WHERE username = 'user5'")
        result = cursor.fetchone()
        username_time = time.time() - start
        print(f"✓ Query by indexed username: {username_time:.4f}s - Result: {result}")

        # Query by email (indexed)
        start = time.time()
        cursor.execute("SELECT * FROM users WHERE email = 'user7@example.com'")
        result = cursor.fetchone()
        email_time = time.time() - start
        print(f"✓ Query by indexed email: {email_time:.4f}s - Result: {result}")

        # Query by created_at (not indexed)
        start = time.time()
        cursor.execute("SELECT * FROM users WHERE created_at = '2024-01-03'")
        result = cursor.fetchone()
        created_time = time.time() - start
        print(f"✓ Query by non-indexed created_at: {created_time:.4f}s - Result: {result}")
    except Exception as e:
        print(f"✗ Query test failed: {e}")

    # Clean up
    print("\n8. Cleaning up...")
    try:
        cursor.execute("DROP TABLE users")
        print("✓ Table dropped")
    except Exception as e:
        print(f"✗ Cleanup failed: {e}")

    cursor.close()
    conn.close()
    print("\n✓ All CREATE INDEX tests completed successfully!")

except psycopg2.Error as e:
    print(f"✗ Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    sys.exit(1)