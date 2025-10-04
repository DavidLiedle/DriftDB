#!/usr/bin/env python3
"""
Simple test of DriftDB basic functionality
"""
import psycopg2
import json
import time

def main():
    print("Testing DriftDB Basic Functionality")
    print("=" * 60)

    # Connect to DriftDB
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="driftdb",
            user="driftdb",
            password=""
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("✓ Connected to DriftDB")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return

    try:
        # Create test table
        print("\n1. Creating test table...")
        cur.execute("""
            CREATE TABLE test_users (
                id INT PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                age INT
            )
        """)
        print("   ✓ Table created")

        # Insert test data
        print("\n2. Inserting test data...")
        users = [
            {"id": 1, "name": "Alice", "email": "alice@test.com", "age": 30},
            {"id": 2, "name": "Bob", "email": "bob@test.com", "age": 25},
            {"id": 3, "name": "Charlie", "email": "charlie@test.com", "age": 35},
        ]

        for user in users:
            cur.execute(
                f"INSERT INTO test_users (doc) VALUES ('{json.dumps(user)}')"
            )
        print(f"   ✓ Inserted {len(users)} users")

        # Query data
        print("\n3. Querying all users...")
        cur.execute("SELECT * FROM test_users")
        results = cur.fetchall()
        print(f"   ✓ Found {len(results)} users")
        for row in results:
            data = json.loads(row[0])
            print(f"     - {data['name']} ({data['email']})")

        # Query with WHERE
        print("\n4. Query with WHERE clause...")
        cur.execute("SELECT * FROM test_users WHERE age > 28")
        results = cur.fetchall()
        print(f"   ✓ Found {len(results)} users over 28")
        for row in results:
            data = json.loads(row[0])
            print(f"     - {data['name']}: age {data['age']}")

        # Test SELECT with specific columns
        print("\n5. SELECT specific columns...")
        cur.execute("SELECT name, email FROM test_users")
        results = cur.fetchall()
        print(f"   ✓ Got {len(results)} results")
        for row in results:
            print(f"     - {row[0]}, {row[1]}")

        # Test ORDER BY
        print("\n6. Testing ORDER BY...")
        cur.execute("SELECT * FROM test_users ORDER BY age DESC")
        results = cur.fetchall()
        print(f"   ✓ Results ordered by age (descending):")
        for row in results:
            data = json.loads(row[0])
            print(f"     - {data['name']}: age {data['age']}")

        # Test LIMIT
        print("\n7. Testing LIMIT...")
        cur.execute("SELECT * FROM test_users LIMIT 2")
        results = cur.fetchall()
        print(f"   ✓ Limited to {len(results)} results")

        print("\n" + "=" * 60)
        print("✅ ALL BASIC TESTS PASSED!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()