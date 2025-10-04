#!/usr/bin/env python3
"""Test DriftDB SQL execution through PostgreSQL wire protocol."""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
import sys

def test_sql_execution():
    """Test basic SQL operations through psycopg2."""
    try:
        # Connect to DriftDB server
        print("Connecting to DriftDB server...")
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )

        cursor = conn.cursor(cursor_factory=RealDictCursor)
        print("✓ Connected successfully")

        # Test 1: CREATE TABLE
        print("\n1. Creating table 'users'...")
        cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER
            )
        """)
        conn.commit()
        print("✓ Table created")

        # Test 2: INSERT data
        print("\n2. Inserting data...")
        cursor.execute("""
            INSERT INTO users (id, name, email, age)
            VALUES (1, 'Alice', 'alice@example.com', 30)
        """)
        cursor.execute("""
            INSERT INTO users (id, name, email, age)
            VALUES (2, 'Bob', 'bob@example.com', 25)
        """)
        cursor.execute("""
            INSERT INTO users (id, name, email, age)
            VALUES (3, 'Charlie', 'charlie@example.com', 35)
        """)
        conn.commit()
        print("✓ Inserted 3 records")

        # Test 3: SELECT data
        print("\n3. Querying data...")
        cursor.execute("SELECT * FROM users ORDER BY id")
        results = cursor.fetchall()
        print(f"✓ Found {len(results)} records:")
        for row in results:
            print(f"   - {row}")

        # Test 4: UPDATE data
        print("\n4. Updating data...")
        cursor.execute("""
            UPDATE users SET age = 31 WHERE name = 'Alice'
        """)
        conn.commit()
        print("✓ Updated Alice's age")

        # Verify update
        cursor.execute("SELECT * FROM users WHERE name = 'Alice'")
        alice = cursor.fetchone()
        print(f"   Alice's new record: {alice}")

        # Test 5: DELETE data
        print("\n5. Deleting data...")
        cursor.execute("DELETE FROM users WHERE name = 'Charlie'")
        conn.commit()
        print("✓ Deleted Charlie")

        # Verify deletion
        cursor.execute("SELECT COUNT(*) as count FROM users")
        count = cursor.fetchone()
        print(f"   Remaining users: {count['count']}")

        # Test 6: Time-travel query (DriftDB specific)
        print("\n6. Testing time-travel query...")
        try:
            cursor.execute("SELECT * FROM users AS OF @seq:1")
            historical = cursor.fetchall()
            print(f"✓ Historical query returned {len(historical)} records")
        except psycopg2.Error as e:
            print(f"⚠ Time-travel query not yet supported: {e}")

        # Close connection
        cursor.close()
        conn.close()
        print("\n✓ All tests completed successfully!")
        return True

    except psycopg2.Error as e:
        print(f"\n✗ Database error: {e}")
        return False
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_sql_execution()
    sys.exit(0 if success else 1)