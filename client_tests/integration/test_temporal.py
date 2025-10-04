#!/usr/bin/env python3
"""
Test SQL:2011 temporal query functionality (FOR SYSTEM_TIME AS OF)
"""
import psycopg2
import json
import time

def main():
    print("Testing SQL:2011 Temporal Queries")
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
        print("\n1. Creating temporal test table...")
        cur.execute("""
            CREATE TABLE temporal_test (
                id INT PRIMARY KEY,
                value VARCHAR,
                amount INT
            )
        """)
        print("   ✓ Table created")

        # Insert initial data
        print("\n2. Inserting initial data...")
        cur.execute("INSERT INTO temporal_test (id, value, amount) VALUES (1, 'initial', 100)")
        cur.execute("INSERT INTO temporal_test (id, value, amount) VALUES (2, 'second', 200)")
        print("   ✓ Inserted 2 rows")

        # Get current data
        print("\n3. Querying current data...")
        cur.execute("SELECT * FROM temporal_test ORDER BY id")
        current_data = cur.fetchall()
        print(f"   Current data: {current_data}")

        # Note: DriftDB tracks changes by sequence number
        # We need to get the current sequence for temporal queries
        print("\n4. Testing temporal query with sequence number...")

        # Update data
        print("   Updating data...")
        cur.execute("INSERT INTO temporal_test (id, value, amount) VALUES (3, 'third', 300)")

        # Query current state
        cur.execute("SELECT COUNT(*) FROM temporal_test")
        count = cur.fetchone()
        print(f"   Current count: {count[0]}")

        # Test temporal query syntaxes
        print("\n5. Testing SQL:2011 temporal query syntaxes...")

        # Test 1: FOR SYSTEM_TIME AS OF with sequence
        print("\n   a) Testing FOR SYSTEM_TIME AS OF <sequence>...")
        try:
            cur.execute("SELECT * FROM temporal_test FOR SYSTEM_TIME AS OF 2")
            result = cur.fetchall()
            print(f"      ✓ Temporal query returned {len(result)} rows")
        except Exception as e:
            print(f"      ✗ Temporal query failed: {e}")

        # Test 2: Legacy AS OF syntax
        print("\n   b) Testing legacy AS OF syntax...")
        try:
            cur.execute("SELECT * FROM temporal_test AS OF 2")
            result = cur.fetchall()
            print(f"      ✓ Legacy temporal query returned {len(result)} rows")
        except Exception as e:
            print(f"      ✗ Legacy temporal query failed: {e}")

        # Test 3: FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
        print("\n   c) Testing FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP...")
        try:
            cur.execute("SELECT * FROM temporal_test FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP")
            result = cur.fetchall()
            print(f"      ✓ CURRENT_TIMESTAMP query returned {len(result)} rows")
        except Exception as e:
            print(f"      ✗ CURRENT_TIMESTAMP query failed: {e}")

        # Test 4: Temporal query with WHERE clause
        print("\n   d) Testing temporal query with WHERE clause...")
        try:
            cur.execute("SELECT * FROM temporal_test FOR SYSTEM_TIME AS OF 2 WHERE amount > 150")
            result = cur.fetchall()
            print(f"      ✓ Temporal query with WHERE returned {len(result)} rows")
        except Exception as e:
            print(f"      ✗ Temporal query with WHERE failed: {e}")

        # Test 5: Check error handling for JOINs
        print("\n6. Testing temporal query limitations...")
        try:
            # Create second table
            cur.execute("""
                CREATE TABLE temporal_test2 (
                    id INT PRIMARY KEY,
                    name VARCHAR
                )
            """)
            cur.execute("INSERT INTO temporal_test2 (id, name) VALUES (1, 'test')")

            # Try temporal query with JOIN (should fail)
            cur.execute("""
                SELECT * FROM temporal_test t1
                JOIN temporal_test2 t2 ON t1.id = t2.id
                FOR SYSTEM_TIME AS OF 1
            """)
            print("      ✗ Temporal JOIN should have failed but didn't")
        except Exception as e:
            if "not supported with JOIN" in str(e) or "not supported with multiple tables" in str(e):
                print(f"      ✓ Correctly rejected temporal JOIN: {e}")
            else:
                print(f"      ? Unexpected error: {e}")

        print("\n" + "=" * 60)
        print("✅ TEMPORAL QUERY TESTS COMPLETED!")
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