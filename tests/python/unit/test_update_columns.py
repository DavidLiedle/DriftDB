#!/usr/bin/env python3
"""
Test UPDATE column data issue specifically
"""
import psycopg2
import sys

def test_update_columns():
    """Test UPDATE returning correct column data"""
    print("Testing UPDATE Column Data")
    print("=" * 40)

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        cur = conn.cursor()

        # Create test table
        try:
            cur.execute("DROP TABLE IF EXISTS update_test")
        except:
            pass  # Table might not exist, that's okay

        cur.execute("CREATE TABLE update_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        print("✅ Created test table")

        # Insert test data
        cur.execute("INSERT INTO update_test (id, name, age) VALUES (1, 'Alice', 25)")
        cur.execute("INSERT INTO update_test (id, name, age) VALUES (2, 'Bob', 30)")
        print("✅ Inserted test data")

        # Test 1: Check original data
        print("\n1. Checking original data...")
        cur.execute("SELECT id, name, age FROM update_test WHERE id = 1")
        row = cur.fetchone()
        print(f"   Original row: {row}")
        print(f"   Expected: (1, 'Alice', 25)")

        # Test 2: Update and check immediate response
        print("\n2. Testing UPDATE...")
        cur.execute("UPDATE update_test SET age = 26 WHERE id = 1")
        print("   ✅ UPDATE executed")

        # Test 3: Check data after update
        print("\n3. Checking data after UPDATE...")
        cur.execute("SELECT id, name, age FROM update_test WHERE id = 1")
        updated_row = cur.fetchone()
        print(f"   Updated row: {updated_row}")
        print(f"   Expected: (1, 'Alice', 26)")

        # Test 4: Check column order explicitly
        print("\n4. Testing column order...")
        cur.execute("SELECT name, id, age FROM update_test WHERE id = 1")
        reordered_row = cur.fetchone()
        print(f"   Reordered query result: {reordered_row}")
        print(f"   Expected: ('Alice', 1, 26)")

        # Test 5: Update with multiple columns
        print("\n5. Testing multiple column UPDATE...")
        cur.execute("UPDATE update_test SET name = 'Alice Smith', age = 27 WHERE id = 1")
        cur.execute("SELECT id, name, age FROM update_test WHERE id = 1")
        multi_updated_row = cur.fetchone()
        print(f"   Multi-update result: {multi_updated_row}")
        print(f"   Expected: (1, 'Alice Smith', 27)")

        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS update_test")
        except:
            pass

        print("\n✅ UPDATE COLUMN TEST COMPLETED")
        return 0

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    sys.exit(test_update_columns())