#!/usr/bin/env python3
"""
Test that the previously broken features now actually work:
1. Timestamp-based temporal queries
2. Constraints validation
3. AUTO_INCREMENT functionality
"""
import psycopg2
import time
from datetime import datetime, timedelta
import sys

def test_temporal_queries_with_timestamps(cur):
    """Test that timestamp-based temporal queries actually work now"""
    print("\n=== Testing Timestamp-Based Temporal Queries ===")

    try:
        # Create test table
        print("Creating test table...")
        cur.execute("""
            CREATE TABLE temporal_test (
                id INT PRIMARY KEY,
                value VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert data with delays to create different timestamps
        print("Inserting data at different times...")

        # Record 1
        before_insert1 = datetime.utcnow()
        cur.execute("INSERT INTO temporal_test (id, value) VALUES (1, 'first')")
        after_insert1 = datetime.utcnow()
        time.sleep(1)  # Wait 1 second

        # Record 2
        before_insert2 = datetime.utcnow()
        cur.execute("INSERT INTO temporal_test (id, value) VALUES (2, 'second')")
        after_insert2 = datetime.utcnow()
        time.sleep(1)  # Wait 1 second

        # Record 3
        cur.execute("INSERT INTO temporal_test (id, value) VALUES (3, 'third')")

        # Test time-travel to before any inserts
        print("\nTesting AS OF before any data...")
        timestamp_before_all = (before_insert1 - timedelta(seconds=1)).isoformat() + 'Z'
        query = f"SELECT * FROM temporal_test AS OF '{timestamp_before_all}'"

        try:
            cur.execute(query)
            rows = cur.fetchall()
            if len(rows) == 0:
                print(f"  ✓ AS OF '{timestamp_before_all}' correctly shows no data")
            else:
                print(f"  ✗ AS OF timestamp incorrectly returned {len(rows)} rows")
                return False
        except psycopg2.Error as e:
            if "No data found before timestamp" in str(e):
                print(f"  ✓ Correctly reported no data before timestamp")
            else:
                print(f"  ✗ Unexpected error: {e}")
                return False

        # Test time-travel to after first insert
        print("\nTesting AS OF after first insert...")
        timestamp_after_first = after_insert1.isoformat() + 'Z'
        query = f"SELECT * FROM temporal_test AS OF '{timestamp_after_first}'"

        cur.execute(query)
        rows = cur.fetchall()
        if len(rows) == 1:
            print(f"  ✓ AS OF '{timestamp_after_first}' correctly shows 1 row")
        else:
            print(f"  ✗ Expected 1 row, got {len(rows)}")
            return False

        # Test time-travel to after second insert
        print("\nTesting AS OF after second insert...")
        timestamp_after_second = after_insert2.isoformat() + 'Z'
        query = f"SELECT * FROM temporal_test AS OF '{timestamp_after_second}'"

        cur.execute(query)
        rows = cur.fetchall()
        if len(rows) == 2:
            print(f"  ✓ AS OF '{timestamp_after_second}' correctly shows 2 rows")
        else:
            print(f"  ✗ Expected 2 rows, got {len(rows)}")
            return False

        print("✅ Timestamp-based temporal queries are WORKING!")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def test_constraints_enforcement(cur):
    """Test that constraints actually validate data"""
    print("\n=== Testing Constraint Enforcement ===")

    try:
        # Create tables with various constraints
        print("Creating tables with constraints...")

        cur.execute("""
            CREATE TABLE constraint_test (
                id INT PRIMARY KEY,
                email VARCHAR NOT NULL,
                age INT CHECK (age >= 18),
                status VARCHAR DEFAULT 'pending',
                department_id INT
            )
        """)

        # Test NOT NULL constraint
        print("\nTesting NOT NULL constraint...")
        try:
            cur.execute("INSERT INTO constraint_test (id, age) VALUES (1, 25)")
            print("  ✗ NOT NULL constraint not enforced!")
            return False
        except psycopg2.Error as e:
            if "NOT NULL" in str(e) or "null" in str(e).lower():
                print(f"  ✓ NOT NULL constraint enforced: {e}")
            else:
                print(f"  ? Unexpected error: {e}")

        # Test CHECK constraint
        print("\nTesting CHECK constraint...")
        try:
            cur.execute("INSERT INTO constraint_test (id, email, age) VALUES (2, 'test@example.com', 16)")
            print("  ✗ CHECK constraint not enforced!")
            return False
        except psycopg2.Error as e:
            if "CHECK" in str(e) or "constraint" in str(e).lower():
                print(f"  ✓ CHECK constraint enforced: {e}")
            else:
                print(f"  ? Unexpected error: {e}")

        # Test DEFAULT value
        print("\nTesting DEFAULT value...")
        cur.execute("INSERT INTO constraint_test (id, email, age) VALUES (3, 'user@example.com', 25)")
        cur.execute("SELECT status FROM constraint_test WHERE id = 3")
        status = cur.fetchone()[0]

        if status == 'pending':
            print(f"  ✓ DEFAULT value applied: {status}")
        else:
            print(f"  ✗ DEFAULT value not applied, got: {status}")
            return False

        print("✅ Constraints are WORKING!")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_auto_increment(cur):
    """Test that AUTO_INCREMENT actually generates IDs"""
    print("\n=== Testing AUTO_INCREMENT ===")

    try:
        # Create sequence and table
        print("Creating sequence and table with AUTO_INCREMENT...")

        cur.execute("CREATE SEQUENCE test_id_seq START WITH 1000")
        cur.execute("""
            CREATE TABLE auto_inc_test (
                id INT PRIMARY KEY DEFAULT nextval('test_id_seq'),
                name VARCHAR
            )
        """)

        # Insert without specifying ID
        print("Inserting records without specifying ID...")
        cur.execute("INSERT INTO auto_inc_test (name) VALUES ('Alice')")
        cur.execute("INSERT INTO auto_inc_test (name) VALUES ('Bob')")
        cur.execute("INSERT INTO auto_inc_test (name) VALUES ('Charlie')")

        # Check generated IDs
        cur.execute("SELECT id, name FROM auto_inc_test ORDER BY id")
        rows = cur.fetchall()

        print(f"  Generated IDs: {[row[0] for row in rows]}")

        if rows[0][0] == 1000 and rows[1][0] == 1001 and rows[2][0] == 1002:
            print("  ✓ AUTO_INCREMENT working correctly")
        else:
            print(f"  ✗ Unexpected IDs: {[row[0] for row in rows]}")
            return False

        print("✅ AUTO_INCREMENT is WORKING!")
        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def main():
    print("=" * 70)
    print("TESTING FIXED FEATURES IN DRIFTDB")
    print("=" * 70)

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="driftdb",
            user="driftdb",
            password=""
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("✓ Connected to DriftDB\n")

        # Run tests
        results = {
            "Temporal Queries (Timestamps)": test_temporal_queries_with_timestamps(cur),
            "Constraint Enforcement": test_constraints_enforcement(cur),
            "AUTO_INCREMENT": test_auto_increment(cur)
        }

        # Summary
        print("\n" + "=" * 70)
        print("TEST RESULTS SUMMARY:")
        print("-" * 70)

        for test_name, passed in results.items():
            status = "✅ WORKING" if passed else "❌ STILL BROKEN"
            print(f"{test_name:30} : {status}")

        all_passed = all(results.values())
        print("=" * 70)

        if all_passed:
            print("\n🎉 ALL PREVIOUSLY BROKEN FEATURES ARE NOW WORKING!")
            return 0
        else:
            print("\n⚠️ Some features are still broken")
            return 1

    except Exception as e:
        print(f"✗ Failed to connect or test: {e}")
        return 1
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    sys.exit(main())