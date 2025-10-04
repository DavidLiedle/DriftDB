#!/usr/bin/env python3
"""
Test column ordering behavior in detail
"""
import psycopg2
import sys

def test_column_ordering():
    """Test specific column ordering scenarios"""
    print("Testing Column Ordering Behavior")
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
            cur.execute("DROP TABLE IF EXISTS col_order_test")
        except:
            pass

        cur.execute("CREATE TABLE col_order_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)")
        print("✅ Created test table with columns: id, name, age, city")

        # Insert test data
        cur.execute("INSERT INTO col_order_test (id, name, age, city) VALUES (1, 'Alice', 25, 'NYC')")
        cur.execute("INSERT INTO col_order_test (id, name, age, city) VALUES (2, 'Bob', 30, 'LA')")
        print("✅ Inserted test data")

        # Test 1: SELECT * (should preserve table schema order)
        print("\n1. Testing SELECT * (table schema order)...")
        cur.execute("SELECT * FROM col_order_test WHERE id = 1")
        row1 = cur.fetchone()
        print(f"   Result: {row1}")
        print(f"   Expected: (1, 'Alice', 25, 'NYC')")

        # Test 2: Explicit column order
        print("\n2. Testing explicit column order...")
        cur.execute("SELECT name, city, age, id FROM col_order_test WHERE id = 1")
        row2 = cur.fetchone()
        print(f"   Result: {row2}")
        print(f"   Expected: ('Alice', 'NYC', 25, 1)")

        # Test 3: Different order
        print("\n3. Testing different column order...")
        cur.execute("SELECT city, id, name FROM col_order_test WHERE id = 1")
        row3 = cur.fetchone()
        print(f"   Result: {row3}")
        print(f"   Expected: ('NYC', 1, 'Alice')")

        # Test 4: Single column
        print("\n4. Testing single column...")
        cur.execute("SELECT name FROM col_order_test WHERE id = 1")
        row4 = cur.fetchone()
        print(f"   Result: {row4}")
        print(f"   Expected: ('Alice',)")

        # Analyze results
        issues = []

        # Check if results match expected patterns
        expected_1 = (1, 'Alice', 25, 'NYC')
        expected_2 = ('Alice', 'NYC', 25, 1)
        expected_3 = ('NYC', 1, 'Alice')
        expected_4 = ('Alice',)

        if row1 != expected_1:
            issues.append(f"SELECT * order wrong: got {row1}, expected {expected_1}")

        if row2 != expected_2:
            issues.append(f"Explicit order wrong: got {row2}, expected {expected_2}")

        if row3 != expected_3:
            issues.append(f"Different order wrong: got {row3}, expected {expected_3}")

        if row4 != expected_4:
            issues.append(f"Single column wrong: got {row4}, expected {expected_4}")

        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS col_order_test")
        except:
            pass

        if issues:
            print(f"\n❌ Column Ordering Issues Found:")
            for issue in issues:
                print(f"   • {issue}")
            print(f"\nRoot cause: Results likely returned in alphabetical column order")
            return 1
        else:
            print(f"\n✅ All column ordering tests passed!")
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
    sys.exit(test_column_ordering())