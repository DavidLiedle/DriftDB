#!/usr/bin/env python3
"""
Test to specifically reproduce and debug column ordering issues
"""
import psycopg2
import sys

def test_column_ordering():
    """Test column ordering in SELECT results"""
    print("Testing Column Ordering in DriftDB...")

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Create table
        print("\n1. Creating test table...")
        try:
            cur.execute("""
                CREATE TABLE ordering_test (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    age INTEGER,
                    score FLOAT
                )
            """)
            print("✅ Table created")
        except psycopg2.Error as e:
            if "already exists" not in str(e):
                print(f"❌ Create failed: {e}")
                return 1

        # Insert test data
        print("\n2. Inserting test data...")
        cur.execute("INSERT INTO ordering_test (id, name, age, score) VALUES (1, 'Alice', 30, 95.5)")
        cur.execute("INSERT INTO ordering_test (id, name, age, score) VALUES (2, 'Bob', 25, 87.3)")
        cur.execute("INSERT INTO ordering_test (id, name, age, score) VALUES (3, 'Charlie', 35, 92.1)")
        print("✅ Data inserted")

        # Test 1: Simple SELECT with explicit columns
        print("\n3. Testing SELECT with explicit columns...")
        cur.execute("SELECT id, name, age, score FROM ordering_test ORDER BY id")
        rows = cur.fetchall()

        # Get column names
        col_names = [desc[0] for desc in cur.description]
        print(f"   Column names from cursor.description: {col_names}")

        print("   Results:")
        for row in rows:
            print(f"   Row data: {row}")
            for i, col_name in enumerate(col_names):
                print(f"     {col_name} = {row[i]}")

        # Test 2: SELECT *
        print("\n4. Testing SELECT * ...")
        cur.execute("SELECT * FROM ordering_test ORDER BY id")
        rows = cur.fetchall()

        col_names = [desc[0] for desc in cur.description]
        print(f"   Column names: {col_names}")
        print("   Results:")
        for row in rows:
            print(f"   {row}")

        # Test 3: Aggregate functions
        print("\n5. Testing aggregate functions...")
        cur.execute("SELECT COUNT(*) as count, AVG(age) as avg_age, MAX(score) as max_score FROM ordering_test")
        row = cur.fetchone()

        col_names = [desc[0] for desc in cur.description]
        print(f"   Column names: {col_names}")
        print(f"   Row data: {row}")

        # Map columns by position
        for i, col_name in enumerate(col_names):
            print(f"   {col_name} = {row[i]}")

        # Check if values match expected
        if row[0] == 3:  # count
            print("   ✅ COUNT is correct")
        else:
            print(f"   ❌ COUNT is wrong: expected 3, got {row[0]}")

        avg_expected = (30 + 25 + 35) / 3.0
        if abs(row[1] - avg_expected) < 0.01:  # avg_age
            print("   ✅ AVG(age) is correct")
        else:
            print(f"   ❌ AVG(age) is wrong: expected {avg_expected}, got {row[1]}")

        if row[2] == 95.5:  # max_score
            print("   ✅ MAX(score) is correct")
        else:
            print(f"   ❌ MAX(score) is wrong: expected 95.5, got {row[2]}")

        # Test 4: Complex SELECT with calculations
        print("\n6. Testing SELECT with calculations...")
        cur.execute("SELECT id, name, age * 2 as double_age, score / 10 as normalized FROM ordering_test WHERE id = 1")
        row = cur.fetchone()

        col_names = [desc[0] for desc in cur.description]
        print(f"   Column names: {col_names}")
        print(f"   Row data: {row}")

        for i, col_name in enumerate(col_names):
            print(f"   {col_name} = {row[i]}")

        cur.close()
        conn.close()
        return 0

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_column_ordering())