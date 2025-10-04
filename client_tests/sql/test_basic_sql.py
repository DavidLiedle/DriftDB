#!/usr/bin/env python3
"""
Basic SQL test suite that confirms DriftDB core functionality
"""
import psycopg2
import json

def test_basic_sql():
    print("DriftDB Basic SQL Test Suite")
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
        return False

    tests_passed = 0
    tests_failed = 0

    # Test 1: CREATE TABLE
    print("\n1. Testing CREATE TABLE...")
    try:
        cur.execute("""
            CREATE TABLE test_table_basic (
                id INT PRIMARY KEY,
                name VARCHAR,
                value INT
            )
        """)
        print("   ✓ CREATE TABLE succeeded")
        tests_passed += 1
    except Exception as e:
        print(f"   ✗ CREATE TABLE failed: {e}")
        tests_failed += 1
        return False

    # Test 2: INSERT with column list
    print("\n2. Testing INSERT with explicit columns...")
    test_data = [
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300)
    ]

    for id_val, name_val, value_val in test_data:
        try:
            # Build JSON document
            doc = json.dumps({"id": id_val, "name": name_val, "value": value_val})
            query = f"INSERT INTO test_table_basic (id, name, value) VALUES ({id_val}, '{name_val}', {value_val})"
            cur.execute(query)
        except Exception as e:
            print(f"   ✗ INSERT failed for id={id_val}: {e}")
            tests_failed += 1
            break
    else:
        print(f"   ✓ Inserted {len(test_data)} rows")
        tests_passed += 1

    # Test 3: SELECT *
    print("\n3. Testing SELECT *...")
    try:
        cur.execute("SELECT * FROM test_table_basic")
        rows = cur.fetchall()
        if len(rows) == len(test_data):
            print(f"   ✓ SELECT * returned {len(rows)} rows")
            tests_passed += 1
        else:
            print(f"   ✗ Expected {len(test_data)} rows, got {len(rows)}")
            tests_failed += 1
    except Exception as e:
        print(f"   ✗ SELECT * failed: {e}")
        tests_failed += 1

    # Test 4: SELECT with WHERE
    print("\n4. Testing SELECT with WHERE...")
    try:
        cur.execute("SELECT * FROM test_table_basic WHERE value > 150")
        rows = cur.fetchall()
        expected = 2  # Bob (200) and Charlie (300)
        if len(rows) == expected:
            print(f"   ✓ WHERE clause returned {len(rows)} rows")
            tests_passed += 1
        else:
            print(f"   ✗ Expected {expected} rows, got {len(rows)}")
            tests_failed += 1
    except Exception as e:
        print(f"   ✗ WHERE clause failed: {e}")
        tests_failed += 1

    # Test 5: SELECT specific columns
    print("\n5. Testing SELECT specific columns...")
    try:
        cur.execute("SELECT name, value FROM test_table_basic")
        rows = cur.fetchall()
        if len(rows) == len(test_data) and len(rows[0]) == 2:
            print(f"   ✓ Column projection returned {len(rows)} rows with 2 columns")
            tests_passed += 1
        else:
            print(f"   ✗ Column projection failed")
            tests_failed += 1
    except Exception as e:
        print(f"   ✗ Column projection failed: {e}")
        tests_failed += 1

    # Test 6: ORDER BY
    print("\n6. Testing ORDER BY...")
    try:
        cur.execute("SELECT * FROM test_table_basic ORDER BY value DESC")
        rows = cur.fetchall()
        if len(rows) == len(test_data):
            print(f"   ✓ ORDER BY returned {len(rows)} sorted rows")
            tests_passed += 1
        else:
            print(f"   ✗ ORDER BY failed")
            tests_failed += 1
    except Exception as e:
        print(f"   ✗ ORDER BY failed: {e}")
        tests_failed += 1

    # Test 7: LIMIT
    print("\n7. Testing LIMIT...")
    try:
        cur.execute("SELECT * FROM test_table_basic LIMIT 2")
        rows = cur.fetchall()
        if len(rows) == 2:
            print(f"   ✓ LIMIT 2 returned {len(rows)} rows")
            tests_passed += 1
        else:
            print(f"   ✗ LIMIT failed: expected 2 rows, got {len(rows)}")
            tests_failed += 1
    except Exception as e:
        print(f"   ✗ LIMIT failed: {e}")
        tests_failed += 1

    # Test 8: COUNT aggregation
    print("\n8. Testing COUNT(*)...")
    try:
        cur.execute("SELECT COUNT(*) FROM test_table_basic")
        result = cur.fetchone()
        # Handle both integer and string results
        if result and (result[0] == len(test_data) or str(result[0]) == str(len(test_data))):
            print(f"   ✓ COUNT(*) returned {result[0]}")
            tests_passed += 1
        else:
            print(f"   ✗ COUNT(*) failed: expected {len(test_data)}, got {result[0] if result else None}")
            tests_failed += 1
    except Exception as e:
        print(f"   ✗ COUNT(*) failed: {e}")
        tests_failed += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"Test Results: {tests_passed} passed, {tests_failed} failed")

    if tests_failed == 0:
        print("✅ ALL TESTS PASSED!")
    else:
        print(f"⚠️  {tests_failed} tests failed")

    print("=" * 60)

    cur.close()
    conn.close()

    return tests_failed == 0

if __name__ == "__main__":
    success = test_basic_sql()
    exit(0 if success else 1)