#!/usr/bin/env python3
"""Test all SQL improvements."""

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
    conn.autocommit = False  # Test with transactions
    print("✓ Connected successfully")

    cursor = conn.cursor()

    # Test 1: CREATE TABLE with proper response
    print("\n1. Testing CREATE TABLE...")
    try:
        cursor.execute("DROP TABLE IF EXISTS test_table")
        conn.commit()  # Commit the DROP
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """)
        conn.commit()  # Commit the CREATE TABLE
        print("✓ CREATE TABLE succeeded")
    except Exception as e:
        print(f"✗ CREATE TABLE failed: {e}")

    # Test 2: INSERT with proper response
    print("\n2. Testing INSERT...")
    try:
        cursor.execute("""
            INSERT INTO test_table (id, name, value)
            VALUES (1, 'test_item', 42.5)
        """)
        print("✓ INSERT succeeded")
    except Exception as e:
        print(f"✗ INSERT failed: {e}")

    # Test 3: UPDATE
    print("\n3. Testing UPDATE...")
    try:
        cursor.execute("""
            UPDATE test_table SET value = 100.0 WHERE id = 1
        """)
        print("✓ UPDATE succeeded")
    except Exception as e:
        print(f"✗ UPDATE failed: {e}")

    # Test 4: DELETE
    print("\n4. Testing DELETE...")
    try:
        cursor.execute("""
            INSERT INTO test_table (id, name, value)
            VALUES (2, 'to_delete', 50.0)
        """)
        cursor.execute("DELETE FROM test_table WHERE id = 2")
        print("✓ DELETE succeeded")
    except Exception as e:
        print(f"✗ DELETE failed: {e}")

    # Test 5: COMMIT
    print("\n5. Testing COMMIT...")
    try:
        conn.commit()
        print("✓ COMMIT succeeded")
    except Exception as e:
        print(f"✗ COMMIT failed: {e}")

    # Test 6: BEGIN and ROLLBACK
    print("\n6. Testing BEGIN and ROLLBACK...")
    try:
        cursor.execute("BEGIN")
        cursor.execute("""
            INSERT INTO test_table (id, name, value)
            VALUES (3, 'rollback_test', 75.0)
        """)
        conn.rollback()
        print("✓ BEGIN and ROLLBACK succeeded")
    except Exception as e:
        print(f"✗ BEGIN/ROLLBACK failed: {e}")

    # Test 7: Verify final state
    print("\n7. Verifying final state...")
    try:
        cursor.execute("SELECT * FROM test_table ORDER BY id")
        results = cursor.fetchall()
        print(f"✓ Final rows: {results}")
        # Should have row 1 with updated value, no row 2 (deleted), no row 3 (rolled back)
        if len(results) == 1 and results[0][2] == 100.0:
            print("✓ Data state is correct!")
        else:
            print("✗ Data state is unexpected")
    except Exception as e:
        print(f"✗ SELECT failed: {e}")

    cursor.close()
    conn.close()
    print("\n✓ All tests completed!")

except psycopg2.Error as e:
    print(f"✗ Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    sys.exit(1)