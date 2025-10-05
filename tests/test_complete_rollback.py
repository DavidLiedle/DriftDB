#!/usr/bin/env python3
"""
Comprehensive ROLLBACK test for all DML operations
Tests INSERT, UPDATE, and DELETE buffering in transactions
"""

import psycopg2
import sys

def test_complete_rollback():
    """Test that ROLLBACK works for all DML operations"""

    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("✓ Connected to DriftDB")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return False

    try:
        # Setup: Create table and insert test data
        print("\n=== Setup ===")
        try:
            cur.execute("DROP TABLE test_complete")
        except:
            pass

        cur.execute("""
            CREATE TABLE test_complete (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                status VARCHAR(50)
            )
        """)

        cur.execute("INSERT INTO test_complete (id, name, status) VALUES (1, 'Alice', 'active')")
        cur.execute("INSERT INTO test_complete (id, name, status) VALUES (2, 'Bob', 'active')")
        cur.execute("INSERT INTO test_complete (id, name, status) VALUES (3, 'Charlie', 'inactive')")
        print("✓ Created table with 3 rows")

        # Test 1: ROLLBACK prevents INSERT
        print("\n=== Test 1: ROLLBACK prevents INSERT ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("INSERT INTO test_complete (id, name, status) VALUES (99, 'Test', 'pending')")
        cur.execute("ROLLBACK")

        cur.execute("SELECT COUNT(*) FROM test_complete WHERE id = 99")
        count = cur.fetchone()[0]
        if count == 0:
            print("✓ ROLLBACK successfully prevented INSERT")
        else:
            print("✗ ROLLBACK failed for INSERT")
            return False

        # Test 2: ROLLBACK prevents UPDATE
        print("\n=== Test 2: ROLLBACK prevents UPDATE ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("UPDATE test_complete SET status = 'updated' WHERE id = 1")

        # Check value in transaction
        cur.execute("SELECT status FROM test_complete WHERE id = 1")
        status_in_txn = cur.fetchone()[0]
        print(f"  Status in transaction: {status_in_txn}")

        cur.execute("ROLLBACK")

        # Verify UPDATE was rolled back
        cur.execute("SELECT status FROM test_complete WHERE id = 1")
        status_after = cur.fetchone()[0]
        print(f"  Status after ROLLBACK: {status_after}")

        if status_after == 'active':
            print("✓ ROLLBACK successfully prevented UPDATE")
        else:
            print(f"✗ ROLLBACK failed for UPDATE - expected 'active', got '{status_after}'")
            return False

        # Test 3: ROLLBACK prevents DELETE
        print("\n=== Test 3: ROLLBACK prevents DELETE ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("DELETE FROM test_complete WHERE id = 2")
        cur.execute("ROLLBACK")

        cur.execute("SELECT COUNT(*) FROM test_complete")
        count = cur.fetchone()[0]
        if count == 3:
            print("✓ ROLLBACK successfully prevented DELETE")
        else:
            print(f"✗ ROLLBACK failed for DELETE - expected 3 rows, got {count}")
            return False

        # Test 4: COMMIT applies all DML operations
        print("\n=== Test 4: COMMIT applies all DML operations ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("INSERT INTO test_complete (id, name, status) VALUES (4, 'David', 'active')")
        cur.execute("UPDATE test_complete SET status = 'pending' WHERE id = 1")
        cur.execute("DELETE FROM test_complete WHERE id = 3")
        cur.execute("COMMIT")

        # Verify INSERT
        cur.execute("SELECT COUNT(*) FROM test_complete WHERE id = 4")
        if cur.fetchone()[0] != 1:
            print("✗ COMMIT failed to apply INSERT")
            return False

        # Verify UPDATE
        cur.execute("SELECT status FROM test_complete WHERE id = 1")
        if cur.fetchone()[0] != 'pending':
            print("✗ COMMIT failed to apply UPDATE")
            return False

        # Verify DELETE
        cur.execute("SELECT COUNT(*) FROM test_complete WHERE id = 3")
        if cur.fetchone()[0] != 0:
            print("✗ COMMIT failed to apply DELETE")
            return False

        print("✓ COMMIT successfully applied all DML operations")

        # Test 5: Mixed operations with ROLLBACK
        print("\n=== Test 5: Mixed operations with ROLLBACK ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("INSERT INTO test_complete (id, name, status) VALUES (5, 'Eve', 'test')")
        cur.execute("UPDATE test_complete SET name = 'Bobby' WHERE id = 2")
        cur.execute("DELETE FROM test_complete WHERE id = 4")
        cur.execute("ROLLBACK")

        # Verify nothing changed
        cur.execute("SELECT COUNT(*) FROM test_complete WHERE id = 5")
        if cur.fetchone()[0] != 0:
            print("✗ ROLLBACK failed - INSERT not prevented")
            return False

        cur.execute("SELECT name FROM test_complete WHERE id = 2")
        if cur.fetchone()[0] != 'Bob':
            print("✗ ROLLBACK failed - UPDATE not prevented")
            return False

        cur.execute("SELECT COUNT(*) FROM test_complete WHERE id = 4")
        if cur.fetchone()[0] != 1:
            print("✗ ROLLBACK failed - DELETE not prevented")
            return False

        print("✓ ROLLBACK successfully prevented mixed DML operations")

        print("\n" + "="*60)
        print("✓ ALL ROLLBACK TESTS PASSED!")
        print("  - INSERT buffering: ✓")
        print("  - UPDATE buffering: ✓")
        print("  - DELETE buffering: ✓")
        print("  - COMMIT execution: ✓")
        print("  - Mixed operations: ✓")
        print("="*60)
        return True

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    success = test_complete_rollback()
    sys.exit(0 if success else 1)
