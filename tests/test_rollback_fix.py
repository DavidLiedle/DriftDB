#!/usr/bin/env python3
"""
Test improved ROLLBACK functionality
Verifies that ROLLBACK properly discards DELETE operations
"""

import psycopg2
import sys

def test_rollback_fix():
    """Test that ROLLBACK properly prevents DELETE from being applied"""

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
            cur.execute("DROP TABLE test_rollback")
        except:
            pass

        cur.execute("""
            CREATE TABLE test_rollback (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            )
        """)

        cur.execute("INSERT INTO test_rollback (id, name) VALUES (1, 'Alice')")
        cur.execute("INSERT INTO test_rollback (id, name) VALUES (2, 'Bob')")
        cur.execute("INSERT INTO test_rollback (id, name) VALUES (3, 'Charlie')")
        print("✓ Created table with 3 rows")

        # Test 1: ROLLBACK should prevent DELETE
        print("\n=== Test 1: ROLLBACK prevents DELETE ===")
        cur.execute("BEGIN TRANSACTION")
        print("  Started transaction")

        cur.execute("DELETE FROM test_rollback WHERE id = 2")
        print("  Deleted row with id=2")

        cur.execute("SELECT COUNT(*) FROM test_rollback")
        count_in_txn = cur.fetchone()[0]
        print(f"  Count in transaction: {count_in_txn}")

        cur.execute("ROLLBACK")
        print("  Rolled back transaction")

        # Verify the row was NOT deleted
        cur.execute("SELECT COUNT(*) FROM test_rollback")
        count_after_rollback = cur.fetchone()[0]
        print(f"  Count after ROLLBACK: {count_after_rollback}")

        if count_after_rollback == 3:
            print("✓ ROLLBACK successfully prevented DELETE!")
        else:
            print(f"✗ ROLLBACK failed - expected 3 rows, got {count_after_rollback}")
            return False

        # Test 2: COMMIT should apply DELETE
        print("\n=== Test 2: COMMIT applies DELETE ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("DELETE FROM test_rollback WHERE id = 3")
        cur.execute("COMMIT")

        cur.execute("SELECT COUNT(*) FROM test_rollback")
        count_after_commit = cur.fetchone()[0]
        print(f"  Count after COMMIT: {count_after_commit}")

        if count_after_commit == 2:
            print("✓ COMMIT successfully applied DELETE!")
        else:
            print(f"✗ COMMIT failed - expected 2 rows, got {count_after_commit}")
            return False

        # Test 3: ROLLBACK prevents INSERT
        print("\n=== Test 3: ROLLBACK prevents INSERT ===")
        cur.execute("BEGIN TRANSACTION")
        cur.execute("INSERT INTO test_rollback (id, name) VALUES (99, 'Test')")
        cur.execute("ROLLBACK")

        cur.execute("SELECT COUNT(*) FROM test_rollback WHERE id = 99")
        count_test = cur.fetchone()[0]

        if count_test == 0:
            print("✓ ROLLBACK successfully prevented INSERT!")
        else:
            print(f"✗ ROLLBACK failed for INSERT")
            return False

        print("\n" + "="*50)
        print("✓ ALL ROLLBACK TESTS PASSED!")
        print("="*50)
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
    success = test_rollback_fix()
    sys.exit(0 if success else 1)
