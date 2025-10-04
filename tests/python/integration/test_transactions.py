#!/usr/bin/env python3
"""
Test transaction support specifically
"""
import psycopg2
import sys

def test_transactions():
    """Test transaction commands work correctly"""
    print("Testing Transaction Support")
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
        conn.autocommit = True
        cur = conn.cursor()

        # Test 1: Basic Transaction Commands
        print("\n1. Testing basic transaction commands...")

        # BEGIN should work now
        try:
            cur.execute("BEGIN")
            print("   ✅ BEGIN command executed successfully")
        except Exception as e:
            print(f"   ❌ BEGIN failed: {e}")
            return 1

        # COMMIT should work now
        try:
            cur.execute("COMMIT")
            print("   ✅ COMMIT command executed successfully")
        except Exception as e:
            print(f"   ❌ COMMIT failed: {e}")
            return 1

        # Test 2: Transaction with actual operations
        print("\n2. Testing transaction with operations...")

        # Create table for testing
        try:
            cur.execute("DROP TABLE IF EXISTS txn_test")
        except:
            pass  # Table might not exist, that's okay
        cur.execute("CREATE TABLE txn_test (id INTEGER PRIMARY KEY, value TEXT)")
        print("   ✅ Created test table")

        # Test transaction with operations
        try:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO txn_test (id, value) VALUES (1, 'test1')")
            cur.execute("INSERT INTO txn_test (id, value) VALUES (2, 'test2')")
            cur.execute("COMMIT")
            print("   ✅ Transaction with operations completed")
        except Exception as e:
            print(f"   ❌ Transaction with operations failed: {e}")
            return 1

        # Verify data was committed
        cur.execute("SELECT COUNT(*) FROM txn_test")
        count = cur.fetchone()[0]
        if count == 2:
            print(f"   ✅ Data committed correctly (count: {count})")
        else:
            print(f"   ❌ Data not committed correctly (count: {count})")
            return 1

        # Test 3: ROLLBACK
        print("\n3. Testing ROLLBACK...")
        try:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO txn_test (id, value) VALUES (3, 'test3')")
            cur.execute("ROLLBACK")
            print("   ✅ ROLLBACK command executed successfully")
        except Exception as e:
            print(f"   ❌ ROLLBACK failed: {e}")
            return 1

        # Verify data was rolled back
        cur.execute("SELECT COUNT(*) FROM txn_test")
        count = cur.fetchone()[0]
        if count == 2:  # Should still be 2, not 3
            print(f"   ✅ Data rolled back correctly (count: {count})")
        else:
            print(f"   ❌ Data not rolled back correctly (count: {count})")
            return 1

        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS txn_test")
        except:
            pass  # Table cleanup - ignore errors

        print("\n✅ ALL TRANSACTION TESTS PASSED!")
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
    sys.exit(test_transactions())
