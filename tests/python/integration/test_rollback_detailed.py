#!/usr/bin/env python3
"""
Detailed test of ROLLBACK transaction behavior to understand the issue
"""
import psycopg2
import sys

def test_rollback_detailed():
    """Test ROLLBACK behavior in detail"""
    print("Testing ROLLBACK Transaction Behavior")
    print("=" * 50)

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

        # Clean up previous test data
        try:
            cur.execute("DROP TABLE IF EXISTS rollback_test")
        except:
            pass

        # Create test table
        cur.execute("CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, value TEXT)")
        print("✅ Created test table")

        # Insert initial data outside transaction
        cur.execute("INSERT INTO rollback_test (id, value) VALUES (1, 'initial')")
        print("✅ Inserted initial data (committed)")

        # Check initial state
        cur.execute("SELECT COUNT(*) FROM rollback_test")
        initial_count = cur.fetchone()[0]
        print(f"   Initial count: {initial_count}")

        # Start transaction (or use existing one)
        print("\n🔄 Starting transaction...")
        try:
            cur.execute("BEGIN")
        except psycopg2.errors.SyntaxError as e:
            if "Transaction already active" in str(e):
                print("   ℹ️  Transaction already active - using existing transaction")
            else:
                raise

        # Insert data within transaction
        cur.execute("INSERT INTO rollback_test (id, value) VALUES (2, 'in_transaction')")
        print("✅ Inserted data within transaction")

        # Check count within transaction
        cur.execute("SELECT COUNT(*) FROM rollback_test")
        txn_count = cur.fetchone()[0]
        print(f"   Count within transaction: {txn_count}")

        # Test the current behavior: what happens when we check data in another connection?
        print("\n🔍 Testing isolation (new connection)...")
        conn2 = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        cur2 = conn2.cursor()

        # Check count from another connection (should be isolated)
        cur2.execute("SELECT COUNT(*) FROM rollback_test")
        other_count = cur2.fetchone()[0]
        print(f"   Count from other connection: {other_count}")

        if other_count == txn_count:
            print("   ❌ ISSUE: Changes visible to other connections (no isolation)")
        else:
            print("   ✅ Changes properly isolated")

        cur2.close()
        conn2.close()

        # Now test ROLLBACK
        print("\n⏪ Testing ROLLBACK...")
        cur.execute("ROLLBACK")
        print("✅ ROLLBACK executed")

        # Check count after rollback
        cur.execute("SELECT COUNT(*) FROM rollback_test")
        rollback_count = cur.fetchone()[0]
        print(f"   Count after ROLLBACK: {rollback_count}")

        # Expected behavior: should be back to initial_count (1)
        if rollback_count == initial_count:
            print("   ✅ ROLLBACK worked correctly!")
        else:
            print(f"   ❌ ROLLBACK failed: expected {initial_count}, got {rollback_count}")
            print("   ❌ This indicates the transaction system is not buffering writes")

        # Test data content
        print("\n📋 Checking data content after ROLLBACK...")
        cur.execute("SELECT id, value FROM rollback_test ORDER BY id")
        rows = cur.fetchall()
        print(f"   Rows after ROLLBACK: {rows}")

        expected_rows = [(1, 'initial')]
        if rows == expected_rows:
            print("   ✅ Data content correct after ROLLBACK")
        else:
            print(f"   ❌ Data content incorrect: expected {expected_rows}, got {rows}")

        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS rollback_test")
        except:
            pass

        # Determine success
        success = (rollback_count == initial_count and rows == expected_rows)

        if success:
            print("\n🎉 ROLLBACK TEST PASSED!")
            return 0
        else:
            print("\n❌ ROLLBACK TEST FAILED!")
            print("\nRoot Cause Analysis:")
            print("- Transaction manager exists but doesn't buffer writes")
            print("- INSERT/UPDATE operations commit immediately to engine")
            print("- ROLLBACK has nothing to roll back")
            print("\nFix needed: Buffer operations during transactions")
            return 1

    except Exception as e:
        print(f"\n❌ Test failed with exception: {e}")
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
    sys.exit(test_rollback_detailed())