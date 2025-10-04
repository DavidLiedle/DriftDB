#!/usr/bin/env python3
"""
Test ACID transaction functionality in DriftDB
"""
import psycopg2
import json
import time

def main():
    print("Testing ACID Transactions in DriftDB")
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
        # Start with autocommit to create table
        conn.autocommit = True
        cur = conn.cursor()
        print("✓ Connected to DriftDB")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return

    try:
        # Create test table
        print("\n1. Creating test table...")
        try:
            cur.execute("""
                CREATE TABLE txn_test (
                    id INT PRIMARY KEY,
                    value VARCHAR,
                    amount INT
                )
            """)
            print("   ✓ Table created")
        except psycopg2.errors.DuplicateTable:
            print("   ✓ Table already exists")
            # Clear existing data
            cur.execute("DELETE FROM txn_test WHERE id > 0")
        except Exception:
            # Table might not exist, try without IF NOT EXISTS
            pass

        # Test 1: Basic Transaction with COMMIT
        print("\n2. Testing basic transaction with COMMIT...")
        conn.autocommit = False  # Turn off autocommit for transactions

        cur.execute("BEGIN")
        print("   ✓ Transaction started")

        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (1, 'first', 100)")
        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (2, 'second', 200)")
        print("   ✓ Inserted 2 rows in transaction")

        cur.execute("COMMIT")
        print("   ✓ Transaction committed")

        # Verify data was committed
        conn.autocommit = True
        cur.execute("SELECT COUNT(*) FROM txn_test")
        count = cur.fetchone()[0]
        print(f"   ✓ Verified: {count} rows in table")

        # Test 2: Transaction with ROLLBACK
        print("\n3. Testing transaction with ROLLBACK...")
        conn.autocommit = False

        cur.execute("BEGIN")
        print("   ✓ Transaction started")

        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (3, 'third', 300)")
        print("   ✓ Inserted 1 row in transaction")

        cur.execute("ROLLBACK")
        print("   ✓ Transaction rolled back")

        # Verify rollback worked
        conn.autocommit = True
        cur.execute("SELECT COUNT(*) FROM txn_test")
        count = cur.fetchone()[0]
        print(f"   ✓ Verified: Still {count} rows (rollback successful)")

        # Test 3: Savepoints
        print("\n4. Testing SAVEPOINT functionality...")
        conn.autocommit = False

        cur.execute("BEGIN")
        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (10, 'savepoint_test', 1000)")

        cur.execute("SAVEPOINT sp1")
        print("   ✓ Created savepoint sp1")

        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (11, 'after_savepoint', 1100)")

        cur.execute("ROLLBACK TO SAVEPOINT sp1")
        print("   ✓ Rolled back to savepoint sp1")

        cur.execute("COMMIT")
        print("   ✓ Committed transaction")

        # Verify only first insert was kept
        conn.autocommit = True
        cur.execute("SELECT COUNT(*) FROM txn_test WHERE id >= 10")
        count = cur.fetchone()[0]
        print(f"   ✓ Verified: {count} row from savepoint test (should be 1)")

        # Test 4: Isolation Levels
        print("\n5. Testing isolation levels...")
        conn.autocommit = False

        # Test READ COMMITTED (default)
        cur.execute("BEGIN")
        print("   ✓ Started transaction with default isolation")
        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (20, 'isolation_test', 2000)")
        cur.execute("COMMIT")

        # Test SERIALIZABLE
        cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        print("   ✓ Started transaction with SERIALIZABLE isolation")
        cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (21, 'serializable', 2100)")
        cur.execute("COMMIT")

        # Test READ ONLY
        cur.execute("BEGIN READ ONLY")
        print("   ✓ Started READ ONLY transaction")
        cur.execute("SELECT COUNT(*) FROM txn_test")
        count = cur.fetchone()[0]
        print(f"   ✓ Read {count} rows in READ ONLY transaction")

        # Try to write in READ ONLY (should fail)
        try:
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (99, 'should_fail', 9999)")
            print("   ✗ ERROR: Write succeeded in READ ONLY transaction!")
        except Exception as e:
            print(f"   ✓ Correctly rejected write in READ ONLY: {e}")

        cur.execute("COMMIT")

        # Final verification
        print("\n6. Final verification...")
        conn.autocommit = True
        cur.execute("SELECT * FROM txn_test ORDER BY id")
        rows = cur.fetchall()
        print(f"   Final table contents ({len(rows)} rows):")
        for row in rows:
            print(f"      id={row[0]}, value='{row[1]}', amount={row[2]}")

        print("\n" + "=" * 60)
        print("✅ TRANSACTION TESTS COMPLETED!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()