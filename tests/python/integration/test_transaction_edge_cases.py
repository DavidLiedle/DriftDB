#!/usr/bin/env python3
"""
Comprehensive transaction edge case tests for DriftDB
Tests advanced transaction scenarios, savepoints, nested operations, etc.
"""

import psycopg2
import sys

def test_transaction_edge_cases():
    """Test various transaction edge cases and scenarios"""

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

        # Cleanup
        try:
            cur.execute("DROP TABLE IF EXISTS txn_test")
        except:
            pass

        cur.execute("""
            CREATE TABLE txn_test (
                id INTEGER PRIMARY KEY,
                value TEXT,
                amount INTEGER
            )
        """)
        print("✓ Created test table")

        passed = 0
        failed = 0

        # Test 1: Empty transaction (BEGIN then COMMIT without operations)
        print("\n=== Test 1: Empty transaction ===")
        try:
            cur.execute("BEGIN")
            cur.execute("COMMIT")
            print("✓ Empty transaction succeeded")
            passed += 1
        except Exception as e:
            print(f"✗ Empty transaction failed: {e}")
            failed += 1

        # Test 2: Multiple COMMITs (second should be no-op or error gracefully)
        print("\n=== Test 2: Multiple operations in one transaction ===")
        try:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (1, 'first', 100)")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (2, 'second', 200)")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (3, 'third', 300)")
            cur.execute("COMMIT")

            cur.execute("SELECT COUNT(*) FROM txn_test")
            count = cur.fetchone()[0]
            if count == 3:
                print(f"✓ Multiple inserts in transaction succeeded (count={count})")
                passed += 1
            else:
                print(f"✗ Expected 3 rows, got {count}")
                failed += 1
        except Exception as e:
            print(f"✗ Multiple operations test failed: {e}")
            failed += 1

        # Test 3: ROLLBACK after partial operations
        print("\n=== Test 3: ROLLBACK after partial operations ===")
        try:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (4, 'fourth', 400)")
            cur.execute("UPDATE txn_test SET amount = 999 WHERE id = 1")
            cur.execute("DELETE FROM txn_test WHERE id = 2")
            cur.execute("ROLLBACK")

            # Verify nothing changed
            cur.execute("SELECT COUNT(*) FROM txn_test")
            count = cur.fetchone()[0]
            cur.execute("SELECT amount FROM txn_test WHERE id = 1")
            amount = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM txn_test WHERE id = 2")
            id2_exists = cur.fetchone()[0]

            if count == 3 and amount == 100 and id2_exists == 1:
                print(f"✓ Partial ROLLBACK succeeded (count={count}, amount={amount}, id2_exists={id2_exists})")
                passed += 1
            else:
                print(f"✗ ROLLBACK verification failed: count={count}, amount={amount}, id2_exists={id2_exists}")
                failed += 1
        except Exception as e:
            print(f"✗ Partial ROLLBACK test failed: {e}")
            failed += 1

        # Test 4: Transaction with WHERE clause affecting 0 rows
        print("\n=== Test 4: Transaction with WHERE affecting 0 rows ===")
        try:
            cur.execute("BEGIN")
            cur.execute("UPDATE txn_test SET value = 'updated' WHERE id = 999")  # Non-existent
            cur.execute("DELETE FROM txn_test WHERE id = 888")  # Non-existent
            cur.execute("COMMIT")

            cur.execute("SELECT COUNT(*) FROM txn_test")
            count = cur.fetchone()[0]
            if count == 3:
                print(f"✓ Zero-row operations in transaction succeeded (count={count})")
                passed += 1
            else:
                print(f"✗ Expected 3 rows, got {count}")
                failed += 1
        except Exception as e:
            print(f"✗ Zero-row operations test failed: {e}")
            failed += 1

        # Test 5: Interleaved reads and writes in transaction
        print("\n=== Test 5: Interleaved reads and writes ===")
        try:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (10, 'ten', 1000)")
            cur.execute("SELECT COUNT(*) FROM txn_test")
            count_in_txn = cur.fetchone()[0]
            cur.execute("UPDATE txn_test SET amount = 1001 WHERE id = 10")
            cur.execute("SELECT amount FROM txn_test WHERE id = 10")
            amount_result = cur.fetchone()
            amount_in_txn = amount_result[0] if amount_result else None
            cur.execute("ROLLBACK")

            cur.execute("SELECT COUNT(*) FROM txn_test WHERE id = 10")
            count_after = cur.fetchone()[0]

            if count_after == 0:
                print(f"✓ Interleaved operations succeeded (in_txn={count_in_txn}, after_rollback={count_after})")
                passed += 1
            else:
                print(f"✗ Row still exists after ROLLBACK")
                failed += 1
        except Exception as e:
            print(f"✗ Interleaved operations test failed: {e}")
            # Try to rollback if transaction is active
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            failed += 1

        # Test 6: UPDATE with complex WHERE clause in transaction
        print("\n=== Test 6: UPDATE with complex WHERE in transaction ===")
        try:
            cur.execute("BEGIN")
            cur.execute("UPDATE txn_test SET value = 'high' WHERE amount > 150")
            cur.execute("ROLLBACK")

            cur.execute("SELECT value FROM txn_test WHERE id = 2")
            value = cur.fetchone()[0]
            if value == 'second':
                print(f"✓ Complex WHERE UPDATE+ROLLBACK succeeded (value='{value}')")
                passed += 1
            else:
                print(f"✗ Expected 'second', got '{value}'")
                failed += 1
        except Exception as e:
            print(f"✗ Complex WHERE test failed: {e}")
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            failed += 1

        # Test 7: Multiple UPDATEs to same row in transaction
        print("\n=== Test 7: Multiple UPDATEs to same row ===")
        try:
            cur.execute("BEGIN")
            cur.execute("UPDATE txn_test SET amount = 150 WHERE id = 1")
            cur.execute("UPDATE txn_test SET amount = 160 WHERE id = 1")
            cur.execute("UPDATE txn_test SET amount = 170 WHERE id = 1")
            cur.execute("COMMIT")

            cur.execute("SELECT amount FROM txn_test WHERE id = 1")
            amount = cur.fetchone()[0]
            if amount == 170:
                print(f"✓ Multiple UPDATEs succeeded (final amount={amount})")
                passed += 1
            else:
                print(f"✗ Expected 170, got {amount}")
                failed += 1
        except Exception as e:
            print(f"✗ Multiple UPDATEs test failed: {e}")
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            failed += 1

        # Test 8: DELETE then INSERT same ID in transaction
        print("\n=== Test 8: DELETE then INSERT same ID ===")
        try:
            cur.execute("BEGIN")
            cur.execute("DELETE FROM txn_test WHERE id = 3")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (3, 'new_third', 999)")
            cur.execute("COMMIT")

            cur.execute("SELECT value FROM txn_test WHERE id = 3")
            value = cur.fetchone()[0]
            if value == 'new_third':
                print(f"✓ DELETE+INSERT same ID succeeded (value='{value}')")
                passed += 1
            else:
                print(f"✗ Expected 'new_third', got '{value}'")
                failed += 1
        except Exception as e:
            print(f"✗ DELETE+INSERT test failed: {e}")
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            failed += 1

        # Test 9: ROLLBACK of DELETE then INSERT
        print("\n=== Test 9: ROLLBACK DELETE+INSERT ===")
        try:
            cur.execute("SELECT value FROM txn_test WHERE id = 2")
            original_value = cur.fetchone()[0]

            cur.execute("BEGIN")
            cur.execute("DELETE FROM txn_test WHERE id = 2")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (2, 'replaced', 777)")
            cur.execute("ROLLBACK")

            cur.execute("SELECT value FROM txn_test WHERE id = 2")
            value_after = cur.fetchone()[0]
            if value_after == original_value:
                print(f"✓ ROLLBACK DELETE+INSERT succeeded (restored to '{value_after}')")
                passed += 1
            else:
                print(f"✗ Expected '{original_value}', got '{value_after}'")
                failed += 1
        except Exception as e:
            print(f"✗ ROLLBACK DELETE+INSERT test failed: {e}")
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            failed += 1

        # Test 10: Transaction with all DML operations mixed
        print("\n=== Test 10: Mixed DML operations ===")
        try:
            cur.execute("SELECT COUNT(*) FROM txn_test")
            initial_count = cur.fetchone()[0]

            cur.execute("BEGIN")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (20, 'twenty', 2000)")
            cur.execute("UPDATE txn_test SET value = 'updated_first' WHERE id = 1")
            cur.execute("DELETE FROM txn_test WHERE id = 3")
            cur.execute("INSERT INTO txn_test (id, value, amount) VALUES (21, 'twenty-one', 2100)")
            cur.execute("UPDATE txn_test SET amount = 999 WHERE id = 2")
            cur.execute("COMMIT")

            cur.execute("SELECT COUNT(*) FROM txn_test")
            final_count = cur.fetchone()[0]
            cur.execute("SELECT value FROM txn_test WHERE id = 1")
            value1 = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM txn_test WHERE id = 3")
            id3_count = cur.fetchone()[0]
            cur.execute("SELECT amount FROM txn_test WHERE id = 2")
            amount2 = cur.fetchone()[0]

            # Verify: count increased by 1 (added 2, deleted 1), value1 updated, id=3 deleted, id=2 amount updated
            if final_count == initial_count + 1 and value1 == 'updated_first' and id3_count == 0 and amount2 == 999:
                print(f"✓ Mixed DML operations succeeded (count: {initial_count}→{final_count})")
                passed += 1
            else:
                print(f"✗ Mixed DML verification failed:")
                print(f"   Expected count: {initial_count + 1}, Got: {final_count}")
                print(f"   Expected value1: 'updated_first', Got: '{value1}'")
                print(f"   Expected id3_count: 0, Got: {id3_count}")
                print(f"   Expected amount2: 999, Got: {amount2}")
                failed += 1
        except Exception as e:
            print(f"✗ Mixed DML test failed: {e}")
            try:
                cur.execute("ROLLBACK")
            except:
                pass
            failed += 1

        # Cleanup
        try:
            cur.execute("DROP TABLE IF EXISTS txn_test")
        except:
            pass

        # Summary
        print("\n" + "="*60)
        print(f"Transaction Edge Case Tests Complete")
        print(f"Passed: {passed}/10")
        print(f"Failed: {failed}/10")
        print("="*60)

        cur.close()
        conn.close()

        return 0 if failed == 0 else 1

    except Exception as e:
        print(f"\n✗ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_transaction_edge_cases())
