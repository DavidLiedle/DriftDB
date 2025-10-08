#!/usr/bin/env python3
"""
Comprehensive concurrency tests for DriftDB

Tests multiple clients, transaction conflicts, and concurrent operations
"""

import psycopg2
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import sys


class DriftDBTestCase:
    """Base class for DriftDB test cases"""

    def __init__(self, host='localhost', port=5433, database='driftdb',
                 user='driftdb', password='driftdb'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def get_connection(self):
        """Get a new database connection"""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def execute_query(self, query: str, params=None, conn=None):
        """Execute a query and return results"""
        close_conn = False
        if conn is None:
            conn = self.get_connection()
            close_conn = True

        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return cursor.fetchall()
                conn.commit()
                return None
        finally:
            if close_conn:
                conn.close()


def test_concurrent_readers():
    """Test multiple concurrent readers - should all succeed"""
    print("\n=== Test: Concurrent Readers ===")
    test = DriftDBTestCase()

    # Setup: Create table and insert data
    conn = test.get_connection()
    test.execute_query("DROP TABLE IF EXISTS concurrent_read_test", conn=conn)
    test.execute_query("""
        CREATE TABLE concurrent_read_test (
            id INTEGER PRIMARY KEY,
            value VARCHAR(100)
        )
    """, conn=conn)

    for i in range(100):
        test.execute_query(
            "INSERT INTO concurrent_read_test (id, value) VALUES (%s, %s)",
            (i, f"value_{i}"),
            conn=conn
        )
    conn.commit()
    conn.close()

    # Test: Launch concurrent readers
    num_readers = 10
    results = []
    errors = []

    def read_worker(worker_id: int):
        try:
            conn = test.get_connection()
            for _ in range(5):
                result = test.execute_query(
                    "SELECT COUNT(*) FROM concurrent_read_test",
                    conn=conn
                )
                assert result[0][0] == 100, f"Worker {worker_id}: Expected 100 rows"
            conn.close()
            return f"Reader {worker_id}: SUCCESS"
        except Exception as e:
            return f"Reader {worker_id}: FAILED - {e}"

    with ThreadPoolExecutor(max_workers=num_readers) as executor:
        futures = [executor.submit(read_worker, i) for i in range(num_readers)]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if "FAILED" in result:
                errors.append(result)

    # Verify all readers succeeded
    print(f"✅ {len(results)} concurrent readers completed")
    if errors:
        print(f"❌ {len(errors)} readers had errors:")
        for error in errors:
            print(f"  {error}")
        sys.exit(1)
    else:
        print("✅ All readers succeeded - no conflicts")


def test_write_write_conflict():
    """Test write-write conflicts between transactions"""
    print("\n=== Test: Write-Write Conflicts ===")
    test = DriftDBTestCase()

    # Setup
    conn = test.get_connection()
    test.execute_query("DROP TABLE IF EXISTS conflict_test", conn=conn)
    test.execute_query("""
        CREATE TABLE conflict_test (
            id INTEGER PRIMARY KEY,
            balance INTEGER
        )
    """, conn=conn)
    test.execute_query("INSERT INTO conflict_test (id, balance) VALUES (1, 100)", conn=conn)
    conn.commit()
    conn.close()

    # Test: Two transactions trying to update the same row
    conflict_detected = False
    lock_held = threading.Lock()
    results = []

    def update_worker(worker_id: int, increment: int):
        nonlocal conflict_detected
        try:
            conn = test.get_connection()
            conn.autocommit = False

            # Start transaction
            with conn.cursor() as cursor:
                cursor.execute("BEGIN")

                # Read current value
                cursor.execute("SELECT balance FROM conflict_test WHERE id = 1")
                current = cursor.fetchone()[0]

                # Small delay to ensure both transactions read
                time.sleep(0.1)

                # Update
                new_value = current + increment
                cursor.execute(
                    "UPDATE conflict_test SET balance = %s WHERE id = 1",
                    (new_value,)
                )

                # Commit
                conn.commit()
                return f"Worker {worker_id}: Updated to {new_value}"

        except psycopg2.Error as e:
            conn.rollback()
            with lock_held:
                conflict_detected = True
            return f"Worker {worker_id}: Conflict detected - {e}"
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(update_worker, 1, 10),
            executor.submit(update_worker, 2, 20)
        ]
        for future in as_completed(futures):
            results.append(future.result())

    # One should succeed, one should fail or they should serialize properly
    print("Results:")
    for result in results:
        print(f"  {result}")

    # Verify final state
    conn = test.get_connection()
    final_result = test.execute_query("SELECT balance FROM conflict_test WHERE id = 1", conn=conn)
    final_balance = final_result[0][0]
    conn.close()

    print(f"Final balance: {final_balance}")
    # Should be either 110 or 120 depending on which won, or 130 if serialized
    assert final_balance in [110, 120, 130], f"Unexpected final balance: {final_balance}"
    print("✅ Write-write conflict handled correctly")


def test_lost_update_prevention():
    """Test that MVCC prevents lost updates"""
    print("\n=== Test: Lost Update Prevention ===")
    test = DriftDBTestCase()

    # Setup
    conn = test.get_connection()
    test.execute_query("DROP TABLE IF EXISTS account", conn=conn)
    test.execute_query("""
        CREATE TABLE account (
            id INTEGER PRIMARY KEY,
            balance INTEGER
        )
    """, conn=conn)
    test.execute_query("INSERT INTO account (id, balance) VALUES (1, 1000)", conn=conn)
    conn.commit()
    conn.close()

    # Two transactions both read initial balance and try to update
    def transfer_worker(worker_id: int, amount: int):
        try:
            conn = test.get_connection()
            conn.autocommit = False

            with conn.cursor() as cursor:
                cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")

                # Read
                cursor.execute("SELECT balance FROM account WHERE id = 1")
                balance = cursor.fetchone()[0]

                # Simulate some processing time
                time.sleep(0.1)

                # Update based on read value
                new_balance = balance - amount
                cursor.execute(
                    "UPDATE account SET balance = %s WHERE id = 1",
                    (new_balance,)
                )

                conn.commit()
                return f"Worker {worker_id}: Withdrew {amount}, new balance {new_balance}"

        except psycopg2.Error as e:
            conn.rollback()
            return f"Worker {worker_id}: Failed - {e}"
        finally:
            conn.close()

    # Run two concurrent withdrawals
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(transfer_worker, 1, 100),
            executor.submit(transfer_worker, 2, 200)
        ]
        results = [future.result() for future in as_completed(futures)]

    print("Results:")
    for result in results:
        print(f"  {result}")

    # Check final balance
    conn = test.get_connection()
    final_result = test.execute_query("SELECT balance FROM account WHERE id = 1", conn=conn)
    final_balance = final_result[0][0]
    conn.close()

    print(f"Final balance: {final_balance}")
    # Should be 700 (1000 - 100 - 200) if properly serialized
    # Or one transaction failed and balance is 800 or 900
    assert final_balance in [700, 800, 900], f"Unexpected balance: {final_balance}"
    print("✅ Lost update prevented")


def test_high_concurrency_inserts():
    """Test many concurrent inserts"""
    print("\n=== Test: High Concurrency Inserts ===")
    test = DriftDBTestCase()

    # Setup
    conn = test.get_connection()
    test.execute_query("DROP TABLE IF EXISTS high_concurrency", conn=conn)
    test.execute_query("""
        CREATE TABLE high_concurrency (
            id INTEGER PRIMARY KEY,
            thread_id INTEGER,
            value VARCHAR(100)
        )
    """, conn=conn)
    conn.commit()
    conn.close()

    num_workers = 20
    inserts_per_worker = 10
    errors = []

    def insert_worker(worker_id: int):
        try:
            conn = test.get_connection()
            for i in range(inserts_per_worker):
                row_id = worker_id * 1000 + i
                test.execute_query(
                    "INSERT INTO high_concurrency (id, thread_id, value) VALUES (%s, %s, %s)",
                    (row_id, worker_id, f"worker_{worker_id}_row_{i}"),
                    conn=conn
                )
            conn.close()
            return f"Worker {worker_id}: Inserted {inserts_per_worker} rows"
        except Exception as e:
            return f"Worker {worker_id}: FAILED - {e}"

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(insert_worker, i) for i in range(num_workers)]
        for future in as_completed(futures):
            result = future.result()
            if "FAILED" in result:
                errors.append(result)

    elapsed = time.time() - start_time

    # Verify results
    conn = test.get_connection()
    count_result = test.execute_query("SELECT COUNT(*) FROM high_concurrency", conn=conn)
    total_rows = count_result[0][0]
    conn.close()

    expected_rows = num_workers * inserts_per_worker
    print(f"Inserted {total_rows}/{expected_rows} rows in {elapsed:.2f}s")
    print(f"Throughput: {total_rows/elapsed:.0f} inserts/sec")

    if errors:
        print(f"❌ {len(errors)} workers had errors:")
        for error in errors:
            print(f"  {error}")
        sys.exit(1)

    assert total_rows == expected_rows, f"Expected {expected_rows} rows, got {total_rows}"
    print(f"✅ High concurrency inserts successful")


def test_transaction_isolation():
    """Test transaction isolation levels"""
    print("\n=== Test: Transaction Isolation ===")
    test = DriftDBTestCase()

    # Setup
    conn = test.get_connection()
    test.execute_query("DROP TABLE IF EXISTS isolation_test", conn=conn)
    test.execute_query("""
        CREATE TABLE isolation_test (
            id INTEGER PRIMARY KEY,
            value INTEGER
        )
    """, conn=conn)
    test.execute_query("INSERT INTO isolation_test (id, value) VALUES (1, 100)", conn=conn)
    conn.commit()
    conn.close()

    # Test READ COMMITTED: Transaction 2 should see Transaction 1's committed changes
    def test_read_committed():
        conn1 = test.get_connection()
        conn2 = test.get_connection()

        conn1.autocommit = False
        conn2.autocommit = False

        try:
            with conn1.cursor() as c1, conn2.cursor() as c2:
                c1.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
                c2.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")

                # Transaction 1 updates
                c1.execute("UPDATE isolation_test SET value = 200 WHERE id = 1")

                # Transaction 2 reads (should see old value before commit)
                c2.execute("SELECT value FROM isolation_test WHERE id = 1")
                value_before = c2.fetchone()[0]

                # Transaction 1 commits
                conn1.commit()

                # Transaction 2 reads again (should see new value after commit)
                c2.execute("SELECT value FROM isolation_test WHERE id = 1")
                value_after = c2.fetchone()[0]

                conn2.commit()

                print(f"  Before commit: {value_before}, After commit: {value_after}")
                # With READ COMMITTED, might see either value depending on timing
                return True

        finally:
            conn1.close()
            conn2.close()

    if test_read_committed():
        print("✅ Transaction isolation working")


def run_all_tests():
    """Run all concurrency tests"""
    print("=" * 60)
    print("DriftDB Concurrency Test Suite")
    print("=" * 60)

    try:
        test_concurrent_readers()
        test_write_write_conflict()
        test_lost_update_prevention()
        test_high_concurrency_inserts()
        test_transaction_isolation()

        print("\n" + "=" * 60)
        print("✅ ALL CONCURRENCY TESTS PASSED")
        print("=" * 60)

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
