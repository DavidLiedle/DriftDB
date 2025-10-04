#!/usr/bin/env python3
"""
DriftDB Integration Test Runner

Executes SQL test cases and validates results.
"""

import psycopg2
import sys
import time
import subprocess
import os
from typing import List, Tuple, Any

class TestRunner:
    def __init__(self, host='localhost', port=5433, database='test_db', user='test'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.conn = None
        self.cur = None
        self.passed = 0
        self.failed = 0
        self.test_results = []

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user
            )
            self.cur = self.conn.cursor()
            print(f"✓ Connected to DriftDB at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False

    def execute_test(self, test_name: str, sql: str, expected_result=None, should_fail=False):
        """Execute a single test case"""
        print(f"\n  Testing: {test_name}")
        try:
            start_time = time.time()
            self.cur.execute(sql)

            # Fetch results if it's a SELECT query
            if sql.strip().upper().startswith('SELECT'):
                result = self.cur.fetchall()
            else:
                result = None
                self.conn.commit()

            elapsed = (time.time() - start_time) * 1000  # Convert to ms

            if should_fail:
                self.failed += 1
                self.test_results.append((test_name, 'FAIL', f'Expected failure but succeeded'))
                print(f"    ✗ Expected failure but succeeded")
                return False

            # Validate result if expected result is provided
            if expected_result is not None and result != expected_result:
                self.failed += 1
                self.test_results.append((test_name, 'FAIL', f'Result mismatch: got {result}, expected {expected_result}'))
                print(f"    ✗ Result mismatch")
                print(f"      Expected: {expected_result}")
                print(f"      Got: {result}")
                return False

            self.passed += 1
            self.test_results.append((test_name, 'PASS', f'{elapsed:.2f}ms'))
            print(f"    ✓ Passed ({elapsed:.2f}ms)")
            return True

        except Exception as e:
            if should_fail:
                self.passed += 1
                self.test_results.append((test_name, 'PASS', 'Failed as expected'))
                print(f"    ✓ Failed as expected: {e}")
                return True
            else:
                self.failed += 1
                self.test_results.append((test_name, 'FAIL', str(e)))
                print(f"    ✗ Error: {e}")
                return False

    def run_basic_tests(self):
        """Run basic SQL functionality tests"""
        print("\n═══ Basic SQL Tests ═══")

        # Test CREATE TABLE
        self.execute_test(
            "CREATE TABLE",
            """CREATE TABLE test_basic (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                value DECIMAL(10,2)
            )"""
        )

        # Test INSERT
        self.execute_test(
            "INSERT single row",
            "INSERT INTO test_basic (id, name, value) VALUES (1, 'Test', 123.45)"
        )

        # Test SELECT
        self.execute_test(
            "SELECT with WHERE",
            "SELECT name, value FROM test_basic WHERE id = 1",
            expected_result=[('Test', 123.45)]
        )

        # Test UPDATE
        self.execute_test(
            "UPDATE",
            "UPDATE test_basic SET value = 999.99 WHERE id = 1"
        )

        # Verify UPDATE
        self.execute_test(
            "Verify UPDATE",
            "SELECT value FROM test_basic WHERE id = 1",
            expected_result=[(999.99,)]
        )

        # Test DELETE
        self.execute_test(
            "DELETE",
            "DELETE FROM test_basic WHERE id = 1"
        )

        # Verify DELETE
        self.execute_test(
            "Verify DELETE",
            "SELECT COUNT(*) FROM test_basic",
            expected_result=[(0,)]
        )

        # Cleanup
        self.execute_test("Drop test table", "DROP TABLE test_basic")

    def run_transaction_tests(self):
        """Test transaction support"""
        print("\n═══ Transaction Tests ═══")

        # Setup
        self.execute_test(
            "Setup transaction test table",
            """CREATE TABLE test_txn (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )"""
        )

        # Test COMMIT
        self.execute_test("BEGIN transaction", "BEGIN")
        self.execute_test("Insert in transaction", "INSERT INTO test_txn VALUES (1, 100)")
        self.execute_test("COMMIT transaction", "COMMIT")

        self.execute_test(
            "Verify committed data",
            "SELECT value FROM test_txn WHERE id = 1",
            expected_result=[(100,)]
        )

        # Test ROLLBACK
        self.execute_test("BEGIN for rollback", "BEGIN")
        self.execute_test("Insert for rollback", "INSERT INTO test_txn VALUES (2, 200)")
        self.execute_test("ROLLBACK transaction", "ROLLBACK")

        self.execute_test(
            "Verify rollback (should not find row)",
            "SELECT COUNT(*) FROM test_txn WHERE id = 2",
            expected_result=[(0,)]
        )

        # Cleanup
        self.execute_test("Cleanup transaction test", "DROP TABLE test_txn")

    def run_join_tests(self):
        """Test JOIN operations"""
        print("\n═══ JOIN Tests ═══")

        # Setup tables
        self.execute_test(
            "Create users table",
            """CREATE TABLE test_users (
                user_id INTEGER PRIMARY KEY,
                username VARCHAR(50)
            )"""
        )

        self.execute_test(
            "Create orders table",
            """CREATE TABLE test_orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount DECIMAL(10,2)
            )"""
        )

        # Insert test data
        self.execute_test(
            "Insert users",
            "INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
        )

        self.execute_test(
            "Insert orders",
            "INSERT INTO test_orders VALUES (1, 1, 100.00), (2, 1, 200.00), (3, 2, 150.00)"
        )

        # Test INNER JOIN
        self.execute_test(
            "INNER JOIN",
            """SELECT u.username, SUM(o.amount) AS total
               FROM test_users u
               INNER JOIN test_orders o ON u.user_id = o.user_id
               GROUP BY u.username
               ORDER BY u.username""",
            expected_result=[('Alice', 300.00), ('Bob', 150.00)]
        )

        # Test LEFT JOIN
        self.execute_test(
            "LEFT JOIN",
            """SELECT u.username, COUNT(o.order_id) AS order_count
               FROM test_users u
               LEFT JOIN test_orders o ON u.user_id = o.user_id
               GROUP BY u.username
               ORDER BY u.username""",
            expected_result=[('Alice', 2), ('Bob', 1), ('Charlie', 0)]
        )

        # Cleanup
        self.execute_test("Drop orders table", "DROP TABLE test_orders")
        self.execute_test("Drop users table", "DROP TABLE test_users")

    def run_aggregate_tests(self):
        """Test aggregate functions"""
        print("\n═══ Aggregate Function Tests ═══")

        # Setup
        self.execute_test(
            "Create aggregate test table",
            """CREATE TABLE test_agg (
                id INTEGER PRIMARY KEY,
                category VARCHAR(10),
                value INTEGER
            )"""
        )

        self.execute_test(
            "Insert aggregate test data",
            """INSERT INTO test_agg VALUES
               (1, 'A', 10), (2, 'A', 20), (3, 'B', 15),
               (4, 'B', 25), (5, 'C', 30)"""
        )

        # Test COUNT
        self.execute_test(
            "COUNT(*)",
            "SELECT COUNT(*) FROM test_agg",
            expected_result=[(5,)]
        )

        # Test SUM
        self.execute_test(
            "SUM",
            "SELECT SUM(value) FROM test_agg",
            expected_result=[(100,)]
        )

        # Test AVG
        self.execute_test(
            "AVG",
            "SELECT AVG(value) FROM test_agg",
            expected_result=[(20.0,)]
        )

        # Test GROUP BY
        self.execute_test(
            "GROUP BY with aggregates",
            """SELECT category, COUNT(*) AS cnt, SUM(value) AS total
               FROM test_agg
               GROUP BY category
               ORDER BY category""",
            expected_result=[('A', 2, 30), ('B', 2, 40), ('C', 1, 30)]
        )

        # Test HAVING
        self.execute_test(
            "HAVING clause",
            """SELECT category, SUM(value) AS total
               FROM test_agg
               GROUP BY category
               HAVING SUM(value) > 30
               ORDER BY category""",
            expected_result=[('B', 40)]
        )

        # Cleanup
        self.execute_test("Drop aggregate test table", "DROP TABLE test_agg")

    def print_summary(self):
        """Print test execution summary"""
        print("\n" + "═" * 50)
        print("TEST SUMMARY")
        print("═" * 50)

        total = self.passed + self.failed
        if total > 0:
            pass_rate = (self.passed / total) * 100

            print(f"\nTotal Tests: {total}")
            print(f"Passed: {self.passed} ({pass_rate:.1f}%)")
            print(f"Failed: {self.failed}")

            if self.failed > 0:
                print("\nFailed Tests:")
                for name, status, details in self.test_results:
                    if status == 'FAIL':
                        print(f"  - {name}: {details}")

            print("\n" + ("✅ ALL TESTS PASSED!" if self.failed == 0 else "❌ SOME TESTS FAILED"))
        else:
            print("No tests were executed")

        return self.failed == 0

def main():
    """Main test runner"""
    print("╔" + "═" * 48 + "╗")
    print("║        DriftDB Integration Test Suite          ║")
    print("╚" + "═" * 48 + "╝")

    # Check if server is running
    runner = TestRunner()

    if not runner.connect():
        print("\n❌ Could not connect to DriftDB server")
        print("   Please ensure the server is running:")
        print("   cargo run --bin driftdb-server --release")
        sys.exit(1)

    try:
        # Run test suites
        runner.run_basic_tests()
        runner.run_transaction_tests()
        runner.run_join_tests()
        runner.run_aggregate_tests()

        # Print summary
        success = runner.print_summary()

        # Cleanup and exit
        runner.conn.close()
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
        runner.conn.close()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        if runner.conn:
            runner.conn.close()
        sys.exit(1)

if __name__ == "__main__":
    main()