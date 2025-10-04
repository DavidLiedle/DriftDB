#!/usr/bin/env python3
"""
Engine-WAL Integration Test for DriftDB

This test verifies that the Engine properly integrates with WAL for data durability.
It focuses on the integration between the Engine and TransactionManager that uses WAL.

Test Areas:
1. Engine operations create WAL entries
2. WAL recovery works after Engine restart
3. Transaction boundaries are respected
4. Data consistency after crash recovery
"""

import os
import sys
import time
import json
import subprocess
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, Optional

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not found. Install with: pip install psycopg2-binary")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EngineWALIntegrationTest:
    """Test Engine-WAL integration"""

    def __init__(self):
        self.temp_dir = None
        self.server_process = None
        self.server_port = 5434
        self.server_host = "127.0.0.1"

    def setup(self) -> bool:
        """Set up test environment"""
        try:
            # Create temporary directory
            self.temp_dir = Path(tempfile.mkdtemp(prefix="driftdb_engine_wal_test_"))
            logger.info(f"Test directory: {self.temp_dir}")
            return True
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

    def cleanup(self) -> None:
        """Clean up test environment"""
        if self.server_process:
            self.stop_server()

        if self.temp_dir and self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
                logger.info("Cleaned up test directory")
            except Exception as e:
                logger.error(f"Error cleaning up: {e}")

    def start_server(self) -> bool:
        """Start DriftDB server"""
        if self.server_process and self.server_process.poll() is None:
            return True

        cmd = [
            "cargo", "run", "--bin", "driftdb-server", "--",
            "--data-path", str(self.temp_dir),
            "--listen", f"{self.server_host}:{self.server_port}",
            "--http-listen", f"{self.server_host}:8081"
        ]

        logger.info("Starting DriftDB server...")

        try:
            self.server_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=Path(__file__).parent,
                env=dict(os.environ, RUST_LOG="debug")
            )

            # Wait for server to be ready
            for _ in range(30):  # 30 second timeout
                try:
                    conn = psycopg2.connect(
                        host=self.server_host,
                        port=self.server_port,
                        database="postgres",
                        user="postgres",
                        password="",
                        connect_timeout=1
                    )
                    conn.close()
                    logger.info("Server is ready")
                    return True
                except psycopg2.OperationalError:
                    time.sleep(1)

            logger.error("Server failed to start within timeout")
            return False

        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            return False

    def stop_server(self) -> None:
        """Stop DriftDB server"""
        if not self.server_process:
            return

        logger.info("Stopping server")
        try:
            self.server_process.terminate()
            self.server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("Server didn't stop gracefully, killing")
            self.server_process.kill()
            self.server_process.wait()
        finally:
            self.server_process = None

    def crash_server(self) -> None:
        """Simulate server crash"""
        if not self.server_process:
            return

        logger.info("Simulating server crash")
        try:
            self.server_process.kill()
            self.server_process.wait()
        except Exception as e:
            logger.error(f"Error crashing server: {e}")
        finally:
            self.server_process = None

    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection"""
        return psycopg2.connect(
            host=self.server_host,
            port=self.server_port,
            database="postgres",
            user="postgres",
            password="",
            autocommit=False
        )

    def execute_sql(self, sql: str, params=None, autocommit: bool = False):
        """Execute SQL and return results"""
        with self.get_connection() as conn:
            if autocommit:
                conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if cur.description:
                    return cur.fetchall()
                return None

    def get_wal_files(self) -> list:
        """Get WAL files from the data directory"""
        wal_dir = self.temp_dir / "wal"
        if not wal_dir.exists():
            return []
        return list(wal_dir.glob("*.log*"))

    def test_basic_engine_wal_integration(self) -> bool:
        """Test 1: Basic Engine-WAL integration"""
        logger.info("=== Test 1: Basic Engine-WAL Integration ===")

        try:
            if not self.start_server():
                return False

            # Create a table (Engine operation)
            self.execute_sql("""
                CREATE TABLE engine_wal_test (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                )
            """, autocommit=True)

            # Check that WAL files are created
            wal_files_before = self.get_wal_files()
            logger.info(f"WAL files before operations: {len(wal_files_before)}")

            # Insert data (should create WAL entries through TransactionManager)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO engine_wal_test (id, name, value)
                        VALUES (%s, %s, %s)
                    """, ("test1", "Test Record", 42))
                    conn.commit()

            # Check that WAL files exist and possibly grew
            wal_files_after = self.get_wal_files()
            logger.info(f"WAL files after operations: {len(wal_files_after)}")

            if not wal_files_after:
                logger.error("No WAL files found after operations")
                return False

            # Verify data exists
            result = self.execute_sql("SELECT COUNT(*) FROM engine_wal_test")
            if not result or result[0][0] != 1:
                logger.error("Data not found after insertion")
                return False

            logger.info("✓ Basic Engine-WAL integration test passed")
            return True

        except Exception as e:
            logger.error(f"Basic Engine-WAL integration test failed: {e}")
            return False

    def test_wal_recovery_after_restart(self) -> bool:
        """Test 2: WAL recovery after Engine restart"""
        logger.info("=== Test 2: WAL Recovery After Restart ===")

        try:
            if not self.start_server():
                return False

            # Create table and insert test data
            self.execute_sql("""
                CREATE TABLE recovery_test (
                    id TEXT PRIMARY KEY,
                    data TEXT,
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """, autocommit=True)

            test_data = [
                ("rec1", "First record"),
                ("rec2", "Second record"),
                ("rec3", "Third record")
            ]

            # Insert data in transactions
            for record_id, data in test_data:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO recovery_test (id, data)
                            VALUES (%s, %s)
                        """, (record_id, data))
                        conn.commit()

            # Verify data before restart
            result = self.execute_sql("SELECT COUNT(*) FROM recovery_test")
            if not result or result[0][0] != len(test_data):
                logger.error("Data not properly inserted before restart")
                return False

            logger.info("Data inserted successfully, stopping server")

            # Stop server gracefully
            self.stop_server()

            # Restart server (should trigger WAL recovery)
            logger.info("Restarting server to test WAL recovery")
            if not self.start_server():
                return False

            # Verify data recovered
            result = self.execute_sql("SELECT COUNT(*) FROM recovery_test")
            if not result or result[0][0] != len(test_data):
                logger.error(f"Data recovery failed: expected {len(test_data)}, got {result[0][0] if result else 0}")
                return False

            # Verify specific records
            for record_id, expected_data in test_data:
                result = self.execute_sql("""
                    SELECT data FROM recovery_test WHERE id = %s
                """, (record_id,))

                if not result or result[0][0] != expected_data:
                    logger.error(f"Record {record_id} not recovered correctly")
                    return False

            logger.info("✓ WAL recovery after restart test passed")
            return True

        except Exception as e:
            logger.error(f"WAL recovery test failed: {e}")
            return False

    def test_transaction_boundaries_in_wal(self) -> bool:
        """Test 3: Transaction boundaries are respected in WAL"""
        logger.info("=== Test 3: Transaction Boundaries in WAL ===")

        try:
            if not self.start_server():
                return False

            # Create test table
            try:
                self.execute_sql("""
                    CREATE TABLE transaction_test (
                        id TEXT PRIMARY KEY,
                        status TEXT,
                        value INTEGER
                    )
                """, autocommit=True)
            except:
                pass

            # Test committed transaction
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO transaction_test (id, status, value)
                        VALUES (%s, %s, %s)
                    """, ("committed1", "committed", 100))
                    conn.commit()

            # Test rollback transaction
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO transaction_test (id, status, value)
                        VALUES (%s, %s, %s)
                    """, ("rollback1", "should_not_exist", 200))
                    conn.rollback()  # Explicit rollback

            # Test uncommitted transaction with crash
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO transaction_test (id, status, value)
                VALUES (%s, %s, %s)
            """, ("uncommitted1", "should_not_exist_either", 300))
            # Don't commit - simulate crash

            # Crash server
            self.crash_server()

            # Close connection (will be broken anyway)
            try:
                cur.close()
                conn.close()
            except:
                pass

            # Restart server
            if not self.start_server():
                return False

            # Verify only committed data exists
            result = self.execute_sql("""
                SELECT COUNT(*) FROM transaction_test
                WHERE status = 'committed'
            """)
            if not result or result[0][0] != 1:
                logger.error("Committed transaction not recovered correctly")
                return False

            # Verify rollback and uncommitted transactions don't exist
            result = self.execute_sql("""
                SELECT COUNT(*) FROM transaction_test
                WHERE status LIKE '%should_not_exist%'
            """)
            if result and result[0][0] > 0:
                logger.error("Rollback or uncommitted transactions found after recovery")
                return False

            logger.info("✓ Transaction boundaries test passed")
            return True

        except Exception as e:
            logger.error(f"Transaction boundaries test failed: {e}")
            return False

    def test_data_consistency_after_crash(self) -> bool:
        """Test 4: Data consistency after crash recovery"""
        logger.info("=== Test 4: Data Consistency After Crash ===")

        try:
            if not self.start_server():
                return False

            # Create test table
            try:
                self.execute_sql("""
                    CREATE TABLE consistency_test (
                        id TEXT PRIMARY KEY,
                        counter INTEGER,
                        checksum TEXT,
                        last_updated TIMESTAMP DEFAULT NOW()
                    )
                """, autocommit=True)
            except:
                pass

            # Insert initial data with known checksums
            test_records = []
            for i in range(10):
                record_id = f"consistency_{i}"
                counter = i * 10
                checksum = f"chk_{counter}"
                test_records.append((record_id, counter, checksum))

                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO consistency_test (id, counter, checksum)
                            VALUES (%s, %s, %s)
                        """, (record_id, counter, checksum))
                        conn.commit()

            # Verify data before crash
            result = self.execute_sql("SELECT COUNT(*) FROM consistency_test")
            if not result or result[0][0] != len(test_records):
                logger.error("Initial data not inserted correctly")
                return False

            # Perform some updates
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE consistency_test
                        SET counter = counter + 1,
                            checksum = 'chk_' || (counter + 1)::text
                        WHERE id LIKE 'consistency_%'
                    """)
                    conn.commit()

            # Crash server
            logger.info("Simulating crash during operations")
            self.crash_server()

            # Restart server
            if not self.start_server():
                return False

            # Verify data consistency
            result = self.execute_sql("SELECT COUNT(*) FROM consistency_test")
            if not result or result[0][0] != len(test_records):
                logger.error("Data count inconsistent after recovery")
                return False

            # Verify data integrity
            for record_id, original_counter, original_checksum in test_records:
                result = self.execute_sql("""
                    SELECT counter, checksum FROM consistency_test WHERE id = %s
                """, (record_id,))

                if not result:
                    logger.error(f"Record {record_id} missing after recovery")
                    return False

                recovered_counter, recovered_checksum = result[0]

                # The counter should be either original or original+1 (if update committed)
                # The checksum should match the counter
                expected_checksum = f"chk_{recovered_counter}"

                if recovered_checksum != expected_checksum:
                    logger.error(f"Data inconsistency in {record_id}: counter={recovered_counter}, checksum={recovered_checksum}")
                    return False

                if recovered_counter not in [original_counter, original_counter + 1]:
                    logger.error(f"Unexpected counter value in {record_id}: {recovered_counter}")
                    return False

            logger.info("✓ Data consistency test passed")
            return True

        except Exception as e:
            logger.error(f"Data consistency test failed: {e}")
            return False

    def test_wal_integration_with_table_operations(self) -> bool:
        """Test 5: WAL integration with table operations"""
        logger.info("=== Test 5: WAL Integration with Table Operations ===")

        try:
            if not self.start_server():
                return False

            # Test CREATE TABLE
            self.execute_sql("""
                CREATE TABLE operations_test (
                    id TEXT PRIMARY KEY,
                    operation_type TEXT,
                    data JSONB
                )
            """, autocommit=True)

            # Test various operations that should generate WAL entries
            operations = [
                ("INSERT", "INSERT INTO operations_test VALUES ('op1', 'insert', '{\"value\": 1}')"),
                ("UPDATE", "UPDATE operations_test SET data = '{\"value\": 2}' WHERE id = 'op1'"),
                ("INSERT", "INSERT INTO operations_test VALUES ('op2', 'insert', '{\"value\": 3}')"),
            ]

            for op_type, sql in operations:
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        conn.commit()
                logger.info(f"Executed {op_type} operation")

            # Record state before crash
            pre_crash_result = self.execute_sql("""
                SELECT id, operation_type, data::text FROM operations_test ORDER BY id
            """)

            # Crash and restart
            self.crash_server()
            if not self.start_server():
                return False

            # Verify all operations were recovered
            post_crash_result = self.execute_sql("""
                SELECT id, operation_type, data::text FROM operations_test ORDER BY id
            """)

            if pre_crash_result != post_crash_result:
                logger.error("Operations not recovered correctly after crash")
                logger.error(f"Before: {pre_crash_result}")
                logger.error(f"After: {post_crash_result}")
                return False

            logger.info("✓ WAL integration with table operations test passed")
            return True

        except Exception as e:
            logger.error(f"WAL integration with table operations test failed: {e}")
            return False

    def run_all_tests(self) -> bool:
        """Run all Engine-WAL integration tests"""
        logger.info("Starting Engine-WAL Integration Tests")

        if not self.setup():
            return False

        tests = [
            ("Basic Engine-WAL Integration", self.test_basic_engine_wal_integration),
            ("WAL Recovery After Restart", self.test_wal_recovery_after_restart),
            ("Transaction Boundaries in WAL", self.test_transaction_boundaries_in_wal),
            ("Data Consistency After Crash", self.test_data_consistency_after_crash),
            ("WAL Integration with Table Operations", self.test_wal_integration_with_table_operations),
        ]

        passed = 0
        failed = 0

        for test_name, test_func in tests:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Running: {test_name}")
            logger.info(f"{'=' * 60}")

            try:
                if test_func():
                    passed += 1
                    logger.info(f"✓ {test_name} PASSED")
                else:
                    failed += 1
                    logger.error(f"✗ {test_name} FAILED")
            except Exception as e:
                failed += 1
                logger.error(f"✗ {test_name} FAILED with exception: {e}")

            # Clean up between tests
            self.stop_server()
            time.sleep(1)

        # Summary
        logger.info(f"\n{'=' * 60}")
        logger.info(f"ENGINE-WAL INTEGRATION TEST SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Total Tests: {len(tests)}")
        logger.info(f"Passed: {passed}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Success Rate: {(passed / len(tests)) * 100:.1f}%")

        if failed == 0:
            logger.info("🎉 All Engine-WAL integration tests PASSED!")
        else:
            logger.error(f"⚠️  {failed} test(s) FAILED")

        return failed == 0

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="DriftDB Engine-WAL Integration Test")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run tests
    tester = EngineWALIntegrationTest()

    try:
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()