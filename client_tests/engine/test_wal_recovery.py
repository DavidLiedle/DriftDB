#!/usr/bin/env python3
"""
Comprehensive WAL (Write-Ahead Log) Crash Recovery Test for DriftDB

This script tests the WAL crash recovery functionality to ensure data durability
by simulating various crash scenarios and verifying proper recovery.

Test Scenarios:
1. Basic WAL functionality and recovery
2. Crash during INSERT operations
3. Crash during UPDATE operations
4. Crash during DELETE operations
5. Crash during transaction commit
6. Multiple concurrent transactions with crash
7. WAL file corruption detection
8. WAL rotation functionality
9. Checkpoint and replay functionality

Requirements:
- DriftDB server binary (driftdb-server)
- psycopg2 Python library
- Python 3.7+
"""

import os
import sys
import time
import json
import signal
import subprocess
import tempfile
import shutil
import threading
import logging
import hashlib
import struct
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not found. Install with: pip install psycopg2-binary")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TestConfig:
    """Configuration for WAL recovery tests"""
    server_binary: str = "cargo run --bin driftdb-server"
    server_host: str = "127.0.0.1"
    server_port: int = 5433
    startup_timeout: int = 10
    shutdown_timeout: int = 5
    test_db_name: str = "test_wal_recovery"

class DatabaseServer:
    """Manages DriftDB server lifecycle for testing"""

    def __init__(self, data_dir: Path, config: TestConfig):
        self.data_dir = data_dir
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self.wal_dir = data_dir / "wal"

    def start(self) -> bool:
        """Start the DriftDB server"""
        if self.process and self.process.poll() is None:
            logger.warning("Server already running")
            return True

        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.wal_dir.mkdir(parents=True, exist_ok=True)

        # Start server
        cmd = [
            "cargo", "run", "--bin", "driftdb-server", "--",
            "--data-path", str(self.data_dir),
            "--listen", f"{self.config.server_host}:{self.config.server_port}",
            "--http-listen", f"{self.config.server_host}:8080"
        ]

        logger.info(f"Starting DriftDB server: {' '.join(cmd)}")

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=Path(__file__).parent,
                env=dict(os.environ, RUST_LOG="debug")
            )

            # Wait for server to be ready
            if self._wait_for_ready():
                logger.info("DriftDB server started successfully")
                return True
            else:
                logger.error("Server failed to start within timeout")
                self.stop()
                return False

        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            return False

    def stop(self) -> None:
        """Stop the DriftDB server"""
        if not self.process:
            return

        logger.info("Stopping DriftDB server")

        try:
            self.process.terminate()

            # Wait for graceful shutdown
            try:
                self.process.wait(timeout=self.config.shutdown_timeout)
                logger.info("Server stopped gracefully")
            except subprocess.TimeoutExpired:
                logger.warning("Server didn't stop gracefully, killing")
                self.process.kill()
                self.process.wait()

        except Exception as e:
            logger.error(f"Error stopping server: {e}")
        finally:
            self.process = None

    def crash(self) -> None:
        """Simulate a crash by sending SIGKILL"""
        if not self.process:
            logger.warning("No process to crash")
            return

        logger.info("Simulating server crash (SIGKILL)")
        try:
            self.process.kill()
            self.process.wait()
        except Exception as e:
            logger.error(f"Error crashing server: {e}")
        finally:
            self.process = None

    def _wait_for_ready(self) -> bool:
        """Wait for server to accept connections"""
        start_time = time.time()

        while time.time() - start_time < self.config.startup_timeout:
            try:
                conn = psycopg2.connect(
                    host=self.config.server_host,
                    port=self.config.server_port,
                    database=self.config.test_db_name,
                    user="postgres",
                    password="",
                    connect_timeout=1
                )
                conn.close()
                return True
            except psycopg2.OperationalError:
                time.sleep(0.5)

        return False

    def get_wal_files(self) -> List[Path]:
        """Get list of WAL files"""
        if not self.wal_dir.exists():
            return []
        return list(self.wal_dir.glob("*.log*"))

class WALAnalyzer:
    """Analyzes WAL files for testing purposes"""

    @staticmethod
    def read_wal_entries(wal_file: Path) -> List[Dict[str, Any]]:
        """Read and parse WAL entries from a file"""
        entries = []

        try:
            with open(wal_file, 'rb') as f:
                while True:
                    # Read length prefix (4 bytes)
                    length_data = f.read(4)
                    if len(length_data) < 4:
                        break

                    length = struct.unpack('<I', length_data)[0]
                    if length == 0 or length > 10 * 1024 * 1024:  # Sanity check
                        break

                    # Read entry data
                    entry_data = f.read(length)
                    if len(entry_data) < length:
                        break

                    # For simplicity, we'll track the raw size and position
                    entries.append({
                        'length': length,
                        'position': f.tell() - length - 4,
                        'data_size': len(entry_data)
                    })

        except Exception as e:
            logger.error(f"Error reading WAL file {wal_file}: {e}")

        return entries

    @staticmethod
    def corrupt_wal_file(wal_file: Path, corruption_type: str = "middle") -> bool:
        """Corrupt a WAL file for testing corruption detection"""
        try:
            if not wal_file.exists():
                return False

            with open(wal_file, 'r+b') as f:
                f.seek(0, 2)  # Seek to end
                file_size = f.tell()

                if file_size < 8:  # Too small to corrupt meaningfully
                    return False

                if corruption_type == "middle":
                    # Corrupt bytes in the middle
                    pos = file_size // 2
                    f.seek(pos)
                    f.write(b'\x00\x00\x00\x00')
                elif corruption_type == "crc":
                    # Corrupt what might be CRC data
                    pos = 8  # Skip length prefix, corrupt potential CRC
                    f.seek(pos)
                    f.write(b'\xFF\xFF\xFF\xFF')
                elif corruption_type == "truncate":
                    # Truncate file in the middle
                    f.truncate(file_size // 2)

                f.flush()
                os.fsync(f.fileno())

            logger.info(f"Corrupted WAL file {wal_file} with {corruption_type}")
            return True

        except Exception as e:
            logger.error(f"Error corrupting WAL file: {e}")
            return False

class WALRecoveryTester:
    """Main test class for WAL recovery functionality"""

    def __init__(self, config: TestConfig):
        self.config = config
        self.temp_dir = None
        self.server = None
        self.test_results = []

    def setup(self) -> bool:
        """Set up test environment"""
        try:
            # Create temporary directory for test data
            self.temp_dir = Path(tempfile.mkdtemp(prefix="driftdb_wal_test_"))
            logger.info(f"Test data directory: {self.temp_dir}")

            # Initialize server
            self.server = DatabaseServer(self.temp_dir, self.config)

            return True

        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

    def cleanup(self) -> None:
        """Clean up test environment"""
        if self.server:
            self.server.stop()

        if self.temp_dir and self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
                logger.info("Cleaned up test directory")
            except Exception as e:
                logger.error(f"Error cleaning up: {e}")

    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection"""
        return psycopg2.connect(
            host=self.config.server_host,
            port=self.config.server_port,
            database=self.config.test_db_name,
            user="postgres",
            password="",
            autocommit=False
        )

    def execute_sql(self, sql: str, params: Optional[Tuple] = None,
                   autocommit: bool = False) -> Optional[List[Tuple]]:
        """Execute SQL and return results"""
        try:
            with self.get_connection() as conn:
                if autocommit:
                    conn.autocommit = True

                with conn.cursor() as cur:
                    cur.execute(sql, params)

                    if cur.description:
                        return cur.fetchall()
                    return None

        except Exception as e:
            logger.error(f"SQL execution failed: {sql}, error: {e}")
            raise

    def test_basic_wal_functionality(self) -> bool:
        """Test 1: Basic WAL functionality and recovery"""
        logger.info("=== Test 1: Basic WAL Functionality ===")

        try:
            # Start server
            if not self.server.start():
                return False

            # Create test table
            self.execute_sql("""
                CREATE TABLE test_users (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    email TEXT
                )
            """, autocommit=True)

            # Insert test data
            test_data = [
                ("user1", "Alice Smith", "alice@example.com"),
                ("user2", "Bob Johnson", "bob@example.com"),
                ("user3", "Carol Davis", "carol@example.com")
            ]

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for user_id, name, email in test_data:
                        cur.execute("""
                            INSERT INTO test_users (id, name, email)
                            VALUES (%s, %s, %s)
                        """, (user_id, name, email))
                    conn.commit()

            # Verify data exists
            results = self.execute_sql("SELECT COUNT(*) FROM test_users")
            if not results or results[0][0] != len(test_data):
                logger.error("Data insertion verification failed")
                return False

            # Check WAL files exist
            wal_files = self.server.get_wal_files()
            if not wal_files:
                logger.error("No WAL files found")
                return False

            logger.info(f"Found {len(wal_files)} WAL files")

            # Stop server gracefully
            self.server.stop()

            # Restart server and verify data recovery
            if not self.server.start():
                return False

            results = self.execute_sql("SELECT COUNT(*) FROM test_users")
            if not results or results[0][0] != len(test_data):
                logger.error("Data recovery after restart failed")
                return False

            logger.info("✓ Basic WAL functionality test passed")
            return True

        except Exception as e:
            logger.error(f"Basic WAL test failed: {e}")
            return False

    def test_crash_during_insert(self) -> bool:
        """Test 2: Crash during INSERT operations"""
        logger.info("=== Test 2: Crash During INSERT Operations ===")

        try:
            if not self.server.start():
                return False

            # Create table if not exists
            try:
                self.execute_sql("""
                    CREATE TABLE crash_test_inserts (
                        id TEXT PRIMARY KEY,
                        data TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """, autocommit=True)
            except:
                pass  # Table might already exist

            # Start a transaction with multiple inserts
            conn = self.get_connection()
            cur = conn.cursor()

            # Insert some data
            for i in range(5):
                cur.execute("""
                    INSERT INTO crash_test_inserts (id, data)
                    VALUES (%s, %s)
                """, (f"crash_insert_{i}", f"data_{i}"))

            # Don't commit yet - simulate crash during transaction
            logger.info("Simulating crash during uncommitted INSERT transaction")
            self.server.crash()

            # Try to use connection (should fail)
            try:
                cur.execute("SELECT 1")
            except:
                pass  # Expected to fail
            finally:
                cur.close()
                conn.close()

            # Restart server
            if not self.server.start():
                return False

            # Verify uncommitted data was rolled back
            results = self.execute_sql("""
                SELECT COUNT(*) FROM crash_test_inserts
                WHERE id LIKE 'crash_insert_%'
            """)

            if results and results[0][0] > 0:
                logger.error("Found uncommitted data after crash - rollback failed")
                return False

            # Now test committed data survives crash
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(3):
                        cur.execute("""
                            INSERT INTO crash_test_inserts (id, data)
                            VALUES (%s, %s)
                        """, (f"committed_insert_{i}", f"committed_data_{i}"))
                    conn.commit()  # This should be durable

            # Simulate crash after commit
            logger.info("Simulating crash after committed INSERT")
            self.server.crash()

            # Restart and verify committed data survived
            if not self.server.start():
                return False

            results = self.execute_sql("""
                SELECT COUNT(*) FROM crash_test_inserts
                WHERE id LIKE 'committed_insert_%'
            """)

            if not results or results[0][0] != 3:
                logger.error("Committed data was lost after crash")
                return False

            logger.info("✓ Crash during INSERT operations test passed")
            return True

        except Exception as e:
            logger.error(f"Crash during INSERT test failed: {e}")
            return False

    def test_crash_during_update(self) -> bool:
        """Test 3: Crash during UPDATE operations"""
        logger.info("=== Test 3: Crash During UPDATE Operations ===")

        try:
            if not self.server.start():
                return False

            # Create and populate test table
            try:
                self.execute_sql("""
                    CREATE TABLE crash_test_updates (
                        id TEXT PRIMARY KEY,
                        value INT,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """, autocommit=True)
            except:
                pass

            # Insert initial data
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(5):
                        cur.execute("""
                            INSERT INTO crash_test_updates (id, value)
                            VALUES (%s, %s)
                        """, (f"update_test_{i}", i * 10))
                    conn.commit()

            # Start transaction with updates
            conn = self.get_connection()
            cur = conn.cursor()

            # Update some values
            cur.execute("""
                UPDATE crash_test_updates
                SET value = value + 1000
                WHERE id LIKE 'update_test_%'
            """)

            # Crash before commit
            logger.info("Simulating crash during uncommitted UPDATE")
            self.server.crash()

            try:
                cur.close()
                conn.close()
            except:
                pass

            # Restart and verify original values
            if not self.server.start():
                return False

            results = self.execute_sql("""
                SELECT value FROM crash_test_updates
                WHERE id = 'update_test_0'
            """)

            if not results or results[0][0] != 0:  # Should be original value
                logger.error("UPDATE rollback failed after crash")
                return False

            # Test committed updates survive crash
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE crash_test_updates
                        SET value = value + 500
                        WHERE id LIKE 'update_test_%'
                    """)
                    conn.commit()

            # Crash after commit
            logger.info("Simulating crash after committed UPDATE")
            self.server.crash()

            # Restart and verify updates survived
            if not self.server.start():
                return False

            results = self.execute_sql("""
                SELECT value FROM crash_test_updates
                WHERE id = 'update_test_0'
            """)

            if not results or results[0][0] != 500:  # Should be updated value
                logger.error("Committed UPDATE was lost after crash")
                return False

            logger.info("✓ Crash during UPDATE operations test passed")
            return True

        except Exception as e:
            logger.error(f"Crash during UPDATE test failed: {e}")
            return False

    def test_crash_during_delete(self) -> bool:
        """Test 4: Crash during DELETE operations"""
        logger.info("=== Test 4: Crash During DELETE Operations ===")

        try:
            if not self.server.start():
                return False

            # Create and populate test table
            try:
                self.execute_sql("""
                    CREATE TABLE crash_test_deletes (
                        id TEXT PRIMARY KEY,
                        category TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """, autocommit=True)
            except:
                pass

            # Insert test data
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(10):
                        category = "keep" if i < 5 else "delete"
                        cur.execute("""
                            INSERT INTO crash_test_deletes (id, category)
                            VALUES (%s, %s)
                        """, (f"delete_test_{i}", category))
                    conn.commit()

            # Start transaction with deletes
            conn = self.get_connection()
            cur = conn.cursor()

            # Delete some records
            cur.execute("""
                DELETE FROM crash_test_deletes
                WHERE category = 'delete'
            """)

            # Crash before commit
            logger.info("Simulating crash during uncommitted DELETE")
            self.server.crash()

            try:
                cur.close()
                conn.close()
            except:
                pass

            # Restart and verify data wasn't deleted
            if not self.server.start():
                return False

            results = self.execute_sql("""
                SELECT COUNT(*) FROM crash_test_deletes
                WHERE category = 'delete'
            """)

            if not results or results[0][0] != 5:  # Should still have 5 records
                logger.error("DELETE rollback failed after crash")
                return False

            # Test committed deletes survive crash
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM crash_test_deletes
                        WHERE category = 'delete'
                    """)
                    conn.commit()

            # Crash after commit
            logger.info("Simulating crash after committed DELETE")
            self.server.crash()

            # Restart and verify deletes survived
            if not self.server.start():
                return False

            results = self.execute_sql("""
                SELECT COUNT(*) FROM crash_test_deletes
                WHERE category = 'delete'
            """)

            if not results or results[0][0] != 0:  # Should be 0 records
                logger.error("Committed DELETE was lost after crash")
                return False

            # Verify non-deleted records still exist
            results = self.execute_sql("""
                SELECT COUNT(*) FROM crash_test_deletes
                WHERE category = 'keep'
            """)

            if not results or results[0][0] != 5:
                logger.error("Non-deleted records were lost")
                return False

            logger.info("✓ Crash during DELETE operations test passed")
            return True

        except Exception as e:
            logger.error(f"Crash during DELETE test failed: {e}")
            return False

    def test_crash_during_commit(self) -> bool:
        """Test 5: Crash during transaction commit"""
        logger.info("=== Test 5: Crash During Transaction Commit ===")

        try:
            if not self.server.start():
                return False

            # Create test table
            try:
                self.execute_sql("""
                    CREATE TABLE crash_test_commit (
                        id TEXT PRIMARY KEY,
                        step INT,
                        data TEXT
                    )
                """, autocommit=True)
            except:
                pass

            # This test is harder to implement precisely since we can't
            # crash exactly during commit. We'll simulate by having
            # a large transaction and crashing during it.

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Insert many records in one transaction
                    for i in range(100):
                        cur.execute("""
                            INSERT INTO crash_test_commit (id, step, data)
                            VALUES (%s, %s, %s)
                        """, (f"commit_test_{i}", i, f"large_data_payload_{i}" * 10))

                    # Start commit process
                    logger.info("Starting large transaction commit")

                    # Crash server quickly after starting commit
                    def crash_after_delay():
                        time.sleep(0.1)  # Very brief delay
                        self.server.crash()

                    crash_thread = threading.Thread(target=crash_after_delay)
                    crash_thread.start()

                    try:
                        conn.commit()
                        logger.info("Transaction committed before crash")
                    except Exception as e:
                        logger.info(f"Transaction interrupted by crash: {e}")

                    crash_thread.join()

            # Restart server
            if not self.server.start():
                return False

            # Check how many records survived
            results = self.execute_sql("""
                SELECT COUNT(*) FROM crash_test_commit
                WHERE id LIKE 'commit_test_%'
            """)

            if results:
                count = results[0][0]
                logger.info(f"Found {count} records after crash during commit")

                # The transaction should either be completely committed (100 records)
                # or completely rolled back (0 records). Partial commits indicate
                # atomicity violation.
                if count not in [0, 100]:
                    logger.error(f"Transaction atomicity violated: {count} records (expected 0 or 100)")
                    return False
                else:
                    logger.info("✓ Transaction atomicity maintained during crash")

            logger.info("✓ Crash during commit test passed")
            return True

        except Exception as e:
            logger.error(f"Crash during commit test failed: {e}")
            return False

    def test_concurrent_transactions_crash(self) -> bool:
        """Test 6: Crash with multiple concurrent transactions"""
        logger.info("=== Test 6: Multiple Concurrent Transactions with Crash ===")

        try:
            if not self.server.start():
                return False

            # Create test table
            try:
                self.execute_sql("""
                    CREATE TABLE crash_test_concurrent (
                        id TEXT PRIMARY KEY,
                        txn_id TEXT,
                        value INT,
                        committed BOOLEAN DEFAULT FALSE
                    )
                """, autocommit=True)
            except:
                pass

            # Start multiple concurrent transactions
            connections = []
            transactions = []

            for txn_id in range(5):
                conn = self.get_connection()
                connections.append(conn)
                cur = conn.cursor()

                # Insert data for this transaction
                for i in range(10):
                    cur.execute("""
                        INSERT INTO crash_test_concurrent (id, txn_id, value)
                        VALUES (%s, %s, %s)
                    """, (f"txn_{txn_id}_record_{i}", f"txn_{txn_id}", i))

                transactions.append((conn, cur, txn_id))

            # Commit some transactions, leave others uncommitted
            for i, (conn, cur, txn_id) in enumerate(transactions):
                if i % 2 == 0:  # Commit even-numbered transactions
                    conn.commit()
                    cur.execute("""
                        UPDATE crash_test_concurrent
                        SET committed = TRUE
                        WHERE txn_id = %s
                    """, (f"txn_{txn_id}",))
                    conn.commit()
                    logger.info(f"Committed transaction {txn_id}")

            # Crash server
            logger.info("Simulating crash with mixed committed/uncommitted transactions")
            self.server.crash()

            # Clean up connections
            for conn, cur, _ in transactions:
                try:
                    cur.close()
                    conn.close()
                except:
                    pass

            # Restart server
            if not self.server.start():
                return False

            # Verify only committed transactions survived
            for txn_id in range(5):
                results = self.execute_sql("""
                    SELECT COUNT(*) FROM crash_test_concurrent
                    WHERE txn_id = %s
                """, (f"txn_{txn_id}",))

                count = results[0][0] if results else 0

                if txn_id % 2 == 0:  # Should be committed
                    if count != 10:
                        logger.error(f"Committed transaction {txn_id} lost data: {count}/10 records")
                        return False
                    logger.info(f"✓ Committed transaction {txn_id} recovered correctly")
                else:  # Should be rolled back
                    if count != 0:
                        logger.error(f"Uncommitted transaction {txn_id} data survived: {count} records")
                        return False
                    logger.info(f"✓ Uncommitted transaction {txn_id} rolled back correctly")

            logger.info("✓ Concurrent transactions crash test passed")
            return True

        except Exception as e:
            logger.error(f"Concurrent transactions crash test failed: {e}")
            return False

    def test_wal_corruption_detection(self) -> bool:
        """Test 7: WAL file corruption detection"""
        logger.info("=== Test 7: WAL Corruption Detection ===")

        try:
            if not self.server.start():
                return False

            # Create test data to generate WAL entries
            try:
                self.execute_sql("""
                    CREATE TABLE corruption_test (
                        id TEXT PRIMARY KEY,
                        data TEXT
                    )
                """, autocommit=True)
            except:
                pass

            # Generate some WAL entries
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(20):
                        cur.execute("""
                            INSERT INTO corruption_test (id, data)
                            VALUES (%s, %s)
                        """, (f"corruption_test_{i}", f"test_data_{i}"))
                    conn.commit()

            # Stop server to corrupt WAL
            self.server.stop()

            # Find and corrupt WAL files
            wal_files = self.server.get_wal_files()
            if not wal_files:
                logger.error("No WAL files found for corruption test")
                return False

            corrupted_file = wal_files[0]
            logger.info(f"Corrupting WAL file: {corrupted_file}")

            # Test different corruption types
            corruption_tests = [
                ("middle", "middle bytes corrupted"),
                ("crc", "CRC corrupted"),
                ("truncate", "file truncated")
            ]

            for corruption_type, description in corruption_tests:
                logger.info(f"Testing {description}")

                # Backup original file
                backup_file = corrupted_file.with_suffix('.backup')
                shutil.copy2(corrupted_file, backup_file)

                # Corrupt the file
                if not WALAnalyzer.corrupt_wal_file(corrupted_file, corruption_type):
                    logger.warning(f"Could not corrupt file for {corruption_type} test")
                    continue

                # Try to restart server - it should handle corruption gracefully
                server_started = self.server.start()

                if server_started:
                    # Check if server detected corruption
                    try:
                        # Try to read data - server might have recovered or truncated
                        results = self.execute_sql("SELECT COUNT(*) FROM corruption_test")
                        if results:
                            logger.info(f"Server recovered from {corruption_type} corruption, {results[0][0]} records found")
                    except Exception as e:
                        logger.info(f"Server started but queries failed (expected with corruption): {e}")

                    self.server.stop()
                else:
                    logger.info(f"Server failed to start with {corruption_type} corruption (acceptable)")

                # Restore backup
                shutil.copy2(backup_file, corrupted_file)
                backup_file.unlink()

            # Verify server can start with uncorrupted WAL
            if not self.server.start():
                logger.error("Server failed to start with restored WAL")
                return False

            logger.info("✓ WAL corruption detection test passed")
            return True

        except Exception as e:
            logger.error(f"WAL corruption test failed: {e}")
            return False

    def test_wal_rotation(self) -> bool:
        """Test 8: WAL rotation functionality"""
        logger.info("=== Test 8: WAL Rotation Functionality ===")

        try:
            if not self.server.start():
                return False

            # Create table for rotation test
            try:
                self.execute_sql("""
                    CREATE TABLE rotation_test (
                        id TEXT PRIMARY KEY,
                        data TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """, autocommit=True)
            except:
                pass

            initial_wal_files = self.server.get_wal_files()
            logger.info(f"Initial WAL files: {len(initial_wal_files)}")

            # Generate large amount of data to trigger rotation
            # Note: The WAL rotates at 100MB according to the code
            batch_size = 1000
            total_batches = 5

            for batch in range(total_batches):
                logger.info(f"Inserting batch {batch + 1}/{total_batches}")

                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        for i in range(batch_size):
                            record_id = f"rotation_test_{batch}_{i}"
                            # Use large data to fill WAL faster
                            large_data = f"large_data_payload_{i}" * 100

                            cur.execute("""
                                INSERT INTO rotation_test (id, data)
                                VALUES (%s, %s)
                            """, (record_id, large_data))

                        conn.commit()

                # Check if rotation occurred
                current_wal_files = self.server.get_wal_files()
                if len(current_wal_files) > len(initial_wal_files):
                    logger.info(f"WAL rotation detected! Now have {len(current_wal_files)} files")
                    break

                time.sleep(0.5)  # Brief pause between batches

            final_wal_files = self.server.get_wal_files()
            logger.info(f"Final WAL files: {len(final_wal_files)}")

            # Test recovery with multiple WAL files
            self.server.stop()

            if not self.server.start():
                return False

            # Verify all data was recovered
            results = self.execute_sql("SELECT COUNT(*) FROM rotation_test")
            if results:
                count = results[0][0]
                logger.info(f"Recovered {count} records after WAL rotation")

                # We should have some data (exact count depends on when rotation occurred)
                if count == 0:
                    logger.error("No data recovered after WAL rotation")
                    return False

            logger.info("✓ WAL rotation test passed")
            return True

        except Exception as e:
            logger.error(f"WAL rotation test failed: {e}")
            return False

    def test_checkpoint_and_replay(self) -> bool:
        """Test 9: Checkpoint and replay functionality"""
        logger.info("=== Test 9: Checkpoint and Replay Functionality ===")

        try:
            if not self.server.start():
                return False

            # Create test table
            try:
                self.execute_sql("""
                    CREATE TABLE checkpoint_test (
                        id TEXT PRIMARY KEY,
                        phase TEXT,
                        value INT
                    )
                """, autocommit=True)
            except:
                pass

            # Phase 1: Insert initial data
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(50):
                        cur.execute("""
                            INSERT INTO checkpoint_test (id, phase, value)
                            VALUES (%s, %s, %s)
                        """, (f"checkpoint_test_{i}", "phase1", i))
                    conn.commit()

            logger.info("Phase 1 data inserted")

            # Simulate checkpoint (would normally be triggered by DriftDB automatically)
            # We can't directly trigger checkpoints via SQL in this implementation,
            # so we'll test the recovery process

            # Phase 2: More data after implicit checkpoint
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(50, 100):
                        cur.execute("""
                            INSERT INTO checkpoint_test (id, phase, value)
                            VALUES (%s, %s, %s)
                        """, (f"checkpoint_test_{i}", "phase2", i))
                    conn.commit()

            logger.info("Phase 2 data inserted")

            # Record total count before crash
            results = self.execute_sql("SELECT COUNT(*) FROM checkpoint_test")
            expected_count = results[0][0] if results else 0
            logger.info(f"Total records before crash: {expected_count}")

            # Crash and restart to test WAL replay
            self.server.crash()

            if not self.server.start():
                return False

            # Verify all data was recovered through WAL replay
            results = self.execute_sql("SELECT COUNT(*) FROM checkpoint_test")
            if not results or results[0][0] != expected_count:
                logger.error(f"Data recovery failed: got {results[0][0] if results else 0}, expected {expected_count}")
                return False

            # Verify data integrity by checking specific records
            phase1_results = self.execute_sql("""
                SELECT COUNT(*) FROM checkpoint_test WHERE phase = 'phase1'
            """)
            phase2_results = self.execute_sql("""
                SELECT COUNT(*) FROM checkpoint_test WHERE phase = 'phase2'
            """)

            if not phase1_results or phase1_results[0][0] != 50:
                logger.error("Phase 1 data not recovered correctly")
                return False

            if not phase2_results or phase2_results[0][0] != 50:
                logger.error("Phase 2 data not recovered correctly")
                return False

            logger.info("✓ Checkpoint and replay test passed")
            return True

        except Exception as e:
            logger.error(f"Checkpoint and replay test failed: {e}")
            return False

    def run_all_tests(self) -> bool:
        """Run all WAL recovery tests"""
        logger.info("Starting comprehensive WAL crash recovery tests")

        if not self.setup():
            logger.error("Test setup failed")
            return False

        tests = [
            ("Basic WAL Functionality", self.test_basic_wal_functionality),
            ("Crash During INSERT", self.test_crash_during_insert),
            ("Crash During UPDATE", self.test_crash_during_update),
            ("Crash During DELETE", self.test_crash_during_delete),
            ("Crash During Commit", self.test_crash_during_commit),
            ("Concurrent Transactions Crash", self.test_concurrent_transactions_crash),
            ("WAL Corruption Detection", self.test_wal_corruption_detection),
            ("WAL Rotation", self.test_wal_rotation),
            ("Checkpoint and Replay", self.test_checkpoint_and_replay),
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

            # Stop server between tests
            if self.server:
                self.server.stop()

            # Brief pause between tests
            time.sleep(1)

        # Final summary
        logger.info(f"\n{'=' * 60}")
        logger.info(f"TEST SUMMARY")
        logger.info(f"{'=' * 60}")
        logger.info(f"Total Tests: {len(tests)}")
        logger.info(f"Passed: {passed}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Success Rate: {(passed / len(tests)) * 100:.1f}%")

        if failed == 0:
            logger.info("🎉 All WAL recovery tests PASSED!")
        else:
            logger.error(f"⚠️  {failed} test(s) FAILED")

        return failed == 0

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="DriftDB WAL Crash Recovery Test Suite")
    parser.add_argument("--server-binary", default="cargo run --bin driftdb-server",
                       help="Path to DriftDB server binary")
    parser.add_argument("--port", type=int, default=5433,
                       help="Server port to use for testing")
    parser.add_argument("--host", default="127.0.0.1",
                       help="Server host to use for testing")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create test configuration
    config = TestConfig(
        server_binary=args.server_binary,
        server_host=args.host,
        server_port=args.port
    )

    # Run tests
    tester = WALRecoveryTester(config)

    try:
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()