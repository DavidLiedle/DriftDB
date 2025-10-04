"""
Shared pytest fixtures and configuration for DriftDB tests
"""
import os
import sys
import time
import subprocess
import tempfile
import shutil
from pathlib import Path
import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path so we can import test modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configuration
DRIFTDB_HOST = os.getenv("DRIFTDB_HOST", "localhost")
DRIFTDB_PORT = int(os.getenv("DRIFTDB_PORT", "5433"))
DRIFTDB_USER = os.getenv("DRIFTDB_USER", "driftdb")
DRIFTDB_PASS = os.getenv("DRIFTDB_PASS", "")
DRIFTDB_DB = os.getenv("DRIFTDB_DB", "driftdb")

# Server binary path
SERVER_BINARY = Path(__file__).parent.parent / "target" / "release" / "driftdb-server"


class DriftDBServer:
    """Manages a DriftDB server instance for testing"""

    def __init__(self, data_dir=None, port=None):
        self.data_dir = data_dir or tempfile.mkdtemp(prefix="driftdb_test_")
        self.port = port or DRIFTDB_PORT
        self.process = None

    def start(self):
        """Start the DriftDB server"""
        if not SERVER_BINARY.exists():
            raise FileNotFoundError(
                f"DriftDB server binary not found at {SERVER_BINARY}. "
                "Please build with: cargo build --release"
            )

        cmd = [
            str(SERVER_BINARY),
            "--data-path", self.data_dir,
            "--port", str(self.port),
            "--auth-method", "trust"
        ]

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Wait for server to be ready
        for _ in range(30):  # 30 second timeout
            try:
                conn = psycopg2.connect(
                    host=DRIFTDB_HOST,
                    port=self.port,
                    database=DRIFTDB_DB,
                    user=DRIFTDB_USER,
                    password=DRIFTDB_PASS
                )
                conn.close()
                return
            except psycopg2.OperationalError:
                time.sleep(1)

        raise RuntimeError("DriftDB server failed to start")

    def stop(self):
        """Stop the DriftDB server"""
        if self.process:
            self.process.terminate()
            self.process.wait(timeout=10)

    def cleanup(self):
        """Clean up test data directory"""
        if os.path.exists(self.data_dir) and self.data_dir.startswith("/tmp/"):
            shutil.rmtree(self.data_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def driftdb_server():
    """Session-scoped DriftDB server instance"""
    server = DriftDBServer()
    server.start()
    yield server
    server.stop()
    server.cleanup()


@pytest.fixture(scope="function")
def driftdb_connection(driftdb_server):
    """Function-scoped database connection"""
    conn = psycopg2.connect(
        host=DRIFTDB_HOST,
        port=driftdb_server.port,
        database=DRIFTDB_DB,
        user=DRIFTDB_USER,
        password=DRIFTDB_PASS
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def cursor(driftdb_connection):
    """Function-scoped database cursor"""
    cur = driftdb_connection.cursor()
    yield cur
    cur.close()


@pytest.fixture(scope="function")
def clean_database(cursor):
    """Ensures a clean database state for each test"""
    # Drop all user tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")

    yield cursor


@pytest.fixture
def test_table(clean_database):
    """Creates a standard test table"""
    clean_database.execute("""
        CREATE TABLE test_table (
            id INT PRIMARY KEY,
            name VARCHAR,
            value INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    yield clean_database


@pytest.fixture
def sample_data(test_table):
    """Inserts sample data into test table"""
    data = [
        (1, 'Alice', 100),
        (2, 'Bob', 200),
        (3, 'Charlie', 150),
        (4, 'David', 300),
        (5, 'Eve', 250)
    ]

    for id_val, name, value in data:
        test_table.execute(
            "INSERT INTO test_table (id, name, value) VALUES (%s, %s, %s)",
            (id_val, name, value)
        )

    yield test_table


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance benchmarks"
    )