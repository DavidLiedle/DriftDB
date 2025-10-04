"""
Base test class with common functionality for DriftDB tests
"""
import unittest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
from datetime import datetime, timedelta
from typing import Any, List, Tuple, Optional


class DriftDBTestCase(unittest.TestCase):
    """Base test class for DriftDB tests"""

    # Connection settings
    HOST = "localhost"
    PORT = 5433
    DATABASE = "driftdb"
    USER = "driftdb"
    PASSWORD = ""

    @classmethod
    def setUpClass(cls):
        """One-time setup for test class"""
        cls.conn = cls.get_connection()
        cls.cur = cls.conn.cursor()

    @classmethod
    def tearDownClass(cls):
        """One-time cleanup for test class"""
        if hasattr(cls, 'cur'):
            cls.cur.close()
        if hasattr(cls, 'conn'):
            cls.conn.close()

    def setUp(self):
        """Setup for each test"""
        self.cleanup_tables()

    def tearDown(self):
        """Cleanup after each test"""
        self.cleanup_tables()

    @classmethod
    def get_connection(cls):
        """Get a database connection"""
        conn = psycopg2.connect(
            host=cls.HOST,
            port=cls.PORT,
            database=cls.DATABASE,
            user=cls.USER,
            password=cls.PASSWORD
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def cleanup_tables(self):
        """Drop all test tables"""
        # Get list of tables
        self.cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'test_%'
        """)
        tables = self.cur.fetchall()

        # Drop each table
        for table in tables:
            try:
                self.cur.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")
            except:
                pass  # Ignore errors

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Execute a query and return results"""
        self.cur.execute(query, params)
        if query.strip().upper().startswith('SELECT'):
            return self.cur.fetchall()
        return None

    def create_test_table(self, table_name: str = "test_table",
                         columns: Optional[List[Tuple[str, str]]] = None):
        """Create a test table with specified columns"""
        if columns is None:
            columns = [
                ("id", "INT PRIMARY KEY"),
                ("name", "VARCHAR"),
                ("value", "INT"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]

        column_defs = ", ".join([f"{name} {dtype}" for name, dtype in columns])
        query = f"CREATE TABLE {table_name} ({column_defs})"
        self.cur.execute(query)

    def insert_test_data(self, table_name: str = "test_table",
                        data: Optional[List[Tuple]] = None):
        """Insert test data into a table"""
        if data is None:
            data = [
                (1, 'Alice', 100),
                (2, 'Bob', 200),
                (3, 'Charlie', 300)
            ]

        placeholders = ", ".join(["%s"] * len(data[0]))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"

        for row in data:
            self.cur.execute(query, row)

    # Assertion helpers
    def assert_result_count(self, result: List, expected: int):
        """Assert the number of rows returned"""
        self.assertEqual(len(result), expected,
                        f"Expected {expected} rows, got {len(result)}")

    def assert_column_value(self, result: List, row: int, col: int,
                           expected: Any):
        """Assert a specific column value"""
        actual = result[row][col]
        self.assertEqual(actual, expected,
                        f"Row {row}, Col {col}: Expected {expected}, got {actual}")

    def assert_query_succeeds(self, query: str):
        """Assert that a query executes without error"""
        try:
            self.cur.execute(query)
            return True
        except Exception as e:
            self.fail(f"Query failed: {query}\nError: {e}")

    def assert_query_fails(self, query: str, expected_error: Optional[str] = None):
        """Assert that a query fails with expected error"""
        try:
            self.cur.execute(query)
            self.fail(f"Query should have failed: {query}")
        except psycopg2.Error as e:
            if expected_error and expected_error not in str(e):
                self.fail(f"Expected error containing '{expected_error}', got: {e}")

    def assert_constraint_violation(self, query: str, constraint_type: str):
        """Assert that a query violates a constraint"""
        self.assert_query_fails(query, constraint_type)

    # Time travel helpers
    def query_as_of_sequence(self, table: str, sequence: int) -> List:
        """Query table as of specific sequence number"""
        query = f"SELECT * FROM {table} AS OF @seq:{sequence}"
        return self.execute_query(query)

    def query_as_of_timestamp(self, table: str, timestamp: datetime) -> List:
        """Query table as of specific timestamp"""
        ts_str = timestamp.isoformat() + 'Z'
        query = f"SELECT * FROM {table} AS OF '{ts_str}'"
        return self.execute_query(query)

    def wait_and_insert(self, table: str, data: Tuple, wait_seconds: float = 0.1):
        """Insert data after a delay (for temporal testing)"""
        time.sleep(wait_seconds)
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table} VALUES ({placeholders})"
        self.cur.execute(query, data)

    # Transaction helpers
    def begin_transaction(self, isolation_level: Optional[str] = None):
        """Begin a transaction with optional isolation level"""
        if isolation_level:
            self.cur.execute(f"BEGIN ISOLATION LEVEL {isolation_level}")
        else:
            self.cur.execute("BEGIN")

    def commit_transaction(self):
        """Commit current transaction"""
        self.cur.execute("COMMIT")

    def rollback_transaction(self):
        """Rollback current transaction"""
        self.cur.execute("ROLLBACK")

    def create_savepoint(self, name: str):
        """Create a savepoint"""
        self.cur.execute(f"SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str):
        """Rollback to a savepoint"""
        self.cur.execute(f"ROLLBACK TO SAVEPOINT {name}")