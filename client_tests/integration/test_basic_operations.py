"""
Unit tests for basic DriftDB operations using the new test framework
"""
import pytest
from tests.utils import DriftDBTestCase


class TestBasicOperations(DriftDBTestCase):
    """Test basic CRUD operations"""

    def test_create_table(self):
        """Test CREATE TABLE"""
        self.assert_query_succeeds("""
            CREATE TABLE test_users (
                id INT PRIMARY KEY,
                name VARCHAR,
                email VARCHAR NOT NULL,
                age INT CHECK (age >= 0)
            )
        """)

        # Verify table exists
        result = self.execute_query("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'test_users'
        """)
        self.assert_result_count(result, 1)

    def test_insert_data(self):
        """Test INSERT operations"""
        self.create_test_table()

        # Single insert
        self.assert_query_succeeds(
            "INSERT INTO test_table (id, name, value) VALUES (1, 'Alice', 100)"
        )

        # Verify data
        result = self.execute_query("SELECT * FROM test_table WHERE id = 1")
        self.assert_result_count(result, 1)
        self.assert_column_value(result, 0, 1, 'Alice')  # name column

    def test_select_queries(self):
        """Test various SELECT queries"""
        self.create_test_table()
        self.insert_test_data()

        # SELECT *
        result = self.execute_query("SELECT * FROM test_table")
        self.assert_result_count(result, 3)

        # SELECT with WHERE
        result = self.execute_query("SELECT * FROM test_table WHERE value > 150")
        self.assert_result_count(result, 2)

        # SELECT specific columns
        result = self.execute_query("SELECT name, value FROM test_table ORDER BY id")
        self.assertEqual(result[0][0], 'Alice')
        self.assertEqual(result[0][1], 100)

    def test_update_data(self):
        """Test UPDATE operations"""
        self.create_test_table()
        self.insert_test_data()

        # Update single row
        self.assert_query_succeeds(
            "UPDATE test_table SET value = 500 WHERE id = 1"
        )

        # Verify update
        result = self.execute_query("SELECT value FROM test_table WHERE id = 1")
        self.assert_column_value(result, 0, 0, 500)

        # Update multiple rows
        self.assert_query_succeeds(
            "UPDATE test_table SET value = value * 2 WHERE value < 300"
        )

    def test_delete_data(self):
        """Test DELETE operations"""
        self.create_test_table()
        self.insert_test_data()

        # Delete single row
        self.assert_query_succeeds("DELETE FROM test_table WHERE id = 2")

        # Verify deletion
        result = self.execute_query("SELECT * FROM test_table")
        self.assert_result_count(result, 2)

        # Delete with condition
        self.assert_query_succeeds("DELETE FROM test_table WHERE value > 200")
        result = self.execute_query("SELECT * FROM test_table")
        self.assert_result_count(result, 1)


class TestConstraints(DriftDBTestCase):
    """Test constraint enforcement"""

    def test_primary_key_constraint(self):
        """Test PRIMARY KEY enforcement"""
        self.create_test_table()
        self.insert_test_data()

        # Try to insert duplicate primary key
        self.assert_constraint_violation(
            "INSERT INTO test_table (id, name, value) VALUES (1, 'Duplicate', 999)",
            "PRIMARY KEY"
        )

    def test_not_null_constraint(self):
        """Test NOT NULL constraint"""
        self.assert_query_succeeds("""
            CREATE TABLE test_not_null (
                id INT PRIMARY KEY,
                required_field VARCHAR NOT NULL
            )
        """)

        # Try to insert NULL
        self.assert_constraint_violation(
            "INSERT INTO test_not_null (id, required_field) VALUES (1, NULL)",
            "NOT NULL"
        )

    def test_check_constraint(self):
        """Test CHECK constraint"""
        self.assert_query_succeeds("""
            CREATE TABLE test_check (
                id INT PRIMARY KEY,
                age INT CHECK (age >= 18 AND age <= 120)
            )
        """)

        # Valid age
        self.assert_query_succeeds(
            "INSERT INTO test_check (id, age) VALUES (1, 25)"
        )

        # Invalid age - too young
        self.assert_constraint_violation(
            "INSERT INTO test_check (id, age) VALUES (2, 16)",
            "CHECK"
        )

        # Invalid age - too old
        self.assert_constraint_violation(
            "INSERT INTO test_check (id, age) VALUES (3, 150)",
            "CHECK"
        )

    def test_default_values(self):
        """Test DEFAULT value application"""
        self.assert_query_succeeds("""
            CREATE TABLE test_defaults (
                id INT PRIMARY KEY,
                status VARCHAR DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert without specifying defaults
        self.assert_query_succeeds(
            "INSERT INTO test_defaults (id) VALUES (1)"
        )

        # Check defaults were applied
        result = self.execute_query("SELECT status FROM test_defaults WHERE id = 1")
        self.assert_column_value(result, 0, 0, 'pending')


class TestTransactions(DriftDBTestCase):
    """Test transaction functionality"""

    def test_basic_transaction(self):
        """Test BEGIN/COMMIT transaction"""
        self.create_test_table()

        # Start transaction
        self.begin_transaction()

        # Insert data
        self.cur.execute(
            "INSERT INTO test_table (id, name, value) VALUES (1, 'TxnTest', 100)"
        )

        # Commit
        self.commit_transaction()

        # Verify data persisted
        result = self.execute_query("SELECT * FROM test_table WHERE id = 1")
        self.assert_result_count(result, 1)

    def test_rollback(self):
        """Test transaction ROLLBACK"""
        self.create_test_table()
        self.insert_test_data()

        # Start transaction
        self.begin_transaction()

        # Delete all data
        self.cur.execute("DELETE FROM test_table")

        # Rollback
        self.rollback_transaction()

        # Verify data still exists
        result = self.execute_query("SELECT * FROM test_table")
        self.assert_result_count(result, 3)

    @pytest.mark.integration
    def test_savepoints(self):
        """Test SAVEPOINT functionality"""
        self.create_test_table()

        self.begin_transaction()

        # Insert first record
        self.cur.execute(
            "INSERT INTO test_table (id, name, value) VALUES (1, 'First', 100)"
        )

        # Create savepoint
        self.create_savepoint("sp1")

        # Insert second record
        self.cur.execute(
            "INSERT INTO test_table (id, name, value) VALUES (2, 'Second', 200)"
        )

        # Rollback to savepoint
        self.rollback_to_savepoint("sp1")

        # Commit transaction
        self.commit_transaction()

        # Only first record should exist
        result = self.execute_query("SELECT * FROM test_table")
        self.assert_result_count(result, 1)
        self.assert_column_value(result, 0, 1, 'First')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])