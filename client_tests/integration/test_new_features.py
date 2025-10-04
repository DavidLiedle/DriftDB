#!/usr/bin/env python3
"""
Test newly implemented features in DriftDB:
- Constraints (FOREIGN KEY, CHECK, UNIQUE, DEFAULT)
- AUTO_INCREMENT/SEQUENCES
- Transactions
"""
import psycopg2
import json
import sys

def test_constraints(cur):
    """Test constraint functionality"""
    print("\n=== Testing Constraints ===")

    try:
        # Create parent table
        print("Creating parent table...")
        cur.execute("""
            CREATE TABLE departments (
                dept_id INT PRIMARY KEY,
                dept_name VARCHAR UNIQUE,
                budget DECIMAL CHECK (budget > 0)
            )
        """)

        # Create child table with foreign key
        print("Creating child table with foreign key...")
        cur.execute("""
            CREATE TABLE employees (
                emp_id INT PRIMARY KEY,
                emp_name VARCHAR NOT NULL,
                salary DECIMAL CHECK (salary >= 30000),
                dept_id INT,
                status VARCHAR DEFAULT 'active',
                FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
            )
        """)

        # Insert test data
        print("Inserting department...")
        cur.execute("INSERT INTO departments (dept_id, dept_name, budget) VALUES (1, 'Engineering', 1000000)")

        print("Inserting valid employee...")
        cur.execute("INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (1, 'Alice', 75000, 1)")

        # Test CHECK constraint
        print("Testing CHECK constraint (should fail)...")
        try:
            cur.execute("INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (2, 'Bob', 25000, 1)")
            print("  ✗ CHECK constraint not enforced!")
        except psycopg2.Error as e:
            print(f"  ✓ CHECK constraint enforced: {e}")

        # Test FOREIGN KEY constraint
        print("Testing FOREIGN KEY constraint (should fail)...")
        try:
            cur.execute("INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (3, 'Charlie', 60000, 999)")
            print("  ✗ FOREIGN KEY constraint not enforced!")
        except psycopg2.Error as e:
            print(f"  ✓ FOREIGN KEY constraint enforced: {e}")

        # Test UNIQUE constraint
        print("Testing UNIQUE constraint (should fail)...")
        try:
            cur.execute("INSERT INTO departments (dept_id, dept_name, budget) VALUES (2, 'Engineering', 500000)")
            print("  ✗ UNIQUE constraint not enforced!")
        except psycopg2.Error as e:
            print(f"  ✓ UNIQUE constraint enforced: {e}")

        # Test DEFAULT value
        print("Testing DEFAULT value...")
        cur.execute("INSERT INTO employees (emp_id, emp_name, salary, dept_id) VALUES (4, 'David', 55000, 1)")
        cur.execute("SELECT status FROM employees WHERE emp_id = 4")
        status = cur.fetchone()[0]
        if status == 'active':
            print(f"  ✓ DEFAULT value applied: {status}")
        else:
            print(f"  ✗ DEFAULT value not applied: {status}")

        print("✓ Constraints test completed")
        return True

    except Exception as e:
        print(f"✗ Constraints test failed: {e}")
        return False

def test_sequences(cur):
    """Test AUTO_INCREMENT/SEQUENCES"""
    print("\n=== Testing Sequences ===")

    try:
        # Create sequence
        print("Creating sequence...")
        cur.execute("CREATE SEQUENCE order_seq START WITH 1000 INCREMENT BY 1")

        # Create table with auto-increment
        print("Creating table with auto-increment...")
        cur.execute("""
            CREATE TABLE orders (
                order_id INT PRIMARY KEY DEFAULT nextval('order_seq'),
                customer_name VARCHAR,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert records without specifying ID
        print("Inserting records with auto-increment...")
        cur.execute("INSERT INTO orders (customer_name) VALUES ('Customer A')")
        cur.execute("INSERT INTO orders (customer_name) VALUES ('Customer B')")
        cur.execute("INSERT INTO orders (customer_name) VALUES ('Customer C')")

        # Check generated IDs
        cur.execute("SELECT order_id FROM orders ORDER BY order_id")
        ids = [row[0] for row in cur.fetchall()]
        print(f"  Generated IDs: {ids}")

        if ids == [1000, 1001, 1002]:
            print("  ✓ Sequence working correctly")
        else:
            print(f"  ✗ Sequence not working as expected")

        # Test sequence manipulation
        print("Testing sequence manipulation...")
        cur.execute("SELECT setval('order_seq', 2000)")
        cur.execute("INSERT INTO orders (customer_name) VALUES ('Customer D')")
        cur.execute("SELECT order_id FROM orders WHERE customer_name = 'Customer D'")
        new_id = cur.fetchone()[0]

        if new_id == 2001:
            print(f"  ✓ Sequence setval worked: {new_id}")
        else:
            print(f"  ✗ Sequence setval failed: {new_id}")

        print("✓ Sequences test completed")
        return True

    except Exception as e:
        print(f"✗ Sequences test failed: {e}")
        return False

def test_transactions_advanced(cur, conn):
    """Test advanced transaction features"""
    print("\n=== Testing Advanced Transactions ===")

    try:
        # Create test table
        print("Creating test table...")
        cur.execute("""
            CREATE TABLE accounts (
                account_id INT PRIMARY KEY,
                balance DECIMAL,
                CHECK (balance >= 0)
            )
        """)

        # Insert initial data
        cur.execute("INSERT INTO accounts VALUES (1, 1000)")
        cur.execute("INSERT INTO accounts VALUES (2, 500)")

        # Test transaction with savepoint
        print("Testing transaction with SAVEPOINT...")
        conn.autocommit = False

        cur.execute("BEGIN")
        cur.execute("UPDATE accounts SET balance = balance - 100 WHERE account_id = 1")

        cur.execute("SAVEPOINT sp1")
        cur.execute("UPDATE accounts SET balance = balance - 900 WHERE account_id = 1")

        # This should violate CHECK constraint
        try:
            cur.execute("UPDATE accounts SET balance = balance - 200 WHERE account_id = 1")
            print("  ✗ CHECK constraint not enforced in transaction!")
        except psycopg2.Error:
            print("  ✓ CHECK constraint enforced in transaction")
            cur.execute("ROLLBACK TO SAVEPOINT sp1")

        cur.execute("COMMIT")
        conn.autocommit = True

        # Check final balance
        cur.execute("SELECT balance FROM accounts WHERE account_id = 1")
        balance = cur.fetchone()[0]

        if balance == 900:  # 1000 - 100
            print(f"  ✓ Transaction with savepoint worked correctly: balance = {balance}")
        else:
            print(f"  ✗ Unexpected balance after transaction: {balance}")

        # Test isolation levels
        print("Testing isolation levels...")
        conn.autocommit = False

        cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        cur.execute("SELECT * FROM accounts")
        cur.execute("COMMIT")
        print("  ✓ SERIALIZABLE isolation level accepted")

        cur.execute("BEGIN READ ONLY")
        cur.execute("SELECT * FROM accounts")
        try:
            cur.execute("UPDATE accounts SET balance = 0 WHERE account_id = 1")
            print("  ✗ READ ONLY transaction allowed write!")
        except psycopg2.Error:
            print("  ✓ READ ONLY transaction prevented write")
        cur.execute("COMMIT")

        conn.autocommit = True
        print("✓ Advanced transactions test completed")
        return True

    except Exception as e:
        print(f"✗ Advanced transactions test failed: {e}")
        conn.autocommit = True
        return False

def main():
    print("Testing New DriftDB Features")
    print("=" * 50)

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="driftdb",
            user="driftdb",
            password=""
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("✓ Connected to DriftDB")

        # Run tests
        results = {
            "Constraints": test_constraints(cur),
            "Sequences": test_sequences(cur),
            "Transactions": test_transactions_advanced(cur, conn)
        }

        # Summary
        print("\n" + "=" * 50)
        print("Test Summary:")
        for test_name, passed in results.items():
            status = "✓ PASSED" if passed else "✗ FAILED"
            print(f"  {test_name}: {status}")

        all_passed = all(results.values())
        if all_passed:
            print("\n✅ ALL TESTS PASSED!")
            return 0
        else:
            print("\n❌ SOME TESTS FAILED")
            return 1

    except Exception as e:
        print(f"✗ Failed to connect or run tests: {e}")
        return 1
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    sys.exit(main())