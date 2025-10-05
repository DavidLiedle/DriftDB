#!/usr/bin/env python3
"""
Test full SQL execution through PostgreSQL protocol
"""
import psycopg2
import sys
import json

def test_full_sql_execution():
    """Test all SQL operations through DriftDB"""
    print("Testing DriftDB SQL Execution...")

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        # Use autocommit mode to send queries directly without wrapping in transactions
        conn.autocommit = True
        cur = conn.cursor()

        # Cleanup first
        try:
            cur.execute("DROP TABLE IF EXISTS users")
            cur.execute("DROP TABLE IF EXISTS orders")
        except:
            pass

        # Test 1: CREATE TABLE
        print("\n1. Testing CREATE TABLE...")
        try:
            cur.execute("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT,
                    age INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("✅ CREATE TABLE succeeded")
        except psycopg2.Error as e:
            print(f"❌ CREATE TABLE failed: {e}")
            pass  # No rollback needed in autocommit mode

        # Test 2: INSERT
        print("\n2. Testing INSERT...")
        try:
            cur.execute("""
                INSERT INTO users (id, name, email, age)
                VALUES (1, 'Alice', 'alice@example.com', 30)
            """)
            cur.execute("""
                INSERT INTO users (id, name, email, age)
                VALUES (2, 'Bob', 'bob@example.com', 25)
            """)
            cur.execute("""
                INSERT INTO users (id, name, email, age)
                VALUES (3, 'Charlie', 'charlie@example.com', 35)
            """)
            print("✅ INSERT succeeded")
        except psycopg2.Error as e:
            print(f"❌ INSERT failed: {e}")
            pass  # No rollback needed in autocommit mode

        # Test 3: SELECT
        print("\n3. Testing SELECT...")
        try:
            cur.execute("SELECT * FROM users")
            rows = cur.fetchall()
            print(f"✅ SELECT returned {len(rows)} rows:")
            for row in rows:
                print(f"   {row}")
        except psycopg2.Error as e:
            print(f"❌ SELECT failed: {e}")

        # Test 4: SELECT with WHERE
        print("\n4. Testing SELECT with WHERE...")
        try:
            cur.execute("SELECT name, age FROM users WHERE age > 25")
            rows = cur.fetchall()
            print(f"✅ SELECT WHERE returned {len(rows)} rows:")
            for row in rows:
                print(f"   {row}")
        except psycopg2.Error as e:
            print(f"❌ SELECT WHERE failed: {e}")

        # Test 5: UPDATE
        print("\n5. Testing UPDATE...")
        try:
            cur.execute("UPDATE users SET age = age + 1 WHERE name = 'Alice'")
            print(f"✅ UPDATE succeeded, affected {cur.rowcount} rows")

            # Verify update
            cur.execute("SELECT name, age FROM users WHERE name = 'Alice'")
            row = cur.fetchone()
            print(f"   Alice's new age: {row[1] if row else 'not found'}")
        except psycopg2.Error as e:
            print(f"❌ UPDATE failed: {e}")
            pass  # No rollback needed in autocommit mode

        # Test 6: DELETE
        print("\n6. Testing DELETE...")
        try:
            cur.execute("DELETE FROM users WHERE name = 'Bob'")
            print(f"✅ DELETE succeeded, affected {cur.rowcount} rows")

            # Verify deletion
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            print(f"   Remaining users: {count}")
        except psycopg2.Error as e:
            print(f"❌ DELETE failed: {e}")
            pass  # No rollback needed in autocommit mode

        # Test 7: Aggregate functions
        print("\n7. Testing Aggregate Functions...")
        try:
            cur.execute("SELECT COUNT(*) as count, AVG(age) as avg_age, MAX(age) as max_age FROM users")
            row = cur.fetchone()
            print(f"✅ Aggregates: COUNT={row[0]}, AVG={row[1]}, MAX={row[2]}")
        except psycopg2.Error as e:
            print(f"❌ Aggregate functions failed: {e}")

        # Test 8: Transaction support
        print("\n8. Testing Transactions...")
        try:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO users (id, name, email, age) VALUES (10, 'Test', 'test@example.com', 40)")
            cur.execute("SELECT COUNT(*) FROM users")
            count_in_txn = cur.fetchone()[0]
            print(f"   Count in transaction: {count_in_txn}")

            cur.execute("ROLLBACK")
            cur.execute("SELECT COUNT(*) FROM users")
            count_after = cur.fetchone()[0]
            print(f"   Count after rollback: {count_after}")

            if count_after < count_in_txn:
                print("✅ Transaction ROLLBACK succeeded")
            else:
                print("❌ Transaction ROLLBACK did not work")
        except psycopg2.Error as e:
            print(f"❌ Transaction test failed: {e}")
            pass  # No rollback needed in autocommit mode

        # Test 9: CREATE INDEX
        print("\n9. Testing CREATE INDEX...")
        try:
            cur.execute("CREATE INDEX idx_users_age ON users (age)")
            print("✅ CREATE INDEX succeeded")
        except psycopg2.Error as e:
            print(f"❌ CREATE INDEX failed: {e}")
            pass  # No rollback needed in autocommit mode

        # Test 10: JOIN (create another table first)
        print("\n10. Testing JOIN...")
        try:
            cur.execute("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    product TEXT,
                    amount DECIMAL
                )
            """)
            cur.execute("INSERT INTO orders VALUES (1, 1, 'Laptop', 999.99)")
            cur.execute("INSERT INTO orders VALUES (2, 1, 'Mouse', 29.99)")
            cur.execute("INSERT INTO orders VALUES (3, 3, 'Keyboard', 79.99)")

            cur.execute("""
                SELECT u.name, o.product, o.amount
                FROM users u
                JOIN orders o ON u.id = o.user_id
            """)
            rows = cur.fetchall()
            print(f"✅ JOIN returned {len(rows)} rows:")
            for row in rows:
                print(f"   {row}")
        except psycopg2.Error as e:
            print(f"❌ JOIN failed: {e}")
            pass  # No rollback needed in autocommit mode

        print("\n" + "="*50)
        print("SQL Execution Test Summary Complete!")

        cur.close()
        conn.close()
        return 0

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_full_sql_execution())