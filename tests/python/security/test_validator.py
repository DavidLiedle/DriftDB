#!/usr/bin/env python3
"""
Test that the SQL validator correctly blocks injections while allowing legitimate queries
"""
import psycopg2
import sys

def test_sql_validator():
    """Test SQL validation with both safe and malicious queries"""
    print("Testing SQL Validator...")

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Test legitimate queries
        print("\n✅ Testing LEGITIMATE queries (should pass):")
        safe_queries = [
            "SELECT * FROM users WHERE id = 1",
            "CREATE TABLE test (id INT PRIMARY KEY, name TEXT)",
            "INSERT INTO test VALUES (1, 'John')",
            "UPDATE test SET name = 'Jane' WHERE id = 1",
            "DELETE FROM test WHERE id = 1",
            "SELECT COUNT(*) FROM test",
            "BEGIN",
            "COMMIT",
        ]

        for query in safe_queries:
            try:
                cur.execute(query)
                print(f"  ✓ {query[:50]}...")
            except psycopg2.Error as e:
                # Ignore table not exists errors
                if "does not exist" not in str(e) and "already exists" not in str(e):
                    print(f"  ✗ {query[:50]}... - {e}")

        # Test malicious queries
        print("\n🛡️ Testing MALICIOUS queries (should be blocked):")
        malicious_queries = [
            "SELECT * FROM users WHERE id = 1'; DROP TABLE users; --",
            "SELECT * FROM users WHERE name = 'admin' OR '1'='1'",
            "'; INSERT INTO users VALUES ('hacker'); --",
            "SELECT * FROM users UNION SELECT password FROM admin",
            "SELECT * FROM users WHERE id = 1 OR 1=1 --",
            "SELECT sleep(10)",
            "SELECT load_file('/etc/passwd')",
        ]

        blocked_count = 0
        for query in malicious_queries:
            try:
                cur.execute(query)
                print(f"  ⚠️ NOT BLOCKED: {query[:50]}...")
            except psycopg2.Error as e:
                if "SQL validation failed" in str(e) or "SQL injection" in str(e):
                    print(f"  ✓ BLOCKED: {query[:50]}...")
                    blocked_count += 1
                else:
                    print(f"  ? Error: {query[:50]}... - {e}")

        print(f"\n📊 Results: {blocked_count}/{len(malicious_queries)} malicious queries blocked")

        cur.close()
        conn.close()
        return 0 if blocked_count == len(malicious_queries) else 1

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(test_sql_validator())