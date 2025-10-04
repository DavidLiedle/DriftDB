#!/usr/bin/env python3
import psycopg2
import sys

def test_sql_injection_protection():
    """Test that SQL injection attempts are blocked"""
    print("Testing SQL injection protection...")

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        cur = conn.cursor()

        # Test 1: Basic SQL injection attempt
        print("\nTest 1: Classic SQL injection with DROP TABLE")
        malicious_query = "SELECT * FROM users WHERE id = 1'; DROP TABLE users; --"
        try:
            cur.execute(malicious_query)
            print("❌ SECURITY FAILURE: SQL injection attack was not blocked!")
            return 1
        except psycopg2.Error as e:
            print(f"✓ SQL injection blocked: {e}")

        # Test 2: Union-based injection
        print("\nTest 2: UNION-based injection")
        union_injection = "SELECT * FROM products UNION SELECT password FROM admin"
        try:
            cur.execute(union_injection)
            print("❌ SECURITY FAILURE: UNION injection was not blocked!")
            return 1
        except psycopg2.Error as e:
            print(f"✓ UNION injection blocked: {e}")

        # Test 3: Boolean-based blind injection
        print("\nTest 3: Boolean-based injection")
        blind_injection = "SELECT * FROM users WHERE name = 'admin' OR '1'='1'"
        try:
            cur.execute(blind_injection)
            print("❌ SECURITY FAILURE: Boolean injection was not blocked!")
            return 1
        except psycopg2.Error as e:
            print(f"✓ Boolean injection blocked: {e}")

        # Test 4: File access attempt
        print("\nTest 4: File access attempt")
        file_injection = "SELECT load_file('/etc/passwd')"
        try:
            cur.execute(file_injection)
            print("❌ SECURITY FAILURE: File access attempt was not blocked!")
            return 1
        except psycopg2.Error as e:
            print(f"✓ File access blocked: {e}")

        # Test 5: Time-based injection
        print("\nTest 5: Time-based injection")
        time_injection = "SELECT * FROM users WHERE id = 1 AND sleep(10)"
        try:
            cur.execute(time_injection)
            print("❌ SECURITY FAILURE: Time-based injection was not blocked!")
            return 1
        except psycopg2.Error as e:
            print(f"✓ Time-based injection blocked: {e}")

        # Test 6: Valid query should work
        print("\nTest 6: Valid query should be allowed")
        valid_query = "SELECT 1"
        try:
            cur.execute(valid_query)
            result = cur.fetchone()
            print(f"✓ Valid query executed successfully: {result}")
        except psycopg2.Error as e:
            print(f"❌ Valid query was incorrectly blocked: {e}")
            return 1

        # Test 7: Valid CREATE TABLE should work
        print("\nTest 7: Valid CREATE TABLE should be allowed")
        create_query = "CREATE TABLE test_security (id INTEGER PRIMARY KEY, name TEXT)"
        try:
            cur.execute(create_query)
            conn.commit()
            print("✓ Valid CREATE TABLE executed successfully")

            # Clean up
            cur.execute("DROP TABLE test_security")
            conn.commit()
        except psycopg2.Error as e:
            print(f"❌ Valid CREATE TABLE was incorrectly blocked: {e}")
            return 1

        print("\n✅ All SQL injection protection tests passed!")
        cur.close()
        conn.close()
        return 0

    except Exception as e:
        print(f"\n❌ Test setup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_sql_injection_protection())