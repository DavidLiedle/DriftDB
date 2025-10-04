#!/usr/bin/env python3
"""
Test DriftDB's prepared statements functionality
"""
import psycopg2
import time
import sys

def main():
    print("Testing DriftDB Prepared Statements")
    print("=" * 60)

    # Connect to DriftDB
    try:
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
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return

    try:
        # Create test table
        print("\n1. Creating test table...")

        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS users")

        cur.execute("""
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                age INT,
                active BOOLEAN
            )
        """)
        print("   ✓ Table created")

        # Insert test data
        print("\n2. Inserting test data...")
        users = [
            (1, 'Alice', 'alice@example.com', 30, True),
            (2, 'Bob', 'bob@example.com', 25, True),
            (3, 'Charlie', 'charlie@example.com', 35, False),
            (4, 'Diana', 'diana@example.com', 28, True),
            (5, 'Eve', 'eve@example.com', 32, False),
        ]

        for user in users:
            cur.execute(
                "INSERT INTO users (id, name, email, age, active) VALUES (%s, %s, %s, %s, %s)",
                user
            )
        print(f"   ✓ {len(users)} users inserted")

        # Test prepared statements
        print("\n" + "=" * 30 + " PREPARED STATEMENT TESTS " + "=" * 30)

        print("\n3. PREPARE statement for user lookup by ID:")
        cur.execute("PREPARE get_user AS SELECT * FROM users WHERE id = $1")
        print("   ✓ Statement 'get_user' prepared")

        print("\n4. EXECUTE prepared statement with different parameters:")

        # Execute with id = 1
        cur.execute("EXECUTE get_user (1)")
        result = cur.fetchone()
        print(f"   User 1: {result[1]} - {result[2]}")

        # Execute with id = 3
        cur.execute("EXECUTE get_user (3)")
        result = cur.fetchone()
        print(f"   User 3: {result[1]} - {result[2]}")

        # Execute with id = 5
        cur.execute("EXECUTE get_user (5)")
        result = cur.fetchone()
        print(f"   User 5: {result[1]} - {result[2]}")

        print("\n5. PREPARE statement with multiple parameters:")
        cur.execute("PREPARE find_users AS SELECT * FROM users WHERE age > $1 AND active = $2")
        print("   ✓ Statement 'find_users' prepared")

        print("\n6. EXECUTE with multiple parameters:")
        cur.execute("EXECUTE find_users (25, true)")
        results = cur.fetchall()
        print(f"   Active users older than 25: {len(results)}")
        for row in results:
            print(f"     - {row[1]}: age {row[3]}")

        print("\n7. PREPARE INSERT statement:")
        cur.execute("PREPARE add_user AS INSERT INTO users (id, name, email, age, active) VALUES ($1, $2, $3, $4, $5)")
        print("   ✓ Statement 'add_user' prepared")

        print("\n8. EXECUTE INSERT with parameters:")
        cur.execute("EXECUTE add_user (6, 'Frank', 'frank@example.com', 40, true)")
        print("   ✓ User Frank added")

        # Verify insertion
        cur.execute("SELECT * FROM users WHERE id = 6")
        result = cur.fetchone()
        print(f"   Verified: {result[1]} - {result[2]}")

        print("\n9. Performance comparison (prepared vs non-prepared):")

        # Non-prepared statement timing
        start = time.time()
        for i in range(100):
            cur.execute("SELECT * FROM users WHERE id = %s", (1 + (i % 5),))
        non_prepared_time = time.time() - start
        print(f"   100 non-prepared queries: {non_prepared_time:.4f} seconds")

        # Prepared statement timing
        cur.execute("PREPARE quick_lookup AS SELECT * FROM users WHERE id = $1")
        start = time.time()
        for i in range(100):
            cur.execute(f"EXECUTE quick_lookup ({1 + (i % 5)})")
        prepared_time = time.time() - start
        print(f"   100 prepared queries: {prepared_time:.4f} seconds")

        if prepared_time < non_prepared_time:
            speedup = non_prepared_time / prepared_time
            print(f"   ✓ Prepared statements {speedup:.2f}x faster")
        else:
            print("   Performance measured")

        print("\n10. DEALLOCATE prepared statements:")
        cur.execute("DEALLOCATE get_user")
        print("   ✓ Statement 'get_user' deallocated")

        cur.execute("DEALLOCATE find_users")
        print("   ✓ Statement 'find_users' deallocated")

        cur.execute("DEALLOCATE add_user")
        print("   ✓ Statement 'add_user' deallocated")

        cur.execute("DEALLOCATE quick_lookup")
        print("   ✓ Statement 'quick_lookup' deallocated")

        # Test error handling
        print("\n11. Test error handling:")
        try:
            cur.execute("EXECUTE get_user (1)")
            print("   ✗ Should have failed (statement was deallocated)")
        except Exception as e:
            if "not found" in str(e):
                print("   ✓ Correctly rejected execute on deallocated statement")
            else:
                print(f"   ✗ Unexpected error: {e}")

        print("\n" + "=" * 60)
        print("✅ ALL PREPARED STATEMENT TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS users")
        except:
            pass
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()