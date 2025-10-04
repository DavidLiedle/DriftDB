#!/usr/bin/env python3
"""
Test DriftDB's index optimization in WHERE clauses
"""
import psycopg2
import time
import sys

def main():
    print("Testing DriftDB Index Optimization")
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
        # Create test table with indexes
        print("\n1. Creating test table with indexes...")

        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS large_table")

        cur.execute("""
            CREATE TABLE large_table (
                id INT PRIMARY KEY,
                user_id INT INDEX,
                status VARCHAR INDEX,
                amount INT,
                description VARCHAR
            )
        """)
        print("   ✓ Table created with indexes on user_id and status")

        # Insert a large amount of test data
        print("\n2. Inserting test data...")
        batch_size = 100
        num_batches = 10
        total_rows = batch_size * num_batches

        for batch in range(num_batches):
            for i in range(batch_size):
                row_id = batch * batch_size + i + 1
                user_id = (row_id % 50) + 1  # 50 different users
                status = ['pending', 'active', 'completed', 'cancelled'][row_id % 4]
                amount = (row_id * 37) % 1000  # pseudo-random amount
                description = f'Transaction {row_id}'

                cur.execute(
                    "INSERT INTO large_table (id, user_id, status, amount, description) VALUES (%s, %s, %s, %s, %s)",
                    (row_id, user_id, status, amount, description)
                )

        print(f"   ✓ {total_rows} rows inserted")

        # Test EXPLAIN PLAN to see index usage
        print("\n" + "=" * 30 + " INDEX TESTS " + "=" * 30)

        print("\n3. EXPLAIN query with indexed column (user_id = 10):")
        cur.execute("EXPLAIN SELECT * FROM large_table WHERE user_id = 10")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n4. EXPLAIN query with indexed column (status = 'active'):")
        cur.execute("EXPLAIN SELECT * FROM large_table WHERE status = 'active'")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n5. EXPLAIN query with non-indexed column (amount > 500):")
        cur.execute("EXPLAIN SELECT * FROM large_table WHERE amount > 500")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n6. EXPLAIN compound query (indexed and non-indexed):")
        cur.execute("EXPLAIN SELECT * FROM large_table WHERE user_id = 25 AND amount > 300")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        # Performance comparison
        print("\n" + "=" * 30 + " PERFORMANCE TEST " + "=" * 30)

        print("\n7. Query performance with indexed column:")
        start = time.time()
        cur.execute("SELECT COUNT(*) FROM large_table WHERE user_id = 10")
        count = cur.fetchone()[0]
        indexed_time = time.time() - start
        print(f"   Found {count} rows in {indexed_time:.4f} seconds (indexed)")

        print("\n8. Query performance with non-indexed column:")
        start = time.time()
        cur.execute("SELECT COUNT(*) FROM large_table WHERE amount = 500")
        count = cur.fetchone()[0]
        non_indexed_time = time.time() - start
        print(f"   Found {count} rows in {non_indexed_time:.4f} seconds (non-indexed)")

        print("\n9. Complex query with multiple conditions:")
        start = time.time()
        cur.execute("""
            SELECT id, user_id, status, amount
            FROM large_table
            WHERE status = 'active' AND amount > 400
            ORDER BY amount DESC
            LIMIT 5
        """)
        results = cur.fetchall()
        complex_time = time.time() - start
        print(f"   Found top 5 active transactions > 400:")
        for row in results:
            print(f"     ID: {row[0]}, User: {row[1]}, Status: {row[2]}, Amount: {row[3]}")
        print(f"   Query completed in {complex_time:.4f} seconds")

        print("\n10. Test EXPLAIN ANALYZE with timing:")
        cur.execute("""
            EXPLAIN ANALYZE
            SELECT * FROM large_table
            WHERE user_id = 15 AND status = 'completed'
        """)
        plan = cur.fetchall()
        print("   Query Plan with Execution Time:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n" + "=" * 60)
        print("✅ INDEX OPTIMIZATION TESTS COMPLETED!")
        print("=" * 60)

        # Summary
        if indexed_time < non_indexed_time:
            speedup = non_indexed_time / indexed_time
            print(f"\n📊 Index provided {speedup:.2f}x speedup for equality queries")
        else:
            print("\n📊 Performance results recorded")

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS large_table")
        except:
            pass
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()