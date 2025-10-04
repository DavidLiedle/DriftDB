#!/usr/bin/env python3
"""
Test DriftDB's EXPLAIN PLAN functionality
"""
import psycopg2
import sys

def main():
    print("Testing DriftDB EXPLAIN PLAN")
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
        # Create test tables
        print("\n1. Creating test tables...")

        # Drop tables if they exist
        cur.execute("DROP TABLE IF EXISTS orders")
        cur.execute("DROP TABLE IF EXISTS customers")

        cur.execute("""
            CREATE TABLE customers (
                id INT PRIMARY KEY,
                name VARCHAR,
                city VARCHAR
            )
        """)

        cur.execute("""
            CREATE TABLE orders (
                id INT PRIMARY KEY,
                customer_id INT,
                amount INT,
                status VARCHAR
            )
        """)
        print("   ✓ Tables created")

        # Insert test data
        print("\n2. Inserting test data...")
        customers = [
            (1, 'Alice', 'New York'),
            (2, 'Bob', 'Los Angeles'),
            (3, 'Charlie', 'Chicago'),
            (4, 'Diana', 'Houston'),
            (5, 'Eve', 'Phoenix'),
        ]

        orders = [
            (1, 1, 100, 'completed'),
            (2, 1, 200, 'pending'),
            (3, 2, 150, 'completed'),
            (4, 3, 300, 'completed'),
            (5, 3, 50, 'cancelled'),
            (6, 4, 400, 'pending'),
        ]

        for cust in customers:
            cur.execute(
                "INSERT INTO customers (id, name, city) VALUES (%s, %s, %s)",
                cust
            )

        for order in orders:
            cur.execute(
                "INSERT INTO orders (id, customer_id, amount, status) VALUES (%s, %s, %s, %s)",
                order
            )

        print(f"   ✓ {len(customers)} customers and {len(orders)} orders inserted")

        # Test EXPLAIN PLAN
        print("\n" + "=" * 30 + " EXPLAIN TESTS " + "=" * 30)

        print("\n3. EXPLAIN simple SELECT:")
        cur.execute("EXPLAIN SELECT * FROM customers")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n4. EXPLAIN SELECT with WHERE:")
        cur.execute("EXPLAIN SELECT * FROM customers WHERE city = 'Chicago'")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n5. EXPLAIN JOIN query:")
        cur.execute("""
            EXPLAIN
            SELECT c.name, o.amount, o.status
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            WHERE o.status = 'completed'
        """)
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n6. EXPLAIN with aggregation:")
        cur.execute("""
            EXPLAIN
            SELECT c.city, COUNT(*) as order_count, SUM(o.amount) as total
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            GROUP BY c.city
        """)
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n7. EXPLAIN with DISTINCT:")
        cur.execute("EXPLAIN SELECT DISTINCT city FROM customers")
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n8. EXPLAIN with ORDER BY and LIMIT:")
        cur.execute("""
            EXPLAIN
            SELECT * FROM orders
            ORDER BY amount DESC
            LIMIT 3
        """)
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n9. EXPLAIN with UNION:")
        cur.execute("""
            EXPLAIN
            SELECT name FROM customers WHERE city = 'New York'
            UNION
            SELECT name FROM customers WHERE city = 'Chicago'
        """)
        plan = cur.fetchall()
        print("   Query Plan:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n10. EXPLAIN ANALYZE (with execution timing):")
        cur.execute("""
            EXPLAIN ANALYZE
            SELECT c.name, COUNT(o.id)
            FROM customers c
            LEFT JOIN orders o ON c.id = o.customer_id
            GROUP BY c.name
        """)
        plan = cur.fetchall()
        print("   Query Plan with Timing:")
        for row in plan:
            print(f"   {row[0]}")

        print("\n" + "=" * 60)
        print("✅ ALL EXPLAIN PLAN TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS orders")
            cur.execute("DROP TABLE IF EXISTS customers")
        except:
            pass
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()