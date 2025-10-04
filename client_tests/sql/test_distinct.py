#!/usr/bin/env python3
"""
Test DriftDB's DISTINCT functionality
"""
import psycopg2
import sys

def main():
    print("Testing DriftDB DISTINCT Feature")
    print("=" * 50)

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
        print("\n1. Creating test table with duplicate data...")
        try:
            cur.execute("""
                CREATE TABLE products (
                    id INT PRIMARY KEY,
                    category VARCHAR,
                    brand VARCHAR,
                    price INT
                )
            """)
            print("   ✓ Table created")
        except Exception as e:
            if "already exists" in str(e):
                print("   ✓ Table already exists, dropping and recreating...")
                cur.execute("DROP TABLE products")
                cur.execute("""
                    CREATE TABLE products (
                        id INT PRIMARY KEY,
                        category VARCHAR,
                        brand VARCHAR,
                        price INT
                    )
                """)
            else:
                raise e

        # Insert test data with duplicates
        print("\n2. Inserting test data with duplicates...")
        test_data = [
            (1, 'Electronics', 'Apple', 1000),
            (2, 'Electronics', 'Samsung', 800),
            (3, 'Electronics', 'Apple', 1200),
            (4, 'Clothing', 'Nike', 100),
            (5, 'Clothing', 'Adidas', 90),
            (6, 'Clothing', 'Nike', 100),  # Duplicate price/brand combo
            (7, 'Electronics', 'Apple', 1000),  # Duplicate category/brand/price
            (8, 'Food', 'Nestle', 10),
            (9, 'Food', 'Nestle', 10),  # Duplicate
            (10, 'Electronics', 'Samsung', 800),  # Duplicate
        ]

        for product in test_data:
            cur.execute(
                "INSERT INTO products (id, category, brand, price) VALUES (%s, %s, %s, %s)",
                product
            )
        print(f"   ✓ {len(test_data)} products inserted")

        # Test DISTINCT queries
        print("\n" + "=" * 30 + " DISTINCT TESTS " + "=" * 30)

        print("\n3. SELECT DISTINCT category:")
        cur.execute("SELECT DISTINCT category FROM products")
        categories = cur.fetchall()
        print("   Distinct categories:")
        for cat in categories:
            print(f"   - {cat[0]}")

        print("\n4. SELECT DISTINCT brand:")
        cur.execute("SELECT DISTINCT brand FROM products")
        brands = cur.fetchall()
        print("   Distinct brands:")
        for brand in brands:
            print(f"   - {brand[0]}")

        print("\n5. SELECT DISTINCT category, brand:")
        cur.execute("SELECT DISTINCT category, brand FROM products")
        combos = cur.fetchall()
        print("   Distinct category-brand combinations:")
        for combo in combos:
            print(f"   - {combo[0]}, {combo[1]}")

        print("\n6. SELECT DISTINCT * (all columns):")
        cur.execute("SELECT DISTINCT * FROM products")
        all_distinct = cur.fetchall()
        print(f"   Total rows: 10, Distinct rows: {len(all_distinct)}")

        print("\n7. SELECT DISTINCT with ORDER BY:")
        cur.execute("SELECT DISTINCT category FROM products ORDER BY category")
        ordered_categories = cur.fetchall()
        print("   Distinct categories (ordered):")
        for cat in ordered_categories:
            print(f"   - {cat[0]}")

        print("\n8. SELECT DISTINCT with LIMIT:")
        cur.execute("SELECT DISTINCT brand FROM products ORDER BY brand LIMIT 3")
        limited_brands = cur.fetchall()
        print("   Top 3 distinct brands:")
        for brand in limited_brands:
            print(f"   - {brand[0]}")

        print("\n9. SELECT DISTINCT with WHERE clause:")
        cur.execute("SELECT DISTINCT brand FROM products WHERE category = 'Electronics'")
        electronics_brands = cur.fetchall()
        print("   Distinct brands in Electronics:")
        for brand in electronics_brands:
            print(f"   - {brand[0]}")

        print("\n10. Count comparison (with and without DISTINCT):")
        cur.execute("SELECT COUNT(*) FROM products")
        total_count = cur.fetchone()[0]
        print(f"   Total products: {total_count}")

        cur.execute("SELECT COUNT(DISTINCT category) FROM products")
        distinct_categories = cur.fetchone()[0]
        print(f"   Distinct categories: {distinct_categories}")

        cur.execute("SELECT COUNT(DISTINCT brand) FROM products")
        distinct_brands = cur.fetchone()[0]
        print(f"   Distinct brands: {distinct_brands}")

        print("\n" + "=" * 50)
        print("✅ ALL DISTINCT TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 50)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS products")
        except:
            pass
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()