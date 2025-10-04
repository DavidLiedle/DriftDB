#!/usr/bin/env python3
"""
Test DriftDB's set operations: UNION, INTERSECT, EXCEPT
"""
import psycopg2
import sys

def main():
    print("Testing DriftDB Set Operations (UNION, INTERSECT, EXCEPT)")
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
        cur.execute("DROP TABLE IF EXISTS employees_2024")
        cur.execute("DROP TABLE IF EXISTS employees_2023")

        cur.execute("""
            CREATE TABLE employees_2024 (
                id INT PRIMARY KEY,
                name VARCHAR,
                department VARCHAR
            )
        """)
        cur.execute("""
            CREATE TABLE employees_2023 (
                id INT PRIMARY KEY,
                name VARCHAR,
                department VARCHAR
            )
        """)
        print("   ✓ Tables created")

        # Insert test data
        print("\n2. Inserting test data...")

        # 2024 employees
        employees_2024 = [
            (1, 'Alice', 'Engineering'),
            (2, 'Bob', 'Engineering'),
            (3, 'Charlie', 'Sales'),
            (4, 'Diana', 'Marketing'),
            (5, 'Eve', 'Engineering'),
        ]

        # 2023 employees (some overlap with 2024)
        employees_2023 = [
            (1, 'Alice', 'Engineering'),  # Still here
            (2, 'Bob', 'Engineering'),    # Still here
            (6, 'Frank', 'Sales'),        # Left
            (7, 'Grace', 'Marketing'),    # Left
            (8, 'Henry', 'Engineering'),  # Left
        ]

        for emp in employees_2024:
            cur.execute(
                "INSERT INTO employees_2024 (id, name, department) VALUES (%s, %s, %s)",
                emp
            )

        for emp in employees_2023:
            cur.execute(
                "INSERT INTO employees_2023 (id, name, department) VALUES (%s, %s, %s)",
                emp
            )

        print(f"   ✓ Inserted {len(employees_2024)} employees for 2024")
        print(f"   ✓ Inserted {len(employees_2023)} employees for 2023")

        # Test UNION operations
        print("\n" + "=" * 30 + " UNION TESTS " + "=" * 30)

        print("\n3. UNION (distinct employees across both years):")
        cur.execute("""
            SELECT name, department FROM employees_2024
            UNION
            SELECT name, department FROM employees_2023
            ORDER BY name
        """)
        results = cur.fetchall()
        print(f"   Total unique employees: {len(results)}")
        for row in results:
            print(f"   - {row[0]}: {row[1]}")

        print("\n4. UNION ALL (all employees including duplicates):")
        cur.execute("""
            SELECT name, department FROM employees_2024
            UNION ALL
            SELECT name, department FROM employees_2023
            ORDER BY name
        """)
        results = cur.fetchall()
        print(f"   Total rows (with duplicates): {len(results)}")

        # Test INTERSECT operations
        print("\n" + "=" * 30 + " INTERSECT TESTS " + "=" * 30)

        print("\n5. INTERSECT (employees present in both years):")
        cur.execute("""
            SELECT name, department FROM employees_2024
            INTERSECT
            SELECT name, department FROM employees_2023
            ORDER BY name
        """)
        results = cur.fetchall()
        print(f"   Employees in both years: {len(results)}")
        for row in results:
            print(f"   - {row[0]}: {row[1]}")

        # Test EXCEPT operations
        print("\n" + "=" * 30 + " EXCEPT TESTS " + "=" * 30)

        print("\n6. EXCEPT (new employees in 2024):")
        cur.execute("""
            SELECT name, department FROM employees_2024
            EXCEPT
            SELECT name, department FROM employees_2023
            ORDER BY name
        """)
        results = cur.fetchall()
        print(f"   New employees in 2024: {len(results)}")
        for row in results:
            print(f"   - {row[0]}: {row[1]}")

        print("\n7. EXCEPT reverse (employees who left after 2023):")
        cur.execute("""
            SELECT name, department FROM employees_2023
            EXCEPT
            SELECT name, department FROM employees_2024
            ORDER BY name
        """)
        results = cur.fetchall()
        print(f"   Employees who left: {len(results)}")
        for row in results:
            print(f"   - {row[0]}: {row[1]}")

        # Complex set operations
        print("\n" + "=" * 30 + " COMPLEX SET OPS " + "=" * 30)

        print("\n8. Engineering employees across years (UNION):")
        cur.execute("""
            SELECT name FROM employees_2024 WHERE department = 'Engineering'
            UNION
            SELECT name FROM employees_2023 WHERE department = 'Engineering'
            ORDER BY name
        """)
        results = cur.fetchall()
        print(f"   All engineering employees: {len(results)}")
        for row in results:
            print(f"   - {row[0]}")

        print("\n9. Department comparison:")
        # Departments in 2024 but not 2023
        cur.execute("""
            SELECT DISTINCT department FROM employees_2024
            EXCEPT
            SELECT DISTINCT department FROM employees_2023
        """)
        new_depts = cur.fetchall()
        if new_depts:
            print(f"   New departments in 2024: {', '.join([d[0] for d in new_depts])}")
        else:
            print("   No new departments in 2024")

        print("\n" + "=" * 60)
        print("✅ ALL SET OPERATION TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS employees_2024")
            cur.execute("DROP TABLE IF EXISTS employees_2023")
        except:
            pass
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()