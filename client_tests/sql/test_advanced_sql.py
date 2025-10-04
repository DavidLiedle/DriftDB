#!/usr/bin/env python3
"""
Test DriftDB's advanced SQL features including:
- ORDER BY and LIMIT
- Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY and HAVING
"""
import psycopg2
import time
import sys

def main():
    print("Testing DriftDB Advanced SQL Features")
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
        print("\n1. Creating employees table...")
        try:
            cur.execute("""
                CREATE TABLE employees (
                    id INT PRIMARY KEY,
                    name VARCHAR,
                    department VARCHAR,
                    salary INT,
                    age INT
                )
            """)
            print("   ✓ Table created")
        except Exception as e:
            if "already exists" in str(e):
                print("   ✓ Table already exists, continuing...")
            else:
                raise e

        # Insert test data
        print("\n2. Inserting test data...")
        test_data = [
            (1, 'Alice', 'Engineering', 90000, 30),
            (2, 'Bob', 'Engineering', 85000, 28),
            (3, 'Charlie', 'Sales', 70000, 35),
            (4, 'Diana', 'Sales', 75000, 32),
            (5, 'Eve', 'Marketing', 60000, 26),
            (6, 'Frank', 'Engineering', 95000, 40),
            (7, 'Grace', 'Marketing', 65000, 29),
            (8, 'Henry', 'Sales', 80000, 33)
        ]

        for emp in test_data:
            cur.execute(
                "INSERT INTO employees (id, name, department, salary, age) VALUES (%s, %s, %s, %s, %s)",
                emp
            )
        print(f"   ✓ {len(test_data)} employees inserted")

        # Test ORDER BY
        print("\n" + "=" * 30 + " ORDER BY TESTS " + "=" * 30)

        print("\n3. ORDER BY salary DESC:")
        cur.execute("SELECT name, salary FROM employees ORDER BY salary DESC")
        for row in cur.fetchall():
            print(f"   {row[0]}: ${row[1]:,}")

        print("\n4. ORDER BY age ASC:")
        cur.execute("SELECT name, age FROM employees ORDER BY age ASC")
        for row in cur.fetchall():
            print(f"   {row[0]}: {row[1]} years old")

        # Test LIMIT
        print("\n" + "=" * 30 + " LIMIT TESTS " + "=" * 30)

        print("\n5. Top 3 earners (ORDER BY salary DESC LIMIT 3):")
        cur.execute("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3")
        for row in cur.fetchall():
            print(f"   {row[0]}: ${row[1]:,}")

        print("\n6. Youngest 2 employees (ORDER BY age ASC LIMIT 2):")
        cur.execute("SELECT name, age FROM employees ORDER BY age ASC LIMIT 2")
        for row in cur.fetchall():
            print(f"   {row[0]}: {row[1]} years old")

        # Test aggregation functions
        print("\n" + "=" * 30 + " AGGREGATION TESTS " + "=" * 30)

        print("\n7. Basic aggregations:")
        cur.execute("SELECT COUNT(*) FROM employees")
        total = cur.fetchone()[0]
        print(f"   Total employees: {total}")

        cur.execute("SELECT AVG(salary) FROM employees")
        avg_salary = cur.fetchone()[0]
        print(f"   Average salary: ${avg_salary:,.2f}")

        cur.execute("SELECT MIN(age), MAX(age) FROM employees")
        min_age, max_age = cur.fetchone()
        print(f"   Age range: {min_age} - {max_age} years")

        cur.execute("SELECT SUM(salary) FROM employees")
        total_payroll = cur.fetchone()[0]
        print(f"   Total payroll: ${total_payroll:,}")

        # Test GROUP BY
        print("\n" + "=" * 30 + " GROUP BY TESTS " + "=" * 30)

        print("\n8. Employees by department:")
        cur.execute("SELECT department, COUNT(*) FROM employees GROUP BY department")
        for row in cur.fetchall():
            print(f"   {row[0]}: {row[1]} employees")

        print("\n9. Average salary by department:")
        cur.execute("SELECT department, AVG(salary) FROM employees GROUP BY department ORDER BY AVG(salary) DESC")
        for row in cur.fetchall():
            print(f"   {row[0]}: ${row[1]:,.2f}")

        print("\n10. Department salary statistics:")
        cur.execute("""
            SELECT department, COUNT(*), MIN(salary), MAX(salary), AVG(salary)
            FROM employees
            GROUP BY department
            ORDER BY department
        """)
        print("   Department | Count | Min Salary | Max Salary | Avg Salary")
        print("   " + "-" * 55)
        for row in cur.fetchall():
            dept, count, min_sal, max_sal, avg_sal = row
            print(f"   {dept:<10} | {count:>5} | ${min_sal:>8,} | ${max_sal:>8,} | ${avg_sal:>8,.0f}")

        # Test HAVING
        print("\n" + "=" * 30 + " HAVING TESTS " + "=" * 30)

        print("\n11. Departments with average salary > $70,000:")
        cur.execute("""
            SELECT department, AVG(salary)
            FROM employees
            GROUP BY department
            HAVING AVG(salary) > 70000
            ORDER BY AVG(salary) DESC
        """)
        for row in cur.fetchall():
            print(f"   {row[0]}: ${row[1]:,.2f}")

        print("\n12. Departments with more than 2 employees:")
        cur.execute("""
            SELECT department, COUNT(*)
            FROM employees
            GROUP BY department
            HAVING COUNT(*) > 2
            ORDER BY COUNT(*) DESC
        """)
        for row in cur.fetchall():
            print(f"   {row[0]}: {row[1]} employees")

        # Test complex queries
        print("\n" + "=" * 30 + " COMPLEX QUERIES " + "=" * 30)

        print("\n13. Top 2 departments by average salary (complex query):")
        cur.execute("""
            SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
            FROM employees
            WHERE age >= 30
            GROUP BY department
            HAVING COUNT(*) >= 2
            ORDER BY AVG(salary) DESC
            LIMIT 2
        """)
        print("   Department | Employees (30+) | Avg Salary")
        print("   " + "-" * 40)
        for row in cur.fetchall():
            dept, count, avg_sal = row
            print(f"   {dept:<10} | {count:>13} | ${avg_sal:>9,.0f}")

        print("\n14. Engineering employees ordered by salary:")
        cur.execute("""
            SELECT name, salary
            FROM employees
            WHERE department = 'Engineering'
            ORDER BY salary DESC
        """)
        for row in cur.fetchall():
            print(f"   {row[0]}: ${row[1]:,}")

        print("\n" + "=" * 50)
        print("✅ ALL ADVANCED SQL TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 50)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            cur.execute("DROP TABLE IF EXISTS employees")
        except:
            pass
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()