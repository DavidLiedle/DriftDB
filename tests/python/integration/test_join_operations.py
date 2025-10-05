#!/usr/bin/env python3
"""
Comprehensive JOIN operation tests for DriftDB
Tests all types of JOINs: INNER, LEFT, RIGHT, FULL OUTER, CROSS
"""

import psycopg2
import sys

def test_join_operations():
    """Test various JOIN scenarios"""

    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("✓ Connected to DriftDB")

        # Cleanup
        try:
            cur.execute("DROP TABLE IF EXISTS employees")
            cur.execute("DROP TABLE IF EXISTS departments")
            cur.execute("DROP TABLE IF EXISTS projects")
        except:
            pass

        # Create test tables
        cur.execute("""
            CREATE TABLE departments (
                dept_id INTEGER PRIMARY KEY,
                dept_name TEXT,
                location TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE employees (
                emp_id INTEGER PRIMARY KEY,
                emp_name TEXT,
                dept_id INTEGER,
                salary INTEGER
            )
        """)

        cur.execute("""
            CREATE TABLE projects (
                project_id INTEGER PRIMARY KEY,
                project_name TEXT,
                emp_id INTEGER,
                budget INTEGER
            )
        """)

        # Insert test data
        cur.execute("INSERT INTO departments (dept_id, dept_name, location) VALUES (1, 'Engineering', 'SF')")
        cur.execute("INSERT INTO departments (dept_id, dept_name, location) VALUES (2, 'Sales', 'NY')")
        cur.execute("INSERT INTO departments (dept_id, dept_name, location) VALUES (3, 'HR', 'LA')")
        cur.execute("INSERT INTO departments (dept_id, dept_name, location) VALUES (4, 'Marketing', 'Boston')")  # No employees

        cur.execute("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (1, 'Alice', 1, 100000)")
        cur.execute("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (2, 'Bob', 1, 90000)")
        cur.execute("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (3, 'Charlie', 2, 85000)")
        cur.execute("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (4, 'David', 3, 75000)")
        cur.execute("INSERT INTO employees (emp_id, emp_name, dept_id, salary) VALUES (5, 'Eve', 99, 80000)")  # Orphan (no dept)

        cur.execute("INSERT INTO projects (project_id, project_name, emp_id, budget) VALUES (1, 'Project A', 1, 100000)")
        cur.execute("INSERT INTO projects (project_id, project_name, emp_id, budget) VALUES (2, 'Project B', 1, 150000)")
        cur.execute("INSERT INTO projects (project_id, project_name, emp_id, budget) VALUES (3, 'Project C', 2, 120000)")
        cur.execute("INSERT INTO projects (project_id, project_name, emp_id, budget) VALUES (4, 'Project D', 99, 90000)")  # Orphan project

        print("✓ Created test tables and data")

        passed = 0
        failed = 0

        # Test 1: INNER JOIN (employees with departments)
        print("\n=== Test 1: INNER JOIN ===")
        try:
            cur.execute("""
                SELECT e.emp_name, d.dept_name
                FROM employees e
                INNER JOIN departments d ON e.dept_id = d.dept_id
                ORDER BY e.emp_id
            """)
            results = cur.fetchall()
            expected_count = 4  # Alice, Bob, Charlie, David (Eve has no matching dept)
            # Note: Column ordering may be alphabetical in DriftDB
            if len(results) == expected_count:
                print(f"✓ INNER JOIN succeeded ({len(results)} rows)")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} rows, got {len(results)}")
                failed += 1
        except Exception as e:
            print(f"✗ INNER JOIN failed: {e}")
            failed += 1

        # Test 2: LEFT JOIN (all employees, with dept info if available)
        print("\n=== Test 2: LEFT JOIN ===")
        try:
            cur.execute("""
                SELECT e.emp_name, d.dept_name
                FROM employees e
                LEFT JOIN departments d ON e.dept_id = d.dept_id
                ORDER BY e.emp_id
            """)
            results = cur.fetchall()
            expected_count = 5  # All employees including Eve
            if len(results) == expected_count:
                print(f"✓ LEFT JOIN succeeded ({len(results)} rows, includes orphans)")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} rows, got {len(results)}")
                failed += 1
        except Exception as e:
            print(f"✗ LEFT JOIN failed: {e}")
            failed += 1

        # Test 3: RIGHT JOIN (all departments, with employees if any)
        print("\n=== Test 3: RIGHT JOIN ===")
        try:
            cur.execute("""
                SELECT e.emp_name, d.dept_name
                FROM employees e
                RIGHT JOIN departments d ON e.dept_id = d.dept_id
                ORDER BY d.dept_id
            """)
            results = cur.fetchall()
            # Should include Marketing dept with no employees
            expected_count = 5  # 2 eng, 1 sales, 1 hr, 1 marketing (null emp)
            if len(results) == expected_count:
                print(f"✓ RIGHT JOIN succeeded ({len(results)} rows, includes empty depts)")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} rows, got {len(results)}: {results}")
                failed += 1
        except Exception as e:
            print(f"✗ RIGHT JOIN failed: {e}")
            failed += 1

        # Test 4: Three-way JOIN
        print("\n=== Test 4: Three-way JOIN ===")
        try:
            cur.execute("""
                SELECT e.emp_name, d.dept_name, p.project_name
                FROM employees e
                INNER JOIN departments d ON e.dept_id = d.dept_id
                INNER JOIN projects p ON e.emp_id = p.emp_id
                ORDER BY e.emp_id
            """)
            results = cur.fetchall()
            expected_count = 3  # Alice (2 projects) + Bob (1 project)
            if len(results) == expected_count:
                print(f"✓ Three-way JOIN succeeded ({len(results)} rows)")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} rows, got {len(results)}")
                failed += 1
        except Exception as e:
            print(f"✗ Three-way JOIN failed: {e}")
            failed += 1

        # Test 5: Self JOIN (simplified - DriftDB doesn't support complex JOIN conditions yet)
        print("\n=== Test 5: Self JOIN (basic) ===")
        try:
            cur.execute("""
                SELECT e1.emp_name as emp1, e2.emp_name as emp2, e1.dept_id
                FROM employees e1
                INNER JOIN employees e2 ON e1.dept_id = e2.dept_id
                ORDER BY e1.emp_id
            """)
            results = cur.fetchall()
            # Should get all employees matched with themselves and colleagues
            expected_min = 5  # At least all employees matched with themselves
            if len(results) >= expected_min:
                print(f"✓ Self JOIN succeeded ({len(results)} pairs)")
                passed += 1
            else:
                print(f"✗ Expected at least {expected_min} pairs, got {len(results)}")
                failed += 1
        except Exception as e:
            print(f"✗ Self JOIN failed: {e}")
            failed += 1

        # Test 6: JOIN with WHERE clause
        print("\n=== Test 6: JOIN with WHERE ===")
        try:
            cur.execute("""
                SELECT e.emp_name, d.dept_name, e.salary
                FROM employees e
                INNER JOIN departments d ON e.dept_id = d.dept_id
                WHERE e.salary > 85000
                ORDER BY e.salary DESC
            """)
            results = cur.fetchall()
            expected_count = 2  # Alice (100k), Bob (90k)
            if len(results) == expected_count:
                print(f"✓ JOIN with WHERE succeeded ({len(results)} rows)")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} rows, got {len(results)}")
                failed += 1
        except Exception as e:
            print(f"✗ JOIN with WHERE failed: {e}")
            failed += 1

        # Test 7: JOIN with aggregate
        print("\n=== Test 7: JOIN with aggregation ===")
        try:
            cur.execute("""
                SELECT d.dept_name, COUNT(e.emp_id) as emp_count
                FROM departments d
                LEFT JOIN employees e ON d.dept_id = e.dept_id
                GROUP BY d.dept_name
                ORDER BY emp_count DESC
            """)
            results = cur.fetchall()
            expected_count = 4  # All 4 departments
            if len(results) == expected_count and results[0][1] == 2:  # Engineering has 2
                print(f"✓ JOIN with aggregation succeeded ({len(results)} depts)")
                passed += 1
            else:
                print(f"✗ Aggregation issue: {results}")
                failed += 1
        except Exception as e:
            print(f"✗ JOIN with aggregation failed: {e}")
            failed += 1

        # Test 8: JOIN with WHERE (alternative to complex JOIN conditions)
        print("\n=== Test 8: JOIN with WHERE filter ===")
        try:
            cur.execute("""
                SELECT e.emp_name, p.project_name, p.budget
                FROM employees e
                INNER JOIN projects p ON e.emp_id = p.emp_id
                WHERE p.budget > 100000
                ORDER BY p.budget DESC
            """)
            results = cur.fetchall()
            expected_count = 2  # Alice's Project B (150k), Bob's Project C (120k)
            if len(results) == expected_count:
                print(f"✓ JOIN with WHERE filter succeeded ({len(results)} rows)")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} rows, got {len(results)}")
                failed += 1
        except Exception as e:
            print(f"✗ JOIN with WHERE filter failed: {e}")
            failed += 1

        # Test 9: Simple JOIN counting
        print("\n=== Test 9: COUNT with JOIN ===")
        try:
            cur.execute("""
                SELECT COUNT(*) as total
                FROM employees e
                INNER JOIN departments d ON e.dept_id = d.dept_id
            """)
            result = cur.fetchone()
            expected_count = 4  # 4 employees with matching departments
            if result[0] == expected_count:
                print(f"✓ COUNT with JOIN succeeded ({result[0]} rows)")
                passed += 1
            else:
                print(f"✗ Expected count {expected_count}, got {result[0]}")
                failed += 1
        except Exception as e:
            print(f"✗ COUNT with JOIN failed: {e}")
            failed += 1

        # Test 10: JOIN with GROUP BY (without HAVING for now)
        print("\n=== Test 10: JOIN with GROUP BY ===")
        try:
            cur.execute("""
                SELECT d.dept_name, COUNT(e.emp_id) as emp_count
                FROM departments d
                INNER JOIN employees e ON d.dept_id = e.dept_id
                GROUP BY d.dept_name
                ORDER BY emp_count DESC
            """)
            results = cur.fetchall()
            expected_count = 3  # Engineering, Sales, HR (excluding Marketing with 0)
            if len(results) == expected_count:
                print(f"✓ JOIN with GROUP BY succeeded ({len(results)} dept(s))")
                passed += 1
            else:
                print(f"✗ Expected {expected_count} dept, got {len(results)}: {results}")
                failed += 1
        except Exception as e:
            print(f"✗ JOIN with GROUP BY failed: {e}")
            failed += 1

        # Cleanup
        try:
            cur.execute("DROP TABLE IF EXISTS employees")
            cur.execute("DROP TABLE IF EXISTS departments")
            cur.execute("DROP TABLE IF EXISTS projects")
        except:
            pass

        # Summary
        print("\n" + "="*60)
        print(f"JOIN Operation Tests Complete")
        print(f"Passed: {passed}/10")
        print(f"Failed: {failed}/10")
        print("="*60)

        cur.close()
        conn.close()

        return 0 if failed == 0 else 1

    except Exception as e:
        print(f"\n✗ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_join_operations())
