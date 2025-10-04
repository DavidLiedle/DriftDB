#!/usr/bin/env python3
"""
Test COUNT(*) functionality
"""
import psycopg2

def main():
    print("Testing COUNT(*)")
    print("=" * 40)

    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="driftdb",
        user="driftdb",
        password=""
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create table
    print("Creating table...")
    cur.execute("""
        CREATE TABLE count_test (
            id INT PRIMARY KEY,
            value VARCHAR
        )
    """)

    # Insert data
    print("Inserting 3 rows...")
    for i in range(1, 4):
        cur.execute(f"INSERT INTO count_test (id, value) VALUES ({i}, 'test{i}')")

    # Test COUNT(*)
    print("\nExecuting: SELECT COUNT(*) FROM count_test")
    try:
        cur.execute("SELECT COUNT(*) FROM count_test")
        result = cur.fetchone()
        print(f"Result: {result}")

        if result:
            print(f"Count value: {result[0]}")
            print(f"Type: {type(result[0])}")
        else:
            print("No result returned")

    except Exception as e:
        print(f"Error: {e}")

    # Test COUNT with column
    print("\nExecuting: SELECT COUNT(id) FROM count_test")
    try:
        cur.execute("SELECT COUNT(id) FROM count_test")
        result = cur.fetchone()
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")

    # Close
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()