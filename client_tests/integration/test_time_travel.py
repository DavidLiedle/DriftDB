#!/usr/bin/env python3
import psycopg2
import time
import sys

def main():
    print("Testing DriftDB Time Travel Functionality")
    print("=" * 50)

    # Connect to DriftDB
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="driftdb",
        user="driftdb",
        password=""
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create a test table
    print("\n1. Creating test table...")
    cur.execute("CREATE TABLE history_test (id INT PRIMARY KEY, name VARCHAR, status VARCHAR)")
    print("   ✓ Table created")

    # Insert initial data (sequence 1)
    print("\n2. Inserting initial data...")
    cur.execute("INSERT INTO history_test (id, name, status) VALUES (1, 'Alice', 'active')")
    cur.execute("INSERT INTO history_test (id, name, status) VALUES (2, 'Bob', 'active')")
    cur.execute("INSERT INTO history_test (id, name, status) VALUES (3, 'Charlie', 'active')")
    print("   ✓ 3 records inserted")

    # Check current state
    print("\n3. Current state:")
    cur.execute("SELECT * FROM history_test")
    for row in cur.fetchall():
        print(f"   {row}")

    # Update some data (sequence 4-5)
    print("\n4. Updating data...")
    cur.execute("UPDATE history_test SET status = 'inactive' WHERE id = 2")
    cur.execute("UPDATE history_test SET name = 'Charles' WHERE id = 3")
    print("   ✓ Updates applied")

    # Check current state again
    print("\n5. Current state after updates:")
    cur.execute("SELECT * FROM history_test")
    for row in cur.fetchall():
        print(f"   {row}")

    # Delete a record (sequence 6)
    print("\n6. Deleting a record...")
    cur.execute("DELETE FROM history_test WHERE id = 1")
    print("   ✓ Record deleted")

    # Check current state after delete
    print("\n7. Current state after delete:")
    cur.execute("SELECT * FROM history_test")
    for row in cur.fetchall():
        print(f"   {row}")

    # Time travel queries
    print("\n" + "=" * 50)
    print("TIME TRAVEL QUERIES")
    print("=" * 50)

    # Query state at sequence 1 (after first insert)
    print("\n8. State at sequence 1 (after first insert):")
    try:
        cur.execute("SELECT * FROM history_test FOR SYSTEM_TIME AS OF @SEQ:1")
        for row in cur.fetchall():
            print(f"   {row}")
    except Exception as e:
        print(f"   Error: {e}")

    # Query state at sequence 3 (after all inserts)
    print("\n9. State at sequence 3 (after all inserts):")
    try:
        cur.execute("SELECT * FROM history_test FOR SYSTEM_TIME AS OF @SEQ:3")
        for row in cur.fetchall():
            print(f"   {row}")
    except Exception as e:
        print(f"   Error: {e}")

    # Query state at sequence 5 (after updates)
    print("\n10. State at sequence 5 (after updates):")
    try:
        cur.execute("SELECT * FROM history_test FOR SYSTEM_TIME AS OF @SEQ:5")
        for row in cur.fetchall():
            print(f"   {row}")
    except Exception as e:
        print(f"   Error: {e}")

    # Time travel with WHERE clause
    print("\n11. Time travel with WHERE clause (active records at seq 3):")
    try:
        cur.execute("SELECT * FROM history_test FOR SYSTEM_TIME AS OF @SEQ:3 WHERE status = 'active'")
        for row in cur.fetchall():
            print(f"   {row}")
    except Exception as e:
        print(f"   Error: {e}")

    # Time travel with WHERE clause (inactive records at seq 5)
    print("\n12. Time travel with WHERE clause (inactive records at seq 5):")
    try:
        cur.execute("SELECT * FROM history_test FOR SYSTEM_TIME AS OF @SEQ:5 WHERE status = 'inactive'")
        for row in cur.fetchall():
            print(f"   {row}")
    except Exception as e:
        print(f"   Error: {e}")

    print("\n" + "=" * 50)
    print("TIME TRAVEL TEST COMPLETE")
    print("=" * 50)

    # Close connection
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
