#!/usr/bin/env python3
"""
Simple test for transaction support
"""
import psycopg2

print("Testing Simple Transaction")
print("-" * 40)

try:
    # Connect with autocommit ON
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

    # Create a test table first
    print("\n1. Creating table...")
    cur.execute("CREATE TABLE simple_test (id INT PRIMARY KEY, value VARCHAR)")
    print("   ✓ Table created")

    # Test BEGIN directly
    print("\n2. Testing BEGIN command...")
    cur.execute("BEGIN")
    print("   ✓ BEGIN executed successfully!")

    # Test INSERT in transaction
    print("\n3. Inserting data in transaction...")
    cur.execute("INSERT INTO simple_test (id, value) VALUES (1, 'test')")
    print("   ✓ INSERT executed")

    # Test COMMIT
    print("\n4. Testing COMMIT...")
    cur.execute("COMMIT")
    print("   ✓ COMMIT executed successfully!")

    # Verify data
    print("\n5. Verifying data...")
    cur.execute("SELECT * FROM simple_test")
    rows = cur.fetchall()
    print(f"   ✓ Found {len(rows)} rows: {rows}")

    print("\n✅ TRANSACTION TEST SUCCESSFUL!")

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()