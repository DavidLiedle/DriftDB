#!/usr/bin/env python3
"""
Simple debug test to understand transaction behavior
"""
import psycopg2
import sys

def test_transaction_debug():
    """Simple transaction debug"""
    print("=== TRANSACTION DEBUG TEST ===")

    try:
        # Connect with autocommit on (required for explicit transaction control)
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.autocommit = True

        print(f"Connection autocommit: {conn.autocommit}")

        cur = conn.cursor()

        print("\n1. Clean setup...")
        try:
            cur.execute("DROP TABLE IF EXISTS debug_test")
        except:
            pass

        cur.execute("CREATE TABLE debug_test (id INTEGER PRIMARY KEY, value TEXT)")
        print("✅ Created table")

        print("\n2. Explicit BEGIN...")
        cur.execute("BEGIN")
        print("✅ Executed BEGIN")

        print("\n3. INSERT in transaction...")
        cur.execute("INSERT INTO debug_test (id, value) VALUES (1, 'test')")
        print("✅ Executed INSERT")

        print("\n4. Check count...")
        cur.execute("SELECT COUNT(*) FROM debug_test")
        count = cur.fetchone()[0]
        print(f"Count: {count}")

        print("\n5. ROLLBACK...")
        cur.execute("ROLLBACK")
        print("✅ Executed ROLLBACK")

        print("\n6. Check count after ROLLBACK...")
        cur.execute("SELECT COUNT(*) FROM debug_test")
        final_count = cur.fetchone()[0]
        print(f"Final count: {final_count}")

        if final_count == 0:
            print("🎉 ROLLBACK WORKED!")
            success = True
        else:
            print("❌ ROLLBACK FAILED!")
            success = False

        # Cleanup
        try:
            cur.execute("DROP TABLE IF EXISTS debug_test")
        except:
            pass

        return 0 if success else 1

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    sys.exit(test_transaction_debug())