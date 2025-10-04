#!/usr/bin/env python3
"""
Basic performance test for DriftDB
"""
import psycopg2
import time
import sys

def test_basic_performance():
    """Run basic performance tests"""
    print("DriftDB Basic Performance Test")
    print("=" * 40)

    try:
        # Connect to DriftDB
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Create test table
        print("\n1. Setting up test table...")
        # Drop if exists first to ensure clean test
        cur.execute("DROP TABLE IF EXISTS perf_test")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS perf_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        """)

        # Test INSERT performance (batch of 100)
        print("\n2. Testing INSERT performance (100 rows)...")
        start = time.perf_counter()
        for i in range(100):
            cur.execute(
                "INSERT INTO perf_test (id, name, value) VALUES (%s, %s, %s)",
                (i, f"item_{i}", i * 10)
            )
        insert_time = (time.perf_counter() - start) * 1000
        print(f"   Total: {insert_time:.2f}ms")
        print(f"   Per row: {insert_time/100:.2f}ms")

        # Test SELECT performance
        print("\n3. Testing SELECT performance...")

        # Simple SELECT by primary key
        start = time.perf_counter()
        cur.execute("SELECT * FROM perf_test WHERE id = 50")
        cur.fetchall()
        select_pk_time = (time.perf_counter() - start) * 1000
        print(f"   SELECT by primary key: {select_pk_time:.2f}ms")

        # SELECT with range
        start = time.perf_counter()
        cur.execute("SELECT * FROM perf_test WHERE value > 500")
        cur.fetchall()
        select_range_time = (time.perf_counter() - start) * 1000
        print(f"   SELECT with range: {select_range_time:.2f}ms")

        # Full table scan
        start = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM perf_test")
        cur.fetchall()
        count_time = (time.perf_counter() - start) * 1000
        print(f"   COUNT(*): {count_time:.2f}ms")

        # Test UPDATE performance
        print("\n4. Testing UPDATE performance...")
        start = time.perf_counter()
        cur.execute("UPDATE perf_test SET value = value + 1 WHERE id < 50")
        update_time = (time.perf_counter() - start) * 1000
        print(f"   UPDATE 50 rows: {update_time:.2f}ms")

        # Test DELETE performance
        print("\n5. Testing DELETE performance...")
        start = time.perf_counter()
        cur.execute("DELETE FROM perf_test WHERE id >= 90")
        delete_time = (time.perf_counter() - start) * 1000
        print(f"   DELETE 10 rows: {delete_time:.2f}ms")

        # Performance summary
        print("\n6. Performance Summary:")
        print("-" * 40)

        score = 0
        total = 5

        # Check performance thresholds
        if insert_time/100 < 10:  # <10ms per insert
            print("   ✅ INSERT: Good performance")
            score += 1
        else:
            print(f"   ⚠️  INSERT: Slow ({insert_time/100:.1f}ms per row)")

        if select_pk_time < 5:  # <5ms for PK lookup
            print("   ✅ SELECT by PK: Good performance")
            score += 1
        else:
            print(f"   ⚠️  SELECT by PK: Slow ({select_pk_time:.1f}ms)")

        if select_range_time < 50:  # <50ms for range scan
            print("   ✅ SELECT range: Good performance")
            score += 1
        else:
            print(f"   ⚠️  SELECT range: Slow ({select_range_time:.1f}ms)")

        if update_time < 100:  # <100ms for bulk update
            print("   ✅ UPDATE: Good performance")
            score += 1
        else:
            print(f"   ⚠️  UPDATE: Slow ({update_time:.1f}ms)")

        if delete_time < 50:  # <50ms for bulk delete
            print("   ✅ DELETE: Good performance")
            score += 1
        else:
            print(f"   ⚠️  DELETE: Slow ({delete_time:.1f}ms)")

        performance_rating = (score / total) * 100
        print(f"\n   Overall Score: {performance_rating:.0f}%")

        if performance_rating >= 80:
            print("   Rating: ✅ GOOD - Acceptable performance")
        elif performance_rating >= 60:
            print("   Rating: ⚠️  FAIR - May need optimization")
        else:
            print("   Rating: ❌ POOR - Needs optimization")

        # Clean up
        cur.execute("DROP TABLE IF EXISTS perf_test")
        cur.close()
        conn.close()

        return 0 if performance_rating >= 60 else 1

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_basic_performance())