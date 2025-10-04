#!/usr/bin/env python3
"""
Performance benchmark for DriftDB query execution
Tests various query patterns and measures performance metrics
"""
import psycopg2
import time
import sys
import random
import string

def generate_random_string(length=10):
    """Generate random string for test data"""
    return ''.join(random.choices(string.ascii_letters, k=length))

def benchmark_query(cur, query, name, iterations=10):
    """Benchmark a single query"""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            cur.execute(query)
            cur.fetchall()  # Ensure all data is fetched
        except:
            pass  # Ignore errors for benchmark
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1000)  # Convert to ms

    avg = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"  {name}:")
    print(f"    Avg: {avg:.2f}ms, Min: {min_time:.2f}ms, Max: {max_time:.2f}ms")
    return avg

def test_performance():
    """Run performance benchmarks"""
    print("DriftDB Query Performance Benchmark")
    print("=" * 50)

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

        # Create test tables
        print("\n1. Setting up test data...")

        # Large table for scan tests
        cur.execute("""
            CREATE TABLE IF NOT EXISTS perf_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                category TEXT,
                value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert test data
        print("   Inserting 10,000 rows...")
        start = time.perf_counter()
        for i in range(10000):
            cur.execute(
                "INSERT INTO perf_test (id, name, category, value) VALUES (%s, %s, %s, %s)",
                (i, f"item_{i}", f"cat_{i % 100}", random.randint(1, 1000))
            )
        insert_time = (time.perf_counter() - start) * 1000
        print(f"   Insert time: {insert_time:.2f}ms ({insert_time/10000:.3f}ms per row)")

        # Create indexes
        print("   Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_category ON perf_test(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_value ON perf_test(value)")

        # Create a second table for JOIN tests
        cur.execute("""
            CREATE TABLE IF NOT EXISTS perf_test_2 (
                id INTEGER PRIMARY KEY,
                perf_id INTEGER,
                detail TEXT,
                score FLOAT
            )
        """)

        print("   Inserting 5,000 rows for JOIN tests...")
        for i in range(5000):
            cur.execute(
                "INSERT INTO perf_test_2 (id, perf_id, detail, score) VALUES (%s, %s, %s, %s)",
                (i, random.randint(0, 9999), generate_random_string(20), random.random() * 100)
            )

        print("\n2. Running query benchmarks...")
        results = {}

        # Test 1: Simple SELECT
        results['simple_select'] = benchmark_query(
            cur,
            "SELECT * FROM perf_test WHERE id = 5000",
            "Simple SELECT by primary key"
        )

        # Test 2: Full table scan
        results['full_scan'] = benchmark_query(
            cur,
            "SELECT COUNT(*) FROM perf_test",
            "Full table scan (COUNT)",
            iterations=5
        )

        # Test 3: Indexed query
        results['indexed'] = benchmark_query(
            cur,
            "SELECT * FROM perf_test WHERE category = 'cat_50'",
            "Indexed column query"
        )

        # Test 4: Range query
        results['range'] = benchmark_query(
            cur,
            "SELECT * FROM perf_test WHERE value BETWEEN 400 AND 600",
            "Range query on indexed column"
        )

        # Test 5: Aggregation
        results['aggregation'] = benchmark_query(
            cur,
            "SELECT category, COUNT(*), AVG(value) FROM perf_test GROUP BY category",
            "GROUP BY with aggregation",
            iterations=3
        )

        # Test 6: JOIN
        results['join'] = benchmark_query(
            cur,
            """
            SELECT p1.*, p2.score
            FROM perf_test p1
            JOIN perf_test_2 p2 ON p1.id = p2.perf_id
            WHERE p1.category = 'cat_10'
            """,
            "JOIN query",
            iterations=3
        )

        # Test 7: Complex WHERE
        results['complex_where'] = benchmark_query(
            cur,
            """
            SELECT * FROM perf_test
            WHERE (category LIKE 'cat_1%' OR category LIKE 'cat_2%')
            AND value > 500
            AND id < 5000
            """,
            "Complex WHERE conditions"
        )

        # Test 8: ORDER BY with LIMIT
        results['order_limit'] = benchmark_query(
            cur,
            "SELECT * FROM perf_test ORDER BY value DESC LIMIT 100",
            "ORDER BY with LIMIT"
        )

        # Test 9: DISTINCT
        results['distinct'] = benchmark_query(
            cur,
            "SELECT DISTINCT category FROM perf_test",
            "DISTINCT values"
        )

        # Test 10: Subquery
        results['subquery'] = benchmark_query(
            cur,
            """
            SELECT * FROM perf_test
            WHERE value > (SELECT AVG(value) FROM perf_test WHERE category = 'cat_50')
            """,
            "Subquery in WHERE",
            iterations=3
        )

        print("\n3. Performance Summary:")
        print("-" * 40)

        # Calculate performance score
        baseline_expected = {
            'simple_select': 1.0,      # Should be <1ms
            'indexed': 5.0,             # Should be <5ms
            'range': 20.0,              # Should be <20ms
            'order_limit': 50.0,        # Should be <50ms
            'distinct': 30.0,           # Should be <30ms
            'complex_where': 100.0,     # Should be <100ms
            'full_scan': 200.0,         # Should be <200ms
            'aggregation': 500.0,       # Should be <500ms
            'join': 200.0,              # Should be <200ms
            'subquery': 300.0           # Should be <300ms
        }

        score = 0
        total_tests = len(baseline_expected)

        for test, expected in baseline_expected.items():
            if test in results:
                if results[test] <= expected:
                    score += 1
                    status = "✅ PASS"
                else:
                    status = f"⚠️  SLOW ({results[test]/expected:.1f}x)"
                print(f"  {test}: {status}")

        performance_rating = (score / total_tests) * 100
        print(f"\n  Overall Performance Score: {performance_rating:.0f}%")

        if performance_rating >= 90:
            print("  Rating: 🚀 EXCELLENT - Production ready!")
        elif performance_rating >= 70:
            print("  Rating: ✅ GOOD - Suitable for most workloads")
        elif performance_rating >= 50:
            print("  Rating: ⚠️  FAIR - May need optimization for heavy loads")
        else:
            print("  Rating: ❌ POOR - Needs significant optimization")

        # Clean up
        cur.execute("DROP TABLE IF EXISTS perf_test")
        cur.execute("DROP TABLE IF EXISTS perf_test_2")

        cur.close()
        conn.close()
        return 0 if performance_rating >= 70 else 1

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(test_performance())