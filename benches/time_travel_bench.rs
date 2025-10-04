use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use driftdb_core::query::executor::QueryExecutor;
use driftdb_core::sql_bridge;
use driftdb_core::{Engine, Query, QueryResult};
use std::time::Instant;
use tempfile::TempDir;

/// Benchmark time-travel query performance at different scales
fn bench_time_travel_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_travel");

    // Test different data scales
    for num_events in &[100, 1_000, 10_000] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Create a test table using SQL
        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE bench_table (id INTEGER PRIMARY KEY, value TEXT, timestamp INTEGER)",
        )
        .unwrap();

        // Insert events with some updates to create history
        let setup_start = Instant::now();
        for i in 0..*num_events {
            let sql = format!(
                "INSERT INTO bench_table (id, value, timestamp) VALUES ({}, 'value_{}', {})",
                i % 100, // Reuse IDs to create update history
                i,
                i
            );
            sql_bridge::execute_sql(&mut engine, &sql).ok();

            // Create periodic updates to simulate real usage
            if i % 10 == 0 && i > 0 {
                let update_sql = format!(
                    "UPDATE bench_table SET value = 'updated_{}' WHERE id = {}",
                    i,
                    i % 100
                );
                sql_bridge::execute_sql(&mut engine, &update_sql).ok();
            }
        }
        println!("Setup {} events in {:?}", num_events, setup_start.elapsed());

        // Get the current state for baseline comparison
        let current_result =
            sql_bridge::execute_sql(&mut engine, "SELECT COUNT(*) FROM bench_table").unwrap();

        // Benchmark time-travel to different points in history
        group.bench_with_input(
            BenchmarkId::new("recent_history", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    // Query recent history (last 10% of events)
                    let target_seq = (num_events * 9) / 10;
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "SELECT * FROM bench_table AS OF @seq:{} LIMIT 10",
                            target_seq
                        ),
                    )
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mid_history", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    // Query middle of history (50% point)
                    let target_seq = num_events / 2;
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "SELECT * FROM bench_table AS OF @seq:{} LIMIT 10",
                            target_seq
                        ),
                    )
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("early_history", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    // Query early history (first 10% of events)
                    let target_seq = num_events / 10;
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "SELECT * FROM bench_table AS OF @seq:{} LIMIT 10",
                            target_seq
                        ),
                    )
                });
            },
        );

        // Test with snapshot if we have enough data
        if *num_events >= 1_000 {
            // Create a snapshot
            engine.create_snapshot("bench_table").ok();

            group.bench_with_input(
                BenchmarkId::new("with_snapshot", num_events),
                num_events,
                |b, _| {
                    b.iter(|| {
                        // Query after snapshot point
                        let target_seq = (num_events * 3) / 4;
                        sql_bridge::execute_sql(
                            &mut engine,
                            &format!(
                                "SELECT * FROM bench_table AS OF @seq:{} LIMIT 10",
                                target_seq
                            ),
                        )
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark the overhead of event replay at scale
fn bench_event_replay_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("replay_overhead");

    for num_events in &[100, 500, 1_000, 5_000] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Create table
        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE replay_test (id INTEGER PRIMARY KEY, data TEXT)",
        )
        .unwrap();

        // Generate events
        for i in 0..*num_events {
            let sql = format!(
                "INSERT INTO replay_test (id, data) VALUES ({}, 'data_{}')",
                i, i
            );
            sql_bridge::execute_sql(&mut engine, &sql).unwrap();
        }

        // Measure the difference between current and historical queries
        group.bench_with_input(
            BenchmarkId::new("current_state", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    // Query current state (no replay needed)
                    sql_bridge::execute_sql(&mut engine, "SELECT COUNT(*) FROM replay_test")
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("full_replay", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    // Query from beginning (full replay)
                    sql_bridge::execute_sql(
                        &mut engine,
                        "SELECT COUNT(*) FROM replay_test AS OF @seq:1",
                    )
                });
            },
        );
    }

    group.finish();
}

/// Benchmark snapshot creation and usage
fn bench_snapshot_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshots");

    for num_events in &[500, 1_000, 5_000] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        // Create table with more columns to make snapshots meaningful
        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE snapshot_test (
                id INTEGER PRIMARY KEY,
                value1 TEXT,
                value2 TEXT,
                value3 INTEGER,
                value4 REAL
            )",
        )
        .unwrap();

        // Generate data
        for i in 0..*num_events {
            let sql = format!(
                "INSERT INTO snapshot_test (id, value1, value2, value3, value4)
                 VALUES ({}, 'val1_{}', 'val2_{}', {}, {})",
                i,
                i,
                i,
                i * 2,
                i as f64 * 1.5
            );
            sql_bridge::execute_sql(&mut engine, &sql).unwrap();
        }

        // Benchmark snapshot creation
        group.bench_with_input(
            BenchmarkId::new("create", num_events),
            num_events,
            |b, _| {
                b.iter(|| engine.create_snapshot("snapshot_test"));
            },
        );

        // Create a snapshot for query benchmarks
        engine.create_snapshot("snapshot_test").unwrap();

        // Benchmark queries before and after snapshot point
        let snapshot_point = num_events / 2;

        group.bench_with_input(
            BenchmarkId::new("query_before_snapshot", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "SELECT * FROM snapshot_test AS OF @seq:{} LIMIT 10",
                            snapshot_point - 10
                        ),
                    )
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("query_after_snapshot", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "SELECT * FROM snapshot_test AS OF @seq:{} LIMIT 10",
                            snapshot_point + 10
                        ),
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_time_travel_queries,
    bench_event_replay_overhead,
    bench_snapshot_performance
);
criterion_main!(benches);
