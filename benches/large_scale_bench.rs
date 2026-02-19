//! Large-scale performance benchmarks for DriftDB
//!
//! These benchmarks test DriftDB performance at scale:
//! - 100K, 500K, and 1M row operations
//! - Concurrent operation stress tests
//! - Memory pressure scenarios
//! - Time-travel with 100K+ event history
//!
//! WARNING: These benchmarks may take significant time and memory to run.
//! Run with: cargo bench --bench large_scale_bench

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use driftdb_core::sql_bridge;
use driftdb_core::Engine;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

/// Benchmark INSERT operations at large scale (100K+ rows)
fn bench_large_scale_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_insert");
    // Configure for longer-running benchmarks
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    for num_rows in &[100_000u64, 500_000u64] {
        group.throughput(Throughput::Elements(*num_rows));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            num_rows,
            |b, &size| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let mut engine = Engine::init(temp_dir.path()).unwrap();
                        sql_bridge::execute_sql(
                            &mut engine,
                            "CREATE TABLE large_insert (id INTEGER PRIMARY KEY, value TEXT, score INTEGER)",
                        )
                        .unwrap();
                        (engine, temp_dir)
                    },
                    |(mut engine, _temp)| {
                        for i in 0..size {
                            let sql = format!(
                                "INSERT INTO large_insert VALUES ({}, 'value_{}', {})",
                                i, i, i % 1000
                            );
                            sql_bridge::execute_sql(&mut engine, &sql).ok();
                        }
                        black_box(engine)
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark SELECT operations on large tables (100K+ rows)
fn bench_large_scale_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_select");
    group.sample_size(10);

    for num_rows in &[100_000u64, 500_000u64] {
        // Setup: create and populate table once
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE large_select (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)",
        )
        .unwrap();

        let setup_start = Instant::now();
        for i in 0..*num_rows {
            let sql = format!(
                "INSERT INTO large_select VALUES ({}, 'cat_{}', {})",
                i,
                i % 100, // 100 categories
                i
            );
            sql_bridge::execute_sql(&mut engine, &sql).ok();
        }
        println!("Setup {} rows in {:?}", num_rows, setup_start.elapsed());

        let engine = Arc::new(Mutex::new(engine));

        // Benchmark full table scan
        let engine_clone = engine.clone();
        group.bench_with_input(BenchmarkId::new("full_scan", num_rows), num_rows, |b, _| {
            b.iter(|| {
                let mut eng = engine_clone.lock().unwrap();
                black_box(sql_bridge::execute_sql(
                    &mut eng,
                    "SELECT COUNT(*) FROM large_select",
                ))
            });
        });

        // Benchmark filtered query (10% of data)
        let engine_clone = engine.clone();
        group.bench_with_input(
            BenchmarkId::new("filtered_10pct", num_rows),
            num_rows,
            |b, _| {
                b.iter(|| {
                    let mut eng = engine_clone.lock().unwrap();
                    black_box(sql_bridge::execute_sql(
                        &mut eng,
                        "SELECT * FROM large_select WHERE category IN ('cat_0', 'cat_1', 'cat_2', 'cat_3', 'cat_4', 'cat_5', 'cat_6', 'cat_7', 'cat_8', 'cat_9') LIMIT 1000",
                    ))
                });
            },
        );

        // Benchmark point query
        let engine_clone = engine.clone();
        let rows = *num_rows;
        group.bench_with_input(
            BenchmarkId::new("point_query", num_rows),
            num_rows,
            |b, _| {
                b.iter(|| {
                    let id = fastrand::u64(0..rows);
                    let mut eng = engine_clone.lock().unwrap();
                    black_box(sql_bridge::execute_sql(
                        &mut eng,
                        &format!("SELECT * FROM large_select WHERE id = {}", id),
                    ))
                });
            },
        );

        // Benchmark aggregation
        let engine_clone = engine.clone();
        group.bench_with_input(
            BenchmarkId::new("aggregation", num_rows),
            num_rows,
            |b, _| {
                b.iter(|| {
                    let mut eng = engine_clone.lock().unwrap();
                    black_box(sql_bridge::execute_sql(
                        &mut eng,
                        "SELECT category, COUNT(*), AVG(value) FROM large_select GROUP BY category",
                    ))
                });
            },
        );
    }

    group.finish();
}

/// Benchmark time-travel queries with large event history (100K+ events)
fn bench_large_scale_time_travel(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_time_travel");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(20));

    for num_events in &[100_000u64, 250_000u64] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE large_tt (id INTEGER PRIMARY KEY, value TEXT, version INTEGER)",
        )
        .unwrap();

        // Insert events with updates to create rich history
        let setup_start = Instant::now();
        for i in 0..*num_events {
            let id = i % 10000; // 10K unique rows with many updates
            if i < 10000 {
                // Initial inserts
                let sql = format!("INSERT INTO large_tt VALUES ({}, 'value_{}', 1)", id, i);
                sql_bridge::execute_sql(&mut engine, &sql).ok();
            } else {
                // Updates to create version history
                let sql = format!(
                    "UPDATE large_tt SET value = 'updated_{}', version = {} WHERE id = {}",
                    i,
                    (i / 10000) + 1,
                    id
                );
                sql_bridge::execute_sql(&mut engine, &sql).ok();
            }
        }
        println!("Setup {} events in {:?}", num_events, setup_start.elapsed());

        // Create snapshot midway for comparison
        let _snapshot_result = engine.create_snapshot("large_tt");

        // Benchmark recent time-travel (last 1%)
        group.bench_with_input(
            BenchmarkId::new("recent_1pct", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    let target_seq = (num_events * 99) / 100;
                    black_box(sql_bridge::execute_sql(
                        &mut engine,
                        &format!("SELECT * FROM large_tt AS OF @seq:{} LIMIT 100", target_seq),
                    ))
                });
            },
        );

        // Benchmark mid-history time-travel (50%)
        group.bench_with_input(
            BenchmarkId::new("mid_50pct", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    let target_seq = num_events / 2;
                    black_box(sql_bridge::execute_sql(
                        &mut engine,
                        &format!("SELECT * FROM large_tt AS OF @seq:{} LIMIT 100", target_seq),
                    ))
                });
            },
        );

        // Benchmark early history time-travel (10%)
        group.bench_with_input(
            BenchmarkId::new("early_10pct", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    let target_seq = num_events / 10;
                    black_box(sql_bridge::execute_sql(
                        &mut engine,
                        &format!("SELECT * FROM large_tt AS OF @seq:{} LIMIT 100", target_seq),
                    ))
                });
            },
        );

        // Benchmark aggregation on historical data
        group.bench_with_input(
            BenchmarkId::new("historical_aggregation", num_events),
            num_events,
            |b, _| {
                b.iter(|| {
                    let target_seq = num_events / 2;
                    black_box(sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "SELECT COUNT(*), AVG(version) FROM large_tt AS OF @seq:{}",
                            target_seq
                        ),
                    ))
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent operation stress tests
fn bench_concurrent_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_stress");
    group.sample_size(10);

    // Setup shared database
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    sql_bridge::execute_sql(
        &mut engine,
        "CREATE TABLE stress_test (id INTEGER PRIMARY KEY, value INTEGER, thread_id INTEGER)",
    )
    .unwrap();

    // Pre-populate with 50K rows
    for i in 0..50_000 {
        let sql = format!("INSERT INTO stress_test VALUES ({}, {}, 0)", i, i);
        sql_bridge::execute_sql(&mut engine, &sql).ok();
    }

    let engine = Arc::new(Mutex::new(engine));

    // Benchmark concurrent reads (4 threads)
    for num_threads in &[2, 4, 8] {
        let engine_clone = engine.clone();
        group.bench_with_input(
            BenchmarkId::new("concurrent_reads", num_threads),
            num_threads,
            |b, &threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|_t| {
                            let ec = engine_clone.clone();
                            thread::spawn(move || {
                                for _ in 0..100 {
                                    let id = fastrand::u64(0..50_000);
                                    let mut eng = ec.lock().unwrap();
                                    black_box(
                                        sql_bridge::execute_sql(
                                            &mut eng,
                                            &format!("SELECT * FROM stress_test WHERE id = {}", id),
                                        )
                                        .ok(),
                                    );
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }

    // Benchmark mixed read/write workload
    let engine_clone = engine.clone();
    group.bench_function("mixed_read_write_4threads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|t| {
                    let ec = engine_clone.clone();
                    thread::spawn(move || {
                        for i in 0..50 {
                            let mut eng = ec.lock().unwrap();
                            if t % 2 == 0 {
                                // Reader threads
                                let id = fastrand::u64(0..50_000);
                                black_box(
                                    sql_bridge::execute_sql(
                                        &mut eng,
                                        &format!("SELECT * FROM stress_test WHERE id = {}", id),
                                    )
                                    .ok(),
                                );
                            } else {
                                // Writer threads
                                let id = fastrand::u64(0..50_000);
                                black_box(
                                    sql_bridge::execute_sql(
                                        &mut eng,
                                        &format!(
                                            "UPDATE stress_test SET value = {} WHERE id = {}",
                                            i, id
                                        ),
                                    )
                                    .ok(),
                                );
                            }
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.finish();
}

/// Benchmark snapshot operations at large scale
fn bench_large_scale_snapshots(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_snapshots");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    for num_rows in &[100_000u64, 250_000u64] {
        group.throughput(Throughput::Elements(*num_rows));

        group.bench_with_input(
            BenchmarkId::new("create_snapshot", num_rows),
            num_rows,
            |b, &size| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let mut engine = Engine::init(temp_dir.path()).unwrap();

                        sql_bridge::execute_sql(
                            &mut engine,
                            "CREATE TABLE snap_large (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)",
                        )
                        .unwrap();

                        // Insert data
                        for i in 0..size {
                            let sql = format!(
                                "INSERT INTO snap_large VALUES ({}, 'data_{}', {})",
                                i, i, i
                            );
                            sql_bridge::execute_sql(&mut engine, &sql).ok();
                        }

                        (engine, temp_dir)
                    },
                    |(engine, _temp)| {
                        black_box(engine.create_snapshot("snap_large").ok())
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark memory pressure scenarios (many small tables vs few large tables)
fn bench_memory_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pressure");
    group.sample_size(10);

    // Scenario 1: Many small tables (100 tables x 1000 rows each)
    group.bench_function("many_small_tables", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let engine = Engine::init(temp_dir.path()).unwrap();
                (engine, temp_dir)
            },
            |(mut engine, _temp)| {
                for t in 0..100 {
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "CREATE TABLE small_{} (id INTEGER PRIMARY KEY, value TEXT)",
                            t
                        ),
                    )
                    .ok();

                    for i in 0..1000 {
                        sql_bridge::execute_sql(
                            &mut engine,
                            &format!("INSERT INTO small_{} VALUES ({}, 'val_{}')", t, i, i),
                        )
                        .ok();
                    }
                }
                black_box(engine)
            },
            BatchSize::PerIteration,
        );
    });

    // Scenario 2: Few large tables (10 tables x 10000 rows each)
    group.bench_function("few_large_tables", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let engine = Engine::init(temp_dir.path()).unwrap();
                (engine, temp_dir)
            },
            |(mut engine, _temp)| {
                for t in 0..10 {
                    sql_bridge::execute_sql(
                        &mut engine,
                        &format!(
                            "CREATE TABLE large_{} (id INTEGER PRIMARY KEY, value TEXT)",
                            t
                        ),
                    )
                    .ok();

                    for i in 0..10000 {
                        sql_bridge::execute_sql(
                            &mut engine,
                            &format!("INSERT INTO large_{} VALUES ({}, 'val_{}')", t, i, i),
                        )
                        .ok();
                    }
                }
                black_box(engine)
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// Benchmark index performance at large scale
fn bench_large_scale_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_index");
    group.sample_size(10);

    for num_rows in &[100_000u64, 500_000u64] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE idx_large (id INTEGER PRIMARY KEY, indexed_col INTEGER, value TEXT)",
        )
        .unwrap();

        // Insert data
        let setup_start = Instant::now();
        for i in 0..*num_rows {
            let sql = format!(
                "INSERT INTO idx_large VALUES ({}, {}, 'value_{}')",
                i,
                i % 1000, // 1000 distinct values
                i
            );
            sql_bridge::execute_sql(&mut engine, &sql).ok();
        }
        println!(
            "Index benchmark: inserted {} rows in {:?}",
            num_rows,
            setup_start.elapsed()
        );

        // Wrap engine in Arc<Mutex> for benchmarks
        let engine = Arc::new(Mutex::new(engine));

        // Benchmark query WITHOUT index
        let engine_clone = engine.clone();
        group.bench_with_input(
            BenchmarkId::new("no_index_scan", num_rows),
            num_rows,
            |b, _| {
                b.iter(|| {
                    let target = fastrand::u64(0..1000);
                    let mut eng = engine_clone.lock().unwrap();
                    black_box(sql_bridge::execute_sql(
                        &mut eng,
                        &format!(
                            "SELECT * FROM idx_large WHERE indexed_col = {} LIMIT 100",
                            target
                        ),
                    ))
                });
            },
        );

        // Create index
        {
            let mut eng = engine.lock().unwrap();
            sql_bridge::execute_sql(&mut eng, "CREATE INDEX idx_col ON idx_large(indexed_col)")
                .unwrap();
        }

        // Benchmark query WITH index
        let engine_clone = engine.clone();
        group.bench_with_input(
            BenchmarkId::new("with_index_scan", num_rows),
            num_rows,
            |b, _| {
                b.iter(|| {
                    let target = fastrand::u64(0..1000);
                    let mut eng = engine_clone.lock().unwrap();
                    black_box(sql_bridge::execute_sql(
                        &mut eng,
                        &format!(
                            "SELECT * FROM idx_large WHERE indexed_col = {} LIMIT 100",
                            target
                        ),
                    ))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_large_scale_insert,
    bench_large_scale_select,
    bench_large_scale_time_travel,
    bench_concurrent_stress,
    bench_large_scale_snapshots,
    bench_memory_pressure,
    bench_large_scale_index,
);
criterion_main!(benches);
