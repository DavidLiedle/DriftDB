use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use driftdb_core::sql_bridge;
use driftdb_core::Engine;
use std::sync::Arc;
use tempfile::TempDir;

/// Benchmark INSERT operations at different scales
fn bench_insert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for batch_size in &[1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                let temp_dir = TempDir::new().unwrap();
                let mut engine = Engine::init(temp_dir.path()).unwrap();

                sql_bridge::execute_sql(
                    &mut engine,
                    "CREATE TABLE bench_insert (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
                )
                .unwrap();

                let mut counter = 0;
                b.iter(|| {
                    for i in 0..size {
                        let sql = format!(
                            "INSERT INTO bench_insert VALUES ({}, 'name_{}', {})",
                            counter + i,
                            counter + i,
                            counter + i
                        );
                        black_box(sql_bridge::execute_sql(&mut engine, &sql).ok());
                    }
                    counter += size;
                });
            },
        );
    }

    group.finish();
}

/// Benchmark SELECT operations with different result set sizes
fn bench_select_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("select");

    for num_rows in &[100, 1_000, 10_000] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE bench_select (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)",
        )
        .unwrap();

        // Insert test data
        for i in 0..*num_rows {
            let sql = format!(
                "INSERT INTO bench_select VALUES ({}, 'cat_{}', {})",
                i,
                i % 10, // 10 categories
                i
            );
            sql_bridge::execute_sql(&mut engine, &sql).unwrap();
        }

        group.throughput(Throughput::Elements(*num_rows as u64));

        // Benchmark full table scan
        group.bench_with_input(
            BenchmarkId::new("full_scan", num_rows),
            &engine,
            |b, engine| {
                b.iter(|| {
                    black_box(sql_bridge::execute_sql(engine, "SELECT * FROM bench_select").ok())
                });
            },
        );

        // Benchmark filtered query
        group.bench_with_input(
            BenchmarkId::new("filtered", num_rows),
            &engine,
            |b, engine| {
                b.iter(|| {
                    black_box(
                        sql_bridge::execute_sql(
                            engine,
                            "SELECT * FROM bench_select WHERE category = 'cat_5'",
                        )
                        .ok(),
                    )
                });
            },
        );

        // Benchmark aggregation
        group.bench_with_input(
            BenchmarkId::new("aggregation", num_rows),
            &engine,
            |b, engine| {
                b.iter(|| {
                    black_box(
                        sql_bridge::execute_sql(
                            engine,
                            "SELECT category, COUNT(*), AVG(value) FROM bench_select GROUP BY category",
                        )
                        .ok(),
                    )
                });
            },
        );
    }

    group.finish();
}

/// Benchmark UPDATE operations
fn bench_update_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");

    for num_rows in &[100, 1_000, 10_000] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE bench_update (id INTEGER PRIMARY KEY, value INTEGER)",
        )
        .unwrap();

        // Insert test data
        for i in 0..*num_rows {
            sql_bridge::execute_sql(
                &mut engine,
                &format!("INSERT INTO bench_update VALUES ({}, {})", i, i),
            )
            .unwrap();
        }

        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            &engine,
            |b, engine| {
                let mut counter = 0;
                b.iter(|| {
                    counter += 1;
                    black_box(
                        sql_bridge::execute_sql(
                            engine,
                            &format!(
                                "UPDATE bench_update SET value = {} WHERE id = {}",
                                counter,
                                counter % num_rows
                            ),
                        )
                        .ok(),
                    )
                });
            },
        );
    }

    group.finish();
}

/// Benchmark DELETE operations
fn bench_delete_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    for num_rows in &[100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));

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
                            "CREATE TABLE bench_delete (id INTEGER PRIMARY KEY, value INTEGER)",
                        )
                        .unwrap();

                        for i in 0..size {
                            sql_bridge::execute_sql(
                                &mut engine,
                                &format!("INSERT INTO bench_delete VALUES ({}, {})", i, i),
                            )
                            .unwrap();
                        }

                        (engine, 0)
                    },
                    |(mut engine, mut counter)| {
                        black_box(
                            sql_bridge::execute_sql(
                                &mut engine,
                                &format!("DELETE FROM bench_delete WHERE id = {}", counter),
                            )
                            .ok(),
                        );
                        counter += 1;
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark index operations
fn bench_index_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("index");

    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    sql_bridge::execute_sql(
        &mut engine,
        "CREATE TABLE bench_index (id INTEGER PRIMARY KEY, indexed_col INTEGER, value TEXT)",
    )
    .unwrap();

    // Insert test data
    for i in 0..10_000 {
        sql_bridge::execute_sql(
            &mut engine,
            &format!(
                "INSERT INTO bench_index VALUES ({}, {}, 'value_{}')",
                i,
                i % 100,
                i
            ),
        )
        .unwrap();
    }

    // Benchmark query WITHOUT index
    group.bench_function("no_index", |b| {
        b.iter(|| {
            black_box(
                sql_bridge::execute_sql(
                    &engine,
                    "SELECT * FROM bench_index WHERE indexed_col = 50",
                )
                .ok(),
            )
        });
    });

    // Create index
    sql_bridge::execute_sql(
        &mut engine,
        "CREATE INDEX idx_col ON bench_index(indexed_col)",
    )
    .unwrap();

    // Benchmark query WITH index
    group.bench_function("with_index", |b| {
        b.iter(|| {
            black_box(
                sql_bridge::execute_sql(
                    &engine,
                    "SELECT * FROM bench_index WHERE indexed_col = 50",
                )
                .ok(),
            )
        });
    });

    group.finish();
}

/// Benchmark transaction operations
fn bench_transaction_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");

    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    sql_bridge::execute_sql(
        &mut engine,
        "CREATE TABLE bench_tx (id INTEGER PRIMARY KEY, value INTEGER)",
    )
    .unwrap();

    group.bench_function("single_insert_no_tx", |b| {
        let mut counter = 0;
        b.iter(|| {
            black_box(
                sql_bridge::execute_sql(
                    &mut engine,
                    &format!("INSERT INTO bench_tx VALUES ({}, {})", counter, counter),
                )
                .ok(),
            );
            counter += 1;
        });
    });

    group.bench_function("batch_10_in_transaction", |b| {
        let mut counter = 10000;
        b.iter(|| {
            sql_bridge::execute_sql(&mut engine, "BEGIN TRANSACTION").ok();
            for i in 0..10 {
                sql_bridge::execute_sql(
                    &mut engine,
                    &format!(
                        "INSERT INTO bench_tx VALUES ({}, {})",
                        counter + i,
                        counter + i
                    ),
                )
                .ok();
            }
            black_box(sql_bridge::execute_sql(&mut engine, "COMMIT").ok());
            counter += 10;
        });
    });

    group.finish();
}

/// Benchmark snapshot operations
fn bench_snapshot_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshots");

    for num_rows in &[1_000, 10_000, 50_000] {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        sql_bridge::execute_sql(
            &mut engine,
            "CREATE TABLE bench_snapshot (id INTEGER PRIMARY KEY, value INTEGER)",
        )
        .unwrap();

        // Insert test data
        for i in 0..*num_rows {
            sql_bridge::execute_sql(
                &mut engine,
                &format!("INSERT INTO bench_snapshot VALUES ({}, {})", i, i),
            )
            .unwrap();
        }

        group.throughput(Throughput::Elements(*num_rows as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            &engine,
            |b, engine| {
                b.iter(|| {
                    black_box(sql_bridge::execute_sql(engine, "SNAPSHOT bench_snapshot").ok())
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_operations,
    bench_select_operations,
    bench_update_operations,
    bench_delete_operations,
    bench_index_operations,
    bench_transaction_operations,
    bench_snapshot_operations,
);
criterion_main!(benches);
