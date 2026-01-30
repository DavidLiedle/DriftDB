//! Simple, comprehensive benchmarks for DriftDB
//!
//! These benchmarks test core operations with the current API

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use driftdb_core::{Engine, Query};
use serde_json::json;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn setup_engine_with_data(rt: &Runtime, rows: usize) -> (Engine, TempDir) {
    let _guard = rt.enter();
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table with simple API - primary key is "id", and we can index "status"
    engine
        .create_table("bench_table", "id", vec!["status".to_string()])
        .unwrap();

    // Insert data
    for i in 0..rows {
        engine
            .insert_record(
                "bench_table",
                json!({
                    "id": format!("key_{}", i),
                    "value": i * 10,
                    "status": if i % 2 == 0 { "active" } else { "inactive" }
                }),
            )
            .unwrap();
    }

    (engine, temp_dir)
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    let rt = Runtime::new().unwrap();

    for size in [1, 10, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let _guard = rt.enter();
                    let temp_dir = TempDir::new().unwrap();
                    let mut engine = Engine::init(temp_dir.path()).unwrap();
                    engine.create_table("bench_table", "id", vec![]).unwrap();
                    (engine, temp_dir)
                },
                |(mut engine, _temp)| {
                    let _guard = rt.enter();
                    for i in 0..size {
                        black_box(
                            engine
                                .insert_record(
                                    "bench_table",
                                    json!({
                                        "id": format!("key_{}", i),
                                        "value": i
                                    }),
                                )
                                .unwrap(),
                        );
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("select");
    let rt = Runtime::new().unwrap();

    for dataset_size in [100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("by_pk", dataset_size),
            dataset_size,
            |b, &size| {
                let (engine, _temp) = setup_engine_with_data(&rt, size);

                b.iter(|| {
                    let _guard = rt.enter();
                    let query = Query::Select {
                        table: "bench_table".to_string(),
                        conditions: vec![], // Simplified - no WHERE clause for now
                        as_of: None,
                        limit: None,
                    };
                    black_box(engine.query(&query).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("full_scan", dataset_size),
            dataset_size,
            |b, &size| {
                let (engine, _temp) = setup_engine_with_data(&rt, size);

                b.iter(|| {
                    let _guard = rt.enter();
                    let query = Query::Select {
                        table: "bench_table".to_string(),
                        conditions: vec![],
                        as_of: None,
                        limit: None,
                    };
                    black_box(engine.query(&query).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    let rt = Runtime::new().unwrap();

    group.bench_function("single", |b| {
        b.iter_batched(
            || setup_engine_with_data(&rt, 100),
            |(mut engine, _temp)| {
                let _guard = rt.enter();
                black_box(
                    engine
                        .update_record(
                            "bench_table",
                            json!({"id": "key_50"}),
                            json!({"value": 999}),
                        )
                        .unwrap(),
                );
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    let rt = Runtime::new().unwrap();

    group.bench_function("single", |b| {
        b.iter_batched(
            || setup_engine_with_data(&rt, 100),
            |(mut engine, _temp)| {
                let _guard = rt.enter();
                black_box(
                    engine
                        .delete_record("bench_table", json!({"id": "key_50"}))
                        .unwrap(),
                );
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_time_travel(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_travel");
    let rt = Runtime::new().unwrap();

    group.bench_function("as_of_seq", |b| {
        let _guard = rt.enter();
        let (mut engine, _temp) = setup_engine_with_data(&rt, 100);

        // Do some updates to create history
        for i in 0..50 {
            engine
                .update_record(
                    "bench_table",
                    json!({"id": format!("key_{}", i)}),
                    json!({"value": i * 100}),
                )
                .unwrap();
        }

        b.iter(|| {
            let _guard = rt.enter();
            let query = Query::Select {
                table: "bench_table".to_string(),
                conditions: vec![],
                as_of: Some(driftdb_core::query::AsOf::Sequence(50)),
                limit: None,
            };
            black_box(engine.query(&query).unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_select,
    bench_update,
    bench_delete,
    bench_time_travel
);
criterion_main!(benches);
