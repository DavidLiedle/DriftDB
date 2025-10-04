//! Performance benchmarks for DriftDB
//!
//! Comprehensive benchmarks covering:
//! - Write throughput (single and batch)
//! - Read latency (point queries and scans)
//! - Index performance
//! - Transaction overhead
//! - Compression ratios
//! - Memory usage

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use driftdb_core::query::{AsOf, Query, WhereCondition};
use driftdb_core::{Engine, Event, EventType, Schema};
use serde_json::json;
use std::path::PathBuf;
use tempfile::TempDir;

/// Create a test engine with sample schema
fn setup_engine() -> (Engine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::open(temp_dir.path()).unwrap();

    // Create test table with indexes
    let schema = Schema {
        primary_key: "id".to_string(),
        indexes: vec!["status".to_string(), "created_at".to_string()],
    };
    engine.create_table("bench_table", schema).unwrap();

    (engine, temp_dir)
}

/// Generate test event
fn generate_event(id: u64) -> Event {
    Event {
        sequence: id,
        timestamp: time::OffsetDateTime::now_utc(),
        event_type: EventType::Insert,
        table_name: "bench_table".to_string(),
        primary_key: json!(format!("key_{}", id)),
        payload: json!({
            "id": format!("key_{}", id),
            "status": if id % 2 == 0 { "active" } else { "inactive" },
            "created_at": "2024-01-15T10:00:00Z",
            "data": format!("Some test data for record {}", id),
            "score": id * 10,
        }),
    }
}

fn benchmark_single_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    group.bench_function("single_insert", |b| {
        let (mut engine, _temp) = setup_engine();
        let mut counter = 0u64;

        b.iter(|| {
            let event = generate_event(counter);
            engine.apply_event(event).unwrap();
            counter += 1;
        });
    });

    group.bench_function("single_update", |b| {
        let (mut engine, _temp) = setup_engine();

        // Pre-insert some records
        for i in 0..1000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        let mut counter = 0u64;

        b.iter(|| {
            let mut event = generate_event(counter % 1000);
            event.event_type = EventType::Patch;
            event.payload = json!({ "status": "updated" });
            engine.apply_event(event).unwrap();
            counter += 1;
        });
    });

    group.finish();
}

fn benchmark_batch_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write_throughput");

    for batch_size in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                let (mut engine, _temp) = setup_engine();
                let mut counter = 0u64;

                b.iter(|| {
                    let events: Vec<Event> = (0..size)
                        .map(|_| {
                            let event = generate_event(counter);
                            counter += 1;
                            event
                        })
                        .collect();

                    for event in events {
                        engine.apply_event(event).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn benchmark_point_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_latency");

    group.bench_function("point_query_by_key", |b| {
        let (mut engine, _temp) = setup_engine();

        // Insert test data
        for i in 0..10000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        b.iter(|| {
            let key = format!("key_{}", rand::random::<u64>() % 10000);
            let query = Query::Select {
                table: "bench_table".to_string(),
                conditions: vec![WhereCondition {
                    column: "id".to_string(),
                    value: json!(key),
                }],
                as_of: None,
                limit: Some(1),
            };

            black_box(engine.query(&query).unwrap());
        });
    });

    group.bench_function("point_query_by_index", |b| {
        let (mut engine, _temp) = setup_engine();

        // Insert test data
        for i in 0..10000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        b.iter(|| {
            let query = Query::Select {
                table: "bench_table".to_string(),
                conditions: vec![WhereCondition {
                    column: "status".to_string(),
                    value: json!("active"),
                }],
                as_of: None,
                limit: Some(1),
            };

            black_box(engine.query(&query).unwrap());
        });
    });

    group.finish();
}

fn benchmark_range_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");

    for result_size in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(result_size),
            result_size,
            |b, &size| {
                let (mut engine, _temp) = setup_engine();

                // Insert test data
                for i in 0..10000 {
                    engine.apply_event(generate_event(i)).unwrap();
                }

                b.iter(|| {
                    let query = Query::Select {
                        table: "bench_table".to_string(),
                        conditions: vec![WhereCondition {
                            column: "status".to_string(),
                            value: json!("active"),
                        }],
                        as_of: None,
                        limit: Some(size),
                    };

                    black_box(engine.query(&query).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn benchmark_time_travel_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_travel");

    group.bench_function("historical_point_query", |b| {
        let (mut engine, _temp) = setup_engine();

        // Insert and update test data
        for i in 0..1000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        // Update half the records
        for i in 0..500 {
            let mut event = generate_event(i);
            event.event_type = EventType::Patch;
            event.payload = json!({ "status": "modified" });
            engine.apply_event(event).unwrap();
        }

        b.iter(|| {
            let query = Query::Select {
                table: "bench_table".to_string(),
                conditions: vec![WhereCondition {
                    column: "id".to_string(),
                    value: json!("key_100"),
                }],
                as_of: Some(AsOf::Sequence(500)),
                limit: Some(1),
            };

            black_box(engine.query(&query).unwrap());
        });
    });

    group.finish();
}

fn benchmark_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");

    group.bench_function("transaction_overhead", |b| {
        let (mut engine, _temp) = setup_engine();
        let mut counter = 0u64;

        b.iter(|| {
            // Begin transaction
            let txn_id = engine
                .begin_transaction(driftdb_core::transaction::IsolationLevel::ReadCommitted)
                .unwrap();

            // Perform operations
            for _ in 0..10 {
                let event = generate_event(counter);
                engine.apply_event_in_transaction(txn_id, event).unwrap();
                counter += 1;
            }

            // Commit
            engine.commit_transaction(txn_id).unwrap();
        });
    });

    group.bench_function("rollback_performance", |b| {
        let (mut engine, _temp) = setup_engine();
        let mut counter = 0u64;

        b.iter(|| {
            // Begin transaction
            let txn_id = engine
                .begin_transaction(driftdb_core::transaction::IsolationLevel::ReadCommitted)
                .unwrap();

            // Perform operations
            for _ in 0..10 {
                let event = generate_event(counter);
                engine.apply_event_in_transaction(txn_id, event).unwrap();
                counter += 1;
            }

            // Rollback
            engine.rollback_transaction(txn_id).unwrap();
        });
    });

    group.finish();
}

fn benchmark_snapshot_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshots");

    group.bench_function("snapshot_creation", |b| {
        b.iter_batched(
            || {
                let (mut engine, temp) = setup_engine();
                // Insert test data
                for i in 0..1000 {
                    engine.apply_event(generate_event(i)).unwrap();
                }
                (engine, temp)
            },
            |(mut engine, _temp)| {
                engine.create_snapshot("bench_table").unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("query_with_snapshot", |b| {
        let (mut engine, _temp) = setup_engine();

        // Insert test data
        for i in 0..10000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        // Create snapshot
        engine.create_snapshot("bench_table").unwrap();

        b.iter(|| {
            let query = Query::Select {
                table: "bench_table".to_string(),
                conditions: vec![WhereCondition {
                    column: "status".to_string(),
                    value: json!("active"),
                }],
                as_of: None,
                limit: Some(100),
            };

            black_box(engine.query(&query).unwrap());
        });
    });

    group.finish();
}

fn benchmark_index_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexes");

    group.bench_function("index_update", |b| {
        let (mut engine, _temp) = setup_engine();

        // Pre-populate
        for i in 0..1000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        let mut counter = 1000u64;

        b.iter(|| {
            let event = generate_event(counter);
            engine.apply_event(event).unwrap();
            counter += 1;
        });
    });

    group.bench_function("index_scan", |b| {
        let (mut engine, _temp) = setup_engine();

        // Insert test data with varied status values
        for i in 0..10000 {
            let mut event = generate_event(i);
            event.payload["status"] = json!(format!("status_{}", i % 100));
            engine.apply_event(event).unwrap();
        }

        b.iter(|| {
            let status = format!("status_{}", rand::random::<u64>() % 100);
            let query = Query::Select {
                table: "bench_table".to_string(),
                conditions: vec![WhereCondition {
                    column: "status".to_string(),
                    value: json!(status),
                }],
                as_of: None,
                limit: Some(100),
            };

            black_box(engine.query(&query).unwrap());
        });
    });

    group.finish();
}

fn benchmark_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    group.bench_function("zstd_compress", |b| {
        let data = vec![0u8; 10000];

        b.iter(|| {
            let compressed = zstd::encode_all(&data[..], 3).unwrap();
            black_box(compressed);
        });
    });

    group.bench_function("zstd_decompress", |b| {
        let data = vec![0u8; 10000];
        let compressed = zstd::encode_all(&data[..], 3).unwrap();

        b.iter(|| {
            let decompressed = zstd::decode_all(&compressed[..]).unwrap();
            black_box(decompressed);
        });
    });

    group.finish();
}

fn benchmark_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency");

    group.bench_function("concurrent_reads", |b| {
        let (mut engine, _temp) = setup_engine();

        // Insert test data
        for i in 0..10000 {
            engine.apply_event(generate_event(i)).unwrap();
        }

        let engine = std::sync::Arc::new(std::sync::Mutex::new(engine));

        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let engine_clone = engine.clone();
                    std::thread::spawn(move || {
                        let query = Query::Select {
                            table: "bench_table".to_string(),
                            conditions: vec![WhereCondition {
                                column: "status".to_string(),
                                value: json!("active"),
                            }],
                            as_of: None,
                            limit: Some(10),
                        };

                        let engine = engine_clone.lock().unwrap();
                        black_box(engine.query(&query).unwrap());
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

criterion_group!(
    benches,
    benchmark_single_writes,
    benchmark_batch_writes,
    benchmark_point_queries,
    benchmark_range_scans,
    benchmark_time_travel_queries,
    benchmark_transactions,
    benchmark_snapshot_operations,
    benchmark_index_performance,
    benchmark_compression,
    benchmark_concurrent_operations
);

criterion_main!(benches);
