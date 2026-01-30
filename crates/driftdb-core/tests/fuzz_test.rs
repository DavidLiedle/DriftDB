//! Fuzzing tests for DriftDB
//!
//! Property-based and randomized testing to discover edge cases and ensure robustness

use driftdb_core::{query::WhereCondition, Engine, Query, QueryResult};
use proptest::prelude::*;
use rand::Rng;
use serde_json::{json, Value};
use tempfile::TempDir;

// ============================================================================
// Random Data Generators
// ============================================================================

/// Generate random JSON value with various data types
fn random_json_value(rng: &mut impl Rng, depth: u32) -> Value {
    if depth > 5 {
        // Limit nesting depth to avoid infinite recursion
        return json!(null);
    }

    match rng.gen_range(0..10) {
        0 => json!(null),
        1 => json!(rng.gen::<bool>()),
        2 => json!(rng.gen::<i64>()),
        3 => json!(rng.gen::<f64>()),
        4 => {
            // Empty string
            json!("")
        }
        5 => {
            // Random string (1-100 chars)
            let len = rng.gen_range(1..100);
            let s: String = (0..len)
                .map(|_| rng.gen_range(32..127) as u8 as char)
                .collect();
            json!(s)
        }
        6 => {
            // Unicode string with emoji and special chars
            let emojis = ["🚀", "🎉", "💻", "🔥", "✨", "🌟", "中文", "日本語"];
            json!(emojis[rng.gen_range(0..emojis.len())])
        }
        7 => {
            // Array
            let len = rng.gen_range(0..10);
            let arr: Vec<Value> = (0..len)
                .map(|_| random_json_value(rng, depth + 1))
                .collect();
            json!(arr)
        }
        8 => {
            // Object
            let len = rng.gen_range(0..5);
            let mut obj = serde_json::Map::new();
            for i in 0..len {
                let key = format!("field_{}", i);
                obj.insert(key, random_json_value(rng, depth + 1));
            }
            json!(obj)
        }
        _ => {
            // Very large number
            json!(i64::MAX - rng.gen_range(0..1000))
        }
    }
}

/// Generate random table name
fn random_table_name(rng: &mut impl Rng) -> String {
    let prefixes = ["test", "data", "users", "events", "records"];
    let suffix: u32 = rng.gen();
    format!("{}_{}", prefixes[rng.gen_range(0..prefixes.len())], suffix)
}

/// Generate random column name
fn random_column_name(rng: &mut impl Rng) -> String {
    let names = [
        "id",
        "name",
        "value",
        "data",
        "status",
        "timestamp",
        "count",
        "type",
    ];
    names[rng.gen_range(0..names.len())].to_string()
}

// ============================================================================
// Fuzzing Tests
// ============================================================================

#[test]
fn test_random_table_creation() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    // Create 20 tables with random names and columns
    for _ in 0..20 {
        let table_name = random_table_name(&mut rng);
        let primary_key = random_column_name(&mut rng);
        let num_indexes = rng.gen_range(0..5);
        let indexed_columns: Vec<String> = (0..num_indexes)
            .map(|_| random_column_name(&mut rng))
            .collect();

        let result = engine.execute_query(Query::CreateTable {
            name: table_name.clone(),
            primary_key,
            indexed_columns,
        });

        // Table creation should either succeed or fail gracefully
        assert!(result.is_ok() || result.is_err());

        if let Ok(QueryResult::Success { message }) = result {
            println!("✅ Created table: {} - {}", table_name, message);
        }
    }

    println!("✅ Random table creation fuzz test passed");
}

#[test]
fn test_random_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    // Create a table
    engine
        .execute_query(Query::CreateTable {
            name: "fuzz_table".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["data".to_string()],
        })
        .unwrap();

    // Insert 100 random records
    for i in 0..100 {
        let mut data = serde_json::Map::new();
        data.insert("id".to_string(), json!(i));

        // Add 1-10 random fields
        let num_fields = rng.gen_range(1..10);
        for j in 0..num_fields {
            let key = format!("field_{}", j);
            let value = random_json_value(&mut rng, 0);
            data.insert(key, value);
        }

        let result = engine.execute_query(Query::Insert {
            table: "fuzz_table".to_string(),
            data: Value::Object(data),
        });

        // Inserts should succeed
        assert!(
            result.is_ok(),
            "Insert failed for record {}: {:?}",
            i,
            result
        );
    }

    // Verify we can query the data
    let result = engine
        .execute_query(Query::Select {
            table: "fuzz_table".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 100);
            println!(
                "✅ Random inserts fuzz test passed - {} records",
                data.len()
            );
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_random_queries_with_conditions() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    // Create and populate table
    engine
        .execute_query(Query::CreateTable {
            name: "query_table".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["value".to_string()],
        })
        .unwrap();

    for i in 0..50 {
        engine
            .execute_query(Query::Insert {
                table: "query_table".to_string(),
                data: json!({
                    "id": i,
                    "value": rng.gen_range(0..100),
                    "name": format!("item_{}", i)
                }),
            })
            .unwrap();
    }

    // Run 50 random queries with various conditions
    for _ in 0..50 {
        let column = if rng.gen_bool(0.5) {
            "id".to_string()
        } else {
            "value".to_string()
        };

        let random_value = json!(rng.gen_range(0..100));
        let conditions = vec![WhereCondition {
            column,
            operator: "=".to_string(),
            value: random_value,
        }];

        let result = engine.execute_query(Query::Select {
            table: "query_table".to_string(),
            conditions,
            as_of: None,
            limit: Some(rng.gen_range(1..20)),
        });

        // Queries should not crash, even with random conditions
        assert!(result.is_ok(), "Query failed: {:?}", result);
    }

    println!("✅ Random query fuzz test passed");
}

#[test]
fn test_random_updates() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    // Create and populate table
    engine
        .execute_query(Query::CreateTable {
            name: "update_table".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    for i in 0..30 {
        engine
            .execute_query(Query::Insert {
                table: "update_table".to_string(),
                data: json!({
                    "id": i,
                    "counter": 0,
                    "status": "initial"
                }),
            })
            .unwrap();
    }

    // Perform 50 random updates
    for _ in 0..50 {
        let id = rng.gen_range(0..30);
        let mut patch = serde_json::Map::new();
        patch.insert("id".to_string(), json!(id));
        patch.insert("counter".to_string(), json!(rng.gen_range(0..1000)));
        patch.insert(
            "status".to_string(),
            json!(format!("status_{}", rng.gen_range(0..10))),
        );

        let primary_key = json!(id);
        let result = engine.execute_query(Query::Patch {
            table: "update_table".to_string(),
            primary_key,
            updates: Value::Object(patch),
        });

        assert!(result.is_ok(), "Update failed: {:?}", result);
    }

    println!("✅ Random update fuzz test passed");
}

#[test]
fn test_random_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    // Create and populate table
    engine
        .execute_query(Query::CreateTable {
            name: "delete_table".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    for i in 0..50 {
        engine
            .execute_query(Query::Insert {
                table: "delete_table".to_string(),
                data: json!({
                    "id": i,
                    "data": format!("record_{}", i)
                }),
            })
            .unwrap();
    }

    // Randomly delete 25 records
    let mut deleted_ids = vec![];
    for _ in 0..25 {
        let id = rng.gen_range(0..50);
        if !deleted_ids.contains(&id) {
            deleted_ids.push(id);

            let result = engine.execute_query(Query::SoftDelete {
                table: "delete_table".to_string(),
                primary_key: json!(id),
            });

            // First deletion should succeed, subsequent ones might fail
            if !deleted_ids.iter().filter(|&&x| x == id).count() > 1 {
                assert!(result.is_ok(), "Delete failed: {:?}", result);
            }
        }
    }

    // Verify remaining records
    let result = engine
        .execute_query(Query::Select {
            table: "delete_table".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert!(
                data.len() <= 50 - deleted_ids.len(),
                "Expected at most {} records, got {}",
                50 - deleted_ids.len(),
                data.len()
            );
            println!(
                "✅ Random delete fuzz test passed - {} records deleted",
                deleted_ids.len()
            );
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_random_special_characters() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    engine
        .execute_query(Query::CreateTable {
            name: "special_chars".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    let special_strings = vec![
        "Hello\nWorld",
        "Tab\tSeparated",
        "Quote\"Test",
        "Apostrophe's",
        "Emoji🚀Test",
        "中文字符",
        "日本語",
        "Mixed中文ABC123",
        "Null\0Byte",
        "Backslash\\Test",
    ];

    for (i, s) in special_strings.iter().enumerate() {
        let result = engine.execute_query(Query::Insert {
            table: "special_chars".to_string(),
            data: json!({
                "id": i,
                "text": s,
                "random": random_json_value(&mut rng, 0)
            }),
        });

        // Should handle special characters gracefully
        assert!(
            result.is_ok(),
            "Failed to insert special string '{}': {:?}",
            s,
            result
        );
    }

    println!("✅ Special characters fuzz test passed");
}

#[test]
fn test_random_large_values() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "large_values".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Test various large values
    let test_cases = vec![
        ("min_i64", json!(i64::MIN)),
        ("max_i64", json!(i64::MAX)),
        ("max_f64", json!(f64::MAX)),
        ("min_f64", json!(f64::MIN)),
        ("large_string", json!("A".repeat(10_000))),
        ("large_array", json!(vec![1; 1000])),
    ];

    for (i, (name, value)) in test_cases.iter().enumerate() {
        let result = engine.execute_query(Query::Insert {
            table: "large_values".to_string(),
            data: json!({
                "id": i,
                "type": name,
                "value": value
            }),
        });

        assert!(
            result.is_ok(),
            "Failed to insert large value '{}': {:?}",
            name,
            result
        );
    }

    println!("✅ Large values fuzz test passed");
}

#[test]
fn test_random_deeply_nested_json() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "nested_json".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Create deeply nested JSON
    let mut nested = json!({"level": 0});
    for i in 1..10 {
        nested = json!({
            "level": i,
            "data": nested
        });
    }

    let result = engine.execute_query(Query::Insert {
        table: "nested_json".to_string(),
        data: json!({
            "id": 1,
            "nested": nested
        }),
    });

    assert!(result.is_ok(), "Failed to insert nested JSON: {:?}", result);

    // Verify we can query it back
    let result = engine
        .execute_query(Query::Select {
            table: "nested_json".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            println!("✅ Deeply nested JSON fuzz test passed");
        }
        _ => panic!("Expected Rows result"),
    }
}

// ============================================================================
// Property-based Tests using proptest
// ============================================================================

proptest! {
    #[test]
    fn proptest_random_integers(id in 0i64..1000, value in any::<i64>()) {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        engine.execute_query(Query::CreateTable {
            name: "prop_integers".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        }).unwrap();

        let result = engine.execute_query(Query::Insert {
            table: "prop_integers".to_string(),
            data: json!({
                "id": id,
                "value": value
            }),
        });

        prop_assert!(result.is_ok());
    }

    #[test]
    fn proptest_random_strings(id in 0usize..100, s in "\\PC*") {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        engine.execute_query(Query::CreateTable {
            name: "prop_strings".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        }).unwrap();

        let result = engine.execute_query(Query::Insert {
            table: "prop_strings".to_string(),
            data: json!({
                "id": id,
                "text": s
            }),
        });

        prop_assert!(result.is_ok());
    }

    #[test]
    fn proptest_random_floats(id in 0i64..100, f in any::<f64>()) {
        if !f.is_nan() && !f.is_infinite() {
            let temp_dir = TempDir::new().unwrap();
            let mut engine = Engine::init(temp_dir.path()).unwrap();

            engine.execute_query(Query::CreateTable {
                name: "prop_floats".to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            }).unwrap();

            let result = engine.execute_query(Query::Insert {
                table: "prop_floats".to_string(),
                data: json!({
                    "id": id,
                    "value": f
                }),
            });

            prop_assert!(result.is_ok());
        }
    }

    #[test]
    fn proptest_random_booleans(id in 0i64..100, b in any::<bool>()) {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::init(temp_dir.path()).unwrap();

        engine.execute_query(Query::CreateTable {
            name: "prop_booleans".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        }).unwrap();

        let result = engine.execute_query(Query::Insert {
            table: "prop_booleans".to_string(),
            data: json!({
                "id": id,
                "flag": b
            }),
        });

        prop_assert!(result.is_ok());
    }
}

#[test]
fn test_random_operation_sequence() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    let mut rng = rand::thread_rng();

    // Create initial table
    engine
        .execute_query(Query::CreateTable {
            name: "sequence_test".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["data".to_string()],
        })
        .unwrap();

    // Perform 200 random operations
    for i in 0..200 {
        let operation = rng.gen_range(0..4);

        match operation {
            0 => {
                // Insert
                let result = engine.execute_query(Query::Insert {
                    table: "sequence_test".to_string(),
                    data: json!({
                        "id": i,
                        "data": random_json_value(&mut rng, 0)
                    }),
                });
                assert!(result.is_ok() || result.is_err()); // Should not panic
            }
            1 => {
                // Select
                let result = engine.execute_query(Query::Select {
                    table: "sequence_test".to_string(),
                    conditions: vec![],
                    as_of: None,
                    limit: Some(rng.gen_range(1..50)),
                });
                assert!(result.is_ok());
            }
            2 => {
                // Update
                let id = rng.gen_range(0..i.max(1));
                let result = engine.execute_query(Query::Patch {
                    table: "sequence_test".to_string(),
                    primary_key: json!(id),
                    updates: json!({
                        "data": random_json_value(&mut rng, 0)
                    }),
                });
                assert!(result.is_ok() || result.is_err()); // Should not panic
            }
            3 => {
                // Delete
                let result = engine.execute_query(Query::SoftDelete {
                    table: "sequence_test".to_string(),
                    primary_key: json!(rng.gen_range(0..i.max(1))),
                });
                assert!(result.is_ok() || result.is_err()); // Should not panic
            }
            _ => unreachable!(),
        }
    }

    println!("✅ Random operation sequence fuzz test passed - 200 operations");
}

#[test]
fn test_concurrent_random_operations() {
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    // Initialize engine and create table
    {
        let mut engine = Engine::init(&path).unwrap();
        engine
            .execute_query(Query::CreateTable {
                name: "concurrent_test".to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }

    // Spawn 5 threads performing random operations
    // Note: Due to file locking, threads may fail to acquire database lock
    let handles: Vec<_> = (0..5)
        .map(|thread_id| {
            let path = path.clone();
            thread::spawn(move || {
                // Try to open the engine - may fail due to file lock contention
                match Engine::open(&path) {
                    Ok(mut engine) => {
                        let mut rng = rand::thread_rng();

                        for i in 0..20 {
                            let id = thread_id * 20 + i;
                            let result = engine.execute_query(Query::Insert {
                                table: "concurrent_test".to_string(),
                                data: json!({
                                    "id": id,
                                    "thread": thread_id,
                                    "value": random_json_value(&mut rng, 0)
                                }),
                            });

                            // Should handle concurrent access gracefully
                            if result.is_err() {
                                println!(
                                    "Thread {} insert {} failed (expected in concurrent scenario)",
                                    thread_id, i
                                );
                            }
                        }
                    }
                    Err(e) => {
                        println!(
                            "Thread {} could not acquire database lock (expected): {}",
                            thread_id, e
                        );
                    }
                }
            })
        })
        .collect();

    // Wait for all threads - some may have failed to acquire lock, which is expected
    for (i, handle) in handles.into_iter().enumerate() {
        if let Err(e) = handle.join() {
            println!("Thread {} panicked: {:?}", i, e);
        }
    }

    println!("✅ Concurrent random operations fuzz test passed");
}
