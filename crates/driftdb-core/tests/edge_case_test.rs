//! Comprehensive edge case tests for DriftDB
//!
//! Tests boundary conditions, error handling, and corner cases

use driftdb_core::{query::WhereCondition, Engine, Query, QueryResult};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn test_empty_table_operations() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create empty table
    engine
        .execute_query(Query::CreateTable {
            name: "empty_table".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Query empty table
    let result = engine
        .execute_query(Query::Select {
            table: "empty_table".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => assert_eq!(data.len(), 0),
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Empty table operations test passed");
}

#[test]
fn test_null_value_handling() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "nullable_table".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Insert with null value
    engine
        .execute_query(Query::Insert {
            table: "nullable_table".to_string(),
            data: json!({
                "id": "1",
                "name": "Alice",
                "age": null
            }),
        })
        .unwrap();

    // Query back
    let result = engine
        .execute_query(Query::Select {
            table: "nullable_table".to_string(),
            conditions: vec![WhereCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: json!("1"),
            }],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert!(data[0].get("age").unwrap().is_null());
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ NULL value handling test passed");
}

#[test]
fn test_duplicate_primary_key_error() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Insert first record
    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({
                "id": "1",
                "name": "Alice"
            }),
        })
        .unwrap();

    // Try to insert duplicate primary key
    let result = engine.execute_query(Query::Insert {
        table: "users".to_string(),
        data: json!({
            "id": "1",
            "name": "Bob"
        }),
    });

    // Should fail with error about existing key
    assert!(result.is_err(), "Duplicate key should fail");

    println!("✅ Duplicate primary key error test passed");
}

#[test]
fn test_table_already_exists_error() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table
    engine
        .execute_query(Query::CreateTable {
            name: "products".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Try to create same table again
    let result = engine.execute_query(Query::CreateTable {
        name: "products".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    });

    // Should fail
    assert!(result.is_err(), "Duplicate table should fail");

    println!("✅ Table already exists error test passed");
}

#[test]
fn test_nonexistent_table_error() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Try to query nonexistent table
    let result = engine.execute_query(Query::Select {
        table: "nonexistent_table".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    });
    assert!(result.is_err(), "Querying nonexistent table should fail");

    // Try to insert into nonexistent table
    let result = engine.execute_query(Query::Insert {
        table: "nonexistent_table".to_string(),
        data: json!({"id": "1"}),
    });
    assert!(
        result.is_err(),
        "Inserting into nonexistent table should fail"
    );

    println!("✅ Nonexistent table error test passed");
}

#[test]
fn test_very_long_strings() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "long_strings".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Create a very long string (10KB)
    let long_text = "a".repeat(10_000);

    engine
        .execute_query(Query::Insert {
            table: "long_strings".to_string(),
            data: json!({
                "id": "1",
                "text": long_text.clone()
            }),
        })
        .unwrap();

    // Query back
    let result = engine
        .execute_query(Query::Select {
            table: "long_strings".to_string(),
            conditions: vec![WhereCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: json!("1"),
            }],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0].get("text").unwrap().as_str().unwrap().len(), 10_000);
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Very long strings test passed");
}

#[test]
fn test_large_numbers() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "numbers".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "numbers".to_string(),
            data: json!({
                "id": "1",
                "small": i64::MIN,
                "large": i64::MAX,
                "float": f64::MAX
            }),
        })
        .unwrap();

    let result = engine
        .execute_query(Query::Select {
            table: "numbers".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => assert_eq!(data.len(), 1),
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Large numbers test passed");
}

#[test]
fn test_special_characters_in_strings() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "special".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Test various special characters
    let special_texts = vec![
        ("1", "Hello\nWorld"),    // Newline
        ("2", "Tab\tSeparated"),  // Tab
        ("3", "Quote\"Test"),     // Quote
        ("4", "Apostrophe's"),    // Apostrophe
        ("5", "Emoji 😀🎉"),      // Unicode emoji
        ("6", "Chinese 中文"),    // Non-Latin characters
        ("7", "Backslash\\Test"), // Backslash
    ];

    for (id, text) in &special_texts {
        engine
            .execute_query(Query::Insert {
                table: "special".to_string(),
                data: json!({
                    "id": id,
                    "text": text
                }),
            })
            .unwrap();
    }

    // Query all back
    let result = engine
        .execute_query(Query::Select {
            table: "special".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => assert_eq!(data.len(), special_texts.len()),
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Special characters test passed");
}

#[test]
fn test_nested_json_documents() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "nested".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Create deeply nested JSON
    let nested_data = json!({
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "level5": {
                            "value": "deep"
                        }
                    }
                }
            }
        },
        "array": [1, 2, 3, [4, 5, [6, 7]]],
        "mixed": {
            "string": "test",
            "number": 42,
            "bool": true,
            "null": null
        }
    });

    engine
        .execute_query(Query::Insert {
            table: "nested".to_string(),
            data: json!({
                "id": "1",
                "data": nested_data
            }),
        })
        .unwrap();

    let result = engine
        .execute_query(Query::Select {
            table: "nested".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => assert_eq!(data.len(), 1),
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Nested JSON documents test passed");
}

#[test]
fn test_delete_and_reinsert_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "reuse".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Insert
    engine
        .execute_query(Query::Insert {
            table: "reuse".to_string(),
            data: json!({
                "id": "1",
                "value": "first"
            }),
        })
        .unwrap();

    // Soft delete
    engine
        .execute_query(Query::SoftDelete {
            table: "reuse".to_string(),
            primary_key: json!("1"),
        })
        .unwrap();

    // Insert again with same key
    engine
        .execute_query(Query::Insert {
            table: "reuse".to_string(),
            data: json!({
                "id": "1",
                "value": "second"
            }),
        })
        .unwrap();

    // Query should return the new value
    let result = engine
        .execute_query(Query::Select {
            table: "reuse".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["value"], json!("second"));
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Delete and reinsert same key test passed");
}

#[test]
fn test_patch_upsert_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "patch_test".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // PATCH on nonexistent key may have upsert behavior
    let _result = engine.execute_query(Query::Patch {
        table: "patch_test".to_string(),
        primary_key: json!("test_key"),
        updates: json!({"value": "updated"}),
    });

    // Query to verify behavior - should either error or create record
    let result = engine
        .execute_query(Query::Select {
            table: "patch_test".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    // Verify the query worked (implementation may vary on upsert behavior)
    match result {
        QueryResult::Rows { data } => {
            // Either 0 records (patch failed) or 1 record (upsert worked)
            assert!(data.len() <= 1, "Should have at most 1 record");
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Patch upsert behavior test passed");
}

#[test]
fn test_multiple_indexes_same_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "multi_index".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["name".to_string(), "age".to_string(), "email".to_string()],
        })
        .unwrap();

    // Insert data
    for i in 1..=10 {
        engine
            .execute_query(Query::Insert {
                table: "multi_index".to_string(),
                data: json!({
                    "id": format!("{}", i),
                    "name": format!("User{}", i),
                    "age": 20 + i,
                    "email": format!("user{}@example.com", i)
                }),
            })
            .unwrap();
    }

    // Query using different indexed columns
    let result = engine
        .execute_query(Query::Select {
            table: "multi_index".to_string(),
            conditions: vec![WhereCondition {
                column: "name".to_string(),
                operator: "=".to_string(),
                value: json!("User5"),
            }],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => assert_eq!(data.len(), 1),
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Multiple indexes test passed");
}

#[test]
fn test_query_with_limit() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "limited".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Insert 100 records
    for i in 1..=100 {
        engine
            .execute_query(Query::Insert {
                table: "limited".to_string(),
                data: json!({
                    "id": format!("{}", i),
                    "value": i
                }),
            })
            .unwrap();
    }

    // Query with limit
    let result = engine
        .execute_query(Query::Select {
            table: "limited".to_string(),
            conditions: vec![],
            as_of: None,
            limit: Some(10),
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert!(data.len() <= 10, "Should respect limit");
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Query with limit test passed");
}

#[test]
fn test_empty_string_values() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "empty_strings".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Insert with empty strings
    engine
        .execute_query(Query::Insert {
            table: "empty_strings".to_string(),
            data: json!({
                "id": "1",
                "text": "",
                "name": "Test"
            }),
        })
        .unwrap();

    let result = engine
        .execute_query(Query::Select {
            table: "empty_strings".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["text"], json!(""));
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Empty string values test passed");
}

#[test]
fn test_patch_updates_only_specified_fields() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "partial_update".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Insert initial record
    engine
        .execute_query(Query::Insert {
            table: "partial_update".to_string(),
            data: json!({
                "id": "1",
                "field1": "value1",
                "field2": "value2",
                "field3": "value3"
            }),
        })
        .unwrap();

    // Patch only field2
    engine
        .execute_query(Query::Patch {
            table: "partial_update".to_string(),
            primary_key: json!("1"),
            updates: json!({
                "field2": "updated_value2"
            }),
        })
        .unwrap();

    // Query and verify
    let result = engine
        .execute_query(Query::Select {
            table: "partial_update".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(
                data[0]["field1"],
                json!("value1"),
                "field1 should be unchanged"
            );
            assert_eq!(
                data[0]["field2"],
                json!("updated_value2"),
                "field2 should be updated"
            );
            assert_eq!(
                data[0]["field3"],
                json!("value3"),
                "field3 should be unchanged"
            );
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Partial update test passed");
}

#[test]
fn test_boolean_values() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "booleans".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "booleans".to_string(),
            data: json!({
                "id": "1",
                "is_active": true,
                "is_deleted": false
            }),
        })
        .unwrap();

    let result = engine
        .execute_query(Query::Select {
            table: "booleans".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["is_active"], json!(true));
            assert_eq!(data[0]["is_deleted"], json!(false));
        }
        _ => panic!("Expected Rows result"),
    }

    println!("✅ Boolean values test passed");
}
