use serde_json::json;
use tempfile::TempDir;

use driftdb_core::{Engine, Query, QueryResult};

#[test]
fn test_query_execution_returns_data() {
    // Setup
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create a table
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["email".to_string()],
        })
        .unwrap();

    // Insert test data
    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({
                "id": "user1",
                "name": "Alice",
                "email": "alice@example.com",
                "age": 30
            }),
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({
                "id": "user2",
                "name": "Bob",
                "email": "bob@example.com",
                "age": 25
            }),
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({
                "id": "user3",
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 35
            }),
        })
        .unwrap();

    // Test 1: Select all records
    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 3, "Should return all 3 records");

            // Verify data contains expected fields
            for row in &data {
                assert!(row.get("id").is_some(), "Each row should have an id");
                assert!(row.get("name").is_some(), "Each row should have a name");
                assert!(row.get("email").is_some(), "Each row should have an email");
            }
        }
        _ => panic!("Expected Rows result"),
    }

    // Test 2: Select with WHERE condition
    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: vec![driftdb_core::query::WhereCondition {
                column: "name".to_string(),
                operator: "=".to_string(),
                value: json!("Bob"),
            }],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1, "Should return only Bob's record");
            assert_eq!(data[0]["name"], json!("Bob"));
            assert_eq!(data[0]["age"], json!(25));
        }
        _ => panic!("Expected Rows result"),
    }

    // Test 3: Update a record
    engine
        .execute_query(Query::Patch {
            table: "users".to_string(),
            primary_key: json!("user1"),
            updates: json!({
                "age": 31,
                "city": "New York"
            }),
        })
        .unwrap();

    // Test 4: Verify update
    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: vec![driftdb_core::query::WhereCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: json!("user1"),
            }],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["age"], json!(31), "Age should be updated");
            assert_eq!(data[0]["city"], json!("New York"), "City should be added");
            assert_eq!(data[0]["name"], json!("Alice"), "Name should be unchanged");
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_time_travel_queries() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table and insert initial data
    engine
        .execute_query(Query::CreateTable {
            name: "products".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "products".to_string(),
            data: json!({
                "id": "prod1",
                "name": "Widget",
                "price": 10.0
            }),
        })
        .unwrap();

    // Capture sequence number (create_table doesn't generate a storage event,
    // so the insert is seq 1)
    let snapshot1_seq = 1; // After first insert

    // Update the product
    engine
        .execute_query(Query::Patch {
            table: "products".to_string(),
            primary_key: json!("prod1"),
            updates: json!({
                "price": 15.0
            }),
        })
        .unwrap();

    // Query current state
    let current_result = engine
        .execute_query(Query::Select {
            table: "products".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match current_result {
        QueryResult::Rows { data } => {
            assert_eq!(
                data[0]["price"],
                json!(15.0),
                "Current price should be 15.0"
            );
        }
        _ => panic!("Expected Rows result"),
    }

    // Query historical state
    let historical_result = engine
        .execute_query(Query::Select {
            table: "products".to_string(),
            conditions: vec![],
            as_of: Some(driftdb_core::query::AsOf::Sequence(snapshot1_seq)),
            limit: None,
        })
        .unwrap();

    match historical_result {
        QueryResult::Rows { data } => {
            assert_eq!(
                data[0]["price"],
                json!(10.0),
                "Historical price should be 10.0"
            );
        }
        _ => panic!("Expected Rows result"),
    }
}

/// Regression test for the Engine::query path silently dropping
/// `AsOf::Timestamp(_)`. Before the fix, this returned current state
/// (price 15.0) for a historical timestamp; now it correctly resolves
/// the timestamp to the largest sequence at or before that instant
/// and returns the historical state (price 10.0).
#[test]
fn test_query_path_resolves_as_of_timestamp() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "products".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "products".to_string(),
            data: json!({ "id": "prod1", "name": "Widget", "price": 10.0 }),
        })
        .unwrap();

    // Capture a timestamp between the insert and the patch. Sleep 10ms on
    // each side so OffsetDateTime::now_utc() lands strictly between the
    // two events' write timestamps regardless of clock granularity.
    std::thread::sleep(std::time::Duration::from_millis(10));
    let mid_ts = time::OffsetDateTime::now_utc();
    std::thread::sleep(std::time::Duration::from_millis(10));

    engine
        .execute_query(Query::Patch {
            table: "products".to_string(),
            primary_key: json!("prod1"),
            updates: json!({ "price": 15.0 }),
        })
        .unwrap();

    // Hit Engine::query (the read-only path used by FK validation),
    // not execute_query, since that's the path with the regression.
    let result = engine
        .query(&Query::Select {
            table: "products".to_string(),
            conditions: vec![],
            as_of: Some(driftdb_core::query::AsOf::Timestamp(mid_ts)),
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(
                data[0]["price"],
                json!(10.0),
                "AsOf::Timestamp on Engine::query must return historical state, \
                 not silently fall through to current state",
            );
        }
        other => panic!("Expected Rows result, got {:?}", other),
    }
}

#[test]
fn test_soft_delete() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Setup
    engine
        .execute_query(Query::CreateTable {
            name: "items".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    engine
        .execute_query(Query::Insert {
            table: "items".to_string(),
            data: json!({
                "id": "item1",
                "name": "Test Item"
            }),
        })
        .unwrap();

    // Soft delete
    engine
        .execute_query(Query::SoftDelete {
            table: "items".to_string(),
            primary_key: json!("item1"),
        })
        .unwrap();

    // Current query should not return deleted item
    let result = engine
        .execute_query(Query::Select {
            table: "items".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })
        .unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(
                data.len(),
                0,
                "Soft deleted items should not appear in current queries"
            );
        }
        _ => panic!("Expected Rows result"),
    }

    // Historical query should still see the item
    // (create_table doesn't generate a storage event, so insert is seq 1, delete is seq 2)
    let historical_result = engine
        .execute_query(Query::Select {
            table: "items".to_string(),
            conditions: vec![],
            as_of: Some(driftdb_core::query::AsOf::Sequence(1)), // After insert, before delete
            limit: None,
        })
        .unwrap();

    match historical_result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1, "Item should be visible in historical query");
            assert_eq!(data[0]["name"], json!("Test Item"));
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_query_non_existent_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    let result = engine.execute_query(Query::Select {
        table: "non_existent".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    });

    assert!(result.is_err(), "Query on non-existent table should fail");
}
