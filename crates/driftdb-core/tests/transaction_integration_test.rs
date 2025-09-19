use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use serde_json::json;
use time::OffsetDateTime;

use driftdb_core::{Engine, Query, QueryResult, Event, EventType};
use driftdb_core::transaction::IsolationLevel;

#[test]
fn test_transaction_commit_persists_data() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create a table
    engine.execute_query(Query::CreateTable {
        name: "accounts".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Begin transaction
    let txn_id = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();

    // Insert data within transaction
    let event = Event {
        sequence: 0,
        timestamp: OffsetDateTime::now_utc(),
        table_name: "accounts".to_string(),
        primary_key: json!("acc1"),
        event_type: EventType::Insert,
        payload: json!({
            "id": "acc1",
            "name": "Alice",
            "balance": 1000
        }),
    };
    engine.apply_event_in_transaction(txn_id, event).unwrap();

    // Data should not be visible before commit
    let result = engine.execute_query(Query::Select {
        table: "accounts".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 0, "Uncommitted data should not be visible");
        }
        _ => panic!("Expected Rows result"),
    }

    // Commit transaction
    engine.commit_transaction(txn_id).unwrap();

    // Data should now be visible
    let result = engine.execute_query(Query::Select {
        table: "accounts".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1, "Committed data should be visible");
            assert_eq!(data[0]["name"], json!("Alice"));
            assert_eq!(data[0]["balance"], json!(1000));
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_transaction_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create a table and insert initial data
    engine.execute_query(Query::CreateTable {
        name: "inventory".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "inventory".to_string(),
        data: json!({
            "id": "item1",
            "name": "Widget",
            "quantity": 100
        }),
    }).unwrap();

    // Begin transaction
    let txn_id = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();

    // Update within transaction
    let update_event = Event {
        sequence: 0,
        timestamp: OffsetDateTime::now_utc(),
        table_name: "inventory".to_string(),
        primary_key: json!("item1"),
        event_type: EventType::Patch,
        payload: json!({
            "quantity": 50
        }),
    };
    engine.apply_event_in_transaction(txn_id, update_event).unwrap();

    // Rollback transaction
    engine.rollback_transaction(txn_id).unwrap();

    // Original data should be unchanged
    let result = engine.execute_query(Query::Select {
        table: "inventory".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["quantity"], json!(100), "Rollback should revert changes");
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_read_your_writes() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table
    engine.execute_query(Query::CreateTable {
        name: "posts".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Begin transaction
    let txn_id = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();

    // Insert within transaction
    let event = Event {
        sequence: 0,
        timestamp: OffsetDateTime::now_utc(),
        table_name: "posts".to_string(),
        primary_key: json!("post1"),
        event_type: EventType::Insert,
        payload: json!({
            "id": "post1",
            "title": "My First Post",
            "content": "Hello World"
        }),
    };
    engine.apply_event_in_transaction(txn_id, event).unwrap();

    // Transaction should be able to read its own writes
    let value = engine.read_in_transaction(txn_id, "posts", "post1").unwrap();
    assert!(value.is_some(), "Transaction should read its own writes");

    let data = value.unwrap();
    assert_eq!(data["title"], json!("My First Post"));

    // Other transactions should not see uncommitted data
    let value = engine.read_in_transaction(999, "posts", "post1");
    assert!(value.is_err(), "Non-existent transaction should error");

    engine.commit_transaction(txn_id).unwrap();
}

#[test]
fn test_concurrent_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table with initial balance
    engine.execute_query(Query::CreateTable {
        name: "balances".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "balances".to_string(),
        data: json!({
            "id": "user1",
            "amount": 1000
        }),
    }).unwrap();

    // Start two concurrent transactions
    let txn1 = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
    let txn2 = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();

    // Both transactions modify different records
    let event1 = Event {
        sequence: 0,
        timestamp: OffsetDateTime::now_utc(),
        table_name: "balances".to_string(),
        primary_key: json!("user2"),
        event_type: EventType::Insert,
        payload: json!({
            "id": "user2",
            "amount": 500
        }),
    };
    engine.apply_event_in_transaction(txn1, event1).unwrap();

    let event2 = Event {
        sequence: 0,
        timestamp: OffsetDateTime::now_utc(),
        table_name: "balances".to_string(),
        primary_key: json!("user3"),
        event_type: EventType::Insert,
        payload: json!({
            "id": "user3",
            "amount": 750
        }),
    };
    engine.apply_event_in_transaction(txn2, event2).unwrap();

    // Commit both transactions
    engine.commit_transaction(txn1).unwrap();
    engine.commit_transaction(txn2).unwrap();

    // Verify both changes are persisted
    let result = engine.execute_query(Query::Select {
        table: "balances".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 3, "All three records should exist");

            // Check that all records exist
            let amounts: Vec<i64> = data.iter()
                .map(|row| row["amount"].as_i64().unwrap())
                .collect();
            assert!(amounts.contains(&1000));
            assert!(amounts.contains(&500));
            assert!(amounts.contains(&750));
        }
        _ => panic!("Expected Rows result"),
    }
}

#[test]
fn test_transaction_isolation_levels() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Setup
    engine.execute_query(Query::CreateTable {
        name: "test_isolation".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "test_isolation".to_string(),
        data: json!({
            "id": "record1",
            "value": 100
        }),
    }).unwrap();

    // Test ReadCommitted isolation
    let txn_rc = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();

    // Test RepeatableRead isolation
    let txn_rr = engine.begin_transaction(IsolationLevel::RepeatableRead).unwrap();

    // Test Serializable isolation
    let txn_ser = engine.begin_transaction(IsolationLevel::Serializable).unwrap();

    // All should be able to begin successfully
    engine.rollback_transaction(txn_rc).unwrap();
    engine.rollback_transaction(txn_rr).unwrap();
    engine.rollback_transaction(txn_ser).unwrap();
}

#[test]
fn test_transaction_persistence_across_restart() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // First session - create and commit transaction
    {
        let mut engine = Engine::init(&db_path).unwrap();

        engine.execute_query(Query::CreateTable {
            name: "persistent".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        }).unwrap();

        let txn = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();

        let event = Event {
            sequence: 0,
            timestamp: OffsetDateTime::now_utc(),
            table_name: "persistent".to_string(),
            primary_key: json!("data1"),
            event_type: EventType::Insert,
            payload: json!({
                "id": "data1",
                "info": "This should persist"
            }),
        };
        engine.apply_event_in_transaction(txn, event).unwrap();
        engine.commit_transaction(txn).unwrap();
    }

    // Second session - verify data persisted
    {
        let mut engine = Engine::open(&db_path).unwrap();

        let result = engine.execute_query(Query::Select {
            table: "persistent".to_string(),
            conditions: vec![],
            as_of: None,
            limit: None,
        }).unwrap();

        match result {
            QueryResult::Rows { data } => {
                assert_eq!(data.len(), 1, "Data should persist across restart");
                assert_eq!(data[0]["info"], json!("This should persist"));
            }
            _ => panic!("Expected Rows result"),
        }
    }
}