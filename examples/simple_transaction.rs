use tempfile::TempDir;
use serde_json::json;
use time::OffsetDateTime;

use driftdb_core::{Engine, Query, QueryResult, Event, EventType};
use driftdb_core::transaction::IsolationLevel;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create a table
    engine.execute_query(Query::CreateTable {
        name: "test".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    println!("✓ Table created");

    // Insert directly (no transaction) - this should work
    engine.execute_query(Query::Insert {
        table: "test".to_string(),
        data: json!({
            "id": "direct1",
            "value": "Direct insert"
        }),
    }).unwrap();

    println!("✓ Direct insert completed");

    // Query to verify direct insert
    let result = engine.execute_query(Query::Select {
        table: "test".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match &result {
        QueryResult::Rows { data } => {
            println!("After direct insert: {} rows", data.len());
            for row in data {
                println!("  Row: {:?}", row);
            }
        }
        _ => println!("Unexpected result type"),
    }

    // Now test transaction
    let txn_id = engine.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
    println!("✓ Transaction {} started", txn_id);

    let event = Event {
        sequence: 0,
        timestamp: OffsetDateTime::now_utc(),
        table_name: "test".to_string(),
        primary_key: json!("txn1"),
        event_type: EventType::Insert,
        payload: json!({
            "id": "txn1",
            "value": "Transaction insert"
        }),
    };

    engine.apply_event_in_transaction(txn_id, event).unwrap();
    println!("✓ Event applied to transaction");

    // Before commit
    let result = engine.execute_query(Query::Select {
        table: "test".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match &result {
        QueryResult::Rows { data } => {
            println!("Before commit: {} rows", data.len());
        }
        _ => {}
    }

    // Commit
    engine.commit_transaction(txn_id).unwrap();
    println!("✓ Transaction committed");

    // After commit
    let result = engine.execute_query(Query::Select {
        table: "test".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match &result {
        QueryResult::Rows { data } => {
            println!("After commit: {} rows", data.len());
            for row in data {
                println!("  Row: {:?}", row);
            }
        }
        _ => {}
    }
}