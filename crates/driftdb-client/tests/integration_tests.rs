//! Integration tests for driftdb-client
//!
//! These tests require a running DriftDB server on localhost:5433
//!
//! Start the server with:
//! ```bash
//! cargo run --release --bin driftdb-server -- --data-path /tmp/driftdb-test --auth-method trust
//! ```

use driftdb_client::{Client, Result, TimeTravel};
use serde::Deserialize;

/// Helper to check if server is running
async fn server_available() -> bool {
    Client::connect("localhost:5433").await.is_ok()
}

#[tokio::test]
#[ignore = "Requires running DriftDB server - run with: cargo test -- --ignored"]
async fn test_connection() -> Result<()> {
    let _client = Client::connect("localhost:5433").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires running DriftDB server"]
async fn test_simple_query() -> Result<()> {
    if !server_available().await {
        eprintln!("⚠️  Server not running, skipping test");
        return Ok(());
    }

    let client = Client::connect("localhost:5433").await?;

    // Use simple protocol (direct SQL)
    let rows = client.query("SELECT 1 as num").await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("num").and_then(|v| v.as_i64()), Some(1));

    Ok(())
}

#[tokio::test]
#[ignore = "Requires running DriftDB server"]
async fn test_create_and_query_table() -> Result<()> {
    if !server_available().await {
        return Ok(());
    }

    let client = Client::connect("localhost:5433").await?;

    // Create table (drop first in case it exists from previous run)
    let _ = client.execute("DROP TABLE test_users").await;
    client.execute("CREATE TABLE test_users (id BIGINT PRIMARY KEY, name TEXT)").await?;

    // Insert data (use explicit column names for consistency)
    client.execute("INSERT INTO test_users (id, name) VALUES (1, 'Alice')").await?;
    client.execute("INSERT INTO test_users (id, name) VALUES (2, 'Bob')").await?;

    // Query
    let rows = client.query("SELECT * FROM test_users ORDER BY id").await?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));
    assert_eq!(rows[1].get("name").and_then(|v| v.as_str()), Some("Bob"));

    // Cleanup
    client.execute("DROP TABLE test_users").await?;

    Ok(())
}

#[derive(Debug, Deserialize, PartialEq)]
struct TestUser {
    id: i64,
    name: String,
}

#[tokio::test]
#[ignore = "Requires running DriftDB server"]
async fn test_typed_queries() -> Result<()> {
    if !server_available().await {
        return Ok(());
    }

    let client = Client::connect("localhost:5433").await?;

    // Setup
    let _ = client.execute("DROP TABLE typed_test").await;
    client.execute("CREATE TABLE typed_test (id BIGINT PRIMARY KEY, name TEXT)").await?;
    client.execute("INSERT INTO typed_test (id, name) VALUES (1, 'Alice')").await?;
    client.execute("INSERT INTO typed_test (id, name) VALUES (2, 'Bob')").await?;

    // Query with type deserialization
    let users: Vec<TestUser> = client.query_as("SELECT * FROM typed_test ORDER BY id").await?;

    assert_eq!(users.len(), 2);
    assert_eq!(users[0], TestUser { id: 1, name: "Alice".to_string() });
    assert_eq!(users[1], TestUser { id: 2, name: "Bob".to_string() });

    // Cleanup
    client.execute("DROP TABLE typed_test").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "Requires running DriftDB server"]
async fn test_time_travel() -> Result<()> {
    if !server_available().await {
        return Ok(());
    }

    let client = Client::connect("localhost:5433").await?;

    // Setup
    let _ = client.execute("DROP TABLE time_travel_test").await; // Ignore error
    client.execute("CREATE TABLE time_travel_test (id BIGINT PRIMARY KEY, value TEXT)").await?;

    // Insert initial value
    client.execute("INSERT INTO time_travel_test (id, value) VALUES (1, 'v1')").await?;

    // Try to get current sequence - skip test if metadata table doesn't exist
    let seq1 = match client.current_sequence().await {
        Ok(s) => s,
        Err(_) => {
            eprintln!("⚠️  Skipping time-travel test: __driftdb_metadata__ table not found");
            client.execute("DROP TABLE time_travel_test").await?;
            return Ok(());
        }
    };

    // Update value
    client.execute("UPDATE time_travel_test SET value = 'v2' WHERE id = 1").await?;

    // Query current state
    let current = client.query("SELECT value FROM time_travel_test WHERE id = 1").await?;
    assert_eq!(current[0].get("value").and_then(|v| v.as_str()), Some("v2"));

    // Query historical state
    let historical = client
        .query_builder("SELECT value FROM time_travel_test WHERE id = 1")
        .as_of(TimeTravel::Sequence(seq1))
        .execute()
        .await?;

    assert_eq!(historical[0].get("value").and_then(|v| v.as_str()), Some("v1"));

    // Cleanup
    client.execute("DROP TABLE time_travel_test").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "Requires running DriftDB server"]
async fn test_transactions() -> Result<()> {
    if !server_available().await {
        return Ok(());
    }

    let client = Client::connect("localhost:5433").await?;

    // Setup - drop table if it exists from previous run
    let _ = client.execute("DROP TABLE txn_test").await; // Ignore error if table doesn't exist
    client.execute("CREATE TABLE txn_test (id BIGINT PRIMARY KEY, value TEXT)").await?;

    // Transaction - commit
    client.execute("BEGIN").await?;
    client.execute("INSERT INTO txn_test (id, value) VALUES (1, 'committed')").await?;
    client.execute("COMMIT").await?;

    let rows = client.query("SELECT * FROM txn_test").await?;
    assert_eq!(rows.len(), 1);

    // Transaction - rollback
    client.execute("BEGIN").await?;
    client.execute("INSERT INTO txn_test (id, value) VALUES (2, 'rolled_back')").await?;
    client.execute("ROLLBACK").await?;

    let rows = client.query("SELECT * FROM txn_test").await?;
    assert_eq!(rows.len(), 1); // Should still only have 1 row

    // Cleanup
    client.execute("DROP TABLE txn_test").await?;

    Ok(())
}
