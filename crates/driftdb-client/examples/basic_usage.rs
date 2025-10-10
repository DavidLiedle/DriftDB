//! Basic usage example for DriftDB client library
//!
//! Run with:
//! ```bash
//! cargo run --example basic_usage
//! ```

use driftdb_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 DriftDB Client - Basic Usage Example\n");

    // Connect to DriftDB
    println!("Connecting to DriftDB...");
    let client = Client::connect("localhost:5433").await?;
    println!("✓ Connected!\n");

    // Create a table (drop first if it exists from previous run)
    println!("Creating table...");
    let _ = client.execute("DROP TABLE users").await; // Ignore error if table doesn't exist
    client
        .execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, email TEXT, created_at TEXT)")
        .await?;
    println!("✓ Table created!\n");

    // Insert some data
    println!("Inserting users...");
    client
        .execute("INSERT INTO users (id, name, email, created_at) VALUES (1, 'Alice', 'alice@example.com', '2025-01-01')")
        .await?;
    client
        .execute("INSERT INTO users (id, name, email, created_at) VALUES (2, 'Bob', 'bob@example.com', '2025-01-02')")
        .await?;
    client
        .execute("INSERT INTO users (id, name, email, created_at) VALUES (3, 'Charlie', 'charlie@example.com', '2025-01-03')")
        .await?;
    println!("✓ Users inserted!\n");

    // Query all users
    println!("Querying all users...");
    let rows = client.query("SELECT * FROM users ORDER BY id").await?;

    println!("Found {} users:", rows.len());
    for row in &rows {
        let id = row.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let email = row.get("email").and_then(|v| v.as_str()).unwrap_or("?");

        println!("  - User #{}: {} <{}>", id, name, email);
    }
    println!();

    // Update a user
    println!("Updating Bob's email...");
    client
        .execute("UPDATE users SET email = 'bob.new@example.com' WHERE id = 2")
        .await?;
    println!("✓ Email updated!\n");

    // Query updated user
    println!("Querying Bob...");
    let rows = client.query("SELECT * FROM users WHERE id = 2").await?;

    if let Some(row) = rows.first() {
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let email = row.get("email").and_then(|v| v.as_str()).unwrap_or("?");
        println!("  {} now has email: {}", name, email);
    }
    println!();

    // Delete a user
    println!("Deleting Charlie...");
    client.execute("DELETE FROM users WHERE id = 3").await?;
    println!("✓ User deleted!\n");

    // Final count
    let rows = client.query("SELECT COUNT(*) FROM users").await?;
    if let Some(row) = rows.first() {
        let count = row.get_idx(0).and_then(|v| v.as_i64()).unwrap_or(0);
        println!("Final user count: {}", count);
    }

    println!("\n✨ Example completed successfully!");

    Ok(())
}
