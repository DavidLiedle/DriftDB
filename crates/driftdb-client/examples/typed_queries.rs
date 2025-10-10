//! Typed queries example using serde deserialization
//!
//! This example shows how to use strongly-typed structs with DriftDB queries
//! for better type safety and ergonomics.
//!
//! Run with:
//! ```bash
//! cargo run --example typed_queries
//! ```

use driftdb_client::{Client, Result, TimeTravel};
use serde::Deserialize;

// Define our domain models with serde
#[derive(Debug, Deserialize)]
struct User {
    id: i64,
    username: String,
    email: String,
    active: bool,
}

#[derive(Debug, Deserialize)]
struct Order {
    id: i64,
    user_id: i64,
    product: String,
    amount: i64,
    status: String,
}

#[derive(Debug, Deserialize)]
struct OrderSummary {
    user_id: i64,
    total_orders: i64,
    total_amount: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🎯 DriftDB Client - Typed Queries Example\n");

    let client = Client::connect("localhost:5433").await?;
    println!("✓ Connected!\n");

    // Setup tables
    println!("Setting up tables...");
    setup_tables(&client).await?;
    println!("✓ Tables ready!\n");

    // === Example 1: Query users with type safety ===
    println!("📋 Example 1: Typed user queries\n");

    let users: Vec<User> = client
        .query_as("SELECT * FROM users ORDER BY id")
        .await?;

    println!("Found {} users:", users.len());
    for user in &users {
        println!(
            "  - #{}: {} <{}> [{}]",
            user.id,
            user.username,
            user.email,
            if user.active { "active" } else { "inactive" }
        );
    }

    // === Example 2: Filtered queries ===
    println!("\n📋 Example 2: Filter active users\n");

    let active_users: Vec<User> = client
        .query_as("SELECT * FROM users WHERE active = true")
        .await?;

    println!("Active users: {}", active_users.len());
    for user in &active_users {
        println!("  - {}", user.username);
    }

    // === Example 3: Typed aggregations ===
    println!("\n📋 Example 3: Order summaries by user\n");

    let summaries: Vec<OrderSummary> = client
        .query_as(
            "SELECT user_id, COUNT(*) as total_orders, SUM(amount) as total_amount \
             FROM orders \
             GROUP BY user_id \
             ORDER BY total_amount DESC"
        )
        .await?;

    println!("Order summaries:");
    for summary in &summaries {
        println!(
            "  - User #{}: {} orders, ${:.2} total",
            summary.user_id,
            summary.total_orders,
            summary.total_amount as f64 / 100.0
        );
    }

    // === Example 4: Time-travel with typed queries ===
    println!("\n📋 Example 4: Time-travel + typed queries\n");

    // Get current sequence
    let current_seq = client.current_sequence().await?;
    println!("Current sequence: {}", current_seq);

    // Update a user
    client
        .execute("UPDATE users SET active = false WHERE id = 2")
        .await?;

    // Query current state
    let users_now: Vec<User> = client
        .query_as("SELECT * FROM users ORDER BY id")
        .await?;

    let active_now = users_now.iter().filter(|u| u.active).count();
    println!("Active users now: {}", active_now);

    // Query historical state (before the update)
    let users_then: Vec<User> = client
        .query_builder("SELECT * FROM users ORDER BY id")
        .as_of(TimeTravel::Sequence(current_seq))
        .execute_as()
        .await?;

    let active_then = users_then.iter().filter(|u| u.active).count();
    println!("Active users at sequence {}: {}", current_seq, active_then);

    println!("\n✨ Typed queries example completed!");
    println!("\n💡 Benefits of typed queries:");
    println!("   ✓ Compile-time type safety");
    println!("   ✓ IDE autocomplete support");
    println!("   ✓ Automatic deserialization");
    println!("   ✓ Less boilerplate code");

    Ok(())
}

async fn setup_tables(client: &Client) -> Result<()> {
    // Drop tables if they exist from previous runs
    let _ = client.execute("DROP TABLE orders").await;
    let _ = client.execute("DROP TABLE users").await;

    // Create users table
    client
        .execute(
            "CREATE TABLE users (\
             id BIGINT PRIMARY KEY, \
             username TEXT, \
             email TEXT, \
             active BOOLEAN\
             )"
        )
        .await?;

    // Create orders table
    client
        .execute(
            "CREATE TABLE orders (\
             id BIGINT PRIMARY KEY, \
             user_id BIGINT, \
             product TEXT, \
             amount BIGINT, \
             status TEXT\
             )"
        )
        .await?;

    // Insert sample users
    client
        .execute("INSERT INTO users (id, username, email, active) VALUES (1, 'alice', 'alice@example.com', true)")
        .await?;
    client
        .execute("INSERT INTO users (id, username, email, active) VALUES (2, 'bob', 'bob@example.com', true)")
        .await?;
    client
        .execute("INSERT INTO users (id, username, email, active) VALUES (3, 'charlie', 'charlie@example.com', false)")
        .await?;

    // Insert sample orders
    client
        .execute("INSERT INTO orders (id, user_id, product, amount, status) VALUES (1, 1, 'Widget', 2500, 'completed')")
        .await?;
    client
        .execute("INSERT INTO orders (id, user_id, product, amount, status) VALUES (2, 1, 'Gadget', 1500, 'completed')")
        .await?;
    client
        .execute("INSERT INTO orders (id, user_id, product, amount, status) VALUES (3, 2, 'Widget', 2500, 'pending')")
        .await?;
    client
        .execute("INSERT INTO orders (id, user_id, product, amount, status) VALUES (4, 2, 'Doohickey', 3500, 'completed')")
        .await?;

    Ok(())
}
