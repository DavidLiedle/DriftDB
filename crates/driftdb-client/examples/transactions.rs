//! Transaction example for DriftDB
//!
//! This example demonstrates ACID transactions with BEGIN/COMMIT/ROLLBACK.
//!
//! Run with:
//! ```bash
//! cargo run --example transactions
//! ```

use driftdb_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("💳 DriftDB Client - Transactions Example\n");

    let client = Client::connect("localhost:5433").await?;
    println!("✓ Connected!\n");

    // Setup
    println!("Setting up demo...");
    setup_tables(&client).await?;
    println!("✓ Setup complete!\n");

    // === Example 1: Successful transaction ===
    println!("📝 Example 1: Successful multi-step transaction\n");

    // Using manual transaction management since Transaction struct needs work
    client.execute("BEGIN").await?;
    println!("  → Transaction started");

    client
        .execute("INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 1000)")
        .await?;
    println!("  → Created Alice's account with $10.00");

    client
        .execute("INSERT INTO accounts (id, name, balance) VALUES (2, 'Bob', 500)")
        .await?;
    println!("  → Created Bob's account with $5.00");

    client.execute("COMMIT").await?;
    println!("  → Transaction committed!");

    // Verify
    let count = get_account_count(&client).await?;
    println!("  ✓ Total accounts: {}\n", count);

    // === Example 2: Money transfer transaction ===
    println!("📝 Example 2: Money transfer between accounts\n");

    println!("  Initial balances:");
    print_balances(&client).await?;

    client.execute("BEGIN").await?;
    println!("\n  → Starting transfer of $2.00 from Alice to Bob...");

    // Deduct from Alice
    client
        .execute("UPDATE accounts SET balance = balance - 200 WHERE id = 1")
        .await?;
    println!("  → Deducted $2.00 from Alice");

    // Add to Bob
    client
        .execute("UPDATE accounts SET balance = balance + 200 WHERE id = 2")
        .await?;
    println!("  → Added $2.00 to Bob");

    client.execute("COMMIT").await?;
    println!("  → Transfer committed!");

    println!("\n  Final balances:");
    print_balances(&client).await?;

    // === Example 3: Rollback on error ===
    println!("\n📝 Example 3: Transaction rollback\n");

    println!("  Current balances:");
    print_balances(&client).await?;

    client.execute("BEGIN").await?;
    println!("\n  → Starting a transaction...");

    client
        .execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
        .await?;
    println!("  → Updated Alice's balance");

    println!("  → Oops! Deciding to cancel the transaction...");

    client.execute("ROLLBACK").await?;
    println!("  → Transaction rolled back!");

    println!("\n  Balances after rollback (should be unchanged):");
    print_balances(&client).await?;

    // === Example 4: Multiple operations in one transaction ===
    println!("\n📝 Example 4: Batch operations\n");

    client.execute("BEGIN").await?;
    println!("  → Starting batch insert...");

    client
        .execute("INSERT INTO accounts (id, name, balance) VALUES (3, 'Charlie', 750)")
        .await?;
    client
        .execute("INSERT INTO accounts (id, name, balance) VALUES (4, 'Diana', 1250)")
        .await?;
    client
        .execute("INSERT INTO accounts (id, name, balance) VALUES (5, 'Eve', 600)")
        .await?;
    println!("  → Inserted 3 new accounts");

    client.execute("COMMIT").await?;
    println!("  → Batch committed!");

    let count = get_account_count(&client).await?;
    println!("  ✓ Total accounts: {}\n", count);

    println!("✨ Transactions example completed!");
    println!("\n💡 ACID Properties Demonstrated:");
    println!("   ✓ Atomicity: All operations succeed or fail together");
    println!("   ✓ Consistency: Database stays in valid state");
    println!("   ✓ Isolation: Transactions don't interfere");
    println!("   ✓ Durability: Committed changes persist");

    Ok(())
}

async fn setup_tables(client: &Client) -> Result<()> {
    // Drop table if it exists from previous run
    let _ = client.execute("DROP TABLE accounts").await;

    client
        .execute(
            "CREATE TABLE accounts (\
             id BIGINT PRIMARY KEY, \
             name TEXT, \
             balance BIGINT\
             )"
        )
        .await?;

    Ok(())
}

async fn print_balances(client: &Client) -> Result<()> {
    let rows = client
        .query("SELECT name, balance FROM accounts ORDER BY id")
        .await?;

    for row in &rows {
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let balance = row.get("balance").and_then(|v| v.as_i64()).unwrap_or(0);
        println!("    {} : ${:.2}", name, balance as f64 / 100.0);
    }

    Ok(())
}

async fn get_account_count(client: &Client) -> Result<i64> {
    let rows = client.query("SELECT COUNT(*) FROM accounts").await?;
    Ok(rows
        .first()
        .and_then(|r| r.get_idx(0))
        .and_then(|v| v.as_i64())
        .unwrap_or(0))
}
