//! Time-travel query example for DriftDB
//!
//! This example demonstrates DriftDB's unique time-travel capabilities,
//! allowing you to query historical states of your data.
//!
//! Run with:
//! ```bash
//! cargo run --example time_travel
//! ```

use driftdb_client::{Client, Result, TimeTravel};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🕰️  DriftDB Client - Time-Travel Example\n");

    // Connect to DriftDB
    println!("Connecting to DriftDB...");
    let client = Client::connect("localhost:5433").await?;
    println!("✓ Connected!\n");

    // Create a table for our demo
    println!("Setting up demo table...");
    let _ = client.execute("DROP TABLE products").await; // Ignore error if table doesn't exist
    client
        .execute("CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT, price BIGINT)")
        .await?;
    println!("✓ Table ready!\n");

    // === Version 1: Initial state ===
    println!("📝 Version 1: Creating initial product...");
    client
        .execute("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 1000)")
        .await?;

    let seq1 = client.current_sequence().await?;
    println!("   Sequence: {}", seq1);
    print_product(&client, "SELECT * FROM products WHERE id = 1").await?;

    // === Version 2: Price increase ===
    println!("\n📝 Version 2: Increasing price...");
    client
        .execute("UPDATE products SET price = 1500 WHERE id = 1")
        .await?;

    let seq2 = client.current_sequence().await?;
    println!("   Sequence: {}", seq2);
    print_product(&client, "SELECT * FROM products WHERE id = 1").await?;

    // === Version 3: Product rename ===
    println!("\n📝 Version 3: Renaming product...");
    client
        .execute("UPDATE products SET name = 'Super Widget' WHERE id = 1")
        .await?;

    let seq3 = client.current_sequence().await?;
    println!("   Sequence: {}", seq3);
    print_product(&client, "SELECT * FROM products WHERE id = 1").await?;

    // === Version 4: Final price update ===
    println!("\n📝 Version 4: Final price adjustment...");
    client
        .execute("UPDATE products SET price = 1200 WHERE id = 1")
        .await?;

    let seq4 = client.current_sequence().await?;
    println!("   Sequence: {}", seq4);
    print_product(&client, "SELECT * FROM products WHERE id = 1").await?;

    // Now let's time-travel!
    println!("\n\n🕰️  TIME-TRAVEL QUERIES\n");
    println!("═══════════════════════════════════════\n");

    // Query at each historical point
    println!("🔍 Product at sequence {} (Version 1):", seq1);
    let rows = client
        .query_builder("SELECT * FROM products WHERE id = 1")
        .as_of(TimeTravel::Sequence(seq1))
        .execute()
        .await?;
    print_row_details(&rows);

    println!("\n🔍 Product at sequence {} (Version 2):", seq2);
    let rows = client
        .query_builder("SELECT * FROM products WHERE id = 1")
        .as_of(TimeTravel::Sequence(seq2))
        .execute()
        .await?;
    print_row_details(&rows);

    println!("\n🔍 Product at sequence {} (Version 3):", seq3);
    let rows = client
        .query_builder("SELECT * FROM products WHERE id = 1")
        .as_of(TimeTravel::Sequence(seq3))
        .execute()
        .await?;
    print_row_details(&rows);

    println!("\n🔍 Product at current state (Version 4):");
    let rows = client.query("SELECT * FROM products WHERE id = 1").await?;
    print_row_details(&rows);

    // Show all versions
    println!("\n\n📜 ALL HISTORICAL VERSIONS\n");
    println!("═══════════════════════════════════════\n");

    let rows = client
        .query_builder("SELECT * FROM products WHERE id = 1")
        .as_of(TimeTravel::All)
        .execute()
        .await?;

    println!("Found {} versions in history:", rows.len());
    for (idx, row) in rows.iter().enumerate() {
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let price = row.get("price").and_then(|v| v.as_i64()).unwrap_or(0);
        println!(
            "  Version {}: {} - ${:.2}",
            idx + 1,
            name,
            price as f64 / 100.0
        );
    }

    println!("\n✨ Time-travel example completed!");
    println!("\n💡 Key Takeaway: DriftDB preserves complete history!");
    println!("   You can query ANY previous state of your data.");

    Ok(())
}

async fn print_product(client: &Client, sql: &str) -> Result<()> {
    let rows = client.query(sql).await?;
    if let Some(row) = rows.first() {
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let price = row.get("price").and_then(|v| v.as_i64()).unwrap_or(0);
        println!("   Product: {} - ${:.2}", name, price as f64 / 100.0);
    }
    Ok(())
}

fn print_row_details(rows: &[driftdb_client::types::Row]) {
    if let Some(row) = rows.first() {
        let name = row.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let price = row.get("price").and_then(|v| v.as_i64()).unwrap_or(0);
        println!("   Name: {}", name);
        println!("   Price: ${:.2}", price as f64 / 100.0);
    } else {
        println!("   (No data)");
    }
}
