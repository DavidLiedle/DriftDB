//! Parameterized queries example for DriftDB
//!
//! This example demonstrates safe parameterized queries that prevent SQL injection.
//! Uses client-side escaping as a workaround until the server supports full
//! parameterized queries via the extended query protocol.
//!
//! Run with:
//! ```bash
//! cargo run --example parameterized_queries
//! ```

use driftdb_client::{Client, Result, Value};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🔒 DriftDB Client - Parameterized Queries Example\n");

    let client = Client::connect("localhost:5433").await?;
    println!("✓ Connected!\n");

    // Setup
    println!("Setting up test table...");
    let _ = client.execute("DROP TABLE param_test").await;
    client
        .execute("CREATE TABLE param_test (id BIGINT PRIMARY KEY, name TEXT, description TEXT)")
        .await?;
    println!("✓ Table created!\n");

    // === Example 1: Basic parameterized INSERT ===
    println!("📝 Example 1: Basic parameterized INSERT\n");

    client
        .execute_escaped(
            "INSERT INTO param_test (id, name, description) VALUES ($1, $2, $3)",
            &[
                Value::Int(1),
                Value::Text("Alice".to_string()),
                Value::Text("Normal user".to_string()),
            ],
        )
        .await?;
    println!("  → Inserted Alice with parameterized query");

    // Verify
    let rows = client.query("SELECT * FROM param_test WHERE id = 1").await?;
    println!(
        "  ✓ Retrieved: {} - {}",
        rows[0].get("name").and_then(|v| v.as_str()).unwrap_or("?"),
        rows[0]
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
    );

    // === Example 2: Special characters (SQL injection test) ===
    println!("\n📝 Example 2: Special characters (SQL injection prevention)\n");

    // This name contains single quotes which would break a non-parameterized query
    let dangerous_name = "O'Reilly";
    let dangerous_description = "User with ' quotes \" and other chars; SELECT * FROM users";

    client
        .execute_escaped(
            "INSERT INTO param_test (id, name, description) VALUES ($1, $2, $3)",
            &[
                Value::Int(2),
                Value::Text(dangerous_name.to_string()),
                Value::Text(dangerous_description.to_string()),
            ],
        )
        .await?;
    println!("  → Inserted user with special characters:");
    println!("     Name: {}", dangerous_name);
    println!("     Description: {}", dangerous_description);

    // Verify the data was stored correctly and table still exists
    let rows = client.query("SELECT * FROM param_test WHERE id = 2").await?;
    println!("\n  ✓ Retrieved safely:");
    println!(
        "     Name: {}",
        rows[0].get("name").and_then(|v| v.as_str()).unwrap_or("?")
    );
    println!(
        "     Description: {}",
        rows[0]
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
    );

    // === Example 3: Parameterized SELECT ===
    println!("\n📝 Example 3: Parameterized SELECT query\n");

    let rows = client
        .query_escaped(
            "SELECT * FROM param_test WHERE name = $1",
            &[Value::Text(dangerous_name.to_string())],
        )
        .await?;

    println!("  → Queried for name = '{}'", dangerous_name);
    println!("  ✓ Found {} row(s)", rows.len());
    if !rows.is_empty() {
        println!(
            "     ID: {}",
            rows[0].get("id").and_then(|v| v.as_i64()).unwrap_or(0)
        );
    }

    // === Example 4: Multiple parameters in WHERE ===
    println!("\n📝 Example 4: Multiple parameters in WHERE clause\n");

    client
        .execute_escaped(
            "INSERT INTO param_test (id, name, description) VALUES ($1, $2, $3)",
            &[
                Value::Int(3),
                Value::Text("Bob".to_string()),
                Value::Text("Another user".to_string()),
            ],
        )
        .await?;

    let rows = client
        .query_escaped(
            "SELECT * FROM param_test WHERE id > $1 AND id < $2",
            &[Value::Int(0), Value::Int(10)],
        )
        .await?;

    println!("  → Queried for id > 0 AND id < 10");
    println!("  ✓ Found {} row(s)", rows.len());
    for row in &rows {
        println!(
            "     - {} (id={})",
            row.get("name").and_then(|v| v.as_str()).unwrap_or("?"),
            row.get("id").and_then(|v| v.as_i64()).unwrap_or(0)
        );
    }

    // === Example 5: Unicode and emoji ===
    println!("\n📝 Example 5: Unicode and emoji support\n");

    let unicode_name = "世界"; // "World" in Chinese
    let emoji_description = "User with emoji 🎉🚀✨";

    client
        .execute_escaped(
            "INSERT INTO param_test (id, name, description) VALUES ($1, $2, $3)",
            &[
                Value::Int(4),
                Value::Text(unicode_name.to_string()),
                Value::Text(emoji_description.to_string()),
            ],
        )
        .await?;

    let rows = client
        .query_escaped(
            "SELECT * FROM param_test WHERE id = $1",
            &[Value::Int(4)],
        )
        .await?;

    println!("  → Inserted and retrieved unicode/emoji:");
    println!(
        "     Name: {}",
        rows[0].get("name").and_then(|v| v.as_str()).unwrap_or("?")
    );
    println!(
        "     Description: {}",
        rows[0]
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
    );

    println!("\n✨ Parameterized queries example completed!");
    println!("\n💡 Key Benefits:");
    println!("   ✓ SQL injection prevention via proper escaping");
    println!("   ✓ Automatic escaping of special characters");
    println!("   ✓ Safe handling of quotes, unicode, and emoji");
    println!("\n⚠️  Note: Using client-side escaping until server supports");
    println!("   the extended query protocol. True parameterized queries");
    println!("   (execute_params/query_params) will be available once");
    println!("   server support is implemented.");

    Ok(())
}
