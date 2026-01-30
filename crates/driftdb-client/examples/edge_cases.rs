//! Edge cases and robustness testing for DriftDB client
//!
//! Tests error handling, data types, and complex queries
//!
//! Run with:
//! ```bash
//! cargo run --example edge_cases
//! ```

use driftdb_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🧪 DriftDB Client - Edge Cases & Robustness Testing\n");
    println!("{}", "=".repeat(60));

    let client = Client::connect("localhost:5433").await?;
    println!("✓ Connected to DriftDB\n");

    // Setup
    setup_tables(&client).await?;

    // Run test suites
    test_error_handling(&client).await;
    test_data_types(&client).await;
    test_complex_queries(&client).await;

    println!("\n{}", "=".repeat(60));
    println!("✨ Edge case testing complete!\n");

    Ok(())
}

async fn setup_tables(client: &Client) -> Result<()> {
    println!("📋 Setting up test tables...");

    // Drop existing tables
    let _ = client.execute("DROP TABLE test_data").await;
    let _ = client.execute("DROP TABLE users").await;
    let _ = client.execute("DROP TABLE orders").await;

    // Create test_data table with various types
    client
        .execute(
            "CREATE TABLE test_data (\
         id BIGINT PRIMARY KEY, \
         name TEXT, \
         active BOOLEAN, \
         count BIGINT, \
         price BIGINT\
         )",
        )
        .await?;

    // Create users and orders for JOIN tests
    client
        .execute(
            "CREATE TABLE users (\
         id BIGINT PRIMARY KEY, \
         name TEXT\
         )",
        )
        .await?;

    client
        .execute(
            "CREATE TABLE orders (\
         id BIGINT PRIMARY KEY, \
         user_id BIGINT, \
         amount BIGINT\
         )",
        )
        .await?;

    println!("✓ Tables ready\n");
    Ok(())
}

async fn test_error_handling(client: &Client) {
    println!("🔍 TEST SUITE 1: Error Handling");
    println!("{}", "-".repeat(60));

    // Test 1: Invalid SQL syntax
    print!("  1. Invalid SQL syntax... ");
    match client.query("SELECT * FORM test_data").await {
        Ok(_) => println!("❌ UNEXPECTED: Should have failed"),
        Err(e) => println!(
            "✓ Correctly rejected: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }

    // Test 2: Non-existent table
    print!("  2. Non-existent table... ");
    match client.query("SELECT * FROM nonexistent_table").await {
        Ok(_) => println!("❌ UNEXPECTED: Should have failed"),
        Err(e) => println!(
            "✓ Correctly rejected: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }

    // Test 3: Duplicate primary key
    print!("  3. Duplicate primary key... ");
    let _ = client
        .execute("INSERT INTO test_data (id, name) VALUES (1, 'first')")
        .await;
    match client
        .execute("INSERT INTO test_data (id, name) VALUES (1, 'duplicate')")
        .await
    {
        Ok(_) => println!("⚠️  WARNING: Duplicate key was accepted"),
        Err(e) => println!(
            "✓ Correctly rejected: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }

    // Test 4: Missing required column
    print!("  4. Missing primary key... ");
    match client
        .execute("INSERT INTO test_data (name) VALUES ('no id')")
        .await
    {
        Ok(_) => println!("⚠️  WARNING: Missing primary key was accepted"),
        Err(e) => println!(
            "✓ Correctly rejected: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }

    // Test 5: Invalid column in INSERT
    print!("  5. Invalid column name... ");
    match client
        .execute("INSERT INTO test_data (id, nonexistent_col) VALUES (99, 'test')")
        .await
    {
        Ok(_) => println!("⚠️  WARNING: Invalid column was accepted"),
        Err(e) => println!(
            "✓ Correctly rejected: {}",
            e.to_string().lines().next().unwrap_or("")
        ),
    }

    println!();
}

async fn test_data_types(client: &Client) {
    println!("🔍 TEST SUITE 2: Data Types & Edge Cases");
    println!("{}", "-".repeat(60));

    // Clear table
    let _ = client.execute("DELETE FROM test_data").await;

    // Test 1: NULL values
    print!("  1. NULL values... ");
    match client
        .execute("INSERT INTO test_data (id, name) VALUES (10, NULL)")
        .await
    {
        Ok(_) => {
            if let Ok(rows) = client
                .query("SELECT name FROM test_data WHERE id = 10")
                .await
            {
                if let Some(row) = rows.first() {
                    if let Some(val) = row.get("name") {
                        if val.is_null() {
                            println!("✓ NULL stored and retrieved correctly");
                        } else {
                            println!("⚠️  Retrieved as: {:?}", val);
                        }
                    } else {
                        println!("⚠️  Column not found");
                    }
                } else {
                    println!("⚠️  No rows returned");
                }
            } else {
                println!("⚠️  Query failed");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 2: Boolean values
    print!("  2. Boolean true/false... ");
    match client
        .execute("INSERT INTO test_data (id, active) VALUES (11, true)")
        .await
    {
        Ok(_) => {
            match client
                .execute("INSERT INTO test_data (id, active) VALUES (12, false)")
                .await
            {
                Ok(_) => {
                    if let Ok(rows) = client
                        .query("SELECT id, active FROM test_data WHERE id IN (11, 12) ORDER BY id")
                        .await
                    {
                        if rows.len() == 2 {
                            let t = rows[0].get("active").and_then(|v| v.as_bool());
                            let f = rows[1].get("active").and_then(|v| v.as_bool());
                            if t == Some(true) && f == Some(false) {
                                println!("✓ Booleans work correctly");
                            } else {
                                println!("⚠️  Values: {:?}, {:?}", t, f);
                            }
                        } else {
                            println!("⚠️  Expected 2 rows, got {}", rows.len());
                        }
                    } else {
                        println!("⚠️  Query failed");
                    }
                }
                Err(e) => println!("❌ Failed on false: {}", e),
            }
        }
        Err(e) => println!("❌ Failed on true: {}", e),
    }

    // Test 3: Empty strings
    print!("  3. Empty string... ");
    match client
        .execute("INSERT INTO test_data (id, name) VALUES (20, '')")
        .await
    {
        Ok(_) => {
            if let Ok(rows) = client
                .query("SELECT name FROM test_data WHERE id = 20")
                .await
            {
                if let Some(row) = rows.first() {
                    if let Some(val) = row.get("name").and_then(|v| v.as_str()) {
                        if val.is_empty() {
                            println!("✓ Empty string preserved");
                        } else {
                            println!("⚠️  Got: '{}'", val);
                        }
                    } else {
                        println!("⚠️  Not a string");
                    }
                } else {
                    println!("⚠️  No rows");
                }
            } else {
                println!("⚠️  Query failed");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 4: Special characters
    print!("  4. Special characters (quotes, backslash)... ");
    let special = "O'Reilly \"quoted\" \\backslash";
    match client
        .execute(&format!(
            "INSERT INTO test_data (id, name) VALUES (21, '{}')",
            special
        ))
        .await
    {
        Ok(_) => {
            if let Ok(rows) = client
                .query("SELECT name FROM test_data WHERE id = 21")
                .await
            {
                if let Some(row) = rows.first() {
                    if let Some(val) = row.get("name").and_then(|v| v.as_str()) {
                        if val == special {
                            println!("✓ Special characters preserved");
                        } else {
                            println!("⚠️  Expected: '{}', Got: '{}'", special, val);
                        }
                    } else {
                        println!("⚠️  Not a string");
                    }
                } else {
                    println!("⚠️  No rows");
                }
            } else {
                println!("⚠️  Query failed");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 5: Unicode
    print!("  5. Unicode characters (emoji, Chinese)... ");
    let unicode = "Hello 👋 世界 🚀";
    match client
        .execute(&format!(
            "INSERT INTO test_data (id, name) VALUES (22, '{}')",
            unicode
        ))
        .await
    {
        Ok(_) => {
            if let Ok(rows) = client
                .query("SELECT name FROM test_data WHERE id = 22")
                .await
            {
                if let Some(row) = rows.first() {
                    if let Some(val) = row.get("name").and_then(|v| v.as_str()) {
                        if val == unicode {
                            println!("✓ Unicode preserved");
                        } else {
                            println!("⚠️  Expected: '{}', Got: '{}'", unicode, val);
                        }
                    } else {
                        println!("⚠️  Not a string");
                    }
                } else {
                    println!("⚠️  No rows");
                }
            } else {
                println!("⚠️  Query failed");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 6: Large integers
    print!("  6. Large integers... ");
    let large = 9223372036854775806_i64; // Near i64::MAX
    match client
        .execute(&format!(
            "INSERT INTO test_data (id, count) VALUES (30, {})",
            large
        ))
        .await
    {
        Ok(_) => {
            if let Ok(rows) = client
                .query("SELECT count FROM test_data WHERE id = 30")
                .await
            {
                if let Some(row) = rows.first() {
                    if let Some(val) = row.get("count").and_then(|v| v.as_i64()) {
                        if val == large {
                            println!("✓ Large integer preserved");
                        } else {
                            println!("⚠️  Expected: {}, Got: {}", large, val);
                        }
                    } else {
                        println!("⚠️  Not an integer");
                    }
                } else {
                    println!("⚠️  No rows");
                }
            } else {
                println!("⚠️  Query failed");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 7: Zero values
    print!("  7. Zero values... ");
    match client
        .execute("INSERT INTO test_data (id, count, price) VALUES (31, 0, 0)")
        .await
    {
        Ok(_) => {
            if let Ok(rows) = client
                .query("SELECT count, price FROM test_data WHERE id = 31")
                .await
            {
                if let Some(row) = rows.first() {
                    let c = row.get("count").and_then(|v| v.as_i64());
                    let p = row.get("price").and_then(|v| v.as_i64());
                    if c == Some(0) && p == Some(0) {
                        println!("✓ Zero values preserved");
                    } else {
                        println!("⚠️  Got: count={:?}, price={:?}", c, p);
                    }
                } else {
                    println!("⚠️  No rows");
                }
            } else {
                println!("⚠️  Query failed");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    println!();
}

async fn test_complex_queries(client: &Client) {
    println!("🔍 TEST SUITE 3: Complex Queries");
    println!("{}", "-".repeat(60));

    // Setup data for complex queries
    let _ = client.execute("DELETE FROM users").await;
    let _ = client.execute("DELETE FROM orders").await;

    let _ = client
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .await;
    let _ = client
        .execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        .await;
    let _ = client
        .execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
        .await;

    let _ = client
        .execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)")
        .await;
    let _ = client
        .execute("INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 200)")
        .await;
    let _ = client
        .execute("INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 150)")
        .await;
    let _ = client
        .execute("INSERT INTO orders (id, user_id, amount) VALUES (4, 2, 250)")
        .await;

    // Test 1: COUNT aggregation
    print!("  1. COUNT aggregation... ");
    match client.query("SELECT COUNT(*) FROM users").await {
        Ok(rows) => {
            if let Some(row) = rows.first() {
                if let Some(count) = row.get_idx(0).and_then(|v| v.as_i64()) {
                    if count == 3 {
                        println!("✓ COUNT works (got {})", count);
                    } else {
                        println!("⚠️  Expected 3, got {}", count);
                    }
                } else {
                    println!("⚠️  Could not parse count");
                }
            } else {
                println!("⚠️  No rows returned");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 2: SUM aggregation
    print!("  2. SUM aggregation... ");
    match client.query("SELECT SUM(amount) FROM orders").await {
        Ok(rows) => {
            if let Some(row) = rows.first() {
                if let Some(sum) = row.get_idx(0).and_then(|v| v.as_i64()) {
                    if sum == 700 {
                        println!("✓ SUM works (got {})", sum);
                    } else {
                        println!("⚠️  Expected 700, got {}", sum);
                    }
                } else {
                    println!("⚠️  Could not parse sum");
                }
            } else {
                println!("⚠️  No rows returned");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 3: AVG aggregation
    print!("  3. AVG aggregation... ");
    match client.query("SELECT AVG(amount) FROM orders").await {
        Ok(rows) => {
            if let Some(row) = rows.first() {
                if let Some(avg) = row.get_idx(0).and_then(|v| v.as_i64()) {
                    if avg == 175 {
                        println!("✓ AVG works (got {})", avg);
                    } else {
                        println!("⚠️  Expected 175, got {}", avg);
                    }
                } else {
                    println!("⚠️  Could not parse average");
                }
            } else {
                println!("⚠️  No rows returned");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 4: MAX/MIN
    print!("  4. MAX/MIN aggregation... ");
    match client
        .query("SELECT MAX(amount), MIN(amount) FROM orders")
        .await
    {
        Ok(rows) => {
            if let Some(row) = rows.first() {
                let max = row.get_idx(0).and_then(|v| v.as_i64());
                let min = row.get_idx(1).and_then(|v| v.as_i64());
                if max == Some(250) && min == Some(100) {
                    println!("✓ MAX/MIN work (max={:?}, min={:?})", max, min);
                } else {
                    println!(
                        "⚠️  Expected max=250, min=100, got max={:?}, min={:?}",
                        max, min
                    );
                }
            } else {
                println!("⚠️  No rows returned");
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 5: GROUP BY
    print!("  5. GROUP BY... ");
    match client
        .query("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id ORDER BY user_id")
        .await
    {
        Ok(rows) => {
            if rows.len() == 2 {
                let u1_count = rows[0].get_idx(1).and_then(|v| v.as_i64());
                let u2_count = rows[1].get_idx(1).and_then(|v| v.as_i64());
                if u1_count == Some(2) && u2_count == Some(2) {
                    println!("✓ GROUP BY works");
                } else {
                    println!(
                        "⚠️  Expected both counts=2, got {:?}, {:?}",
                        u1_count, u2_count
                    );
                }
            } else {
                println!("⚠️  Expected 2 groups, got {}", rows.len());
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 6: JOIN
    print!("  6. JOIN query... ");
    match client.query("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1 ORDER BY orders.amount").await {
        Ok(rows) => {
            if rows.len() == 2 {
                let name1 = rows[0].get("name").and_then(|v| v.as_str());
                let name2 = rows[1].get("name").and_then(|v| v.as_str());
                let amt1 = rows[0].get("amount").and_then(|v| v.as_i64());
                let amt2 = rows[1].get("amount").and_then(|v| v.as_i64());

                if name1 == Some("Alice") && name2 == Some("Alice") && amt1 == Some(100) && amt2 == Some(200) {
                    println!("✓ JOIN works");
                } else {
                    println!("⚠️  Unexpected results: {:?}, {:?}", rows[0].get("name"), rows[1].get("amount"));
                }
            } else {
                println!("⚠️  Expected 2 rows, got {}", rows.len());
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 7: ORDER BY with multiple columns
    print!("  7. ORDER BY multiple columns... ");
    match client
        .query("SELECT user_id, amount FROM orders ORDER BY user_id DESC, amount ASC")
        .await
    {
        Ok(rows) => {
            if rows.len() == 4 {
                // Should be: (2,150), (2,250), (1,100), (1,200)
                let check = rows[0].get("user_id").and_then(|v| v.as_i64()) == Some(2)
                    && rows[0].get("amount").and_then(|v| v.as_i64()) == Some(150)
                    && rows[3].get("user_id").and_then(|v| v.as_i64()) == Some(1)
                    && rows[3].get("amount").and_then(|v| v.as_i64()) == Some(200);

                if check {
                    println!("✓ Multi-column ORDER BY works");
                } else {
                    println!("⚠️  Ordering incorrect");
                }
            } else {
                println!("⚠️  Expected 4 rows, got {}", rows.len());
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 8: LIMIT and offset
    print!("  8. LIMIT... ");
    match client.query("SELECT * FROM orders LIMIT 2").await {
        Ok(rows) => {
            if rows.len() == 2 {
                println!("✓ LIMIT works");
            } else {
                println!("⚠️  Expected 2 rows, got {}", rows.len());
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 9: WHERE with AND/OR
    print!("  9. WHERE with AND/OR... ");
    match client
        .query("SELECT * FROM orders WHERE user_id = 1 AND amount > 150")
        .await
    {
        Ok(rows) => {
            if rows.len() == 1 {
                if let Some(amt) = rows[0].get("amount").and_then(|v| v.as_i64()) {
                    if amt == 200 {
                        println!("✓ Complex WHERE works");
                    } else {
                        println!("⚠️  Wrong amount: {}", amt);
                    }
                } else {
                    println!("⚠️  Could not get amount");
                }
            } else {
                println!("⚠️  Expected 1 row, got {}", rows.len());
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    // Test 10: Empty result set
    print!("  10. Empty result set... ");
    match client
        .query("SELECT * FROM orders WHERE user_id = 999")
        .await
    {
        Ok(rows) => {
            if rows.is_empty() {
                println!("✓ Empty result set handled correctly");
            } else {
                println!("⚠️  Expected 0 rows, got {}", rows.len());
            }
        }
        Err(e) => println!("❌ Failed: {}", e),
    }

    println!();
}
