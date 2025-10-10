# DriftDB Client - Native Rust Client Library

A high-level, ergonomic Rust client library for [DriftDB](https://github.com/driftdb/driftdb) with first-class support for time-travel queries.

## Features

- ✅ **Async/await API** - Built on tokio for high performance
- ✅ **Type-safe queries** - Deserialize results directly into Rust structs using serde
- ✅ **Time-travel queries** - Native support for querying historical database states
- ✅ **Transaction support** - ACID transactions with BEGIN/COMMIT/ROLLBACK
- ✅ **Ergonomic API** - Builder pattern for complex queries
- ✅ **Connection pooling** - Efficient connection management (coming soon)

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
driftdb-client = "0.8.0-alpha"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Basic Usage

```rust
use driftdb_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to DriftDB
    let client = Client::connect("localhost:5433").await?;

    // Create a table
    client.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)").await?;

    // Insert data
    client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
    client.execute("INSERT INTO users VALUES (2, 'Bob')").await?;

    // Query data
    let rows = client.query("SELECT * FROM users").await?;
    for row in rows {
        let id = row.get("id").and_then(|v| v.as_i64()).unwrap();
        let name = row.get("name").and_then(|v| v.as_str()).unwrap();
        println!("User {}: {}", id, name);
    }

    Ok(())
}
```

## Time-Travel Queries

DriftDB's killer feature - query your data at any point in history:

```rust
use driftdb_client::{Client, TimeTravel};

// Get current sequence number
let seq = client.current_sequence().await?;

// Make some changes
client.execute("UPDATE users SET name = 'Alice Smith' WHERE id = 1").await?;

// Query the historical state (before the update!)
let historical_rows = client
    .query_builder("SELECT * FROM users WHERE id = 1")
    .as_of(TimeTravel::Sequence(seq))
    .execute()
    .await?;

// Query all historical versions
let all_versions = client
    .query_builder("SELECT * FROM users WHERE id = 1")
    .as_of(TimeTravel::All)
    .execute()
    .await?;
```

## Typed Queries with Serde

Deserialize query results directly into Rust structs:

```rust
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct User {
    id: i64,
    name: String,
    email: String,
}

// Type-safe query execution
let users: Vec<User> = client
    .query_as("SELECT * FROM users")
    .await?;

for user in users {
    println!("{}: {} <{}>", user.id, user.name, user.email);
}

// Works with time-travel too!
let historical_users: Vec<User> = client
    .query_builder("SELECT * FROM users")
    .as_of(TimeTravel::Sequence(42))
    .execute_as()
    .await?;
```

## Transactions

ACID transactions for data integrity:

```rust
// Manual transaction management
client.execute("BEGIN").await?;

client.execute("INSERT INTO users VALUES (3, 'Charlie')").await?;
client.execute("INSERT INTO users VALUES (4, 'Diana')").await?;

// Commit or rollback
client.execute("COMMIT").await?;
// or: client.execute("ROLLBACK").await?;
```

## Examples

The `examples/` directory contains complete working examples:

- **`basic_usage.rs`** - CRUD operations and basic querying
- **`time_travel.rs`** - Time-travel query demonstrations
- **`typed_queries.rs`** - Type-safe queries with serde
- **`transactions.rs`** - ACID transaction examples

Run an example:

```bash
cargo run --example basic_usage
cargo run --example time_travel
cargo run --example typed_queries
cargo run --example transactions
```

## TimeTravel Options

```rust
// Query at a specific sequence number
TimeTravel::Sequence(42)

// Query at a timestamp
TimeTravel::Timestamp("2025-01-01T00:00:00Z".to_string())

// Query between two sequences
TimeTravel::Between { start: 10, end: 20 }

// Get all historical versions
TimeTravel::All
```

## API Documentation

### Client

```rust
// Connect to DriftDB
Client::connect("localhost:5433").await?

// Execute a statement (returns affected rows)
client.execute("INSERT INTO ...").await?

// Query and get rows
client.query("SELECT * FROM ...").await?

// Query with type deserialization
client.query_as::<User>("SELECT * FROM users").await?

// Get current sequence number
client.current_sequence().await?
```

### Query Builder

```rust
client.query_builder("SELECT * FROM users")
    .as_of(TimeTravel::Sequence(42))
    .execute()
    .await?

client.query_builder("SELECT * FROM users")
    .as_of(TimeTravel::All)
    .execute_as::<User>()
    .await?
```

### Row Access

```rust
let row = &rows[0];

// Get value by column name
row.get("id")?.as_i64()

// Get value by index
row.get_idx(0)?.as_i64()

// Deserialize entire row
let user: User = row.deserialize()?;
```

## Requirements

- DriftDB server running on port 5433 (or custom port)
- Tokio runtime
- Rust 1.70+

## Status

**Alpha** - The API is functional but may change. The core functionality works:
- ✅ Connection and basic queries
- ✅ Time-travel queries
- ✅ Type-safe deserialization
- ✅ Transaction support (manual BEGIN/COMMIT/ROLLBACK)

Coming soon:
- Connection pooling
- Better transaction API
- Query parameter binding
- Batch operations
- Streaming results

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR on the [DriftDB repository](https://github.com/driftdb/driftdb).
