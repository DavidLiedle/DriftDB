//! DriftDB Native Rust Client Library
//!
//! A high-level, ergonomic client library for DriftDB with native support for time-travel queries.
//!
//! # Features
//!
//! - **Async/await API** - Built on tokio for high performance
//! - **Type-safe queries** - Deserialize results directly into Rust structs using serde
//! - **Time-travel queries** - First-class support for temporal queries
//! - **Transaction support** - ACID transactions with BEGIN/COMMIT/ROLLBACK
//! - **Connection pooling** - Efficient connection management (coming soon)
//!
//! # Quick Start
//!
//! ```no_run
//! use driftdb_client::{Client, TimeTravel};
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct User {
//!     id: i64,
//!     email: String,
//!     created_at: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to DriftDB
//!     let client = Client::connect("localhost:5433").await?;
//!
//!     // Execute queries
//!     client.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT, created_at TEXT)").await?;
//!     client.execute("INSERT INTO users VALUES (1, 'alice@example.com', '2025-01-01')").await?;
//!
//!     // Query with type-safe deserialization
//!     let users: Vec<User> = client
//!         .query_as("SELECT * FROM users")
//!         .await?;
//!
//!     // Time-travel query
//!     let historical_users: Vec<User> = client
//!         .query_as("SELECT * FROM users")
//!         .as_of(TimeTravel::Sequence(42))
//!         .await?;
//!
//!     // Transactions
//!     let mut tx = client.begin().await?;
//!     tx.execute("INSERT INTO users VALUES (2, 'bob@example.com', '2025-01-02')").await?;
//!     tx.commit().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod error;
pub mod query;
pub mod transaction;
pub mod types;

pub use client::Client;
pub use error::{Error, Result};
pub use query::Query;
pub use transaction::Transaction;
pub use types::{Row, TimeTravel, Value};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_api_exists() {
        // Verify the public API is accessible
        let _: Option<Client> = None;
        let _: Option<Query> = None;
        let _: Option<Transaction> = None;
    }
}
