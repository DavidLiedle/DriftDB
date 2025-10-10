//! Error types for the DriftDB client library

use thiserror::Error;

/// Result type alias for DriftDB client operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when using the DriftDB client
#[derive(Debug, Error)]
pub enum Error {
    /// Connection failed
    #[error("Failed to connect to DriftDB: {0}")]
    Connection(String),

    /// Query execution failed
    #[error("Query execution failed: {0}")]
    Query(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Deserialization error
    #[error("Failed to deserialize result: {0}")]
    Deserialization(#[from] serde_json::Error),

    /// Invalid time-travel specification
    #[error("Invalid time-travel specification: {0}")]
    InvalidTimeTravel(String),

    /// PostgreSQL protocol error
    #[error("PostgreSQL protocol error: {0}")]
    Protocol(#[from] tokio_postgres::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Other errors
    #[error("DriftDB client error: {0}")]
    Other(String),
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Other(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Other(s.to_string())
    }
}
