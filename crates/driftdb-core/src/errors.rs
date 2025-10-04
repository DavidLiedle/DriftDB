use thiserror::Error;

#[derive(Error, Debug)]
pub enum DriftError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Lock error: {0}")]
    Lock(String),

    #[error("Corrupt segment: {0}")]
    CorruptSegment(String),

    #[error("Data corruption detected: {0}")]
    Corruption(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Snapshot error: {0}")]
    Snapshot(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Pool exhausted")]
    PoolExhausted,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not leader")]
    NotLeader,

    #[error("Timeout")]
    Timeout,

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Conflict error: {0}")]
    Conflict(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, DriftError>;

impl From<rmp_serde::encode::Error> for DriftError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        DriftError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for DriftError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        DriftError::Deserialization(e.to_string())
    }
}

impl From<bincode::Error> for DriftError {
    fn from(e: bincode::Error) -> Self {
        DriftError::Serialization(e.to_string())
    }
}

impl From<serde_yaml::Error> for DriftError {
    fn from(e: serde_yaml::Error) -> Self {
        DriftError::Schema(e.to_string())
    }
}

impl From<serde_json::Error> for DriftError {
    fn from(e: serde_json::Error) -> Self {
        DriftError::Serialization(e.to_string())
    }
}
