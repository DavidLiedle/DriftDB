//! PostgreSQL Wire Protocol Implementation
//!
//! Implements the PostgreSQL v3 wire protocol to allow any PostgreSQL
//! client to connect to DriftDB.

pub mod auth;
pub mod codec;
pub mod messages;

pub use messages::Message;

/// PostgreSQL protocol version
#[allow(dead_code)]
pub const PROTOCOL_VERSION: i32 = 196608; // 3.0

/// Transaction status for ReadyForQuery message
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionStatus {
    Idle,          // 'I'
    InTransaction, // 'T'
    #[allow(dead_code)]
    Failed, // 'E'
}

impl TransactionStatus {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_byte(&self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }
}

/// PostgreSQL data type OIDs
#[derive(Debug, Clone, Copy)]
#[repr(i32)]
#[allow(dead_code)]
pub enum DataType {
    Bool = 16,
    Int2 = 21,
    Int4 = 23,
    Int8 = 20,
    Float4 = 700,
    Float8 = 701,
    Text = 25,
    Varchar = 1043,
    Timestamp = 1114,
    TimestampTz = 1184,
    Json = 114,
    Jsonb = 3802,
}

/// Field description for row results
#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_id: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16, // 0 = text, 1 = binary
}

impl FieldDescription {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            table_oid: 0,
            column_id: 0,
            type_oid: data_type as i32,
            type_size: Self::type_size(data_type),
            type_modifier: -1,
            format_code: 0, // Always text for now
        }
    }

    fn type_size(data_type: DataType) -> i16 {
        match data_type {
            DataType::Bool => 1,
            DataType::Int2 => 2,
            DataType::Int4 => 4,
            DataType::Int8 => 8,
            DataType::Float4 => 4,
            DataType::Float8 => 8,
            _ => -1, // Variable length
        }
    }
}

/// Convert DriftDB values to PostgreSQL text format
pub fn value_to_postgres_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Some(value.to_string()) // JSON as text
        }
    }
}

/// PostgreSQL error codes
#[allow(dead_code)]
pub mod error_codes {
    pub const SUCCESSFUL_COMPLETION: &str = "00000";
    pub const WARNING: &str = "01000";
    pub const NO_DATA: &str = "02000";
    pub const INVALID_SQL_STATEMENT: &str = "07001";
    pub const CONNECTION_EXCEPTION: &str = "08000";
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const INVALID_TRANSACTION_STATE: &str = "25000";
    pub const INVALID_AUTHORIZATION: &str = "28000";
    pub const INVALID_CATALOG_NAME: &str = "3D000";
    pub const INVALID_CURSOR_NAME: &str = "34000";
    pub const INVALID_SQL_STATEMENT_NAME: &str = "26000";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
    pub const TOO_MANY_CONNECTIONS: &str = "53300";
    pub const INTERNAL_ERROR: &str = "XX000";
}
