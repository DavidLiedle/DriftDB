pub mod executor;
pub mod parser;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use parser::parse_driftql;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    CreateTable {
        name: String,
        primary_key: String,
        indexed_columns: Vec<String>,
    },
    Insert {
        table: String,
        data: Value,
    },
    Patch {
        table: String,
        primary_key: Value,
        updates: Value,
    },
    SoftDelete {
        table: String,
        primary_key: Value,
    },
    Select {
        table: String,
        conditions: Vec<WhereCondition>,
        as_of: Option<AsOf>,
        limit: Option<usize>,
    },
    ShowDrift {
        table: String,
        primary_key: Value,
    },
    Snapshot {
        table: String,
    },
    Compact {
        table: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhereCondition {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AsOf {
    Timestamp(time::OffsetDateTime),
    Sequence(u64),
    Now,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    Success { message: String },
    Rows { data: Vec<Value> },
    DriftHistory { events: Vec<Value> },
    Error { message: String },
}
