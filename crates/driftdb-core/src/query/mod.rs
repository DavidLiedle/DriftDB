pub mod parser;
pub mod executor;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// DriftQL parser removed - using 100% SQL compatibility

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
    BackupDatabase {
        destination: String,
        compression: Option<String>,
        incremental: bool,
    },
    BackupTable {
        table: String,
        destination: String,
        compression: Option<String>,
    },
    RestoreDatabase {
        source: String,
        target: Option<String>,
        verify: bool,
    },
    RestoreTable {
        table: String,
        source: String,
        target: Option<String>,
        verify: bool,
    },
    ShowBackups {
        directory: Option<String>,
    },
    VerifyBackup {
        backup_path: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhereCondition {
    pub column: String,
    pub operator: String,
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