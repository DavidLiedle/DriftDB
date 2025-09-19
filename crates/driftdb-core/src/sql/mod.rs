//! SQL:2011 Temporal Query Support
//!
//! Implements the SQL:2011 standard for temporal tables, allowing:
//! - FOR SYSTEM_TIME AS OF queries
//! - FOR SYSTEM_TIME BETWEEN queries
//! - FOR SYSTEM_TIME FROM TO queries
//! - FOR SYSTEM_TIME ALL queries

pub mod parser;
pub mod executor;
pub mod temporal;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Statement};

pub use parser::TemporalSqlParser;
pub use parser::TemporalSqlParser as Parser;  // Alias for compatibility
pub use executor::SqlExecutor;

// Simplified Executor wrapper that doesn't require mutable reference
pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Executor
    }

    pub fn execute(&self, engine: &crate::engine::Engine, stmt: &TemporalStatement) -> crate::errors::Result<QueryResult> {
        // For read queries, we can use the engine directly
        // For write queries, we'll need to convert to DriftQL and execute
        match &stmt.statement {
            sqlparser::ast::Statement::Query(_) => {
                // For queries, we need to convert to DriftQL and execute
                // This is a temporary bridge until full SQL support is ready
                Err(crate::errors::DriftError::InvalidQuery(
                    "SQL queries not yet fully implemented - use DriftQL CLI".to_string()
                ))
            }
            _ => {
                Err(crate::errors::DriftError::InvalidQuery(
                    "Only SELECT queries supported via SQL currently".to_string()
                ))
            }
        }
    }
}

/// SQL:2011 temporal clause types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SystemTimeClause {
    /// AS OF <point_in_time>
    AsOf(TemporalPoint),

    /// BETWEEN <start> AND <end>
    Between {
        start: TemporalPoint,
        end: TemporalPoint,
    },

    /// FROM <start> TO <end> (excludes end)
    FromTo {
        start: TemporalPoint,
        end: TemporalPoint,
    },

    /// ALL (entire history)
    All,
}

/// A point in time for temporal queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TemporalPoint {
    /// Specific timestamp
    Timestamp(DateTime<Utc>),

    /// Sequence number (DriftDB extension)
    Sequence(u64),

    /// Current time
    CurrentTimestamp,
}

/// Extended SQL statement with temporal support
#[derive(Debug, Clone)]
pub struct TemporalStatement {
    /// The base SQL statement
    pub statement: Statement,

    /// Optional temporal clause
    pub system_time: Option<SystemTimeClause>,
}

/// Result of a temporal SQL query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalQueryResult {
    /// Result rows
    pub rows: Vec<serde_json::Value>,

    /// Metadata about the temporal query
    pub temporal_metadata: Option<TemporalMetadata>,
}

/// Query result types for SQL execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    Success { message: String },
    Records { columns: Vec<String>, rows: Vec<Vec<serde_json::Value>> },
    Error { message: String },
}

/// Metadata about temporal query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalMetadata {
    /// Actual timestamp used for AS OF queries
    pub as_of_timestamp: Option<DateTime<Utc>>,

    /// Sequence number used
    pub as_of_sequence: Option<u64>,

    /// Number of historical versions examined
    pub versions_scanned: usize,
}

impl SystemTimeClause {
    /// Parse from SQL:2011 syntax
    pub fn from_sql(expr: &str) -> Result<Self, String> {
        // Parse expressions like:
        // - AS OF TIMESTAMP '2024-01-15 10:30:00'
        // - AS OF CURRENT_TIMESTAMP
        // - BETWEEN '2024-01-01' AND '2024-01-31'
        // - ALL

        let normalized = expr.to_uppercase();

        if normalized.contains("AS OF") {
            if normalized.contains("CURRENT_TIMESTAMP") {
                Ok(SystemTimeClause::AsOf(TemporalPoint::CurrentTimestamp))
            } else if let Some(ts_str) = Self::extract_timestamp(&expr) {
                let dt = DateTime::parse_from_rfc3339(&ts_str)
                    .map_err(|e| format!("Invalid timestamp: {}", e))?
                    .with_timezone(&Utc);
                Ok(SystemTimeClause::AsOf(TemporalPoint::Timestamp(dt)))
            } else {
                Err("Invalid AS OF clause".to_string())
            }
        } else if normalized == "ALL" {
            Ok(SystemTimeClause::All)
        } else {
            Err(format!("Unsupported temporal clause: {}", expr))
        }
    }

    fn extract_timestamp(expr: &str) -> Option<String> {
        // Simple extraction - in production use proper parser
        if let Some(start) = expr.find('\'') {
            if let Some(end) = expr[start + 1..].find('\'') {
                return Some(expr[start + 1..start + 1 + end].to_string());
            }
        }
        None
    }
}