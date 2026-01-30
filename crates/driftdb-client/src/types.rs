//! Core types for the DriftDB client library

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Time-travel query specification
///
/// DriftDB supports querying historical states of the database.
#[derive(Debug, Clone)]
pub enum TimeTravel {
    /// Query at a specific sequence number
    Sequence(u64),

    /// Query at a specific timestamp (ISO 8601 format)
    Timestamp(String),

    /// Query between two sequence numbers
    Between { start: u64, end: u64 },

    /// Query all historical versions
    All,
}

impl TimeTravel {
    /// Convert to SQL AS OF clause
    pub fn to_sql(&self) -> String {
        match self {
            TimeTravel::Sequence(seq) => format!("AS OF @seq:{}", seq),
            TimeTravel::Timestamp(ts) => format!("AS OF TIMESTAMP '{}'", ts),
            TimeTravel::Between { start, end } => {
                format!("FOR SYSTEM_TIME BETWEEN @seq:{} AND @seq:{}", start, end)
            }
            TimeTravel::All => "FOR SYSTEM_TIME ALL".to_string(),
        }
    }
}

/// A value returned from a query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl Value {
    /// Try to convert value to a bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to convert value to an i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to convert value to a f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert value to a string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// A row returned from a query
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    /// Create a new row
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }

    /// Get value by column name
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == column)
            .and_then(|idx| self.values.get(idx))
    }

    /// Get value by index
    pub fn get_idx(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// Get all column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get all values
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Convert row to a HashMap
    pub fn to_map(&self) -> HashMap<String, Value> {
        self.columns
            .iter()
            .zip(self.values.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Deserialize row into a typed struct
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        let map = self.to_map();
        let json = serde_json::to_value(map)?;
        serde_json::from_value(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_travel_to_sql() {
        assert_eq!(TimeTravel::Sequence(42).to_sql(), "AS OF @seq:42");
        assert_eq!(
            TimeTravel::Timestamp("2025-01-01T00:00:00Z".to_string()).to_sql(),
            "AS OF TIMESTAMP '2025-01-01T00:00:00Z'"
        );
        assert_eq!(
            TimeTravel::Between { start: 10, end: 20 }.to_sql(),
            "FOR SYSTEM_TIME BETWEEN @seq:10 AND @seq:20"
        );
        assert_eq!(TimeTravel::All.to_sql(), "FOR SYSTEM_TIME ALL");
    }

    #[test]
    fn test_value_conversions() {
        let v = Value::Int(42);
        assert_eq!(v.as_i64(), Some(42));
        assert_eq!(v.as_f64(), Some(42.0));
        assert!(!v.is_null());

        let v = Value::Null;
        assert!(v.is_null());
        assert_eq!(v.as_i64(), None);
    }

    #[test]
    fn test_row_access() {
        let row = Row::new(
            vec!["id".to_string(), "name".to_string()],
            vec![Value::Int(1), Value::Text("Alice".to_string())],
        );

        assert_eq!(row.get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(row.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(row.get("missing"), None);
    }
}
