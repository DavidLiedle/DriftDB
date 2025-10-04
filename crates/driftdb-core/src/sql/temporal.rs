//! SQL:2011 Temporal Table Support
//!
//! Implements system-versioned tables according to SQL:2011 standard

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{SystemTimeClause, TemporalPoint};
use crate::errors::Result;

/// SQL:2011 Temporal Table Definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalTable {
    /// Table name
    pub name: String,

    /// System time period definition
    pub system_time_period: SystemTimePeriod,

    /// Whether this is a system-versioned table
    pub with_system_versioning: bool,

    /// History table name (optional, DriftDB stores inline)
    pub history_table: Option<String>,
}

/// System time period columns (SQL:2011)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemTimePeriod {
    /// Start time column name (usually SYSTEM_TIME_START)
    pub start_column: String,

    /// End time column name (usually SYSTEM_TIME_END)
    pub end_column: String,
}

impl Default for SystemTimePeriod {
    fn default() -> Self {
        Self {
            start_column: "SYSTEM_TIME_START".to_string(),
            end_column: "SYSTEM_TIME_END".to_string(),
        }
    }
}

/// SQL:2011 DDL for temporal tables
pub struct TemporalDDL;

impl TemporalDDL {
    /// Generate CREATE TABLE statement for temporal table
    pub fn create_temporal_table(name: &str, columns: Vec<ColumnDef>, primary_key: &str) -> String {
        let mut ddl = format!("CREATE TABLE {} (\n", name);

        // Add user columns
        for col in &columns {
            ddl.push_str(&format!(
                "    {} {} {},\n",
                col.name,
                col.data_type,
                if col.not_null { "NOT NULL" } else { "" }
            ));
        }

        // Add system time columns (hidden by default)
        ddl.push_str("    SYSTEM_TIME_START TIMESTAMP(12) GENERATED ALWAYS AS ROW START,\n");
        ddl.push_str("    SYSTEM_TIME_END TIMESTAMP(12) GENERATED ALWAYS AS ROW END,\n");

        // Add primary key
        ddl.push_str(&format!("    PRIMARY KEY ({}),\n", primary_key));

        // Add period definition
        ddl.push_str("    PERIOD FOR SYSTEM_TIME (SYSTEM_TIME_START, SYSTEM_TIME_END)\n");

        ddl.push_str(") WITH SYSTEM VERSIONING");

        ddl
    }

    /// Generate ALTER TABLE to add system versioning
    pub fn add_system_versioning(table_name: &str) -> String {
        format!(
            "ALTER TABLE {} ADD PERIOD FOR SYSTEM_TIME (SYSTEM_TIME_START, SYSTEM_TIME_END), \
             ADD SYSTEM VERSIONING",
            table_name
        )
    }

    /// Generate ALTER TABLE to drop system versioning
    pub fn drop_system_versioning(table_name: &str) -> String {
        format!("ALTER TABLE {} DROP SYSTEM VERSIONING", table_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
}

/// Convert DriftDB time-travel to SQL:2011 semantics
pub struct TemporalSemantics;

impl TemporalSemantics {
    /// Translate SQL:2011 temporal point to DriftDB concepts
    pub fn to_driftdb_point(point: &TemporalPoint) -> Result<DriftDbPoint> {
        match point {
            TemporalPoint::Timestamp(ts) => Ok(DriftDbPoint::Timestamp(*ts)),
            TemporalPoint::Sequence(seq) => Ok(DriftDbPoint::Sequence(*seq)),
            TemporalPoint::CurrentTimestamp => Ok(DriftDbPoint::Timestamp(Utc::now())),
        }
    }

    /// Apply temporal clause to query
    pub fn apply_temporal_filter(
        clause: &SystemTimeClause,
        _current_time: DateTime<Utc>,
    ) -> Result<TemporalFilter> {
        match clause {
            SystemTimeClause::AsOf(point) => {
                let drift_point = Self::to_driftdb_point(point)?;
                Ok(TemporalFilter::AsOf(drift_point))
            }

            SystemTimeClause::Between { start, end } => {
                let start_point = Self::to_driftdb_point(start)?;
                let end_point = Self::to_driftdb_point(end)?;
                Ok(TemporalFilter::Between {
                    start: start_point,
                    end: end_point,
                    inclusive: true,
                })
            }

            SystemTimeClause::FromTo { start, end } => {
                let start_point = Self::to_driftdb_point(start)?;
                let end_point = Self::to_driftdb_point(end)?;
                Ok(TemporalFilter::Between {
                    start: start_point,
                    end: end_point,
                    inclusive: false, // FROM...TO excludes end
                })
            }

            SystemTimeClause::All => Ok(TemporalFilter::All),
        }
    }

    /// Check if a row is valid at a specific time
    pub fn is_valid_at(
        row_start: DateTime<Utc>,
        row_end: Option<DateTime<Utc>>,
        query_time: DateTime<Utc>,
    ) -> bool {
        // Row is valid if:
        // - start_time <= query_time
        // - end_time > query_time (or end_time is NULL for current rows)

        if row_start > query_time {
            return false;
        }

        match row_end {
            Some(end) => end > query_time,
            None => true, // Current row (no end time)
        }
    }
}

/// Internal representation of temporal filter
#[derive(Debug, Clone)]
pub enum TemporalFilter {
    AsOf(DriftDbPoint),
    Between {
        start: DriftDbPoint,
        end: DriftDbPoint,
        inclusive: bool,
    },
    All,
}

#[derive(Debug, Clone)]
pub enum DriftDbPoint {
    Timestamp(DateTime<Utc>),
    Sequence(u64),
}

/// SQL:2011 compliant temporal operations
pub trait TemporalOperations {
    /// Query as of a specific point in time
    fn query_as_of(&self, table: &str, timestamp: DateTime<Utc>) -> Result<Vec<serde_json::Value>>;

    /// Query all versions between two points
    fn query_between(
        &self,
        table: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<serde_json::Value>>;

    /// Query all historical versions
    fn query_all_versions(&self, table: &str) -> Result<Vec<serde_json::Value>>;

    /// Get the history of a specific row
    fn query_row_history(
        &self,
        table: &str,
        primary_key: &serde_json::Value,
    ) -> Result<Vec<serde_json::Value>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_temporal_table_ddl() {
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                not_null: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: "VARCHAR(100)".to_string(),
                not_null: false,
            },
        ];

        let ddl = TemporalDDL::create_temporal_table("users", columns, "id");

        assert!(ddl.contains("WITH SYSTEM VERSIONING"));
        assert!(ddl.contains("SYSTEM_TIME_START"));
        assert!(ddl.contains("SYSTEM_TIME_END"));
        assert!(ddl.contains("PERIOD FOR SYSTEM_TIME"));
    }

    #[test]
    fn test_is_valid_at() {
        let start = Utc::now() - chrono::Duration::hours(2);
        let end = Some(Utc::now() - chrono::Duration::hours(1));
        let query_time = Utc::now() - chrono::Duration::minutes(90);

        assert!(TemporalSemantics::is_valid_at(start, end, query_time));

        let query_time_after = Utc::now();
        assert!(!TemporalSemantics::is_valid_at(
            start,
            end,
            query_time_after
        ));
    }
}
