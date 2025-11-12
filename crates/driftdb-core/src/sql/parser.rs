//! SQL:2011 Temporal Parser
//!
//! Extends standard SQL parser to support temporal table syntax

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::{SystemTimeClause, TemporalPoint, TemporalStatement};
use crate::errors::{DriftError, Result};

pub struct TemporalSqlParser {
    dialect: GenericDialect,
}

impl Default for TemporalSqlParser {
    fn default() -> Self {
        Self::new()
    }
}

impl TemporalSqlParser {
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Parse SQL with temporal extensions
    pub fn parse(&self, sql: &str) -> Result<TemporalStatement> {
        // Look for FOR SYSTEM_TIME clause
        let (base_sql, system_time) = self.extract_temporal_clause(sql)?;

        // Parse the base SQL
        let parser = Parser::parse_sql(&self.dialect, &base_sql)
            .map_err(|e| DriftError::InvalidQuery(format!("SQL parse error: {}", e)))?;

        if parser.is_empty() {
            return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
        }

        let statement = parser.into_iter().next().unwrap();

        Ok(TemporalStatement {
            statement,
            system_time,
        })
    }

    /// Extract FOR SYSTEM_TIME clause from SQL
    fn extract_temporal_clause(&self, sql: &str) -> Result<(String, Option<SystemTimeClause>)> {
        // Look for FOR SYSTEM_TIME patterns
        if let Some(pos) = sql.to_uppercase().find("FOR SYSTEM_TIME") {
            let before = &sql[..pos];
            let temporal_part = &sql[pos..];

            // Find the end of the temporal clause
            let clause_end = self.find_clause_end(temporal_part);
            let temporal_clause = &temporal_part[..clause_end];
            let after = &temporal_part[clause_end..];

            // Parse the temporal clause
            let system_time = self.parse_system_time_clause(temporal_clause)?;

            // Reconstruct SQL without the temporal clause
            let base_sql = format!("{} {}", before.trim(), after.trim());

            Ok((base_sql, Some(system_time)))
        } else {
            Ok((sql.to_string(), None))
        }
    }

    /// Find where the temporal clause ends
    fn find_clause_end(&self, sql: &str) -> usize {
        // Find the next SQL keyword that would end the temporal clause
        let keywords = ["WHERE", "GROUP", "ORDER", "LIMIT", "UNION", ";"];

        let upper = sql.to_uppercase();
        let mut min_pos = sql.len();

        for keyword in keywords {
            if let Some(pos) = upper.find(keyword) {
                min_pos = min_pos.min(pos);
            }
        }

        min_pos
    }

    /// Parse a FOR SYSTEM_TIME clause
    fn parse_system_time_clause(&self, clause: &str) -> Result<SystemTimeClause> {
        let upper = clause.to_uppercase();

        if upper.contains("AS OF") {
            self.parse_as_of(clause)
        } else if upper.contains("BETWEEN") && upper.contains("AND") {
            self.parse_between(clause)
        } else if upper.contains("FROM") && upper.contains("TO") {
            self.parse_from_to(clause)
        } else if upper.contains("ALL") {
            Ok(SystemTimeClause::All)
        } else {
            Err(DriftError::InvalidQuery(format!(
                "Invalid FOR SYSTEM_TIME clause: {}",
                clause
            )))
        }
    }

    /// Parse AS OF clause
    fn parse_as_of(&self, clause: &str) -> Result<SystemTimeClause> {
        let upper = clause.to_uppercase();

        if upper.contains("CURRENT_TIMESTAMP") {
            Ok(SystemTimeClause::AsOf(TemporalPoint::CurrentTimestamp))
        } else if upper.contains("@SEQ:") {
            // DriftDB extension: sequence numbers
            let seq_str = clause
                .split("@SEQ:")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .ok_or_else(|| DriftError::InvalidQuery("Invalid sequence number".to_string()))?;

            let sequence = seq_str
                .parse::<u64>()
                .map_err(|_| DriftError::InvalidQuery("Invalid sequence number".to_string()))?;

            Ok(SystemTimeClause::AsOf(TemporalPoint::Sequence(sequence)))
        } else {
            // Parse timestamp
            let timestamp = self.extract_timestamp(clause)?;
            Ok(SystemTimeClause::AsOf(TemporalPoint::Timestamp(timestamp)))
        }
    }

    /// Parse BETWEEN clause
    fn parse_between(&self, clause: &str) -> Result<SystemTimeClause> {
        // Extract start and end timestamps
        let parts: Vec<&str> = clause.split_whitespace().collect();

        let between_idx = parts
            .iter()
            .position(|&s| s.to_uppercase() == "BETWEEN")
            .ok_or_else(|| DriftError::InvalidQuery("Missing BETWEEN".to_string()))?;

        let and_idx = parts
            .iter()
            .position(|&s| s.to_uppercase() == "AND")
            .ok_or_else(|| DriftError::InvalidQuery("Missing AND".to_string()))?;

        if between_idx >= and_idx {
            return Err(DriftError::InvalidQuery(
                "Invalid BETWEEN...AND syntax".to_string(),
            ));
        }

        let start_str = parts[between_idx + 1..and_idx].join(" ");
        let end_str = parts[and_idx + 1..].join(" ");

        let start = self.parse_temporal_point(&start_str)?;
        let end = self.parse_temporal_point(&end_str)?;

        Ok(SystemTimeClause::Between { start, end })
    }

    /// Parse FROM...TO clause
    fn parse_from_to(&self, clause: &str) -> Result<SystemTimeClause> {
        let parts: Vec<&str> = clause.split_whitespace().collect();

        let from_idx = parts
            .iter()
            .position(|&s| s.to_uppercase() == "FROM")
            .ok_or_else(|| DriftError::InvalidQuery("Missing FROM".to_string()))?;

        let to_idx = parts
            .iter()
            .position(|&s| s.to_uppercase() == "TO")
            .ok_or_else(|| DriftError::InvalidQuery("Missing TO".to_string()))?;

        if from_idx >= to_idx {
            return Err(DriftError::InvalidQuery(
                "Invalid FROM...TO syntax".to_string(),
            ));
        }

        let start_str = parts[from_idx + 1..to_idx].join(" ");
        let end_str = parts[to_idx + 1..].join(" ");

        let start = self.parse_temporal_point(&start_str)?;
        let end = self.parse_temporal_point(&end_str)?;

        Ok(SystemTimeClause::FromTo { start, end })
    }

    /// Parse a temporal point (timestamp or sequence)
    fn parse_temporal_point(&self, s: &str) -> Result<TemporalPoint> {
        let trimmed = s.trim();

        if trimmed.to_uppercase() == "CURRENT_TIMESTAMP" {
            Ok(TemporalPoint::CurrentTimestamp)
        } else if trimmed.starts_with("@SEQ:") {
            let seq = trimmed[5..]
                .parse::<u64>()
                .map_err(|_| DriftError::InvalidQuery("Invalid sequence number".to_string()))?;
            Ok(TemporalPoint::Sequence(seq))
        } else {
            let timestamp = self.extract_timestamp(trimmed)?;
            Ok(TemporalPoint::Timestamp(timestamp))
        }
    }

    /// Extract timestamp from string (handles various formats)
    fn extract_timestamp(&self, s: &str) -> Result<chrono::DateTime<chrono::Utc>> {
        let cleaned = s.trim().trim_matches('\'').trim_matches('"');

        // Try ISO 8601
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(cleaned) {
            return Ok(dt.with_timezone(&chrono::Utc));
        }

        // Try other common formats
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(cleaned, "%Y-%m-%d %H:%M:%S") {
            return Ok(chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc));
        }

        if let Ok(date) = chrono::NaiveDate::parse_from_str(cleaned, "%Y-%m-%d") {
            let dt = date.and_hms_opt(0, 0, 0).unwrap();
            return Ok(chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc));
        }

        Err(DriftError::InvalidQuery(format!(
            "Invalid timestamp: {}",
            s
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_as_of_current() {
        let parser = TemporalSqlParser::new();
        let result = parser
            .parse("SELECT * FROM users FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP")
            .unwrap();

        assert!(matches!(
            result.system_time,
            Some(SystemTimeClause::AsOf(TemporalPoint::CurrentTimestamp))
        ));
    }

    #[test]
    fn test_parse_as_of_timestamp() {
        let parser = TemporalSqlParser::new();
        let result = parser.parse(
            "SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-15T10:30:00Z' WHERE status = 'pending'"
        ).unwrap();

        assert!(result.system_time.is_some());
    }

    #[test]
    fn test_parse_as_of_sequence() {
        let parser = TemporalSqlParser::new();
        let result = parser
            .parse("SELECT * FROM events FOR SYSTEM_TIME AS OF @SEQ:12345")
            .unwrap();

        assert!(matches!(
            result.system_time,
            Some(SystemTimeClause::AsOf(TemporalPoint::Sequence(12345)))
        ));
    }

    #[test]
    fn test_parse_all() {
        let parser = TemporalSqlParser::new();
        let result = parser
            .parse("SELECT * FROM audit_log FOR SYSTEM_TIME ALL")
            .unwrap();

        assert!(matches!(result.system_time, Some(SystemTimeClause::All)));
    }

    #[test]
    fn test_standard_sql_unchanged() {
        let parser = TemporalSqlParser::new();
        let result = parser.parse("SELECT * FROM users WHERE id = 1").unwrap();

        assert!(result.system_time.is_none());
    }
}
