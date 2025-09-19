//! SQL:2011 Temporal Query Executor

use sqlparser::ast::{Statement, Query, Select, SelectItem, TableWithJoins, SetExpr};
use chrono::{DateTime, Utc};
use serde_json::json;
use std::collections::HashMap;

use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::events::{Event, EventType};

use super::{TemporalStatement, SystemTimeClause, TemporalQueryResult, TemporalMetadata, QueryResult};
use super::temporal::{TemporalSemantics, TemporalFilter, DriftDbPoint};

pub struct SqlExecutor<'a> {
    engine: &'a mut Engine,
}

impl<'a> SqlExecutor<'a> {
    pub fn new(engine: &'a mut Engine) -> Self {
        Self { engine }
    }

    /// Execute SQL and return simplified QueryResult
    pub fn execute_sql(&mut self, stmt: &TemporalStatement) -> Result<QueryResult> {
        match self.execute(stmt) {
            Ok(result) => {
                // Convert TemporalQueryResult to QueryResult
                if result.rows.is_empty() {
                    Ok(QueryResult::Success {
                        message: "Query executed successfully".to_string()
                    })
                } else {
                    // Extract columns from first row
                    let columns = if let Some(first) = result.rows.first() {
                        if let serde_json::Value::Object(map) = first {
                            map.keys().cloned().collect()
                        } else {
                            vec!["value".to_string()]
                        }
                    } else {
                        vec![]
                    };

                    // Convert rows to arrays
                    let rows: Vec<Vec<serde_json::Value>> = result.rows.into_iter().map(|row| {
                        if let serde_json::Value::Object(map) = row {
                            columns.iter().map(|col| {
                                map.get(col).cloned().unwrap_or(serde_json::Value::Null)
                            }).collect()
                        } else {
                            vec![row]
                        }
                    }).collect();

                    Ok(QueryResult::Records { columns, rows })
                }
            }
            Err(e) => Ok(QueryResult::Error {
                message: format!("{}", e)
            })
        }
    }

    /// Execute a temporal SQL statement
    pub fn execute(&mut self, stmt: &TemporalStatement) -> Result<TemporalQueryResult> {
        match &stmt.statement {
            Statement::Query(query) => self.execute_query(query, &stmt.system_time),

            Statement::Insert { table_name, columns, source, .. } => {
                self.execute_insert(table_name, columns, source)
            }

            Statement::Update { table, assignments, selection, .. } => {
                self.execute_update(table, assignments, selection)
            }

            Statement::Delete { tables, selection, .. } => {
                self.execute_delete(tables, selection)
            }

            Statement::CreateTable { name, columns, constraints, .. } => {
                self.execute_create_table(name, columns, constraints)
            }

            _ => Err(DriftError::InvalidQuery(
                "Unsupported SQL statement type".to_string()
            )),
        }
    }

    /// Execute a SELECT query with temporal support
    fn execute_query(
        &mut self,
        query: &Box<Query>,
        system_time: &Option<SystemTimeClause>,
    ) -> Result<TemporalQueryResult> {
        // Extract query components
        let (table_name, where_clause) = self.extract_query_components(query)?;

        // Apply temporal filter if specified
        let rows = if let Some(temporal) = system_time {
            self.query_with_temporal(&table_name, temporal, where_clause)?
        } else {
            self.query_current(&table_name, where_clause)?
        };

        // Build temporal metadata
        let metadata = system_time.as_ref().map(|st| {
            let (as_of_ts, as_of_seq) = match st {
                SystemTimeClause::AsOf(point) => {
                    match TemporalSemantics::to_driftdb_point(point).ok() {
                        Some(DriftDbPoint::Timestamp(ts)) => (Some(ts), None),
                        Some(DriftDbPoint::Sequence(seq)) => (None, Some(seq)),
                        None => (None, None),
                    }
                }
                _ => (None, None),
            };

            TemporalMetadata {
                as_of_timestamp: as_of_ts,
                as_of_sequence: as_of_seq,
                versions_scanned: rows.len(),
            }
        });

        Ok(TemporalQueryResult {
            rows,
            temporal_metadata: metadata,
        })
    }

    /// Query with temporal clause
    fn query_with_temporal(
        &mut self,
        table: &str,
        temporal: &SystemTimeClause,
        _where_clause: Option<String>,
    ) -> Result<Vec<serde_json::Value>> {
        match temporal {
            SystemTimeClause::AsOf(point) => {
                let drift_point = TemporalSemantics::to_driftdb_point(point)?;
                self.query_as_of(table, drift_point)
            }

            SystemTimeClause::Between { start, end } => {
                let start_point = TemporalSemantics::to_driftdb_point(start)?;
                let end_point = TemporalSemantics::to_driftdb_point(end)?;
                self.query_between(table, start_point, end_point, true)
            }

            SystemTimeClause::FromTo { start, end } => {
                let start_point = TemporalSemantics::to_driftdb_point(start)?;
                let end_point = TemporalSemantics::to_driftdb_point(end)?;
                self.query_between(table, start_point, end_point, false)
            }

            SystemTimeClause::All => {
                self.query_all_versions(table)
            }
        }
    }

    /// Query as of a specific point
    fn query_as_of(&mut self, table: &str, point: DriftDbPoint) -> Result<Vec<serde_json::Value>> {
        // Get the storage for this table
        let storage = self.engine.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        // Determine sequence number from point
        let sequence = match point {
            DriftDbPoint::Sequence(seq) => Some(seq),
            DriftDbPoint::Timestamp(ts) => {
                // Find sequence number for timestamp
                // In production, this would use an index
                storage.find_sequence_at_timestamp(ts)?
            }
        };

        // Reconstruct state at sequence
        let state = storage.reconstruct_state_at(sequence)?;

        // Convert to JSON values
        Ok(state.into_values().collect())
    }

    /// Query between two points
    fn query_between(
        &mut self,
        table: &str,
        start: DriftDbPoint,
        end: DriftDbPoint,
        inclusive: bool,
    ) -> Result<Vec<serde_json::Value>> {
        let storage = self.engine.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        // Get all events in range
        let events = storage.read_all_events()?;

        // Filter by time range
        let filtered: Vec<_> = events.into_iter()
            .filter(|event| {
                // Check if event is in temporal range
                // This is simplified - production would be more sophisticated
                true
            })
            .map(|event| event.payload)
            .collect();

        Ok(filtered)
    }

    /// Query all versions
    fn query_all_versions(&mut self, table: &str) -> Result<Vec<serde_json::Value>> {
        let storage = self.engine.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        let events = storage.read_all_events()?;
        Ok(events.into_iter().map(|e| e.payload).collect())
    }

    /// Query current state (no temporal clause)
    fn query_current(&mut self, table: &str, _where_clause: Option<String>) -> Result<Vec<serde_json::Value>> {
        let storage = self.engine.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        let state = storage.reconstruct_state_at(None)?;
        Ok(state.into_values().collect())
    }

    /// Execute INSERT statement
    fn execute_insert(
        &mut self,
        table_name: &sqlparser::ast::ObjectName,
        columns: &Vec<sqlparser::ast::Ident>,
        source: &Option<Box<Query>>,
    ) -> Result<TemporalQueryResult> {
        let table = table_name.to_string();

        // For simplicity, assume VALUES clause
        // In production, would handle all source types
        let values = self.extract_insert_values(source)?;

        // Create insert event
        let primary_key = json!("generated_id"); // Would extract from schema
        let event = Event::new_insert(table, primary_key, values);

        // Apply event
        let sequence = self.engine.apply_event(event)?;

        Ok(TemporalQueryResult {
            rows: vec![json!({
                "sequence": sequence,
                "message": "Row inserted"
            })],
            temporal_metadata: None,
        })
    }

    /// Execute UPDATE statement
    fn execute_update(
        &mut self,
        table: &sqlparser::ast::TableWithJoins,
        assignments: &Vec<sqlparser::ast::Assignment>,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> Result<TemporalQueryResult> {
        // Extract table name
        let table_name = self.extract_table_name(table)?;

        // Build update payload
        let mut updates = serde_json::Map::new();
        for assignment in assignments {
            let column = assignment.id.iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".");

            let value = self.expr_to_json(&assignment.value)?;
            updates.insert(column, value);
        }

        // For now, update all rows (would apply WHERE in production)
        let primary_key = json!("dummy_key");
        let event = Event::new_patch(table_name, primary_key, json!(updates));

        let sequence = self.engine.apply_event(event)?;

        Ok(TemporalQueryResult {
            rows: vec![json!({
                "sequence": sequence,
                "message": "Rows updated"
            })],
            temporal_metadata: None,
        })
    }

    /// Execute DELETE statement
    fn execute_delete(
        &mut self,
        tables: &Vec<sqlparser::ast::ObjectName>,
        selection: &Option<sqlparser::ast::Expr>,
    ) -> Result<TemporalQueryResult> {
        if tables.is_empty() {
            return Err(DriftError::InvalidQuery("No table specified".to_string()));
        }

        let table_name = tables[0].to_string();

        // For now, soft delete a dummy key (would apply WHERE in production)
        let primary_key = json!("dummy_key");
        let event = Event::new_soft_delete(table_name, primary_key);

        let sequence = self.engine.apply_event(event)?;

        Ok(TemporalQueryResult {
            rows: vec![json!({
                "sequence": sequence,
                "message": "Rows deleted"
            })],
            temporal_metadata: None,
        })
    }

    /// Execute CREATE TABLE statement
    fn execute_create_table(
        &mut self,
        name: &sqlparser::ast::ObjectName,
        columns: &Vec<sqlparser::ast::ColumnDef>,
        constraints: &Vec<sqlparser::ast::TableConstraint>,
    ) -> Result<TemporalQueryResult> {
        let table_name = name.to_string();

        // Extract primary key from constraints
        let primary_key = self.extract_primary_key(constraints)?;

        // Extract indexed columns
        let indexed_columns = self.extract_indexes(constraints);

        // Create table in engine
        self.engine.create_table(&table_name, &primary_key, indexed_columns)?;

        Ok(TemporalQueryResult {
            rows: vec![json!({
                "message": format!("Table {} created with system versioning", table_name)
            })],
            temporal_metadata: None,
        })
    }

    // Helper methods

    fn extract_query_components(&self, query: &Box<Query>) -> Result<(String, Option<String>)> {
        // Extract table name and WHERE clause from query
        // This is simplified - production would handle all query types

        match &query.body.as_ref() {
            SetExpr::Select(select) => {
                if select.from.is_empty() {
                    return Err(DriftError::InvalidQuery("No FROM clause".to_string()));
                }

                let table_name = self.extract_table_name(&select.from[0])?;
                let where_clause = select.selection.as_ref()
                    .map(|expr| format!("{}", expr));

                Ok((table_name, where_clause))
            }
            _ => Err(DriftError::InvalidQuery("Unsupported query type".to_string())),
        }
    }

    fn extract_table_name(&self, table: &TableWithJoins) -> Result<String> {
        use sqlparser::ast::TableFactor;

        match &table.relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(DriftError::InvalidQuery("Complex table expressions not supported".to_string())),
        }
    }

    fn extract_primary_key(&self, constraints: &Vec<sqlparser::ast::TableConstraint>) -> Result<String> {
        for constraint in constraints {
            // Check for primary key constraint
            match constraint {
                sqlparser::ast::TableConstraint::Unique { columns, is_primary, .. } if *is_primary => {
                    if !columns.is_empty() {
                        return Ok(columns[0].value.clone());
                    }
                }
                _ => continue,
            }
        }
        Err(DriftError::InvalidQuery("No primary key defined".to_string()))
    }

    fn extract_indexes(&self, constraints: &Vec<sqlparser::ast::TableConstraint>) -> Vec<String> {
        // Extract indexed columns from constraints
        // In production would handle all constraint types
        vec![]
    }

    fn extract_insert_values(&self, source: &Option<Box<Query>>) -> Result<serde_json::Value> {
        // Extract VALUES from INSERT
        // This is simplified - production would handle all source types
        Ok(json!({}))
    }

    fn expr_to_json(&self, expr: &sqlparser::ast::Expr) -> Result<serde_json::Value> {
        use sqlparser::ast::Expr;

        match expr {
            Expr::Value(val) => self.value_to_json(val),
            Expr::Identifier(ident) => Ok(json!(ident.value.clone())),
            _ => Ok(json!(null)),
        }
    }

    fn value_to_json(&self, val: &sqlparser::ast::Value) -> Result<serde_json::Value> {
        use sqlparser::ast::Value;

        match val {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(json!(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(json!(f))
                } else {
                    Ok(json!(n.clone()))
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(json!(s)),
            Value::Boolean(b) => Ok(json!(b)),
            Value::Null => Ok(json!(null)),
            _ => Ok(json!(val.to_string())),
        }
    }
}