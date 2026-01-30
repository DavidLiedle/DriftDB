//! SQL JOIN Implementation
//!
//! Provides support for complex JOIN operations including:
//! - INNER JOIN
//! - LEFT/RIGHT OUTER JOIN
//! - FULL OUTER JOIN
//! - CROSS JOIN
//! - Self joins
//! - Multi-table joins

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{json, Value};
use tracing::debug;

use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::parallel::{JoinType as ParallelJoinType, ParallelConfig, ParallelExecutor};
use crate::query::{AsOf, WhereCondition};

/// SQL JOIN types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

impl From<JoinType> for ParallelJoinType {
    fn from(jt: JoinType) -> Self {
        match jt {
            JoinType::Inner => ParallelJoinType::Inner,
            JoinType::LeftOuter => ParallelJoinType::LeftOuter,
            JoinType::RightOuter => ParallelJoinType::RightOuter,
            JoinType::FullOuter => ParallelJoinType::Full,
            JoinType::Cross => ParallelJoinType::Inner, // Cross join handled separately
        }
    }
}

/// JOIN condition
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_column: String,
    pub operator: String,
    pub right_column: String,
}

/// JOIN clause
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub conditions: Vec<JoinCondition>,
}

/// Multi-table JOIN query
#[derive(Debug, Clone)]
pub struct JoinQuery {
    pub base_table: String,
    pub base_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub select_columns: Vec<SelectColumn>,
    pub where_conditions: Vec<WhereCondition>,
    pub as_of: Option<AsOf>,
    pub limit: Option<usize>,
}

/// Column selection for JOIN results
#[derive(Debug, Clone)]
pub enum SelectColumn {
    All,
    TableAll(String), // table.*
    Column {
        table: Option<String>,
        column: String,
        alias: Option<String>,
    },
    Expression {
        expr: String,
        alias: String,
    },
}

/// JOIN executor
pub struct JoinExecutor {
    engine: Arc<Engine>,
    parallel_executor: Arc<ParallelExecutor>,
}

impl JoinExecutor {
    /// Create a new JOIN executor
    pub fn new(engine: Arc<Engine>) -> Result<Self> {
        let parallel_executor = Arc::new(ParallelExecutor::new(ParallelConfig::default())?);
        Ok(Self {
            engine,
            parallel_executor,
        })
    }

    /// Execute a JOIN query
    pub fn execute_join(&self, query: &JoinQuery) -> Result<Vec<Value>> {
        debug!("Executing JOIN query on base table: {}", query.base_table);

        // Load base table data
        let base_data = self.load_table_data(
            &query.base_table,
            query.base_alias.as_deref(),
            &query.where_conditions,
            query.as_of.clone(),
        )?;

        // If no joins, just return filtered base data
        if query.joins.is_empty() {
            return self.project_columns(base_data, &query.select_columns, query.limit);
        }

        // Execute joins sequentially (could be optimized with query planning)
        let mut result_data = base_data;

        for join_clause in &query.joins {
            result_data =
                self.execute_single_join(result_data, join_clause, query.as_of.clone())?;
        }

        // Project final columns and apply limit
        self.project_columns(result_data, &query.select_columns, query.limit)
    }

    /// Execute a single JOIN
    fn execute_single_join(
        &self,
        left_data: Vec<Value>,
        join_clause: &JoinClause,
        as_of: Option<AsOf>,
    ) -> Result<Vec<Value>> {
        debug!(
            "Executing {} JOIN with table: {}",
            format!("{:?}", join_clause.join_type),
            join_clause.table
        );

        // Load right table data
        let right_data = self.load_table_data(
            &join_clause.table,
            join_clause.alias.as_deref(),
            &[], // JOIN conditions applied during join, not during load
            as_of,
        )?;

        // Handle CROSS JOIN specially
        if join_clause.join_type == JoinType::Cross {
            return self.execute_cross_join(left_data, right_data);
        }

        // Validate join conditions
        if join_clause.conditions.is_empty() {
            return Err(DriftError::InvalidQuery(
                "JOIN requires at least one condition".to_string(),
            ));
        }

        // For now, support only single equi-join condition
        // TODO: Support multiple conditions and non-equi joins
        let condition = &join_clause.conditions[0];
        if condition.operator != "=" {
            return Err(DriftError::InvalidQuery(format!(
                "Unsupported JOIN operator: {}",
                condition.operator
            )));
        }

        // Execute join based on type
        match join_clause.join_type {
            JoinType::Inner => self.parallel_executor.parallel_join(
                left_data,
                right_data,
                ParallelJoinType::Inner,
                &condition.left_column,
                &condition.right_column,
            ),
            JoinType::LeftOuter => self.parallel_executor.parallel_join(
                left_data,
                right_data,
                ParallelJoinType::LeftOuter,
                &condition.left_column,
                &condition.right_column,
            ),
            JoinType::RightOuter => {
                // Swap tables for right outer join
                self.parallel_executor.parallel_join(
                    right_data,
                    left_data,
                    ParallelJoinType::LeftOuter,
                    &condition.right_column,
                    &condition.left_column,
                )
            }
            JoinType::FullOuter => {
                // Full outer join = left outer + right anti-join
                let left_outer = self.parallel_executor.parallel_join(
                    left_data.clone(),
                    right_data.clone(),
                    ParallelJoinType::LeftOuter,
                    &condition.left_column,
                    &condition.right_column,
                )?;

                // Get right rows that don't match
                let right_anti = self.execute_anti_join(
                    right_data,
                    left_data,
                    &condition.right_column,
                    &condition.left_column,
                )?;

                // Combine results
                let mut results = left_outer;
                results.extend(right_anti);
                Ok(results)
            }
            _ => unreachable!(),
        }
    }

    /// Execute CROSS JOIN (Cartesian product)
    fn execute_cross_join(
        &self,
        left_data: Vec<Value>,
        right_data: Vec<Value>,
    ) -> Result<Vec<Value>> {
        let mut results = Vec::with_capacity(left_data.len() * right_data.len());

        for left_row in &left_data {
            for right_row in &right_data {
                let mut combined = json!({});

                // Merge both rows
                if let (Value::Object(left_map), Value::Object(right_map)) = (left_row, right_row) {
                    for (k, v) in left_map {
                        combined[format!("left_{}", k)] = v.clone();
                    }
                    for (k, v) in right_map {
                        combined[format!("right_{}", k)] = v.clone();
                    }
                }

                results.push(combined);
            }
        }

        Ok(results)
    }

    /// Execute anti-join (rows from left that don't match right)
    fn execute_anti_join(
        &self,
        left_data: Vec<Value>,
        right_data: Vec<Value>,
        left_key: &str,
        right_key: &str,
    ) -> Result<Vec<Value>> {
        // Build hash set of right keys
        let right_keys: HashSet<String> = right_data
            .iter()
            .filter_map(|row| row.get(right_key))
            .map(|v| v.to_string())
            .collect();

        // Filter left rows that don't have matching keys
        let results = left_data
            .into_iter()
            .filter(|row| {
                row.get(left_key)
                    .map(|v| !right_keys.contains(&v.to_string()))
                    .unwrap_or(true)
            })
            .collect();

        Ok(results)
    }

    /// Load table data with optional filtering
    fn load_table_data(
        &self,
        table: &str,
        alias: Option<&str>,
        conditions: &[WhereCondition],
        as_of: Option<AsOf>,
    ) -> Result<Vec<Value>> {
        // Get table storage
        let storage = self
            .engine
            .tables
            .get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        // Determine sequence for temporal query
        let sequence = match as_of {
            Some(AsOf::Sequence(seq)) => Some(seq),
            Some(AsOf::Timestamp(ts)) => {
                let events = storage.read_all_events()?;
                events
                    .iter()
                    .filter(|e| e.timestamp <= ts)
                    .map(|e| e.sequence)
                    .max()
            }
            Some(AsOf::Now) | None => None,
        };

        // Reconstruct state at sequence
        let state = storage.reconstruct_state_at(sequence)?;

        // Convert to Value and apply alias if needed
        let mut rows: Vec<Value> = state
            .into_values()
            .map(|row| {
                // Add table prefix to columns if alias provided
                if let Some(alias) = alias {
                    if let Value::Object(map) = row {
                        let mut aliased_row = serde_json::Map::new();
                        for (k, v) in map {
                            aliased_row.insert(format!("{}.{}", alias, k), v);
                        }
                        Value::Object(aliased_row)
                    } else {
                        row
                    }
                } else {
                    row
                }
            })
            .collect();

        // Apply WHERE conditions if any
        if !conditions.is_empty() {
            rows.retain(|row| self.matches_conditions(row, conditions));
        }

        Ok(rows)
    }

    /// Project specific columns from result set
    fn project_columns(
        &self,
        data: Vec<Value>,
        columns: &[SelectColumn],
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for row in data {
            let mut projected_row = json!({});

            for col in columns {
                match col {
                    SelectColumn::All => {
                        // Include all columns
                        if let Value::Object(map) = &row {
                            for (k, v) in map {
                                projected_row[k] = v.clone();
                            }
                        }
                    }
                    SelectColumn::TableAll(table) => {
                        // Include all columns from specific table
                        if let Value::Object(map) = &row {
                            let prefix = format!("{}.", table);
                            for (k, v) in map {
                                if k.starts_with(&prefix)
                                    || k.starts_with(&format!("{}_{}", table, ""))
                                {
                                    projected_row[k] = v.clone();
                                }
                            }
                        }
                    }
                    SelectColumn::Column {
                        table,
                        column,
                        alias,
                    } => {
                        // Include specific column
                        let col_name = if let Some(t) = table {
                            format!("{}.{}", t, column)
                        } else {
                            column.clone()
                        };

                        if let Some(value) = row.get(&col_name) {
                            let output_name = alias.as_ref().unwrap_or(column);
                            projected_row[output_name] = value.clone();
                        }
                    }
                    SelectColumn::Expression { expr: _expr, alias } => {
                        // TODO: Implement expression evaluation
                        // For now, just return null
                        projected_row[alias] = Value::Null;
                    }
                }
            }

            results.push(projected_row);

            // Apply limit if specified
            if let Some(limit) = limit {
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Check if a row matches WHERE conditions
    fn matches_conditions(&self, row: &Value, conditions: &[WhereCondition]) -> bool {
        conditions.iter().all(|cond| {
            if let Some(field_value) = row.get(&cond.column) {
                match cond.operator.as_str() {
                    "=" | "==" => field_value == &cond.value,
                    "!=" | "<>" => field_value != &cond.value,
                    _ => false, // TODO: Support more operators
                }
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_query_parsing() {
        let query = JoinQuery {
            base_table: "users".to_string(),
            base_alias: Some("u".to_string()),
            joins: vec![JoinClause {
                join_type: JoinType::Inner,
                table: "orders".to_string(),
                alias: Some("o".to_string()),
                conditions: vec![JoinCondition {
                    left_column: "u.id".to_string(),
                    operator: "=".to_string(),
                    right_column: "o.user_id".to_string(),
                }],
            }],
            select_columns: vec![
                SelectColumn::Column {
                    table: Some("u".to_string()),
                    column: "name".to_string(),
                    alias: None,
                },
                SelectColumn::Column {
                    table: Some("o".to_string()),
                    column: "total".to_string(),
                    alias: Some("order_total".to_string()),
                },
            ],
            where_conditions: vec![],
            as_of: None,
            limit: Some(10),
        };

        assert_eq!(query.base_table, "users");
        assert_eq!(query.joins.len(), 1);
        assert_eq!(query.select_columns.len(), 2);
    }

    #[test]
    fn test_join_type_conversion() {
        assert_eq!(
            ParallelJoinType::from(JoinType::Inner),
            ParallelJoinType::Inner
        );
        assert_eq!(
            ParallelJoinType::from(JoinType::LeftOuter),
            ParallelJoinType::LeftOuter
        );
    }
}
