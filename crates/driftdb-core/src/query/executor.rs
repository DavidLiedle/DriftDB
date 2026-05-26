use serde_json::json;

use super::{AsOf, Query, QueryResult, WhereCondition};
use crate::engine::Engine;
use crate::errors::Result;
use crate::events::Event;
use crate::parallel::{ParallelConfig, ParallelExecutor};

impl Engine {
    pub fn execute_query(&mut self, query: Query) -> Result<QueryResult> {
        match query {
            Query::CreateTable {
                name,
                primary_key,
                indexed_columns,
            } => {
                self.create_table(&name, &primary_key, indexed_columns)?;
                Ok(QueryResult::Success {
                    message: format!("Table '{}' created", name),
                })
            }
            Query::Insert { table, data } => {
                let pk_field = self.get_table_primary_key(&table)?;
                let primary_key = data
                    .get(&pk_field)
                    .ok_or_else(|| {
                        crate::errors::DriftError::InvalidQuery(format!(
                            "Missing primary key field '{}'",
                            pk_field
                        ))
                    })?
                    .clone();

                // Check if primary key already exists
                let existing = self.select(
                    &table,
                    vec![super::WhereCondition {
                        column: pk_field.clone(),
                        operator: "=".to_string(),
                        value: primary_key.clone(),
                    }],
                    None,
                    Some(1),
                )?;

                if !existing.is_empty() {
                    return Err(crate::errors::DriftError::InvalidQuery(format!(
                        "Primary key violation: {} already exists",
                        primary_key
                    )));
                }

                let event = Event::new_insert(table.clone(), primary_key, data);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Inserted with sequence {}", seq),
                })
            }
            Query::Patch {
                table,
                primary_key,
                updates,
            } => {
                let event = Event::new_patch(table.clone(), primary_key, updates);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Patched with sequence {}", seq),
                })
            }
            Query::SoftDelete { table, primary_key } => {
                let event = Event::new_soft_delete(table.clone(), primary_key);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Soft deleted with sequence {}", seq),
                })
            }
            Query::Select {
                table,
                conditions,
                as_of,
                limit,
            } => {
                let rows = self.select(&table, conditions, as_of, limit)?;
                Ok(QueryResult::Rows { data: rows })
            }
            Query::ShowDrift { table, primary_key } => {
                let events = self.get_drift_history(&table, primary_key)?;
                Ok(QueryResult::DriftHistory { events })
            }
            Query::Snapshot { table } => {
                self.create_snapshot(&table)?;
                Ok(QueryResult::Success {
                    message: format!("Snapshot created for table '{}'", table),
                })
            }
            Query::Compact { table } => {
                self.compact_table(&table)?;
                Ok(QueryResult::Success {
                    message: format!("Table '{}' compacted", table),
                })
            }
        }
    }

    fn select(
        &self,
        table: &str,
        conditions: Vec<WhereCondition>,
        as_of: Option<AsOf>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        // EXPLAIN flows through `sql_bridge::execute_sql` → `crate::explain`
        // now; the legacy `Query::Explain` variant and its produced-then-
        // discarded plan are gone with the retired `crate::query::optimizer`
        // module. The select path proceeds directly to storage.
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

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

        let state = storage.reconstruct_state_at(sequence)?;

        // Use parallel execution for large datasets
        if state.len() > 1000 {
            // Create parallel executor with default config
            let parallel_executor = ParallelExecutor::new(ParallelConfig::default())?;

            // Convert state to format expected by parallel executor
            let data: Vec<(serde_json::Value, serde_json::Value)> = state
                .into_iter()
                .map(|(pk, row)| (serde_json::Value::String(pk), row))
                .collect();

            // Execute query in parallel
            parallel_executor.parallel_select(data, &conditions, limit)
        } else {
            // Use sequential execution for small datasets
            let mut results: Vec<serde_json::Value> = state
                .into_values()
                .filter(|row| super::predicate::matches_conditions(row, &conditions))
                .collect();

            if let Some(limit) = limit {
                results.truncate(limit);
            }

            Ok(results)
        }
    }

    fn get_drift_history(
        &self,
        table: &str,
        primary_key: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>> {
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

        let events = storage.read_all_events()?;
        let pk_str = primary_key.to_string();

        let history: Vec<serde_json::Value> = events
            .into_iter()
            .filter(|e| e.primary_key == pk_str)
            .map(|e| {
                json!({
                    "sequence": e.sequence,
                    "timestamp": e.timestamp.to_string(),
                    "event_type": format!("{:?}", e.event_type),
                    "payload": e.payload,
                })
            })
            .collect();

        Ok(history)
    }

    pub fn get_table_primary_key(&self, table: &str) -> Result<String> {
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;
        Ok(storage.schema().primary_key.clone())
    }

    pub fn get_table_columns(&self, table: &str) -> Result<Vec<String>> {
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

        // Get columns from schema
        let schema = storage.schema();

        // Return all column names from the schema
        Ok(schema.columns.iter().map(|c| c.name.clone()).collect())
    }

}
