use serde_json::json;

use crate::engine::Engine;
use crate::errors::Result;
use crate::events::Event;
use super::{AsOf, Query, QueryResult, WhereCondition};

impl Engine {
    pub fn execute_query(&mut self, query: Query) -> Result<QueryResult> {
        match query {
            Query::CreateTable { name, primary_key, indexed_columns } => {
                self.create_table(&name, &primary_key, indexed_columns)?;
                Ok(QueryResult::Success {
                    message: format!("Table '{}' created", name),
                })
            }
            Query::Insert { table, data } => {
                let pk_field = self.get_table_primary_key(&table)?;
                let primary_key = data.get(&pk_field)
                    .ok_or_else(|| crate::errors::DriftError::InvalidQuery(
                        format!("Missing primary key field '{}'", pk_field)
                    ))?
                    .clone();

                let event = Event::new_insert(table.clone(), primary_key, data);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Inserted with sequence {}", seq),
                })
            }
            Query::Patch { table, primary_key, updates } => {
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
            Query::Select { table, conditions, as_of, limit } => {
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
        let storage = self.tables.get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

        let sequence = match as_of {
            Some(AsOf::Sequence(seq)) => Some(seq),
            Some(AsOf::Timestamp(ts)) => {
                let events = storage.read_all_events()?;
                events.iter()
                    .filter(|e| e.timestamp <= ts)
                    .map(|e| e.sequence)
                    .max()
            }
            Some(AsOf::Now) | None => None,
        };

        let state = storage.reconstruct_state_at(sequence)?;

        let mut results: Vec<serde_json::Value> = state
            .into_iter()
            .filter_map(|(_, row)| {
                if Self::matches_conditions(&row, &conditions) {
                    Some(row)
                } else {
                    None
                }
            })
            .collect();

        if let Some(limit) = limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    fn matches_conditions(row: &serde_json::Value, conditions: &[WhereCondition]) -> bool {
        conditions.iter().all(|cond| {
            if let serde_json::Value::Object(map) = row {
                if let Some(field_value) = map.get(&cond.column) {
                    field_value == &cond.value
                } else {
                    false
                }
            } else {
                false
            }
        })
    }

    fn get_drift_history(&self, table: &str, primary_key: serde_json::Value) -> Result<Vec<serde_json::Value>> {
        let storage = self.tables.get(table)
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

    fn get_table_primary_key(&self, table: &str) -> Result<String> {
        let storage = self.tables.get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;
        Ok(storage.schema().primary_key.clone())
    }
}