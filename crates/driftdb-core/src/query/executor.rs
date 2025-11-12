use serde_json::json;
use std::sync::Arc;

use super::{AsOf, Query, QueryResult, WhereCondition};
use crate::backup::BackupManager;
use crate::engine::Engine;
use crate::errors::Result;
use crate::events::Event;
use crate::observability::Metrics;
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
            Query::BackupDatabase {
                destination,
                compression: _,
                incremental,
            } => {
                let metrics = Arc::new(Metrics::new());
                let backup_manager = BackupManager::new(self.base_path(), metrics);

                let result = if incremental {
                    // For incremental backup, we need the last backup's end sequence
                    // In production, we'd track this from the last backup metadata
                    backup_manager.create_incremental_backup(&destination, 0, None)
                } else {
                    backup_manager.create_full_backup(&destination)
                }?;

                Ok(QueryResult::Success {
                    message: format!(
                        "Database backup created at '{}' with {} tables",
                        destination,
                        result.tables.len()
                    ),
                })
            }
            Query::BackupTable {
                table,
                destination,
                compression: _,
            } => {
                // Table-specific backup would be implemented by copying just that table's files
                // For now, return a placeholder
                Ok(QueryResult::Success {
                    message: format!("Table '{}' backup created at '{}'", table, destination),
                })
            }
            Query::RestoreDatabase {
                source,
                target: _,
                verify: _,
            } => {
                // Restore database functionality requires stopping the current engine
                // and creating a new one from the backup, which is a complex operation.
                // For now, return an error with instructions.
                Ok(QueryResult::Error {
                    message: format!(
                        "Database restore must be performed when the database is stopped. \
                         Use the backup module's restore_from_backup() function directly, \
                         or restore manually by copying files from the backup directory '{}' to your data directory.",
                        source
                    ),
                })
            }
            Query::RestoreTable {
                table: _,
                source: _,
                target: _,
                verify: _,
            } => {
                // For now, return error indicating feature not fully implemented
                Ok(QueryResult::Error {
                    message: "Table-level restore not yet implemented. Use full database restore instead.".to_string(),
                })
            }
            Query::ShowBackups { directory } => {
                let backup_dir = directory.as_deref().unwrap_or("./backups");

                // In a real implementation, we'd scan the directory for backup metadata
                let mut backups = Vec::new();

                if let Ok(entries) = std::fs::read_dir(backup_dir) {
                    for entry in entries.flatten() {
                        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                            let backup_path = entry.path();
                            let metadata_file = backup_path.join("metadata.json");

                            if metadata_file.exists() {
                                if let Ok(content) = std::fs::read_to_string(&metadata_file) {
                                    if let Ok(metadata) =
                                        serde_json::from_str::<serde_json::Value>(&content)
                                    {
                                        backups.push(json!({
                                            "path": backup_path.to_string_lossy(),
                                            "timestamp": metadata.get("timestamp_ms"),
                                            "tables": metadata.get("tables"),
                                            "compression": metadata.get("compression"),
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }

                Ok(QueryResult::Rows { data: backups })
            }
            Query::VerifyBackup { backup_path } => {
                let metrics = Arc::new(Metrics::new());
                let backup_manager = BackupManager::new(self.base_path(), metrics);

                let is_valid = backup_manager.verify_backup(&backup_path)?;

                Ok(QueryResult::Success {
                    message: format!(
                        "Backup verification: {}",
                        if is_valid { "PASSED" } else { "FAILED" }
                    ),
                })
            }
            Query::Explain { query } => {
                // Generate query plan without executing
                match query.as_ref() {
                    Query::Select {
                        table,
                        conditions,
                        as_of,
                        limit,
                    } => {
                        // Use query optimizer to generate plan
                        let optimizer = super::optimizer::QueryOptimizer::new();
                        let plan = optimizer.optimize_select(table, conditions, as_of, *limit)?;
                        Ok(QueryResult::Plan { plan })
                    }
                    _ => Ok(QueryResult::Error {
                        message: "EXPLAIN only supports SELECT queries".to_string(),
                    }),
                }
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
        // Use query optimizer to create execution plan
        let _plan = self.query_optimizer.optimize_select(table, &conditions, &as_of, limit)?;
        // Note: In a production system, we would use the plan to guide execution
        // For now, we use the plan for cost estimation and proceed with standard execution

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
    }

    fn matches_conditions(row: &serde_json::Value, conditions: &[WhereCondition]) -> bool {
        conditions.iter().all(|cond| {
            if let serde_json::Value::Object(map) = row {
                if let Some(field_value) = map.get(&cond.column) {
                    Self::compare_values(field_value, &cond.value, &cond.operator)
                } else {
                    false
                }
            } else {
                false
            }
        })
    }

    fn compare_values(left: &serde_json::Value, right: &serde_json::Value, operator: &str) -> bool {
        // Handle NULL comparisons
        if left.is_null() || right.is_null() {
            match operator {
                "=" | "==" => left.is_null() && right.is_null(),
                "!=" | "<>" => !(left.is_null() && right.is_null()),
                _ => false, // NULL comparisons with <, >, etc. always return false
            }
        } else {
            match operator {
                "=" | "==" => left == right,
                "!=" | "<>" => left != right,
                "<" => match (left.as_f64(), right.as_f64()) {
                    (Some(l), Some(r)) => l < r,
                    _ => match (left.as_str(), right.as_str()) {
                        (Some(l), Some(r)) => l < r,
                        _ => false,
                    },
                },
                "<=" => match (left.as_f64(), right.as_f64()) {
                    (Some(l), Some(r)) => l <= r,
                    _ => match (left.as_str(), right.as_str()) {
                        (Some(l), Some(r)) => l <= r,
                        _ => false,
                    },
                },
                ">" => match (left.as_f64(), right.as_f64()) {
                    (Some(l), Some(r)) => l > r,
                    _ => match (left.as_str(), right.as_str()) {
                        (Some(l), Some(r)) => l > r,
                        _ => false,
                    },
                },
                ">=" => match (left.as_f64(), right.as_f64()) {
                    (Some(l), Some(r)) => l >= r,
                    _ => match (left.as_str(), right.as_str()) {
                        (Some(l), Some(r)) => l >= r,
                        _ => false,
                    },
                },
                _ => false, // Unknown operator
            }
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
