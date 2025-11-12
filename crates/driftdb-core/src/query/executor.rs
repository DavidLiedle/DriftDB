use serde_json::json;
use std::path::{Path, PathBuf};
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
                compression,
            } => {
                // Verify table exists
                if !self.tables.contains_key(&table) {
                    return Ok(QueryResult::Error {
                        message: format!("Table '{}' not found", table),
                    });
                }

                // Create a BackupManager and backup just this table
                let metrics = Arc::new(Metrics::new());
                let _backup_manager = BackupManager::new(self.base_path(), metrics);

                // Create backup directory
                std::fs::create_dir_all(&destination).map_err(|e| {
                    crate::errors::DriftError::Other(format!("Failed to create backup directory: {}", e))
                })?;

                // Call the private backup_table_full method via a new public wrapper
                // For now, we'll use the same approach as full backup but only for one table
                let src_table_dir = self.base_path().join("tables").join(&table);
                let dst_table_dir = PathBuf::from(&destination).join("tables").join(&table);

                std::fs::create_dir_all(&dst_table_dir).map_err(|e| {
                    crate::errors::DriftError::Other(format!("Failed to create table backup directory: {}", e))
                })?;

                // Copy all table files
                let mut files_copied = 0;
                if src_table_dir.exists() {
                    for entry in std::fs::read_dir(&src_table_dir)? {
                        let entry = entry?;
                        let src_path = entry.path();
                        let file_name = entry.file_name();
                        let dst_path = dst_table_dir.join(file_name);

                        if src_path.is_file() {
                            std::fs::copy(&src_path, &dst_path)?;
                            files_copied += 1;
                        } else if src_path.is_dir() {
                            // Recursively copy directories (like segments/)
                            Self::copy_dir_recursive(&src_path, &dst_path)?;
                            files_copied += 1;
                        }
                    }
                }

                // Create simple metadata
                let metadata = serde_json::json!({
                    "table": table,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "compression": format!("{:?}", compression),
                    "files_copied": files_copied,
                });

                let metadata_path = PathBuf::from(&destination).join("metadata.json");
                std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;

                Ok(QueryResult::Success {
                    message: format!("Table '{}' backed up to '{}' ({} files)", table, destination, files_copied),
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
                table,
                source,
                target,
                verify,
            } => {
                // Verify source backup exists
                let source_path = PathBuf::from(&source);
                if !source_path.exists() {
                    return Ok(QueryResult::Error {
                        message: format!("Backup source '{}' not found", source),
                    });
                }

                // Read metadata if available
                let metadata_path = source_path.join("metadata.json");
                if metadata_path.exists() {
                    let metadata_content = std::fs::read_to_string(&metadata_path)?;
                    let metadata: serde_json::Value = serde_json::from_str(&metadata_content)?;

                    // Verify it's the right table
                    if let Some(backup_table) = metadata.get("table").and_then(|t| t.as_str()) {
                        if backup_table != table {
                            return Ok(QueryResult::Error {
                                message: format!("Backup is for table '{}', but trying to restore '{}'", backup_table, table),
                            });
                        }
                    }
                }

                // Determine target table name
                let target_table = target.as_deref().unwrap_or(&table);

                // Check if target table already exists
                if self.tables.contains_key(target_table) {
                    return Ok(QueryResult::Error {
                        message: format!("Target table '{}' already exists. Drop it first or use a different target name.", target_table),
                    });
                }

                // Restore the table files
                let src_table_dir = source_path.join("tables").join(&table);
                if !src_table_dir.exists() {
                    return Ok(QueryResult::Error {
                        message: format!("Table '{}' not found in backup", table),
                    });
                }

                let dst_table_dir = self.base_path().join("tables").join(target_table);

                // Verify backup integrity if requested
                if verify {
                    // Basic verification: check if required files exist
                    let schema_file = src_table_dir.join("schema.json");
                    if !schema_file.exists() {
                        return Ok(QueryResult::Error {
                            message: "Backup verification failed: schema.json not found".to_string(),
                        });
                    }
                }

                // Copy all table files
                Self::copy_dir_recursive(&src_table_dir, &dst_table_dir)?;

                // Reload the table into the engine
                // Note: This requires the Engine to be mutable, which it is in execute_query
                // We'll need to use interior mutability or restructure this
                // For now, return success and note that engine restart may be needed
                Ok(QueryResult::Success {
                    message: format!(
                        "Table '{}' restored to '{}'. Restart the engine or reload the table to use it.",
                        table,
                        target_table
                    ),
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

    /// Recursively copy a directory
    fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
        std::fs::create_dir_all(dst)?;

        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());

            if src_path.is_file() {
                std::fs::copy(&src_path, &dst_path)?;
            } else if src_path.is_dir() {
                Self::copy_dir_recursive(&src_path, &dst_path)?;
            }
        }

        Ok(())
    }
}
