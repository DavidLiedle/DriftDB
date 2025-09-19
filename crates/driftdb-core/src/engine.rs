use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::index::IndexManager;
use crate::query::{Query, QueryResult};
use crate::schema::{ColumnDef, Schema};
use crate::snapshot::SnapshotManager;
use crate::storage::{Segment, TableStorage};
use crate::transaction::{IsolationLevel, TransactionManager};

pub struct Engine {
    base_path: PathBuf,
    pub(crate) tables: HashMap<String, Arc<TableStorage>>,
    indexes: HashMap<String, Arc<RwLock<IndexManager>>>,
    snapshots: HashMap<String, Arc<SnapshotManager>>,
    transaction_manager: Arc<RwLock<TransactionManager>>,
}

impl Engine {
    pub fn open<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();

        if !base_path.exists() {
            return Err(DriftError::Other(format!(
                "Database path does not exist: {}",
                base_path.display()
            )));
        }

        let mut engine = Self {
            base_path: base_path.clone(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            snapshots: HashMap::new(),
            transaction_manager: Arc::new(RwLock::new(TransactionManager::new())),
        };

        let tables_dir = base_path.join("tables");
        if tables_dir.exists() {
            for entry in fs::read_dir(&tables_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let table_name = entry.file_name().to_string_lossy().to_string();
                    engine.load_table(&table_name)?;
                }
            }
        }

        Ok(engine)
    }

    pub fn init<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        fs::create_dir_all(base_path.join("tables"))?;

        Ok(Self {
            base_path,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            snapshots: HashMap::new(),
            transaction_manager: Arc::new(RwLock::new(TransactionManager::new())),
        })
    }

    fn load_table(&mut self, table_name: &str) -> Result<()> {
        let storage = Arc::new(TableStorage::open(&self.base_path, table_name)?);

        let mut index_mgr = IndexManager::new(storage.path());
        index_mgr.load_indexes(&storage.schema().indexed_columns())?;

        let snapshot_mgr = SnapshotManager::new(storage.path());

        self.tables.insert(table_name.to_string(), storage.clone());
        self.indexes.insert(table_name.to_string(), Arc::new(RwLock::new(index_mgr)));
        self.snapshots.insert(table_name.to_string(), Arc::new(snapshot_mgr));

        Ok(())
    }

    pub fn create_table(
        &mut self,
        name: &str,
        primary_key: &str,
        indexed_columns: Vec<String>,
    ) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(DriftError::Other(format!("Table '{}' already exists", name)));
        }

        let mut columns = vec![ColumnDef {
            name: primary_key.to_string(),
            col_type: "string".to_string(),
            index: false,
        }];

        for col in &indexed_columns {
            if col != primary_key {
                columns.push(ColumnDef {
                    name: col.clone(),
                    col_type: "string".to_string(),
                    index: true,
                });
            }
        }

        let schema = Schema::new(name.to_string(), primary_key.to_string(), columns);
        schema.validate()?;

        let storage = Arc::new(TableStorage::create(&self.base_path, schema.clone())?);

        let mut index_mgr = IndexManager::new(storage.path());
        index_mgr.load_indexes(&schema.indexed_columns())?;

        let snapshot_mgr = SnapshotManager::new(storage.path());

        self.tables.insert(name.to_string(), storage);
        self.indexes.insert(name.to_string(), Arc::new(RwLock::new(index_mgr)));
        self.snapshots.insert(name.to_string(), Arc::new(snapshot_mgr));

        Ok(())
    }

    pub fn apply_event(&mut self, event: Event) -> Result<u64> {
        let storage = self
            .tables
            .get(&event.table_name)
            .ok_or_else(|| DriftError::TableNotFound(event.table_name.clone()))?
            .clone();

        let sequence = storage.append_event(event.clone())?;

        if let Some(index_mgr) = self.indexes.get(&event.table_name) {
            let mut index_mgr = index_mgr.write();
            index_mgr.update_indexes(&event, &storage.schema().indexed_columns())?;
            index_mgr.save_all()?;
        }

        Ok(sequence)
    }

    pub fn create_snapshot(&self, table_name: &str) -> Result<()> {
        let storage = self
            .tables
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let snapshot_mgr = self
            .snapshots
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let meta = storage.path().join("meta.json");
        let table_meta = crate::storage::TableMeta::load_from_file(meta)?;

        snapshot_mgr.create_snapshot(storage, table_meta.last_sequence)?;

        Ok(())
    }

    pub fn compact_table(&self, table_name: &str) -> Result<()> {
        let storage = self
            .tables
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let snapshot_mgr = self
            .snapshots
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let snapshots = snapshot_mgr.list_snapshots()?;
        if snapshots.is_empty() {
            return Err(DriftError::Other("No snapshots available for compaction".into()));
        }

        let latest_snapshot_seq = *snapshots.last()
            .ok_or_else(|| DriftError::Other("Snapshots list unexpectedly empty".into()))?;
        let latest_snapshot = snapshot_mgr
            .find_latest_before(u64::MAX)?
            .ok_or_else(|| DriftError::Other("Failed to load snapshot".into()))?;

        let segments_dir = storage.path().join("segments");
        let compacted_path = segments_dir.join("compacted.seg");
        let compacted_segment = Segment::new(compacted_path, 0);
        let mut writer = compacted_segment.create()?;

        for (pk, row_str) in latest_snapshot.state {
            // Parse the JSON string back to Value
            let row: serde_json::Value = match serde_json::from_str(&row_str) {
                Ok(val) => val,
                Err(e) => {
                    tracing::error!("Failed to parse row during compaction: {}, pk: {}", e, pk);
                    continue; // Skip corrupted rows instead of defaulting to null
                }
            };
            let event = Event::new_insert(
                table_name.to_string(),
                serde_json::Value::String(pk.clone()),
                row,
            );
            writer.append_event(&event)?;
        }

        writer.sync()?;

        let mut segment_files: Vec<_> = fs::read_dir(&segments_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "seg" && !entry.path().to_string_lossy().contains("compacted"))
                    .unwrap_or(false)
            })
            .collect();

        segment_files.sort_by_key(|entry| entry.path());

        for entry in segment_files {
            let segment = Segment::new(entry.path(), 0);
            let mut reader = segment.open_reader()?;
            let events = reader.read_all_events()?;

            let mut has_post_snapshot_events = false;
            for event in events {
                if event.sequence > latest_snapshot_seq {
                    has_post_snapshot_events = true;
                    writer.append_event(&event)?;
                }
            }

            if !has_post_snapshot_events {
                fs::remove_file(entry.path())?;
            }
        }

        writer.sync()?;

        let final_path = segments_dir.join("00000001.seg");
        fs::rename(segments_dir.join("compacted.seg"), final_path)?;

        Ok(())
    }

    pub fn doctor(&self) -> Result<Vec<String>> {
        let mut report = Vec::new();

        for (table_name, storage) in &self.tables {
            report.push(format!("Checking table: {}", table_name));

            let segments_dir = storage.path().join("segments");
            let mut segment_files: Vec<_> = fs::read_dir(&segments_dir)?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "seg")
                        .unwrap_or(false)
                })
                .collect();

            segment_files.sort_by_key(|entry| entry.path());

            for entry in segment_files {
                let segment = Segment::new(entry.path(), 0);
                let mut reader = segment.open_reader()?;

                if let Some(corrupt_pos) = reader.verify_and_find_corruption()? {
                    report.push(format!(
                        "  Found corruption in {} at position {}, truncating...",
                        entry.path().display(),
                        corrupt_pos
                    ));
                    segment.truncate_at(corrupt_pos)?;
                } else {
                    report.push(format!("  Segment {} is healthy", entry.path().display()));
                }
            }
        }

        Ok(report)
    }

    // Transaction support methods
    pub fn begin_transaction(&self, isolation: IsolationLevel) -> Result<u64> {
        self.transaction_manager.write().simple_begin(isolation)
    }

    pub fn commit_transaction(&mut self, txn_id: u64) -> Result<()> {
        let events = {
            let mut txn_mgr = self.transaction_manager.write();
            txn_mgr.simple_commit(txn_id)?
        };

        // Apply all events from the committed transaction
        for event in events {
            self.apply_event(event)?;
        }

        Ok(())
    }

    pub fn rollback_transaction(&self, txn_id: u64) -> Result<()> {
        self.transaction_manager.write().rollback(txn_id)
    }

    pub fn apply_event_in_transaction(&self, txn_id: u64, event: Event) -> Result<()> {
        self.transaction_manager.write().add_write(txn_id, event)
    }

    pub fn read_in_transaction(&self, txn_id: u64, table: &str, key: &str) -> Result<Option<serde_json::Value>> {
        // First check transaction's write set
        let txn_mgr = self.transaction_manager.read();
        let active_txns = txn_mgr.active_transactions.read();

        if let Some(txn) = active_txns.get(&txn_id) {
            let txn_guard = txn.lock();

            // Check write set first (read-your-writes)
            if let Some(event) = txn_guard.write_set.get(key) {
                return Ok(Some(event.payload.clone()));
            }

            let _snapshot_version = txn_guard.snapshot_version;
            drop(txn_guard);
        } else {
            return Err(DriftError::Other(format!("Transaction {} not found", txn_id)));
        }

        // Read from storage at snapshot version
        let storage = self.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?;

        // Get state at snapshot version (simplified - in production would use snapshot version)
        let state = storage.reconstruct_state_at(None)?;

        Ok(state.get(key).cloned())
    }

    pub fn query(&self, query: &Query) -> Result<QueryResult> {
        match query {
            Query::Select { table, conditions, as_of, .. } => {
                let storage = self.tables.get(table)
                    .ok_or_else(|| DriftError::TableNotFound(table.clone()))?;

                // Determine the target sequence number based on as_of clause
                let target_sequence = match as_of {
                    Some(crate::query::AsOf::Sequence(seq)) => Some(*seq),
                    Some(crate::query::AsOf::Timestamp(_)) => {
                        // Would need to map timestamp to sequence in production
                        None
                    }
                    Some(crate::query::AsOf::Now) | None => None,
                };

                // Reconstruct state at the target point in time
                let state = storage.reconstruct_state_at(target_sequence)?;

                // Apply WHERE conditions if any
                let mut results = Vec::new();
                for (_key, value) in state {
                    // Check if row matches conditions
                    let matches = if !conditions.is_empty() {
                        conditions.iter().all(|cond| {
                            // Simple equality check for now
                            if let Some(field_value) = value.get(&cond.column) {
                                field_value == &cond.value
                            } else {
                                false
                            }
                        })
                    } else {
                        true // No conditions means select all
                    };

                    if matches {
                        results.push(value);
                    }
                }

                Ok(QueryResult::Rows { data: results })
            }
            _ => Ok(QueryResult::Error { message: "Query type not supported in this method".to_string() })
        }
    }

    /// List all tables in the database
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    // Migration support methods

    /// Apply a schema migration to add a column with optional default value
    pub fn migrate_add_column(
        &mut self,
        table: &str,
        column: &crate::schema::ColumnDef,
        default_value: Option<serde_json::Value>,
    ) -> Result<()> {
        // Get the table storage
        let storage = self.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?
            .clone();

        // Update the schema file
        let table_path = self.base_path.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        let mut schema = crate::schema::Schema::load_from_file(&schema_path)?;

        // Check if column already exists
        if schema.columns.iter().any(|c| c.name == column.name) {
            return Err(DriftError::Other(format!("Column {} already exists", column.name)));
        }

        // Add the new column to schema
        schema.columns.push(column.clone());

        // Save updated schema
        let updated_schema = serde_yaml::to_string(&schema)?;
        fs::write(&schema_path, updated_schema)?;

        // If there's a default value, backfill existing records
        if let Some(default) = default_value {
            // Get current state to find all records
            let current_state = storage.reconstruct_state_at(None)?;

            // Create patch events for each existing record
            for (key, _value) in current_state {
                // The key is the stringified version of the primary key value (e.g., "\"user1\"")
                // We need to parse it back to the original JSON value
                let primary_key: serde_json::Value = serde_json::from_str(&key)
                    .unwrap_or_else(|_| serde_json::Value::String(key.clone()));

                let patch_event = crate::events::Event::new_patch(
                    table.to_string(),
                    primary_key,
                    serde_json::json!({
                        &column.name: default.clone()
                    })
                );

                // Use the engine's apply_event method to ensure proper handling
                self.apply_event(patch_event)?;
            }
        }

        Ok(())
    }

    /// Apply a schema migration to drop a column
    pub fn migrate_drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        // Update the schema file
        let table_path = self.base_path.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        let mut schema = crate::schema::Schema::load_from_file(&schema_path)?;

        // Remove the column from schema
        schema.columns.retain(|c| c.name != column);

        // Save updated schema
        let updated_schema = serde_yaml::to_string(&schema)?;
        fs::write(&schema_path, updated_schema)?;

        // Note: We don't remove the data from existing events (append-only)
        // The column will just be ignored in future queries

        Ok(())
    }

    /// Apply a schema migration to rename a column
    pub fn migrate_rename_column(&mut self, table: &str, old_name: &str, new_name: &str) -> Result<()> {
        let storage = self.tables.get(table)
            .ok_or_else(|| DriftError::TableNotFound(table.to_string()))?
            .clone();

        // Update the schema file
        let table_path = self.base_path.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        let mut schema = crate::schema::Schema::load_from_file(&schema_path)?;

        // Find and rename the column
        let mut found = false;
        for column in &mut schema.columns {
            if column.name == old_name {
                column.name = new_name.to_string();
                found = true;
                break;
            }
        }

        if !found {
            return Err(DriftError::Other(format!("Column {} not found", old_name)));
        }

        // Save updated schema
        let updated_schema = serde_yaml::to_string(&schema)?;
        fs::write(&schema_path, updated_schema)?;

        // Create patch events to rename the field in existing records
        let current_state = storage.reconstruct_state_at(None)?;

        for (key, value) in current_state {
            if let Some(old_value) = value.get(old_name) {
                // Parse the stringified primary key back to JSON value
                let primary_key: serde_json::Value = serde_json::from_str(&key)
                    .unwrap_or_else(|_| serde_json::Value::String(key.clone()));

                // Create a patch that adds the new field and removes the old
                let mut patch = serde_json::Map::new();
                patch.insert(new_name.to_string(), old_value.clone());

                let patch_event = crate::events::Event::new_patch(
                    table.to_string(),
                    primary_key,
                    serde_json::Value::Object(patch)
                );

                self.apply_event(patch_event)?;
            }
        }

        Ok(())
    }

    /// Begin a migration transaction
    pub fn begin_migration_transaction(&mut self) -> Result<u64> {
        self.begin_transaction(IsolationLevel::Serializable)
    }

    /// Commit a migration transaction
    pub fn commit_migration_transaction(&mut self, txn_id: u64) -> Result<()> {
        self.commit_transaction(txn_id)
    }

    /// Rollback a migration transaction
    pub fn rollback_migration_transaction(&mut self, txn_id: u64) -> Result<()> {
        self.rollback_transaction(txn_id)
    }
}