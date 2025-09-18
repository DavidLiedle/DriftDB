use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::index::IndexManager;
use crate::observability::Metrics;
use crate::query::{Query, QueryResult, WhereCondition};
use crate::schema::{ColumnDef, Schema};
use crate::snapshot::SnapshotManager;
use crate::storage::{Segment, TableMeta, TableStorage};
use crate::transaction::{IsolationLevel, TransactionManager};
use crate::wal::{Wal, WalOperation};
use serde_json::Value;

pub struct Engine {
    base_path: PathBuf,
    pub(crate) tables: HashMap<String, Arc<TableStorage>>,
    indexes: HashMap<String, Arc<RwLock<IndexManager>>>,
    snapshots: HashMap<String, Arc<SnapshotManager>>,
    metrics: Arc<Metrics>,
    wal: Arc<Wal>,
    transaction_manager: Arc<RwLock<TransactionManager>>,
    last_sequence: Arc<AtomicU64>,
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

        let metrics = Arc::new(Metrics::new());
        let wal = Arc::new(Wal::new(&base_path)?);
        let last_sequence = Arc::new(AtomicU64::new(0));

        let transaction_manager = TransactionManager::new(wal.clone(), metrics.clone());

        let mut engine = Self {
            base_path: base_path.clone(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            snapshots: HashMap::new(),
            metrics,
            wal,
            transaction_manager: Arc::new(RwLock::new(transaction_manager)),
            last_sequence: last_sequence.clone(),
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

        engine.replay_wal()?;

        Ok(engine)
    }

    pub fn init<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        fs::create_dir_all(base_path.join("tables"))?;

        let metrics = Arc::new(Metrics::new());
        let wal = Arc::new(Wal::new(&base_path)?);
        let last_sequence = Arc::new(AtomicU64::new(0));
        let transaction_manager = TransactionManager::new(wal.clone(), metrics.clone());

        let mut engine = Self {
            base_path,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            snapshots: HashMap::new(),
            metrics,
            wal,
            transaction_manager: Arc::new(RwLock::new(transaction_manager)),
            last_sequence: last_sequence.clone(),
        };

        engine.replay_wal()?;

        Ok(engine)
    }

    fn load_table(&mut self, table_name: &str) -> Result<()> {
        let storage = Arc::new(TableStorage::open(&self.base_path, table_name)?);

        let mut index_mgr = IndexManager::new(storage.path());
        index_mgr.load_indexes(&storage.schema().indexed_columns())?;

        let snapshot_mgr = SnapshotManager::new(storage.path());

        self.tables.insert(table_name.to_string(), storage.clone());
        self.indexes
            .insert(table_name.to_string(), Arc::new(RwLock::new(index_mgr)));
        self.snapshots
            .insert(table_name.to_string(), Arc::new(snapshot_mgr));

        let meta = TableMeta::load_from_file(storage.path().join("meta.json"))?;
        self.record_sequence(meta.last_sequence);

        Ok(())
    }

    pub fn create_table(
        &mut self,
        name: &str,
        primary_key: &str,
        indexed_columns: Vec<String>,
    ) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(DriftError::Other(format!(
                "Table '{}' already exists",
                name
            )));
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
        self.indexes
            .insert(name.to_string(), Arc::new(RwLock::new(index_mgr)));
        self.snapshots
            .insert(name.to_string(), Arc::new(snapshot_mgr));

        Ok(())
    }

    pub fn apply_event(&mut self, event: Event) -> Result<u64> {
        self.apply_event_inner(event, true)
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

        self.metrics
            .snapshots_created
            .fetch_add(1, Ordering::Relaxed);
        self.wal.checkpoint(table_meta.last_sequence)?;
        self.metrics.wal_syncs.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn apply_event_inner(&mut self, mut event: Event, log_to_wal: bool) -> Result<u64> {
        let storage = self
            .tables
            .get(&event.table_name)
            .ok_or_else(|| DriftError::TableNotFound(event.table_name.clone()))?
            .clone();

        let timer = log_to_wal.then(Instant::now);

        let sequence = if log_to_wal {
            let seq = storage.append_event(event.clone())?;
            event.sequence = seq;
            seq
        } else {
            storage.append_event_preserving_sequence(&event)?;
            event.sequence
        };

        if let Some(index_mgr) = self.indexes.get(&event.table_name) {
            let mut index_mgr = index_mgr.write();
            index_mgr.update_indexes(&event, &storage.schema().indexed_columns())?;
            index_mgr.save_all()?;
        }

        if log_to_wal {
            let txn_id = self.wal.begin_transaction()?;
            self.wal.write_event(txn_id, event.clone())?;
            self.wal.commit_transaction(txn_id)?;
            self.metrics.wal_writes.fetch_add(1, Ordering::Relaxed);

            if let Some(start) = timer {
                let payload_size = event.payload.to_string().len() as u64;
                self.metrics
                    .record_write(payload_size, start.elapsed(), true);
            }

            self.wal.checkpoint(sequence)?;
            self.metrics.wal_syncs.fetch_add(1, Ordering::Relaxed);
        }

        self.record_sequence(sequence);

        Ok(sequence)
    }

    pub fn compact_table(&self, table_name: &str) -> Result<()> {
        let storage = self
            .tables
            .get(table_name)
            .ok_or_else(|| DriftError::TableNotFound(table_name.to_string()))?;

        let events = storage.read_all_events()?;
        if events.is_empty() {
            return Ok(());
        }

        storage.rewrite_segments(&events)?;

        if let Some(last_seq) = events.last().map(|e| e.sequence) {
            self.wal.truncate_at(last_seq)?;
            self.record_sequence(last_seq);
        }

        self.metrics
            .compactions_performed
            .fetch_add(1, Ordering::Relaxed);

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
                    entry
                        .path()
                        .extension()
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

    fn replay_wal(&mut self) -> Result<()> {
        let mut known_sequences = self.collect_table_sequences()?;
        let mut to_apply = Vec::new();

        self.wal.replay_from(None, |op| match op {
            WalOperation::Write { event, .. } => {
                let current = known_sequences
                    .get(&event.table_name)
                    .copied()
                    .unwrap_or_default();
                if event.sequence > current {
                    known_sequences.insert(event.table_name.clone(), event.sequence);
                    to_apply.push(event.clone());
                }
                Ok(())
            }
            WalOperation::Checkpoint { sequence, .. } => {
                for value in known_sequences.values_mut() {
                    if *value < sequence {
                        *value = sequence;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        })?;

        for event in to_apply {
            self.apply_event_inner(event, false)?;
        }

        Ok(())
    }

    pub(crate) fn index_candidate_keys(
        &self,
        table: &str,
        conditions: &[WhereCondition],
    ) -> Option<HashSet<String>> {
        let index_mgr = self.indexes.get(table)?;
        let index_guard = index_mgr.read();

        let mut used_index = false;
        let mut candidates: Option<HashSet<String>> = None;

        for cond in conditions {
            if let Some(index) = index_guard.get_index(&cond.column) {
                used_index = true;
                let lookup_key = Self::index_lookup_value(&cond.value);
                if let Some(keys) = index.find(&lookup_key) {
                    let key_set: HashSet<String> = keys.iter().cloned().collect();
                    candidates = Some(match candidates {
                        Some(existing) => existing
                            .into_iter()
                            .filter(|k| key_set.contains(k))
                            .collect(),
                        None => key_set,
                    });

                    if let Some(ref set) = candidates {
                        if set.is_empty() {
                            return Some(HashSet::new());
                        }
                    }
                } else {
                    return Some(HashSet::new());
                }
            }
        }

        if used_index {
            candidates
        } else {
            None
        }
    }

    pub(crate) fn index_lookup_value(value: &Value) -> String {
        if let Some(s) = value.as_str() {
            s.to_string()
        } else {
            value.to_string()
        }
    }

    fn collect_table_sequences(&self) -> Result<HashMap<String, u64>> {
        let mut map = HashMap::new();
        let mut max_seq = 0u64;
        for (name, storage) in &self.tables {
            let meta_path = storage.path().join("meta.json");
            let meta = TableMeta::load_from_file(meta_path)?;
            if meta.last_sequence > max_seq {
                max_seq = meta.last_sequence;
            }
            map.insert(name.clone(), meta.last_sequence);
        }
        self.record_sequence(max_seq);
        Ok(map)
    }

    fn record_sequence(&self, sequence: u64) {
        loop {
            let current = self.last_sequence.load(Ordering::Relaxed);
            if sequence <= current {
                break;
            }
            if self
                .last_sequence
                .compare_exchange(current, sequence, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    // Transaction support methods
    pub fn begin_transaction(&self, isolation: IsolationLevel) -> Result<u64> {
        let snapshot_seq = self.last_sequence.load(Ordering::SeqCst);
        self.transaction_manager
            .write()
            .simple_begin(isolation, snapshot_seq)
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

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    pub fn rollback_transaction(&self, txn_id: u64) -> Result<()> {
        self.transaction_manager.write().rollback(txn_id)
    }

    pub fn apply_event_in_transaction(&self, txn_id: u64, event: Event) -> Result<()> {
        self.transaction_manager.write().add_write(txn_id, event)
    }

    pub fn query(&self, query: &Query) -> Result<QueryResult> {
        // Simple implementation - would be more complex in production
        match query {
            Query::Select { table, .. } => {
                let _storage = self
                    .tables
                    .get(table)
                    .ok_or_else(|| DriftError::TableNotFound(table.clone()))?;

                let results = Vec::new(); // Would implement actual query logic

                Ok(QueryResult::Rows { data: results })
            }
            _ => Ok(QueryResult::Error {
                message: "Query type not supported".to_string(),
            }),
        }
    }
}
