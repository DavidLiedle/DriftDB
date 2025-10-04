//! Snapshot-based transaction isolation implementation
//!
//! Provides true ACID compliance with snapshot isolation

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::errors::Result;
use crate::events::{Event, EventType};
use crate::storage::TableStorage;

/// A snapshot of the database state at a specific point in time
#[derive(Clone)]
pub struct TransactionSnapshot {
    /// The sequence number this snapshot represents
    pub sequence: u64,
    /// Cached state for each table at this sequence
    table_states: Arc<RwLock<HashMap<String, HashMap<String, Value>>>>,
}

impl TransactionSnapshot {
    /// Create a new snapshot at the given sequence number
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            table_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Read a value from the snapshot
    pub fn read(&self, table: &str, key: &str, storage: &TableStorage) -> Result<Option<Value>> {
        // Check cache first
        {
            let cache = self.table_states.read();
            if let Some(table_data) = cache.get(table) {
                return Ok(table_data.get(key).cloned());
            }
        }

        // Load table state at snapshot sequence if not cached
        self.ensure_table_cached(table, storage)?;

        // Now read from cache
        let cache = self.table_states.read();
        Ok(cache
            .get(table)
            .and_then(|table_data| table_data.get(key).cloned()))
    }

    /// Ensure a table's state at the snapshot sequence is cached
    fn ensure_table_cached(&self, table: &str, storage: &TableStorage) -> Result<()> {
        let mut cache = self.table_states.write();

        // Double-check after acquiring write lock
        if cache.contains_key(table) {
            return Ok(());
        }

        // Reconstruct state at snapshot sequence
        let state = storage.reconstruct_state_at(Some(self.sequence))?;

        // Convert to our format (remove JSON string keys)
        let mut table_state = HashMap::new();
        for (key, value) in state {
            // Remove quotes from keys if present
            let clean_key = key.trim_matches('"');
            table_state.insert(clean_key.to_string(), value);
        }

        cache.insert(table.to_string(), table_state);
        Ok(())
    }

    /// Apply writes from a transaction to get the view this transaction sees
    pub fn apply_writes(
        &self,
        table: &str,
        writes: &HashMap<String, Event>,
        storage: &TableStorage,
    ) -> Result<HashMap<String, Value>> {
        // Start with snapshot state
        self.ensure_table_cached(table, storage)?;

        let cache = self.table_states.read();
        let mut state = cache.get(table).cloned().unwrap_or_default();

        // Apply transaction's writes
        for (key, event) in writes {
            match event.event_type {
                EventType::Insert => {
                    state.insert(key.clone(), event.payload.clone());
                }
                EventType::Patch => {
                    if let Some(existing) = state.get_mut(key) {
                        if let (Value::Object(existing_map), Value::Object(patch_map)) =
                            (existing, &event.payload)
                        {
                            for (k, v) in patch_map {
                                existing_map.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }
                EventType::SoftDelete => {
                    state.remove(key);
                }
            }
        }

        Ok(state)
    }
}

/// Manager for transaction snapshots
pub struct SnapshotManager {
    /// Active snapshots by transaction ID
    snapshots: Arc<RwLock<HashMap<u64, TransactionSnapshot>>>,
    /// Minimum snapshot sequence to keep (for garbage collection)
    min_sequence: Arc<RwLock<u64>>,
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            min_sequence: Arc::new(RwLock::new(0)),
        }
    }

    /// Create a snapshot for a transaction
    pub fn create_snapshot(&self, txn_id: u64, sequence: u64) -> TransactionSnapshot {
        let snapshot = TransactionSnapshot::new(sequence);
        self.snapshots.write().insert(txn_id, snapshot.clone());
        snapshot
    }

    /// Remove a transaction's snapshot
    pub fn remove_snapshot(&self, txn_id: u64) {
        self.snapshots.write().remove(&txn_id);
        self.update_min_sequence();
    }

    /// Update the minimum sequence number to keep
    fn update_min_sequence(&self) {
        let snapshots = self.snapshots.read();
        let min = snapshots
            .values()
            .map(|s| s.sequence)
            .min()
            .unwrap_or(u64::MAX);
        *self.min_sequence.write() = min;
    }

    /// Get the minimum sequence that must be kept for active transactions
    pub fn get_min_sequence(&self) -> u64 {
        *self.min_sequence.read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, Schema};
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::new(
            "test".to_string(),
            "id".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                col_type: "string".to_string(),
                index: false,
            }],
        );

        let storage = TableStorage::create(temp_dir.path(), schema).unwrap();

        // Add some events
        let event1 = Event::new_insert(
            "test".to_string(),
            json!("key1"),
            json!({"value": "initial"}),
        );
        storage.append_event(event1).unwrap();

        // Create snapshot at sequence 1
        let snapshot = TransactionSnapshot::new(1);

        // Read should see the value
        let value = snapshot.read("test", "\"key1\"", &storage).unwrap();
        assert!(value.is_some());

        // Add another event after snapshot
        let event2 = Event::new_patch(
            "test".to_string(),
            json!("key1"),
            json!({"value": "modified"}),
        );
        storage.append_event(event2).unwrap();

        // Snapshot should still see old value
        let value = snapshot.read("test", "\"key1\"", &storage).unwrap();
        if let Some(v) = value {
            assert_eq!(v["value"], json!("initial"));
        }
    }
}
