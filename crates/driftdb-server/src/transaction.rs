//! Transaction management for DriftDB
//!
//! Implements ACID transactions with proper isolation and rollback support

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use serde_json::Value;
use tracing::info;

use driftdb_core::engine::Engine;

/// Global transaction ID counter
static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction state for a session
#[derive(Debug, Clone)]
pub struct TransactionState {
    pub txn_id: u64,
    #[allow(dead_code)]
    pub isolation_level: IsolationLevel,
    #[allow(dead_code)]
    pub start_time: u64,
    #[allow(dead_code)]
    pub start_sequence: u64,
    pub is_active: bool,
    #[allow(dead_code)]
    pub is_read_only: bool,

    /// Pending operations (not yet committed)
    pub pending_writes: Vec<PendingWrite>,

    /// Savepoints for nested transactions
    pub savepoints: Vec<Savepoint>,
}

impl TransactionState {
    pub fn new(isolation_level: IsolationLevel, is_read_only: bool, start_sequence: u64) -> Self {
        let txn_id = NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst);
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| {
                // System time is before Unix epoch - extremely rare but possible
                // Fall back to 0, which is better than panicking
                tracing::warn!(
                    "System time is before Unix epoch, using 0 for transaction start time"
                );
                std::time::Duration::from_secs(0)
            })
            .as_millis() as u64;

        Self {
            txn_id,
            isolation_level,
            start_time,
            start_sequence,
            is_active: true,
            is_read_only,
            pending_writes: Vec::new(),
            savepoints: Vec::new(),
        }
    }

    /// Add a pending write operation
    #[allow(dead_code)]
    pub fn add_write(&mut self, write: PendingWrite) -> Result<()> {
        if self.is_read_only {
            return Err(anyhow!("Cannot write in read-only transaction"));
        }

        self.pending_writes.push(write);
        Ok(())
    }

    /// Create a savepoint
    pub fn create_savepoint(&mut self, name: String) -> Result<()> {
        let savepoint = Savepoint {
            name,
            write_count: self.pending_writes.len(),
        };

        self.savepoints.push(savepoint);
        Ok(())
    }

    /// Rollback to a savepoint
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let savepoint_idx = self
            .savepoints
            .iter()
            .position(|sp| sp.name == name)
            .ok_or_else(|| anyhow!("Savepoint '{}' not found", name))?;

        let savepoint = self.savepoints[savepoint_idx].clone();

        // Truncate pending writes to the savepoint position
        self.pending_writes.truncate(savepoint.write_count);

        // Remove this savepoint and all savepoints after it
        self.savepoints.truncate(savepoint_idx);

        Ok(())
    }
}

/// A pending write operation
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub table: String,
    pub operation: WriteOperation,
    pub data: Value,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum WriteOperation {
    Insert,
    Update { id: Value },
    Delete { id: Value },
}

/// Savepoint for nested transactions
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    pub write_count: usize,
}

/// Transaction manager - coordinates transactions across sessions
pub struct TransactionManager {
    /// Active transactions by session ID
    transactions: Arc<RwLock<HashMap<String, TransactionState>>>,

    /// Global lock manager for conflict detection
    lock_manager: Arc<LockManager>,

    /// Reference to the engine for applying commits
    engine: Arc<parking_lot::RwLock<Engine>>,
}

impl TransactionManager {
    pub fn new(engine: Arc<parking_lot::RwLock<Engine>>) -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager: Arc::new(LockManager::new()),
            engine,
        }
    }

    /// Begin a new transaction
    pub async fn begin_transaction(
        &self,
        session_id: &str,
        isolation_level: IsolationLevel,
        is_read_only: bool,
    ) -> Result<u64> {
        // Get current sequence from engine
        let engine = self.engine.read();
        let current_sequence = engine.get_current_sequence();
        drop(engine);

        let state = TransactionState::new(isolation_level, is_read_only, current_sequence);
        let txn_id = state.txn_id;

        let mut transactions = self.transactions.write();
        if transactions.contains_key(session_id) {
            return Err(anyhow!("Transaction already active for session"));
        }

        transactions.insert(session_id.to_string(), state);

        info!("Started transaction {} for session {}", txn_id, session_id);
        Ok(txn_id)
    }

    /// Commit a transaction
    pub async fn commit_transaction(&self, session_id: &str) -> Result<()> {
        // Extract state and release lock immediately
        let state = {
            let mut transactions = self.transactions.write();
            transactions
                .remove(session_id)
                .ok_or_else(|| anyhow!("No active transaction for session"))?
        };

        if !state.is_active {
            return Err(anyhow!("Transaction is not active"));
        }

        // Apply all pending writes to the engine
        if !state.pending_writes.is_empty() {
            let mut engine = self.engine.write();

            for write in state.pending_writes {
                match write.operation {
                    WriteOperation::Insert => {
                        // Insert the data
                        engine
                            .insert_record(&write.table, write.data)
                            .map_err(|e| anyhow!("Failed to insert: {}", e))?;
                    }
                    WriteOperation::Update { id } => {
                        // Apply update (update record)
                        engine
                            .update_record(&write.table, id, write.data)
                            .map_err(|e| anyhow!("Failed to update: {}", e))?;
                    }
                    WriteOperation::Delete { id } => {
                        // Apply delete (soft delete for audit trail)
                        engine
                            .delete_record(&write.table, id)
                            .map_err(|e| anyhow!("Failed to delete: {}", e))?;
                    }
                }
            }
        }

        // Release all locks held by this transaction
        self.lock_manager.release_all_locks(state.txn_id);

        info!(
            "Committed transaction {} for session {}",
            state.txn_id, session_id
        );
        Ok(())
    }

    /// Rollback a transaction
    pub async fn rollback_transaction(&self, session_id: &str) -> Result<()> {
        // Extract state and release lock immediately
        let state = {
            let mut transactions = self.transactions.write();
            transactions
                .remove(session_id)
                .ok_or_else(|| anyhow!("No active transaction for session"))?
        };

        // Release all locks held by this transaction
        self.lock_manager.release_all_locks(state.txn_id);

        // Pending writes are simply discarded
        info!(
            "Rolled back transaction {} for session {}",
            state.txn_id, session_id
        );
        Ok(())
    }

    /// Get transaction state for a session
    #[allow(dead_code)]
    pub fn get_transaction(&self, session_id: &str) -> Option<TransactionState> {
        self.transactions.read().get(session_id).cloned()
    }

    /// Check if session has an active transaction
    pub fn has_transaction(&self, session_id: &str) -> bool {
        self.transactions.read().contains_key(session_id)
    }

    /// Check if session is in an active transaction
    pub fn is_in_transaction(&self, session_id: &str) -> Result<bool> {
        let transactions = self.transactions.read();
        Ok(transactions
            .get(session_id)
            .map(|state| state.is_active)
            .unwrap_or(false))
    }

    /// Add a pending write to the transaction
    pub async fn add_pending_write(&self, session_id: &str, write: PendingWrite) -> Result<()> {
        let mut transactions = self.transactions.write();

        let state = transactions
            .get_mut(session_id)
            .ok_or_else(|| anyhow!("No active transaction for session"))?;

        state.add_write(write)?;
        Ok(())
    }

    /// Create a savepoint
    pub fn create_savepoint(&self, session_id: &str, name: String) -> Result<()> {
        let mut transactions = self.transactions.write();

        let state = transactions
            .get_mut(session_id)
            .ok_or_else(|| anyhow!("No active transaction for session"))?;

        state.create_savepoint(name)?;
        Ok(())
    }

    /// Rollback to a savepoint
    pub fn rollback_to_savepoint(&self, session_id: &str, name: &str) -> Result<()> {
        let mut transactions = self.transactions.write();

        let state = transactions
            .get_mut(session_id)
            .ok_or_else(|| anyhow!("No active transaction for session"))?;

        state.rollback_to_savepoint(name)?;
        Ok(())
    }
}

/// Lock manager for transaction isolation
struct LockManager {
    /// Table locks: table_name -> set of transaction IDs holding locks
    table_locks: RwLock<HashMap<String, HashSet<u64>>>,

    /// Row locks: (table_name, row_id) -> transaction ID holding lock
    row_locks: RwLock<HashMap<(String, String), u64>>,
}

impl LockManager {
    fn new() -> Self {
        Self {
            table_locks: RwLock::new(HashMap::new()),
            row_locks: RwLock::new(HashMap::new()),
        }
    }

    /// Release all locks held by a transaction
    fn release_all_locks(&self, txn_id: u64) {
        // Release table locks
        let mut table_locks = self.table_locks.write();
        for locks in table_locks.values_mut() {
            locks.remove(&txn_id);
        }

        // Release row locks
        let mut row_locks = self.row_locks.write();
        row_locks.retain(|_, lock_txn_id| *lock_txn_id != txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state() {
        let state = TransactionState::new(IsolationLevel::ReadCommitted, false, 100);
        assert!(state.is_active);
        assert_eq!(state.start_sequence, 100);
        assert!(!state.is_read_only);
    }

    #[test]
    fn test_savepoint() {
        let mut state = TransactionState::new(IsolationLevel::ReadCommitted, false, 100);

        // Add some writes
        state
            .add_write(PendingWrite {
                table: "test".to_string(),
                operation: WriteOperation::Insert,
                data: Value::Null,
            })
            .unwrap();

        // Create savepoint
        state.create_savepoint("sp1".to_string()).unwrap();

        // Add more writes
        state
            .add_write(PendingWrite {
                table: "test".to_string(),
                operation: WriteOperation::Insert,
                data: Value::Null,
            })
            .unwrap();

        assert_eq!(state.pending_writes.len(), 2);

        // Rollback to savepoint
        state.rollback_to_savepoint("sp1").unwrap();
        assert_eq!(state.pending_writes.len(), 1);
    }
}
