//! Transaction buffering system for ROLLBACK support

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde_json::Value;
use tracing::{debug, info, warn};

/// Transaction operation that can be rolled back
#[derive(Debug, Clone)]
pub enum BufferedOperation {
    Insert {
        table: String,
        data: Value,
    },
    Update {
        table: String,
        key: String,
        data: Value,
        old_data: Option<Value>, // Store old data for rollback
    },
    Delete {
        table: String,
        key: String,
        old_data: Value, // Store data for rollback
    },
    CreateTable {
        table: String,
    },
    DropTable {
        table: String,
        schema: Value, // Store schema for rollback
    },
}

/// Transaction buffer that stores operations until commit or rollback
#[derive(Debug)]
pub struct TransactionBuffer {
    /// Transaction ID
    txn_id: u64,
    /// Buffered operations
    operations: Vec<BufferedOperation>,
    /// Read snapshot at transaction start
    read_snapshot: HashMap<String, HashMap<String, Value>>,
    /// Is transaction active
    is_active: bool,
}

impl TransactionBuffer {
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            operations: Vec::new(),
            read_snapshot: HashMap::new(),
            is_active: true,
        }
    }

    /// Add an operation to the buffer
    pub fn add_operation(&mut self, op: BufferedOperation) {
        if !self.is_active {
            warn!("Attempt to add operation to inactive transaction {}", self.txn_id);
            return;
        }
        debug!("Transaction {} buffering operation: {:?}", self.txn_id, op);
        self.operations.push(op);
    }

    /// Get buffered operations
    pub fn get_operations(&self) -> &[BufferedOperation] {
        &self.operations
    }

    /// Clear buffer (for commit)
    pub fn clear(&mut self) {
        self.operations.clear();
        self.read_snapshot.clear();
        self.is_active = false;
    }

    /// Mark transaction as inactive (for rollback)
    pub fn deactivate(&mut self) {
        self.is_active = false;
    }

    /// Check if transaction is active
    pub fn is_active(&self) -> bool {
        self.is_active
    }

    /// Store read snapshot for isolation
    pub fn set_read_snapshot(&mut self, snapshot: HashMap<String, HashMap<String, Value>>) {
        self.read_snapshot = snapshot;
    }

    /// Get read snapshot
    pub fn get_read_snapshot(&self) -> &HashMap<String, HashMap<String, Value>> {
        &self.read_snapshot
    }
}

/// Transaction buffer manager for all active transactions
pub struct TransactionBufferManager {
    /// Active transaction buffers
    buffers: Arc<RwLock<HashMap<u64, TransactionBuffer>>>,
    /// Next transaction ID
    next_txn_id: Arc<RwLock<u64>>,
}

impl TransactionBufferManager {
    pub fn new() -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            next_txn_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Start a new transaction
    pub fn begin_transaction(&self) -> u64 {
        let mut next_id = self.next_txn_id.write();
        let txn_id = *next_id;
        *next_id += 1;

        let buffer = TransactionBuffer::new(txn_id);
        self.buffers.write().insert(txn_id, buffer);

        info!("Started transaction {}", txn_id);
        txn_id
    }

    /// Add operation to transaction
    pub fn add_operation(&self, txn_id: u64, op: BufferedOperation) -> Result<(), String> {
        let mut buffers = self.buffers.write();
        match buffers.get_mut(&txn_id) {
            Some(buffer) => {
                buffer.add_operation(op);
                Ok(())
            }
            None => Err(format!("Transaction {} not found", txn_id)),
        }
    }

    /// Commit transaction (apply all operations)
    pub fn commit_transaction(&self, txn_id: u64) -> Result<Vec<BufferedOperation>, String> {
        let mut buffers = self.buffers.write();
        match buffers.remove(&txn_id) {
            Some(mut buffer) => {
                let operations = buffer.get_operations().to_vec();
                buffer.clear();
                info!("Committed transaction {} with {} operations", txn_id, operations.len());
                Ok(operations)
            }
            None => Err(format!("Transaction {} not found", txn_id)),
        }
    }

    /// Rollback transaction (discard all operations)
    pub fn rollback_transaction(&self, txn_id: u64) -> Result<(), String> {
        let mut buffers = self.buffers.write();
        match buffers.remove(&txn_id) {
            Some(mut buffer) => {
                let op_count = buffer.get_operations().len();
                buffer.deactivate();
                info!("Rolled back transaction {} with {} operations", txn_id, op_count);
                Ok(())
            }
            None => Err(format!("Transaction {} not found", txn_id)),
        }
    }

    /// Check if transaction exists and is active
    pub fn is_transaction_active(&self, txn_id: u64) -> bool {
        self.buffers.read()
            .get(&txn_id)
            .map(|b| b.is_active())
            .unwrap_or(false)
    }

    /// Get transaction buffer for reading
    pub fn get_buffer(&self, txn_id: u64) -> Option<Vec<BufferedOperation>> {
        self.buffers.read()
            .get(&txn_id)
            .map(|b| b.get_operations().to_vec())
    }

    /// Set read snapshot for transaction
    pub fn set_read_snapshot(&self, txn_id: u64, snapshot: HashMap<String, HashMap<String, Value>>) -> Result<(), String> {
        let mut buffers = self.buffers.write();
        match buffers.get_mut(&txn_id) {
            Some(buffer) => {
                buffer.set_read_snapshot(snapshot);
                Ok(())
            }
            None => Err(format!("Transaction {} not found", txn_id)),
        }
    }

    /// Apply buffered operations to actual storage
    pub async fn apply_operations(
        &self,
        operations: Vec<BufferedOperation>,
        executor: &mut impl OperationExecutor,
    ) -> Result<(), String> {
        for op in operations {
            match op {
                BufferedOperation::Insert { table, data } => {
                    executor.insert(&table, data).await?;
                }
                BufferedOperation::Update { table, key, data, .. } => {
                    executor.update(&table, &key, data).await?;
                }
                BufferedOperation::Delete { table, key, .. } => {
                    executor.delete(&table, &key).await?;
                }
                BufferedOperation::CreateTable { table } => {
                    executor.create_table(&table).await?;
                }
                BufferedOperation::DropTable { table, .. } => {
                    executor.drop_table(&table).await?;
                }
            }
        }
        Ok(())
    }

    /// Cleanup inactive transactions (garbage collection)
    pub fn cleanup_inactive(&self) {
        let mut buffers = self.buffers.write();
        let before_count = buffers.len();

        buffers.retain(|txn_id, buffer| {
            if !buffer.is_active() {
                debug!("Cleaning up inactive transaction {}", txn_id);
                false
            } else {
                true
            }
        });

        let removed = before_count - buffers.len();
        if removed > 0 {
            info!("Cleaned up {} inactive transactions", removed);
        }
    }

    /// Get statistics
    pub fn get_stats(&self) -> TransactionBufferStats {
        let buffers = self.buffers.read();
        let total_operations: usize = buffers.values()
            .map(|b| b.get_operations().len())
            .sum();

        TransactionBufferStats {
            active_transactions: buffers.len(),
            total_buffered_operations: total_operations,
            next_transaction_id: *self.next_txn_id.read(),
        }
    }
}

/// Statistics for transaction buffer manager
#[derive(Debug, Clone)]
pub struct TransactionBufferStats {
    pub active_transactions: usize,
    pub total_buffered_operations: usize,
    pub next_transaction_id: u64,
}

/// Trait for executing operations (implemented by actual executor)
#[async_trait::async_trait]
pub trait OperationExecutor {
    async fn insert(&mut self, table: &str, data: Value) -> Result<(), String>;
    async fn update(&mut self, table: &str, key: &str, data: Value) -> Result<(), String>;
    async fn delete(&mut self, table: &str, key: &str) -> Result<(), String>;
    async fn create_table(&mut self, table: &str) -> Result<(), String>;
    async fn drop_table(&mut self, table: &str) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_buffer() {
        let manager = TransactionBufferManager::new();

        // Start transaction
        let txn_id = manager.begin_transaction();
        assert!(manager.is_transaction_active(txn_id));

        // Add operations
        let op = BufferedOperation::Insert {
            table: "users".to_string(),
            data: serde_json::json!({"name": "Alice"}),
        };
        manager.add_operation(txn_id, op.clone()).unwrap();

        // Check buffer
        let buffer = manager.get_buffer(txn_id).unwrap();
        assert_eq!(buffer.len(), 1);

        // Rollback
        manager.rollback_transaction(txn_id).unwrap();
        assert!(!manager.is_transaction_active(txn_id));
    }

    #[test]
    fn test_commit_transaction() {
        let manager = TransactionBufferManager::new();

        let txn_id = manager.begin_transaction();

        // Add multiple operations
        for i in 0..5 {
            let op = BufferedOperation::Insert {
                table: format!("table_{}", i),
                data: serde_json::json!({"id": i}),
            };
            manager.add_operation(txn_id, op).unwrap();
        }

        // Commit
        let operations = manager.commit_transaction(txn_id).unwrap();
        assert_eq!(operations.len(), 5);
        assert!(!manager.is_transaction_active(txn_id));
    }

    #[test]
    fn test_cleanup_inactive() {
        let manager = TransactionBufferManager::new();

        // Create multiple transactions
        let txn1 = manager.begin_transaction();
        let txn2 = manager.begin_transaction();
        let txn3 = manager.begin_transaction();

        // Rollback one
        manager.rollback_transaction(txn2).unwrap();

        // Check stats before cleanup
        let stats = manager.get_stats();
        assert_eq!(stats.active_transactions, 2); // txn1 and txn3 still active

        // Cleanup
        manager.cleanup_inactive();

        // Check stats after cleanup
        let stats = manager.get_stats();
        assert_eq!(stats.active_transactions, 2);
    }
}