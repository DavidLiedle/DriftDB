use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

use crate::distributed_coordinator::{CoordinationResult, DistributedCoordinator};
use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::mvcc::{IsolationLevel, MVCCConfig, MVCCManager, MVCCTransaction, RecordId};
use crate::transaction::IsolationLevel as TxnIsolationLevel;
use crate::wal::{WalManager, WalOperation};

/// Enhanced transaction coordinator that integrates MVCC, WAL, and distributed coordination
pub struct TransactionCoordinator {
    mvcc_manager: Arc<MVCCManager>,
    wal_manager: Arc<WalManager>,
    distributed_coordinator: Option<Arc<DistributedCoordinator>>,
    active_transactions: Arc<RwLock<HashMap<u64, Arc<MVCCTransaction>>>>,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator
    pub fn new(
        wal_manager: Arc<WalManager>,
        distributed_coordinator: Option<Arc<DistributedCoordinator>>,
    ) -> Self {
        let config = MVCCConfig {
            default_isolation: IsolationLevel::ReadCommitted,
            deadlock_detection: true,
            deadlock_check_interval_ms: 100,
            max_transaction_duration_ms: 60000,
            vacuum_interval_ms: 5000,
            min_versions_to_keep: 100,
            detect_write_conflicts: true,
        };

        Self {
            mvcc_manager: Arc::new(MVCCManager::new(config)),
            wal_manager,
            distributed_coordinator,
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Begin a new transaction with ACID guarantees
    #[instrument(skip(self))]
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<MVCCTransaction>> {
        // Check if we can accept writes (distributed coordination)
        if let Some(ref coordinator) = self.distributed_coordinator {
            let status = coordinator.cluster_status();
            if !status.can_accept_writes() {
                return Err(DriftError::Other(
                    "Cannot start transaction: node cannot accept writes".to_string(),
                ));
            }
        }

        // Begin transaction with MVCC
        let txn = self.mvcc_manager.begin_transaction(isolation_level)?;

        // Log transaction begin to WAL
        self.wal_manager
            .log_operation(WalOperation::TransactionBegin {
                transaction_id: txn.id,
            })?;

        // Track active transaction
        self.active_transactions.write().insert(txn.id, txn.clone());

        info!(
            "Started transaction {} with isolation {:?}",
            txn.id, isolation_level
        );
        Ok(txn)
    }

    /// Read a value within a transaction
    #[instrument(skip(self, txn))]
    pub fn read(
        &self,
        txn: &MVCCTransaction,
        table: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>> {
        let record_id = RecordId {
            table: table.to_string(),
            key: key.to_string(),
        };

        self.mvcc_manager.read(txn, record_id)
    }

    /// Write a value within a transaction
    #[instrument(skip(self, txn, value))]
    pub fn write(
        &self,
        txn: &MVCCTransaction,
        table: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        let record_id = RecordId {
            table: table.to_string(),
            key: key.to_string(),
        };

        self.mvcc_manager.write(txn, record_id, value)
    }

    /// Delete a record within a transaction
    #[instrument(skip(self, txn))]
    pub fn delete(&self, txn: &MVCCTransaction, table: &str, key: &str) -> Result<()> {
        let record_id = RecordId {
            table: table.to_string(),
            key: key.to_string(),
        };

        // Delete is implemented as writing None
        self.mvcc_manager.delete(txn, record_id)
    }

    /// Commit a transaction with full ACID guarantees
    #[instrument(skip(self, txn))]
    pub fn commit_transaction(&self, txn: &Arc<MVCCTransaction>) -> Result<()> {
        debug!("Starting commit for transaction {}", txn.id);

        // Check if this is a distributed transaction
        if let Some(ref coordinator) = self.distributed_coordinator {
            // Create a dummy event for coordination
            let dummy_event = Event::new_insert(
                "txn_commit".to_string(),
                serde_json::json!(txn.id),
                serde_json::json!({"txn_id": txn.id}),
            );

            match coordinator.coordinate_event(&dummy_event)? {
                CoordinationResult::ForwardToLeader(leader) => {
                    return Err(DriftError::Other(format!(
                        "Transaction must be committed on leader: {:?}",
                        leader
                    )));
                }
                CoordinationResult::Rejected(reason) => {
                    return Err(DriftError::Other(format!(
                        "Transaction rejected by cluster: {}",
                        reason
                    )));
                }
                CoordinationResult::Committed => {
                    // Continue with commit process
                }
            }
        }

        // Pre-commit validation for serializable isolation is handled within the MVCC commit

        // Write all changes to WAL
        let write_set = txn.write_set.read();
        for (record_id, version) in write_set.iter() {
            let wal_op = if version.data.is_some() {
                WalOperation::Insert {
                    table: record_id.table.clone(),
                    row_id: record_id.key.clone(),
                    data: version.data.clone().unwrap(),
                }
            } else {
                WalOperation::Delete {
                    table: record_id.table.clone(),
                    row_id: record_id.key.clone(),
                    data: serde_json::Value::Null,
                }
            };
            self.wal_manager.log_operation(wal_op)?;
        }

        // Log commit to WAL
        self.wal_manager
            .log_operation(WalOperation::TransactionCommit {
                transaction_id: txn.id,
            })?;

        // Commit in MVCC manager
        self.mvcc_manager.commit(txn.clone())?;

        // Remove from active transactions
        self.active_transactions.write().remove(&txn.id);

        info!("Successfully committed transaction {}", txn.id);
        Ok(())
    }

    /// Abort a transaction
    #[instrument(skip(self, txn))]
    pub fn abort_transaction(&self, txn: &Arc<MVCCTransaction>) -> Result<()> {
        debug!("Aborting transaction {}", txn.id);

        // Log abort to WAL
        self.wal_manager
            .log_operation(WalOperation::TransactionAbort {
                transaction_id: txn.id,
            })?;

        // Abort in MVCC manager
        self.mvcc_manager.abort(txn.clone())?;

        // Remove from active transactions
        self.active_transactions.write().remove(&txn.id);

        warn!("Aborted transaction {}", txn.id);
        Ok(())
    }

    /// Execute a transaction with automatic retry logic
    #[instrument(skip(self, operation))]
    pub fn execute_transaction<F, R>(
        &self,
        isolation_level: IsolationLevel,
        operation: F,
    ) -> Result<R>
    where
        F: Fn(&Arc<MVCCTransaction>) -> Result<R> + Send + Sync,
        R: Send,
    {
        const MAX_RETRIES: usize = 3;
        let mut attempt = 0;

        loop {
            attempt += 1;
            let txn = self.begin_transaction(isolation_level)?;

            match operation(&txn) {
                Ok(result) => {
                    match self.commit_transaction(&txn) {
                        Ok(()) => return Ok(result),
                        Err(e) if attempt < MAX_RETRIES && self.is_retryable_error(&e) => {
                            debug!("Transaction {} failed with retryable error: {}, retrying (attempt {})",
                                   txn.id, e, attempt);
                            continue;
                        }
                        Err(e) => {
                            let _ = self.abort_transaction(&txn);
                            return Err(e);
                        }
                    }
                }
                Err(e) if attempt < MAX_RETRIES && self.is_retryable_error(&e) => {
                    let _ = self.abort_transaction(&txn);
                    debug!(
                        "Operation failed with retryable error: {}, retrying (attempt {})",
                        e, attempt
                    );
                    continue;
                }
                Err(e) => {
                    let _ = self.abort_transaction(&txn);
                    return Err(e);
                }
            }
        }
    }

    /// Check if an error is retryable
    fn is_retryable_error(&self, error: &DriftError) -> bool {
        match error {
            DriftError::Lock(_) => true, // Lock conflicts are retryable
            DriftError::Other(msg) if msg.contains("conflict") => true,
            DriftError::Other(msg) if msg.contains("timeout") => true,
            DriftError::Other(msg) if msg.contains("validation failed") => true,
            _ => false,
        }
    }

    /// Get transaction statistics
    pub fn get_transaction_stats(&self) -> TransactionStats {
        let active_count = self.active_transactions.read().len();
        TransactionStats {
            active_transactions: active_count,
            mvcc_stats: self.mvcc_manager.get_stats(),
        }
    }

    /// Cleanup old transactions and perform maintenance
    pub fn cleanup(&self) -> Result<()> {
        // Cleanup timed-out transactions
        let timeout_txns: Vec<Arc<crate::mvcc::MVCCTransaction>> = {
            let active_txns = self.active_transactions.read();
            active_txns
                .iter()
                .filter_map(|(txn_id, txn)| {
                    // Check for timeout based on configuration
                    let start_time = std::time::SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_millis(txn.start_timestamp);
                    let elapsed = std::time::SystemTime::now()
                        .duration_since(start_time)
                        .unwrap_or(std::time::Duration::ZERO);

                    if elapsed > std::time::Duration::from_millis(60000) {
                        // 60 second timeout
                        warn!("Transaction {} timed out, will abort", txn_id);
                        Some(txn.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Abort timed-out transactions
        for txn in timeout_txns {
            let _ = self.abort_transaction(&txn);
        }

        // Run MVCC garbage collection (vacuum)
        self.mvcc_manager.vacuum()?;

        Ok(())
    }

    /// Convert from old isolation level enum to MVCC isolation level
    pub fn convert_isolation_level(old_level: TxnIsolationLevel) -> IsolationLevel {
        match old_level {
            TxnIsolationLevel::ReadUncommitted => IsolationLevel::ReadUncommitted,
            TxnIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            TxnIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            TxnIsolationLevel::Serializable => IsolationLevel::Serializable,
        }
    }
}

/// Transaction statistics
#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub active_transactions: usize,
    pub mvcc_stats: crate::mvcc::MVCCStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Arc::new(
            WalManager::new(temp_dir.path().join("test.wal"), WalConfig::default()).unwrap(),
        );

        let coordinator = TransactionCoordinator::new(wal, None);

        // Begin transaction
        let txn = coordinator
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Write some data
        coordinator
            .write(
                &txn,
                "users",
                "user1",
                serde_json::json!({"name": "Alice", "age": 30}),
            )
            .unwrap();

        // Read it back
        let result = coordinator.read(&txn, "users", "user1").unwrap();
        assert!(result.is_some());

        // Commit
        coordinator.commit_transaction(&txn).unwrap();
    }

    #[tokio::test]
    async fn test_transaction_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Arc::new(
            WalManager::new(temp_dir.path().join("test.wal"), WalConfig::default()).unwrap(),
        );

        let coordinator = TransactionCoordinator::new(wal, None);

        // Start two transactions
        let txn1 = coordinator
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let txn2 = coordinator
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Txn1 writes data
        coordinator
            .write(
                &txn1,
                "users",
                "user1",
                serde_json::json!({"name": "Alice"}),
            )
            .unwrap();

        // Txn2 shouldn't see uncommitted data
        let result = coordinator.read(&txn2, "users", "user1").unwrap();
        assert!(result.is_none());

        // Commit txn1
        coordinator.commit_transaction(&txn1).unwrap();

        // Now start txn3, should see committed data
        let txn3 = coordinator
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let result = coordinator.read(&txn3, "users", "user1").unwrap();
        assert!(result.is_some());

        coordinator.commit_transaction(&txn2).unwrap();
        coordinator.commit_transaction(&txn3).unwrap();
    }

    #[tokio::test]
    async fn test_transaction_retry() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Arc::new(
            WalManager::new(temp_dir.path().join("test.wal"), WalConfig::default()).unwrap(),
        );

        let coordinator = TransactionCoordinator::new(wal, None);

        let result = coordinator
            .execute_transaction(IsolationLevel::ReadCommitted, |txn| {
                coordinator.write(txn, "users", "user1", serde_json::json!({"name": "Bob"}))?;
                Ok("success")
            })
            .unwrap();

        assert_eq!(result, "success");
    }
}
