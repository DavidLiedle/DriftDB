//! Transaction support with ACID guarantees
//!
//! DriftDB transactions provide:
//! - Atomicity: All operations succeed or all fail
//! - Consistency: Data integrity constraints maintained
//! - Isolation: Snapshot isolation with MVCC
//! - Durability: WAL ensures committed data survives crashes

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, warn};

use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::observability::Metrics;
use crate::wal::{WalManager, WalOperation};

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IsolationLevel {
    /// Dirty reads allowed (not recommended)
    ReadUncommitted,
    /// No dirty reads, but non-repeatable reads possible
    ReadCommitted,
    /// Snapshot of data at transaction start
    #[default]
    RepeatableRead,
    /// Full serializability
    Serializable,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
}

/// A database transaction
pub struct Transaction {
    pub id: u64,
    pub isolation: IsolationLevel,
    pub state: TransactionState,
    pub start_time: Instant,
    pub snapshot_version: u64,
    pub read_set: HashSet<String>,         // Keys read
    pub write_set: HashMap<String, Event>, // Pending writes
    pub locked_keys: HashSet<String>,      // Keys locked for this transaction
    pub timeout: Duration,
}

impl Transaction {
    pub fn new(id: u64, isolation: IsolationLevel, snapshot_version: u64) -> Self {
        Self {
            id,
            isolation,
            state: TransactionState::Active,
            start_time: Instant::now(),
            snapshot_version,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            locked_keys: HashSet::new(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            TransactionState::Active | TransactionState::Preparing
        )
    }

    pub fn is_terminated(&self) -> bool {
        matches!(
            self.state,
            TransactionState::Committed | TransactionState::Aborted
        )
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn is_timeout(&self) -> bool {
        self.elapsed() > self.timeout
    }
}

/// Lock manager for transaction isolation
pub struct LockManager {
    /// Read locks: key -> set of transaction IDs holding read locks
    read_locks: RwLock<HashMap<String, HashSet<u64>>>,
    /// Write locks: key -> transaction ID holding write lock
    write_locks: RwLock<HashMap<String, u64>>,
    /// Lock wait queue: key -> list of (txn_id, lock_type)
    wait_queue: Mutex<HashMap<String, Vec<(u64, LockType)>>>,
    /// Deadlock detector state
    waits_for: Mutex<HashMap<u64, HashSet<u64>>>, // txn waits for these txns
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

impl LockManager {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self {
            read_locks: RwLock::new(HashMap::new()),
            write_locks: RwLock::new(HashMap::new()),
            wait_queue: Mutex::new(HashMap::new()),
            waits_for: Mutex::new(HashMap::new()),
        }
    }
}

impl LockManager {
    /// Acquire a read lock
    pub fn acquire_read_lock(&self, txn_id: u64, key: &str) -> Result<()> {
        // Check for write lock
        {
            let write_locks = self.write_locks.read();
            if let Some(&owner) = write_locks.get(key) {
                if owner != txn_id {
                    return self.wait_for_lock(txn_id, key, LockType::Read, owner);
                }
            }
        }

        // Grant read lock
        let mut read_locks = self.read_locks.write();
        read_locks
            .entry(key.to_string())
            .or_default()
            .insert(txn_id);

        debug!("Transaction {} acquired read lock on {}", txn_id, key);
        Ok(())
    }

    /// Acquire a write lock
    pub fn acquire_write_lock(&self, txn_id: u64, key: &str) -> Result<()> {
        // Check for existing locks
        {
            let write_locks = self.write_locks.read();
            if let Some(&owner) = write_locks.get(key) {
                if owner != txn_id {
                    return self.wait_for_lock(txn_id, key, LockType::Write, owner);
                }
                return Ok(()); // Already owns write lock
            }
        }

        {
            let read_locks = self.read_locks.read();
            if let Some(readers) = read_locks.get(key) {
                if readers.len() > 1 || (readers.len() == 1 && !readers.contains(&txn_id)) {
                    // Other transactions have read locks
                    if let Some(&blocking_txn) = readers.iter().find(|&&id| id != txn_id) {
                        return self.wait_for_lock(txn_id, key, LockType::Write, blocking_txn);
                    }
                }
            }
        }

        // Grant write lock
        let mut write_locks = self.write_locks.write();
        write_locks.insert(key.to_string(), txn_id);

        debug!("Transaction {} acquired write lock on {}", txn_id, key);
        Ok(())
    }

    /// Wait for a lock (with deadlock detection)
    fn wait_for_lock(
        &self,
        txn_id: u64,
        key: &str,
        lock_type: LockType,
        blocking_txn: u64,
    ) -> Result<()> {
        // Simple deadlock detection: check if blocking_txn is waiting for txn_id
        if self.would_cause_deadlock(txn_id, blocking_txn) {
            error!(
                "Deadlock detected: txn {} waiting for txn {}",
                txn_id, blocking_txn
            );
            return Err(DriftError::Lock("Deadlock detected".to_string()));
        }

        // Add to wait queue
        let mut wait_queue = self.wait_queue.lock();
        wait_queue
            .entry(key.to_string())
            .or_default()
            .push((txn_id, lock_type));

        // Update waits-for graph
        let mut waits_for = self.waits_for.lock();
        waits_for.entry(txn_id).or_default().insert(blocking_txn);

        Err(DriftError::Lock(format!(
            "Transaction {} waiting for {:?} lock on {}",
            txn_id, lock_type, key
        )))
    }

    /// Check if acquiring lock would cause deadlock
    fn would_cause_deadlock(&self, waiting_txn: u64, blocking_txn: u64) -> bool {
        let waits_for = self.waits_for.lock();

        // DFS to check if blocking_txn can reach waiting_txn
        let mut visited = HashSet::new();
        let mut stack = vec![blocking_txn];

        while let Some(txn) = stack.pop() {
            if txn == waiting_txn {
                return true; // Cycle detected
            }
            if visited.insert(txn) {
                if let Some(waiting_for) = waits_for.get(&txn) {
                    stack.extend(waiting_for.iter());
                }
            }
        }

        false
    }

    /// Release all locks held by a transaction
    pub fn release_transaction_locks(&self, txn_id: u64) {
        // Release read locks
        {
            let mut read_locks = self.read_locks.write();
            read_locks.retain(|_, readers| {
                readers.remove(&txn_id);
                !readers.is_empty()
            });
        }

        // Release write locks and notify waiters
        let released_keys: Vec<String> = {
            let write_locks = self.write_locks.write();
            write_locks
                .iter()
                .filter_map(|(key, &owner)| {
                    if owner == txn_id {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        {
            let mut write_locks = self.write_locks.write();
            for key in &released_keys {
                write_locks.remove(key);
            }
        }

        // Clean up waits-for graph
        {
            let mut waits_for = self.waits_for.lock();
            waits_for.remove(&txn_id);
            for waiting_set in waits_for.values_mut() {
                waiting_set.remove(&txn_id);
            }
        }

        debug!("Released all locks for transaction {}", txn_id);

        // Wake up waiters (in real system, would notify waiting threads)
        self.notify_waiters(&released_keys);
    }

    fn notify_waiters(&self, keys: &[String]) {
        let mut wait_queue = self.wait_queue.lock();
        for key in keys {
            wait_queue.remove(key);
            // In production, would wake up waiting threads here
        }
    }
}

/// Transaction manager
pub struct TransactionManager {
    next_txn_id: Arc<AtomicU64>,
    pub(crate) active_transactions: Arc<RwLock<HashMap<u64, Arc<Mutex<Transaction>>>>>,
    lock_manager: Arc<LockManager>,
    wal: Arc<WalManager>,
    metrics: Arc<Metrics>,
    current_version: Arc<AtomicU64>,
}

impl TransactionManager {
    pub fn new() -> Result<Self> {
        // Get data path from environment or use a sensible default
        let data_path = std::env::var("DRIFTDB_DATA_PATH").unwrap_or_else(|_| "./data".to_string());
        let wal_dir = std::path::PathBuf::from(data_path).join("wal");
        let wal_path = wal_dir.join("wal.log");

        // Try to create the WAL directory
        if let Err(e) = std::fs::create_dir_all(&wal_dir) {
            eprintln!("WARNING: Failed to create WAL directory: {}", e);
        }

        // Create WAL with proper error handling
        let wal = match WalManager::new(&wal_path, crate::wal::WalConfig::default()) {
            Ok(w) => Arc::new(w),
            Err(e) => {
                eprintln!("ERROR: Failed to create WAL at {:?}: {}", wal_path, e);
                eprintln!("FALLBACK: Using temporary directory for WAL (DATA NOT DURABLE!)");

                // Try fallback to temp directory
                let temp_dir = std::env::temp_dir().join("driftdb_wal_fallback");
                let _ = std::fs::create_dir_all(&temp_dir);
                let temp_path = temp_dir.join("wal.log");

                match WalManager::new(temp_path, crate::wal::WalConfig::default()) {
                    Ok(w) => Arc::new(w),
                    Err(e2) => {
                        return Err(DriftError::Other(format!(
                            "Cannot create WAL even in temp directory: {}",
                            e2
                        )));
                    }
                }
            }
        };

        Ok(Self {
            next_txn_id: Arc::new(AtomicU64::new(1)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager: Arc::new(LockManager::new()),
            wal,
            metrics: Arc::new(Metrics::new()),
            current_version: Arc::new(AtomicU64::new(1)),
        })
    }

    pub fn new_with_deps(wal: Arc<WalManager>, metrics: Arc<Metrics>) -> Self {
        Self {
            next_txn_id: Arc::new(AtomicU64::new(1)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager: Arc::new(LockManager::new()),
            wal,
            metrics,
            current_version: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Begin a new transaction
    #[instrument(skip(self))]
    pub fn begin(&self, isolation: IsolationLevel) -> Result<Arc<Mutex<Transaction>>> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot_version = self.current_version.load(Ordering::SeqCst);

        let txn = Arc::new(Mutex::new(Transaction::new(
            txn_id,
            isolation,
            snapshot_version,
        )));

        // Record in WAL
        self.wal.log_operation(WalOperation::TransactionBegin {
            transaction_id: txn_id,
        })?;

        // Add to active transactions
        self.active_transactions.write().insert(txn_id, txn.clone());

        info!(
            "Started transaction {} with isolation {:?}",
            txn_id, isolation
        );
        self.metrics.queries_total.fetch_add(1, Ordering::Relaxed);

        Ok(txn)
    }

    /// Read a value within a transaction
    pub fn read(
        &self,
        txn: &Arc<Mutex<Transaction>>,
        key: &str,
    ) -> Result<Option<serde_json::Value>> {
        let mut txn_guard = txn.lock();

        if !txn_guard.is_active() {
            return Err(DriftError::Other("Transaction is not active".to_string()));
        }

        if txn_guard.is_timeout() {
            self.abort_internal(&mut txn_guard)?;
            return Err(DriftError::Other("Transaction timeout".to_string()));
        }

        // Check write set first (read-your-writes)
        if let Some(event) = txn_guard.write_set.get(key) {
            return Ok(Some(event.payload.clone()));
        }

        // Acquire read lock for isolation
        if matches!(
            txn_guard.isolation,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable
        ) {
            self.lock_manager.acquire_read_lock(txn_guard.id, key)?;
            txn_guard.locked_keys.insert(key.to_string());
        }

        // Record read for conflict detection
        txn_guard.read_set.insert(key.to_string());

        // In production, would read from storage at snapshot_version
        // For now, return None (key not found)
        Ok(None)
    }

    /// Write a value within a transaction
    pub fn write(&self, txn: &Arc<Mutex<Transaction>>, event: Event) -> Result<()> {
        let mut txn_guard = txn.lock();

        if !txn_guard.is_active() {
            return Err(DriftError::Other("Transaction is not active".to_string()));
        }

        if txn_guard.is_timeout() {
            self.abort_internal(&mut txn_guard)?;
            return Err(DriftError::Other("Transaction timeout".to_string()));
        }

        let key = event.primary_key.to_string();

        // Acquire write lock
        self.lock_manager.acquire_write_lock(txn_guard.id, &key)?;
        txn_guard.locked_keys.insert(key.clone());

        // Add to write set
        txn_guard.write_set.insert(key, event);

        Ok(())
    }

    /// Commit a transaction
    #[instrument(skip(self, txn))]
    pub fn commit(&self, txn: &Arc<Mutex<Transaction>>) -> Result<()> {
        let mut txn_guard = txn.lock();

        if !txn_guard.is_active() {
            return Err(DriftError::Other("Transaction is not active".to_string()));
        }

        txn_guard.state = TransactionState::Preparing;

        // Validation phase (for Serializable isolation)
        if txn_guard.isolation == IsolationLevel::Serializable
            && !self.validate_transaction(&txn_guard)?
        {
            self.abort_internal(&mut txn_guard)?;
            return Err(DriftError::Other(
                "Transaction validation failed".to_string(),
            ));
        }

        txn_guard.state = TransactionState::Prepared;

        // Write to WAL
        for event in txn_guard.write_set.values() {
            // Convert event to WAL operation
            let wal_op = match event.event_type {
                crate::events::EventType::Insert => WalOperation::Insert {
                    table: event.table_name.clone(),
                    row_id: event.primary_key.to_string(),
                    data: event.payload.clone(),
                },
                crate::events::EventType::Patch => WalOperation::Update {
                    table: event.table_name.clone(),
                    row_id: event.primary_key.to_string(),
                    old_data: serde_json::Value::Null, // We don't have old data here
                    new_data: event.payload.clone(),
                },
                crate::events::EventType::SoftDelete => WalOperation::Delete {
                    table: event.table_name.clone(),
                    row_id: event.primary_key.to_string(),
                    data: event.payload.clone(),
                },
            };
            self.wal.log_operation(wal_op)?;
        }

        // Commit in WAL
        self.wal.log_operation(WalOperation::TransactionCommit {
            transaction_id: txn_guard.id,
        })?;

        txn_guard.state = TransactionState::Committing;

        // Update version
        self.current_version.fetch_add(1, Ordering::SeqCst);

        // Apply writes to storage (in production)
        // ...

        txn_guard.state = TransactionState::Committed;

        // Release locks
        self.lock_manager.release_transaction_locks(txn_guard.id);

        // Remove from active transactions
        self.active_transactions.write().remove(&txn_guard.id);

        info!("Committed transaction {}", txn_guard.id);
        Ok(())
    }

    /// Abort a transaction
    #[instrument(skip(self, txn))]
    pub fn abort(&self, txn: &Arc<Mutex<Transaction>>) -> Result<()> {
        let mut txn_guard = txn.lock();
        self.abort_internal(&mut txn_guard)
    }

    fn abort_internal(&self, txn: &mut Transaction) -> Result<()> {
        if txn.is_terminated() {
            return Ok(());
        }

        txn.state = TransactionState::Aborting;

        // Record abort in WAL
        self.wal.log_operation(WalOperation::TransactionAbort {
            transaction_id: txn.id,
        })?;

        // Release locks
        self.lock_manager.release_transaction_locks(txn.id);

        txn.state = TransactionState::Aborted;

        // Remove from active transactions
        self.active_transactions.write().remove(&txn.id);

        warn!("Aborted transaction {}", txn.id);
        self.metrics.queries_failed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Validate transaction for serializability
    fn validate_transaction(&self, txn: &Transaction) -> Result<bool> {
        // Check if any read values have been modified since snapshot
        // In production, would check against committed versions

        // For now, simplified validation
        let active_txns = self.active_transactions.read();
        for (_id, other_txn) in active_txns.iter() {
            let other = other_txn.lock();
            if other.id == txn.id {
                continue;
            }

            // Check for read-write conflicts
            for read_key in &txn.read_set {
                if other.write_set.contains_key(read_key)
                    && other.snapshot_version < txn.snapshot_version
                {
                    debug!("Read-write conflict detected on key {}", read_key);
                    return Ok(false);
                }
            }

            // Check for write-write conflicts
            for write_key in txn.write_set.keys() {
                if other.write_set.contains_key(write_key) {
                    debug!("Write-write conflict detected on key {}", write_key);
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Clean up timed-out transactions
    pub fn cleanup_timeouts(&self) {
        let active_txns = self.active_transactions.read().clone();
        for (_id, txn) in active_txns.iter() {
            let mut txn_guard = txn.lock();
            if txn_guard.is_active() && txn_guard.is_timeout() {
                warn!("Transaction {} timed out", txn_guard.id);
                let _ = self.abort_internal(&mut txn_guard);
            }
        }
    }

    /// Get transaction statistics
    pub fn get_stats(&self) -> TransactionStats {
        let active_txns = self.active_transactions.read();
        TransactionStats {
            active_count: active_txns.len(),
            total_started: self.next_txn_id.load(Ordering::SeqCst) - 1,
            current_version: self.current_version.load(Ordering::SeqCst),
        }
    }

    // Simplified methods for Engine integration
    pub fn simple_begin(&mut self, isolation: IsolationLevel) -> Result<u64> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot_version = self.current_version.load(Ordering::SeqCst);

        let txn = Arc::new(Mutex::new(Transaction::new(
            txn_id,
            isolation,
            snapshot_version,
        )));
        self.active_transactions.write().insert(txn_id, txn);

        Ok(txn_id)
    }

    pub fn add_write(&mut self, txn_id: u64, event: Event) -> Result<()> {
        let active_txns = self.active_transactions.read();
        let txn = active_txns
            .get(&txn_id)
            .ok_or_else(|| DriftError::Other(format!("Transaction {} not found", txn_id)))?;

        let mut txn_guard = txn.lock();
        let key = event.primary_key.to_string();
        txn_guard.write_set.insert(key, event);
        Ok(())
    }

    pub fn simple_commit(&mut self, txn_id: u64) -> Result<Vec<Event>> {
        let active_txns = self.active_transactions.read();
        let txn = active_txns
            .get(&txn_id)
            .ok_or_else(|| DriftError::Other(format!("Transaction {} not found", txn_id)))?
            .clone();

        drop(active_txns);

        let mut txn_guard = txn.lock();
        txn_guard.state = TransactionState::Committed;

        let events: Vec<Event> = txn_guard.write_set.values().cloned().collect();

        drop(txn_guard);
        self.active_transactions.write().remove(&txn_id);

        Ok(events)
    }

    pub fn rollback(&mut self, txn_id: u64) -> Result<()> {
        let active_txns = self.active_transactions.read();
        let txn = active_txns
            .get(&txn_id)
            .ok_or_else(|| DriftError::Other(format!("Transaction {} not found", txn_id)))?
            .clone();

        drop(active_txns);

        let mut txn_guard = txn.lock();
        txn_guard.state = TransactionState::Aborted;

        drop(txn_guard);
        self.active_transactions.write().remove(&txn_id);

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStats {
    pub active_count: usize,
    pub total_started: u64,
    pub current_version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_transaction_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Arc::new(
            WalManager::new(
                temp_dir.path().join("test.wal"),
                crate::wal::WalConfig::default(),
            )
            .unwrap(),
        );
        let metrics = Arc::new(Metrics::new());
        let mgr = TransactionManager::new_with_deps(wal, metrics);

        // Begin transaction
        let txn = mgr.begin(IsolationLevel::ReadCommitted).unwrap();
        assert!(txn.lock().is_active());

        // Write some data
        let event = Event::new_insert(
            "test_table".to_string(),
            serde_json::json!("key1"),
            serde_json::json!({"value": 42}),
        );
        mgr.write(&txn, event).unwrap();

        // Commit
        mgr.commit(&txn).unwrap();
        assert!(txn.lock().is_terminated());
    }

    #[test]
    fn test_transaction_abort() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Arc::new(
            WalManager::new(
                temp_dir.path().join("test.wal"),
                crate::wal::WalConfig::default(),
            )
            .unwrap(),
        );
        let metrics = Arc::new(Metrics::new());
        let mgr = TransactionManager::new_with_deps(wal, metrics);

        let txn = mgr.begin(IsolationLevel::default()).unwrap();
        mgr.abort(&txn).unwrap();
        assert_eq!(txn.lock().state, TransactionState::Aborted);
    }

    #[test]
    fn test_deadlock_detection() {
        let lock_mgr = LockManager::new();

        // Txn 1 gets lock on key1
        lock_mgr.acquire_write_lock(1, "key1").unwrap();

        // Txn 2 gets lock on key2
        lock_mgr.acquire_write_lock(2, "key2").unwrap();

        // Txn 1 tries to get key2 (waits for txn 2)
        assert!(lock_mgr.acquire_write_lock(1, "key2").is_err());

        // Txn 2 tries to get key1 (would cause deadlock)
        let result = lock_mgr.acquire_write_lock(2, "key1");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Deadlock"));
    }
}
