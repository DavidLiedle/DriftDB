//! Multi-Version Concurrency Control (MVCC) implementation
//!
//! Provides true ACID transaction support with:
//! - Snapshot isolation
//! - Read committed isolation
//! - Serializable isolation
//! - Optimistic concurrency control
//! - Deadlock detection and resolution

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use crate::errors::{DriftError, Result};

/// Transaction ID type
pub type TxnId = u64;

/// Version timestamp type
pub type VersionTimestamp = u64;

/// MVCC configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVCCConfig {
    /// Default isolation level
    pub default_isolation: IsolationLevel,
    /// Enable deadlock detection
    pub deadlock_detection: bool,
    /// Deadlock detection interval (ms)
    pub deadlock_check_interval_ms: u64,
    /// Maximum transaction duration (ms)
    pub max_transaction_duration_ms: u64,
    /// Vacuum interval for old versions (ms)
    pub vacuum_interval_ms: u64,
    /// Minimum versions to keep
    pub min_versions_to_keep: usize,
    /// Enable write conflict detection
    pub detect_write_conflicts: bool,
}

impl Default for MVCCConfig {
    fn default() -> Self {
        Self {
            default_isolation: IsolationLevel::ReadCommitted,
            deadlock_detection: true,
            deadlock_check_interval_ms: 100,
            max_transaction_duration_ms: 60000,
            vacuum_interval_ms: 5000,
            min_versions_to_keep: 100,
            detect_write_conflicts: true,
        }
    }
}

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Dirty reads allowed
    ReadUncommitted,
    /// Only committed data visible
    ReadCommitted,
    /// Repeatable reads within transaction
    RepeatableRead,
    /// Full serializability
    Serializable,
    /// Snapshot isolation
    Snapshot,
}

/// MVCC version of a record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVCCVersion {
    /// Transaction that created this version
    pub txn_id: TxnId,
    /// Timestamp when version was created
    pub timestamp: VersionTimestamp,
    /// The actual data (None means deleted)
    pub data: Option<Value>,
    /// Previous version pointer
    pub prev_version: Option<Box<MVCCVersion>>,
    /// Is this version committed
    pub committed: bool,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

/// MVCC transaction
pub struct MVCCTransaction {
    /// Transaction ID
    pub id: TxnId,
    /// Start timestamp
    pub start_timestamp: VersionTimestamp,
    /// Wall-clock time when transaction started (for timeout enforcement)
    pub start_time: std::time::Instant,
    /// Commit timestamp (if committed)
    pub commit_timestamp: Option<VersionTimestamp>,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// State
    pub state: Arc<RwLock<TransactionState>>,
    /// Read set (for validation)
    pub read_set: Arc<RwLock<HashSet<RecordId>>>,
    /// Write set
    pub write_set: Arc<RwLock<HashMap<RecordId, MVCCVersion>>>,
    /// Locks held by this transaction
    pub locks: Arc<RwLock<HashSet<RecordId>>>,
    /// Snapshot of active transactions at start
    pub snapshot: Arc<TransactionSnapshot>,
}

#[derive(Debug, Clone)]
pub struct RecordId {
    pub table: String,
    pub key: String,
}

impl std::hash::Hash for RecordId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table.hash(state);
        self.key.hash(state);
    }
}

impl PartialEq for RecordId {
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table && self.key == other.key
    }
}

impl Eq for RecordId {}

/// Transaction snapshot for MVCC visibility
#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    /// Minimum active transaction at snapshot time
    pub min_active_txn: TxnId,
    /// Maximum transaction ID at snapshot time
    pub max_txn_id: TxnId,
    /// Active transactions at snapshot time
    pub active_txns: HashSet<TxnId>,
}

impl TransactionSnapshot {
    /// Check if a transaction is visible in this snapshot
    pub fn is_visible(&self, txn_id: TxnId, committed: bool) -> bool {
        if !committed {
            return false;
        }

        if txn_id >= self.max_txn_id {
            return false; // Created after snapshot
        }

        if txn_id < self.min_active_txn {
            return true; // Committed before snapshot
        }

        !self.active_txns.contains(&txn_id)
    }
}

/// MVCC manager
pub struct MVCCManager {
    config: MVCCConfig,
    /// Next transaction ID
    next_txn_id: Arc<AtomicU64>,
    /// Current timestamp
    current_timestamp: Arc<AtomicU64>,
    /// Active transactions
    active_txns: Arc<RwLock<HashMap<TxnId, Arc<MVCCTransaction>>>>,
    /// Version store
    versions: Arc<RwLock<HashMap<RecordId, MVCCVersion>>>,
    /// Lock manager
    lock_manager: Arc<LockManager>,
    /// Deadlock detector
    deadlock_detector: Arc<DeadlockDetector>,
    /// Garbage collector
    gc_queue: Arc<Mutex<VecDeque<(RecordId, VersionTimestamp)>>>,
}

impl MVCCManager {
    pub fn new(config: MVCCConfig) -> Self {
        Self {
            config: config.clone(),
            next_txn_id: Arc::new(AtomicU64::new(1)),
            current_timestamp: Arc::new(AtomicU64::new(1)),
            active_txns: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(HashMap::new())),
            lock_manager: Arc::new(LockManager::new()),
            deadlock_detector: Arc::new(DeadlockDetector::new(config.deadlock_detection)),
            gc_queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(
        &self,
        isolation_level: IsolationLevel,
    ) -> Result<Arc<MVCCTransaction>> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let start_timestamp = self.current_timestamp.fetch_add(1, Ordering::SeqCst);

        // Create snapshot of active transactions - capture data then release lock immediately
        let snapshot = {
            let active_txns = self.active_txns.read();
            TransactionSnapshot {
                min_active_txn: active_txns.keys().min().cloned().unwrap_or(txn_id),
                max_txn_id: txn_id,
                active_txns: active_txns.keys().cloned().collect(),
            }
        }; // read lock released here

        let txn = Arc::new(MVCCTransaction {
            id: txn_id,
            start_timestamp,
            start_time: std::time::Instant::now(),
            commit_timestamp: None,
            isolation_level,
            state: Arc::new(RwLock::new(TransactionState::Active)),
            read_set: Arc::new(RwLock::new(HashSet::new())),
            write_set: Arc::new(RwLock::new(HashMap::new())),
            locks: Arc::new(RwLock::new(HashSet::new())),
            snapshot: Arc::new(snapshot),
        });

        // Register transaction
        self.active_txns.write().insert(txn_id, txn.clone());

        debug!(
            "Started transaction {} with isolation {:?}",
            txn_id, isolation_level
        );
        Ok(txn)
    }

    /// Read a record with MVCC
    pub fn read(&self, txn: &MVCCTransaction, record_id: RecordId) -> Result<Option<Value>> {
        // Enforce transaction timeout
        self.enforce_timeout(txn)?;

        // Add to read set
        txn.read_set.write().insert(record_id.clone());

        // Check write set first
        if let Some(version) = txn.write_set.read().get(&record_id) {
            return Ok(version.data.clone());
        }

        // Find visible version
        let versions = self.versions.read();
        if let Some(version) = versions.get(&record_id) {
            let visible_version = self.find_visible_version(version, txn)?;
            Ok(visible_version.and_then(|v| v.data.clone()))
        } else {
            Ok(None)
        }
    }

    /// Write a record with MVCC
    pub fn write(&self, txn: &MVCCTransaction, record_id: RecordId, data: Value) -> Result<()> {
        // Enforce transaction timeout
        self.enforce_timeout(txn)?;

        // Check transaction state
        if *txn.state.read() != TransactionState::Active {
            return Err(DriftError::Other("Transaction is not active".to_string()));
        }

        // Acquire lock for serializable isolation
        if txn.isolation_level == IsolationLevel::Serializable {
            self.lock_manager.acquire_write_lock(txn.id, &record_id)?;
            txn.locks.write().insert(record_id.clone());
        }

        // Check for write-write conflicts
        if self.config.detect_write_conflicts {
            let active_txns = self.active_txns.read();
            for (other_txn_id, other_txn) in active_txns.iter() {
                if *other_txn_id != txn.id && other_txn.write_set.read().contains_key(&record_id) {
                    return Err(DriftError::Other(format!(
                        "Write conflict on record {:?}",
                        record_id
                    )));
                }
            }
        }

        // Add to write set
        let new_version = MVCCVersion {
            txn_id: txn.id,
            timestamp: self.current_timestamp.fetch_add(1, Ordering::SeqCst),
            data: Some(data),
            prev_version: None, // Will be set on commit
            committed: false,
        };

        txn.write_set.write().insert(record_id, new_version);
        Ok(())
    }

    /// Delete a record with MVCC
    pub fn delete(&self, txn: &MVCCTransaction, record_id: RecordId) -> Result<()> {
        // Enforce transaction timeout
        self.enforce_timeout(txn)?;

        // Check transaction state
        if *txn.state.read() != TransactionState::Active {
            return Err(DriftError::Other("Transaction is not active".to_string()));
        }

        // Deletion is a write with None data
        let delete_version = MVCCVersion {
            txn_id: txn.id,
            timestamp: self.current_timestamp.fetch_add(1, Ordering::SeqCst),
            data: None,
            prev_version: None,
            committed: false,
        };

        txn.write_set.write().insert(record_id, delete_version);
        Ok(())
    }

    /// Commit a transaction
    pub fn commit(&self, txn: Arc<MVCCTransaction>) -> Result<()> {
        // Enforce transaction timeout
        self.enforce_timeout(&txn)?;

        // Change state to preparing
        *txn.state.write() = TransactionState::Preparing;

        // Validate read set for serializable isolation
        if txn.isolation_level == IsolationLevel::Serializable {
            self.validate_read_set(&txn)?;
        }

        // Get commit timestamp
        let commit_timestamp = self.current_timestamp.fetch_add(1, Ordering::SeqCst);

        // Apply write set to version store
        let mut versions = self.versions.write();
        let write_set = txn.write_set.read();

        for (record_id, new_version) in write_set.iter() {
            let mut version_to_commit = new_version.clone();
            version_to_commit.committed = true;
            version_to_commit.timestamp = commit_timestamp;

            // Link to previous version
            if let Some(prev) = versions.get(record_id) {
                version_to_commit.prev_version = Some(Box::new(prev.clone()));
            }

            versions.insert(record_id.clone(), version_to_commit);

            // Add to GC queue
            self.gc_queue
                .lock()
                .push_back((record_id.clone(), commit_timestamp));
        }

        // Release locks
        for lock in txn.locks.read().iter() {
            self.lock_manager.release_lock(txn.id, lock);
        }

        // Update transaction state
        *txn.state.write() = TransactionState::Committed;

        // Remove from active transactions
        self.active_txns.write().remove(&txn.id);

        Ok(())
    }

    /// Abort a transaction
    pub fn abort(&self, txn: Arc<MVCCTransaction>) -> Result<()> {
        // Update state
        *txn.state.write() = TransactionState::Aborted;

        // Release locks - collect lock records first to avoid holding lock while releasing
        let locks_to_release: Vec<_> = txn.locks.read().iter().cloned().collect();
        for lock in locks_to_release {
            self.lock_manager.release_lock(txn.id, &lock);
        }

        // Clear write set
        txn.write_set.write().clear();

        // Remove from active transactions
        self.active_txns.write().remove(&txn.id);

        Ok(())
    }

    /// Find visible version for a transaction
    fn find_visible_version<'a>(
        &self,
        version: &'a MVCCVersion,
        txn: &MVCCTransaction,
    ) -> Result<Option<&'a MVCCVersion>> {
        let mut current = Some(version);

        while let Some(v) = current {
            match txn.isolation_level {
                IsolationLevel::ReadUncommitted => {
                    return Ok(Some(v));
                }
                IsolationLevel::ReadCommitted => {
                    if v.committed {
                        return Ok(Some(v));
                    }
                }
                IsolationLevel::RepeatableRead | IsolationLevel::Snapshot => {
                    if txn.snapshot.is_visible(v.txn_id, v.committed) {
                        return Ok(Some(v));
                    }
                }
                IsolationLevel::Serializable => {
                    if v.txn_id == txn.id || txn.snapshot.is_visible(v.txn_id, v.committed) {
                        return Ok(Some(v));
                    }
                }
            }

            // Check previous version
            current = v.prev_version.as_deref();
        }

        Ok(None)
    }

    /// Validate read set for serializable isolation (SSI)
    /// Performs write-skew detection to ensure serializability
    fn validate_read_set(&self, txn: &MVCCTransaction) -> Result<()> {
        let versions = self.versions.read();
        let read_set = txn.read_set.read();
        let write_set = txn.write_set.read();

        // 1. Check for direct modifications to read set (basic validation)
        for record_id in read_set.iter() {
            if let Some(current_version) = versions.get(record_id) {
                // Check if version changed since read
                if current_version.timestamp > txn.start_timestamp && current_version.committed {
                    return Err(DriftError::Other(format!(
                        "Serialization failure: Record {:?} was modified by a concurrent transaction",
                        record_id
                    )));
                }
            }
        }

        // 2. Write-skew detection (SSI)
        // A write-skew occurs when:
        // - Two transactions T1 and T2 both read overlapping sets R1 and R2
        // - T1 writes to records not in R2, T2 writes to records not in R1
        // - The combined writes violate a constraint that depends on the read sets
        //
        // We detect this by checking if any concurrent transaction has:
        // - Read any record that we wrote to
        // - Written to any record that we read

        let active_txns = self.active_txns.read();
        for (other_txn_id, other_txn) in active_txns.iter() {
            if *other_txn_id == txn.id {
                continue;
            }

            // Skip transactions that started after us (they can't cause write-skew for us)
            if other_txn.start_timestamp > txn.start_timestamp {
                continue;
            }

            let other_read_set = other_txn.read_set.read();
            let other_write_set = other_txn.write_set.read();

            // Check if other transaction wrote to something we read (rw-conflict)
            for record_id in read_set.iter() {
                if other_write_set.contains_key(record_id) {
                    // Check if the write is committed or from a preparing transaction
                    let other_state = *other_txn.state.read();
                    if other_state == TransactionState::Committed
                        || other_state == TransactionState::Preparing
                    {
                        return Err(DriftError::Other(format!(
                            "Serialization failure: Write-skew detected - transaction {} wrote to {:?} which we read",
                            other_txn_id, record_id
                        )));
                    }
                }
            }

            // Check if other transaction read something we wrote (wr-conflict)
            for record_id in write_set.keys() {
                if other_read_set.contains(record_id) {
                    // This is a potential write-skew if the other transaction is also preparing/committed
                    let other_state = *other_txn.state.read();
                    if other_state == TransactionState::Preparing {
                        // Both transactions are preparing with overlapping read/write sets
                        // This is a write-skew - abort the younger transaction
                        if txn.id > *other_txn_id {
                            return Err(DriftError::Other(format!(
                                "Serialization failure: Write-skew detected - we wrote to {:?} which transaction {} read",
                                record_id, other_txn_id
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check for potential write-skew conflicts before committing
    /// This is an additional check for dangerous structure patterns
    pub fn check_write_skew_risk(&self, txn: &MVCCTransaction) -> bool {
        let read_set = txn.read_set.read();
        let write_set = txn.write_set.read();

        // A transaction has write-skew risk if it reads records without writing them
        // (predicate reads) and writes other records based on those reads
        !read_set.is_empty() && !write_set.is_empty() && {
            // Check if any read record is NOT in the write set
            read_set.iter().any(|r| !write_set.contains_key(r))
        }
    }

    /// Vacuum old versions
    pub fn vacuum(&self) -> Result<()> {
        let mut versions = self.versions.write();
        let mut gc_queue = self.gc_queue.lock();

        let min_timestamp = self.get_min_active_timestamp();

        while let Some((record_id, timestamp)) = gc_queue.front() {
            if *timestamp < min_timestamp {
                // Safe to garbage collect
                if let Some(version) = versions.get_mut(record_id) {
                    self.cleanup_old_versions(version, min_timestamp);
                }
                gc_queue.pop_front();
            } else {
                break;
            }
        }

        Ok(())
    }

    fn get_min_active_timestamp(&self) -> VersionTimestamp {
        let active_txns = self.active_txns.read();
        active_txns
            .values()
            .map(|t| t.start_timestamp)
            .min()
            .unwrap_or(self.current_timestamp.load(Ordering::SeqCst))
    }

    /// Clean up old versions that are no longer needed
    /// Uses an iterative approach to traverse the full version chain
    fn cleanup_old_versions(&self, version: &mut MVCCVersion, min_timestamp: VersionTimestamp) {
        // First, count versions and find where to truncate
        let mut count = 1;
        let mut should_truncate_at = None;

        // Traverse version chain to count and find truncation point
        {
            let mut current = version.prev_version.as_ref();
            while let Some(v) = current {
                count += 1;
                if count > self.config.min_versions_to_keep && v.timestamp < min_timestamp {
                    should_truncate_at = Some(count - 1); // Truncate after this position
                    break;
                }
                current = v.prev_version.as_ref();
            }
        }

        // If we found a truncation point, rebuild the chain without old versions
        if let Some(truncate_pos) = should_truncate_at {
            if truncate_pos == 0 {
                // Truncate immediately - remove all previous versions
                version.prev_version = None;
            } else {
                // Navigate to the truncation point and remove older versions
                let mut current = &mut version.prev_version;
                let mut pos = 0;
                while let Some(ref mut v) = current {
                    pos += 1;
                    if pos >= truncate_pos {
                        v.prev_version = None;
                        break;
                    }
                    current = &mut v.prev_version;
                }
            }
        }
    }

    /// Count total versions in a version chain
    pub fn count_version_chain(&self, record_id: &RecordId) -> usize {
        let versions = self.versions.read();
        if let Some(version) = versions.get(record_id) {
            let mut count = 1;
            let mut current = version.prev_version.as_ref();
            while let Some(v) = current {
                count += 1;
                current = v.prev_version.as_ref();
            }
            count
        } else {
            0
        }
    }

    /// Get transaction statistics
    pub fn get_stats(&self) -> MVCCStats {
        let active_txns = self.active_txns.read();
        let versions = self.versions.read();

        MVCCStats {
            active_transactions: active_txns.len(),
            total_versions: versions.len(),
            gc_queue_size: self.gc_queue.lock().len(),
        }
    }

    // ========== Storage Layer Integration ==========

    /// Export current version state for persistence
    /// Returns serializable version data that can be saved to disk
    pub fn export_version_state(&self) -> HashMap<String, Vec<MVCCVersionData>> {
        let versions = self.versions.read();
        let mut export = HashMap::new();

        for (record_id, version) in versions.iter() {
            let key = format!("{}:{}", record_id.table, record_id.key);
            let mut version_chain = Vec::new();

            // Collect all versions in the chain
            let mut current = Some(version);
            while let Some(v) = current {
                version_chain.push(MVCCVersionData {
                    txn_id: v.txn_id,
                    timestamp: v.timestamp,
                    data: v.data.clone(),
                    committed: v.committed,
                });
                current = v.prev_version.as_deref();
            }

            export.insert(key, version_chain);
        }

        export
    }

    /// Import version state from persisted data
    /// Used during recovery to restore MVCC state
    pub fn import_version_state(&self, state: HashMap<String, Vec<MVCCVersionData>>) {
        let mut versions = self.versions.write();

        for (key, version_chain) in state {
            // Parse record ID from key
            let parts: Vec<&str> = key.splitn(2, ':').collect();
            if parts.len() != 2 {
                continue;
            }

            let record_id = RecordId {
                table: parts[0].to_string(),
                key: parts[1].to_string(),
            };

            // Rebuild version chain from the data
            if let Some(head) = Self::rebuild_version_chain(version_chain) {
                versions.insert(record_id, head);
            }
        }

        // Update timestamps to be after the imported data
        let max_timestamp = versions.values().map(|v| v.timestamp).max().unwrap_or(0);
        let max_txn_id = versions.values().map(|v| v.txn_id).max().unwrap_or(0);

        self.current_timestamp
            .store(max_timestamp + 1, Ordering::SeqCst);
        self.next_txn_id.store(max_txn_id + 1, Ordering::SeqCst);
    }

    /// Rebuild a version chain from serialized data
    fn rebuild_version_chain(data: Vec<MVCCVersionData>) -> Option<MVCCVersion> {
        if data.is_empty() {
            return None;
        }

        // Build chain from back to front
        let mut chain: Option<MVCCVersion> = None;

        for version_data in data.into_iter().rev() {
            let version = MVCCVersion {
                txn_id: version_data.txn_id,
                timestamp: version_data.timestamp,
                data: version_data.data,
                prev_version: chain.map(Box::new),
                committed: version_data.committed,
            };
            chain = Some(version);
        }

        chain
    }

    /// Get the current committed state for a table
    /// This is useful for syncing MVCC state with the persistent storage
    pub fn get_committed_state(&self, table: &str) -> HashMap<String, Value> {
        let versions = self.versions.read();
        let mut state = HashMap::new();

        for (record_id, version) in versions.iter() {
            if record_id.table != table {
                continue;
            }

            // Find the latest committed version
            let mut current = Some(version);
            while let Some(v) = current {
                if v.committed {
                    if let Some(ref data) = v.data {
                        state.insert(record_id.key.clone(), data.clone());
                    }
                    break;
                }
                current = v.prev_version.as_deref();
            }
        }

        state
    }

    /// Sync MVCC state from storage layer
    /// Loads committed state from persistent storage into MVCC
    pub fn sync_from_storage(&self, table: &str, storage_state: HashMap<String, Value>) {
        let mut versions = self.versions.write();
        let timestamp = self.current_timestamp.fetch_add(1, Ordering::SeqCst);

        for (key, data) in storage_state {
            let record_id = RecordId {
                table: table.to_string(),
                key,
            };

            let version = MVCCVersion {
                txn_id: 0, // System transaction
                timestamp,
                data: Some(data),
                prev_version: versions.get(&record_id).map(|v| Box::new(v.clone())),
                committed: true,
            };

            versions.insert(record_id, version);
        }
    }

    /// Clear all MVCC state (for testing or reset)
    pub fn clear(&self) {
        self.versions.write().clear();
        self.active_txns.write().clear();
        self.gc_queue.lock().clear();
        self.next_txn_id.store(1, Ordering::SeqCst);
        self.current_timestamp.store(1, Ordering::SeqCst);
    }

    /// Check if a transaction has exceeded its timeout
    pub fn check_transaction_timeout(&self, txn: &MVCCTransaction) -> bool {
        let elapsed = txn.start_time.elapsed();
        let max_duration =
            std::time::Duration::from_millis(self.config.max_transaction_duration_ms);
        elapsed > max_duration
    }

    /// Check and abort timed-out transactions
    /// Returns the IDs of aborted transactions
    pub fn check_timeouts(&self) -> Vec<TxnId> {
        let mut timed_out = Vec::new();
        let max_duration =
            std::time::Duration::from_millis(self.config.max_transaction_duration_ms);

        // Collect timed-out transactions
        let txns_to_check: Vec<(TxnId, Arc<MVCCTransaction>)> = {
            let active_txns = self.active_txns.read();
            active_txns
                .iter()
                .map(|(id, txn)| (*id, txn.clone()))
                .collect()
        };

        for (txn_id, txn) in txns_to_check {
            if txn.start_time.elapsed() > max_duration
                && *txn.state.read() == TransactionState::Active
            {
                timed_out.push(txn_id);
                let _ = self.abort(txn);
            }
        }

        timed_out
    }

    /// Enforce transaction timeout - call this before any operation
    fn enforce_timeout(&self, txn: &MVCCTransaction) -> Result<()> {
        if self.check_transaction_timeout(txn) {
            return Err(DriftError::Other(format!(
                "Transaction {} has exceeded maximum duration of {}ms",
                txn.id, self.config.max_transaction_duration_ms
            )));
        }
        Ok(())
    }

    /// Run deadlock detection and return any detected cycles
    pub fn detect_deadlocks(&self) -> Vec<Vec<TxnId>> {
        let wait_graph = self.lock_manager.wait_graph.read();
        self.deadlock_detector.detect_deadlocks(&wait_graph)
    }

    /// Run deadlock detection and abort victim transactions
    /// Returns the IDs of aborted transactions
    pub fn resolve_deadlocks(&self) -> Vec<TxnId> {
        if !self.config.deadlock_detection {
            return Vec::new();
        }

        let cycles = self.detect_deadlocks();
        let mut aborted = Vec::new();

        for cycle in cycles {
            if let Some(victim_id) = self.deadlock_detector.select_victim(&cycle) {
                if let Some(victim_txn) = self.active_txns.read().get(&victim_id).cloned() {
                    if self.abort(victim_txn).is_ok() {
                        aborted.push(victim_id);
                    }
                }
            }
        }

        aborted
    }

    /// Get configuration
    pub fn config(&self) -> &MVCCConfig {
        &self.config
    }
}

/// Lock manager for pessimistic locking
struct LockManager {
    locks: Arc<RwLock<HashMap<RecordId, LockInfo>>>,
    wait_graph: Arc<RwLock<HashMap<TxnId, HashSet<TxnId>>>>,
}

#[derive(Debug, Clone)]
struct LockInfo {
    mode: LockMode,
    holders: HashSet<TxnId>,
    waiters: VecDeque<(TxnId, LockMode)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum LockMode {
    Shared,
    Exclusive,
}

impl LockManager {
    fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            wait_graph: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn acquire_write_lock(&self, txn_id: TxnId, record_id: &RecordId) -> Result<()> {
        self.acquire_lock(txn_id, record_id, LockMode::Exclusive)
    }

    fn acquire_lock(&self, txn_id: TxnId, record_id: &RecordId, mode: LockMode) -> Result<()> {
        let mut locks = self.locks.write();

        let lock_info = locks.entry(record_id.clone()).or_insert_with(|| LockInfo {
            mode: LockMode::Shared,
            holders: HashSet::new(),
            waiters: VecDeque::new(),
        });

        // Check compatibility
        if lock_info.holders.is_empty() {
            // No holders, grant immediately
            lock_info.holders.insert(txn_id);
            lock_info.mode = mode;
            Ok(())
        } else if lock_info.holders.contains(&txn_id) {
            // Already holds lock
            if mode == LockMode::Exclusive && lock_info.mode == LockMode::Shared {
                // Upgrade lock
                if lock_info.holders.len() == 1 {
                    lock_info.mode = LockMode::Exclusive;
                    Ok(())
                } else {
                    // Wait for other holders
                    lock_info.waiters.push_back((txn_id, mode));
                    Err(DriftError::Other("Lock upgrade blocked".to_string()))
                }
            } else {
                Ok(())
            }
        } else if mode == LockMode::Shared && lock_info.mode == LockMode::Shared {
            // Compatible shared lock
            lock_info.holders.insert(txn_id);
            Ok(())
        } else {
            // Incompatible, must wait
            lock_info.waiters.push_back((txn_id, mode));

            // Update wait graph for deadlock detection
            let mut wait_graph = self.wait_graph.write();
            let waiting_for = wait_graph.entry(txn_id).or_default();
            waiting_for.extend(&lock_info.holders);

            Err(DriftError::Other("Lock acquisition blocked".to_string()))
        }
    }

    fn release_lock(&self, txn_id: TxnId, record_id: &RecordId) {
        let mut locks = self.locks.write();

        if let Some(lock_info) = locks.get_mut(record_id) {
            lock_info.holders.remove(&txn_id);

            // Grant lock to waiters if possible
            if lock_info.holders.is_empty() && !lock_info.waiters.is_empty() {
                if let Some((next_txn, next_mode)) = lock_info.waiters.pop_front() {
                    lock_info.holders.insert(next_txn);
                    lock_info.mode = next_mode;

                    // Update wait graph
                    self.wait_graph.write().remove(&next_txn);
                }
            }

            // Remove lock entry if no holders or waiters
            if lock_info.holders.is_empty() && lock_info.waiters.is_empty() {
                locks.remove(record_id);
            }
        }

        // Clean up wait graph
        self.wait_graph.write().remove(&txn_id);
    }
}

/// Deadlock detector using wait-for graph cycle detection
pub struct DeadlockDetector {
    enabled: bool,
}

impl DeadlockDetector {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// Check if deadlock detection is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Detect deadlocks in the wait-for graph
    /// Returns a list of cycles (each cycle is a list of transaction IDs)
    pub fn detect_deadlocks(&self, wait_graph: &HashMap<TxnId, HashSet<TxnId>>) -> Vec<Vec<TxnId>> {
        if !self.enabled {
            return Vec::new();
        }

        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        let mut path = Vec::new();

        for &node in wait_graph.keys() {
            if !visited.contains(&node) {
                if let Some(cycle) =
                    Self::dfs_find_cycle(node, wait_graph, &mut visited, &mut stack, &mut path)
                {
                    cycles.push(cycle);
                }
            }
        }

        cycles
    }

    /// DFS-based cycle detection with path tracking
    fn dfs_find_cycle(
        node: TxnId,
        graph: &HashMap<TxnId, HashSet<TxnId>>,
        visited: &mut HashSet<TxnId>,
        stack: &mut HashSet<TxnId>,
        path: &mut Vec<TxnId>,
    ) -> Option<Vec<TxnId>> {
        visited.insert(node);
        stack.insert(node);
        path.push(node);

        if let Some(neighbors) = graph.get(&node) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if let Some(cycle) = Self::dfs_find_cycle(neighbor, graph, visited, stack, path)
                    {
                        return Some(cycle);
                    }
                } else if stack.contains(&neighbor) {
                    // Found cycle - extract the cycle from the path
                    let cycle_start = path.iter().position(|&n| n == neighbor).unwrap_or(0);
                    let mut cycle: Vec<TxnId> = path[cycle_start..].to_vec();
                    cycle.push(neighbor); // Close the cycle
                    return Some(cycle);
                }
            }
        }

        stack.remove(&node);
        path.pop();
        None
    }

    /// Select a victim transaction to abort in order to break the deadlock
    /// Strategy: abort the youngest transaction (highest ID) to minimize wasted work
    pub fn select_victim(&self, cycle: &[TxnId]) -> Option<TxnId> {
        cycle.iter().max().copied()
    }
}

/// MVCC statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVCCStats {
    pub active_transactions: usize,
    pub total_versions: usize,
    pub gc_queue_size: usize,
}

/// Serializable version data for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVCCVersionData {
    pub txn_id: TxnId,
    pub timestamp: VersionTimestamp,
    pub data: Option<Value>,
    pub committed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_basic_operations() {
        let config = MVCCConfig {
            detect_write_conflicts: false, // Disable to avoid lock contention in test
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Start transaction
        let txn1 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Write data
        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        mvcc.write(
            &txn1,
            record_id.clone(),
            Value::String("value1".to_string()),
        )
        .unwrap();

        // Read uncommitted data
        let txn2 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let read_result = mvcc.read(&txn2, record_id.clone()).unwrap();
        assert!(read_result.is_none()); // Shouldn't see uncommitted data

        // Commit first transaction
        mvcc.commit(txn1).unwrap();

        // Now should see committed data
        let read_result = mvcc.read(&txn2, record_id.clone()).unwrap();
        assert_eq!(read_result, Some(Value::String("value1".to_string())));
    }

    #[test]
    fn test_snapshot_isolation() {
        let config = MVCCConfig {
            detect_write_conflicts: false, // Disable to avoid lock contention in test
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record_id = RecordId {
            table: "test".to_string(),
            key: "counter".to_string(),
        };

        // Initial value
        let txn0 = mvcc.begin_transaction(IsolationLevel::Snapshot).unwrap();
        mvcc.write(
            &txn0,
            record_id.clone(),
            Value::Number(serde_json::Number::from(0)),
        )
        .unwrap();
        mvcc.commit(txn0).unwrap();

        // Two concurrent transactions
        let txn1 = mvcc.begin_transaction(IsolationLevel::Snapshot).unwrap();
        let txn2 = mvcc.begin_transaction(IsolationLevel::Snapshot).unwrap();

        // Both read the same value
        let val1 = mvcc.read(&txn1, record_id.clone()).unwrap();
        let val2 = mvcc.read(&txn2, record_id.clone()).unwrap();
        assert_eq!(val1, val2);

        // Both try to increment
        mvcc.write(
            &txn1,
            record_id.clone(),
            Value::Number(serde_json::Number::from(1)),
        )
        .unwrap();
        mvcc.commit(txn1).unwrap();

        // txn2 should still see old value due to snapshot isolation
        let val2_again = mvcc.read(&txn2, record_id.clone()).unwrap();
        assert_eq!(val2_again, Some(Value::Number(serde_json::Number::from(0))));
    }

    // ==================== Transaction Timeout Tests ====================

    #[test]
    fn test_transaction_timeout_detection() {
        let config = MVCCConfig {
            max_transaction_duration_ms: 1, // 1ms timeout for testing
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Transaction should not be timed out immediately
        assert!(!mvcc.check_transaction_timeout(&txn));

        // Wait for timeout (use longer sleep to be safe)
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Now it should be timed out
        assert!(mvcc.check_transaction_timeout(&txn));
    }

    #[test]
    fn test_transaction_timeout_enforcement_on_read() {
        let config = MVCCConfig {
            max_transaction_duration_ms: 1,
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Wait for timeout
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Read should fail due to timeout
        let result = mvcc.read(&txn, record_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeded"));
    }

    #[test]
    fn test_transaction_timeout_enforcement_on_write() {
        let config = MVCCConfig {
            max_transaction_duration_ms: 1,
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Wait for timeout
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Write should fail due to timeout
        let result = mvcc.write(&txn, record_id, Value::String("test".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeded"));
    }

    #[test]
    fn test_transaction_timeout_enforcement_on_commit() {
        let config = MVCCConfig {
            max_transaction_duration_ms: 50, // Longer so we can write first
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Write before timeout
        mvcc.write(&txn, record_id, Value::String("test".to_string()))
            .unwrap();

        // Wait for timeout
        std::thread::sleep(std::time::Duration::from_millis(60));

        // Commit should fail due to timeout
        let result = mvcc.commit(txn);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeded"));
    }

    #[test]
    fn test_check_timeouts_aborts_old_transactions() {
        let config = MVCCConfig {
            max_transaction_duration_ms: 1,
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Start some transactions
        let _txn1 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let _txn2 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Verify we have active transactions
        assert_eq!(mvcc.get_stats().active_transactions, 2);

        // Wait for timeout
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Check timeouts should abort both
        let aborted = mvcc.check_timeouts();
        assert_eq!(aborted.len(), 2);

        // No more active transactions
        assert_eq!(mvcc.get_stats().active_transactions, 0);
    }

    // ==================== Deadlock Detection Tests ====================

    #[test]
    fn test_deadlock_detector_no_cycle() {
        let detector = DeadlockDetector::new(true);

        // Linear wait chain: T1 -> T2 -> T3
        let mut wait_graph = HashMap::new();
        wait_graph.insert(1, vec![2].into_iter().collect());
        wait_graph.insert(2, vec![3].into_iter().collect());

        let cycles = detector.detect_deadlocks(&wait_graph);
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_deadlock_detector_simple_cycle() {
        let detector = DeadlockDetector::new(true);

        // Simple cycle: T1 -> T2 -> T1
        let mut wait_graph = HashMap::new();
        wait_graph.insert(1, vec![2].into_iter().collect());
        wait_graph.insert(2, vec![1].into_iter().collect());

        let cycles = detector.detect_deadlocks(&wait_graph);
        assert_eq!(cycles.len(), 1);
        // Cycle should contain both transactions
        let cycle = &cycles[0];
        assert!(cycle.contains(&1) || cycle.contains(&2));
    }

    #[test]
    fn test_deadlock_detector_complex_cycle() {
        let detector = DeadlockDetector::new(true);

        // Cycle: T1 -> T2 -> T3 -> T1
        let mut wait_graph = HashMap::new();
        wait_graph.insert(1, vec![2].into_iter().collect());
        wait_graph.insert(2, vec![3].into_iter().collect());
        wait_graph.insert(3, vec![1].into_iter().collect());

        let cycles = detector.detect_deadlocks(&wait_graph);
        assert!(!cycles.is_empty());
    }

    #[test]
    fn test_deadlock_detector_disabled() {
        let detector = DeadlockDetector::new(false);

        // Even with a cycle, disabled detector returns empty
        let mut wait_graph = HashMap::new();
        wait_graph.insert(1, vec![2].into_iter().collect());
        wait_graph.insert(2, vec![1].into_iter().collect());

        let cycles = detector.detect_deadlocks(&wait_graph);
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_deadlock_victim_selection() {
        let detector = DeadlockDetector::new(true);

        // Cycle with transactions 1, 5, 3
        let cycle = vec![1, 5, 3];

        // Should select the youngest (highest ID) as victim
        let victim = detector.select_victim(&cycle);
        assert_eq!(victim, Some(5));
    }

    #[test]
    fn test_deadlock_victim_selection_empty() {
        let detector = DeadlockDetector::new(true);

        let victim = detector.select_victim(&[]);
        assert!(victim.is_none());
    }

    #[test]
    fn test_mvcc_manager_detect_deadlocks() {
        let config = MVCCConfig {
            deadlock_detection: true,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Without any waiting transactions, no deadlocks
        let cycles = mvcc.detect_deadlocks();
        assert!(cycles.is_empty());
    }

    #[test]
    fn test_mvcc_manager_resolve_deadlocks_disabled() {
        let config = MVCCConfig {
            deadlock_detection: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // With detection disabled, should return empty
        let aborted = mvcc.resolve_deadlocks();
        assert!(aborted.is_empty());
    }

    // ==================== Transaction State Tests ====================

    #[test]
    fn test_write_on_inactive_transaction_fails() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Abort the transaction
        mvcc.abort(txn.clone()).unwrap();

        // Write should fail
        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };
        let result = mvcc.write(&txn, record_id, Value::String("test".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not active"));
    }

    #[test]
    fn test_delete_on_inactive_transaction_fails() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        // Abort the transaction
        mvcc.abort(txn.clone()).unwrap();

        // Delete should fail
        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };
        let result = mvcc.delete(&txn, record_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not active"));
    }

    #[test]
    fn test_mvcc_delete_operation() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Insert a record
        let txn1 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        mvcc.write(&txn1, record_id.clone(), Value::String("value".to_string()))
            .unwrap();
        mvcc.commit(txn1).unwrap();

        // Delete the record
        let txn2 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        mvcc.delete(&txn2, record_id.clone()).unwrap();
        mvcc.commit(txn2).unwrap();

        // Read should return None
        let txn3 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let result = mvcc.read(&txn3, record_id).unwrap();
        assert!(result.is_none());
    }

    // ==================== Statistics Tests ====================

    #[test]
    fn test_mvcc_stats() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Initial stats
        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 0);
        assert_eq!(stats.total_versions, 0);

        // Start a transaction
        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 1);

        // Write and commit
        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };
        mvcc.write(&txn, record_id, Value::String("value".to_string()))
            .unwrap();
        mvcc.commit(txn).unwrap();

        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 0);
        assert_eq!(stats.total_versions, 1);
    }

    // ==================== Configuration Tests ====================

    #[test]
    fn test_mvcc_config_default() {
        let config = MVCCConfig::default();
        assert_eq!(config.default_isolation, IsolationLevel::ReadCommitted);
        assert!(config.deadlock_detection);
        assert_eq!(config.deadlock_check_interval_ms, 100);
        assert_eq!(config.max_transaction_duration_ms, 60000);
        assert_eq!(config.vacuum_interval_ms, 5000);
        assert_eq!(config.min_versions_to_keep, 100);
        assert!(config.detect_write_conflicts);
    }

    #[test]
    fn test_mvcc_manager_config_access() {
        let config = MVCCConfig {
            max_transaction_duration_ms: 30000,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        assert_eq!(mvcc.config().max_transaction_duration_ms, 30000);
    }

    // ==================== Snapshot Visibility Tests ====================

    #[test]
    fn test_transaction_snapshot_visibility() {
        let snapshot = TransactionSnapshot {
            min_active_txn: 5,
            max_txn_id: 10,
            active_txns: vec![6, 8].into_iter().collect(),
        };

        // Committed before snapshot - visible
        assert!(snapshot.is_visible(3, true));

        // Uncommitted - not visible
        assert!(!snapshot.is_visible(3, false));

        // Created after snapshot - not visible
        assert!(!snapshot.is_visible(11, true));

        // Active at snapshot time - not visible
        assert!(!snapshot.is_visible(6, true));
        assert!(!snapshot.is_visible(8, true));

        // Committed between min_active and max, not in active set - visible
        assert!(snapshot.is_visible(7, true));
    }

    // ==================== Write Conflict Tests ====================

    #[test]
    fn test_write_write_conflict_detection() {
        let config = MVCCConfig {
            detect_write_conflicts: true,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // First transaction writes
        let txn1 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        mvcc.write(
            &txn1,
            record_id.clone(),
            Value::String("value1".to_string()),
        )
        .unwrap();

        // Second transaction tries to write same record - should fail
        let txn2 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let result = mvcc.write(&txn2, record_id, Value::String("value2".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("conflict"));
    }

    #[test]
    fn test_write_conflict_detection_disabled() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // First transaction writes
        let txn1 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        mvcc.write(
            &txn1,
            record_id.clone(),
            Value::String("value1".to_string()),
        )
        .unwrap();

        // Second transaction can also write (conflict detection disabled)
        let txn2 = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let result = mvcc.write(&txn2, record_id, Value::String("value2".to_string()));
        assert!(result.is_ok());
    }

    // ==================== Version Chain & GC Tests ====================

    #[test]
    fn test_version_chain_count() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Create multiple versions
        for i in 0..5 {
            let txn = mvcc
                .begin_transaction(IsolationLevel::ReadCommitted)
                .unwrap();
            mvcc.write(
                &txn,
                record_id.clone(),
                Value::Number(serde_json::Number::from(i)),
            )
            .unwrap();
            mvcc.commit(txn).unwrap();
        }

        // Should have 5 versions in the chain
        let count = mvcc.count_version_chain(&record_id);
        assert_eq!(count, 5);
    }

    #[test]
    fn test_vacuum_cleans_old_versions() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            min_versions_to_keep: 2, // Keep at least 2 versions
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Create 5 versions
        for i in 0..5 {
            let txn = mvcc
                .begin_transaction(IsolationLevel::ReadCommitted)
                .unwrap();
            mvcc.write(
                &txn,
                record_id.clone(),
                Value::Number(serde_json::Number::from(i)),
            )
            .unwrap();
            mvcc.commit(txn).unwrap();
        }

        // Run vacuum
        mvcc.vacuum().unwrap();

        // Should still be able to read the latest value
        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let value = mvcc.read(&txn, record_id).unwrap();
        assert_eq!(value, Some(Value::Number(serde_json::Number::from(4))));
    }

    // ==================== Storage Integration Tests ====================

    #[test]
    fn test_export_import_version_state() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config.clone());

        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Write some data
        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        mvcc.write(
            &txn,
            record_id.clone(),
            Value::String("test_value".to_string()),
        )
        .unwrap();
        mvcc.commit(txn).unwrap();

        // Export state
        let exported = mvcc.export_version_state();
        assert!(!exported.is_empty());

        // Create a new MVCC manager and import
        let mvcc2 = MVCCManager::new(config);
        mvcc2.import_version_state(exported);

        // Should be able to read the imported data
        let txn = mvcc2
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        let value = mvcc2.read(&txn, record_id).unwrap();
        assert_eq!(value, Some(Value::String("test_value".to_string())));
    }

    #[test]
    fn test_get_committed_state() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Write data to multiple records
        for i in 0..3 {
            let record_id = RecordId {
                table: "test".to_string(),
                key: format!("key{}", i),
            };
            let txn = mvcc
                .begin_transaction(IsolationLevel::ReadCommitted)
                .unwrap();
            mvcc.write(&txn, record_id, Value::Number(serde_json::Number::from(i)))
                .unwrap();
            mvcc.commit(txn).unwrap();
        }

        // Get committed state
        let state = mvcc.get_committed_state("test");
        assert_eq!(state.len(), 3);
        assert_eq!(
            state.get("key0"),
            Some(&Value::Number(serde_json::Number::from(0)))
        );
        assert_eq!(
            state.get("key1"),
            Some(&Value::Number(serde_json::Number::from(1)))
        );
        assert_eq!(
            state.get("key2"),
            Some(&Value::Number(serde_json::Number::from(2)))
        );
    }

    #[test]
    fn test_sync_from_storage() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Simulate loading state from storage
        let mut storage_state = HashMap::new();
        storage_state.insert("key1".to_string(), Value::String("value1".to_string()));
        storage_state.insert("key2".to_string(), Value::String("value2".to_string()));

        mvcc.sync_from_storage("test", storage_state);

        // Should be able to read the synced data
        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();

        let record1 = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };
        let value1 = mvcc.read(&txn, record1).unwrap();
        assert_eq!(value1, Some(Value::String("value1".to_string())));

        let record2 = RecordId {
            table: "test".to_string(),
            key: "key2".to_string(),
        };
        let value2 = mvcc.read(&txn, record2).unwrap();
        assert_eq!(value2, Some(Value::String("value2".to_string())));
    }

    #[test]
    fn test_clear_mvcc_state() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        // Write some data
        let record_id = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };
        let txn = mvcc
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        mvcc.write(
            &txn,
            record_id.clone(),
            Value::String("test_value".to_string()),
        )
        .unwrap();
        mvcc.commit(txn).unwrap();

        // Clear state
        mvcc.clear();

        // Should be empty
        let stats = mvcc.get_stats();
        assert_eq!(stats.active_transactions, 0);
        assert_eq!(stats.total_versions, 0);
        assert_eq!(stats.gc_queue_size, 0);
    }

    // ==================== Write-Skew Detection Tests ====================

    #[test]
    fn test_check_write_skew_risk() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record1 = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };
        let record2 = RecordId {
            table: "test".to_string(),
            key: "key2".to_string(),
        };

        // Transaction that reads and writes different records has write-skew risk
        let txn = mvcc
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Read record1
        let _ = mvcc.read(&txn, record1);

        // Write to record2
        mvcc.write(&txn, record2, Value::String("value".to_string()))
            .unwrap();

        // Should have write-skew risk
        assert!(mvcc.check_write_skew_risk(&txn));
    }

    #[test]
    fn test_no_write_skew_risk_when_writing_same_record() {
        let config = MVCCConfig {
            detect_write_conflicts: false,
            ..Default::default()
        };
        let mvcc = MVCCManager::new(config);

        let record = RecordId {
            table: "test".to_string(),
            key: "key1".to_string(),
        };

        // Transaction that reads and writes the same record has no write-skew risk
        let txn = mvcc
            .begin_transaction(IsolationLevel::Serializable)
            .unwrap();

        // Read and write same record
        let _ = mvcc.read(&txn, record.clone());
        mvcc.write(&txn, record, Value::String("value".to_string()))
            .unwrap();

        // Should NOT have write-skew risk (reading and writing same record)
        assert!(!mvcc.check_write_skew_risk(&txn));
    }
}
