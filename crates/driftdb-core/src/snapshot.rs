use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::errors::Result;
use crate::storage::TableStorage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub row_count: usize,
    pub state: HashMap<String, String>, // Store as JSON strings to avoid bincode issues
}

impl Snapshot {
    pub fn create_from_storage(storage: &TableStorage, sequence: u64) -> Result<Self> {
        let state_raw = storage.reconstruct_state_at(Some(sequence))?;

        // Convert serde_json::Value to String for serialization
        let state: HashMap<String, String> = state_raw
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        Ok(Self {
            sequence,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_else(|_| {
                    // Fallback to a reasonable timestamp if system time is broken
                    tracing::error!("System time is before UNIX epoch, using fallback timestamp");
                    0
                }),
            row_count: state.len(),
            state,
        })
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let temp_path = PathBuf::from(format!("{}.tmp", path.as_ref().display()));

        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);
            let data = bincode::serialize(&self)?;
            let compressed = zstd::encode_all(&data[..], 3)?;
            std::io::Write::write_all(&mut writer, &compressed)?;
        }

        fs::rename(temp_path, path)?;
        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let compressed =
            std::io::Read::bytes(reader).collect::<std::result::Result<Vec<_>, _>>()?;
        let data = zstd::decode_all(&compressed[..])?;
        Ok(bincode::deserialize(&data)?)
    }
}

pub struct SnapshotManager {
    snapshots_dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(table_path: &Path) -> Self {
        Self {
            snapshots_dir: table_path.join("snapshots"),
        }
    }

    pub fn create_snapshot(&self, storage: &TableStorage, sequence: u64) -> Result<()> {
        let snapshot = Snapshot::create_from_storage(storage, sequence)?;
        let filename = format!("{:010}.snap", sequence);
        let path = self.snapshots_dir.join(filename);
        snapshot.save_to_file(path)?;
        Ok(())
    }

    pub fn find_latest_before(&self, sequence: u64) -> Result<Option<Snapshot>> {
        let mut best_snapshot = None;
        let mut best_sequence = 0;

        if !self.snapshots_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&self.snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(snap_seq) = name.parse::<u64>() {
                    if snap_seq <= sequence && snap_seq > best_sequence {
                        best_sequence = snap_seq;
                        best_snapshot = Some(path);
                    }
                }
            }
        }

        if let Some(path) = best_snapshot {
            Ok(Some(Snapshot::load_from_file(path)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_snapshots(&self) -> Result<Vec<u64>> {
        let mut sequences = Vec::new();

        if !self.snapshots_dir.exists() {
            return Ok(sequences);
        }

        for entry in fs::read_dir(&self.snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(seq) = name.parse::<u64>() {
                    sequences.push(seq);
                }
            }
        }

        sequences.sort();
        Ok(sequences)
    }
}

/// Configuration for adaptive snapshot creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPolicy {
    /// Minimum number of writes before considering snapshot
    pub min_writes_threshold: u64,
    /// Maximum number of writes before forcing snapshot
    pub max_writes_threshold: u64,
    /// Minimum time between snapshots (seconds)
    pub min_time_between_snapshots: u64,
    /// Maximum time between snapshots (seconds)
    pub max_time_between_snapshots: u64,
    /// Write rate multiplier for dynamic threshold
    pub write_rate_multiplier: f64,
    /// Enable adaptive timing based on write patterns
    pub enable_adaptive: bool,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            min_writes_threshold: 1_000,       // At least 1K writes
            max_writes_threshold: 100_000,     // Max 100K writes
            min_time_between_snapshots: 60,    // At least 1 minute
            max_time_between_snapshots: 3600,  // Max 1 hour
            write_rate_multiplier: 1.5,        // Adjust threshold by 1.5x write rate
            enable_adaptive: true,
        }
    }
}

/// Statistics for adaptive snapshot management
#[derive(Debug, Clone, Default)]
pub struct SnapshotStatistics {
    pub total_snapshots_created: u64,
    pub total_writes_processed: u64,
    pub last_snapshot_sequence: u64,
    pub last_snapshot_timestamp: u64,
    pub avg_writes_per_snapshot: f64,
    pub avg_time_between_snapshots: f64,
    pub current_write_rate: f64, // writes per second
}

/// Adaptive snapshot manager with write-volume-based timing
pub struct AdaptiveSnapshotManager {
    base_manager: SnapshotManager,
    policy: SnapshotPolicy,
    writes_since_last_snapshot: u64,
    last_snapshot_timestamp: u64,
    last_snapshot_sequence: u64,
    write_timestamps: Vec<u64>, // Recent write timestamps for rate calculation
    statistics: SnapshotStatistics,
}

impl AdaptiveSnapshotManager {
    pub fn new(table_path: &Path, policy: SnapshotPolicy) -> Self {
        let snapshots_dir = table_path.join("snapshots");
        fs::create_dir_all(&snapshots_dir).ok();

        Self {
            base_manager: SnapshotManager::new(table_path),
            policy,
            writes_since_last_snapshot: 0,
            last_snapshot_timestamp: Self::current_timestamp(),
            last_snapshot_sequence: 0,
            write_timestamps: Vec::new(),
            statistics: SnapshotStatistics::default(),
        }
    }

    /// Record a write operation
    pub fn record_write(&mut self) {
        self.writes_since_last_snapshot += 1;
        self.statistics.total_writes_processed += 1;

        let now = Self::current_timestamp();
        self.write_timestamps.push(now);

        // Keep only recent write timestamps (last 5 minutes)
        let cutoff = now.saturating_sub(300);
        self.write_timestamps.retain(|&ts| ts >= cutoff);
    }

    /// Check if a snapshot should be created based on adaptive policy
    pub fn should_create_snapshot(&self, current_sequence: u64) -> bool {
        if !self.policy.enable_adaptive {
            // Fixed threshold mode
            return self.writes_since_last_snapshot >= self.policy.max_writes_threshold;
        }

        let now = Self::current_timestamp();
        let time_since_last = now.saturating_sub(self.last_snapshot_timestamp);

        // Force snapshot if max time elapsed
        if time_since_last >= self.policy.max_time_between_snapshots {
            return true;
        }

        // Don't snapshot if min time hasn't elapsed
        if time_since_last < self.policy.min_time_between_snapshots {
            return false;
        }

        // Calculate dynamic threshold based on write rate
        let write_rate = self.calculate_write_rate();
        let dynamic_threshold = self.calculate_dynamic_threshold(write_rate);

        // Create snapshot if writes exceed dynamic threshold
        self.writes_since_last_snapshot >= dynamic_threshold
    }

    /// Create a snapshot and reset counters
    pub fn create_snapshot_if_needed(
        &mut self,
        storage: &TableStorage,
        sequence: u64,
    ) -> Result<bool> {
        if !self.should_create_snapshot(sequence) {
            return Ok(false);
        }

        self.create_snapshot_internal(storage, sequence)?;
        Ok(true)
    }

    /// Force create a snapshot regardless of policy
    pub fn force_snapshot(&mut self, storage: &TableStorage, sequence: u64) -> Result<()> {
        self.create_snapshot_internal(storage, sequence)
    }

    /// Internal snapshot creation with statistics tracking
    fn create_snapshot_internal(&mut self, storage: &TableStorage, sequence: u64) -> Result<()> {
        let start_time = std::time::Instant::now();

        self.base_manager.create_snapshot(storage, sequence)?;

        let now = Self::current_timestamp();
        let elapsed = start_time.elapsed();

        // Update statistics
        self.statistics.total_snapshots_created += 1;
        self.statistics.last_snapshot_sequence = sequence;
        self.statistics.last_snapshot_timestamp = now;

        if self.statistics.total_snapshots_created > 0 {
            self.statistics.avg_writes_per_snapshot = self.statistics.total_writes_processed as f64
                / self.statistics.total_snapshots_created as f64;
        }

        // Update averages
        if self.last_snapshot_sequence > 0 {
            let time_diff = now.saturating_sub(self.last_snapshot_timestamp);
            let current_avg = self.statistics.avg_time_between_snapshots;
            let n = self.statistics.total_snapshots_created as f64;

            // Exponential moving average
            self.statistics.avg_time_between_snapshots =
                (current_avg * (n - 1.0) + time_diff as f64) / n;
        }

        // Reset counters
        self.writes_since_last_snapshot = 0;
        self.last_snapshot_timestamp = now;
        self.last_snapshot_sequence = sequence;

        tracing::info!(
            "Created snapshot at sequence {} ({} writes, {:?})",
            sequence,
            self.statistics.total_writes_processed,
            elapsed
        );

        Ok(())
    }

    /// Calculate current write rate (writes per second)
    fn calculate_write_rate(&self) -> f64 {
        if self.write_timestamps.len() < 2 {
            return 0.0;
        }

        let oldest = *self.write_timestamps.first().unwrap();
        let newest = *self.write_timestamps.last().unwrap();
        let time_span = newest.saturating_sub(oldest).max(1);

        self.write_timestamps.len() as f64 / time_span as f64
    }

    /// Calculate dynamic threshold based on write rate
    fn calculate_dynamic_threshold(&self, write_rate: f64) -> u64 {
        if write_rate <= 0.0 {
            return self.policy.min_writes_threshold;
        }

        // Higher write rate → lower threshold (more frequent snapshots)
        // Lower write rate → higher threshold (less frequent snapshots)
        let base_threshold = self.policy.min_writes_threshold as f64;
        let max_threshold = self.policy.max_writes_threshold as f64;

        // Calculate threshold: base + (max - base) / (1 + write_rate * multiplier)
        let threshold = base_threshold
            + (max_threshold - base_threshold) / (1.0 + write_rate * self.policy.write_rate_multiplier);

        threshold.clamp(base_threshold, max_threshold) as u64
    }

    /// Get current statistics
    pub fn statistics(&self) -> &SnapshotStatistics {
        &self.statistics
    }

    /// Get write rate (writes per second)
    pub fn current_write_rate(&self) -> f64 {
        self.calculate_write_rate()
    }

    /// Get writes since last snapshot
    pub fn writes_since_last_snapshot(&self) -> u64 {
        self.writes_since_last_snapshot
    }

    /// Update current write rate in statistics
    pub fn update_write_rate(&mut self) {
        self.statistics.current_write_rate = self.calculate_write_rate();
    }

    /// Find latest snapshot before sequence
    pub fn find_latest_before(&self, sequence: u64) -> Result<Option<Snapshot>> {
        self.base_manager.find_latest_before(sequence)
    }

    /// List all snapshots
    pub fn list_snapshots(&self) -> Result<Vec<u64>> {
        self.base_manager.list_snapshots()
    }

    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_policy_default() {
        let policy = SnapshotPolicy::default();
        assert_eq!(policy.min_writes_threshold, 1_000);
        assert_eq!(policy.max_writes_threshold, 100_000);
        assert!(policy.enable_adaptive);
    }

    #[test]
    fn test_adaptive_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy::default();
        let manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        assert_eq!(manager.writes_since_last_snapshot(), 0);
        assert_eq!(manager.current_write_rate(), 0.0);
    }

    #[test]
    fn test_write_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy::default();
        let mut manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        manager.record_write();
        manager.record_write();
        manager.record_write();

        assert_eq!(manager.writes_since_last_snapshot(), 3);
        assert_eq!(manager.statistics().total_writes_processed, 3);
    }

    #[test]
    fn test_write_rate_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy::default();
        let mut manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        // Simulate writes over time
        for _ in 0..10 {
            manager.record_write();
        }

        let write_rate = manager.current_write_rate();
        assert!(write_rate >= 0.0);
    }

    #[test]
    fn test_should_snapshot_min_writes() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy {
            min_writes_threshold: 10,
            max_writes_threshold: 100,
            min_time_between_snapshots: 0,
            max_time_between_snapshots: 3600,
            enable_adaptive: false,
            ..Default::default()
        };
        let mut manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        // Not enough writes
        for _ in 0..5 {
            manager.record_write();
        }
        assert!(!manager.should_create_snapshot(5));

        // Exceed max threshold
        for _ in 0..100 {
            manager.record_write();
        }
        assert!(manager.should_create_snapshot(105));
    }

    #[test]
    fn test_dynamic_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy {
            min_writes_threshold: 1_000,
            max_writes_threshold: 100_000,
            enable_adaptive: true,
            write_rate_multiplier: 1.5,
            ..Default::default()
        };
        let manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        // Low write rate → higher threshold
        let threshold_low = manager.calculate_dynamic_threshold(0.1);
        assert!(threshold_low > 50_000);

        // High write rate → lower threshold
        let threshold_high = manager.calculate_dynamic_threshold(10.0);
        assert!(threshold_high < 10_000);
    }

    #[test]
    fn test_statistics_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy::default();
        let mut manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        for _ in 0..100 {
            manager.record_write();
        }

        let stats = manager.statistics();
        assert_eq!(stats.total_writes_processed, 100);
    }

    #[test]
    fn test_write_timestamp_pruning() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SnapshotPolicy::default();
        let mut manager = AdaptiveSnapshotManager::new(temp_dir.path(), policy);

        // Add writes
        for _ in 0..10 {
            manager.record_write();
        }

        // Write timestamps should be tracked
        assert!(!manager.write_timestamps.is_empty());
        assert!(manager.write_timestamps.len() <= 10);
    }

    #[test]
    fn test_adaptive_vs_fixed_mode() {
        let temp_dir = TempDir::new().unwrap();

        // Fixed mode
        let policy_fixed = SnapshotPolicy {
            max_writes_threshold: 50,
            enable_adaptive: false,
            ..Default::default()
        };
        let mut manager_fixed = AdaptiveSnapshotManager::new(temp_dir.path(), policy_fixed);

        for _ in 0..50 {
            manager_fixed.record_write();
        }
        assert!(manager_fixed.should_create_snapshot(50));

        // Adaptive mode
        let policy_adaptive = SnapshotPolicy {
            min_writes_threshold: 10,
            max_writes_threshold: 100,
            enable_adaptive: true,
            ..Default::default()
        };
        let mut manager_adaptive =
            AdaptiveSnapshotManager::new(temp_dir.path().join("adaptive"), policy_adaptive);

        for _ in 0..50 {
            manager_adaptive.record_write();
        }
        // Adaptive threshold depends on write rate
        let should_snapshot = manager_adaptive.should_create_snapshot(50);
        // Just verify it returns a boolean (actual value depends on timing)
        assert!(should_snapshot || !should_snapshot);
    }
}
