//! Replica management and tracking for DriftDB replication
//!
//! Tracks connected replicas, their replication status, lag, and health.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::metrics;

/// Replica identifier
pub type ReplicaId = Uuid;

/// Replica connection state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Replica is connecting and authenticating
    Connecting,
    /// Replica is catching up with historical WAL
    CatchingUp,
    /// Replica is streaming current WAL entries
    Streaming,
    /// Replica is temporarily disconnected
    Disconnected,
    /// Replica has failed (exceeded failure threshold)
    Failed,
}

/// Replication mode for a replica
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// Asynchronous replication - don't wait for replica acknowledgment
    Async,
    /// Synchronous replication - wait for replica to acknowledge writes
    Sync,
}

/// Information about a connected replica
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Unique replica identifier
    pub id: ReplicaId,
    /// Replica name (user-provided)
    pub name: String,
    /// Network address of the replica
    pub address: SocketAddr,
    /// Current replication state
    pub state: ReplicaState,
    /// Replication mode (sync/async)
    pub mode: ReplicationMode,
    /// Last WAL sequence number confirmed by replica
    pub last_received_lsn: u64,
    /// Last WAL sequence number applied by replica
    pub last_applied_lsn: u64,
    /// Time of last heartbeat from replica
    pub last_heartbeat: Instant,
    /// Time when replica connected
    pub connected_at: Instant,
    /// Number of bytes sent to this replica
    pub bytes_sent: u64,
    /// Number of WAL entries sent to this replica
    pub entries_sent: u64,
    /// Replication lag in bytes (primary LSN - replica LSN)
    pub lag_bytes: u64,
    /// Estimated replication lag in time
    pub lag_duration: Duration,
    /// Number of consecutive failures
    pub failure_count: u32,
}

impl ReplicaInfo {
    /// Create a new replica info
    pub fn new(
        id: ReplicaId,
        name: String,
        address: SocketAddr,
        mode: ReplicationMode,
    ) -> Self {
        Self {
            id,
            name,
            address,
            state: ReplicaState::Connecting,
            mode,
            last_received_lsn: 0,
            last_applied_lsn: 0,
            last_heartbeat: Instant::now(),
            connected_at: Instant::now(),
            bytes_sent: 0,
            entries_sent: 0,
            lag_bytes: 0,
            lag_duration: Duration::from_secs(0),
            failure_count: 0,
        }
    }

    /// Check if replica is healthy (heartbeat within threshold)
    pub fn is_healthy(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() < timeout && self.state != ReplicaState::Failed
    }

    /// Calculate current replication lag
    pub fn calculate_lag(&mut self, current_lsn: u64) {
        if current_lsn >= self.last_applied_lsn {
            self.lag_bytes = current_lsn - self.last_applied_lsn;
        } else {
            self.lag_bytes = 0;
        }

        // Estimate time lag based on lag bytes (rough approximation)
        // Assume ~1MB/sec replication speed
        let lag_seconds = self.lag_bytes as f64 / 1_000_000.0;
        self.lag_duration = Duration::from_secs_f64(lag_seconds);
    }

    /// Update heartbeat timestamp
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.failure_count = 0; // Reset failure count on successful heartbeat
    }

    /// Record a failure
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= 3 {
            self.state = ReplicaState::Failed;
            warn!(
                "Replica {} ({}) marked as failed after {} consecutive failures",
                self.name, self.id, self.failure_count
            );
        }
    }
}

/// Configuration for replica management
#[derive(Debug, Clone)]
pub struct ReplicaManagerConfig {
    /// Maximum number of replicas
    pub max_replicas: usize,
    /// Heartbeat timeout (mark replica as unhealthy if no heartbeat)
    pub heartbeat_timeout: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Maximum replication lag before alerting
    pub max_lag_bytes: u64,
}

impl Default for ReplicaManagerConfig {
    fn default() -> Self {
        Self {
            max_replicas: 10,
            heartbeat_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            max_lag_bytes: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Manages all connected replicas
pub struct ReplicaManager {
    /// All registered replicas
    replicas: Arc<RwLock<HashMap<ReplicaId, ReplicaInfo>>>,
    /// Configuration
    config: ReplicaManagerConfig,
}

impl ReplicaManager {
    /// Create a new replica manager
    pub fn new(config: ReplicaManagerConfig) -> Self {
        Self {
            replicas: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Register a new replica
    pub fn register_replica(
        &self,
        name: String,
        address: SocketAddr,
        mode: ReplicationMode,
    ) -> Result<ReplicaId, String> {
        let mut replicas = self.replicas.write();

        if replicas.len() >= self.config.max_replicas {
            return Err(format!(
                "Maximum replica limit reached ({})",
                self.config.max_replicas
            ));
        }

        let id = Uuid::new_v4();
        let info = ReplicaInfo::new(id, name.clone(), address, mode);

        info!(
            "Registering replica {} ({}) from {} in {:?} mode",
            name, id, address, mode
        );

        replicas.insert(id, info);

        // Update metrics
        metrics::record_replica_status("active", replicas.len() as i64);

        Ok(id)
    }

    /// Unregister a replica
    pub fn unregister_replica(&self, id: ReplicaId) {
        let mut replicas = self.replicas.write();
        if let Some(info) = replicas.remove(&id) {
            info!(
                "Unregistered replica {} ({}) from {}",
                info.name, id, info.address
            );

            // Update metrics
            metrics::record_replica_status("active", replicas.len() as i64);
        }
    }

    /// Update replica position (LSN)
    pub fn update_replica_position(
        &self,
        id: ReplicaId,
        received_lsn: u64,
        applied_lsn: u64,
        current_lsn: u64,
    ) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(&id) {
            replica.last_received_lsn = received_lsn;
            replica.last_applied_lsn = applied_lsn;
            replica.calculate_lag(current_lsn);

            debug!(
                "Updated replica {} position: received={}, applied={}, lag={}",
                replica.name, received_lsn, applied_lsn, replica.lag_bytes
            );

            // Update metrics
            metrics::record_replication_lag(replica.lag_bytes as f64 / 1024.0); // KB
        }
    }

    /// Update replica state
    pub fn update_replica_state(&self, id: ReplicaId, state: ReplicaState) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(&id) {
            debug!(
                "Replica {} state changed: {:?} -> {:?}",
                replica.name, replica.state, state
            );
            replica.state = state;
        }
    }

    /// Update replica heartbeat
    pub fn update_heartbeat(&self, id: ReplicaId) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(&id) {
            replica.update_heartbeat();
        }
    }

    /// Record bytes sent to replica
    pub fn record_bytes_sent(&self, id: ReplicaId, bytes: u64, entries: u64) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(&id) {
            replica.bytes_sent += bytes;
            replica.entries_sent += entries;

            // Update metrics
            metrics::record_replication_bytes_sent_total(bytes as f64);
        }
    }

    /// Record a replica failure
    pub fn record_failure(&self, id: ReplicaId) {
        let mut replicas = self.replicas.write();
        if let Some(replica) = replicas.get_mut(&id) {
            replica.record_failure();
        }
    }

    /// Get replica info
    pub fn get_replica(&self, id: ReplicaId) -> Option<ReplicaInfo> {
        self.replicas.read().get(&id).cloned()
    }

    /// Get all replicas
    pub fn get_all_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas.read().values().cloned().collect()
    }

    /// Get all synchronous replicas
    pub fn get_sync_replicas(&self) -> Vec<ReplicaInfo> {
        self.replicas
            .read()
            .values()
            .filter(|r| r.mode == ReplicationMode::Sync && r.state == ReplicaState::Streaming)
            .cloned()
            .collect()
    }

    /// Check health of all replicas
    pub fn check_health(&self, current_lsn: u64) {
        let mut replicas = self.replicas.write();

        for replica in replicas.values_mut() {
            // Update lag
            replica.calculate_lag(current_lsn);

            // Check heartbeat timeout
            if !replica.is_healthy(self.config.heartbeat_timeout) {
                if replica.state != ReplicaState::Disconnected
                    && replica.state != ReplicaState::Failed
                {
                    warn!(
                        "Replica {} ({}) heartbeat timeout - marking as disconnected",
                        replica.name, replica.id
                    );
                    replica.state = ReplicaState::Disconnected;
                }
            }

            // Check lag threshold
            if replica.lag_bytes > self.config.max_lag_bytes {
                warn!(
                    "Replica {} ({}) exceeds max lag: {} bytes",
                    replica.name, replica.id, replica.lag_bytes
                );
            }
        }
    }

    /// Get count of healthy replicas
    pub fn healthy_replica_count(&self) -> usize {
        self.replicas
            .read()
            .values()
            .filter(|r| r.is_healthy(self.config.heartbeat_timeout))
            .count()
    }

    /// Get count of synchronous replicas
    pub fn sync_replica_count(&self) -> usize {
        self.replicas
            .read()
            .values()
            .filter(|r| r.mode == ReplicationMode::Sync && r.state == ReplicaState::Streaming)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_replica_registration() {
        let manager = ReplicaManager::new(ReplicaManagerConfig::default());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433);
        let id = manager
            .register_replica("replica-1".to_string(), addr, ReplicationMode::Async)
            .unwrap();

        let replica = manager.get_replica(id).unwrap();
        assert_eq!(replica.name, "replica-1");
        assert_eq!(replica.address, addr);
        assert_eq!(replica.mode, ReplicationMode::Async);
        assert_eq!(replica.state, ReplicaState::Connecting);
    }

    #[test]
    fn test_replica_position_update() {
        let manager = ReplicaManager::new(ReplicaManagerConfig::default());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433);
        let id = manager
            .register_replica("replica-1".to_string(), addr, ReplicationMode::Async)
            .unwrap();

        manager.update_replica_position(id, 100, 90, 150);

        let replica = manager.get_replica(id).unwrap();
        assert_eq!(replica.last_received_lsn, 100);
        assert_eq!(replica.last_applied_lsn, 90);
        assert_eq!(replica.lag_bytes, 60); // 150 - 90
    }

    #[test]
    fn test_replica_health_tracking() {
        let manager = ReplicaManager::new(ReplicaManagerConfig::default());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433);
        let id = manager
            .register_replica("replica-1".to_string(), addr, ReplicationMode::Sync)
            .unwrap();

        assert_eq!(manager.healthy_replica_count(), 1);

        // Record failures
        manager.record_failure(id);
        manager.record_failure(id);
        manager.record_failure(id);

        let replica = manager.get_replica(id).unwrap();
        assert_eq!(replica.state, ReplicaState::Failed);
    }

    #[test]
    fn test_max_replica_limit() {
        let config = ReplicaManagerConfig {
            max_replicas: 2,
            ..Default::default()
        };
        let manager = ReplicaManager::new(config);

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433);

        manager
            .register_replica("replica-1".to_string(), addr, ReplicationMode::Async)
            .unwrap();
        manager
            .register_replica("replica-2".to_string(), addr, ReplicationMode::Async)
            .unwrap();

        let result = manager.register_replica("replica-3".to_string(), addr, ReplicationMode::Async);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Maximum replica limit"));
    }
}
