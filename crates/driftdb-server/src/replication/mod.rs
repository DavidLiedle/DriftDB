//! Replication module for DriftDB
//!
//! Provides WAL-based streaming replication with support for:
//! - Asynchronous and synchronous replication modes
//! - Multiple replicas with lag tracking
//! - Health monitoring and automatic failover detection
//! - PostgreSQL-compatible replication protocol

#![allow(dead_code)]

pub mod replica;
pub mod stream;

use std::sync::Arc;

pub use replica::{
    ReplicaManager, ReplicaManagerConfig,
};
pub use stream::{
    ReplicationMessage, StreamingConfig, WalStreamer,
};

use tokio::sync::RwLock;
use tracing::info;


/// Main replication coordinator
pub struct ReplicationCoordinator {
    /// Replica manager
    pub replica_manager: Arc<ReplicaManager>,
    /// WAL streamer
    pub wal_streamer: Arc<RwLock<WalStreamer>>,
    /// System identifier (unique per DriftDB instance)
    system_id: String,
    /// Current timeline ID
    timeline: u64,
}

impl ReplicationCoordinator {
    /// Create a new replication coordinator
    pub fn new(
        replica_config: ReplicaManagerConfig,
        streaming_config: StreamingConfig,
        system_id: String,
    ) -> Self {
        let replica_manager = Arc::new(ReplicaManager::new(replica_config));
        let wal_streamer = Arc::new(RwLock::new(WalStreamer::new(
            replica_manager.clone(),
            streaming_config,
        )));

        info!("Initialized replication coordinator with system_id={}", system_id);

        Self {
            replica_manager,
            wal_streamer,
            system_id,
            timeline: 1, // Start with timeline 1
        }
    }

    /// Get system identifier
    pub fn system_id(&self) -> &str {
        &self.system_id
    }

    /// Get current timeline
    pub fn timeline(&self) -> u64 {
        self.timeline
    }

    /// Get current LSN from WAL streamer
    pub async fn current_lsn(&self) -> u64 {
        self.wal_streamer.read().await.current_lsn().await
    }

    /// Handle IDENTIFY_SYSTEM replication command
    pub async fn handle_identify_system(&self) -> ReplicationMessage {
        let current_lsn = self.current_lsn().await;

        ReplicationMessage::IdentifySystem {
            system_id: self.system_id.clone(),
            timeline: self.timeline,
            current_lsn,
        }
    }

    /// Check health of all replicas
    pub async fn check_replica_health(&self) {
        let current_lsn = self.current_lsn().await;
        self.replica_manager.check_health(current_lsn);
    }

    /// Get replication statistics
    pub async fn get_stats(&self) -> ReplicationStats {
        let replicas = self.replica_manager.get_all_replicas();
        let current_lsn = self.current_lsn().await;

        let total_replicas = replicas.len();
        let healthy_replicas = self.replica_manager.healthy_replica_count();
        let sync_replicas = self.replica_manager.sync_replica_count();

        let total_bytes_sent: u64 = replicas.iter().map(|r| r.bytes_sent).sum();
        let total_entries_sent: u64 = replicas.iter().map(|r| r.entries_sent).sum();

        let max_lag = replicas
            .iter()
            .map(|r| r.lag_bytes)
            .max()
            .unwrap_or(0);

        ReplicationStats {
            total_replicas,
            healthy_replicas,
            sync_replicas,
            current_lsn,
            total_bytes_sent,
            total_entries_sent,
            max_lag_bytes: max_lag,
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    /// Total number of replicas
    pub total_replicas: usize,
    /// Number of healthy replicas
    pub healthy_replicas: usize,
    /// Number of synchronous replicas
    pub sync_replicas: usize,
    /// Current LSN
    pub current_lsn: u64,
    /// Total bytes sent to all replicas
    pub total_bytes_sent: u64,
    /// Total WAL entries sent to all replicas
    pub total_entries_sent: u64,
    /// Maximum lag across all replicas
    pub max_lag_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_coordinator_creation() {
        let coordinator = ReplicationCoordinator::new(
            ReplicaManagerConfig::default(),
            StreamingConfig::default(),
            "test-system-123".to_string(),
        );

        assert_eq!(coordinator.system_id(), "test-system-123");
        assert_eq!(coordinator.timeline(), 1);
        assert_eq!(coordinator.current_lsn().await, 0);
    }

    #[tokio::test]
    async fn test_identify_system_message() {
        let coordinator = ReplicationCoordinator::new(
            ReplicaManagerConfig::default(),
            StreamingConfig::default(),
            "test-system-456".to_string(),
        );

        let msg = coordinator.handle_identify_system().await;

        match msg {
            ReplicationMessage::IdentifySystem {
                system_id,
                timeline,
                current_lsn,
            } => {
                assert_eq!(system_id, "test-system-456");
                assert_eq!(timeline, 1);
                assert_eq!(current_lsn, 0);
            }
            _ => panic!("Expected IdentifySystem message"),
        }
    }

    #[tokio::test]
    async fn test_replication_stats() {
        let coordinator = ReplicationCoordinator::new(
            ReplicaManagerConfig::default(),
            StreamingConfig::default(),
            "test-system-789".to_string(),
        );

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.total_replicas, 0);
        assert_eq!(stats.healthy_replicas, 0);
        assert_eq!(stats.sync_replicas, 0);
        assert_eq!(stats.current_lsn, 0);
    }
}
