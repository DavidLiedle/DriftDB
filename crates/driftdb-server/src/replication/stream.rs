//! WAL streaming protocol for replication
//!
//! Handles continuous streaming of WAL entries from primary to replicas.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

use super::replica::{ReplicaId, ReplicaManager};
use anyhow::Result;

/// Maximum size of the WAL entry broadcast channel
const WAL_CHANNEL_SIZE: usize = 10000;

/// Replication message types sent over the wire
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// Start replication from a specific LSN
    StartReplication { start_lsn: u64, timeline: u64 },
    /// WAL data chunk
    WalData {
        start_lsn: u64,
        end_lsn: u64,
        data: Vec<u8>,
        timestamp: u64,
    },
    /// Keepalive/heartbeat message
    Keepalive {
        current_lsn: u64,
        timestamp: u64,
        reply_requested: bool,
    },
    /// Standby status update from replica
    StatusUpdate {
        received_lsn: u64,
        applied_lsn: u64,
        flushed_lsn: u64,
        timestamp: u64,
    },
    /// Identify system - get replication slot info
    IdentifySystem {
        system_id: String,
        timeline: u64,
        current_lsn: u64,
    },
    /// Hot standby feedback (for conflict resolution)
    HotStandbyFeedback { timestamp: u64, oldest_xid: u64 },
    /// Error message
    Error { message: String },
}

/// WAL entry for streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingWalEntry {
    /// LSN (Log Sequence Number) for this entry
    pub lsn: u64,
    /// Transaction ID
    pub transaction_id: Option<u64>,
    /// Operation type
    pub operation: String,
    /// Serialized operation data
    pub data: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// Checksum
    pub checksum: u32,
}

impl StreamingWalEntry {
    /// Serialize to bytes for transmission
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let data = serde_json::to_vec(self)?;
        Ok(data)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let entry = serde_json::from_slice(data)?;
        Ok(entry)
    }

    /// Get the size of this entry in bytes
    pub fn size(&self) -> usize {
        std::mem::size_of::<u64>() // lsn
            + std::mem::size_of::<Option<u64>>() // transaction_id
            + self.operation.len()
            + self.data.len()
            + std::mem::size_of::<u64>() // timestamp
            + std::mem::size_of::<u32>() // checksum
    }
}

/// Configuration for WAL streaming
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Keepalive interval
    pub keepalive_interval: Duration,
    /// Status update timeout (if replica doesn't send status)
    pub status_timeout: Duration,
    /// Maximum WAL send buffer size
    pub max_send_buffer: usize,
    /// Batch multiple WAL entries into single message
    pub batch_entries: bool,
    /// Maximum batch size
    pub max_batch_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: Duration::from_secs(10),
            status_timeout: Duration::from_secs(60),
            max_send_buffer: 10 * 1024 * 1024, // 10MB
            batch_entries: true,
            max_batch_size: 100,
        }
    }
}

/// WAL streaming manager - broadcasts WAL entries to all replicas
pub struct WalStreamer {
    /// Broadcast channel for new WAL entries
    wal_tx: broadcast::Sender<StreamingWalEntry>,
    /// Current LSN
    current_lsn: Arc<RwLock<u64>>,
    /// Replica manager
    replica_manager: Arc<ReplicaManager>,
    /// Configuration
    config: StreamingConfig,
}

impl WalStreamer {
    /// Create a new WAL streamer
    pub fn new(replica_manager: Arc<ReplicaManager>, config: StreamingConfig) -> Self {
        let (wal_tx, _) = broadcast::channel(WAL_CHANNEL_SIZE);

        Self {
            wal_tx,
            current_lsn: Arc::new(RwLock::new(0)),
            replica_manager,
            config,
        }
    }

    /// Broadcast a new WAL entry to all subscribed replicas
    pub async fn broadcast_entry(&self, entry: StreamingWalEntry) -> Result<()> {
        // Update current LSN
        {
            let mut lsn = self.current_lsn.write().await;
            *lsn = entry.lsn;
        }

        // Broadcast to all subscribers
        match self.wal_tx.send(entry.clone()) {
            Ok(receiver_count) => {
                debug!(
                    "Broadcasted WAL entry LSN {} to {} replicas",
                    entry.lsn, receiver_count
                );
                Ok(())
            }
            Err(e) => {
                warn!("Failed to broadcast WAL entry: {}", e);
                // This is not a fatal error - might just mean no active replicas
                Ok(())
            }
        }
    }

    /// Get current LSN
    pub async fn current_lsn(&self) -> u64 {
        *self.current_lsn.read().await
    }

    /// Subscribe to WAL stream for a replica
    pub fn subscribe(&self) -> broadcast::Receiver<StreamingWalEntry> {
        self.wal_tx.subscribe()
    }

    /// Start streaming to a specific replica
    pub async fn stream_to_replica(
        &self,
        replica_id: ReplicaId,
        start_lsn: u64,
        sender: mpsc::Sender<ReplicationMessage>,
    ) -> Result<()> {
        info!(
            "Starting WAL streaming to replica {} from LSN {}",
            replica_id, start_lsn
        );

        // Subscribe to WAL broadcast
        let mut wal_rx = self.subscribe();

        // Get replica info
        let replica = self
            .replica_manager
            .get_replica(replica_id)
            .ok_or_else(|| anyhow::anyhow!("Replica not found"))?;

        // Set up keepalive timer
        let mut keepalive = interval(self.config.keepalive_interval);

        // TODO: If start_lsn < current_lsn, need to send historical WAL first
        // For now, we just start streaming from current position

        loop {
            tokio::select! {
                // Receive new WAL entry
                entry_result = wal_rx.recv() => {
                    match entry_result {
                        Ok(entry) => {
                            // Only send entries >= start_lsn
                            if entry.lsn >= start_lsn {
                                // Get size before moving entry
                                let entry_size = entry.size();

                                if let Err(e) = self.send_wal_entry(&sender, entry).await {
                                    error!("Failed to send WAL entry to replica {}: {}", replica_id, e);
                                    self.replica_manager.record_failure(replica_id);
                                    return Err(e);
                                }

                                // Update metrics
                                self.replica_manager.record_bytes_sent(
                                    replica_id,
                                    entry_size as u64,
                                    1,
                                );
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(
                                "Replica {} lagged behind, skipped {} entries",
                                replica.name, skipped
                            );
                            // Continue streaming - replica will catch up
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("WAL broadcast channel closed, stopping stream to replica {}", replica_id);
                            return Ok(());
                        }
                    }
                }

                // Send keepalive
                _ = keepalive.tick() => {
                    let current_lsn = self.current_lsn().await;
                    let msg = ReplicationMessage::Keepalive {
                        current_lsn,
                        timestamp: current_timestamp(),
                        reply_requested: true,
                    };

                    if let Err(e) = sender.send(msg).await {
                        error!("Failed to send keepalive to replica {}: {}", replica_id, e);
                        self.replica_manager.record_failure(replica_id);
                        return Err(anyhow::anyhow!("Failed to send keepalive: {}", e));
                    }

                    debug!("Sent keepalive to replica {}", replica_id);
                }
            }
        }
    }

    /// Send a WAL entry to a replica
    async fn send_wal_entry(
        &self,
        sender: &mpsc::Sender<ReplicationMessage>,
        entry: StreamingWalEntry,
    ) -> Result<()> {
        let data = entry.to_bytes()?;

        let msg = ReplicationMessage::WalData {
            start_lsn: entry.lsn,
            end_lsn: entry.lsn,
            data,
            timestamp: entry.timestamp,
        };

        sender
            .send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send WAL entry: {}", e))?;

        Ok(())
    }

    /// Handle status update from replica
    pub async fn handle_status_update(
        &self,
        replica_id: ReplicaId,
        received_lsn: u64,
        applied_lsn: u64,
        _flushed_lsn: u64,
    ) {
        let current_lsn = self.current_lsn().await;

        // Update replica position
        self.replica_manager.update_replica_position(
            replica_id,
            received_lsn,
            applied_lsn,
            current_lsn,
        );

        // Update heartbeat
        self.replica_manager.update_heartbeat(replica_id);

        debug!(
            "Received status update from replica {}: received={}, applied={}",
            replica_id, received_lsn, applied_lsn
        );
    }

    /// Wait for synchronous replicas to confirm write
    pub async fn wait_for_sync_replicas(&self, lsn: u64, timeout_duration: Duration) -> Result<()> {
        let sync_replicas = self.replica_manager.get_sync_replicas();

        if sync_replicas.is_empty() {
            // No sync replicas, return immediately
            return Ok(());
        }

        info!(
            "Waiting for {} synchronous replicas to confirm LSN {}",
            sync_replicas.len(),
            lsn
        );

        // Wait for all sync replicas to confirm
        let result = timeout(timeout_duration, async {
            loop {
                let sync_replicas = self.replica_manager.get_sync_replicas();
                let confirmed = sync_replicas
                    .iter()
                    .filter(|r| r.last_applied_lsn >= lsn)
                    .count();

                if confirmed == sync_replicas.len() {
                    return Ok::<(), anyhow::Error>(());
                }

                // Wait a bit before checking again
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        match result {
            Ok(_) => {
                debug!("All synchronous replicas confirmed LSN {}", lsn);
                Ok(())
            }
            Err(_) => {
                error!(
                    "Timeout waiting for synchronous replicas to confirm LSN {}",
                    lsn
                );
                Err(anyhow::anyhow!("Synchronous replication timeout"))
            }
        }
    }
}

/// Get current timestamp in microseconds
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Serialize a replication message to wire format
pub fn serialize_message(msg: &ReplicationMessage) -> Result<Vec<u8>> {
    let data = serde_json::to_vec(msg)?;
    Ok(data)
}

/// Deserialize a replication message from wire format
pub fn deserialize_message(data: &[u8]) -> Result<ReplicationMessage> {
    let msg = serde_json::from_slice(data)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::replica::{ReplicaManagerConfig, ReplicationMode};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[tokio::test]
    async fn test_wal_streaming_basic() {
        let replica_manager = Arc::new(ReplicaManager::new(ReplicaManagerConfig::default()));
        let streamer = WalStreamer::new(replica_manager, StreamingConfig::default());

        let entry = StreamingWalEntry {
            lsn: 1,
            transaction_id: Some(100),
            operation: "INSERT".to_string(),
            data: vec![1, 2, 3, 4],
            timestamp: current_timestamp(),
            checksum: 12345,
        };

        let result = streamer.broadcast_entry(entry).await;
        assert!(result.is_ok());

        let current = streamer.current_lsn().await;
        assert_eq!(current, 1);
    }

    #[tokio::test]
    async fn test_message_serialization() {
        let msg = ReplicationMessage::StartReplication {
            start_lsn: 100,
            timeline: 1,
        };

        let serialized = serialize_message(&msg).unwrap();
        let deserialized = deserialize_message(&serialized).unwrap();

        match deserialized {
            ReplicationMessage::StartReplication {
                start_lsn,
                timeline,
            } => {
                assert_eq!(start_lsn, 100);
                assert_eq!(timeline, 1);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[tokio::test]
    async fn test_status_update_handling() {
        let replica_manager = Arc::new(ReplicaManager::new(ReplicaManagerConfig::default()));

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433);
        let replica_id = replica_manager
            .register_replica("test-replica".to_string(), addr, ReplicationMode::Async)
            .unwrap();

        let streamer = WalStreamer::new(replica_manager.clone(), StreamingConfig::default());

        // Broadcast an entry to set current LSN
        let entry = StreamingWalEntry {
            lsn: 100,
            transaction_id: None,
            operation: "INSERT".to_string(),
            data: vec![],
            timestamp: current_timestamp(),
            checksum: 0,
        };
        streamer.broadcast_entry(entry).await.unwrap();

        // Handle status update
        streamer.handle_status_update(replica_id, 90, 85, 85).await;

        let replica = replica_manager.get_replica(replica_id).unwrap();
        assert_eq!(replica.last_received_lsn, 90);
        assert_eq!(replica.last_applied_lsn, 85);
        assert_eq!(replica.lag_bytes, 15); // 100 - 85
    }
}
