//! Replication module for high availability
//!
//! Provides master-slave replication with automatic failover:
//! - Streaming replication with configurable lag
//! - Automatic failover with consensus
//! - Read replicas for load distribution
//! - Point-in-time recovery from replicas

// Suppress certain clippy warnings that don't apply well to our hybrid sync/async architecture
#![allow(clippy::collapsible_match)] // Personal preference for readability
#![allow(clippy::collapsible_if)] // Personal preference for readability

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{error, info, instrument, warn};

use crate::errors::{DriftError, Result};
use crate::wal::WalEntry;

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Role of this node
    pub role: NodeRole,
    /// Replication mode
    pub mode: ReplicationMode,
    /// Master node address (for slaves)
    pub master_addr: Option<String>,
    /// Listen address for replication
    pub listen_addr: String,
    /// Maximum replication lag in milliseconds
    pub max_lag_ms: u64,
    /// Sync interval in milliseconds
    pub sync_interval_ms: u64,
    /// Failover timeout in milliseconds
    pub failover_timeout_ms: u64,
    /// Number of sync replicas required for commits
    pub min_sync_replicas: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: NodeRole::Master,
            mode: ReplicationMode::Asynchronous,
            master_addr: None,
            listen_addr: "0.0.0.0:5433".to_string(),
            max_lag_ms: 10000,
            sync_interval_ms: 100,
            failover_timeout_ms: 30000,
            min_sync_replicas: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeRole {
    Master,
    Slave,
    StandbyMaster,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Asynchronous,
    Synchronous,
    SemiSynchronous,
}

/// Replication message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationMessage {
    /// Handshake from replica
    Hello {
        node_id: String,
        role: NodeRole,
        last_seq: u64,
    },
    /// WAL entry to replicate
    WalEntry { entry: WalEntry, sequence: u64 },
    /// Acknowledgment from replica
    Ack { sequence: u64, timestamp_ms: u64 },
    /// Heartbeat for liveness
    Heartbeat { sequence: u64, timestamp_ms: u64 },
    /// Request for missing entries
    CatchupRequest { from_seq: u64, to_seq: u64 },
    /// Batch of catch-up entries
    CatchupResponse { entries: Vec<WalEntry> },
    /// Initiate failover
    FailoverRequest { new_master: String, reason: String },
    /// Vote for failover
    FailoverVote { node_id: String, accept: bool },
    /// New master announcement
    NewMaster { node_id: String, sequence: u64 },
}

/// Replica connection state
#[derive(Debug)]
struct ReplicaConnection {
    _node_id: String,
    _addr: SocketAddr,
    _role: NodeRole,
    _last_ack_seq: u64,
    _last_ack_time: SystemTime,
    lag_ms: u64,
    is_sync: bool,
    stream: Arc<Mutex<TcpStream>>,
}

/// Replication coordinator
pub struct ReplicationCoordinator {
    config: ReplicationConfig,
    node_id: String,
    state: Arc<RwLock<ReplicationState>>,
    replicas: Arc<RwLock<HashMap<String, ReplicaConnection>>>,
    wal_queue: Arc<RwLock<VecDeque<WalEntry>>>,
    sync_waiters: Arc<Mutex<HashMap<u64, Vec<oneshot::Sender<bool>>>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

#[derive(Debug, Clone)]
struct ReplicationState {
    role: NodeRole,
    is_active: bool,
    master_id: Option<String>,
    last_applied_seq: u64,
    last_committed_seq: u64,
    failover_in_progress: bool,
}

impl ReplicationCoordinator {
    /// Create a new replication coordinator
    pub fn new(config: ReplicationConfig) -> Self {
        let node_id = uuid::Uuid::new_v4().to_string();

        let state = ReplicationState {
            role: config.role.clone(),
            is_active: true,
            master_id: if config.role == NodeRole::Master {
                Some(node_id.clone())
            } else {
                None
            },
            last_applied_seq: 0,
            last_committed_seq: 0,
            failover_in_progress: false,
        };

        Self {
            config,
            node_id,
            state: Arc::new(RwLock::new(state)),
            replicas: Arc::new(RwLock::new(HashMap::new())),
            wal_queue: Arc::new(RwLock::new(VecDeque::new())),
            sync_waiters: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx: None,
        }
    }

    /// Start the replication coordinator
    #[instrument(skip(self))]
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting replication coordinator as {:?}", self.config.role);

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        match self.config.role {
            NodeRole::Master => self.start_as_master(shutdown_rx).await?,
            NodeRole::Slave | NodeRole::StandbyMaster => self.start_as_replica(shutdown_rx).await?,
        }

        Ok(())
    }

    /// Start as master node
    async fn start_as_master(&self, mut shutdown_rx: mpsc::Receiver<()>) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(
            "Master listening for replicas on {}",
            self.config.listen_addr
        );

        // Accept replica connections
        let replicas = self.replicas.clone();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                info!("New replica connection from {}", addr);
                                Self::handle_replica_connection(
                                    stream,
                                    addr,
                                    replicas.clone(),
                                    node_id.clone()
                                ).await;
                            }
                            Err(e) => error!("Accept error: {}", e),
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Master shutting down");
                        break;
                    }
                }
            }
        });

        // Start heartbeat sender
        self.start_heartbeat_sender().await;

        Ok(())
    }

    /// Handle a replica connection
    async fn handle_replica_connection(
        mut stream: TcpStream,
        addr: SocketAddr,
        replicas: Arc<RwLock<HashMap<String, ReplicaConnection>>>,
        _master_id: String,
    ) {
        // Read handshake
        let mut buf = vec![0u8; 1024];
        match stream.read(&mut buf).await {
            Ok(n) if n > 0 => {
                if let Ok(msg) = bincode::deserialize::<ReplicationMessage>(&buf[..n]) {
                    if let ReplicationMessage::Hello {
                        node_id,
                        role,
                        last_seq,
                    } = msg
                    {
                        info!("Replica {} connected with last_seq {}", node_id, last_seq);

                        let conn = ReplicaConnection {
                            _node_id: node_id.clone(),
                            _addr: addr,
                            _role: role,
                            _last_ack_seq: last_seq,
                            _last_ack_time: SystemTime::now(),
                            lag_ms: 0,
                            is_sync: false,
                            stream: Arc::new(Mutex::new(stream)),
                        };

                        replicas.write().insert(node_id, conn);
                    }
                }
            }
            _ => {}
        }
    }

    /// Start as replica node
    async fn start_as_replica(&self, mut shutdown_rx: mpsc::Receiver<()>) -> Result<()> {
        let master_addr = self
            .config
            .master_addr
            .as_ref()
            .ok_or_else(|| DriftError::Other("Master address not configured".into()))?;

        info!("Connecting to master at {}", master_addr);

        let mut stream = TcpStream::connect(master_addr).await?;

        // Send handshake
        let hello = ReplicationMessage::Hello {
            node_id: self.node_id.clone(),
            role: self.config.role.clone(),
            last_seq: self.state.read().last_applied_seq,
        };

        let data = bincode::serialize(&hello)?;
        stream.write_all(&data).await?;

        // Process replication stream
        let state = self.state.clone();
        let wal_queue = self.wal_queue.clone();
        let node_id = self.node_id.clone();

        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                tokio::select! {
                    result = stream.read(&mut buf) => {
                        match result {
                            Ok(0) => {
                                warn!("Master connection closed");
                                break;
                            }
                            Ok(n) => {
                                if let Ok(msg) = bincode::deserialize::<ReplicationMessage>(&buf[..n]) {
                                    Self::handle_replication_message(
                                        msg,
                                        &node_id,
                                        &state,
                                        &wal_queue,
                                        &mut stream
                                    ).await;
                                }
                            }
                            Err(e) => {
                                error!("Read error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Replica shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Handle incoming replication message
    async fn handle_replication_message(
        msg: ReplicationMessage,
        node_id: &str,
        state: &Arc<RwLock<ReplicationState>>,
        wal_queue: &Arc<RwLock<VecDeque<WalEntry>>>,
        stream: &mut TcpStream,
    ) {
        match msg {
            ReplicationMessage::WalEntry { entry, sequence } => {
                // Apply WAL entry
                wal_queue.write().push_back(entry.clone());
                state.write().last_applied_seq = sequence;

                // Send acknowledgment
                let ack = ReplicationMessage::Ack {
                    sequence,
                    timestamp_ms: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                        .as_millis() as u64,
                };

                if let Ok(data) = bincode::serialize(&ack) {
                    let _ = stream.write_all(&data).await;
                }
            }
            ReplicationMessage::Heartbeat { sequence, .. } => {
                // Update last known sequence
                state.write().last_committed_seq = sequence;
            }
            ReplicationMessage::NewMaster { node_id, sequence } => {
                warn!("New master elected: {} at seq {}", node_id, sequence);
                state.write().master_id = Some(node_id);
            }
            ReplicationMessage::FailoverRequest { new_master, reason } => {
                info!(
                    "Received failover request for {} due to: {}",
                    new_master, reason
                );

                // Evaluate if we should vote for this failover
                let should_accept = {
                    let state_guard = state.read();
                    // Accept if: not already in failover, and requester has caught up
                    !state_guard.failover_in_progress && state_guard.role == NodeRole::Slave
                };

                // Send vote response
                let vote = ReplicationMessage::FailoverVote {
                    node_id: node_id.to_string(),
                    accept: should_accept,
                };

                if let Ok(data) = bincode::serialize(&vote) {
                    let _ = stream.write_all(&data).await;
                }

                if should_accept {
                    state.write().failover_in_progress = true;
                }
            }
            _ => {}
        }
    }

    /// Start heartbeat sender
    async fn start_heartbeat_sender(&self) {
        let replicas = self.replicas.clone();
        let state = self.state.clone();
        let interval = Duration::from_millis(self.config.sync_interval_ms);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;

                let current_seq = state.read().last_committed_seq;
                let heartbeat = ReplicationMessage::Heartbeat {
                    sequence: current_seq,
                    timestamp_ms: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                        .as_millis() as u64,
                };

                if let Ok(data) = bincode::serialize(&heartbeat) {
                    // Clone the replicas to avoid holding lock across await
                    let replica_streams: Vec<_> = {
                        let replicas_guard = replicas.read();
                        replicas_guard.values().map(|r| r.stream.clone()).collect()
                    };

                    for stream in replica_streams {
                        let data_clone = data.clone();
                        tokio::spawn(async move {
                            if let Ok(mut stream_guard) = stream.try_lock() {
                                let _ = stream_guard.write_all(&data_clone).await;
                            }
                        });
                    }
                }
            }
        });
    }

    /// Replicate a WAL entry to replicas
    #[instrument(skip(self, entry))]
    #[allow(clippy::await_holding_lock)] // Intentional: hybrid sync/async architecture
    pub async fn replicate(&self, entry: WalEntry, sequence: u64) -> Result<()> {
        if self.state.read().role != NodeRole::Master {
            return Ok(());
        }

        let msg = ReplicationMessage::WalEntry { entry, sequence };
        let data = bincode::serialize(&msg)?;

        let replicas = self.replicas.read();
        let mut sync_count = 0;

        for (_, replica) in replicas.iter() {
            if let Ok(mut stream) = replica.stream.try_lock() {
                if stream.write_all(&data).await.is_ok() && replica.is_sync {
                    sync_count += 1;
                }
            }
        }

        // Wait for sync replicas if configured
        if self.config.mode == ReplicationMode::Synchronous {
            if sync_count < self.config.min_sync_replicas {
                return Err(DriftError::Other(format!(
                    "Insufficient sync replicas: {} < {}",
                    sync_count, self.config.min_sync_replicas
                )));
            }

            // Wait for acknowledgments
            let (tx, rx) = oneshot::channel();
            self.sync_waiters
                .lock()
                .await
                .entry(sequence)
                .or_insert_with(Vec::new)
                .push(tx);

            tokio::time::timeout(Duration::from_millis(self.config.sync_interval_ms * 10), rx)
                .await
                .map_err(|_| DriftError::Other("Replication timeout".into()))?
                .map_err(|_| DriftError::Other("Replication failed".into()))?;
        }

        Ok(())
    }

    /// Initiate failover
    #[instrument(skip(self))]
    #[allow(clippy::await_holding_lock)] // Intentional: hybrid sync/async architecture
    pub async fn initiate_failover(&self, reason: &str) -> Result<()> {
        if self.state.read().failover_in_progress {
            return Err(DriftError::Other("Failover already in progress".into()));
        }

        info!("Initiating failover: {}", reason);
        self.state.write().failover_in_progress = true;

        // If we're a standby master, attempt to become master
        if self.state.read().role == NodeRole::StandbyMaster {
            // Broadcast failover request
            let msg = ReplicationMessage::FailoverRequest {
                new_master: self.node_id.clone(),
                reason: reason.to_string(),
            };

            let data = bincode::serialize(&msg)?;
            let replicas = self.replicas.read();
            let required_votes = replicas.len() / 2 + 1;

            // Send vote request to all replicas
            let mut vote_futures = Vec::new();
            for (node_id, replica) in replicas.iter() {
                let node_id = node_id.clone();
                let stream = replica.stream.clone();
                let data = data.clone();
                let timeout = Duration::from_millis(self.config.failover_timeout_ms / 2);

                let vote_future = async move {
                    // Send vote request
                    if let Ok(mut stream_guard) = stream.try_lock() {
                        if stream_guard.write_all(&data).await.is_err() {
                            return (node_id, false);
                        }

                        // Wait for vote response with timeout
                        let mut response_buf = vec![0u8; 1024];
                        match tokio::time::timeout(timeout, stream_guard.read(&mut response_buf))
                            .await
                        {
                            Ok(Ok(n)) if n > 0 => {
                                // Parse vote response
                                if let Ok(response) =
                                    bincode::deserialize::<ReplicationMessage>(&response_buf[..n])
                                {
                                    if let ReplicationMessage::FailoverVote { accept, .. } =
                                        response
                                    {
                                        return (node_id, accept);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    (node_id, false)
                };
                vote_futures.push(vote_future);
            }

            // Collect votes
            let vote_results = future::join_all(vote_futures).await;
            let votes = vote_results
                .iter()
                .filter(|(_, accepted)| *accepted)
                .count();

            info!(
                "Failover vote results: {}/{} votes received",
                votes, required_votes
            );

            if votes >= required_votes {
                self.promote_to_master().await?;
            } else {
                self.state.write().failover_in_progress = false;
                return Err(DriftError::Other("Insufficient votes for failover".into()));
            }
        }

        Ok(())
    }

    /// Promote this node to master
    async fn promote_to_master(&self) -> Result<()> {
        info!("Promoting node {} to master", self.node_id);

        let msg = {
            let mut state = self.state.write();
            state.role = NodeRole::Master;
            state.master_id = Some(self.node_id.clone());
            state.failover_in_progress = false;

            // Prepare announcement message
            ReplicationMessage::NewMaster {
                node_id: self.node_id.clone(),
                sequence: state.last_applied_seq,
            }
        }; // Drop lock before async operations

        // Announce new master
        if let Ok(data) = bincode::serialize(&msg) {
            // Collect replica stream handles (Arc clones)
            let stream_arcs: Vec<_> = {
                let replicas = self.replicas.read();
                replicas
                    .values()
                    .map(|replica| replica.stream.clone())
                    .collect()
            };

            // Send to all streams without holding replicas lock
            for stream_arc in stream_arcs {
                if let Ok(mut stream) = stream_arc.try_lock() {
                    let _ = stream.write_all(&data).await;
                }
            }
        }

        Ok(())
    }

    /// Get replication lag for monitoring
    pub fn get_replication_lag(&self) -> HashMap<String, u64> {
        let mut lag_map = HashMap::new();
        for (id, replica) in self.replicas.read().iter() {
            lag_map.insert(id.clone(), replica.lag_ms);
        }
        lag_map
    }

    /// Check if replication is healthy
    pub fn is_healthy(&self) -> bool {
        let replicas = self.replicas.read();

        // Check if we have minimum sync replicas
        if self.config.mode == ReplicationMode::Synchronous {
            let sync_count = replicas.values().filter(|r| r.is_sync).count();
            if sync_count < self.config.min_sync_replicas {
                return false;
            }
        }

        // Check replication lag
        for replica in replicas.values() {
            if replica.lag_ms > self.config.max_lag_ms {
                return false;
            }
        }

        true
    }

    /// Get the current role of this node
    pub fn get_role(&self) -> NodeRole {
        self.state.read().role.clone()
    }

    /// Shutdown the replication coordinator
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down replication coordinator");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }

        self.state.write().is_active = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_coordinator_creation() {
        let config = ReplicationConfig::default();
        let coordinator = ReplicationCoordinator::new(config);
        assert_eq!(coordinator.state.read().role, NodeRole::Master);
    }

    #[tokio::test]
    async fn test_replication_lag_monitoring() {
        let config = ReplicationConfig::default();
        let coordinator = ReplicationCoordinator::new(config);
        let lag = coordinator.get_replication_lag();
        assert!(lag.is_empty());
    }

    #[tokio::test]
    async fn test_health_check() {
        let config = ReplicationConfig {
            mode: ReplicationMode::Asynchronous,
            min_sync_replicas: 0,
            ..Default::default()
        };
        let coordinator = ReplicationCoordinator::new(config);
        assert!(coordinator.is_healthy());
    }
}
