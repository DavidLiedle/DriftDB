use crate::{DriftError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub node_id: String,
    pub peers: Vec<String>,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub snapshot_threshold: usize,
    pub max_append_entries: usize,
    pub batch_size: usize,
    pub pipeline_enabled: bool,
    pub pre_vote_enabled: bool,
    pub learner_nodes: Vec<String>,
    pub witness_nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConsensusState {
    Follower,
    Candidate,
    Leader,
    Learner,
    Witness,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub client_id: String,
    pub request_id: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Write { key: String, value: Vec<u8> },
    Delete { key: String },
    Transaction { ops: Vec<TransactionOp> },
    ConfigChange { change: ConfigChange },
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOp {
    pub op_type: OpType,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub condition: Option<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    Read,
    Write,
    Delete,
    Compare,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub key: String,
    pub comparison: Comparison,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Comparison {
    Equal,
    NotEqual,
    Greater,
    Less,
    Exists,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChange {
    AddNode { node_id: String, address: String },
    RemoveNode { node_id: String },
    PromoteLearner { node_id: String },
    DemoteToLearner { node_id: String },
}

pub struct ConsensusEngine {
    config: Arc<ConsensusConfig>,
    state: Arc<RwLock<ConsensusState>>,
    current_term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<String>>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    commit_index: Arc<RwLock<u64>>,
    #[allow(dead_code)]
    last_applied: Arc<RwLock<u64>>,

    #[allow(dead_code)]
    leader_state: Arc<Mutex<Option<LeaderState>>>,
    #[allow(dead_code)]
    election_timer: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    state_machine: Arc<dyn StateMachine>,
    transport: Arc<dyn Transport>,

    metrics: Arc<ConsensusMetrics>,
    #[allow(dead_code)]
    snapshot_manager: Arc<SnapshotManager>,
}

struct LeaderState {
    #[allow(dead_code)]
    next_index: HashMap<String, u64>,
    #[allow(dead_code)]
    match_index: HashMap<String, u64>,
    #[allow(dead_code)]
    in_flight: HashMap<String, Vec<InflightRequest>>,
    #[allow(dead_code)]
    pipeline_depth: HashMap<String, usize>,
}

struct InflightRequest {
    #[allow(dead_code)]
    index: u64,
    #[allow(dead_code)]
    term: u64,
    #[allow(dead_code)]
    sent_at: SystemTime,
    #[allow(dead_code)]
    entries: Vec<LogEntry>,
}

pub trait StateMachine: Send + Sync {
    fn apply(&self, entry: &LogEntry) -> Result<Vec<u8>>;
    fn snapshot(&self) -> Result<Vec<u8>>;
    fn restore(&self, snapshot: &[u8]) -> Result<()>;
    fn query(&self, key: &str) -> Result<Option<Vec<u8>>>;
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn send_request_vote(
        &self,
        target: &str,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse>;
    async fn send_append_entries(
        &self,
        target: &str,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse>;
    async fn send_install_snapshot(
        &self,
        target: &str,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse>;
    async fn send_pre_vote(&self, target: &str, req: PreVoteRequest) -> Result<PreVoteResponse>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub pre_vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
    pub pipeline_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: Option<u64>,
    pub conflict_index: Option<u64>,
    pub conflict_term: Option<u64>,
    pub pipeline_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: String,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

impl ConsensusEngine {
    pub fn new(
        config: ConsensusConfig,
        state_machine: Arc<dyn StateMachine>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        let initial_state = if config.learner_nodes.contains(&config.node_id) {
            ConsensusState::Learner
        } else if config.witness_nodes.contains(&config.node_id) {
            ConsensusState::Witness
        } else {
            ConsensusState::Follower
        };

        Self {
            config: Arc::new(config),
            state: Arc::new(RwLock::new(initial_state)),
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(vec![])),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            leader_state: Arc::new(Mutex::new(None)),
            election_timer: Arc::new(Mutex::new(None)),
            state_machine,
            transport,
            metrics: Arc::new(ConsensusMetrics::new()),
            snapshot_manager: Arc::new(SnapshotManager::new()),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.start_election_timer().await;
        self.start_apply_loop().await;
        self.start_snapshot_loop().await;
        Ok(())
    }

    async fn start_election_timer(&self) {
        // TODO: Fix Send issue with Arc<dyn Transport>
        /*
        let config = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let current_term = Arc::clone(&self.current_term);
        let voted_for = Arc::clone(&self.voted_for);
        let log = Arc::clone(&self.log);
        let transport = Arc::clone(&self.transport);
        let election_timer = Arc::clone(&self.election_timer);

        // TODO: Fix Send issue with spawning
        // let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(config.election_timeout_ms));

            loop {
                interval.tick().await;

                let current_state = state.read().unwrap().clone();

                match current_state {
                    ConsensusState::Follower => {
                        info!("Election timeout, starting election");
                        Self::start_election_static(
                            &config,
                            &state,
                            &current_term,
                            &voted_for,
                            &log,
                            &transport,
                        ).await;
                    }
                    ConsensusState::Candidate => {
                        info!("Election timeout as candidate, restarting election");
                        Self::start_election_static(
                            &config,
                            &state,
                            &current_term,
                            &voted_for,
                            &log,
                            &transport,
                        ).await;
                    }
                    _ => {}
                }
            }
        });

        *election_timer.lock().await = Some(handle);
        */
    }

    #[allow(dead_code)]
    async fn start_election_static(
        config: &Arc<ConsensusConfig>,
        state: &Arc<RwLock<ConsensusState>>,
        current_term: &Arc<RwLock<u64>>,
        voted_for: &Arc<RwLock<Option<String>>>,
        log: &Arc<RwLock<Vec<LogEntry>>>,
        transport: &Arc<dyn Transport>,
    ) {
        if config.pre_vote_enabled
            && !Self::conduct_pre_vote(config, log, transport).await {
                return;
            }

        *state.write().unwrap() = ConsensusState::Candidate;

        let mut term = current_term.write().unwrap();
        *term += 1;
        let election_term = *term;
        drop(term);

        *voted_for.write().unwrap() = Some(config.node_id.clone());

        let last_log_index = log.read().unwrap().len() as u64;
        let last_log_term = log.read().unwrap().last().map(|e| e.term).unwrap_or(0);

        let mut votes = 1;
        let majority = config.peers.len().div_ceil(2) + 1;

        for peer in &config.peers {
            let req = RequestVoteRequest {
                term: election_term,
                candidate_id: config.node_id.clone(),
                last_log_index,
                last_log_term,
                pre_vote: false,
            };

            match transport.send_request_vote(peer, req).await {
                Ok(resp) => {
                    if resp.term > election_term {
                        *current_term.write().unwrap() = resp.term;
                        *state.write().unwrap() = ConsensusState::Follower;
                        *voted_for.write().unwrap() = None;
                        return;
                    }

                    if resp.vote_granted {
                        votes += 1;

                        if votes >= majority {
                            info!("Won election with {} votes", votes);
                            *state.write().unwrap() = ConsensusState::Leader;
                            Self::initialize_leader_state(config, log).await;
                            return;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to request vote from {}: {}", peer, e);
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn conduct_pre_vote(
        config: &Arc<ConsensusConfig>,
        log: &Arc<RwLock<Vec<LogEntry>>>,
        transport: &Arc<dyn Transport>,
    ) -> bool {
        let last_log_index = log.read().unwrap().len() as u64;
        let last_log_term = log.read().unwrap().last().map(|e| e.term).unwrap_or(0);

        let mut votes = 1;
        let majority = config.peers.len().div_ceil(2) + 1;

        for peer in &config.peers {
            let req = PreVoteRequest {
                term: 0,
                candidate_id: config.node_id.clone(),
                last_log_index,
                last_log_term,
            };

            match transport.send_pre_vote(peer, req).await {
                Ok(resp) => {
                    if resp.vote_granted {
                        votes += 1;

                        if votes >= majority {
                            return true;
                        }
                    }
                }
                Err(e) => {
                    warn!("Pre-vote failed for {}: {}", peer, e);
                }
            }
        }

        false
    }

    #[allow(dead_code)]
    async fn initialize_leader_state(
        config: &Arc<ConsensusConfig>,
        log: &Arc<RwLock<Vec<LogEntry>>>,
    ) {
        let last_log_index = log.read().unwrap().len() as u64;

        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for peer in &config.peers {
            next_index.insert(peer.clone(), last_log_index + 1);
            match_index.insert(peer.clone(), 0);
        }

        for learner in &config.learner_nodes {
            next_index.insert(learner.clone(), last_log_index + 1);
            match_index.insert(learner.clone(), 0);
        }
    }

    async fn start_apply_loop(&self) {
        // TODO: Fix Send issue
        /*
        let commit_index = Arc::clone(&self.commit_index);
        let last_applied = Arc::clone(&self.last_applied);
        let log = Arc::clone(&self.log);
        let state_machine = Arc::clone(&self.state_machine);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(10));

            loop {
                interval.tick().await;

                let commit = *commit_index.read().unwrap();
                let mut applied = *last_applied.read().unwrap();

                while applied < commit {
                    applied += 1;

                    let entry = log.read().unwrap()
                        .get((applied - 1) as usize)
                        .cloned();

                    if let Some(entry) = entry {
                        if let Err(e) = state_machine.apply(&entry) {
                            error!("Failed to apply entry: {}", e);
                        }
                    }

                    *last_applied.write().unwrap() = applied;
                }
            }
        });
        */
    }

    async fn start_snapshot_loop(&self) {
        // TODO: Fix Send issue
        /*
        let config = Arc::clone(&self.config);
        let log = Arc::clone(&self.log);
        let commit_index = Arc::clone(&self.commit_index);
        let snapshot_manager = Arc::clone(&self.snapshot_manager);
        let state_machine = Arc::clone(&self.state_machine);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));

            loop {
                interval.tick().await;

                let log_size = log.read().unwrap().len();

                if log_size > config.snapshot_threshold {
                    info!("Creating snapshot, log size: {}", log_size);

                    match state_machine.snapshot() {
                        Ok(data) => {
                            let commit = *commit_index.read().unwrap();

                            if let Err(e) = snapshot_manager.save_snapshot(commit, data).await {
                                error!("Failed to save snapshot: {}", e);
                            } else {
                                let mut log_guard = log.write().unwrap();
                                log_guard.drain(0..commit as usize);
                            }
                        }
                        Err(e) => {
                            error!("Failed to create snapshot: {}", e);
                        }
                    }
                }
            }
        });
        */
    }

    pub async fn propose(&self, command: Command) -> Result<Vec<u8>> {
        let state = self.state.read().unwrap().clone();

        if state != ConsensusState::Leader {
            return Err(DriftError::NotLeader);
        }

        let term = *self.current_term.read().unwrap();
        let index = self.log.read().unwrap().len() as u64 + 1;

        let entry = LogEntry {
            term,
            index,
            command,
            client_id: "client".to_string(),
            request_id: 0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        self.log.write().unwrap().push(entry.clone());

        self.replicate_entries().await?;

        self.wait_for_commit(index).await?;

        self.state_machine.apply(&entry)
    }

    async fn replicate_entries(&self) -> Result<()> {
        let current_term = *self.current_term.read().unwrap();
        let log = self.log.read().unwrap().clone();
        let commit_index = *self.commit_index.read().unwrap();

        for peer in &self.config.peers {
            let config = Arc::clone(&self.config);
            let transport = Arc::clone(&self.transport);
            let peer = peer.clone();
            let entries = log.clone();

            tokio::spawn(async move {
                let req = AppendEntriesRequest {
                    term: current_term,
                    leader_id: config.node_id.clone(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries,
                    leader_commit: commit_index,
                    pipeline_id: None,
                };

                if let Err(e) = transport.send_append_entries(&peer, req).await {
                    warn!("Failed to replicate to {}: {}", peer, e);
                }
            });
        }

        Ok(())
    }

    async fn wait_for_commit(&self, index: u64) -> Result<()> {
        let timeout = Duration::from_secs(5);
        let start = SystemTime::now();

        loop {
            if *self.commit_index.read().unwrap() >= index {
                return Ok(());
            }

            if SystemTime::now().duration_since(start).unwrap() > timeout {
                return Err(DriftError::Timeout);
            }

            sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn handle_request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        let current_term = *self.current_term.read().unwrap();

        if req.term < current_term {
            return RequestVoteResponse {
                term: current_term,
                vote_granted: false,
                reason: Some("Outdated term".to_string()),
            };
        }

        if req.term > current_term {
            *self.current_term.write().unwrap() = req.term;
            *self.state.write().unwrap() = ConsensusState::Follower;
            *self.voted_for.write().unwrap() = None;
        }

        let voted_for = self.voted_for.read().unwrap().clone();
        let can_vote = voted_for.is_none() || voted_for == Some(req.candidate_id.clone());

        if !can_vote {
            return RequestVoteResponse {
                term: req.term,
                vote_granted: false,
                reason: Some("Already voted".to_string()),
            };
        }

        let log = self.log.read().unwrap();
        let last_log_index = log.len() as u64;
        let last_log_term = log.last().map(|e| e.term).unwrap_or(0);

        let log_ok = req.last_log_term > last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index);

        if log_ok {
            *self.voted_for.write().unwrap() = Some(req.candidate_id.clone());

            RequestVoteResponse {
                term: req.term,
                vote_granted: true,
                reason: None,
            }
        } else {
            RequestVoteResponse {
                term: req.term,
                vote_granted: false,
                reason: Some("Log not up to date".to_string()),
            }
        }
    }

    pub async fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let current_term = *self.current_term.read().unwrap();

        if req.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: None,
                conflict_index: None,
                conflict_term: None,
                pipeline_id: req.pipeline_id,
            };
        }

        *self.state.write().unwrap() = ConsensusState::Follower;
        *self.current_term.write().unwrap() = req.term;

        let mut log = self.log.write().unwrap();

        if req.prev_log_index > 0 {
            if req.prev_log_index > log.len() as u64 {
                return AppendEntriesResponse {
                    term: req.term,
                    success: false,
                    match_index: Some(log.len() as u64),
                    conflict_index: Some(log.len() as u64 + 1),
                    conflict_term: None,
                    pipeline_id: req.pipeline_id,
                };
            }

            let prev_entry = &log[(req.prev_log_index - 1) as usize];
            if prev_entry.term != req.prev_log_term {
                let conflict_term = prev_entry.term;
                let mut conflict_index = req.prev_log_index;

                for i in (0..req.prev_log_index as usize).rev() {
                    if log[i].term != conflict_term {
                        conflict_index = i as u64 + 2;
                        break;
                    }
                }

                return AppendEntriesResponse {
                    term: req.term,
                    success: false,
                    match_index: None,
                    conflict_index: Some(conflict_index),
                    conflict_term: Some(conflict_term),
                    pipeline_id: req.pipeline_id,
                };
            }
        }

        log.truncate(req.prev_log_index as usize);
        log.extend(req.entries.clone());

        if req.leader_commit > *self.commit_index.read().unwrap() {
            let new_commit = req.leader_commit.min(log.len() as u64);
            *self.commit_index.write().unwrap() = new_commit;
        }

        AppendEntriesResponse {
            term: req.term,
            success: true,
            match_index: Some(log.len() as u64),
            conflict_index: None,
            conflict_term: None,
            pipeline_id: req.pipeline_id,
        }
    }

    pub fn get_state(&self) -> ConsensusState {
        self.state.read().unwrap().clone()
    }

    pub fn get_metrics(&self) -> ConsensusMetrics {
        (*self.metrics).clone()
    }
}

pub struct SnapshotManager {
    snapshots: Arc<RwLock<Vec<SnapshotMetadata>>>,
}

#[derive(Debug, Clone)]
struct SnapshotMetadata {
    #[allow(dead_code)]
    index: u64,
    #[allow(dead_code)]
    term: u64,
    #[allow(dead_code)]
    size: usize,
    #[allow(dead_code)]
    created_at: SystemTime,
    #[allow(dead_code)]
    path: String,
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            snapshots: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn save_snapshot(&self, index: u64, data: Vec<u8>) -> Result<()> {
        let metadata = SnapshotMetadata {
            index,
            term: 0,
            size: data.len(),
            created_at: SystemTime::now(),
            path: format!("snapshot_{}.bin", index),
        };

        self.snapshots.write().unwrap().push(metadata);

        Ok(())
    }

    pub async fn load_snapshot(&self, _index: u64) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}

#[derive(Debug, Clone)]
pub struct ConsensusMetrics {
    pub elections_started: u64,
    pub elections_won: u64,
    pub elections_lost: u64,
    pub proposals_accepted: u64,
    pub proposals_rejected: u64,
    pub entries_replicated: u64,
    pub snapshots_created: u64,
    pub snapshots_installed: u64,
}

impl Default for ConsensusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsensusMetrics {
    pub fn new() -> Self {
        Self {
            elections_started: 0,
            elections_won: 0,
            elections_lost: 0,
            proposals_accepted: 0,
            proposals_rejected: 0,
            entries_replicated: 0,
            snapshots_created: 0,
            snapshots_installed: 0,
        }
    }
}
