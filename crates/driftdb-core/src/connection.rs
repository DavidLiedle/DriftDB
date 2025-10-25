//! Connection pooling and client management
//!
//! Provides efficient connection pooling with:
//! - Configurable pool size limits
//! - Connection health checking
//! - Automatic cleanup of idle connections
//! - Fair scheduling and backpressure
//! - Rate limiting per client

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, instrument};

use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::observability::Metrics;
use crate::transaction::{IsolationLevel, TransactionManager};
use crate::wal::{WalConfig, WalManager};

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain
    pub min_connections: usize,
    /// Maximum number of connections allowed
    pub max_connections: usize,
    /// Maximum time to wait for a connection
    pub connection_timeout: Duration,
    /// How long a connection can be idle before removal
    pub idle_timeout: Duration,
    /// How often to run health checks
    pub health_check_interval: Duration,
    /// Maximum requests per second per client
    pub rate_limit_per_client: Option<u32>,
    /// Maximum concurrent requests per client
    pub max_concurrent_per_client: usize,
    /// Queue size for pending requests
    pub max_queue_size: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 10,
            max_connections: 100,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300), // 5 minutes
            health_check_interval: Duration::from_secs(30),
            rate_limit_per_client: Some(1000), // 1000 req/s per client
            max_concurrent_per_client: 10,
            max_queue_size: 1000,
        }
    }
}

/// Individual connection state
pub struct Connection {
    pub id: u64,
    pub client_addr: Option<SocketAddr>,
    pub created_at: Instant,
    pub last_used: Instant,
    pub requests_handled: AtomicU64,
    pub state: ConnectionState,
    pub current_transaction: Option<Arc<Mutex<crate::transaction::Transaction>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Idle,
    Active,
    Closing,
    Closed,
}

impl Connection {
    pub fn new(id: u64, client_addr: Option<SocketAddr>) -> Self {
        let now = Instant::now();
        Self {
            id,
            client_addr,
            created_at: now,
            last_used: now,
            requests_handled: AtomicU64::new(0),
            state: ConnectionState::Idle,
            current_transaction: None,
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.state, ConnectionState::Idle)
    }

    pub fn is_expired(&self, idle_timeout: Duration) -> bool {
        self.is_idle() && self.last_used.elapsed() > idle_timeout
    }

    pub fn mark_active(&mut self) {
        self.state = ConnectionState::Active;
        self.last_used = Instant::now();
        self.requests_handled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_idle(&mut self) {
        self.state = ConnectionState::Idle;
        self.current_transaction = None;
    }

    /// Get the number of requests handled by this connection
    pub fn request_count(&self) -> u64 {
        self.requests_handled.load(Ordering::Relaxed)
    }

    /// Check if connection has an active transaction
    pub fn has_transaction(&self) -> bool {
        self.current_transaction.is_some()
    }
}

/// Rate limiter using token bucket algorithm
pub struct RateLimiter {
    tokens: AtomicU64,
    max_tokens: u64,
    refill_rate: u64, // tokens per second
    last_refill: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(rate_per_second: u32) -> Self {
        let max_tokens = rate_per_second as u64 * 10; // Allow burst of 10x rate
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate: rate_per_second as u64,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    pub fn try_acquire(&self, tokens: u64) -> bool {
        // Refill bucket
        self.refill();

        // Try to acquire tokens
        let mut current = self.tokens.load(Ordering::Acquire);
        loop {
            if current < tokens {
                return false; // Not enough tokens
            }
            match self.tokens.compare_exchange_weak(
                current,
                current - tokens,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn refill(&self) {
        let mut last_refill = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        if elapsed >= Duration::from_secs(1) {
            let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
            let current = self.tokens.load(Ordering::Acquire);
            let new_tokens = (current + tokens_to_add).min(self.max_tokens);
            self.tokens.store(new_tokens, Ordering::Release);
            *last_refill = now;
        }
    }
}

/// Client session tracking
pub struct ClientSession {
    pub addr: SocketAddr,
    pub connected_at: Instant,
    pub last_request: Instant,
    pub request_count: AtomicU64,
    pub rate_limiter: Option<RateLimiter>,
    pub concurrent_requests: AtomicUsize,
}

impl ClientSession {
    pub fn new(addr: SocketAddr, rate_limit: Option<u32>) -> Self {
        let now = Instant::now();
        Self {
            addr,
            connected_at: now,
            last_request: now,
            request_count: AtomicU64::new(0),
            rate_limiter: rate_limit.map(RateLimiter::new),
            concurrent_requests: AtomicUsize::new(0),
        }
    }

    pub fn can_make_request(&self, max_concurrent: usize) -> bool {
        // Check rate limit
        if let Some(limiter) = &self.rate_limiter {
            if !limiter.try_acquire(1) {
                return false;
            }
        }

        // Check concurrent request limit
        self.concurrent_requests.load(Ordering::Acquire) < max_concurrent
    }

    pub fn begin_request(&self) {
        self.concurrent_requests.fetch_add(1, Ordering::AcqRel);
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_request(&self) {
        self.concurrent_requests.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Connection pool manager
pub struct ConnectionPool {
    config: PoolConfig,
    connections: Arc<RwLock<HashMap<u64, Arc<Mutex<Connection>>>>>,
    available: Arc<Mutex<VecDeque<u64>>>,
    clients: Arc<RwLock<HashMap<SocketAddr, Arc<ClientSession>>>>,
    next_conn_id: Arc<AtomicU64>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    transaction_manager: Arc<TransactionManager>,
    shutdown: Arc<AtomicU64>, // 0 = running, 1 = shutting down
}

impl ConnectionPool {
    pub fn new(
        config: PoolConfig,
        metrics: Arc<Metrics>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Result<Self> {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));

        let pool = Self {
            config: config.clone(),
            connections: Arc::new(RwLock::new(HashMap::new())),
            available: Arc::new(Mutex::new(VecDeque::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            next_conn_id: Arc::new(AtomicU64::new(1)),
            semaphore,
            metrics,
            transaction_manager,
            shutdown: Arc::new(AtomicU64::new(0)),
        };

        // Pre-create minimum connections
        pool.ensure_min_connections()?;

        Ok(pool)
    }

    /// Ensure minimum connections exist
    fn ensure_min_connections(&self) -> Result<()> {
        let current = self.connections.read().len();
        if current < self.config.min_connections {
            for _ in current..self.config.min_connections {
                self.create_connection(None)?;
            }
        }
        Ok(())
    }

    /// Create a new connection
    fn create_connection(&self, client_addr: Option<SocketAddr>) -> Result<u64> {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::SeqCst);
        let conn = Arc::new(Mutex::new(Connection::new(conn_id, client_addr)));

        self.connections.write().insert(conn_id, conn);
        self.available.lock().push_back(conn_id);
        self.metrics
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        debug!("Created connection {}", conn_id);
        Ok(conn_id)
    }

    /// Acquire a connection from the pool
    #[instrument(skip(self))]
    pub async fn acquire(&self, client_addr: SocketAddr) -> Result<ConnectionGuard> {
        // Check if shutting down
        if self.shutdown.load(Ordering::Acquire) > 0 {
            return Err(DriftError::Other("Pool is shutting down".to_string()));
        }

        // Get or create client session
        let client_session = {
            let mut clients = self.clients.write();
            clients
                .entry(client_addr)
                .or_insert_with(|| {
                    Arc::new(ClientSession::new(
                        client_addr,
                        self.config.rate_limit_per_client,
                    ))
                })
                .clone()
        };

        // Check rate limiting and concurrency
        if !client_session.can_make_request(self.config.max_concurrent_per_client) {
            self.metrics.queries_failed.fetch_add(1, Ordering::Relaxed);
            return Err(DriftError::Other(
                "Rate limit or concurrency limit exceeded".to_string(),
            ));
        }

        // Try to acquire semaphore permit
        let permit = match tokio::time::timeout(
            self.config.connection_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) | Err(_) => {
                return Err(DriftError::Other("Connection timeout".to_string()));
            }
        };

        // Get available connection
        let conn_id = {
            let mut available = self.available.lock();
            if let Some(id) = available.pop_front() {
                id
            } else if self.connections.read().len() < self.config.max_connections {
                drop(available); // Release lock before creating
                self.create_connection(Some(client_addr))?
            } else {
                return Err(DriftError::Other("No connections available".to_string()));
            }
        };

        // Mark connection as active
        {
            let connections = self.connections.read();
            if let Some(conn) = connections.get(&conn_id) {
                conn.lock().mark_active();
            }
        }

        client_session.begin_request();

        Ok(ConnectionGuard {
            pool: self.clone(),
            conn_id,
            client_session,
            _permit: permit,
        })
    }

    /// Return connection to pool
    fn release(&self, conn_id: u64, client_session: Arc<ClientSession>) {
        client_session.end_request();

        let should_keep = {
            let connections = self.connections.read();
            if let Some(conn) = connections.get(&conn_id) {
                let mut conn_guard = conn.lock();
                conn_guard.mark_idle();
                !conn_guard.is_expired(self.config.idle_timeout)
            } else {
                false
            }
        };

        if should_keep {
            self.available.lock().push_back(conn_id);
        } else {
            self.remove_connection(conn_id);
        }
    }

    /// Remove a connection from the pool
    fn remove_connection(&self, conn_id: u64) {
        self.connections.write().remove(&conn_id);
        self.available.lock().retain(|&id| id != conn_id);
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
        debug!("Removed connection {}", conn_id);
    }

    /// Run periodic health checks
    pub async fn run_health_checks(&self) {
        let mut interval = tokio::time::interval(self.config.health_check_interval);

        loop {
            interval.tick().await;

            if self.shutdown.load(Ordering::Acquire) > 0 {
                break;
            }

            // Clean up expired connections
            let connections = self.connections.read().clone();
            for (conn_id, conn) in connections.iter() {
                if conn.lock().is_expired(self.config.idle_timeout) {
                    self.remove_connection(*conn_id);
                }
            }

            // Clean up idle client sessions
            let mut clients = self.clients.write();
            clients.retain(|_, session| {
                session.last_request.elapsed() < Duration::from_secs(3600) // 1 hour
            });

            // Ensure minimum connections
            let _ = self.ensure_min_connections();

            debug!(
                "Health check completed: {} connections, {} clients",
                self.connections.read().len(),
                clients.len()
            );
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) {
        info!("Shutting down connection pool");
        self.shutdown.store(1, Ordering::Release);

        // Close all connections
        let connections = self.connections.read().clone();
        for (conn_id, conn) in connections.iter() {
            conn.lock().state = ConnectionState::Closing;
            self.remove_connection(*conn_id);
        }

        info!("Connection pool shut down");
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let connections = self.connections.read();
        let available = self.available.lock();
        let active_count = connections.len() - available.len();

        // Calculate detailed metrics
        let mut with_transactions = 0;
        let mut total_requests = 0u64;
        for conn in connections.values() {
            let conn_locked = conn.lock();
            if conn_locked.has_transaction() {
                with_transactions += 1;
            }
            total_requests += conn_locked.request_count();
        }

        PoolStats {
            total_connections: connections.len(),
            available_connections: available.len(),
            active_clients: self.clients.read().len(),
            total_created: self.next_conn_id.load(Ordering::Relaxed) - 1,
            active_connections: active_count,
            connections_with_transactions: with_transactions,
            total_requests_handled: total_requests,
        }
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            connections: self.connections.clone(),
            available: self.available.clone(),
            clients: self.clients.clone(),
            next_conn_id: self.next_conn_id.clone(),
            semaphore: self.semaphore.clone(),
            metrics: self.metrics.clone(),
            transaction_manager: self.transaction_manager.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

/// RAII guard for connection usage
pub struct ConnectionGuard {
    pool: ConnectionPool,
    conn_id: u64,
    client_session: Arc<ClientSession>,
    _permit: OwnedSemaphorePermit,
}

impl ConnectionGuard {
    pub fn id(&self) -> u64 {
        self.conn_id
    }

    /// Begin a transaction on this connection
    pub fn begin_transaction(&mut self, isolation: IsolationLevel) -> Result<()> {
        let connections = self.pool.connections.read();
        if let Some(conn) = connections.get(&self.conn_id) {
            let mut conn_guard = conn.lock();
            if conn_guard.current_transaction.is_some() {
                return Err(DriftError::Other("Transaction already active".to_string()));
            }
            let txn = self.pool.transaction_manager.begin(isolation)?;
            conn_guard.current_transaction = Some(txn);
            Ok(())
        } else {
            Err(DriftError::Other("Connection not found".to_string()))
        }
    }

    /// Get current transaction
    pub fn transaction(&self) -> Option<Arc<Mutex<crate::transaction::Transaction>>> {
        let connections = self.pool.connections.read();
        connections
            .get(&self.conn_id)
            .and_then(|conn| conn.lock().current_transaction.clone())
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.pool.release(self.conn_id, self.client_session.clone());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PoolStats {
    pub total_connections: usize,
    pub available_connections: usize,
    pub active_connections: usize,
    pub active_clients: usize,
    pub total_created: u64,
    pub connections_with_transactions: usize,
    pub total_requests_handled: u64,
}

/// Engine pool that manages connections to a DriftDB Engine
pub struct EnginePool {
    engine: Arc<parking_lot::RwLock<Engine>>,
    connection_pool: ConnectionPool,
}

impl EnginePool {
    pub fn new(
        engine: Arc<parking_lot::RwLock<Engine>>,
        config: PoolConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        // Create a WAL instance for the transaction manager
        // This is a temporary solution - ideally the Engine should expose its WAL
        let temp_dir = std::env::temp_dir().join(format!("driftdb_pool_{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir)?;
        let wal = Arc::new(WalManager::new(
            temp_dir.join("test.wal"),
            WalConfig::default(),
        )?);
        let transaction_manager = Arc::new(TransactionManager::new_with_deps(wal, metrics.clone()));

        let connection_pool = ConnectionPool::new(config, metrics, transaction_manager)?;

        Ok(Self {
            engine,
            connection_pool,
        })
    }

    /// Acquire a connection and engine access
    #[instrument(skip(self))]
    pub async fn acquire(&self, client_addr: SocketAddr) -> Result<EngineGuard> {
        let connection_guard = self.connection_pool.acquire(client_addr).await?;

        Ok(EngineGuard {
            engine: self.engine.clone(),
            _connection_guard: connection_guard,
        })
    }

    /// Run periodic health checks
    pub async fn run_health_checks(&self) {
        self.connection_pool.run_health_checks().await;
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) {
        self.connection_pool.shutdown().await;
    }

    /// Get pool statistics
    pub fn stats(&self) -> EnginePoolStats {
        let conn_stats = self.connection_pool.stats();
        EnginePoolStats {
            connection_stats: conn_stats,
            engine_available: true, // Could be enhanced to check engine health
        }
    }
}

impl Clone for EnginePool {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            connection_pool: self.connection_pool.clone(),
        }
    }
}

/// RAII guard for engine access through the pool
pub struct EngineGuard {
    engine: Arc<parking_lot::RwLock<Engine>>,
    _connection_guard: ConnectionGuard,
}

impl EngineGuard {
    /// Get read access to the engine
    pub fn read(&self) -> parking_lot::RwLockReadGuard<Engine> {
        self.engine.read()
    }

    /// Get write access to the engine
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<Engine> {
        self.engine.write()
    }

    /// Get a reference to the engine Arc for transaction management
    pub fn get_engine_ref(&self) -> Arc<parking_lot::RwLock<Engine>> {
        self.engine.clone()
    }

    /// Get the connection ID
    pub fn connection_id(&self) -> u64 {
        self._connection_guard.id()
    }

    /// Begin a transaction on this connection
    pub fn begin_transaction(&mut self, isolation: IsolationLevel) -> Result<()> {
        self._connection_guard.begin_transaction(isolation)
    }

    /// Get current transaction
    pub fn transaction(&self) -> Option<Arc<Mutex<crate::transaction::Transaction>>> {
        self._connection_guard.transaction()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnginePoolStats {
    pub connection_stats: PoolStats,
    pub engine_available: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalManager;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_connection_pool() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Arc::new(WalManager::new(temp_dir.path().join("test.wal"), WalConfig::default()).unwrap());
        let metrics = Arc::new(Metrics::new());
        let tx_mgr = Arc::new(TransactionManager::new_with_deps(wal, metrics.clone()));

        let config = PoolConfig {
            min_connections: 2,
            max_connections: 5,
            ..Default::default()
        };

        let pool = ConnectionPool::new(config, metrics, tx_mgr).unwrap();

        // Check initial state
        let stats = pool.stats();
        assert_eq!(stats.total_connections, 2);
        assert_eq!(stats.available_connections, 2);

        // Acquire a connection
        let client_addr = "127.0.0.1:12345".parse().unwrap();
        let guard = pool.acquire(client_addr).await.unwrap();
        assert!(guard.id() > 0);

        // Stats should reflect acquisition
        let stats = pool.stats();
        assert_eq!(stats.available_connections, 1);
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(10); // 10 tokens per second

        // Should allow initial burst
        for _ in 0..10 {
            assert!(limiter.try_acquire(1));
        }

        // Should be rate limited now
        assert!(!limiter.try_acquire(100));
    }
}
