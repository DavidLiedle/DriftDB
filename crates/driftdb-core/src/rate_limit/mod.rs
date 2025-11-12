//! Enhanced Rate Limiting Implementation
//!
//! Provides multi-level rate limiting to protect against DoS attacks and resource abuse:
//! - Per-client connection rate limiting
//! - Per-client query rate limiting
//! - Global server rate limits
//! - Query cost-based rate limiting
//! - Adaptive rate limiting based on server load

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::observability::Metrics;

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Connections per minute per client
    pub connections_per_minute: Option<u32>,
    /// Queries per second per client
    pub queries_per_second: Option<u32>,
    /// Token bucket burst size
    pub burst_size: u32,
    /// Global queries per second limit
    pub global_queries_per_second: Option<u32>,
    /// IP addresses exempt from rate limiting
    pub exempt_ips: Vec<IpAddr>,
    /// Enable adaptive rate limiting based on server load
    pub adaptive_limiting: bool,
    /// Query cost multiplier for expensive operations
    pub cost_multiplier: f64,
    /// Different limits for authenticated vs unauthenticated users
    pub auth_multiplier: f64,
    /// Higher limits for superusers
    pub superuser_multiplier: f64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            connections_per_minute: Some(30),
            queries_per_second: Some(100),
            burst_size: 1000,
            global_queries_per_second: Some(10000),
            exempt_ips: vec!["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()],
            adaptive_limiting: true,
            cost_multiplier: 1.0,
            auth_multiplier: 2.0,
            superuser_multiplier: 5.0,
        }
    }
}

/// Query cost estimation based on SQL content
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryCost {
    /// Simple queries (SELECT with WHERE on indexed columns)
    Low = 1,
    /// Standard queries (most SELECT, INSERT, UPDATE, DELETE)
    Medium = 5,
    /// Expensive queries (aggregations, large scans, joins)
    High = 20,
    /// Very expensive operations (full table scans, complex analytics)
    VeryHigh = 100,
}

impl QueryCost {
    /// Estimate query cost from SQL text
    pub fn estimate(sql: &str) -> Self {
        let sql_upper = sql.trim().to_uppercase();

        // Very expensive operations
        if sql_upper.contains("FULL OUTER JOIN")
            || sql_upper.contains("CROSS JOIN")
            || (sql_upper.contains("SELECT")
                && sql_upper.contains("ORDER BY")
                && !sql_upper.contains("LIMIT"))
            || sql_upper.contains("GROUP BY") && sql_upper.contains("HAVING")
            || sql_upper.matches("JOIN").count() > 2
        {
            return QueryCost::VeryHigh;
        }

        // High cost operations
        if sql_upper.contains("GROUP BY")
            || sql_upper.contains("ORDER BY")
            || sql_upper.contains("DISTINCT")
            || sql_upper.contains("COUNT(")
            || sql_upper.contains("SUM(")
            || sql_upper.contains("AVG(")
            || sql_upper.contains("MIN(")
            || sql_upper.contains("MAX(")
            || sql_upper.contains("JOIN")
            || sql_upper.contains("UNION")
            || sql_upper.contains("LIKE '%")
            || (sql_upper.contains("SELECT")
                && !sql_upper.contains("WHERE")
                && !sql_upper.contains("LIMIT"))
        {
            return QueryCost::High;
        }

        // Medium cost operations
        if sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("INSERT")
            || sql_upper.starts_with("UPDATE")
            || sql_upper.starts_with("DELETE")
            || sql_upper.starts_with("PATCH")
            || sql_upper.starts_with("SOFT DELETE")
        {
            return QueryCost::Medium;
        }

        // Low cost operations (DDL, simple commands)
        QueryCost::Low
    }

    /// Get the token cost as a number
    pub fn tokens(self) -> u64 {
        self as u64
    }
}

/// Token bucket rate limiter with configurable refill rate
pub struct TokenBucket {
    tokens: AtomicU64,
    max_tokens: u64,
    refill_rate: u64, // tokens per second
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Try to acquire the specified number of tokens
    pub fn try_acquire(&self, tokens: u64) -> bool {
        self.refill();

        let mut current = self.tokens.load(Ordering::Acquire);
        loop {
            if current < tokens {
                return false;
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

    /// Get current token count
    pub fn available_tokens(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Acquire)
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let mut last_refill = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        if elapsed >= Duration::from_millis(100) {
            // Refill every 100ms for smoothness
            let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
            if tokens_to_add > 0 {
                let current = self.tokens.load(Ordering::Acquire);
                let new_tokens = (current + tokens_to_add).min(self.max_tokens);
                self.tokens.store(new_tokens, Ordering::Release);
                *last_refill = now;
            }
        }
    }
}

/// Per-client rate limiting state
pub struct ClientRateLimit {
    pub addr: SocketAddr,
    pub connection_limiter: Option<TokenBucket>,
    pub query_limiter: Option<TokenBucket>,
    pub connection_count: AtomicUsize,
    pub query_count: AtomicU64,
    pub last_activity: Mutex<Instant>,
    pub violations: AtomicU64,
    pub is_authenticated: std::sync::atomic::AtomicBool,
    pub is_superuser: std::sync::atomic::AtomicBool,
}

impl ClientRateLimit {
    pub fn new(addr: SocketAddr, config: &RateLimitConfig) -> Self {
        let connection_limiter = config
            .connections_per_minute
            .map(|rate| TokenBucket::new(rate as u64, rate as u64 / 60));

        let query_limiter = config.queries_per_second.map(|rate| {
            TokenBucket::new(
                (rate as f64 * config.burst_size as f64 / 100.0) as u64,
                rate as u64,
            )
        });

        Self {
            addr,
            connection_limiter,
            query_limiter,
            connection_count: AtomicUsize::new(0),
            query_count: AtomicU64::new(0),
            last_activity: Mutex::new(Instant::now()),
            violations: AtomicU64::new(0),
            is_authenticated: std::sync::atomic::AtomicBool::new(false),
            is_superuser: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Check if a new connection is allowed
    pub fn allow_connection(&self) -> bool {
        if let Some(limiter) = &self.connection_limiter {
            if !limiter.try_acquire(1) {
                self.violations.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        self.connection_count.fetch_add(1, Ordering::Relaxed);
        *self.last_activity.lock() = Instant::now();
        true
    }

    /// Check if a query is allowed with cost estimation
    pub fn allow_query(&self, cost: QueryCost) -> bool {
        let mut tokens_needed = cost.tokens();

        // Apply multipliers based on authentication status
        if !self.is_authenticated.load(Ordering::Relaxed) {
            tokens_needed *= 2; // Unauthenticated users pay double
        } else if self.is_superuser.load(Ordering::Relaxed) {
            tokens_needed = (tokens_needed as f64 * 0.2) as u64; // Superusers pay 20%
        }

        if let Some(limiter) = &self.query_limiter {
            if !limiter.try_acquire(tokens_needed) {
                self.violations.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        self.query_count.fetch_add(1, Ordering::Relaxed);
        *self.last_activity.lock() = Instant::now();
        true
    }

    /// Release a connection
    pub fn release_connection(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Update authentication status
    pub fn set_authenticated(&self, authenticated: bool, is_superuser: bool) {
        self.is_authenticated
            .store(authenticated, Ordering::Relaxed);
        self.is_superuser.store(is_superuser, Ordering::Relaxed);
    }

    /// Check if client has been inactive for too long
    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.last_activity.lock().elapsed() > timeout
            && self.connection_count.load(Ordering::Relaxed) == 0
    }
}

/// Server load monitoring for adaptive rate limiting
pub struct LoadMonitor {
    cpu_usage: AtomicU64,    // Percentage * 100
    memory_usage: AtomicU64, // Percentage * 100
    active_connections: AtomicUsize,
    query_rate: AtomicU64, // Queries per second
    last_update: Mutex<Instant>,
}

impl Default for LoadMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadMonitor {
    pub fn new() -> Self {
        Self {
            cpu_usage: AtomicU64::new(0),
            memory_usage: AtomicU64::new(0),
            active_connections: AtomicUsize::new(0),
            query_rate: AtomicU64::new(0),
            last_update: Mutex::new(Instant::now()),
        }
    }

    /// Update load metrics
    pub fn update_load(&self, cpu: f64, memory: f64, connections: usize, query_rate: u64) {
        self.cpu_usage
            .store((cpu * 100.0) as u64, Ordering::Relaxed);
        self.memory_usage
            .store((memory * 100.0) as u64, Ordering::Relaxed);
        self.active_connections
            .store(connections, Ordering::Relaxed);
        self.query_rate.store(query_rate, Ordering::Relaxed);
        *self.last_update.lock() = Instant::now();
    }

    /// Get current load factor (0.0 = no load, 1.0 = high load)
    pub fn load_factor(&self) -> f64 {
        let cpu = self.cpu_usage.load(Ordering::Relaxed) as f64 / 100.0;
        let memory = self.memory_usage.load(Ordering::Relaxed) as f64 / 100.0;

        // Combine metrics with weights
        let load = (cpu * 0.4)
            + (memory * 0.3)
            + (self.active_connections.load(Ordering::Relaxed) as f64 / 1000.0 * 0.3);

        load.min(1.0)
    }

    /// Calculate adaptive rate limit multiplier
    pub fn adaptive_multiplier(&self) -> f64 {
        let load = self.load_factor();
        if load < 0.5 {
            1.0 // Normal rates
        } else if load < 0.8 {
            0.7 // Reduce rates by 30%
        } else {
            0.3 // Reduce rates by 70% under high load
        }
    }
}

/// Main rate limiting manager
pub struct RateLimitManager {
    config: RateLimitConfig,
    clients: Arc<RwLock<HashMap<SocketAddr, Arc<ClientRateLimit>>>>,
    global_limiter: Option<TokenBucket>,
    load_monitor: Arc<LoadMonitor>,
    metrics: Arc<Metrics>,
    violation_count: AtomicU64,
}

impl RateLimitManager {
    pub fn new(config: RateLimitConfig, metrics: Arc<Metrics>) -> Self {
        let global_limiter = config
            .global_queries_per_second
            .map(|rate| TokenBucket::new(rate as u64 * 10, rate as u64));

        Self {
            config,
            clients: Arc::new(RwLock::new(HashMap::new())),
            global_limiter,
            load_monitor: Arc::new(LoadMonitor::new()),
            metrics,
            violation_count: AtomicU64::new(0),
        }
    }

    /// Check if IP is exempt from rate limiting
    pub fn is_exempt(&self, addr: &SocketAddr) -> bool {
        self.config.exempt_ips.contains(&addr.ip())
    }

    /// Get or create client rate limiter
    pub fn get_client_limiter(&self, addr: SocketAddr) -> Arc<ClientRateLimit> {
        if self.is_exempt(&addr) {
            // Return a client limiter with no restrictions for exempt IPs
            return Arc::new(ClientRateLimit {
                addr,
                connection_limiter: None,
                query_limiter: None,
                connection_count: AtomicUsize::new(0),
                query_count: AtomicU64::new(0),
                last_activity: Mutex::new(Instant::now()),
                violations: AtomicU64::new(0),
                is_authenticated: std::sync::atomic::AtomicBool::new(true),
                is_superuser: std::sync::atomic::AtomicBool::new(true),
            });
        }

        let clients = self.clients.read();
        if let Some(client) = clients.get(&addr) {
            return client.clone();
        }
        drop(clients);

        // Create new client limiter
        let mut clients = self.clients.write();
        // Double-check in case another thread created it
        if let Some(client) = clients.get(&addr) {
            return client.clone();
        }

        let client = Arc::new(ClientRateLimit::new(addr, &self.config));
        clients.insert(addr, client.clone());
        client
    }

    /// Check if a connection is allowed
    pub fn allow_connection(&self, addr: SocketAddr) -> bool {
        if self.is_exempt(&addr) {
            return true;
        }

        let client = self.get_client_limiter(addr);
        let allowed = client.allow_connection();

        if !allowed {
            self.violation_count.fetch_add(1, Ordering::Relaxed);
            warn!("Connection rate limit exceeded for {}", addr);
            self.metrics
                .rate_limit_violations
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .connection_rate_limit_hits
                .fetch_add(1, Ordering::Relaxed);
        }

        allowed
    }

    /// Check if a query is allowed
    pub fn allow_query(&self, addr: SocketAddr, sql: &str) -> bool {
        if self.is_exempt(&addr) {
            return true;
        }

        let cost = QueryCost::estimate(sql);
        let mut tokens_needed = cost.tokens();

        // Apply adaptive limiting if enabled
        if self.config.adaptive_limiting {
            let multiplier = self.load_monitor.adaptive_multiplier();
            tokens_needed = (tokens_needed as f64 / multiplier) as u64;
        }

        // Check global rate limit first
        if let Some(global_limiter) = &self.global_limiter {
            if !global_limiter.try_acquire(tokens_needed) {
                self.violation_count.fetch_add(1, Ordering::Relaxed);
                warn!("Global rate limit exceeded for query from {}", addr);
                self.metrics
                    .rate_limit_violations
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .global_rate_limit_hits
                    .fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        // Check per-client rate limit
        let client = self.get_client_limiter(addr);
        let allowed = client.allow_query(cost);

        if !allowed {
            self.violation_count.fetch_add(1, Ordering::Relaxed);
            warn!("Query rate limit exceeded for {} (cost: {:?})", addr, cost);
            self.metrics
                .rate_limit_violations
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .query_rate_limit_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            debug!(
                "Query allowed for {} (cost: {:?}, tokens: {})",
                addr, cost, tokens_needed
            );
        }

        allowed
    }

    /// Release a connection
    pub fn release_connection(&self, addr: SocketAddr) {
        if let Some(client) = self.clients.read().get(&addr) {
            client.release_connection();
        }
    }

    /// Update client authentication status
    pub fn set_client_auth(&self, addr: SocketAddr, authenticated: bool, is_superuser: bool) {
        let client = self.get_client_limiter(addr);
        client.set_authenticated(authenticated, is_superuser);
    }

    /// Clean up expired client entries
    pub fn cleanup_expired(&self) {
        let timeout = Duration::from_secs(3600); // 1 hour
        let mut clients = self.clients.write();
        clients.retain(|_, client| !client.is_expired(timeout));
    }

    /// Get rate limiting statistics
    pub fn stats(&self) -> RateLimitStats {
        let clients = self.clients.read();
        let active_clients = clients.len();
        let total_violations = self.violation_count.load(Ordering::Relaxed);

        let global_tokens = self
            .global_limiter
            .as_ref()
            .map(|limiter| limiter.available_tokens())
            .unwrap_or(0);

        RateLimitStats {
            active_clients,
            total_violations,
            global_tokens_available: global_tokens,
            load_factor: self.load_monitor.load_factor(),
        }
    }

    /// Update server load for adaptive rate limiting
    pub fn update_load(&self, cpu: f64, memory: f64, connections: usize, query_rate: u64) {
        self.load_monitor
            .update_load(cpu, memory, connections, query_rate);
    }

    /// Get load monitor for metrics
    pub fn load_monitor(&self) -> &Arc<LoadMonitor> {
        &self.load_monitor
    }
}

/// Rate limiting statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitStats {
    pub active_clients: usize,
    pub total_violations: u64,
    pub global_tokens_available: u64,
    pub load_factor: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket() {
        let bucket = TokenBucket::new(10, 5); // 10 max tokens, 5 per second

        // Should allow initial burst
        for _ in 0..10 {
            assert!(bucket.try_acquire(1));
        }

        // Should be exhausted
        assert!(!bucket.try_acquire(1));

        // Wait for refill
        thread::sleep(Duration::from_millis(1100));

        // Should have some tokens back
        assert!(bucket.try_acquire(1));
    }

    #[test]
    fn test_query_cost_estimation() {
        assert_eq!(
            QueryCost::estimate("SELECT * FROM users WHERE id = 1"),
            QueryCost::Medium
        );
        assert_eq!(
            QueryCost::estimate("SELECT COUNT(*) FROM users"),
            QueryCost::High
        );
        assert_eq!(QueryCost::estimate("SELECT * FROM users"), QueryCost::High);
        assert_eq!(
            QueryCost::estimate("CREATE TABLE test (id INTEGER)"),
            QueryCost::Low
        );
        assert_eq!(
            QueryCost::estimate(
                "SELECT u.*, p.* FROM users u FULL OUTER JOIN posts p ON u.id = p.user_id"
            ),
            QueryCost::VeryHigh
        );
    }

    #[test]
    fn test_rate_limit_manager() {
        let config = RateLimitConfig::default();
        let metrics = Arc::new(Metrics::new());
        let manager = RateLimitManager::new(config, metrics);

        let addr: SocketAddr = "192.168.1.1:12345".parse().unwrap();

        // Should allow initial connections
        assert!(manager.allow_connection(addr));
        assert!(manager.allow_query(addr, "SELECT 1"));

        // Exempt IP should always be allowed
        let localhost: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        assert!(manager.allow_connection(localhost));
        assert!(manager.allow_query(localhost, "SELECT * FROM huge_table"));
    }

    #[test]
    fn test_load_monitor() {
        let monitor = LoadMonitor::new();

        // Test low load
        monitor.update_load(0.1, 0.2, 10, 100);
        assert!(monitor.load_factor() < 0.5);
        assert_eq!(monitor.adaptive_multiplier(), 1.0);

        // Test high load
        monitor.update_load(0.9, 0.8, 500, 1000);
        assert!(monitor.load_factor() > 0.5);
        assert!(monitor.adaptive_multiplier() < 1.0);
    }
}
