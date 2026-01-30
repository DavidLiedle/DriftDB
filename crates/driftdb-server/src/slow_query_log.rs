//! Slow query logging for DriftDB
//!
//! Tracks and logs queries that exceed configured thresholds
//! Helps identify performance bottlenecks in production

#![allow(dead_code)]

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use uuid::Uuid;

/// Configuration for slow query logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryConfig {
    /// Minimum duration to be considered slow (milliseconds)
    pub slow_threshold_ms: u64,
    /// Maximum number of slow queries to keep in memory
    pub max_stored_queries: usize,
    /// Enable logging to file
    pub log_to_file: bool,
    /// Enable logging to stdout
    pub log_to_stdout: bool,
    /// Path to slow query log file
    pub log_file_path: String,
}

impl Default for SlowQueryConfig {
    fn default() -> Self {
        Self {
            slow_threshold_ms: 1000, // 1 second
            max_stored_queries: 1000,
            log_to_file: true,
            log_to_stdout: false,
            log_file_path: "./logs/slow_queries.log".to_string(),
        }
    }
}

/// Represents a logged slow query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryEntry {
    /// Unique request ID for tracing
    pub request_id: String,
    /// The SQL query text
    pub query: String,
    /// Execution duration in milliseconds
    pub duration_ms: u64,
    /// Timestamp when query started
    pub timestamp: u64,
    /// Client address/identifier
    pub client_addr: String,
    /// Database user
    pub user: String,
    /// Database name
    pub database: String,
    /// Number of rows returned/affected
    pub rows_affected: Option<u64>,
    /// Additional context (transaction ID, etc.)
    pub context: Option<String>,
}

/// Slow query logger
pub struct SlowQueryLogger {
    config: Arc<RwLock<SlowQueryConfig>>,
    queries: Arc<RwLock<VecDeque<SlowQueryEntry>>>,
}

impl SlowQueryLogger {
    /// Create a new slow query logger
    pub fn new(config: SlowQueryConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            queries: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Log a query execution
    #[allow(clippy::too_many_arguments)]
    pub fn log_query(
        &self,
        query: String,
        duration: Duration,
        client_addr: String,
        user: String,
        database: String,
        rows_affected: Option<u64>,
        context: Option<String>,
    ) {
        let duration_ms = duration.as_millis() as u64;
        let threshold = self.config.read().slow_threshold_ms;

        // Only log if query exceeds threshold
        if duration_ms < threshold {
            return;
        }

        let request_id = Uuid::new_v4().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();

        let entry = SlowQueryEntry {
            request_id: request_id.clone(),
            query: query.clone(),
            duration_ms,
            timestamp,
            client_addr: client_addr.clone(),
            user: user.clone(),
            database: database.clone(),
            rows_affected,
            context,
        };

        // Store in memory
        {
            let mut queries = self.queries.write();
            let max_stored = self.config.read().max_stored_queries;

            queries.push_back(entry.clone());

            // Keep only most recent queries
            while queries.len() > max_stored {
                queries.pop_front();
            }
        }

        // Log to configured outputs
        let config = self.config.read();

        if config.log_to_stdout {
            warn!(
                "SLOW QUERY [{}ms] request_id={} user={} database={} client={} query={}",
                duration_ms, request_id, user, database, client_addr, query
            );
        }

        if config.log_to_file {
            self.log_to_file(&entry);
        }
    }

    /// Write slow query to log file
    fn log_to_file(&self, entry: &SlowQueryEntry) {
        let config = self.config.read();
        let log_path = &config.log_file_path;

        // Ensure log directory exists
        if let Some(parent) = std::path::Path::new(log_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Failed to create slow query log directory: {}", e);
                return;
            }
        }

        // Format log entry as JSON
        let log_line = match serde_json::to_string(entry) {
            Ok(json) => format!("{}\n", json),
            Err(e) => {
                warn!("Failed to serialize slow query entry: {}", e);
                return;
            }
        };

        // Append to log file
        use std::fs::OpenOptions;
        use std::io::Write;

        match OpenOptions::new().create(true).append(true).open(log_path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(log_line.as_bytes()) {
                    warn!("Failed to write to slow query log: {}", e);
                }
            }
            Err(e) => {
                warn!("Failed to open slow query log file: {}", e);
            }
        }
    }

    /// Get recent slow queries
    pub fn get_recent_queries(&self, limit: usize) -> Vec<SlowQueryEntry> {
        let queries = self.queries.read();
        queries.iter().rev().take(limit).cloned().collect()
    }

    /// Get slow queries within a time range
    pub fn get_queries_in_range(
        &self,
        start_timestamp: u64,
        end_timestamp: u64,
    ) -> Vec<SlowQueryEntry> {
        let queries = self.queries.read();
        queries
            .iter()
            .filter(|q| q.timestamp >= start_timestamp && q.timestamp <= end_timestamp)
            .cloned()
            .collect()
    }

    /// Get statistics about slow queries
    pub fn get_statistics(&self) -> SlowQueryStatistics {
        let queries = self.queries.read();

        if queries.is_empty() {
            return SlowQueryStatistics::default();
        }

        let total = queries.len();
        let total_duration: u64 = queries.iter().map(|q| q.duration_ms).sum();
        let avg_duration = total_duration / total as u64;

        let mut durations: Vec<u64> = queries.iter().map(|q| q.duration_ms).collect();
        durations.sort_unstable();

        let min_duration = *durations.first().unwrap();
        let max_duration = *durations.last().unwrap();
        let p50_duration = durations[durations.len() / 2];
        let p95_duration = durations[durations.len() * 95 / 100];
        let p99_duration = durations[durations.len() * 99 / 100];

        SlowQueryStatistics {
            total_slow_queries: total,
            avg_duration_ms: avg_duration,
            min_duration_ms: min_duration,
            max_duration_ms: max_duration,
            p50_duration_ms: p50_duration,
            p95_duration_ms: p95_duration,
            p99_duration_ms: p99_duration,
        }
    }

    /// Update configuration
    pub fn update_config(&self, config: SlowQueryConfig) {
        *self.config.write() = config;
        info!("Slow query logger configuration updated");
    }

    /// Clear stored slow queries
    pub fn clear(&self) {
        self.queries.write().clear();
        info!("Slow query log cleared");
    }

    /// Get current configuration
    pub fn get_config(&self) -> SlowQueryConfig {
        self.config.read().clone()
    }
}

/// Statistics about logged slow queries
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SlowQueryStatistics {
    pub total_slow_queries: usize,
    pub avg_duration_ms: u64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub p50_duration_ms: u64,
    pub p95_duration_ms: u64,
    pub p99_duration_ms: u64,
}

/// Request ID generator for tracing
pub struct RequestIdGenerator {
    counter: std::sync::atomic::AtomicU64,
}

impl RequestIdGenerator {
    pub fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Generate a unique request ID
    pub fn next(&self) -> String {
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("req_{:016x}", id)
    }
}

impl Default for RequestIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_query_logging() {
        let config = SlowQueryConfig {
            slow_threshold_ms: 100,
            max_stored_queries: 10,
            log_to_file: false,
            log_to_stdout: false,
            log_file_path: "/tmp/test_slow_queries.log".to_string(),
        };

        let logger = SlowQueryLogger::new(config);

        // Log a slow query
        logger.log_query(
            "SELECT * FROM users WHERE age > 30".to_string(),
            Duration::from_millis(150),
            "127.0.0.1:5432".to_string(),
            "testuser".to_string(),
            "testdb".to_string(),
            Some(100),
            Some("txn_123".to_string()),
        );

        // Log a fast query (should not be recorded)
        logger.log_query(
            "SELECT 1".to_string(),
            Duration::from_millis(5),
            "127.0.0.1:5432".to_string(),
            "testuser".to_string(),
            "testdb".to_string(),
            Some(1),
            None,
        );

        let recent = logger.get_recent_queries(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].query, "SELECT * FROM users WHERE age > 30");
        assert_eq!(recent[0].duration_ms, 150);
    }

    #[test]
    fn test_slow_query_statistics() {
        let config = SlowQueryConfig {
            slow_threshold_ms: 50,
            max_stored_queries: 100,
            log_to_file: false,
            log_to_stdout: false,
            log_file_path: "/tmp/test.log".to_string(),
        };

        let logger = SlowQueryLogger::new(config);

        // Log several slow queries
        for i in 1..=10 {
            logger.log_query(
                format!("Query {}", i),
                Duration::from_millis(i * 100),
                "localhost".to_string(),
                "user".to_string(),
                "db".to_string(),
                None,
                None,
            );
        }

        let stats = logger.get_statistics();
        assert_eq!(stats.total_slow_queries, 10);
        assert!(stats.avg_duration_ms > 0);
        assert!(stats.p95_duration_ms >= stats.p50_duration_ms);
    }

    #[test]
    fn test_request_id_generator() {
        let gen = RequestIdGenerator::new();

        let id1 = gen.next();
        let id2 = gen.next();

        assert_ne!(id1, id2);
        assert!(id1.starts_with("req_"));
        assert!(id2.starts_with("req_"));
    }

    #[test]
    fn test_max_stored_queries() {
        let config = SlowQueryConfig {
            slow_threshold_ms: 10,
            max_stored_queries: 5,
            log_to_file: false,
            log_to_stdout: false,
            log_file_path: "/tmp/test.log".to_string(),
        };

        let logger = SlowQueryLogger::new(config);

        // Log more queries than max
        for i in 1..=10 {
            logger.log_query(
                format!("Query {}", i),
                Duration::from_millis(20),
                "localhost".to_string(),
                "user".to_string(),
                "db".to_string(),
                None,
                None,
            );
        }

        let recent = logger.get_recent_queries(100);
        assert_eq!(recent.len(), 5); // Should only keep most recent 5
        assert_eq!(recent[0].query, "Query 10"); // Most recent first
    }
}
