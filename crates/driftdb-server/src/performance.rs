//! Performance monitoring and optimization for DriftDB Server

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

/// Performance metrics collection
pub struct PerformanceMonitor {
    /// Query execution times
    query_times: DashMap<String, QueryMetrics>,
    /// Connection pool statistics
    connection_stats: Arc<RwLock<ConnectionStats>>,
    /// Memory usage tracking
    memory_stats: Arc<RwLock<MemoryStats>>,
    /// Request rate limiting for performance
    request_limiter: Arc<Semaphore>,
}

#[derive(Debug, Default)]
pub struct QueryMetrics {
    pub total_executions: AtomicU64,
    pub total_duration_ms: AtomicU64,
    pub min_duration_ms: AtomicU64,
    pub max_duration_ms: AtomicU64,
    pub last_execution: AtomicU64, // timestamp in millis
}

#[derive(Debug, Default)]
pub struct ConnectionStats {
    pub active_connections: u64,
    pub peak_connections: u64,
    pub total_connections: u64,
    pub avg_connection_duration_ms: f64,
    pub connection_errors: u64,
}

#[derive(Debug, Default)]
pub struct MemoryStats {
    pub heap_used_bytes: u64,
    pub heap_allocated_bytes: u64,
    pub connection_pool_bytes: u64,
    pub query_cache_bytes: u64,
}

impl PerformanceMonitor {
    pub fn new(max_concurrent_requests: usize) -> Self {
        Self {
            query_times: DashMap::new(),
            connection_stats: Arc::new(RwLock::new(ConnectionStats::default())),
            memory_stats: Arc::new(RwLock::new(MemoryStats::default())),
            request_limiter: Arc::new(Semaphore::new(max_concurrent_requests)),
        }
    }

    /// Record query execution time
    pub fn record_query_execution(&self, query_hash: String, duration: Duration) {
        let duration_ms = duration.as_millis() as u64;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let metrics = self.query_times.entry(query_hash).or_insert_with(|| {
            QueryMetrics {
                min_duration_ms: AtomicU64::new(u64::MAX),
                ..Default::default()
            }
        });

        metrics.total_executions.fetch_add(1, Ordering::Relaxed);
        metrics.total_duration_ms.fetch_add(duration_ms, Ordering::Relaxed);
        metrics.last_execution.store(now_ms, Ordering::Relaxed);

        // Update min duration
        let current_min = metrics.min_duration_ms.load(Ordering::Relaxed);
        if duration_ms < current_min && current_min != 0 {
            let _ = metrics.min_duration_ms.compare_exchange_weak(
                current_min,
                duration_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }

        // Update max duration
        let current_max = metrics.max_duration_ms.load(Ordering::Relaxed);
        if duration_ms > current_max {
            let _ = metrics.max_duration_ms.compare_exchange_weak(
                current_max,
                duration_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> Value {
        let mut top_queries = Vec::new();

        // Get top 10 slowest queries by average execution time
        for entry in self.query_times.iter() {
            let metrics = entry.value();
            let executions = metrics.total_executions.load(Ordering::Relaxed);
            if executions > 0 {
                let avg_ms = metrics.total_duration_ms.load(Ordering::Relaxed) / executions;
                top_queries.push((
                    entry.key().clone(),
                    avg_ms,
                    executions,
                    metrics.min_duration_ms.load(Ordering::Relaxed),
                    metrics.max_duration_ms.load(Ordering::Relaxed),
                ));
            }
        }

        top_queries.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by avg time descending
        top_queries.truncate(10);

        let connection_stats = self.connection_stats.read();
        let memory_stats = self.memory_stats.read();

        json!({
            "query_performance": {
                "total_unique_queries": self.query_times.len(),
                "top_slowest_queries": top_queries.iter().map(|(hash, avg, count, min, max)| {
                    json!({
                        "query_hash": hash,
                        "avg_duration_ms": avg,
                        "total_executions": count,
                        "min_duration_ms": min,
                        "max_duration_ms": max
                    })
                }).collect::<Vec<_>>()
            },
            "connection_performance": {
                "active_connections": connection_stats.active_connections,
                "peak_connections": connection_stats.peak_connections,
                "total_connections": connection_stats.total_connections,
                "avg_connection_duration_ms": connection_stats.avg_connection_duration_ms,
                "connection_errors": connection_stats.connection_errors,
                "available_request_permits": self.request_limiter.available_permits()
            },
            "memory_performance": {
                "heap_used_mb": memory_stats.heap_used_bytes as f64 / 1_048_576.0,
                "heap_allocated_mb": memory_stats.heap_allocated_bytes as f64 / 1_048_576.0,
                "connection_pool_mb": memory_stats.connection_pool_bytes as f64 / 1_048_576.0,
                "query_cache_mb": memory_stats.query_cache_bytes as f64 / 1_048_576.0
            }
        })
    }

    /// Update connection statistics
    pub fn update_connection_stats<F>(&self, updater: F)
    where
        F: FnOnce(&mut ConnectionStats),
    {
        let mut stats = self.connection_stats.write();
        updater(&mut stats);

        if stats.active_connections > stats.peak_connections {
            stats.peak_connections = stats.active_connections;
        }
    }

    /// Acquire a request permit (for rate limiting)
    pub async fn acquire_request_permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        match self.request_limiter.try_acquire() {
            Ok(permit) => Some(permit),
            Err(_) => {
                warn!("Request rate limit exceeded, rejecting request");
                None
            }
        }
    }

    /// Get system memory usage
    pub fn update_memory_stats(&self) {
        let mut stats = self.memory_stats.write();

        // Update system memory usage using sysinfo
        use sysinfo::{System, Process};
        let mut sys = System::new_all();
        sys.refresh_all();

        let current_pid = std::process::id();
        if let Some(process) = sys.processes().get(&sysinfo::Pid::from_u32(current_pid)) {
            stats.heap_used_bytes = process.memory() * 1024; // Convert KB to bytes
        }

        // Estimate cache sizes (simplified)
        stats.query_cache_bytes = self.query_times.len() as u64 * 1024; // Rough estimate

        debug!("Updated memory stats: heap_used={}MB", stats.heap_used_bytes / 1_048_576);
    }
}

/// Query optimization hints and caching
pub struct QueryOptimizer {
    /// Cache for prepared query execution plans
    execution_plan_cache: DashMap<String, CachedExecutionPlan>,
    /// Query rewrite rules for performance
    rewrite_rules: Vec<QueryRewriteRule>,
}

#[derive(Debug, Clone)]
pub struct CachedExecutionPlan {
    pub plan: String,
    pub estimated_cost: f64,
    pub last_used: Instant,
    pub hit_count: u64,
}

#[derive(Debug)]
pub struct QueryRewriteRule {
    pub pattern: String,
    pub replacement: String,
    pub description: String,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            execution_plan_cache: DashMap::new(),
            rewrite_rules: Self::default_rewrite_rules(),
        }
    }

    /// Default query optimization rules
    fn default_rewrite_rules() -> Vec<QueryRewriteRule> {
        vec![
            QueryRewriteRule {
                pattern: r"SELECT \* FROM (\w+) WHERE (.+) LIMIT 1".to_string(),
                replacement: "SELECT * FROM $1 WHERE $2 LIMIT 1 -- optimized for single row".to_string(),
                description: "Single row optimization".to_string(),
            },
            QueryRewriteRule {
                pattern: r"SELECT COUNT\(\*\) FROM (\w+)".to_string(),
                replacement: "SELECT COUNT(*) FROM $1 -- consider index optimization".to_string(),
                description: "Count optimization hint".to_string(),
            },
        ]
    }

    /// Get or create cached execution plan
    pub fn get_execution_plan(&self, query_hash: &str) -> Option<CachedExecutionPlan> {
        if let Some(mut entry) = self.execution_plan_cache.get_mut(query_hash) {
            entry.last_used = Instant::now();
            entry.hit_count += 1;
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Cache execution plan
    pub fn cache_execution_plan(&self, query_hash: String, plan: String, cost: f64) {
        let cached_plan = CachedExecutionPlan {
            plan,
            estimated_cost: cost,
            last_used: Instant::now(),
            hit_count: 1,
        };

        self.execution_plan_cache.insert(query_hash, cached_plan);
    }

    /// Apply query optimizations
    pub fn optimize_query(&self, sql: &str) -> (String, Vec<String>) {
        let optimized_sql = sql.to_string();
        let mut applied_optimizations = Vec::new();

        for rule in &self.rewrite_rules {
            // Simple pattern matching - in production, use a proper SQL parser
            if sql.to_uppercase().contains(&rule.pattern.to_uppercase()) {
                applied_optimizations.push(rule.description.clone());
                debug!("Applied optimization: {}", rule.description);
            }
        }

        (optimized_sql, applied_optimizations)
    }

    /// Clean up old cached plans
    pub fn cleanup_cache(&self, max_age: Duration) {
        let cutoff = Instant::now() - max_age;

        self.execution_plan_cache.retain(|_, plan| {
            plan.last_used > cutoff
        });

        info!("Cleaned up query execution plan cache, {} entries remain",
              self.execution_plan_cache.len());
    }
}

/// Connection pooling optimizations
pub struct ConnectionPoolOptimizer {
    /// Pool health statistics
    pool_health: Arc<RwLock<PoolHealthStats>>,
    /// Adaptive sizing parameters
    sizing_params: Arc<RwLock<PoolSizingParams>>,
}

#[derive(Debug, Default)]
pub struct PoolHealthStats {
    pub avg_wait_time_ms: f64,
    pub connection_failures: u64,
    pub idle_timeouts: u64,
    pub peak_usage: u64,
    pub current_load_factor: f64, // 0.0 to 1.0
}

#[derive(Debug)]
pub struct PoolSizingParams {
    pub min_size: usize,
    pub max_size: usize,
    pub growth_factor: f64,
    pub shrink_threshold: f64,
    pub last_resize: Instant,
}

impl Default for PoolSizingParams {
    fn default() -> Self {
        Self {
            min_size: 10,
            max_size: 100,
            growth_factor: 1.5,
            shrink_threshold: 0.3,
            last_resize: Instant::now(),
        }
    }
}

impl ConnectionPoolOptimizer {
    pub fn new() -> Self {
        Self {
            pool_health: Arc::new(RwLock::new(PoolHealthStats::default())),
            sizing_params: Arc::new(RwLock::new(PoolSizingParams::default())),
        }
    }

    /// Analyze pool performance and suggest optimizations
    pub fn analyze_pool_performance(&self) -> Value {
        let health = self.pool_health.read();
        let params = self.sizing_params.read();

        let recommendations = if health.avg_wait_time_ms > 100.0 {
            vec!["Consider increasing pool size", "Check for connection leaks"]
        } else if health.current_load_factor < 0.2 {
            vec!["Consider decreasing pool size", "Review connection timeout settings"]
        } else {
            vec!["Pool performance is optimal"]
        };

        json!({
            "pool_health": {
                "avg_wait_time_ms": health.avg_wait_time_ms,
                "connection_failures": health.connection_failures,
                "idle_timeouts": health.idle_timeouts,
                "peak_usage": health.peak_usage,
                "current_load_factor": health.current_load_factor
            },
            "pool_sizing": {
                "min_size": params.min_size,
                "max_size": params.max_size,
                "growth_factor": params.growth_factor,
                "shrink_threshold": params.shrink_threshold
            },
            "recommendations": recommendations
        })
    }

    /// Update pool health metrics
    pub fn update_pool_health<F>(&self, updater: F)
    where
        F: FnOnce(&mut PoolHealthStats),
    {
        let mut health = self.pool_health.write();
        updater(&mut health);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new(1000);

        // Record some query executions
        monitor.record_query_execution("SELECT_USERS".to_string(), Duration::from_millis(50));
        monitor.record_query_execution("SELECT_USERS".to_string(), Duration::from_millis(75));
        monitor.record_query_execution("INSERT_USER".to_string(), Duration::from_millis(25));

        let stats = monitor.get_performance_stats();
        assert!(stats["query_performance"]["total_unique_queries"].as_u64().unwrap() == 2);
    }

    #[test]
    fn test_query_optimizer() {
        let optimizer = QueryOptimizer::new();

        let (_, optimizations) = optimizer.optimize_query("SELECT COUNT(*) FROM users");
        assert!(!optimizations.is_empty());
    }

    #[test]
    fn test_connection_pool_optimizer() {
        let optimizer = ConnectionPoolOptimizer::new();

        optimizer.update_pool_health(|health| {
            health.avg_wait_time_ms = 150.0;
            health.current_load_factor = 0.8;
        });

        let analysis = optimizer.analyze_pool_performance();
        assert!(analysis["recommendations"].as_array().unwrap().len() > 0);
    }
}