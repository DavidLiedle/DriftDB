//! Metrics Collection Module
//!
//! Provides Prometheus-compatible metrics for DriftDB server monitoring

#![allow(dead_code, unused_variables, unused_imports)]

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::Response, routing::get, Router};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use sysinfo::{Pid, System};
use tracing::{debug, error};

use crate::session::SessionManager;
use driftdb_core::Engine;

lazy_static! {
    /// Global metrics registry
    pub static ref REGISTRY: Registry = Registry::new();

    /// System information for CPU metrics
    static ref SYSTEM: RwLock<System> = RwLock::new(System::new_all());

    /// Total number of queries executed
    pub static ref QUERY_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_queries_total", "Total number of queries executed")
            .namespace("driftdb"),
        &["query_type", "status"]
    ).unwrap();

    /// Query execution duration histogram
    pub static ref QUERY_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_query_duration_seconds", "Query execution duration in seconds")
            .namespace("driftdb")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
        &["query_type"]
    ).unwrap();

    /// Active database connections
    pub static ref ACTIVE_CONNECTIONS: Gauge = Gauge::new(
        "driftdb_active_connections",
        "Number of active database connections"
    ).unwrap();

    /// Total connections accepted
    pub static ref CONNECTIONS_TOTAL: Counter = Counter::new(
        "driftdb_connections_total",
        "Total number of connections accepted"
    ).unwrap();

    /// Database size metrics
    pub static ref DATABASE_SIZE_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("driftdb_database_size_bytes", "Database size in bytes")
            .namespace("driftdb"),
        &["table", "component"]
    ).unwrap();

    /// Error rate by error type
    pub static ref ERROR_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_errors_total", "Total number of errors by type")
            .namespace("driftdb"),
        &["error_type", "operation"]
    ).unwrap();

    /// Server uptime
    pub static ref SERVER_UPTIME: Gauge = Gauge::new(
        "driftdb_server_uptime_seconds",
        "Server uptime in seconds"
    ).unwrap();

    /// Memory usage
    pub static ref MEMORY_USAGE_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("driftdb_memory_usage_bytes", "Memory usage in bytes")
            .namespace("driftdb"),
        &["type"]
    ).unwrap();

    /// CPU usage
    pub static ref CPU_USAGE_PERCENT: GaugeVec = GaugeVec::new(
        Opts::new("driftdb_cpu_usage_percent", "CPU usage percentage")
            .namespace("driftdb"),
        &["type"]
    ).unwrap();

    /// Connection pool size
    pub static ref POOL_SIZE: Gauge = Gauge::new(
        "driftdb_pool_size_total",
        "Total connections in the pool"
    ).unwrap();

    /// Available connections in pool
    pub static ref POOL_AVAILABLE: Gauge = Gauge::new(
        "driftdb_pool_available_connections",
        "Number of available connections in the pool"
    ).unwrap();

    /// Active connections from pool
    pub static ref POOL_ACTIVE: Gauge = Gauge::new(
        "driftdb_pool_active_connections",
        "Number of active connections from the pool"
    ).unwrap();

    /// Connection acquisition wait time
    pub static ref POOL_WAIT_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_pool_wait_time_seconds", "Time waiting to acquire a connection from the pool")
            .namespace("driftdb")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
        &["result"]
    ).unwrap();

    /// Pool connection total created
    pub static ref POOL_CONNECTIONS_CREATED: Gauge = Gauge::new(
        "driftdb_pool_connections_created_total",
        "Total number of connections created by the pool"
    ).unwrap();

    /// Connection encryption status
    pub static ref CONNECTION_ENCRYPTION: CounterVec = CounterVec::new(
        Opts::new("driftdb_connections_by_encryption", "Total connections by encryption status")
            .namespace("driftdb"),
        &["encrypted"]
    ).unwrap();

    // ========== Enhanced Metrics for Production Monitoring ==========

    /// Query latency histogram for percentile calculation (p50, p95, p99)
    /// Use Prometheus histogram_quantile() function to calculate percentiles
    pub static ref QUERY_LATENCY_HISTOGRAM: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_query_latency_seconds", "Query execution latency in seconds for percentile calculation")
            .namespace("driftdb")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
        &["query_type"]
    ).unwrap();

    /// Transaction metrics
    pub static ref TRANSACTION_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_transactions_total", "Total number of transactions")
            .namespace("driftdb"),
        &["type", "status"] // type: read-only, read-write; status: committed, rolled-back, aborted
    ).unwrap();

    pub static ref TRANSACTION_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_transaction_duration_seconds", "Transaction duration in seconds")
            .namespace("driftdb")
            .buckets(vec![0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]),
        &["type"]
    ).unwrap();

    pub static ref ACTIVE_TRANSACTIONS: Gauge = Gauge::new(
        "driftdb_active_transactions",
        "Number of currently active transactions"
    ).unwrap();

    /// Pool health and performance metrics
    pub static ref POOL_WAIT_TIME_TOTAL: Counter = Counter::new(
        "driftdb_pool_wait_time_seconds_total",
        "Total time spent waiting for pool connections"
    ).unwrap();

    pub static ref POOL_TIMEOUTS_TOTAL: Counter = Counter::new(
        "driftdb_pool_timeouts_total",
        "Total number of connection acquisition timeouts"
    ).unwrap();

    pub static ref POOL_ERRORS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_pool_errors_total", "Total pool errors by type")
            .namespace("driftdb"),
        &["error_type"]
    ).unwrap();

    pub static ref POOL_UTILIZATION: Gauge = Gauge::new(
        "driftdb_pool_utilization_percent",
        "Pool utilization percentage (active / total)"
    ).unwrap();

    /// WAL (Write-Ahead Log) metrics
    pub static ref WAL_WRITES_TOTAL: Counter = Counter::new(
        "driftdb_wal_writes_total",
        "Total number of WAL writes"
    ).unwrap();

    pub static ref WAL_SYNC_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new("driftdb_wal_sync_duration_seconds", "WAL fsync duration in seconds")
            .namespace("driftdb")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1])
    ).unwrap();

    pub static ref WAL_SIZE_BYTES: Gauge = Gauge::new(
        "driftdb_wal_size_bytes",
        "Current WAL size in bytes"
    ).unwrap();

    pub static ref WAL_SEGMENTS_TOTAL: Gauge = Gauge::new(
        "driftdb_wal_segments_total",
        "Total number of WAL segments"
    ).unwrap();

    /// Cache metrics
    pub static ref CACHE_HITS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_cache_hits_total", "Total cache hits by cache type")
            .namespace("driftdb"),
        &["cache_type"]
    ).unwrap();

    pub static ref CACHE_MISSES_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_cache_misses_total", "Total cache misses by cache type")
            .namespace("driftdb"),
        &["cache_type"]
    ).unwrap();

    pub static ref CACHE_SIZE_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("driftdb_cache_size_bytes", "Cache size in bytes by cache type")
            .namespace("driftdb"),
        &["cache_type"]
    ).unwrap();

    pub static ref CACHE_EVICTIONS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_cache_evictions_total", "Total cache evictions by cache type")
            .namespace("driftdb"),
        &["cache_type"]
    ).unwrap();

    /// Index usage metrics
    pub static ref INDEX_SCANS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_index_scans_total", "Total index scans by table and index")
            .namespace("driftdb"),
        &["table", "index"]
    ).unwrap();

    pub static ref TABLE_SCANS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_table_scans_total", "Total full table scans by table")
            .namespace("driftdb"),
        &["table"]
    ).unwrap();

    /// Disk I/O metrics
    pub static ref DISK_READS_TOTAL: Counter = Counter::new(
        "driftdb_disk_reads_total",
        "Total number of disk reads"
    ).unwrap();

    pub static ref DISK_WRITES_TOTAL: Counter = Counter::new(
        "driftdb_disk_writes_total",
        "Total number of disk writes"
    ).unwrap();

    pub static ref DISK_READ_BYTES_TOTAL: Counter = Counter::new(
        "driftdb_disk_read_bytes_total",
        "Total bytes read from disk"
    ).unwrap();

    pub static ref DISK_WRITE_BYTES_TOTAL: Counter = Counter::new(
        "driftdb_disk_write_bytes_total",
        "Total bytes written to disk"
    ).unwrap();

    /// Replication metrics (for future use)
    pub static ref REPLICATION_LAG_SECONDS: GaugeVec = GaugeVec::new(
        Opts::new("driftdb_replication_lag_seconds", "Replication lag in seconds")
            .namespace("driftdb"),
        &["replica"]
    ).unwrap();

    pub static ref REPLICATION_BYTES_SENT: CounterVec = CounterVec::new(
        Opts::new("driftdb_replication_bytes_sent_total", "Total bytes sent to replicas")
            .namespace("driftdb"),
        &["replica"]
    ).unwrap();

    pub static ref REPLICATION_STATUS: GaugeVec = GaugeVec::new(
        Opts::new("driftdb_replication_status", "Replication status (1=healthy, 0=unhealthy)")
            .namespace("driftdb"),
        &["replica"]
    ).unwrap();

    /// Rate limiting metrics
    pub static ref RATE_LIMIT_HITS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_rate_limit_hits_total", "Total rate limit hits by type")
            .namespace("driftdb"),
        &["limit_type"]
    ).unwrap();

    pub static ref RATE_LIMIT_BLOCKS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_rate_limit_blocks_total", "Total rate limit blocks by type")
            .namespace("driftdb"),
        &["limit_type"]
    ).unwrap();

    /// Authentication metrics
    pub static ref AUTH_ATTEMPTS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_auth_attempts_total", "Total authentication attempts")
            .namespace("driftdb"),
        &["method", "result"] // method: password, trust, cert; result: success, failure
    ).unwrap();

    pub static ref AUTH_FAILURES_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_auth_failures_total", "Total authentication failures by reason")
            .namespace("driftdb"),
        &["reason"]
    ).unwrap();

    /// Snapshot and compaction metrics
    pub static ref SNAPSHOTS_CREATED_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_snapshots_created_total", "Total snapshots created by table")
            .namespace("driftdb"),
        &["table"]
    ).unwrap();

    pub static ref COMPACTIONS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_compactions_total", "Total compactions by table")
            .namespace("driftdb"),
        &["table"]
    ).unwrap();

    pub static ref COMPACTION_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_compaction_duration_seconds", "Compaction duration in seconds")
            .namespace("driftdb")
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]),
        &["table"]
    ).unwrap();

    /// Slow query metrics
    pub static ref SLOW_QUERIES_TOTAL: CounterVec = CounterVec::new(
        Opts::new("driftdb_slow_queries_total", "Total slow queries by type")
            .namespace("driftdb"),
        &["query_type"]
    ).unwrap();

    pub static ref QUERY_ROWS_RETURNED: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_query_rows_returned", "Number of rows returned by queries")
            .namespace("driftdb")
            .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0]),
        &["query_type"]
    ).unwrap();

    pub static ref QUERY_ROWS_AFFECTED: HistogramVec = HistogramVec::new(
        HistogramOpts::new("driftdb_query_rows_affected", "Number of rows affected by queries")
            .namespace("driftdb")
            .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0]),
        &["query_type"]
    ).unwrap();
}

/// Initialize all metrics with the registry
pub fn init_metrics() -> anyhow::Result<()> {
    REGISTRY.register(Box::new(QUERY_TOTAL.clone()))?;
    REGISTRY.register(Box::new(QUERY_DURATION.clone()))?;
    REGISTRY.register(Box::new(ACTIVE_CONNECTIONS.clone()))?;
    REGISTRY.register(Box::new(CONNECTIONS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(DATABASE_SIZE_BYTES.clone()))?;
    REGISTRY.register(Box::new(ERROR_TOTAL.clone()))?;
    REGISTRY.register(Box::new(SERVER_UPTIME.clone()))?;
    REGISTRY.register(Box::new(MEMORY_USAGE_BYTES.clone()))?;
    REGISTRY.register(Box::new(CPU_USAGE_PERCENT.clone()))?;
    REGISTRY.register(Box::new(POOL_SIZE.clone()))?;
    REGISTRY.register(Box::new(POOL_AVAILABLE.clone()))?;
    REGISTRY.register(Box::new(POOL_ACTIVE.clone()))?;
    REGISTRY.register(Box::new(POOL_WAIT_TIME.clone()))?;
    REGISTRY.register(Box::new(POOL_CONNECTIONS_CREATED.clone()))?;
    REGISTRY.register(Box::new(CONNECTION_ENCRYPTION.clone()))?;

    // Register enhanced metrics
    REGISTRY.register(Box::new(QUERY_LATENCY_HISTOGRAM.clone()))?;
    REGISTRY.register(Box::new(TRANSACTION_TOTAL.clone()))?;
    REGISTRY.register(Box::new(TRANSACTION_DURATION.clone()))?;
    REGISTRY.register(Box::new(ACTIVE_TRANSACTIONS.clone()))?;
    REGISTRY.register(Box::new(POOL_WAIT_TIME_TOTAL.clone()))?;
    REGISTRY.register(Box::new(POOL_TIMEOUTS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(POOL_ERRORS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(POOL_UTILIZATION.clone()))?;
    REGISTRY.register(Box::new(WAL_WRITES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(WAL_SYNC_DURATION.clone()))?;
    REGISTRY.register(Box::new(WAL_SIZE_BYTES.clone()))?;
    REGISTRY.register(Box::new(WAL_SEGMENTS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(CACHE_HITS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(CACHE_MISSES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(CACHE_SIZE_BYTES.clone()))?;
    REGISTRY.register(Box::new(CACHE_EVICTIONS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(INDEX_SCANS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(TABLE_SCANS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(DISK_READS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(DISK_WRITES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(DISK_READ_BYTES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(DISK_WRITE_BYTES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(REPLICATION_LAG_SECONDS.clone()))?;
    REGISTRY.register(Box::new(REPLICATION_BYTES_SENT.clone()))?;
    REGISTRY.register(Box::new(REPLICATION_STATUS.clone()))?;
    REGISTRY.register(Box::new(RATE_LIMIT_HITS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(RATE_LIMIT_BLOCKS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(AUTH_ATTEMPTS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(AUTH_FAILURES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(SNAPSHOTS_CREATED_TOTAL.clone()))?;
    REGISTRY.register(Box::new(COMPACTIONS_TOTAL.clone()))?;
    REGISTRY.register(Box::new(COMPACTION_DURATION.clone()))?;
    REGISTRY.register(Box::new(SLOW_QUERIES_TOTAL.clone()))?;
    REGISTRY.register(Box::new(QUERY_ROWS_RETURNED.clone()))?;
    REGISTRY.register(Box::new(QUERY_ROWS_AFFECTED.clone()))?;

    debug!(
        "Metrics initialized successfully - {} metrics registered",
        51
    );
    Ok(())
}

/// Application state for metrics endpoints
#[derive(Clone)]
pub struct MetricsState {
    pub engine: Arc<RwLock<Engine>>,
    #[allow(dead_code)]
    pub session_manager: Arc<SessionManager>,
    pub start_time: std::time::Instant,
}

impl MetricsState {
    pub fn new(engine: Arc<RwLock<Engine>>, session_manager: Arc<SessionManager>) -> Self {
        Self {
            engine,
            session_manager,
            start_time: std::time::Instant::now(),
        }
    }
}

/// Create the metrics router
pub fn create_metrics_router(state: MetricsState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// Prometheus metrics endpoint
async fn metrics_handler(
    State(state): State<MetricsState>,
) -> Result<Response<String>, StatusCode> {
    debug!("Metrics endpoint requested");

    // Update dynamic metrics before serving
    update_dynamic_metrics(&state);

    // Encode metrics in Prometheus format
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!("Failed to encode metrics: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let body = String::from_utf8(buffer).map_err(|e| {
        error!("Failed to convert metrics to string: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let response = Response::builder()
        .status(200)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(body)
        .map_err(|e| {
            error!("Failed to build response: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(response)
}

/// Update dynamic metrics that change over time
fn update_dynamic_metrics(state: &MetricsState) {
    // Update server uptime
    let uptime_seconds = state.start_time.elapsed().as_secs() as f64;
    SERVER_UPTIME.set(uptime_seconds);

    // Update database size metrics
    if let Some(engine) = state.engine.try_read() {
        match collect_database_size_metrics(&engine) {
            Ok(_) => debug!("Database size metrics updated"),
            Err(e) => error!("Failed to update database size metrics: {}", e),
        }
    }

    // Update system metrics (memory and CPU)
    update_system_metrics();
}

/// Collect database size metrics
fn collect_database_size_metrics(engine: &Engine) -> anyhow::Result<()> {
    // Get list of tables
    let tables = engine.list_tables();

    for table_name in tables {
        // Get actual table storage breakdown
        if let Ok(breakdown) = engine.get_table_storage_breakdown(&table_name) {
            // Set metrics for each storage component
            for (component, size_bytes) in breakdown {
                DATABASE_SIZE_BYTES
                    .with_label_values(&[&table_name, &component])
                    .set(size_bytes as f64);
            }
        }

        // Also get and record total table size
        if let Ok(total_size) = engine.get_table_size(&table_name) {
            DATABASE_SIZE_BYTES
                .with_label_values(&[table_name.as_str(), "total"])
                .set(total_size as f64);
        }
    }

    // Record total database size
    let total_db_size = engine.get_total_database_size();
    DATABASE_SIZE_BYTES
        .with_label_values(&["_total", "all"])
        .set(total_db_size as f64);

    Ok(())
}

/// Update system metrics (memory and CPU)
fn update_system_metrics() {
    // Get or update system information
    let mut sys = SYSTEM.write();
    sys.refresh_all();
    sys.refresh_cpu();

    // Get current process info
    let pid = Pid::from(std::process::id() as usize);
    if let Some(process) = sys.process(pid) {
        // Process memory metrics
        MEMORY_USAGE_BYTES
            .with_label_values(&["process_virtual"])
            .set(process.virtual_memory() as f64);

        MEMORY_USAGE_BYTES
            .with_label_values(&["process_physical"])
            .set(process.memory() as f64 * 1024.0); // Convert from KB to bytes

        // Process CPU usage
        CPU_USAGE_PERCENT
            .with_label_values(&["process"])
            .set(process.cpu_usage() as f64);
    }

    // System-wide memory metrics
    MEMORY_USAGE_BYTES
        .with_label_values(&["system_total"])
        .set(sys.total_memory() as f64 * 1024.0); // Convert from KB to bytes

    MEMORY_USAGE_BYTES
        .with_label_values(&["system_used"])
        .set(sys.used_memory() as f64 * 1024.0); // Convert from KB to bytes

    MEMORY_USAGE_BYTES
        .with_label_values(&["system_available"])
        .set(sys.available_memory() as f64 * 1024.0); // Convert from KB to bytes

    // System-wide CPU metrics
    let cpus = sys.cpus();
    if !cpus.is_empty() {
        let total_cpu_usage: f32 =
            cpus.iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / cpus.len() as f32;
        CPU_USAGE_PERCENT
            .with_label_values(&["system"])
            .set(total_cpu_usage as f64);
    }

    // Per-CPU core metrics
    for (i, cpu) in sys.cpus().iter().enumerate() {
        CPU_USAGE_PERCENT
            .with_label_values(&[&format!("core_{}", i)])
            .set(cpu.cpu_usage() as f64);
    }
}

/// Metrics helper functions for use throughout the application
/// Record a query execution
pub fn record_query(query_type: &str, status: &str, duration_seconds: f64) {
    QUERY_TOTAL.with_label_values(&[query_type, status]).inc();

    QUERY_DURATION
        .with_label_values(&[query_type])
        .observe(duration_seconds);
}

/// Record a new connection
pub fn record_connection() {
    CONNECTIONS_TOTAL.inc();
    ACTIVE_CONNECTIONS.inc();
}

/// Record a connection closed
pub fn record_connection_closed() {
    ACTIVE_CONNECTIONS.dec();
}

/// Record an error
pub fn record_error(error_type: &str, operation: &str) {
    ERROR_TOTAL
        .with_label_values(&[error_type, operation])
        .inc();
}

/// Update pool size metrics
pub fn update_pool_size(total: usize, available: usize, active: usize) {
    POOL_SIZE.set(total as f64);
    POOL_AVAILABLE.set(available as f64);
    POOL_ACTIVE.set(active as f64);
}

/// Record a connection acquisition wait time
#[allow(dead_code)]
pub fn record_pool_wait_time(duration_seconds: f64, success: bool) {
    let result = if success { "success" } else { "timeout" };
    POOL_WAIT_TIME
        .with_label_values(&[result])
        .observe(duration_seconds);
}

/// Update the total connections created by the pool
#[allow(dead_code)]
pub fn update_pool_connections_created(total: u64) {
    POOL_CONNECTIONS_CREATED.set(total as f64);
}

/// Record a connection encryption status
pub fn record_connection_encryption(is_encrypted: bool) {
    let label = if is_encrypted { "true" } else { "false" };
    CONNECTION_ENCRYPTION.with_label_values(&[label]).inc();
}

// ========== Enhanced Metrics Helper Functions ==========

/// Record query latency for percentile calculation
pub fn record_query_latency(query_type: &str, duration_seconds: f64) {
    QUERY_LATENCY_HISTOGRAM
        .with_label_values(&[query_type])
        .observe(duration_seconds);
}

/// Record transaction start
pub fn record_transaction_start() {
    ACTIVE_TRANSACTIONS.inc();
}

/// Record transaction completion
pub fn record_transaction_complete(txn_type: &str, status: &str, duration_seconds: f64) {
    ACTIVE_TRANSACTIONS.dec();
    TRANSACTION_TOTAL
        .with_label_values(&[txn_type, status])
        .inc();
    TRANSACTION_DURATION
        .with_label_values(&[txn_type])
        .observe(duration_seconds);
}

/// Record pool timeout
pub fn record_pool_timeout() {
    POOL_TIMEOUTS_TOTAL.inc();
}

/// Record pool error
pub fn record_pool_error(error_type: &str) {
    POOL_ERRORS_TOTAL.with_label_values(&[error_type]).inc();
}

/// Update pool utilization percentage
pub fn update_pool_utilization(utilization_percent: f64) {
    POOL_UTILIZATION.set(utilization_percent);
}

/// Record WAL write
pub fn record_wal_write() {
    WAL_WRITES_TOTAL.inc();
}

/// Record WAL sync duration
pub fn record_wal_sync(duration_seconds: f64) {
    WAL_SYNC_DURATION.observe(duration_seconds);
}

/// Update WAL size
pub fn update_wal_size(size_bytes: u64) {
    WAL_SIZE_BYTES.set(size_bytes as f64);
}

/// Update WAL segments count
pub fn update_wal_segments(count: usize) {
    WAL_SEGMENTS_TOTAL.set(count as f64);
}

/// Record cache hit
pub fn record_cache_hit(cache_type: &str) {
    CACHE_HITS_TOTAL.with_label_values(&[cache_type]).inc();
}

/// Record cache miss
pub fn record_cache_miss(cache_type: &str) {
    CACHE_MISSES_TOTAL.with_label_values(&[cache_type]).inc();
}

/// Update cache size
pub fn update_cache_size(cache_type: &str, size_bytes: usize) {
    CACHE_SIZE_BYTES
        .with_label_values(&[cache_type])
        .set(size_bytes as f64);
}

/// Record cache eviction
pub fn record_cache_eviction(cache_type: &str) {
    CACHE_EVICTIONS_TOTAL.with_label_values(&[cache_type]).inc();
}

/// Record index scan
pub fn record_index_scan(table: &str, index: &str) {
    INDEX_SCANS_TOTAL.with_label_values(&[table, index]).inc();
}

/// Record table scan
pub fn record_table_scan(table: &str) {
    TABLE_SCANS_TOTAL.with_label_values(&[table]).inc();
}

/// Record disk read
pub fn record_disk_read(bytes: usize) {
    DISK_READS_TOTAL.inc();
    DISK_READ_BYTES_TOTAL.inc_by(bytes as f64);
}

/// Record disk write
pub fn record_disk_write(bytes: usize) {
    DISK_WRITES_TOTAL.inc();
    DISK_WRITE_BYTES_TOTAL.inc_by(bytes as f64);
}

/// Update replication lag
pub fn update_replication_lag(replica: &str, lag_seconds: f64) {
    REPLICATION_LAG_SECONDS
        .with_label_values(&[replica])
        .set(lag_seconds);
}

/// Record replication bytes sent
pub fn record_replication_bytes_sent(replica: &str, bytes: usize) {
    REPLICATION_BYTES_SENT
        .with_label_values(&[replica])
        .inc_by(bytes as f64);
}

/// Update replication status
pub fn update_replication_status(replica: &str, is_healthy: bool) {
    let status = if is_healthy { 1.0 } else { 0.0 };
    REPLICATION_STATUS.with_label_values(&[replica]).set(status);
}

/// Record replica status (for tracking active replica counts)
pub fn record_replica_status(_status_type: &str, count: i64) {
    // Update the active replicas gauge
    // We use a simple gauge without labels for total active count
    POOL_SIZE.set(count as f64); // Temporarily reuse pool size gauge
                                 // TODO: Add dedicated ACTIVE_REPLICAS gauge
}

/// Record replication lag in KB (generic version without replica name)
pub fn record_replication_lag(lag_kb: f64) {
    // For now, this is a no-op as we track per-replica lag
    // Individual replica lag is tracked via update_replication_lag
    // This is called for aggregate lag tracking
}

/// Record replication bytes sent (aggregate version for all replicas)
pub fn record_replication_bytes_sent_total(bytes: f64) {
    // Aggregate replication bytes across all replicas
    REPLICATION_BYTES_SENT
        .with_label_values(&["total"])
        .inc_by(bytes);
}

/// Record rate limit hit
pub fn record_rate_limit_hit(limit_type: &str) {
    RATE_LIMIT_HITS_TOTAL.with_label_values(&[limit_type]).inc();
}

/// Record rate limit block
pub fn record_rate_limit_block(limit_type: &str) {
    RATE_LIMIT_BLOCKS_TOTAL
        .with_label_values(&[limit_type])
        .inc();
}

/// Record authentication attempt
pub fn record_auth_attempt(method: &str, result: &str) {
    AUTH_ATTEMPTS_TOTAL
        .with_label_values(&[method, result])
        .inc();
}

/// Record authentication failure
pub fn record_auth_failure(reason: &str) {
    AUTH_FAILURES_TOTAL.with_label_values(&[reason]).inc();
}

/// Record snapshot created
pub fn record_snapshot_created(table: &str) {
    SNAPSHOTS_CREATED_TOTAL.with_label_values(&[table]).inc();
}

/// Record compaction
pub fn record_compaction(table: &str, duration_seconds: f64) {
    COMPACTIONS_TOTAL.with_label_values(&[table]).inc();
    COMPACTION_DURATION
        .with_label_values(&[table])
        .observe(duration_seconds);
}

/// Record slow query
pub fn record_slow_query(query_type: &str) {
    SLOW_QUERIES_TOTAL.with_label_values(&[query_type]).inc();
}

/// Record query rows returned
pub fn record_query_rows_returned(query_type: &str, rows: usize) {
    QUERY_ROWS_RETURNED
        .with_label_values(&[query_type])
        .observe(rows as f64);
}

/// Record query rows affected
pub fn record_query_rows_affected(query_type: &str, rows: usize) {
    QUERY_ROWS_AFFECTED
        .with_label_values(&[query_type])
        .observe(rows as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use driftdb_core::{Engine, EnginePool, PoolConfig};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_metrics_initialization() {
        let result = init_metrics();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_record_query() {
        let _ = init_metrics();
        record_query("SELECT", "success", 0.1);

        let metric_families = REGISTRY.gather();
        assert!(!metric_families.is_empty());
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        use crate::protocol::auth::AuthConfig;
        use crate::security::rbac::RbacManager;
        use crate::security_audit::{AuditConfig, SecurityAuditLogger};
        use crate::slow_query_log::{SlowQueryConfig, SlowQueryLogger};
        use driftdb_core::RateLimitManager;

        let _ = init_metrics();
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine = Arc::new(RwLock::new(engine));

        // Create metrics and engine pool
        let pool_metrics = Arc::new(driftdb_core::observability::Metrics::new());
        let pool_config = PoolConfig::default();
        let engine_pool =
            EnginePool::new(engine.clone(), pool_config, pool_metrics.clone()).unwrap();

        // Create all SessionManager dependencies
        let auth_config = AuthConfig::default();
        let rate_limit_manager = Arc::new(RateLimitManager::new(Default::default(), pool_metrics));
        let slow_query_logger = Arc::new(SlowQueryLogger::new(SlowQueryConfig::default()));
        let audit_logger = Arc::new(SecurityAuditLogger::new(AuditConfig::default()));
        let rbac_manager = Arc::new(RbacManager::new());

        let session_manager = Arc::new(SessionManager::new(
            engine_pool,
            auth_config,
            rate_limit_manager,
            slow_query_logger,
            audit_logger,
            rbac_manager,
        ));
        let state = MetricsState::new(engine, session_manager);

        let result = metrics_handler(axum::extract::State(state)).await;
        assert!(result.is_ok());
    }
}
