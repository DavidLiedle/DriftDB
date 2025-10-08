//! Metrics Collection Module
//!
//! Provides Prometheus-compatible metrics for DriftDB server monitoring

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::Response, routing::get, Router};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
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

    debug!("Metrics initialized successfully");
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
        match collect_database_size_metrics(&*engine) {
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
                .with_label_values(&[&table_name, "total"])
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
        let _ = init_metrics();
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine = Arc::new(RwLock::new(engine));

        // Create metrics and engine pool
        let pool_metrics = Arc::new(driftdb_core::observability::Metrics::new());
        let pool_config = PoolConfig::default();
        let engine_pool = EnginePool::new(engine.clone(), pool_config, pool_metrics).unwrap();
        let session_manager = Arc::new(SessionManager::new(engine_pool));
        let state = MetricsState::new(engine, session_manager);

        let result = metrics_handler(axum::extract::State(state)).await;
        assert!(result.is_ok());
    }
}
