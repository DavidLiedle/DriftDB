//! Observability module for metrics, tracing, and monitoring

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, trace, warn};

/// Global metrics collector
pub struct Metrics {
    // Write metrics
    pub writes_total: AtomicU64,
    pub writes_failed: AtomicU64,
    pub write_bytes: AtomicU64,
    pub write_latency_us: AtomicU64,

    // Read metrics
    pub reads_total: AtomicU64,
    pub reads_failed: AtomicU64,
    pub read_bytes: AtomicU64,
    pub read_latency_us: AtomicU64,

    // Query metrics
    pub queries_total: AtomicU64,
    pub queries_failed: AtomicU64,
    pub query_latency_us: AtomicU64,

    // Storage metrics
    pub segments_created: AtomicU64,
    pub segments_rotated: AtomicU64,
    pub snapshots_created: AtomicU64,
    pub compactions_performed: AtomicU64,

    // WAL metrics
    pub wal_writes: AtomicU64,
    pub wal_syncs: AtomicU64,
    pub wal_rotations: AtomicU64,
    pub wal_replay_events: AtomicU64,

    // Resource metrics
    pub active_connections: AtomicUsize,
    pub memory_usage_bytes: AtomicU64,
    pub disk_usage_bytes: AtomicU64,

    // Error metrics
    pub corruption_detected: AtomicU64,
    pub panic_recovered: AtomicU64,

    // Rate limiting metrics
    pub rate_limit_violations: AtomicU64,
    pub connection_rate_limit_hits: AtomicU64,
    pub query_rate_limit_hits: AtomicU64,
    pub global_rate_limit_hits: AtomicU64,

    // Cache metrics
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,

    // Transaction metrics
    pub active_transactions: AtomicU64,
    pub deadlocks_detected: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            writes_total: AtomicU64::new(0),
            writes_failed: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            write_latency_us: AtomicU64::new(0),

            reads_total: AtomicU64::new(0),
            reads_failed: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            read_latency_us: AtomicU64::new(0),

            queries_total: AtomicU64::new(0),
            queries_failed: AtomicU64::new(0),
            query_latency_us: AtomicU64::new(0),

            segments_created: AtomicU64::new(0),
            segments_rotated: AtomicU64::new(0),
            snapshots_created: AtomicU64::new(0),
            compactions_performed: AtomicU64::new(0),

            wal_writes: AtomicU64::new(0),
            wal_syncs: AtomicU64::new(0),
            wal_rotations: AtomicU64::new(0),
            wal_replay_events: AtomicU64::new(0),

            active_connections: AtomicUsize::new(0),
            memory_usage_bytes: AtomicU64::new(0),
            disk_usage_bytes: AtomicU64::new(0),

            corruption_detected: AtomicU64::new(0),
            panic_recovered: AtomicU64::new(0),

            rate_limit_violations: AtomicU64::new(0),
            connection_rate_limit_hits: AtomicU64::new(0),
            query_rate_limit_hits: AtomicU64::new(0),
            global_rate_limit_hits: AtomicU64::new(0),

            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),

            active_transactions: AtomicU64::new(0),
            deadlocks_detected: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            writes_total: self.writes_total.load(Ordering::Relaxed),
            writes_failed: self.writes_failed.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),

            reads_total: self.reads_total.load(Ordering::Relaxed),
            reads_failed: self.reads_failed.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            read_latency_us: self.read_latency_us.load(Ordering::Relaxed),

            queries_total: self.queries_total.load(Ordering::Relaxed),
            queries_failed: self.queries_failed.load(Ordering::Relaxed),
            query_latency_us: self.query_latency_us.load(Ordering::Relaxed),

            segments_created: self.segments_created.load(Ordering::Relaxed),
            segments_rotated: self.segments_rotated.load(Ordering::Relaxed),
            snapshots_created: self.snapshots_created.load(Ordering::Relaxed),
            compactions_performed: self.compactions_performed.load(Ordering::Relaxed),

            wal_writes: self.wal_writes.load(Ordering::Relaxed),
            wal_syncs: self.wal_syncs.load(Ordering::Relaxed),
            wal_rotations: self.wal_rotations.load(Ordering::Relaxed),
            wal_replay_events: self.wal_replay_events.load(Ordering::Relaxed),

            active_connections: self.active_connections.load(Ordering::Relaxed),
            memory_usage_bytes: self.memory_usage_bytes.load(Ordering::Relaxed),
            disk_usage_bytes: self.disk_usage_bytes.load(Ordering::Relaxed),

            corruption_detected: self.corruption_detected.load(Ordering::Relaxed),
            panic_recovered: self.panic_recovered.load(Ordering::Relaxed),

            rate_limit_violations: self.rate_limit_violations.load(Ordering::Relaxed),
            connection_rate_limit_hits: self.connection_rate_limit_hits.load(Ordering::Relaxed),
            query_rate_limit_hits: self.query_rate_limit_hits.load(Ordering::Relaxed),
            global_rate_limit_hits: self.global_rate_limit_hits.load(Ordering::Relaxed),
        }
    }

    pub fn record_write(&self, bytes: u64, duration: Duration, success: bool) {
        self.writes_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.writes_failed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        self.write_latency_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn record_read(&self, bytes: u64, duration: Duration, success: bool) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.reads_failed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        self.read_latency_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }
}

/// Serializable snapshot of metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub writes_total: u64,
    pub writes_failed: u64,
    pub write_bytes: u64,
    pub write_latency_us: u64,

    pub reads_total: u64,
    pub reads_failed: u64,
    pub read_bytes: u64,
    pub read_latency_us: u64,

    pub queries_total: u64,
    pub queries_failed: u64,
    pub query_latency_us: u64,

    pub segments_created: u64,
    pub segments_rotated: u64,
    pub snapshots_created: u64,
    pub compactions_performed: u64,

    pub wal_writes: u64,
    pub wal_syncs: u64,
    pub wal_rotations: u64,
    pub wal_replay_events: u64,

    pub active_connections: usize,
    pub memory_usage_bytes: u64,
    pub disk_usage_bytes: u64,

    pub corruption_detected: u64,
    pub panic_recovered: u64,

    pub rate_limit_violations: u64,
    pub connection_rate_limit_hits: u64,
    pub query_rate_limit_hits: u64,
    pub global_rate_limit_hits: u64,
}

/// Timer for measuring operation latency
pub struct LatencyTimer {
    start: Instant,
    operation: String,
}

impl LatencyTimer {
    pub fn start(operation: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            operation: operation.into(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if elapsed > Duration::from_millis(100) {
            warn!(
                operation = %self.operation,
                latency_ms = elapsed.as_millis(),
                "Slow operation detected"
            );
        } else {
            trace!(
                operation = %self.operation,
                latency_us = elapsed.as_micros(),
                "Operation completed"
            );
        }
    }
}

/// Instrumented wrapper for critical operations
pub struct InstrumentedOperation<'a> {
    name: &'a str,
    _metrics: Arc<Metrics>,
    timer: LatencyTimer,
}

impl<'a> InstrumentedOperation<'a> {
    pub fn new(name: &'a str, metrics: Arc<Metrics>) -> Self {
        debug!("Starting operation: {}", name);
        Self {
            name,
            _metrics: metrics,
            timer: LatencyTimer::start(name),
        }
    }

    pub fn complete(self, success: bool) {
        let duration = self.timer.elapsed();
        if success {
            debug!(
                operation = self.name,
                duration_us = duration.as_micros(),
                "Operation completed successfully"
            );
        } else {
            error!(
                operation = self.name,
                duration_us = duration.as_micros(),
                "Operation failed"
            );
        }
    }
}

/// Health check status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: HealthState,
    pub version: String,
    pub uptime_seconds: u64,
    pub checks: Vec<HealthCheck>,
    pub metrics: MetricsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: HealthState,
    pub message: Option<String>,
    pub latency_ms: u64,
}

/// Perform health checks
#[instrument(skip(metrics))]
pub fn perform_health_check(metrics: Arc<Metrics>, data_dir: &std::path::Path) -> HealthStatus {
    let start_time = Instant::now();
    let mut checks = Vec::new();

    // Check disk space
    let disk_check_start = Instant::now();
    let disk_status = check_disk_space(data_dir);
    checks.push(HealthCheck {
        name: "disk_space".to_string(),
        status: if disk_status.0 {
            HealthState::Healthy
        } else {
            HealthState::Unhealthy
        },
        message: disk_status.1,
        latency_ms: disk_check_start.elapsed().as_millis() as u64,
    });

    // Check memory usage
    let mem_check_start = Instant::now();
    let memory_status = check_memory_usage();
    checks.push(HealthCheck {
        name: "memory".to_string(),
        status: if memory_status.0 {
            HealthState::Healthy
        } else {
            HealthState::Degraded
        },
        message: memory_status.1,
        latency_ms: mem_check_start.elapsed().as_millis() as u64,
    });

    // Check WAL health
    let wal_check_start = Instant::now();
    let wal_status = check_wal_health(data_dir);
    checks.push(HealthCheck {
        name: "wal".to_string(),
        status: if wal_status.0 {
            HealthState::Healthy
        } else {
            HealthState::Unhealthy
        },
        message: wal_status.1,
        latency_ms: wal_check_start.elapsed().as_millis() as u64,
    });

    // Determine overall health
    let overall_status = if checks.iter().all(|c| c.status == HealthState::Healthy) {
        HealthState::Healthy
    } else if checks.iter().any(|c| c.status == HealthState::Unhealthy) {
        HealthState::Unhealthy
    } else {
        HealthState::Degraded
    };

    HealthStatus {
        status: overall_status,
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: start_time.elapsed().as_secs(),
        checks,
        metrics: metrics.snapshot(),
    }
}

fn check_disk_space(data_dir: &std::path::Path) -> (bool, Option<String>) {
    match fs2::available_space(data_dir) {
        Ok(bytes) => {
            let gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            if gb < 1.0 {
                (
                    false,
                    Some(format!("Low disk space: {:.2} GB available", gb)),
                )
            } else if gb < 5.0 {
                (
                    true,
                    Some(format!("Disk space warning: {:.2} GB available", gb)),
                )
            } else {
                (true, None)
            }
        }
        Err(e) => (false, Some(format!("Failed to check disk space: {}", e))),
    }
}

fn check_memory_usage() -> (bool, Option<String>) {
    // Simplified memory check - in production would use system APIs
    // For now, always return healthy
    (true, None)
}

fn check_wal_health(data_dir: &std::path::Path) -> (bool, Option<String>) {
    let wal_path = data_dir.join("wal").join("wal.log");
    if wal_path.exists() {
        match std::fs::metadata(&wal_path) {
            Ok(metadata) => {
                let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
                if size_mb > 1000.0 {
                    (false, Some(format!("WAL too large: {:.2} MB", size_mb)))
                } else {
                    (true, None)
                }
            }
            Err(e) => (false, Some(format!("Failed to check WAL: {}", e))),
        }
    } else {
        (true, Some("WAL not initialized".to_string()))
    }
}

/// Initialize tracing with appropriate filters
pub fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,driftdb=debug"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    info!("DriftDB tracing initialized");
}
