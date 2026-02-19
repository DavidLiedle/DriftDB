use crate::engine::Engine;
use crate::errors::Result;
use crate::observability::Metrics;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
// use tokio::sync::mpsc;

/// Rate tracker for calculating per-second metrics
struct RateTracker {
    last_value: u64,
    last_timestamp: SystemTime,
    current_rate: f64,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            last_value: 0,
            last_timestamp: SystemTime::now(),
            current_rate: 0.0,
        }
    }

    fn update(&mut self, current_value: u64) -> f64 {
        let now = SystemTime::now();
        let elapsed = now
            .duration_since(self.last_timestamp)
            .unwrap_or(Duration::from_secs(1));
        let elapsed_secs = elapsed.as_secs_f64();

        if elapsed_secs > 0.0 && current_value >= self.last_value {
            self.current_rate = (current_value - self.last_value) as f64 / elapsed_secs;
        }

        self.last_value = current_value;
        self.last_timestamp = now;
        self.current_rate
    }
}

/// Percentile tracker using a simple histogram approach
struct PercentileTracker {
    values: VecDeque<u64>,
    max_samples: usize,
}

impl PercentileTracker {
    fn new(max_samples: usize) -> Self {
        Self {
            values: VecDeque::with_capacity(max_samples),
            max_samples,
        }
    }

    fn add(&mut self, value: u64) {
        if self.values.len() >= self.max_samples {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    fn percentile(&self, p: f64) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }

        let mut sorted: Vec<u64> = self.values.iter().copied().collect();
        sorted.sort_unstable();

        let index = ((p / 100.0) * (sorted.len() as f64)) as usize;
        let index = index.min(sorted.len() - 1);
        sorted[index] as f64 / 1000.0 // Convert microseconds to milliseconds
    }
}

/// Comprehensive monitoring system for DriftDB
pub struct MonitoringSystem {
    metrics: Arc<Metrics>,
    config: MonitoringConfig,
    collectors: Arc<RwLock<Vec<Box<dyn MetricCollector>>>>,
    exporters: Arc<RwLock<Vec<Box<dyn MetricExporter>>>>,
    alert_manager: Arc<AlertManager>,
    history: Arc<RwLock<MetricsHistory>>,
    #[allow(dead_code)]
    dashboard: Arc<RwLock<Dashboard>>,
    engine: Option<Arc<RwLock<Engine>>>,
    // Rate trackers
    query_rate_tracker: Arc<RwLock<RateTracker>>,
    request_rate_tracker: Arc<RwLock<RateTracker>>,
    // Percentile trackers
    query_latency_tracker: Arc<RwLock<PercentileTracker>>,
    slow_query_threshold_ms: f64,
    slow_query_count: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub collection_interval: Duration,
    pub history_retention: Duration,
    pub alert_evaluation_interval: Duration,
    pub export_interval: Duration,
    pub enable_profiling: bool,
    pub enable_tracing: bool,
    pub prometheus_enabled: bool,
    pub grafana_enabled: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(10),
            history_retention: Duration::from_secs(24 * 60 * 60),
            alert_evaluation_interval: Duration::from_secs(30),
            export_interval: Duration::from_secs(60),
            enable_profiling: true,
            enable_tracing: true,
            prometheus_enabled: true,
            grafana_enabled: false,
        }
    }
}

/// Trait for custom metric collectors
pub trait MetricCollector: Send + Sync {
    fn name(&self) -> &str;
    fn collect(&self) -> MetricCollection;
}

/// Trait for metric exporters (Prometheus, Grafana, etc.)
pub trait MetricExporter: Send + Sync {
    fn name(&self) -> &str;
    fn export(&mut self, metrics: &MetricSnapshot) -> Result<()>;
}

/// Collection of metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricCollection {
    pub timestamp: SystemTime,
    pub gauges: HashMap<String, f64>,
    pub counters: HashMap<String, u64>,
    pub histograms: HashMap<String, Histogram>,
    pub summaries: HashMap<String, Summary>,
}

/// Histogram for distribution metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub buckets: Vec<(f64, u64)>, // (upper_bound, count)
    pub sum: f64,
    pub count: u64,
}

/// Summary for percentile metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub percentiles: Vec<(f64, f64)>, // (percentile, value)
    pub sum: f64,
    pub count: u64,
}

/// Snapshot of all metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSnapshot {
    pub timestamp: SystemTime,
    pub system: SystemMetrics,
    pub database: DatabaseMetrics,
    pub query: QueryMetrics,
    pub storage: StorageMetrics,
    pub network: NetworkMetrics,
    pub custom: HashMap<String, MetricCollection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f64,
    pub memory_usage_bytes: u64,
    pub memory_usage_percent: f64,
    pub disk_usage_bytes: u64,
    pub disk_free_bytes: u64,
    pub uptime_seconds: u64,
    pub process_threads: usize,
    pub open_files: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseMetrics {
    pub tables_count: usize,
    pub total_rows: u64,
    pub total_size_bytes: u64,
    pub active_transactions: usize,
    pub deadlocks_detected: u64,
    pub cache_hit_ratio: f64,
    pub buffer_pool_usage_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryMetrics {
    pub queries_per_second: f64,
    pub avg_query_time_ms: f64,
    pub p50_query_time_ms: f64,
    pub p95_query_time_ms: f64,
    pub p99_query_time_ms: f64,
    pub slow_queries_count: u64,
    pub failed_queries_count: u64,
    pub query_queue_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageMetrics {
    pub segments_count: usize,
    pub segment_avg_size_bytes: u64,
    pub compaction_pending: usize,
    pub wal_size_bytes: u64,
    pub wal_lag_bytes: u64,
    pub snapshots_count: usize,
    pub index_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NetworkMetrics {
    pub active_connections: usize,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub requests_per_second: f64,
    pub avg_response_time_ms: f64,
    pub connection_errors: u64,
}

/// Historical metrics storage
pub struct MetricsHistory {
    snapshots: VecDeque<MetricSnapshot>,
    max_retention: Duration,
    resolution_buckets: Vec<ResolutionBucket>,
}

#[derive(Debug)]
struct ResolutionBucket {
    duration: Duration,
    interval: Duration,
    data: VecDeque<MetricSnapshot>,
}

/// Alert management system
pub struct AlertManager {
    rules: Arc<RwLock<Vec<AlertRule>>>,
    active_alerts: Arc<RwLock<Vec<Alert>>>,
    notification_channels: Arc<RwLock<Vec<Box<dyn NotificationChannel>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub name: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub cooldown: Duration,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertCondition {
    ThresholdExceeded {
        metric: String,
        threshold: f64,
    },
    ThresholdBelow {
        metric: String,
        threshold: f64,
    },
    RateOfChange {
        metric: String,
        threshold_percent: f64,
        window: Duration,
    },
    Anomaly {
        metric: String,
        deviation_factor: f64,
    },
    Custom(String), // Custom expression
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AlertSeverity {
    Critical,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub rule_name: String,
    pub severity: AlertSeverity,
    pub triggered_at: SystemTime,
    pub message: String,
    pub labels: HashMap<String, String>,
    pub value: f64,
}

/// Notification channel for alerts
pub trait NotificationChannel: Send + Sync {
    fn name(&self) -> &str;
    fn notify(&self, alert: &Alert) -> Result<()>;
}

/// Dashboard for real-time monitoring
pub struct Dashboard {
    #[allow(dead_code)]
    widgets: Vec<Widget>,
    #[allow(dead_code)]
    layout: DashboardLayout,
    #[allow(dead_code)]
    refresh_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Widget {
    pub id: String,
    pub title: String,
    pub widget_type: WidgetType,
    pub data_source: String,
    pub refresh_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WidgetType {
    LineChart {
        metrics: Vec<String>,
        time_range: Duration,
    },
    GaugeChart {
        metric: String,
        min: f64,
        max: f64,
    },
    BarChart {
        metrics: Vec<String>,
    },
    HeatMap {
        metric: String,
        buckets: usize,
    },
    Table {
        columns: Vec<String>,
    },
    Counter {
        metric: String,
    },
    Text {
        content: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardLayout {
    pub rows: usize,
    pub columns: usize,
    pub widget_positions: HashMap<String, (usize, usize, usize, usize)>, // (row, col, width, height)
}

impl MonitoringSystem {
    pub fn new(metrics: Arc<Metrics>, config: MonitoringConfig) -> Self {
        let alert_manager = Arc::new(AlertManager::new());

        Self {
            metrics,
            config,
            collectors: Arc::new(RwLock::new(Vec::new())),
            exporters: Arc::new(RwLock::new(Vec::new())),
            alert_manager,
            history: Arc::new(RwLock::new(MetricsHistory::new(Duration::from_secs(
                24 * 60 * 60,
            )))),
            dashboard: Arc::new(RwLock::new(Dashboard::default())),
            engine: None,
            query_rate_tracker: Arc::new(RwLock::new(RateTracker::new())),
            request_rate_tracker: Arc::new(RwLock::new(RateTracker::new())),
            query_latency_tracker: Arc::new(RwLock::new(PercentileTracker::new(10000))), // Track last 10k queries
            slow_query_threshold_ms: 1000.0, // 1 second default threshold
            slow_query_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Set the database engine for collecting real metrics
    pub fn with_engine(mut self, engine: Arc<RwLock<Engine>>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Record a query latency and detect slow queries
    pub fn record_query_latency(&self, latency_us: u64) {
        // Add to percentile tracker
        self.query_latency_tracker.write().add(latency_us);

        // Check if it's a slow query
        let latency_ms = latency_us as f64 / 1000.0;
        if latency_ms > self.slow_query_threshold_ms {
            *self.slow_query_count.write() += 1;
        }
    }

    /// Set the slow query threshold in milliseconds
    pub fn set_slow_query_threshold(&mut self, threshold_ms: f64) {
        self.slow_query_threshold_ms = threshold_ms;
    }

    /// Start the monitoring system
    pub async fn start(self: Arc<Self>) {
        let config = self.config.clone();
        let collectors = self.collectors.clone();
        let exporters = self.exporters.clone();
        let history = self.history.clone();
        let alert_manager = self.alert_manager.clone();
        let metrics = self.metrics.clone();

        // Start collection task
        let monitoring_self = self.clone();
        let history_clone = history.clone();
        let collection_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.collection_interval);
            loop {
                interval.tick().await;

                // Collect metrics
                let snapshot = monitoring_self.collect_metrics(&metrics, &collectors).await;

                // Store in history
                history_clone.write().add_snapshot(snapshot.clone());

                // Export metrics
                if config.prometheus_enabled {
                    Self::export_metrics(&exporters, &snapshot).await;
                }
            }
        });

        // Start alert evaluation task
        let alert_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.alert_evaluation_interval);
            loop {
                interval.tick().await;
                alert_manager.evaluate_rules(&history).await;
            }
        });

        // Keep handles alive
        tokio::select! {
            _ = collection_handle => {},
            _ = alert_handle => {},
        }
    }

    async fn collect_metrics(
        &self,
        metrics: &Arc<Metrics>,
        collectors: &Arc<RwLock<Vec<Box<dyn MetricCollector>>>>,
    ) -> MetricSnapshot {
        let system = Self::collect_system_metrics();
        let database = self.collect_database_metrics(metrics);
        let query = self.collect_query_metrics(metrics);
        let storage = self.collect_storage_metrics(metrics);
        let network = self.collect_network_metrics(metrics);

        let mut custom = HashMap::new();
        for collector in collectors.read().iter() {
            custom.insert(collector.name().to_string(), collector.collect());
        }

        MetricSnapshot {
            timestamp: SystemTime::now(),
            system,
            database,
            query,
            storage,
            network,
            custom,
        }
    }

    fn collect_system_metrics() -> SystemMetrics {
        SystemMetrics {
            cpu_usage_percent: Self::get_cpu_usage(),
            memory_usage_bytes: Self::get_memory_usage(),
            memory_usage_percent: Self::get_memory_usage_percent(),
            disk_usage_bytes: Self::get_disk_usage(),
            disk_free_bytes: Self::get_disk_free(),
            uptime_seconds: Self::get_uptime(),
            process_threads: Self::get_thread_count(),
            open_files: Self::get_open_files(),
        }
    }

    fn collect_database_metrics(&self, metrics: &Arc<Metrics>) -> DatabaseMetrics {
        let (tables_count, total_rows) = if let Some(ref engine_arc) = self.engine {
            let engine = engine_arc.read();
            let table_names = engine.list_tables();
            let mut total_rows = 0;

            // Count total rows across all tables
            for table_name in &table_names {
                if let Ok(stats) = engine.get_table_stats(table_name) {
                    total_rows += stats.row_count;
                }
            }

            (table_names.len(), total_rows)
        } else {
            (0, 0)
        };

        let cache_hits = metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = metrics.cache_misses.load(Ordering::Relaxed);
        let total_cache_requests = cache_hits + cache_misses;
        let cache_hit_ratio = if total_cache_requests > 0 {
            cache_hits as f64 / total_cache_requests as f64
        } else {
            0.0
        };

        DatabaseMetrics {
            tables_count,
            total_rows: total_rows as u64,
            total_size_bytes: metrics.disk_usage_bytes.load(Ordering::Relaxed),
            active_transactions: metrics.active_transactions.load(Ordering::Relaxed) as usize,
            deadlocks_detected: metrics.deadlocks_detected.load(Ordering::Relaxed),
            cache_hit_ratio,
            buffer_pool_usage_percent: self.calculate_buffer_pool_usage(),
        }
    }

    fn calculate_buffer_pool_usage(&self) -> f64 {
        // Simple buffer pool usage calculation
        // In a real implementation, this would track actual buffer pool memory
        let used_memory = Self::get_memory_usage();
        let total_memory = used_memory + 1024 * 1024 * 1024; // Assume 1GB total for example
        (used_memory as f64 / total_memory as f64) * 100.0
    }

    fn collect_query_metrics(&self, metrics: &Arc<Metrics>) -> QueryMetrics {
        let total_queries = metrics.queries_total.load(Ordering::Relaxed);
        let failed_queries = metrics.queries_failed.load(Ordering::Relaxed);
        let total_latency = metrics.query_latency_us.load(Ordering::Relaxed);

        let avg_latency = if total_queries > 0 {
            (total_latency / total_queries) as f64 / 1000.0
        } else {
            0.0
        };

        // Calculate queries per second
        let queries_per_second = self.query_rate_tracker.write().update(total_queries);

        // Get percentiles from tracker
        let latency_tracker = self.query_latency_tracker.read();
        let p50 = latency_tracker.percentile(50.0);
        let p95 = latency_tracker.percentile(95.0);
        let p99 = latency_tracker.percentile(99.0);
        drop(latency_tracker);

        // Get slow query count
        let slow_queries_count = *self.slow_query_count.read();

        QueryMetrics {
            queries_per_second,
            avg_query_time_ms: avg_latency,
            p50_query_time_ms: p50,
            p95_query_time_ms: p95,
            p99_query_time_ms: p99,
            slow_queries_count,
            failed_queries_count: failed_queries,
            query_queue_length: 0, // Queue tracking would require integration with query executor
        }
    }

    fn collect_storage_metrics(&self, metrics: &Arc<Metrics>) -> StorageMetrics {
        // Calculate WAL size from filesystem if engine is available
        let wal_size_bytes = if let Some(engine) = &self.engine {
            if let Some(guard) = engine.try_read() {
                self.calculate_wal_size_from_engine(&guard)
            } else {
                0
            }
        } else {
            0
        };

        StorageMetrics {
            segments_count: metrics.segments_created.load(Ordering::Relaxed) as usize,
            segment_avg_size_bytes: 0, // Would need segment size tracking in storage layer
            compaction_pending: 0,     // Would need integration with compaction scheduler
            wal_size_bytes,
            wal_lag_bytes: 0, // Would need replication lag tracking
            snapshots_count: metrics.snapshots_created.load(Ordering::Relaxed) as usize,
            index_size_bytes: 0, // Would need index size tracking
        }
    }

    fn calculate_wal_size_from_engine(&self, _engine: &Engine) -> u64 {
        // In a real implementation, this would traverse the data directory
        // and sum up WAL file sizes. For now, return a placeholder.
        // This would require Engine to expose its data_dir path.
        0
    }

    fn collect_network_metrics(&self, metrics: &Arc<Metrics>) -> NetworkMetrics {
        // Calculate requests per second from total queries + writes + reads
        let total_requests = metrics.queries_total.load(Ordering::Relaxed)
            + metrics.writes_total.load(Ordering::Relaxed)
            + metrics.reads_total.load(Ordering::Relaxed);
        let requests_per_second = self.request_rate_tracker.write().update(total_requests);

        NetworkMetrics {
            active_connections: metrics.active_connections.load(Ordering::Relaxed),
            bytes_received: metrics.read_bytes.load(Ordering::Relaxed),
            bytes_sent: metrics.write_bytes.load(Ordering::Relaxed),
            requests_per_second,
            avg_response_time_ms: 0.0, // Would need response time tracking
            connection_errors: 0,      // Would need error tracking
        }
    }

    async fn export_metrics(
        exporters: &Arc<RwLock<Vec<Box<dyn MetricExporter>>>>,
        snapshot: &MetricSnapshot,
    ) {
        for exporter in exporters.write().iter_mut() {
            if let Err(e) = exporter.export(snapshot) {
                tracing::error!("Failed to export metrics to {}: {}", exporter.name(), e);
            }
        }
    }

    // System metric helpers
    fn get_cpu_usage() -> f64 {
        // Simplified - would use sysinfo crate in production
        0.0
    }

    fn get_memory_usage() -> u64 {
        // Simplified - would use sysinfo crate in production
        0
    }

    fn get_memory_usage_percent() -> f64 {
        0.0
    }

    fn get_disk_usage() -> u64 {
        0
    }

    fn get_disk_free() -> u64 {
        0
    }

    fn get_uptime() -> u64 {
        0
    }

    fn get_thread_count() -> usize {
        0
    }

    fn get_open_files() -> usize {
        0
    }

    /// Register a custom metric collector
    pub fn register_collector(&self, collector: Box<dyn MetricCollector>) {
        self.collectors.write().push(collector);
    }

    /// Register a metric exporter
    pub fn register_exporter(&self, exporter: Box<dyn MetricExporter>) {
        self.exporters.write().push(exporter);
    }

    /// Add an alert rule
    pub fn add_alert_rule(&self, rule: AlertRule) {
        self.alert_manager.add_rule(rule);
    }

    /// Get current metrics snapshot
    pub fn current_snapshot(&self) -> Option<MetricSnapshot> {
        self.history.read().latest()
    }

    /// Get metrics history
    pub fn get_history(&self, duration: Duration) -> Vec<MetricSnapshot> {
        self.history.read().get_range(duration)
    }

    /// Get active alerts
    pub fn active_alerts(&self) -> Vec<Alert> {
        self.alert_manager.active_alerts()
    }
}

impl MetricsHistory {
    fn new(retention: Duration) -> Self {
        Self {
            snapshots: VecDeque::new(),
            max_retention: retention,
            resolution_buckets: vec![
                ResolutionBucket {
                    duration: Duration::from_secs(3600), // 1 hour
                    interval: Duration::from_secs(10),   // 10 second resolution
                    data: VecDeque::new(),
                },
                ResolutionBucket {
                    duration: Duration::from_secs(86400), // 24 hours
                    interval: Duration::from_secs(60),    // 1 minute resolution
                    data: VecDeque::new(),
                },
                ResolutionBucket {
                    duration: Duration::from_secs(604800), // 7 days
                    interval: Duration::from_secs(300),    // 5 minute resolution
                    data: VecDeque::new(),
                },
            ],
        }
    }

    fn add_snapshot(&mut self, snapshot: MetricSnapshot) {
        self.snapshots.push_back(snapshot.clone());

        // Trim old data
        let cutoff = SystemTime::now() - self.max_retention;
        while let Some(front) = self.snapshots.front() {
            if front.timestamp < cutoff {
                self.snapshots.pop_front();
            } else {
                break;
            }
        }

        // Update resolution buckets
        for bucket in &mut self.resolution_buckets {
            bucket.add_snapshot(snapshot.clone());
        }
    }

    fn latest(&self) -> Option<MetricSnapshot> {
        self.snapshots.back().cloned()
    }

    fn get_range(&self, duration: Duration) -> Vec<MetricSnapshot> {
        let cutoff = SystemTime::now() - duration;
        self.snapshots
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .cloned()
            .collect()
    }
}

impl ResolutionBucket {
    fn add_snapshot(&mut self, snapshot: MetricSnapshot) {
        // Add if enough time has passed since last entry
        if self.data.is_empty()
            || snapshot
                .timestamp
                .duration_since(self.data.back().unwrap().timestamp)
                .unwrap_or_default()
                >= self.interval
        {
            self.data.push_back(snapshot);
        }

        // Trim old data
        let cutoff = SystemTime::now() - self.duration;
        while let Some(front) = self.data.front() {
            if front.timestamp < cutoff {
                self.data.pop_front();
            } else {
                break;
            }
        }
    }
}

impl AlertManager {
    fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
            active_alerts: Arc::new(RwLock::new(Vec::new())),
            notification_channels: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn add_rule(&self, rule: AlertRule) {
        self.rules.write().push(rule);
    }

    async fn evaluate_rules(&self, history: &Arc<RwLock<MetricsHistory>>) {
        let snapshot = match history.read().latest() {
            Some(s) => s,
            None => return,
        };

        let mut new_alerts = Vec::new();

        let rules = self.rules.read().clone();
        for rule in &rules {
            if let Some(alert) = self.evaluate_rule(rule, &snapshot) {
                new_alerts.push(alert.clone());

                // Send notifications
                let channels = self.notification_channels.read();
                for channel in channels.iter() {
                    if let Err(e) = channel.notify(&alert) {
                        tracing::error!("Failed to send alert notification: {}", e);
                    }
                }
            }
        }

        *self.active_alerts.write() = new_alerts;
    }

    fn evaluate_rule(&self, rule: &AlertRule, _snapshot: &MetricSnapshot) -> Option<Alert> {
        // Simplified evaluation - would be more complex in production
        match &rule.condition {
            AlertCondition::ThresholdExceeded {
                metric: _metric,
                threshold: _threshold,
            } => {
                // Check if metric exceeds threshold
                // This is simplified - would need to extract actual metric value
                None
            }
            _ => None,
        }
    }

    fn active_alerts(&self) -> Vec<Alert> {
        self.active_alerts.read().clone()
    }
}

impl Default for Dashboard {
    fn default() -> Self {
        Self {
            widgets: Vec::new(),
            layout: DashboardLayout {
                rows: 4,
                columns: 4,
                widget_positions: HashMap::new(),
            },
            refresh_interval: Duration::from_secs(5),
        }
    }
}

/// Prometheus exporter implementation
pub struct PrometheusExporter {
    #[allow(dead_code)]
    endpoint: String,
    #[allow(dead_code)]
    port: u16,
}

impl PrometheusExporter {
    pub fn new(port: u16) -> Self {
        Self {
            endpoint: format!("0.0.0.0:{}", port),
            port,
        }
    }

    fn format_metrics(&self, snapshot: &MetricSnapshot) -> String {
        let mut output = String::new();

        // System metrics
        output.push_str("# TYPE cpu_usage_percent gauge\n");
        output.push_str(&format!(
            "cpu_usage_percent {}\n",
            snapshot.system.cpu_usage_percent
        ));

        output.push_str("# TYPE memory_usage_bytes gauge\n");
        output.push_str(&format!(
            "memory_usage_bytes {}\n",
            snapshot.system.memory_usage_bytes
        ));

        // Database metrics
        output.push_str("# TYPE database_tables_count gauge\n");
        output.push_str(&format!(
            "database_tables_count {}\n",
            snapshot.database.tables_count
        ));

        // Query metrics
        output.push_str("# TYPE query_avg_time_ms histogram\n");
        output.push_str(&format!(
            "query_avg_time_ms {}\n",
            snapshot.query.avg_query_time_ms
        ));

        output
    }
}

impl MetricExporter for PrometheusExporter {
    fn name(&self) -> &str {
        "prometheus"
    }

    fn export(&mut self, metrics: &MetricSnapshot) -> Result<()> {
        // In production, would serve metrics via HTTP endpoint
        let _formatted = self.format_metrics(metrics);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_history() {
        let mut history = MetricsHistory::new(Duration::from_secs(60 * 60));

        let snapshot = MetricSnapshot {
            timestamp: SystemTime::now(),
            system: SystemMetrics {
                cpu_usage_percent: 50.0,
                memory_usage_bytes: 1000000,
                memory_usage_percent: 25.0,
                disk_usage_bytes: 5000000,
                disk_free_bytes: 10000000,
                uptime_seconds: 3600,
                process_threads: 10,
                open_files: 100,
            },
            database: Default::default(),
            query: Default::default(),
            storage: Default::default(),
            network: Default::default(),
            custom: HashMap::new(),
        };

        history.add_snapshot(snapshot);
        assert!(history.latest().is_some());
    }

    #[test]
    fn test_alert_rules() {
        let alert_rule = AlertRule {
            name: "High CPU Usage".to_string(),
            condition: AlertCondition::ThresholdExceeded {
                metric: "cpu_usage_percent".to_string(),
                threshold: 80.0,
            },
            severity: AlertSeverity::Warning,
            cooldown: Duration::from_secs(300),
            labels: HashMap::new(),
            annotations: HashMap::new(),
        };

        assert_eq!(alert_rule.name, "High CPU Usage");
    }

    #[test]
    fn test_rate_tracker() {
        let mut tracker = RateTracker::new();

        // Initial update with value 0
        let rate = tracker.update(0);
        assert_eq!(rate, 0.0);

        // Simulate some time passing and value increasing
        std::thread::sleep(std::time::Duration::from_millis(100));
        let rate = tracker.update(100);

        // Rate should be approximately 100 / 0.1 = 1000 per second
        // Allow some tolerance for timing variations
        assert!(
            rate > 500.0 && rate < 2000.0,
            "Rate {} is outside expected range",
            rate
        );

        // Another update
        std::thread::sleep(std::time::Duration::from_millis(100));
        let rate = tracker.update(200);
        assert!(rate > 500.0 && rate < 2000.0);
    }

    #[test]
    fn test_percentile_tracker() {
        let mut tracker = PercentileTracker::new(100);

        // Add some latency samples (in microseconds)
        tracker.add(10000); // 10ms
        tracker.add(20000); // 20ms
        tracker.add(30000); // 30ms
        tracker.add(40000); // 40ms
        tracker.add(50000); // 50ms
        tracker.add(60000); // 60ms
        tracker.add(70000); // 70ms
        tracker.add(80000); // 80ms
        tracker.add(90000); // 90ms
        tracker.add(100000); // 100ms

        // Check percentiles (returned in milliseconds)
        let p50 = tracker.percentile(50.0);
        assert!(
            p50 >= 40.0 && p50 <= 65.0,
            "p50 {} is outside expected range",
            p50
        );

        let p95 = tracker.percentile(95.0);
        assert!(
            p95 >= 85.0 && p95 <= 100.0,
            "p95 {} is outside expected range",
            p95
        );

        let p99 = tracker.percentile(99.0);
        assert!(
            p99 >= 90.0 && p99 <= 100.0,
            "p99 {} is outside expected range",
            p99
        );
    }

    #[test]
    fn test_percentile_tracker_max_samples() {
        let mut tracker = PercentileTracker::new(5);

        // Add more samples than max (in microseconds)
        for i in 1..=10 {
            tracker.add(i * 10000); // 10ms, 20ms, ..., 100ms
        }

        // Should only keep last 5 samples (60ms, 70ms, 80ms, 90ms, 100ms)
        let p50 = tracker.percentile(50.0);
        assert!(
            p50 >= 70.0 && p50 <= 85.0,
            "p50 {} is outside expected range",
            p50
        );
    }

    #[tokio::test]
    async fn test_monitoring_system_query_metrics() {
        let metrics = Arc::new(Metrics::new());
        let config = MonitoringConfig::default();
        let system = Arc::new(MonitoringSystem::new(metrics.clone(), config));

        // Record some query latencies (in microseconds)
        system.record_query_latency(10000); // 10ms
        system.record_query_latency(20000); // 20ms
        system.record_query_latency(30000); // 30ms

        // Collect metrics
        let collectors = Arc::new(RwLock::new(Vec::new()));
        let snapshot = system.collect_metrics(&metrics, &collectors).await;

        // Check that percentiles are calculated
        assert!(snapshot.query.p50_query_time_ms >= 0.0);
        assert!(snapshot.query.p95_query_time_ms >= 0.0);
        assert!(snapshot.query.p99_query_time_ms >= 0.0);
    }

    #[test]
    fn test_monitoring_system_slow_queries() {
        let metrics = Arc::new(Metrics::new());
        let config = MonitoringConfig::default();
        let mut system = MonitoringSystem::new(metrics.clone(), config);

        // Set slow query threshold
        system.set_slow_query_threshold(25.0);

        // Record some queries (in microseconds)
        system.record_query_latency(10000); // 10ms - Fast
        system.record_query_latency(30000); // 30ms - Slow
        system.record_query_latency(40000); // 40ms - Slow

        // Check slow query count
        let slow_count = *system.slow_query_count.read();
        assert_eq!(slow_count, 2);
    }
}
