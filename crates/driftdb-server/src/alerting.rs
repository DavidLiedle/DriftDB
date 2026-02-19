//! Alerting Rules for DriftDB
//!
//! Provides configurable alerting based on Prometheus metrics.
//! Monitors critical system health indicators and fires alerts when
//! thresholds are exceeded.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Informational - low priority
    Info,
    /// Warning - should be investigated
    Warning,
    /// Critical - requires immediate attention
    Critical,
    /// Fatal - system is in危险状态
    Fatal,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertSeverity::Info => write!(f, "INFO"),
            AlertSeverity::Warning => write!(f, "WARNING"),
            AlertSeverity::Critical => write!(f, "CRITICAL"),
            AlertSeverity::Fatal => write!(f, "FATAL"),
        }
    }
}

/// Alert state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertState {
    /// Alert is currently firing
    Firing,
    /// Alert condition resolved
    Resolved,
    /// Alert is pending (waiting for evaluation)
    Pending,
}

/// A single alert instance
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert name/identifier
    pub name: String,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Current state
    pub state: AlertState,
    /// Alert message/description
    pub message: String,
    /// Additional context/labels
    pub labels: HashMap<String, String>,
    /// When the alert first fired
    pub fired_at: Option<Instant>,
    /// When the alert was resolved
    pub resolved_at: Option<Instant>,
    /// Current metric value that triggered the alert
    pub current_value: f64,
    /// Threshold that was exceeded
    pub threshold: f64,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        name: String,
        severity: AlertSeverity,
        message: String,
        current_value: f64,
        threshold: f64,
    ) -> Self {
        Self {
            name,
            severity,
            state: AlertState::Pending,
            message,
            labels: HashMap::new(),
            fired_at: None,
            resolved_at: None,
            current_value,
            threshold,
        }
    }

    /// Add a label to the alert
    pub fn with_label(mut self, key: String, value: String) -> Self {
        self.labels.insert(key, value);
        self
    }

    /// Fire the alert
    pub fn fire(&mut self) {
        if self.state != AlertState::Firing {
            self.state = AlertState::Firing;
            self.fired_at = Some(Instant::now());
            self.resolved_at = None;

            match self.severity {
                AlertSeverity::Info => info!("🔔 ALERT [{}]: {}", self.name, self.message),
                AlertSeverity::Warning => warn!("⚠️  ALERT [{}]: {}", self.name, self.message),
                AlertSeverity::Critical => error!("🚨 ALERT [{}]: {}", self.name, self.message),
                AlertSeverity::Fatal => error!("💀 ALERT [{}]: {}", self.name, self.message),
            }
        }
    }

    /// Resolve the alert
    pub fn resolve(&mut self) {
        if self.state == AlertState::Firing {
            self.state = AlertState::Resolved;
            self.resolved_at = Some(Instant::now());
            info!("✅ RESOLVED [{}]: {}", self.name, self.message);
        }
    }

    /// Duration since alert fired
    pub fn duration(&self) -> Option<Duration> {
        self.fired_at.map(|fired| fired.elapsed())
    }
}

/// Configuration for a single alert rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Rule name
    pub name: String,
    /// Severity level
    pub severity: AlertSeverity,
    /// Threshold value
    pub threshold: f64,
    /// Comparison operator
    pub operator: ComparisonOperator,
    /// Duration threshold must be exceeded before firing
    pub for_duration: Duration,
    /// Alert message template
    pub message: String,
    /// Labels to attach to alerts
    pub labels: HashMap<String, String>,
}

/// Comparison operators for alert rules
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    /// Greater than
    GreaterThan,
    /// Greater than or equal
    GreaterThanOrEqual,
    /// Less than
    LessThan,
    /// Less than or equal
    LessThanOrEqual,
    /// Equal to
    Equal,
}

impl ComparisonOperator {
    /// Evaluate the comparison
    pub fn evaluate(&self, value: f64, threshold: f64) -> bool {
        match self {
            ComparisonOperator::GreaterThan => value > threshold,
            ComparisonOperator::GreaterThanOrEqual => value >= threshold,
            ComparisonOperator::LessThan => value < threshold,
            ComparisonOperator::LessThanOrEqual => value <= threshold,
            ComparisonOperator::Equal => (value - threshold).abs() < f64::EPSILON,
        }
    }
}

/// Alert manager configuration
#[derive(Debug, Clone)]
pub struct AlertManagerConfig {
    /// Enable/disable alerting
    pub enabled: bool,
    /// Evaluation interval
    #[allow(dead_code)]
    pub evaluation_interval: Duration,
    /// Alert resolution timeout (auto-resolve if not re-triggered)
    #[allow(dead_code)]
    pub resolution_timeout: Duration,
}

impl Default for AlertManagerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(30),
            resolution_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Manages alert rules and active alerts
pub struct AlertManager {
    /// Configuration
    config: AlertManagerConfig,
    /// Alert rules
    rules: Arc<RwLock<Vec<AlertRule>>>,
    /// Currently active alerts
    active_alerts: Arc<RwLock<HashMap<String, Alert>>>,
    /// Alert history (for metrics/debugging)
    alert_history: Arc<RwLock<Vec<Alert>>>,
}

impl AlertManager {
    /// Create a new alert manager with default rules
    pub fn new(config: AlertManagerConfig) -> Self {
        let mut manager = Self {
            config,
            rules: Arc::new(RwLock::new(Vec::new())),
            active_alerts: Arc::new(RwLock::new(HashMap::new())),
            alert_history: Arc::new(RwLock::new(Vec::new())),
        };

        // Register default alert rules
        manager.register_default_rules();

        manager
    }

    /// Register default alert rules
    fn register_default_rules(&mut self) {
        let mut rules = self.rules.write();

        // Error rate alerts
        rules.push(AlertRule {
            name: "HighErrorRate".to_string(),
            severity: AlertSeverity::Critical,
            threshold: 10.0, // 10 errors per second
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(60),
            message: "High error rate detected: {value} errors/sec (threshold: {threshold})"
                .to_string(),
            labels: [("type".to_string(), "error_rate".to_string())].into(),
        });

        // Replication lag alerts
        rules.push(AlertRule {
            name: "HighReplicationLag".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 10.0 * 1024.0 * 1024.0, // 10 MB
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(120),
            message: "Replication lag is high: {value} bytes (threshold: {threshold})".to_string(),
            labels: [("type".to_string(), "replication".to_string())].into(),
        });

        rules.push(AlertRule {
            name: "CriticalReplicationLag".to_string(),
            severity: AlertSeverity::Critical,
            threshold: 100.0 * 1024.0 * 1024.0, // 100 MB
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(60),
            message: "CRITICAL: Replication lag exceeds 100MB: {value} bytes".to_string(),
            labels: [("type".to_string(), "replication".to_string())].into(),
        });

        // Pool exhaustion alerts
        rules.push(AlertRule {
            name: "PoolNearExhaustion".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 90.0, // 90% utilization
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(120),
            message: "Connection pool utilization high: {value}% (threshold: {threshold}%)"
                .to_string(),
            labels: [("type".to_string(), "pool".to_string())].into(),
        });

        rules.push(AlertRule {
            name: "PoolExhausted".to_string(),
            severity: AlertSeverity::Critical,
            threshold: 100.0, // 100% utilization
            operator: ComparisonOperator::GreaterThanOrEqual,
            for_duration: Duration::from_secs(30),
            message: "CRITICAL: Connection pool exhausted!".to_string(),
            labels: [("type".to_string(), "pool".to_string())].into(),
        });

        // Disk space alerts
        rules.push(AlertRule {
            name: "LowDiskSpace".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 20.0, // 20% free
            operator: ComparisonOperator::LessThan,
            for_duration: Duration::from_secs(300),
            message: "Low disk space: {value}% free (threshold: {threshold}%)".to_string(),
            labels: [("type".to_string(), "disk".to_string())].into(),
        });

        rules.push(AlertRule {
            name: "CriticalDiskSpace".to_string(),
            severity: AlertSeverity::Critical,
            threshold: 10.0, // 10% free
            operator: ComparisonOperator::LessThan,
            for_duration: Duration::from_secs(60),
            message: "CRITICAL: Disk space critically low: {value}% free".to_string(),
            labels: [("type".to_string(), "disk".to_string())].into(),
        });

        // Memory usage alerts
        rules.push(AlertRule {
            name: "HighMemoryUsage".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 80.0, // 80% usage
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(300),
            message: "High memory usage: {value}% (threshold: {threshold}%)".to_string(),
            labels: [("type".to_string(), "memory".to_string())].into(),
        });

        rules.push(AlertRule {
            name: "CriticalMemoryUsage".to_string(),
            severity: AlertSeverity::Critical,
            threshold: 95.0, // 95% usage
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(60),
            message: "CRITICAL: Memory usage critical: {value}%".to_string(),
            labels: [("type".to_string(), "memory".to_string())].into(),
        });

        // Transaction alerts
        rules.push(AlertRule {
            name: "HighTransactionAbortRate".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 10.0, // 10% abort rate
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(120),
            message: "High transaction abort rate: {value}% (threshold: {threshold}%)".to_string(),
            labels: [("type".to_string(), "transaction".to_string())].into(),
        });

        // Slow query alerts
        rules.push(AlertRule {
            name: "HighSlowQueryRate".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 5.0, // 5 slow queries per minute
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(300),
            message: "High slow query rate: {value} queries/min (threshold: {threshold})"
                .to_string(),
            labels: [("type".to_string(), "query".to_string())].into(),
        });

        // CPU usage alerts
        rules.push(AlertRule {
            name: "HighCPUUsage".to_string(),
            severity: AlertSeverity::Warning,
            threshold: 80.0, // 80% CPU
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(300),
            message: "High CPU usage: {value}% (threshold: {threshold}%)".to_string(),
            labels: [("type".to_string(), "cpu".to_string())].into(),
        });

        rules.push(AlertRule {
            name: "CriticalCPUUsage".to_string(),
            severity: AlertSeverity::Critical,
            threshold: 95.0, // 95% CPU
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(60),
            message: "CRITICAL: CPU usage critical: {value}%".to_string(),
            labels: [("type".to_string(), "cpu".to_string())].into(),
        });

        info!("Registered {} default alert rules", rules.len());
    }

    /// Add a custom alert rule
    #[allow(dead_code)]
    pub fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write();
        info!("Adding alert rule: {}", rule.name);
        rules.push(rule);
    }

    /// Remove an alert rule by name
    pub fn remove_rule(&self, name: &str) -> bool {
        let mut rules = self.rules.write();
        let initial_len = rules.len();
        rules.retain(|r| r.name != name);
        rules.len() < initial_len
    }

    /// Evaluate all alert rules (should be called periodically)
    pub fn evaluate_rules(&self) {
        if !self.config.enabled {
            return;
        }

        let rules = self.rules.read();

        for rule in rules.iter() {
            self.evaluate_rule(rule);
        }

        // Check for auto-resolution
        self.check_auto_resolution();
    }

    /// Evaluate a single alert rule
    fn evaluate_rule(&self, rule: &AlertRule) {
        // Get current metric value based on rule name
        let current_value = match rule.name.as_str() {
            "HighErrorRate" => self.get_error_rate(),
            "HighReplicationLag" | "CriticalReplicationLag" => self.get_max_replication_lag(),
            "PoolNearExhaustion" | "PoolExhausted" => self.get_pool_utilization(),
            "LowDiskSpace" | "CriticalDiskSpace" => self.get_disk_space_free_percent(),
            "HighMemoryUsage" | "CriticalMemoryUsage" => self.get_memory_usage_percent(),
            "HighTransactionAbortRate" => self.get_transaction_abort_rate(),
            "HighSlowQueryRate" => self.get_slow_query_rate(),
            "HighCPUUsage" | "CriticalCPUUsage" => self.get_cpu_usage_percent(),
            _ => {
                debug!("Unknown alert rule: {}", rule.name);
                return;
            }
        };

        // Evaluate threshold
        if rule.operator.evaluate(current_value, rule.threshold) {
            self.fire_alert(rule, current_value);
        } else {
            self.resolve_alert(&rule.name);
        }
    }

    /// Fire an alert
    fn fire_alert(&self, rule: &AlertRule, current_value: f64) {
        let mut active_alerts = self.active_alerts.write();

        let alert = active_alerts.entry(rule.name.clone()).or_insert_with(|| {
            let message = rule
                .message
                .replace("{value}", &format!("{:.2}", current_value))
                .replace("{threshold}", &format!("{:.2}", rule.threshold));

            let mut alert = Alert::new(
                rule.name.clone(),
                rule.severity,
                message,
                current_value,
                rule.threshold,
            );

            for (k, v) in &rule.labels {
                alert = alert.with_label(k.clone(), v.clone());
            }

            alert
        });

        // Update current value
        alert.current_value = current_value;

        // Fire if not already firing and duration exceeded
        if alert.state != AlertState::Firing {
            if let Some(fired_at) = alert.fired_at {
                if fired_at.elapsed() >= rule.for_duration {
                    alert.fire();

                    // Add to history
                    let mut history = self.alert_history.write();
                    history.push(alert.clone());
                }
            } else {
                // First time threshold exceeded - start timer
                alert.fired_at = Some(Instant::now());
                alert.state = AlertState::Pending;
            }
        }
    }

    /// Resolve an alert
    fn resolve_alert(&self, name: &str) {
        let mut active_alerts = self.active_alerts.write();

        if let Some(alert) = active_alerts.get_mut(name) {
            alert.resolve();
        }
    }

    /// Check for alerts that should auto-resolve
    fn check_auto_resolution(&self) {
        let mut active_alerts = self.active_alerts.write();

        active_alerts.retain(|_, alert| {
            if alert.state == AlertState::Resolved {
                if let Some(resolved_at) = alert.resolved_at {
                    // Keep for a bit after resolution for history
                    return resolved_at.elapsed() < Duration::from_secs(60);
                }
            }
            true
        });
    }

    /// Get all currently active alerts
    pub fn get_active_alerts(&self) -> Vec<Alert> {
        self.active_alerts
            .read()
            .values()
            .filter(|a| a.state == AlertState::Firing)
            .cloned()
            .collect()
    }

    /// Get alert history
    pub fn get_alert_history(&self, limit: usize) -> Vec<Alert> {
        let history = self.alert_history.read();
        history.iter().rev().take(limit).cloned().collect()
    }

    // Metric getter helpers that query actual Prometheus metrics
    // Note: For labeled metrics (CounterVec, GaugeVec), we use with_label_values()
    // to access specific label combinations directly

    /// Get current error rate as a ratio of failed to total queries
    fn get_error_rate(&self) -> f64 {
        use crate::metrics::{ERROR_TOTAL, QUERY_TOTAL};

        // Count errors for common error types and operations
        let error_types = ["syntax", "permission", "not_found", "timeout", "internal"];
        let operations = ["query", "insert", "update", "delete", "connect"];

        let mut error_count = 0.0_f64;
        for error_type in &error_types {
            for operation in &operations {
                error_count += ERROR_TOTAL
                    .with_label_values(&[error_type, operation])
                    .get();
            }
        }

        // Count total queries for common types and statuses
        let query_types = ["SELECT", "INSERT", "UPDATE", "DELETE", "PATCH"];
        let statuses = ["success", "error"];

        let mut total_count = 0.0_f64;
        for query_type in &query_types {
            for status in &statuses {
                total_count += QUERY_TOTAL.with_label_values(&[query_type, status]).get();
            }
        }

        if total_count > 0.0 {
            error_count / total_count
        } else {
            0.0
        }
    }

    /// Get maximum replication lag across all replicas
    fn get_max_replication_lag(&self) -> f64 {
        use crate::metrics::REPLICATION_LAG_SECONDS;

        // Check a set of common replica names for lag
        // In production, this list would be dynamically populated
        let replicas = ["replica_1", "replica_2", "replica_3", "standby"];

        let mut max_lag = 0.0_f64;
        for replica in &replicas {
            let lag = REPLICATION_LAG_SECONDS.with_label_values(&[replica]).get();
            if lag > max_lag {
                max_lag = lag;
            }
        }
        max_lag
    }

    /// Get pool utilization as percentage (active / total)
    fn get_pool_utilization(&self) -> f64 {
        use crate::metrics::{POOL_ACTIVE, POOL_SIZE};

        let pool_size = POOL_SIZE.get();
        let pool_active = POOL_ACTIVE.get();

        if pool_size > 0.0 {
            (pool_active / pool_size) * 100.0
        } else {
            0.0
        }
    }

    /// Get free disk space percentage using fs2 crate
    fn get_disk_space_free_percent(&self) -> f64 {
        use std::path::Path;

        // Try to get disk space for current directory
        let path = Path::new(".");

        match fs2::available_space(path) {
            Ok(available) => match fs2::total_space(path) {
                Ok(total) if total > 0 => (available as f64 / total as f64) * 100.0,
                _ => 100.0, // Unknown, assume OK
            },
            Err(_) => 100.0, // Unknown, assume OK
        }
    }

    /// Get memory usage percentage using sysinfo
    fn get_memory_usage_percent(&self) -> f64 {
        use crate::metrics::MEMORY_USAGE_BYTES;
        use sysinfo::System;

        // First try to get process memory from our metric
        let process_memory = MEMORY_USAGE_BYTES
            .with_label_values(&["process_physical"])
            .get();

        // Get system total memory using sysinfo
        let mut sys = System::new();
        sys.refresh_memory();

        let total_memory = sys.total_memory() as f64 * 1024.0; // Convert KB to bytes
        if total_memory > 0.0 && process_memory > 0.0 {
            (process_memory / total_memory) * 100.0
        } else {
            // Fallback: use system used memory / total memory
            let used_memory = sys.used_memory() as f64 * 1024.0;
            if total_memory > 0.0 {
                (used_memory / total_memory) * 100.0
            } else {
                0.0
            }
        }
    }

    /// Get transaction abort rate as ratio of aborted to total transactions
    fn get_transaction_abort_rate(&self) -> f64 {
        use crate::metrics::TRANSACTION_TOTAL;

        // Transaction types and statuses
        let txn_types = ["read-only", "read-write"];
        let abort_statuses = ["aborted", "rolled-back"];
        let all_statuses = ["committed", "aborted", "rolled-back"];

        let mut aborted = 0.0_f64;
        let mut total = 0.0_f64;

        for txn_type in &txn_types {
            for status in &all_statuses {
                let value = TRANSACTION_TOTAL
                    .with_label_values(&[txn_type, status])
                    .get();
                total += value;
                if abort_statuses.contains(status) {
                    aborted += value;
                }
            }
        }

        if total > 0.0 {
            (aborted / total) * 100.0 // Return as percentage
        } else {
            0.0
        }
    }

    /// Get slow query rate as percentage of total queries
    fn get_slow_query_rate(&self) -> f64 {
        use crate::metrics::{QUERY_TOTAL, SLOW_QUERIES_TOTAL};

        // Count slow queries by type
        let query_types = ["SELECT", "INSERT", "UPDATE", "DELETE", "PATCH"];

        let mut slow_count = 0.0_f64;
        for query_type in &query_types {
            slow_count += SLOW_QUERIES_TOTAL.with_label_values(&[query_type]).get();
        }

        // Count total queries
        let statuses = ["success", "error"];
        let mut total_count = 0.0_f64;
        for query_type in &query_types {
            for status in &statuses {
                total_count += QUERY_TOTAL.with_label_values(&[query_type, status]).get();
            }
        }

        if total_count > 0.0 {
            (slow_count / total_count) * 100.0
        } else {
            0.0
        }
    }

    /// Get CPU usage percentage using sysinfo
    fn get_cpu_usage_percent(&self) -> f64 {
        use crate::metrics::CPU_USAGE_PERCENT;

        // First try to get from our metric (updated by update_system_metrics)
        let process_cpu = CPU_USAGE_PERCENT.with_label_values(&["process"]).get();

        if process_cpu > 0.0 {
            process_cpu
        } else {
            // Fallback: calculate directly using sysinfo
            use sysinfo::{Pid, System};
            let mut sys = System::new();
            sys.refresh_all();

            let pid = Pid::from(std::process::id() as usize);
            if let Some(process) = sys.process(pid) {
                process.cpu_usage() as f64
            } else {
                0.0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_creation() {
        let alert = Alert::new(
            "TestAlert".to_string(),
            AlertSeverity::Warning,
            "Test message".to_string(),
            100.0,
            50.0,
        );

        assert_eq!(alert.name, "TestAlert");
        assert_eq!(alert.severity, AlertSeverity::Warning);
        assert_eq!(alert.state, AlertState::Pending);
        assert_eq!(alert.current_value, 100.0);
        assert_eq!(alert.threshold, 50.0);
    }

    #[test]
    fn test_alert_fire_and_resolve() {
        let mut alert = Alert::new(
            "TestAlert".to_string(),
            AlertSeverity::Warning,
            "Test message".to_string(),
            100.0,
            50.0,
        );

        assert_eq!(alert.state, AlertState::Pending);

        alert.fire();
        assert_eq!(alert.state, AlertState::Firing);
        assert!(alert.fired_at.is_some());

        alert.resolve();
        assert_eq!(alert.state, AlertState::Resolved);
        assert!(alert.resolved_at.is_some());
    }

    #[test]
    fn test_comparison_operators() {
        assert!(ComparisonOperator::GreaterThan.evaluate(10.0, 5.0));
        assert!(!ComparisonOperator::GreaterThan.evaluate(5.0, 10.0));

        assert!(ComparisonOperator::LessThan.evaluate(5.0, 10.0));
        assert!(!ComparisonOperator::LessThan.evaluate(10.0, 5.0));

        assert!(ComparisonOperator::GreaterThanOrEqual.evaluate(10.0, 10.0));
        assert!(ComparisonOperator::LessThanOrEqual.evaluate(10.0, 10.0));
    }

    #[test]
    fn test_alert_manager_initialization() {
        let manager = AlertManager::new(AlertManagerConfig::default());
        let rules = manager.rules.read();

        // Should have default rules
        assert!(!rules.is_empty());

        // Check for specific rules
        assert!(rules.iter().any(|r| r.name == "HighErrorRate"));
        assert!(rules.iter().any(|r| r.name == "PoolExhausted"));
        assert!(rules.iter().any(|r| r.name == "CriticalDiskSpace"));
    }

    #[test]
    fn test_add_remove_rules() {
        let manager = AlertManager::new(AlertManagerConfig::default());

        let rule = AlertRule {
            name: "CustomRule".to_string(),
            severity: AlertSeverity::Info,
            threshold: 100.0,
            operator: ComparisonOperator::GreaterThan,
            for_duration: Duration::from_secs(60),
            message: "Custom alert".to_string(),
            labels: HashMap::new(),
        };

        manager.add_rule(rule);

        {
            let rules = manager.rules.read();
            assert!(rules.iter().any(|r| r.name == "CustomRule"));
        }

        assert!(manager.remove_rule("CustomRule"));

        {
            let rules = manager.rules.read();
            assert!(!rules.iter().any(|r| r.name == "CustomRule"));
        }
    }

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Info < AlertSeverity::Warning);
        assert!(AlertSeverity::Warning < AlertSeverity::Critical);
        assert!(AlertSeverity::Critical < AlertSeverity::Fatal);
    }
}
