//! Production monitoring and observability features

#![allow(dead_code)]

use std::sync::Arc;
use std::collections::VecDeque;
use parking_lot::RwLock;
use axum::{extract::State, response::Json, routing::get, Router};
use serde_json::{json, Value};
use tracing::info;

use crate::errors::{DriftDbError, ErrorSeverity};

/// Error monitoring system that tracks recent errors
pub struct ErrorMonitor {
    recent_errors: Arc<RwLock<VecDeque<ErrorRecord>>>,
    max_errors: usize,
}

#[derive(Debug, Clone)]
pub struct ErrorRecord {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub error: String,
    pub severity: ErrorSeverity,
    pub structured_data: Value,
}

impl ErrorMonitor {
    pub fn new() -> Self {
        Self {
            recent_errors: Arc::new(RwLock::new(VecDeque::new())),
            max_errors: 1000, // Keep last 1000 errors
        }
    }

    /// Record an error for monitoring
    pub fn record_error(&self, error: &DriftDbError) {
        let record = ErrorRecord {
            timestamp: chrono::Utc::now(),
            error: error.to_string(),
            severity: error.severity(),
            structured_data: error.to_structured_json(),
        };

        let mut errors = self.recent_errors.write();
        errors.push_back(record);

        // Keep only the most recent errors
        while errors.len() > self.max_errors {
            errors.pop_front();
        }

        info!(
            "Recorded error: {} (severity: {})",
            error,
            error.severity()
        );
    }

    /// Get recent errors for monitoring dashboard
    pub fn get_recent_errors(&self, limit: Option<usize>) -> Vec<ErrorRecord> {
        let errors = self.recent_errors.read();
        let limit = limit.unwrap_or(100).min(errors.len());

        errors.iter()
            .rev() // Most recent first
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get error statistics
    pub fn get_error_stats(&self) -> ErrorStats {
        let errors = self.recent_errors.read();
        let mut stats = ErrorStats::default();

        for error in errors.iter() {
            stats.total_count += 1;
            match error.severity {
                ErrorSeverity::Low => stats.low_severity += 1,
                ErrorSeverity::Medium => stats.medium_severity += 1,
                ErrorSeverity::High => stats.high_severity += 1,
                ErrorSeverity::Critical => stats.critical_severity += 1,
            }
        }

        stats
    }
}

#[derive(Debug, Clone, Default)]
pub struct ErrorStats {
    pub total_count: usize,
    pub low_severity: usize,
    pub medium_severity: usize,
    pub high_severity: usize,
    pub critical_severity: usize,
}

/// Production monitoring routes
pub fn monitoring_routes(error_monitor: Arc<ErrorMonitor>) -> Router {
    Router::new()
        .route("/errors", get(get_recent_errors))
        .route("/errors/stats", get(get_error_stats))
        .route("/health/errors", get(get_error_health))
        .with_state(error_monitor)
}

/// Get recent errors endpoint
async fn get_recent_errors(
    State(monitor): State<Arc<ErrorMonitor>>,
) -> Json<Value> {
    let errors = monitor.get_recent_errors(Some(50));

    Json(json!({
        "recent_errors": errors.iter().map(|e| json!({
            "timestamp": e.timestamp.to_rfc3339(),
            "error": e.error,
            "severity": e.severity.to_string(),
            "details": e.structured_data
        })).collect::<Vec<_>>(),
        "count": errors.len()
    }))
}

/// Get error statistics endpoint
async fn get_error_stats(
    State(monitor): State<Arc<ErrorMonitor>>,
) -> Json<Value> {
    let stats = monitor.get_error_stats();

    Json(json!({
        "error_statistics": {
            "total_errors": stats.total_count,
            "by_severity": {
                "critical": stats.critical_severity,
                "high": stats.high_severity,
                "medium": stats.medium_severity,
                "low": stats.low_severity
            },
            "health_status": if stats.critical_severity > 0 {
                "critical"
            } else if stats.high_severity > 10 {
                "warning"
            } else {
                "healthy"
            }
        }
    }))
}

/// Health check based on error patterns
async fn get_error_health(
    State(monitor): State<Arc<ErrorMonitor>>,
) -> Json<Value> {
    let stats = monitor.get_error_stats();
    let recent_errors = monitor.get_recent_errors(Some(10));

    // Determine health status
    let (status, message) = if stats.critical_severity > 0 {
        ("critical", "Critical errors detected")
    } else if stats.high_severity > 20 {
        ("warning", "High number of severe errors")
    } else if stats.total_count > 100 {
        ("warning", "High error rate")
    } else {
        ("healthy", "Error levels normal")
    };

    Json(json!({
        "status": status,
        "message": message,
        "error_summary": {
            "total_errors": stats.total_count,
            "critical_count": stats.critical_severity,
            "high_count": stats.high_severity,
            "recent_errors": recent_errors.len()
        },
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}