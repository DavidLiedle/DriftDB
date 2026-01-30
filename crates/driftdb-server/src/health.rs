//! Health Check Module
//!
//! Provides HTTP endpoints for monitoring DriftDB server health and readiness

use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, http::StatusCode, response::Json, routing::get, Router};
use parking_lot::RwLock;
use serde_json::{json, Value};
use tracing::{debug, error, info};

use crate::session::SessionManager;
use driftdb_core::Engine;

/// Application state for health check endpoints
#[derive(Clone)]
pub struct HealthState {
    pub engine: Arc<RwLock<Engine>>,
    pub session_manager: Arc<SessionManager>,
    pub start_time: Instant,
}

impl HealthState {
    pub fn new(engine: Arc<RwLock<Engine>>, session_manager: Arc<SessionManager>) -> Self {
        Self {
            engine,
            session_manager,
            start_time: Instant::now(),
        }
    }
}

/// Create the health check router
pub fn create_health_router(state: HealthState) -> Router {
    Router::new()
        .route("/health/live", get(liveness_check))
        .route("/health/ready", get(readiness_check))
        .with_state(state)
}

/// Liveness probe - checks if the server process is running
/// Returns 200 if the server is alive and responding to requests
async fn liveness_check(State(state): State<HealthState>) -> Result<Json<Value>, StatusCode> {
    debug!("Liveness check requested");

    let uptime_seconds = state.start_time.elapsed().as_secs();
    let response = json!({
        "status": "alive",
        "uptime_seconds": uptime_seconds,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    Ok(Json(response))
}

/// Readiness probe - checks if the server is ready to accept requests
/// Returns 200 if the database is ready and can execute queries
async fn readiness_check(State(state): State<HealthState>) -> Result<Json<Value>, StatusCode> {
    debug!("Readiness check requested");

    // Check if engine is accessible
    let engine_status = match state.engine.try_read() {
        Some(_engine) => {
            // Try to execute a simple health check query
            match perform_engine_health_check(&state.engine) {
                Ok(_) => "ready",
                Err(e) => {
                    error!("Engine health check failed: {}", e);
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
            }
        }
        None => {
            error!("Engine is locked, not ready");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Check disk space (basic check)
    let disk_status = match check_disk_space().await {
        Ok(available_gb) => {
            if available_gb < 1.0 {
                error!("Low disk space: {:.2} GB available", available_gb);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            "ok"
        }
        Err(e) => {
            error!("Failed to check disk space: {}", e);
            "unknown"
        }
    };

    // Get rate limiting statistics
    let rate_limit_stats = state.session_manager.rate_limit_manager().stats();

    let response = json!({
        "status": "ready",
        "engine": engine_status,
        "disk": disk_status,
        "rate_limiting": {
            "active_clients": rate_limit_stats.active_clients,
            "total_violations": rate_limit_stats.total_violations,
            "global_tokens_available": rate_limit_stats.global_tokens_available,
            "load_factor": rate_limit_stats.load_factor,
        },
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    Ok(Json(response))
}

/// Perform a basic health check on the engine
fn perform_engine_health_check(engine: &Arc<RwLock<Engine>>) -> anyhow::Result<()> {
    // Try to acquire a read lock and perform a basic operation
    let engine_guard = engine.read();

    // Check if we can list tables (basic engine functionality)
    let _tables = engine_guard.list_tables();

    info!("Engine health check passed");
    Ok(())
}

/// Check available disk space
async fn check_disk_space() -> anyhow::Result<f64> {
    use std::path::Path;

    // Get disk usage for current directory
    let path = Path::new(".");

    // Use system command to get actual disk space
    #[cfg(unix)]
    {
        use std::process::Command;

        let output = Command::new("df")
            .arg("-k") // Use 1K blocks
            .arg(".")
            .output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let lines: Vec<&str> = stdout.lines().collect();

            if lines.len() >= 2 {
                // Parse the second line which contains the data
                let parts: Vec<&str> = lines[1].split_whitespace().collect();
                if parts.len() >= 4 {
                    // Available space is typically in the 4th column (in KB)
                    if let Ok(available_kb) = parts[3].parse::<u64>() {
                        let available_gb = available_kb as f64 / (1024.0 * 1024.0);
                        return Ok(available_gb);
                    }
                }
            }
        }
    }

    #[cfg(windows)]
    {
        use std::process::Command;

        let output = Command::new("powershell")
            .arg("-Command")
            .arg("(Get-PSDrive -Name (Get-Location).Drive.Name).Free / 1GB")
            .output()?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Ok(available_gb) = stdout.trim().parse::<f64>() {
                return Ok(available_gb);
            }
        }
    }

    // Fallback: try to estimate based on metadata
    let metadata = tokio::fs::metadata(path).await?;

    // If we can't get real disk space, at least check if we can write
    // Return a conservative estimate
    if metadata.permissions().readonly() {
        Ok(0.0) // No write access
    } else {
        Ok(1.0) // Assume at least 1GB if we have write access
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use driftdb_core::Engine;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_liveness_check() {
        use crate::protocol::auth::AuthConfig;
        use crate::security::rbac::RbacManager;
        use crate::security_audit::{AuditConfig, SecurityAuditLogger};
        use crate::slow_query_log::{SlowQueryConfig, SlowQueryLogger};
        use driftdb_core::{
            connection::PoolConfig, observability::Metrics, EnginePool, RateLimitManager,
        };

        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine = Arc::new(RwLock::new(engine));

        // Create all required dependencies
        let metrics = Arc::new(Metrics::new());
        let engine_pool =
            EnginePool::new(engine.clone(), PoolConfig::default(), metrics.clone()).unwrap();
        let auth_config = AuthConfig::default();
        let rate_limit_manager = Arc::new(RateLimitManager::new(Default::default(), metrics));
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
        let state = HealthState::new(engine, session_manager);

        let result = liveness_check(State(state)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_readiness_check() {
        use crate::protocol::auth::AuthConfig;
        use crate::security::rbac::RbacManager;
        use crate::security_audit::{AuditConfig, SecurityAuditLogger};
        use crate::slow_query_log::{SlowQueryConfig, SlowQueryLogger};
        use driftdb_core::{
            connection::PoolConfig, observability::Metrics, EnginePool, RateLimitManager,
        };

        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine = Arc::new(RwLock::new(engine));

        // Create all required dependencies
        let metrics = Arc::new(Metrics::new());
        let engine_pool =
            EnginePool::new(engine.clone(), PoolConfig::default(), metrics.clone()).unwrap();
        let auth_config = AuthConfig::default();
        let rate_limit_manager = Arc::new(RateLimitManager::new(Default::default(), metrics));
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
        let state = HealthState::new(engine, session_manager);

        let result = readiness_check(State(state)).await;
        assert!(result.is_ok());
    }
}
