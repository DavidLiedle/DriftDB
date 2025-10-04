//! HTTP routes for advanced connection pool analytics and management

use std::sync::Arc;

use axum::{extract::State, response::Json, routing::get, Router};
use serde_json::{json, Value};
use tracing::info;

use crate::advanced_pool::AdvancedPoolManager;

/// State for advanced pool monitoring endpoints
#[derive(Clone)]
pub struct AdvancedPoolState {
    pub pool_manager: Option<Arc<AdvancedPoolManager>>,
}

impl AdvancedPoolState {
    pub fn new(pool_manager: Option<Arc<AdvancedPoolManager>>) -> Self {
        Self { pool_manager }
    }
}

/// Create advanced pool monitoring routes
pub fn create_advanced_pool_routes(state: AdvancedPoolState) -> Router {
    Router::new()
        .route("/pool/analytics", get(get_pool_analytics))
        .route("/pool/affinity", get(get_connection_affinity))
        .route("/pool/health", get(get_pool_health))
        .route("/pool/loadbalancing", get(get_load_balancing_stats))
        .route("/pool/optimization", get(get_pool_optimization))
        .route("/pool/predictions", get(get_health_predictions))
        .route("/pool/resources", get(get_resource_usage))
        .with_state(state)
}

/// Get comprehensive pool analytics
async fn get_pool_analytics(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Advanced pool analytics requested");

    if let Some(pool_manager) = &state.pool_manager {
        let analytics = pool_manager.get_comprehensive_analytics().await;
        Json(analytics)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view detailed analytics"
        }))
    }
}

/// Get connection affinity statistics
async fn get_connection_affinity(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Connection affinity stats requested");

    if let Some(pool_manager) = &state.pool_manager {
        let affinity_stats = pool_manager.get_affinity_analytics().await;
        Json(affinity_stats)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view connection affinity stats"
        }))
    }
}

/// Get detailed pool health metrics
async fn get_pool_health(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Pool health metrics requested");

    if let Some(pool_manager) = &state.pool_manager {
        let health_report = pool_manager.get_health_report().await;
        Json(health_report)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view health metrics"
        }))
    }
}

/// Get load balancing statistics
async fn get_load_balancing_stats(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Load balancing stats requested");

    if let Some(pool_manager) = &state.pool_manager {
        let lb_stats = pool_manager.get_load_balancer_stats().await;
        Json(lb_stats)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view load balancing stats"
        }))
    }
}

/// Get pool optimization recommendations
async fn get_pool_optimization(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Pool optimization recommendations requested");

    if let Some(pool_manager) = &state.pool_manager {
        let optimization_report = pool_manager.get_optimization_recommendations().await;
        Json(optimization_report)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view optimization recommendations"
        }))
    }
}

/// Get health prediction analytics
async fn get_health_predictions(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Health predictions requested");

    if let Some(pool_manager) = &state.pool_manager {
        let predictions = pool_manager.get_health_predictions().await;
        Json(predictions)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view health predictions"
        }))
    }
}

/// Get resource usage analytics
async fn get_resource_usage(
    State(state): State<AdvancedPoolState>,
) -> Json<Value> {
    info!("Resource usage analytics requested");

    if let Some(pool_manager) = &state.pool_manager {
        let resource_report = pool_manager.get_resource_analytics().await;
        Json(resource_report)
    } else {
        Json(json!({
            "error": "Advanced pool management is disabled",
            "message": "Enable advanced pool features to view resource usage"
        }))
    }
}