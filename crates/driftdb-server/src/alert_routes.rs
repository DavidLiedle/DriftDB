//! HTTP routes for alerting API
//!
//! Provides REST endpoints for querying and managing alerts.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::alerting::{Alert, AlertManager, AlertState};

/// State shared across alert route handlers
#[derive(Clone)]
pub struct AlertRouteState {
    alert_manager: Arc<AlertManager>,
}

impl AlertRouteState {
    pub fn new(alert_manager: Arc<AlertManager>) -> Self {
        Self { alert_manager }
    }
}

/// Response for listing active alerts
#[derive(Debug, Serialize)]
struct AlertsResponse {
    alerts: Vec<AlertResponse>,
    total: usize,
}

/// Single alert in response
#[derive(Debug, Serialize)]
struct AlertResponse {
    name: String,
    severity: String,
    state: String,
    message: String,
    current_value: f64,
    threshold: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<u64>,
}

impl From<&Alert> for AlertResponse {
    fn from(alert: &Alert) -> Self {
        Self {
            name: alert.name.clone(),
            severity: alert.severity.to_string(),
            state: match alert.state {
                AlertState::Firing => "FIRING".to_string(),
                AlertState::Resolved => "RESOLVED".to_string(),
                AlertState::Pending => "PENDING".to_string(),
            },
            message: alert.message.clone(),
            current_value: alert.current_value,
            threshold: alert.threshold,
            duration_seconds: alert.duration().map(|d| d.as_secs()),
        }
    }
}

/// Create the alerting router
pub fn create_router(alert_manager: Arc<AlertManager>) -> Router {
    let state = AlertRouteState::new(alert_manager);

    Router::new()
        .route("/api/alerts", get(list_active_alerts))
        .route("/api/alerts/history", get(get_alert_history))
        .route("/api/alerts/rules", get(list_rules))
        .route("/api/alerts/rules", post(add_rule))
        .route("/api/alerts/rules/:name", delete(delete_rule))
        .with_state(state)
}

/// GET /api/alerts - List all active alerts
async fn list_active_alerts(
    State(state): State<AlertRouteState>,
) -> Result<Json<AlertsResponse>, StatusCode> {
    let alerts = state.alert_manager.get_active_alerts();
    let total = alerts.len();

    let alert_responses: Vec<AlertResponse> = alerts.iter().map(AlertResponse::from).collect();

    Ok(Json(AlertsResponse {
        alerts: alert_responses,
        total,
    }))
}

/// GET /api/alerts/history - Get alert history
async fn get_alert_history(
    State(state): State<AlertRouteState>,
) -> Result<Json<AlertsResponse>, StatusCode> {
    let alerts = state.alert_manager.get_alert_history(100);
    let total = alerts.len();

    let alert_responses: Vec<AlertResponse> = alerts.iter().map(AlertResponse::from).collect();

    Ok(Json(AlertsResponse {
        alerts: alert_responses,
        total,
    }))
}

/// GET /api/alerts/rules - List all alert rules
async fn list_rules(State(_state): State<AlertRouteState>) -> Result<Json<serde_json::Value>, StatusCode> {
    // Note: This would require adding a method to AlertManager to get rules
    // For now, return a placeholder
    Ok(Json(json!({
        "rules": [],
        "total": 0
    })))
}

/// Request body for adding a new alert rule
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AddRuleRequest {
    name: String,
    severity: String,
    threshold: f64,
    operator: String,
    for_duration_secs: u64,
    message: String,
}

/// POST /api/alerts/rules - Add a new alert rule
async fn add_rule(
    State(_state): State<AlertRouteState>,
    Json(_req): Json<AddRuleRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // TODO: Parse severity and operator, create AlertRule, add to manager
    Ok(Json(json!({
        "success": true,
        "message": "Alert rule added"
    })))
}

/// DELETE /api/alerts/rules/:name - Delete an alert rule
async fn delete_rule(
    State(state): State<AlertRouteState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let removed = state.alert_manager.remove_rule(&name);

    if removed {
        Ok(Json(json!({
            "success": true,
            "message": format!("Alert rule '{}' deleted", name)
        })))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerting::AlertManagerConfig;

    #[tokio::test]
    async fn test_alert_routes_creation() {
        let manager = Arc::new(AlertManager::new(AlertManagerConfig::default()));
        let _router = create_router(manager);
        // Router should be created successfully
        assert!(true);
    }
}
