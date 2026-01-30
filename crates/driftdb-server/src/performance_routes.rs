//! HTTP routes for performance monitoring and optimization

use std::sync::Arc;

use axum::{extract::State, response::Json, routing::get, Router};
use serde_json::{json, Value};
use tracing::info;

use crate::performance::{ConnectionPoolOptimizer, PerformanceMonitor, QueryOptimizer};

/// State for performance monitoring endpoints
#[derive(Clone)]
pub struct PerformanceState {
    pub monitor: Option<Arc<PerformanceMonitor>>,
    pub query_optimizer: Option<Arc<QueryOptimizer>>,
    pub pool_optimizer: Option<Arc<ConnectionPoolOptimizer>>,
}

impl PerformanceState {
    pub fn new(
        monitor: Option<Arc<PerformanceMonitor>>,
        query_optimizer: Option<Arc<QueryOptimizer>>,
        pool_optimizer: Option<Arc<ConnectionPoolOptimizer>>,
    ) -> Self {
        Self {
            monitor,
            query_optimizer,
            pool_optimizer,
        }
    }
}

/// Create performance monitoring routes
pub fn create_performance_routes(state: PerformanceState) -> Router {
    Router::new()
        .route("/performance", get(get_performance_overview))
        .route("/performance/queries", get(get_query_performance))
        .route("/performance/connections", get(get_connection_performance))
        .route("/performance/memory", get(get_memory_performance))
        .route(
            "/performance/optimization",
            get(get_optimization_suggestions),
        )
        .route("/performance/cache", get(get_cache_stats))
        .with_state(state)
}

/// Get overall performance overview
async fn get_performance_overview(State(state): State<PerformanceState>) -> Json<Value> {
    info!("Performance overview requested");

    let mut overview = json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "monitoring_enabled": state.monitor.is_some()
    });

    if let Some(monitor) = &state.monitor {
        let stats = monitor.get_performance_stats();
        overview["query_stats"] = json!({
            "total_queries": stats["query_performance"]["total_unique_queries"],
            "active_connections": stats["connection_performance"]["active_connections"]
        });
    }

    if let Some(pool_optimizer) = &state.pool_optimizer {
        let pool_stats = pool_optimizer.analyze_pool_performance();
        overview["pool_health"] = json!({
            "load_factor": pool_stats["pool_health"]["current_load_factor"],
            "recommendations_count": pool_stats["recommendations"].as_array().unwrap_or(&vec![]).len()
        });
    }

    Json(overview)
}

/// Get detailed query performance metrics
async fn get_query_performance(State(state): State<PerformanceState>) -> Json<Value> {
    info!("Query performance metrics requested");

    if let Some(monitor) = &state.monitor {
        let stats = monitor.get_performance_stats();
        Json(stats["query_performance"].clone())
    } else {
        Json(json!({
            "error": "Performance monitoring is disabled",
            "message": "Enable performance monitoring to view query metrics"
        }))
    }
}

/// Get connection pool performance metrics
async fn get_connection_performance(State(state): State<PerformanceState>) -> Json<Value> {
    info!("Connection performance metrics requested");

    let mut response = json!({});

    if let Some(monitor) = &state.monitor {
        let stats = monitor.get_performance_stats();
        response["connection_stats"] = stats["connection_performance"].clone();
    }

    if let Some(pool_optimizer) = &state.pool_optimizer {
        let pool_analysis = pool_optimizer.analyze_pool_performance();
        response["pool_analysis"] = pool_analysis;
    }

    if response.as_object().unwrap().is_empty() {
        Json(json!({
            "error": "Performance monitoring is disabled",
            "message": "Enable performance monitoring to view connection metrics"
        }))
    } else {
        Json(response)
    }
}

/// Get memory performance metrics
async fn get_memory_performance(State(state): State<PerformanceState>) -> Json<Value> {
    info!("Memory performance metrics requested");

    if let Some(monitor) = &state.monitor {
        // Update memory stats before returning
        monitor.update_memory_stats();
        let stats = monitor.get_performance_stats();

        Json(json!({
            "memory_stats": stats["memory_performance"].clone(),
            "recommendations": analyze_memory_performance(&stats)
        }))
    } else {
        Json(json!({
            "error": "Performance monitoring is disabled",
            "message": "Enable performance monitoring to view memory metrics"
        }))
    }
}

/// Get optimization suggestions
async fn get_optimization_suggestions(State(state): State<PerformanceState>) -> Json<Value> {
    info!("Optimization suggestions requested");

    let mut suggestions = Vec::new();

    // Query optimization suggestions
    if let Some(monitor) = &state.monitor {
        let stats = monitor.get_performance_stats();

        if let Some(queries) = stats["query_performance"]["top_slowest_queries"].as_array() {
            let slow_queries: Vec<&Value> = queries
                .iter()
                .filter(|q| q["avg_duration_ms"].as_u64().unwrap_or(0) > 500)
                .collect();

            if !slow_queries.is_empty() {
                suggestions.push(json!({
                    "category": "Query Performance",
                    "priority": "high",
                    "suggestion": format!("Found {} slow queries (>500ms avg). Consider query optimization.", slow_queries.len()),
                    "details": slow_queries
                }));
            }
        }

        // Memory suggestions
        if let Some(memory) = stats["memory_performance"].as_object() {
            let heap_mb = memory["heap_used_mb"].as_f64().unwrap_or(0.0);
            if heap_mb > 1000.0 {
                suggestions.push(json!({
                    "category": "Memory Usage",
                    "priority": "medium",
                    "suggestion": format!("High memory usage ({:.1}MB). Consider implementing query result caching limits.", heap_mb)
                }));
            }
        }
    }

    // Connection pool suggestions
    if let Some(pool_optimizer) = &state.pool_optimizer {
        let pool_analysis = pool_optimizer.analyze_pool_performance();
        if let Some(recommendations) = pool_analysis["recommendations"].as_array() {
            for rec in recommendations {
                if let Some(rec_str) = rec.as_str() {
                    suggestions.push(json!({
                        "category": "Connection Pool",
                        "priority": "medium",
                        "suggestion": rec_str
                    }));
                }
            }
        }
    }

    // Add general performance suggestions
    suggestions.push(json!({
        "category": "General",
        "priority": "low",
        "suggestion": "Consider enabling query result caching for frequently executed queries"
    }));

    suggestions.push(json!({
        "category": "General",
        "priority": "low",
        "suggestion": "Monitor connection pool utilization and adjust sizing as needed"
    }));

    Json(json!({
        "total_suggestions": suggestions.len(),
        "suggestions": suggestions,
        "generated_at": chrono::Utc::now().to_rfc3339()
    }))
}

/// Get cache statistics and hit rates
async fn get_cache_stats(State(state): State<PerformanceState>) -> Json<Value> {
    info!("Cache statistics requested");

    let mut cache_stats = json!({
        "query_plan_cache": {
            "enabled": state.query_optimizer.is_some()
        },
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    if let Some(_optimizer) = &state.query_optimizer {
        // Note: In a real implementation, you'd expose cache statistics from QueryOptimizer
        cache_stats["query_plan_cache"]["hit_rate"] = json!("N/A");
        cache_stats["query_plan_cache"]["size"] = json!("N/A");
        cache_stats["query_plan_cache"]["max_size"] = json!(1000);
    }

    Json(cache_stats)
}

/// Analyze memory performance and provide recommendations
fn analyze_memory_performance(stats: &Value) -> Vec<String> {
    let mut recommendations = Vec::new();

    if let Some(memory) = stats["memory_performance"].as_object() {
        let heap_mb = memory["heap_used_mb"].as_f64().unwrap_or(0.0);
        let cache_mb = memory["query_cache_mb"].as_f64().unwrap_or(0.0);

        if heap_mb > 2000.0 {
            recommendations.push(
                "Consider reducing connection pool size or implementing connection recycling"
                    .to_string(),
            );
        }

        if cache_mb > 100.0 {
            recommendations.push("Query cache is using significant memory. Consider implementing cache eviction policies".to_string());
        }

        if heap_mb < 100.0 {
            recommendations.push("Low memory usage detected. Consider increasing buffer sizes for better performance".to_string());
        }
    }

    if recommendations.is_empty() {
        recommendations.push("Memory usage appears optimal".to_string());
    }

    recommendations
}
