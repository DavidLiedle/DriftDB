//! Optimized query executor with performance monitoring and caching

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use tracing::{debug, info, warn, instrument};

use crate::executor::{QueryExecutor, QueryResult};
use crate::performance::{PerformanceMonitor, QueryOptimizer, ConnectionPoolOptimizer};
use driftdb_core::EngineGuard;

/// High-performance query executor with caching and optimization
pub struct OptimizedQueryExecutor {
    engine_guard: EngineGuard,
    performance_monitor: Option<Arc<PerformanceMonitor>>,
    query_optimizer: Option<Arc<QueryOptimizer>>,
    pool_optimizer: Option<Arc<ConnectionPoolOptimizer>>,
}

impl OptimizedQueryExecutor {
    pub fn new(
        engine_guard: EngineGuard,
        performance_monitor: Option<Arc<PerformanceMonitor>>,
        query_optimizer: Option<Arc<QueryOptimizer>>,
        pool_optimizer: Option<Arc<ConnectionPoolOptimizer>>,
    ) -> Self {
        Self {
            engine_guard,
            performance_monitor,
            query_optimizer,
            pool_optimizer,
        }
    }

    /// Execute query with performance optimization and monitoring
    #[instrument(skip(self, sql), fields(sql_hash))]
    pub async fn execute_optimized(&mut self, sql: &str) -> Result<Value> {
        let start_time = Instant::now();
        let sql_hash = self.calculate_sql_hash(sql);

        // Record in tracing span
        tracing::Span::current().record("sql_hash", &sql_hash);

        // Acquire performance permit if monitoring is enabled
        let _permit = if let Some(monitor) = &self.performance_monitor {
            match monitor.acquire_request_permit().await {
                Some(permit) => Some(permit),
                None => {
                    warn!("Request rate limit exceeded for query: {}", &sql_hash);
                    return Err(anyhow!("Server too busy - request rate limited"));
                }
            }
        } else {
            None
        };

        // Apply query optimizations if available
        let (optimized_sql, optimizations) = if let Some(optimizer) = &self.query_optimizer {
            let (opt_sql, opts) = optimizer.optimize_query(sql);
            if !opts.is_empty() {
                info!("Applied query optimizations: {:?}", opts);
            }
            (opt_sql, opts)
        } else {
            (sql.to_string(), vec![])
        };

        // Check for cached execution plan
        let execution_plan = if let Some(optimizer) = &self.query_optimizer {
            optimizer.get_execution_plan(&sql_hash)
        } else {
            None
        };

        if let Some(plan) = execution_plan {
            debug!("Using cached execution plan for query {}: cost={}", sql_hash, plan.estimated_cost);
        }

        // Execute the query using a temporary executor
        let engine_ref = self.engine_guard.get_engine_ref();
        let mut executor = QueryExecutor::new(engine_ref);
        let result = executor.execute(&optimized_sql).await;

        // Record execution time
        let execution_time = start_time.elapsed();

        match &result {
            Ok(_) => {
                info!("Query executed successfully in {}ms", execution_time.as_millis());

                // Record performance metrics
                if let Some(monitor) = &self.performance_monitor {
                    monitor.record_query_execution(sql_hash.clone(), execution_time);
                }

                // Cache execution plan for future use
                if let Some(optimizer) = &self.query_optimizer {
                    let estimated_cost = Self::estimate_query_cost(sql, execution_time);
                    optimizer.cache_execution_plan(sql_hash, optimized_sql, estimated_cost);
                }
            }
            Err(e) => {
                warn!("Query execution failed in {}ms: {}", execution_time.as_millis(), e);

                // Still record the execution time for failed queries
                if let Some(monitor) = &self.performance_monitor {
                    monitor.record_query_execution(sql_hash, execution_time);
                }
            }
        }

        // Convert QueryResult to Value
        result.map(|query_result| Self::query_result_to_json(query_result))
    }

    /// Convert QueryResult to JSON Value
    fn query_result_to_json(query_result: QueryResult) -> Value {
        match query_result {
            QueryResult::Select { columns, rows } => json!({
                "type": "select",
                "columns": columns,
                "rows": rows,
                "count": rows.len()
            }),
            QueryResult::Insert { count } => json!({
                "type": "insert",
                "rows_affected": count
            }),
            QueryResult::Update { count } => json!({
                "type": "update",
                "rows_affected": count
            }),
            QueryResult::Delete { count } => json!({
                "type": "delete",
                "rows_affected": count
            }),
            QueryResult::CreateTable => json!({
                "type": "create_table",
                "success": true
            }),
            QueryResult::DropTable => json!({
                "type": "drop_table",
                "success": true
            }),
            QueryResult::CreateIndex => json!({
                "type": "create_index",
                "success": true
            }),
            QueryResult::Begin => json!({
                "type": "begin",
                "success": true
            }),
            QueryResult::Commit => json!({
                "type": "commit",
                "success": true
            }),
            QueryResult::Rollback => json!({
                "type": "rollback",
                "success": true
            }),
            QueryResult::Empty => json!({
                "type": "empty",
                "success": true
            }),
        }
    }

    /// Calculate a hash for the SQL query for caching purposes
    fn calculate_sql_hash(&self, sql: &str) -> String {
        let mut hasher = DefaultHasher::new();

        // Normalize SQL for consistent hashing
        let normalized = sql
            .trim()
            .to_uppercase()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        normalized.hash(&mut hasher);
        format!("sql_{:x}", hasher.finish())
    }

    /// Estimate query execution cost based on SQL complexity and execution time
    fn estimate_query_cost(sql: &str, execution_time: std::time::Duration) -> f64 {
        let base_cost = execution_time.as_millis() as f64;
        let complexity_multiplier = Self::calculate_query_complexity(sql);

        base_cost * complexity_multiplier
    }

    /// Calculate query complexity factor for cost estimation
    fn calculate_query_complexity(sql: &str) -> f64 {
        let sql_upper = sql.to_uppercase();
        let mut complexity = 1.0;

        // Add complexity for different SQL features
        if sql_upper.contains("JOIN") {
            complexity += 0.5;
        }
        if sql_upper.contains("SUBQUERY") || sql_upper.contains("EXISTS") {
            complexity += 0.7;
        }
        if sql_upper.contains("ORDER BY") {
            complexity += 0.3;
        }
        if sql_upper.contains("GROUP BY") {
            complexity += 0.4;
        }
        if sql_upper.contains("HAVING") {
            complexity += 0.2;
        }

        // Count of conditions
        let where_count = sql_upper.matches("WHERE").count() as f64;
        complexity += where_count * 0.1;

        complexity.max(1.0)
    }

    /// Get performance statistics
    pub fn get_performance_stats(&self) -> Option<Value> {
        self.performance_monitor.as_ref().map(|monitor| {
            monitor.get_performance_stats()
        })
    }

    /// Execute batch of queries with optimization
    pub async fn execute_batch(&mut self, queries: Vec<&str>) -> Result<Vec<Value>> {
        let mut results = Vec::with_capacity(queries.len());
        let start_time = Instant::now();

        info!("Executing batch of {} queries", queries.len());

        // Group similar queries for potential optimization
        let mut query_groups = std::collections::HashMap::new();
        for (idx, query) in queries.iter().enumerate() {
            let pattern = Self::extract_query_pattern(query);
            query_groups.entry(pattern).or_insert(Vec::new()).push((idx, query));
        }

        info!("Grouped {} queries into {} patterns", queries.len(), query_groups.len());

        // Execute each group
        for (_pattern, group) in query_groups {
            for (original_idx, query) in group {
                let result = self.execute_optimized(query).await?;
                results.push((original_idx, result));
            }
        }

        // Sort results back to original order
        results.sort_by_key(|(idx, _)| *idx);
        let final_results: Vec<Value> = results.into_iter().map(|(_, result)| result).collect();

        let total_time = start_time.elapsed();
        info!("Batch execution completed in {}ms", total_time.as_millis());

        Ok(final_results)
    }

    /// Extract a pattern from a query for grouping similar queries
    fn extract_query_pattern(sql: &str) -> String {
        let sql_upper = sql.to_uppercase();

        // Extract basic pattern by removing literals and parameter values
        let pattern = sql_upper
            .split_whitespace()
            .map(|word| {
                if word.starts_with('\'') && word.ends_with('\'') {
                    "'LITERAL'"
                } else if word.parse::<i64>().is_ok() || word.parse::<f64>().is_ok() {
                    "NUMBER"
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        pattern
    }

    /// Analyze slow queries and provide optimization suggestions
    pub fn analyze_slow_queries(&self) -> Option<Value> {
        self.performance_monitor.as_ref().map(|monitor| {
            let stats = monitor.get_performance_stats();

            // Extract slow queries from stats
            if let Some(queries) = stats["query_performance"]["top_slowest_queries"].as_array() {
                let slow_queries: Vec<&Value> = queries
                    .iter()
                    .filter(|q| q["avg_duration_ms"].as_u64().unwrap_or(0) > 1000)
                    .collect();

                serde_json::json!({
                    "slow_queries_count": slow_queries.len(),
                    "slow_queries": slow_queries,
                    "recommendations": [
                        "Consider adding indexes for frequently filtered columns",
                        "Review query structure for unnecessary JOINs",
                        "Check if subqueries can be optimized",
                        "Consider query result caching for repeated patterns"
                    ]
                })
            } else {
                serde_json::json!({
                    "slow_queries_count": 0,
                    "message": "No slow queries detected"
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use driftdb_core::Engine;

    #[tokio::test]
    async fn test_optimized_executor() {
        let temp_dir = tempdir().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let engine_guard = EngineGuard::new(Arc::new(parking_lot::RwLock::new(engine)));

        let monitor = Arc::new(PerformanceMonitor::new(100));
        let optimizer = Arc::new(QueryOptimizer::new());

        let mut executor = OptimizedQueryExecutor::new(
            engine_guard,
            Some(monitor),
            Some(optimizer),
            None,
        );

        // Test basic query execution
        let result = executor.execute_optimized("SELECT 1").await;
        assert!(result.is_ok());

        // Test performance stats collection
        let stats = executor.get_performance_stats();
        assert!(stats.is_some());
    }

    #[test]
    fn test_query_complexity_calculation() {
        let simple_query = "SELECT * FROM users";
        let complex_query = "SELECT u.*, p.* FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true ORDER BY p.created_at";

        let simple_complexity = OptimizedQueryExecutor::calculate_query_complexity(simple_query);
        let complex_complexity = OptimizedQueryExecutor::calculate_query_complexity(complex_query);

        assert!(complex_complexity > simple_complexity);
    }

    #[test]
    fn test_query_pattern_extraction() {
        let query1 = "SELECT * FROM users WHERE id = 123";
        let query2 = "SELECT * FROM users WHERE id = 456";

        let pattern1 = OptimizedQueryExecutor::extract_query_pattern(query1);
        let pattern2 = OptimizedQueryExecutor::extract_query_pattern(query2);

        assert_eq!(pattern1, pattern2); // Should have same pattern
    }
}