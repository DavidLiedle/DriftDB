//! Query Optimizer for DriftDB
//!
//! Implements cost-based query optimization including:
//! - Index selection
//! - Query plan generation
//! - Statistics-based cost estimation
//! - Query rewriting
//! - Plan caching

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::{AsOf, WhereCondition};
use crate::errors::Result;

/// Query execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Human-readable description of the plan
    pub description: String,
    /// Estimated cost (arbitrary units)
    pub estimated_cost: f64,
    /// Estimated number of rows to scan
    pub estimated_rows: usize,
    /// Whether an index will be used
    pub uses_index: bool,
    /// Index name if applicable
    pub index_name: Option<String>,
    /// Execution steps
    pub steps: Vec<PlanStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub operation: String,
    pub description: String,
    pub estimated_cost: f64,
}

/// Table statistics for cost-based optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    /// Total number of rows
    pub row_count: usize,
    /// Number of deleted rows
    pub deleted_count: usize,
    /// Average row size in bytes
    pub avg_row_size: usize,
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStats>,
    /// Last update timestamp
    pub last_updated: time::OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Number of distinct values
    pub distinct_count: usize,
    /// Number of null values
    pub null_count: usize,
    /// Minimum value (if numeric/comparable)
    pub min_value: Option<Value>,
    /// Maximum value (if numeric/comparable)
    pub max_value: Option<Value>,
    /// Most common values and their frequencies
    pub most_common_values: Vec<(Value, usize)>,
}

/// Query optimizer
pub struct QueryOptimizer {
    /// Table statistics cache
    stats_cache: Arc<RwLock<HashMap<String, TableStats>>>,
    /// Query plan cache
    plan_cache: Arc<RwLock<HashMap<String, QueryPlan>>>,
    /// Available indexes per table
    indexes: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            stats_cache: Arc::new(RwLock::new(HashMap::new())),
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Optimize a SELECT query
    pub fn optimize_select(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        as_of: &Option<AsOf>,
        limit: Option<usize>,
    ) -> Result<QueryPlan> {
        // Check plan cache
        let cache_key = self.generate_cache_key(table, conditions, as_of, limit);
        if let Some(cached_plan) = self.get_cached_plan(&cache_key) {
            return Ok(cached_plan);
        }

        // Build execution plan
        let plan = self.build_select_plan(table, conditions, as_of, limit)?;

        // Cache the plan
        self.cache_plan(cache_key, plan.clone());

        Ok(plan)
    }

    /// Build execution plan for SELECT query
    fn build_select_plan(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        as_of: &Option<AsOf>,
        limit: Option<usize>,
    ) -> Result<QueryPlan> {
        let mut steps = Vec::new();
        let mut total_cost = 0.0;

        // Get table statistics
        let stats = self.get_table_stats(table);

        // Step 1: Determine data source (snapshot vs. event replay)
        let (data_source, source_cost) = self.determine_data_source(table, as_of, &stats);
        steps.push(PlanStep {
            operation: "DataSource".to_string(),
            description: data_source.clone(),
            estimated_cost: source_cost,
        });
        total_cost += source_cost;

        // Step 2: Index selection
        let (uses_index, index_name, index_cost, rows_after_index) =
            self.select_index(table, conditions, &stats);

        let mut estimated_rows = if uses_index {
            steps.push(PlanStep {
                operation: "IndexScan".to_string(),
                description: format!(
                    "Use index '{}' on conditions {:?}",
                    index_name.as_ref().unwrap(),
                    conditions
                ),
                estimated_cost: index_cost,
            });
            rows_after_index
        } else {
            steps.push(PlanStep {
                operation: "TableScan".to_string(),
                description: format!("Full table scan of '{}'", table),
                estimated_cost: self.estimate_scan_cost(&stats),
            });
            stats.row_count
        };
        total_cost += index_cost;

        // Step 3: Filter conditions (those not covered by index)
        if !conditions.is_empty() {
            let filter_cost = self.estimate_filter_cost(conditions, estimated_rows);
            steps.push(PlanStep {
                operation: "Filter".to_string(),
                description: format!("Apply {} conditions", conditions.len()),
                estimated_cost: filter_cost,
            });
            total_cost += filter_cost;

            // Estimate selectivity
            estimated_rows = self.estimate_selectivity(conditions, estimated_rows, &stats);
        }

        // Step 4: Time-travel reconstruction (if needed)
        if as_of.is_some() {
            let tt_cost = self.estimate_time_travel_cost(estimated_rows);
            steps.push(PlanStep {
                operation: "TimeTravelReconstruction".to_string(),
                description: "Reconstruct historical state".to_string(),
                estimated_cost: tt_cost,
            });
            total_cost += tt_cost;
        }

        // Step 5: Limit application
        if let Some(limit_val) = limit {
            estimated_rows = std::cmp::min(estimated_rows, limit_val);
            steps.push(PlanStep {
                operation: "Limit".to_string(),
                description: format!("Apply LIMIT {}", limit_val),
                estimated_cost: 1.0, // Negligible cost
            });
        }

        // Build description
        let description = if uses_index {
            format!(
                "Index scan on {} using '{}', estimated {} rows",
                table,
                index_name.as_ref().unwrap(),
                estimated_rows
            )
        } else {
            format!(
                "Full table scan on {}, estimated {} rows",
                table, estimated_rows
            )
        };

        Ok(QueryPlan {
            description,
            estimated_cost: total_cost,
            estimated_rows,
            uses_index,
            index_name,
            steps,
        })
    }

    /// Determine optimal data source (snapshot vs. event replay)
    fn determine_data_source(
        &self,
        table: &str,
        as_of: &Option<AsOf>,
        _stats: &TableStats,
    ) -> (String, f64) {
        match as_of {
            None => {
                // Current state: use latest snapshot + recent events
                let cost = 10.0; // Base cost for snapshot loading
                (format!("Latest snapshot of '{}'", table), cost)
            }
            Some(AsOf::Sequence(seq)) => {
                // Historical query: estimate cost based on distance from current
                let distance_cost = (*seq as f64 / 1000.0) * 0.1; // Cost increases with age
                (
                    format!("Snapshot + event replay from sequence {}", seq),
                    10.0 + distance_cost,
                )
            }
            Some(AsOf::Timestamp(_)) => {
                // Timestamp-based: similar to sequence
                (
                    "Snapshot + event replay from timestamp".to_string(),
                    15.0, // Slightly more expensive due to timestamp lookup
                )
            }
            Some(AsOf::Now) => ("Latest snapshot".to_string(), 10.0),
        }
    }

    /// Select best index for query conditions
    fn select_index(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        stats: &TableStats,
    ) -> (bool, Option<String>, f64, usize) {
        if conditions.is_empty() {
            return (false, None, self.estimate_scan_cost(stats), stats.row_count);
        }

        // Get available indexes
        let indexes = self.indexes.read().unwrap();
        let table_indexes = match indexes.get(table) {
            Some(idx) => idx,
            None => return (false, None, self.estimate_scan_cost(stats), stats.row_count),
        };

        // Try to match conditions to indexes
        let mut best_index: Option<String> = None;
        let mut best_cost = self.estimate_scan_cost(stats);
        let mut best_selectivity = 1.0;

        for index_col in table_indexes {
            // Check if this index can be used for any condition
            for condition in conditions {
                if condition.column == *index_col {
                    // Estimate index scan cost
                    let selectivity = self.estimate_condition_selectivity(condition, stats);
                    let estimated_rows = (stats.row_count as f64 * selectivity) as usize;
                    let index_cost = self.estimate_index_scan_cost(estimated_rows);

                    if index_cost < best_cost {
                        best_index = Some(index_col.clone());
                        best_cost = index_cost;
                        best_selectivity = selectivity;
                    }
                }
            }
        }

        if let Some(index) = best_index {
            let rows = (stats.row_count as f64 * best_selectivity) as usize;
            (true, Some(index), best_cost, rows)
        } else {
            (false, None, best_cost, stats.row_count)
        }
    }

    /// Estimate cost of full table scan
    fn estimate_scan_cost(&self, stats: &TableStats) -> f64 {
        // Cost = rows * avg_row_size * scan_cost_per_byte
        let scan_cost_per_byte = 0.001;
        stats.row_count as f64 * stats.avg_row_size as f64 * scan_cost_per_byte
    }

    /// Estimate cost of index scan
    fn estimate_index_scan_cost(&self, estimated_rows: usize) -> f64 {
        // Index lookup is cheaper than full scan
        // Cost = fixed_cost + (rows * lookup_cost)
        let fixed_cost = 5.0;
        let lookup_cost = 0.1;
        fixed_cost + (estimated_rows as f64 * lookup_cost)
    }

    /// Estimate cost of filtering
    fn estimate_filter_cost(&self, conditions: &[WhereCondition], rows: usize) -> f64 {
        // Cost per condition per row
        let cost_per_condition = 0.01;
        conditions.len() as f64 * rows as f64 * cost_per_condition
    }

    /// Estimate cost of time-travel reconstruction
    fn estimate_time_travel_cost(&self, rows: usize) -> f64 {
        // Time-travel requires event replay, which is more expensive
        let event_replay_cost = 0.5;
        rows as f64 * event_replay_cost
    }

    /// Estimate selectivity of conditions
    fn estimate_selectivity(
        &self,
        conditions: &[WhereCondition],
        initial_rows: usize,
        stats: &TableStats,
    ) -> usize {
        let mut selectivity = 1.0;

        for condition in conditions {
            selectivity *= self.estimate_condition_selectivity(condition, stats);
        }

        (initial_rows as f64 * selectivity) as usize
    }

    /// Estimate selectivity of a single condition
    fn estimate_condition_selectivity(
        &self,
        condition: &WhereCondition,
        stats: &TableStats,
    ) -> f64 {
        // Get column statistics if available
        if let Some(col_stats) = stats.column_stats.get(&condition.column) {
            match condition.operator.as_str() {
                "=" => {
                    // Equality: 1 / distinct_values
                    if col_stats.distinct_count > 0 {
                        1.0 / col_stats.distinct_count as f64
                    } else {
                        0.01 // Default 1% selectivity
                    }
                }
                "!=" | "<>" => {
                    // Not equal: 1 - (1 / distinct_values)
                    if col_stats.distinct_count > 0 {
                        1.0 - (1.0 / col_stats.distinct_count as f64)
                    } else {
                        0.99
                    }
                }
                "<" | "<=" | ">" | ">=" => {
                    // Range queries: estimate 33% selectivity by default
                    0.33
                }
                "LIKE" => {
                    // Pattern matching: estimate 20% selectivity
                    0.20
                }
                "IN" => {
                    // IN clause: depends on number of values
                    // Assume 5% per value
                    0.05
                }
                _ => 0.5, // Unknown operator: 50% selectivity
            }
        } else {
            // No statistics available: use conservative estimate
            0.5
        }
    }

    /// Get table statistics (from cache or generate default)
    fn get_table_stats(&self, table: &str) -> TableStats {
        let cache = self.stats_cache.read().unwrap();
        cache.get(table).cloned().unwrap_or_else(|| {
            // Return default stats if not cached
            TableStats {
                row_count: 1000, // Conservative default
                deleted_count: 0,
                avg_row_size: 256,
                column_stats: HashMap::new(),
                last_updated: time::OffsetDateTime::now_utc(),
            }
        })
    }

    /// Update table statistics
    pub fn update_stats(&self, table: String, stats: TableStats) {
        let mut cache = self.stats_cache.write().unwrap();
        cache.insert(table, stats);
    }

    /// Register an index
    pub fn register_index(&self, table: String, column: String) {
        let mut indexes = self.indexes.write().unwrap();
        indexes.entry(table).or_default().push(column);
    }

    /// Generate cache key for query plan
    fn generate_cache_key(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        as_of: &Option<AsOf>,
        limit: Option<usize>,
    ) -> String {
        format!(
            "{}:{}:{}:{}",
            table,
            conditions.len(),
            as_of.is_some(),
            limit.is_some()
        )
    }

    /// Get cached query plan
    fn get_cached_plan(&self, key: &str) -> Option<QueryPlan> {
        let cache = self.plan_cache.read().unwrap();
        cache.get(key).cloned()
    }

    /// Cache query plan
    fn cache_plan(&self, key: String, plan: QueryPlan) {
        let mut cache = self.plan_cache.write().unwrap();

        // Limit cache size to 1000 plans
        if cache.len() >= 1000 {
            // Remove oldest entry (simple LRU approximation)
            if let Some(first_key) = cache.keys().next().cloned() {
                cache.remove(&first_key);
            }
        }

        cache.insert(key, plan);
    }

    /// Clear plan cache
    pub fn clear_plan_cache(&self) {
        let mut cache = self.plan_cache.write().unwrap();
        cache.clear();
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> (usize, usize) {
        let plan_cache = self.plan_cache.read().unwrap();
        let stats_cache = self.stats_cache.read().unwrap();
        (plan_cache.len(), stats_cache.len())
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_optimizer_creation() {
        let optimizer = QueryOptimizer::new();
        let (plan_cache_size, stats_cache_size) = optimizer.get_cache_stats();
        assert_eq!(plan_cache_size, 0);
        assert_eq!(stats_cache_size, 0);
    }

    #[test]
    fn test_index_registration() {
        let optimizer = QueryOptimizer::new();
        optimizer.register_index("users".to_string(), "email".to_string());
        optimizer.register_index("users".to_string(), "age".to_string());

        let indexes = optimizer.indexes.read().unwrap();
        let user_indexes = indexes.get("users").unwrap();
        assert_eq!(user_indexes.len(), 2);
        assert!(user_indexes.contains(&"email".to_string()));
        assert!(user_indexes.contains(&"age".to_string()));
    }

    #[test]
    fn test_simple_select_plan() {
        let optimizer = QueryOptimizer::new();

        let plan = optimizer
            .optimize_select("users", &[], &None, None)
            .unwrap();

        assert!(plan.description.contains("Full table scan"));
        assert!(!plan.uses_index);
        assert!(plan.estimated_cost > 0.0);
    }

    #[test]
    fn test_indexed_select_plan() {
        let optimizer = QueryOptimizer::new();
        optimizer.register_index("users".to_string(), "email".to_string());

        // Update stats
        let mut stats = TableStats {
            row_count: 10000,
            deleted_count: 0,
            avg_row_size: 256,
            column_stats: HashMap::new(),
            last_updated: time::OffsetDateTime::now_utc(),
        };

        stats.column_stats.insert(
            "email".to_string(),
            ColumnStats {
                distinct_count: 9000,
                null_count: 0,
                min_value: None,
                max_value: None,
                most_common_values: vec![],
            },
        );

        optimizer.update_stats("users".to_string(), stats);

        let conditions = vec![WhereCondition {
            column: "email".to_string(),
            operator: "=".to_string(),
            value: json!("test@example.com"),
        }];

        let plan = optimizer
            .optimize_select("users", &conditions, &None, None)
            .unwrap();

        assert!(plan.uses_index);
        assert_eq!(plan.index_name, Some("email".to_string()));
        assert!(plan.description.contains("Index scan"));
    }

    #[test]
    fn test_plan_caching() {
        let optimizer = QueryOptimizer::new();

        let conditions = vec![WhereCondition {
            column: "id".to_string(),
            operator: "=".to_string(),
            value: json!(1),
        }];

        // First query - should be cached
        let plan1 = optimizer
            .optimize_select("users", &conditions, &None, None)
            .unwrap();

        // Second identical query - should hit cache
        let plan2 = optimizer
            .optimize_select("users", &conditions, &None, None)
            .unwrap();

        assert_eq!(plan1.estimated_cost, plan2.estimated_cost);

        let (cache_size, _) = optimizer.get_cache_stats();
        assert_eq!(cache_size, 1);
    }

    #[test]
    fn test_selectivity_estimation() {
        let optimizer = QueryOptimizer::new();

        let mut stats = TableStats {
            row_count: 10000,
            deleted_count: 0,
            avg_row_size: 256,
            column_stats: HashMap::new(),
            last_updated: time::OffsetDateTime::now_utc(),
        };

        stats.column_stats.insert(
            "status".to_string(),
            ColumnStats {
                distinct_count: 3, // active, inactive, pending
                null_count: 0,
                min_value: None,
                max_value: None,
                most_common_values: vec![],
            },
        );

        let condition = WhereCondition {
            column: "status".to_string(),
            operator: "=".to_string(),
            value: json!("active"),
        };

        let selectivity = optimizer.estimate_condition_selectivity(&condition, &stats);

        // Should be approximately 1/3
        assert!(selectivity > 0.3 && selectivity < 0.4);
    }
}
