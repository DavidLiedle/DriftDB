//! Query optimizer with cost-based planning
//!
//! Optimizes query execution using:
//! - Statistics-based cost estimation
//! - Index selection
//! - Join order optimization
//! - Predicate pushdown
//! - Query plan caching

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::errors::Result;
use crate::query::{AsOf, Query, WhereCondition};

/// Query execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub steps: Vec<PlanStep>,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
    pub uses_index: bool,
    pub cacheable: bool,
}

/// Individual step in query plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanStep {
    /// Full table scan
    TableScan {
        table: String,
        estimated_rows: usize,
        cost: f64,
    },
    /// Index scan
    IndexScan {
        table: String,
        index: String,
        start_key: Option<String>,
        end_key: Option<String>,
        estimated_rows: usize,
        cost: f64,
    },
    /// Index lookup (point query)
    IndexLookup {
        table: String,
        index: String,
        key: String,
        estimated_rows: usize,
        cost: f64,
    },
    /// Filter rows based on predicate
    Filter {
        predicate: WhereCondition,
        selectivity: f64,
        cost: f64,
    },
    /// Sort rows
    Sort {
        column: String,
        ascending: bool,
        estimated_rows: usize,
        cost: f64,
    },
    /// Limit results
    Limit { count: usize, cost: f64 },
    /// Time travel to specific version
    TimeTravel { as_of: AsOf, cost: f64 },
    /// Load snapshot
    SnapshotLoad { sequence: u64, cost: f64 },
    /// Replay events from WAL
    EventReplay {
        from_sequence: u64,
        to_sequence: u64,
        estimated_events: usize,
        cost: f64,
    },
}

/// Table statistics for cost estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: usize,
    pub column_count: usize,
    pub avg_row_size: usize,
    pub total_size_bytes: u64,
    pub data_size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
    pub column_statistics: HashMap<String, ColumnStatistics>,
    pub index_stats: HashMap<String, IndexStatistics>,
    pub last_updated: u64,
    pub collection_method: String,
    pub collection_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub distinct_values: usize,
    pub null_count: usize,
    pub min_value: Option<serde_json::Value>,
    pub max_value: Option<serde_json::Value>,
    pub histogram: Option<Histogram>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    pub index_name: String,
    pub unique_keys: usize,
    pub depth: usize,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
    pub bucket_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower_bound: serde_json::Value,
    pub upper_bound: serde_json::Value,
    pub frequency: usize,
    pub min_value: serde_json::Value,
    pub max_value: serde_json::Value,
    pub distinct_count: usize,
}

/// Query optimizer
pub struct QueryOptimizer {
    statistics: Arc<RwLock<HashMap<String, TableStatistics>>>,
    plan_cache: Arc<RwLock<HashMap<String, QueryPlan>>>,
    cost_model: CostModel,
    snapshot_registry: Arc<RwLock<HashMap<String, Vec<SnapshotInfo>>>>,
}

/// Information about available snapshots
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub sequence: u64,
    pub timestamp: u64,
    pub size_bytes: u64,
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            cost_model: CostModel::default(),
            snapshot_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Optimize a query and produce execution plan
    #[instrument(skip(self))]
    pub fn optimize(&self, query: &Query) -> Result<QueryPlan> {
        // Check plan cache
        let cache_key = self.query_cache_key(query);
        if let Some(cached_plan) = self.plan_cache.read().get(&cache_key) {
            debug!("Using cached query plan");
            return Ok(cached_plan.clone());
        }

        let plan = match query {
            Query::Select {
                table,
                conditions,
                as_of,
                limit,
            } => self.optimize_select(table, conditions, as_of.as_ref(), limit.as_ref()),
            _ => {
                // Non-select queries don't need optimization
                Ok(QueryPlan {
                    steps: vec![],
                    estimated_cost: 1.0,
                    estimated_rows: 1,
                    uses_index: false,
                    cacheable: false,
                })
            }
        }?;

        // Cache the plan if it's cacheable
        if plan.cacheable {
            self.plan_cache.write().insert(cache_key, plan.clone());
        }

        Ok(plan)
    }

    /// Optimize SELECT query
    fn optimize_select(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        as_of: Option<&AsOf>,
        limit: Option<&usize>,
    ) -> Result<QueryPlan> {
        let mut steps = Vec::new();
        let mut estimated_cost = 0.0;
        let mut estimated_rows = self.estimate_table_rows(table);
        let mut uses_index = false;

        // Step 1: Handle time travel if specified
        if let Some(as_of) = as_of {
            let (snapshot_step, replay_step) = self.plan_time_travel(table, as_of);
            if let Some(step) = snapshot_step {
                estimated_cost += self.cost_of_step(&step);
                steps.push(step);
            }
            if let Some(step) = replay_step {
                estimated_cost += self.cost_of_step(&step);
                steps.push(step);
            }
        }

        // Step 2: Choose access method (index vs table scan)
        let access_plans = self.generate_access_plans(table, conditions);
        let best_access = self.choose_best_plan(&access_plans);

        if let Some(plan) = best_access {
            uses_index = matches!(
                plan,
                PlanStep::IndexScan { .. } | PlanStep::IndexLookup { .. }
            );
            estimated_rows = self.rows_after_step(&plan, estimated_rows);
            estimated_cost += self.cost_of_step(&plan);
            steps.push(plan);
        } else {
            // Fallback to table scan
            let scan_cost = self.cost_model.table_scan_cost(estimated_rows);
            steps.push(PlanStep::TableScan {
                table: table.to_string(),
                estimated_rows,
                cost: scan_cost,
            });
            estimated_cost += scan_cost;
        }

        // Step 3: Apply remaining filters
        for condition in conditions {
            if !self.is_condition_covered_by_index(condition, uses_index) {
                let selectivity = self.estimate_selectivity(table, condition);
                let filter_cost = self.cost_model.filter_cost(estimated_rows);

                steps.push(PlanStep::Filter {
                    predicate: condition.clone(),
                    selectivity,
                    cost: filter_cost,
                });

                estimated_rows = (estimated_rows as f64 * selectivity) as usize;
                estimated_cost += filter_cost;
            }
        }

        // Step 4: Apply limit if specified
        if let Some(limit_count) = limit {
            steps.push(PlanStep::Limit {
                count: *limit_count,
                cost: 0.1, // Minimal cost for limit
            });
            estimated_rows = estimated_rows.min(*limit_count);
            estimated_cost += 0.1;
        }

        Ok(QueryPlan {
            steps,
            estimated_cost,
            estimated_rows,
            uses_index,
            cacheable: true,
        })
    }

    /// Generate possible access plans for a table
    fn generate_access_plans(&self, table: &str, conditions: &[WhereCondition]) -> Vec<PlanStep> {
        let mut plans = Vec::new();
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            // Check each index
            for index_name in table_stats.index_stats.keys() {
                // Check if any condition can use this index
                for condition in conditions {
                    if condition.column == *index_name {
                        let selectivity = self.estimate_selectivity(table, condition);
                        let estimated_rows = (table_stats.row_count as f64 * selectivity) as usize;

                        if condition.operator == "=" {
                            // Point lookup
                            plans.push(PlanStep::IndexLookup {
                                table: table.to_string(),
                                index: index_name.clone(),
                                key: condition.value.to_string(),
                                estimated_rows: 1,
                                cost: self.cost_model.index_lookup_cost(),
                            });
                        } else {
                            // Range scan
                            plans.push(PlanStep::IndexScan {
                                table: table.to_string(),
                                index: index_name.clone(),
                                start_key: Some(condition.value.to_string()),
                                end_key: None,
                                estimated_rows,
                                cost: self.cost_model.index_scan_cost(estimated_rows),
                            });
                        }
                    }
                }
            }
        }

        // Always consider table scan as fallback
        let scan_rows = self.estimate_table_rows(table);
        plans.push(PlanStep::TableScan {
            table: table.to_string(),
            estimated_rows: scan_rows,
            cost: self.cost_model.table_scan_cost(scan_rows),
        });

        plans
    }

    /// Choose the best plan based on cost
    fn choose_best_plan(&self, plans: &[PlanStep]) -> Option<PlanStep> {
        plans
            .iter()
            .min_by(|a, b| {
                let cost_a = self.cost_of_step(a);
                let cost_b = self.cost_of_step(b);
                cost_a
                    .partial_cmp(&cost_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Plan time travel operations
    fn plan_time_travel(&self, table: &str, as_of: &AsOf) -> (Option<PlanStep>, Option<PlanStep>) {
        match as_of {
            AsOf::Sequence(seq) => {
                // Find closest snapshot
                let snapshot_seq = self.find_closest_snapshot(table, *seq);

                let snapshot_step = snapshot_seq.map(|s| PlanStep::SnapshotLoad {
                    sequence: s,
                    cost: self.cost_model.snapshot_load_cost(),
                });

                let replay_step = if let Some(snap_seq) = snapshot_seq {
                    if snap_seq < *seq {
                        Some(PlanStep::EventReplay {
                            from_sequence: snap_seq,
                            to_sequence: *seq,
                            estimated_events: (*seq - snap_seq) as usize,
                            cost: self
                                .cost_model
                                .event_replay_cost((*seq - snap_seq) as usize),
                        })
                    } else {
                        None
                    }
                } else {
                    Some(PlanStep::EventReplay {
                        from_sequence: 0,
                        to_sequence: *seq,
                        estimated_events: *seq as usize,
                        cost: self.cost_model.event_replay_cost(*seq as usize),
                    })
                };

                (snapshot_step, replay_step)
            }
            AsOf::Timestamp(_ts) => {
                // Convert timestamp to sequence (simplified)
                (None, None)
            }
            AsOf::Now => {
                // No time travel needed for current state
                (None, None)
            }
        }
    }

    /// Estimate selectivity of a predicate
    fn estimate_selectivity(&self, table: &str, condition: &WhereCondition) -> f64 {
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            if let Some(col_stats) = table_stats.column_stats.get(&condition.column) {
                // Account for nulls
                let null_fraction = if table_stats.row_count > 0 {
                    col_stats.null_count as f64 / table_stats.row_count as f64
                } else {
                    0.0
                };

                // Use statistics to estimate selectivity
                let non_null_selectivity = match condition.operator.as_str() {
                    "=" => {
                        // Point query selectivity
                        if col_stats.distinct_values > 0 {
                            // Check if we have histogram data for more accurate estimate
                            if let Some(histogram) = &col_stats.histogram {
                                self.estimate_equality_selectivity_with_histogram(
                                    &condition.value,
                                    histogram,
                                    table_stats.row_count,
                                )
                            } else {
                                // Uniform distribution assumption
                                1.0 / col_stats.distinct_values as f64
                            }
                        } else {
                            0.1 // Default
                        }
                    }
                    "<" | ">" | "<=" | ">=" => {
                        // Range query selectivity using min/max or histogram
                        if let Some(histogram) = &col_stats.histogram {
                            self.estimate_range_selectivity_with_histogram(
                                &condition.value,
                                &condition.operator,
                                histogram,
                                table_stats.row_count,
                            )
                        } else if col_stats.min_value.is_some() && col_stats.max_value.is_some() {
                            self.estimate_range_selectivity_with_bounds(
                                &condition.value,
                                &condition.operator,
                                &col_stats.min_value,
                                &col_stats.max_value,
                            )
                        } else {
                            0.3 // Default 30% selectivity for range
                        }
                    }
                    "IS NULL" => null_fraction,
                    "IS NOT NULL" => 1.0 - null_fraction,
                    _ => 0.5, // Default 50% for unknown operators
                };

                // Adjust for nulls (most operators don't match nulls)
                if condition.operator != "IS NULL" && condition.operator != "IS NOT NULL" {
                    non_null_selectivity * (1.0 - null_fraction)
                } else {
                    non_null_selectivity
                }
            } else {
                0.3 // No statistics, use default
            }
        } else {
            0.3 // No table statistics
        }
    }

    /// Estimate selectivity for equality using histogram
    fn estimate_equality_selectivity_with_histogram(
        &self,
        value: &serde_json::Value,
        histogram: &Histogram,
        total_rows: usize,
    ) -> f64 {
        // Find the bucket containing the value
        for bucket in &histogram.buckets {
            if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                // Estimate based on bucket frequency
                return bucket.frequency as f64 / total_rows as f64;
            }
        }
        // Value not in histogram
        0.01
    }

    /// Estimate selectivity for range queries using histogram
    fn estimate_range_selectivity_with_histogram(
        &self,
        value: &serde_json::Value,
        operator: &str,
        histogram: &Histogram,
        total_rows: usize,
    ) -> f64 {
        let mut matching_frequency = 0;

        for bucket in &histogram.buckets {
            match operator {
                "<" => {
                    if self.compare_values(&bucket.upper_bound, value) < 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                ">" => {
                    if self.compare_values(&bucket.lower_bound, value) > 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                "<=" => {
                    if self.compare_values(&bucket.upper_bound, value) <= 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                ">=" => {
                    if self.compare_values(&bucket.lower_bound, value) >= 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                _ => {}
            }
        }

        matching_frequency as f64 / total_rows as f64
    }

    /// Estimate range selectivity using min/max bounds
    fn estimate_range_selectivity_with_bounds(
        &self,
        value: &serde_json::Value,
        operator: &str,
        min_value: &Option<serde_json::Value>,
        max_value: &Option<serde_json::Value>,
    ) -> f64 {
        // Simplified linear interpolation between min and max
        if let (Some(min), Some(max)) = (min_value, max_value) {
            let position = self.interpolate_value_position(value, min, max);
            match operator {
                "<" | "<=" => position,
                ">" | ">=" => 1.0 - position,
                _ => 0.3,
            }
        } else {
            0.3
        }
    }

    /// Check if a value is within a range
    fn value_in_range(
        &self,
        value: &serde_json::Value,
        lower: &serde_json::Value,
        upper: &serde_json::Value,
    ) -> bool {
        self.compare_values(value, lower) >= 0 && self.compare_values(value, upper) <= 0
    }

    /// Compare two JSON values
    fn compare_values(&self, a: &serde_json::Value, b: &serde_json::Value) -> i32 {
        // Simple comparison for common types
        match (a, b) {
            (serde_json::Value::Number(n1), serde_json::Value::Number(n2)) => {
                if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                    f1.partial_cmp(&f2).map(|o| o as i32).unwrap_or(0)
                } else {
                    0
                }
            }
            (serde_json::Value::String(s1), serde_json::Value::String(s2)) => s1.cmp(s2) as i32,
            _ => 0,
        }
    }

    /// Interpolate value position between min and max
    fn interpolate_value_position(
        &self,
        value: &serde_json::Value,
        min: &serde_json::Value,
        max: &serde_json::Value,
    ) -> f64 {
        // Simple linear interpolation for numeric values
        if let (
            serde_json::Value::Number(v),
            serde_json::Value::Number(min_n),
            serde_json::Value::Number(max_n),
        ) = (value, min, max)
        {
            if let (Some(v_f), Some(min_f), Some(max_f)) =
                (v.as_f64(), min_n.as_f64(), max_n.as_f64())
            {
                if max_f > min_f {
                    ((v_f - min_f) / (max_f - min_f)).clamp(0.0, 1.0)
                } else {
                    0.5
                }
            } else {
                0.5
            }
        } else {
            0.5 // Default to middle
        }
    }

    /// Check if condition is covered by index
    fn is_condition_covered_by_index(&self, condition: &WhereCondition, uses_index: bool) -> bool {
        // Simplified: assume index covers equality conditions on indexed column
        uses_index && condition.operator == "="
    }

    /// Estimate rows in table
    fn estimate_table_rows(&self, table: &str) -> usize {
        self.statistics
            .read()
            .get(table)
            .map(|s| s.row_count)
            .unwrap_or(0) // Return 0 if no statistics - forces statistics collection
    }

    /// Calculate cost of a plan step
    fn cost_of_step(&self, step: &PlanStep) -> f64 {
        match step {
            PlanStep::TableScan { cost, .. } => *cost,
            PlanStep::IndexScan { cost, .. } => *cost,
            PlanStep::IndexLookup { cost, .. } => *cost,
            PlanStep::Filter { cost, .. } => *cost,
            PlanStep::Sort { cost, .. } => *cost,
            PlanStep::Limit { cost, .. } => *cost,
            PlanStep::TimeTravel { cost, .. } => *cost,
            PlanStep::SnapshotLoad { cost, .. } => *cost,
            PlanStep::EventReplay { cost, .. } => *cost,
        }
    }

    /// Estimate rows after applying a step
    fn rows_after_step(&self, step: &PlanStep, input_rows: usize) -> usize {
        match step {
            PlanStep::TableScan { estimated_rows, .. } => *estimated_rows,
            PlanStep::IndexScan { estimated_rows, .. } => *estimated_rows,
            PlanStep::IndexLookup { estimated_rows, .. } => *estimated_rows,
            PlanStep::Filter { selectivity, .. } => (input_rows as f64 * selectivity) as usize,
            PlanStep::Limit { count, .. } => input_rows.min(*count),
            _ => input_rows,
        }
    }

    /// Find closest snapshot for time travel
    fn find_closest_snapshot(&self, table: &str, sequence: u64) -> Option<u64> {
        let registry = self.snapshot_registry.read();

        if let Some(snapshots) = registry.get(table) {
            // Find the snapshot with the largest sequence that's still <= target sequence
            snapshots
                .iter()
                .filter(|s| s.sequence <= sequence)
                .max_by_key(|s| s.sequence)
                .map(|s| s.sequence)
        } else {
            None
        }
    }

    /// Register a snapshot with the optimizer
    pub fn register_snapshot(&self, table: &str, info: SnapshotInfo) {
        let mut registry = self.snapshot_registry.write();
        registry.entry(table.to_string()).or_default().push(info);
    }

    /// Generate cache key for query
    fn query_cache_key(&self, query: &Query) -> String {
        format!("{:?}", query) // Simple serialization
    }

    /// Update table statistics
    pub fn update_statistics(&self, table: &str, stats: TableStatistics) {
        self.statistics.write().insert(table.to_string(), stats);
    }

    /// Clear plan cache
    pub fn clear_cache(&self) {
        self.plan_cache.write().clear();
    }

    /// Optimize multiple conditions by reordering for efficiency
    #[allow(dead_code)]
    fn optimize_condition_order(
        &self,
        table: &str,
        conditions: &[WhereCondition],
    ) -> Vec<WhereCondition> {
        let mut conditions = conditions.to_vec();

        // Sort conditions by selectivity (most selective first)
        conditions.sort_by_cached_key(|cond| {
            let selectivity = self.estimate_selectivity(table, cond);
            (selectivity * 1000.0) as i64 // Convert to integer for stable sorting
        });

        conditions
    }

    /// Analyze query patterns and suggest new indexes
    pub fn suggest_indexes(&self, table: &str) -> Vec<String> {
        let mut suggestions = Vec::new();
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            // Analyze column access patterns
            for column_name in table_stats.column_stats.keys() {
                // Suggest index if column is frequently used in WHERE but not indexed
                if !table_stats.index_stats.contains_key(column_name) {
                    // In production, would check query history for this column
                    suggestions.push(format!(
                        "CREATE INDEX idx_{}_{} ON {} ({})",
                        table, column_name, table, column_name
                    ));
                }
            }
        }

        suggestions
    }

    /// Estimate memory usage for query execution
    pub fn estimate_memory_usage(&self, plan: &QueryPlan) -> usize {
        let mut memory = 0;

        for step in &plan.steps {
            match step {
                PlanStep::TableScan { estimated_rows, .. }
                | PlanStep::IndexScan { estimated_rows, .. } => {
                    // Assume average row size of 1KB
                    memory = memory.max(estimated_rows * 1024);
                }
                PlanStep::Sort { estimated_rows, .. } => {
                    // Sorting requires full dataset in memory
                    memory = memory.max(estimated_rows * 1024);
                }
                PlanStep::Limit { count, .. } => {
                    // Limit only needs to buffer the limit amount
                    memory = memory.max(count * 1024);
                }
                _ => {}
            }
        }

        memory
    }
}

/// Cost model for different operations
#[derive(Debug, Clone)]
pub struct CostModel {
    pub seq_page_cost: f64,
    pub random_page_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_operator_cost: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.005,
        }
    }
}

impl CostModel {
    pub fn table_scan_cost(&self, rows: usize) -> f64 {
        let pages = (rows / 100).max(1); // Assume 100 rows per page
        self.seq_page_cost * pages as f64 + self.cpu_tuple_cost * rows as f64
    }

    pub fn index_scan_cost(&self, rows: usize) -> f64 {
        let pages = (rows / 200).max(1); // More rows per index page
        self.random_page_cost * pages as f64 + self.cpu_tuple_cost * rows as f64
    }

    pub fn index_lookup_cost(&self) -> f64 {
        self.random_page_cost * 2.0 + self.cpu_tuple_cost // Index + data page
    }

    pub fn filter_cost(&self, rows: usize) -> f64 {
        self.cpu_operator_cost * rows as f64
    }

    pub fn sort_cost(&self, rows: usize) -> f64 {
        let log_rows = (rows as f64).log2().max(1.0);
        rows as f64 * log_rows * self.cpu_operator_cost
    }

    pub fn snapshot_load_cost(&self) -> f64 {
        self.seq_page_cost * 10.0 // Assume 10 pages for snapshot
    }

    pub fn event_replay_cost(&self, events: usize) -> f64 {
        self.cpu_tuple_cost * events as f64 * 2.0 // Higher cost for replay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_optimization() {
        let optimizer = QueryOptimizer::new();

        // Add some statistics
        let mut stats = TableStatistics {
            table_name: "users".to_string(),
            row_count: 10000,
            column_count: 5,
            avg_row_size: 100,
            total_size_bytes: 1_000_000,
            data_size_bytes: 900_000,
            column_stats: HashMap::new(),
            column_statistics: HashMap::new(),
            index_stats: HashMap::new(),
            last_updated: 0,
            collection_method: "analyze".to_string(),
            collection_duration_ms: 100,
        };

        stats.column_stats.insert(
            "status".to_string(),
            ColumnStatistics {
                column_name: "status".to_string(),
                distinct_values: 3,
                null_count: 0,
                min_value: None,
                max_value: None,
                histogram: None,
            },
        );

        stats.index_stats.insert(
            "status".to_string(),
            IndexStatistics {
                index_name: "status_idx".to_string(),
                unique_keys: 3,
                depth: 2,
                size_bytes: 1024,
            },
        );

        optimizer.update_statistics("users", stats);

        // Create a query
        let query = Query::Select {
            table: "users".to_string(),
            conditions: vec![WhereCondition {
                column: "status".to_string(),
                operator: "=".to_string(),
                value: serde_json::json!("active"),
            }],
            as_of: None,
            limit: Some(100),
        };

        let plan = optimizer.optimize(&query).unwrap();
        assert!(plan.uses_index);
        assert!(plan.estimated_cost > 0.0);
    }

    #[test]
    fn test_cost_model() {
        let cost_model = CostModel::default();

        assert!(cost_model.table_scan_cost(1000) > cost_model.table_scan_cost(100));
        assert!(cost_model.index_lookup_cost() < cost_model.table_scan_cost(1000));
        assert!(cost_model.sort_cost(1000) > cost_model.filter_cost(1000));
    }
}
