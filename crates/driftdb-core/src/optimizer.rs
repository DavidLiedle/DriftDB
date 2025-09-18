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
    pub avg_row_size: usize,
    pub total_size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
    pub index_stats: HashMap<String, IndexStatistics>,
    pub last_updated: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower_bound: serde_json::Value,
    pub upper_bound: serde_json::Value,
    pub frequency: usize,
}

/// Query optimizer
pub struct QueryOptimizer {
    statistics: Arc<RwLock<HashMap<String, TableStatistics>>>,
    plan_cache: Arc<RwLock<HashMap<String, QueryPlan>>>,
    cost_model: CostModel,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            cost_model: CostModel::default(),
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
            for (index_name, index_stats) in &table_stats.index_stats {
                // Check if any condition can use this index
                for condition in conditions {
                    if condition.column == *index_name {
                        let selectivity = self.estimate_selectivity(table, condition);
                        let estimated_rows = (table_stats.row_count as f64 * selectivity) as usize;
                        let depth_penalty = (index_stats.depth.max(1) as f64).ln().max(1.0);

                        plans.push(PlanStep::IndexLookup {
                            table: table.to_string(),
                            index: index_name.clone(),
                            key: condition.value.to_string(),
                            estimated_rows: estimated_rows.max(1),
                            cost: self.cost_model.index_lookup_cost() * depth_penalty,
                        });
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
                cost_a.partial_cmp(&cost_b).unwrap()
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
            AsOf::Now => (None, None),
        }
    }

    /// Estimate selectivity of a predicate
    fn estimate_selectivity(&self, table: &str, condition: &WhereCondition) -> f64 {
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            if let Some(col_stats) = table_stats.column_stats.get(&condition.column) {
                if col_stats.distinct_values > 0 {
                    1.0 / col_stats.distinct_values as f64
                } else {
                    0.1
                }
            } else {
                0.3 // No statistics, use default
            }
        } else {
            0.3 // No table statistics
        }
    }

    /// Check if condition is covered by index
    fn is_condition_covered_by_index(&self, condition: &WhereCondition, uses_index: bool) -> bool {
        // Simplified: assume index aids equality predicates on non-null values
        uses_index && !matches!(condition.value, serde_json::Value::Null)
    }

    /// Estimate rows in table
    fn estimate_table_rows(&self, table: &str) -> usize {
        self.statistics
            .read()
            .get(table)
            .map(|s| s.row_count)
            .unwrap_or(10000) // Default estimate
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
    fn find_closest_snapshot(&self, _table: &str, _sequence: u64) -> Option<u64> {
        // In production, would query snapshot manager
        Some(1000) // Dummy value
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
            avg_row_size: 100,
            total_size_bytes: 1_000_000,
            column_stats: HashMap::new(),
            index_stats: HashMap::new(),
            last_updated: 0,
        };

        stats.column_stats.insert(
            "status".to_string(),
            ColumnStatistics {
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
