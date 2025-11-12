//! Advanced Query Optimization Engine
//!
//! Provides cost-based query optimization using collected statistics to:
//! - Generate optimal execution plans
//! - Choose best join algorithms and order
//! - Optimize predicate pushdown and early filtering
//! - Select appropriate indexes
//! - Estimate query costs and resource usage
//! - Adaptive query execution with runtime feedback

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info, trace};

use crate::errors::{DriftError, Result};
use crate::optimizer::{ColumnStatistics, IndexStatistics, TableStatistics};
use crate::parallel::JoinType;
use crate::query::{AsOf, WhereCondition};
use crate::stats::QueryExecution;

/// Query optimization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerConfig {
    /// Enable cost-based optimization
    pub enable_cbo: bool,
    /// Cost model parameters
    pub cost_model: CostModel,
    /// Join algorithm selection strategy
    pub join_strategy: JoinStrategy,
    /// Enable adaptive optimization
    pub adaptive_optimization: bool,
    /// Index hint usage
    pub use_index_hints: bool,
    /// Maximum optimization time (ms)
    pub max_optimization_time_ms: u64,
    /// Enable parallel execution hints
    pub enable_parallel_hints: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_cbo: true,
            cost_model: CostModel::default(),
            join_strategy: JoinStrategy::CostBased,
            adaptive_optimization: true,
            use_index_hints: true,
            max_optimization_time_ms: 5000,
            enable_parallel_hints: true,
        }
    }
}

/// Cost model parameters for optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModel {
    /// Cost per row read (base unit)
    pub seq_scan_cost_per_row: f64,
    /// Cost per index lookup
    pub index_lookup_cost: f64,
    /// Cost per hash join probe
    pub hash_join_cost_per_probe: f64,
    /// Cost per nested loop join iteration
    pub nested_loop_cost_per_iteration: f64,
    /// Cost per sort operation per row
    pub sort_cost_per_row: f64,
    /// Memory cost factor
    pub memory_cost_factor: f64,
    /// CPU cost factor
    pub cpu_cost_factor: f64,
    /// I/O cost factor
    pub io_cost_factor: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            seq_scan_cost_per_row: 1.0,
            index_lookup_cost: 0.1,
            hash_join_cost_per_probe: 0.5,
            nested_loop_cost_per_iteration: 2.0,
            sort_cost_per_row: 1.5,
            memory_cost_factor: 0.01,
            cpu_cost_factor: 1.0,
            io_cost_factor: 10.0,
        }
    }
}

/// Join algorithm selection strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinStrategy {
    /// Always use hash joins
    HashJoin,
    /// Always use nested loop joins
    NestedLoop,
    /// Choose based on cost estimates
    CostBased,
    /// Adaptive based on runtime feedback
    Adaptive,
}

/// Query execution plan
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Plan nodes organized as a tree
    pub root: PlanNode,
    /// Estimated total cost
    pub estimated_cost: f64,
    /// Estimated execution time (ms)
    pub estimated_time_ms: u64,
    /// Estimated memory usage (bytes)
    pub estimated_memory_bytes: u64,
    /// Optimization metadata
    pub metadata: OptimizationMetadata,
}

/// Individual plan node
#[derive(Debug, Clone)]
pub struct PlanNode {
    /// Node type and operation
    pub operation: PlanOperation,
    /// Child nodes
    pub children: Vec<PlanNode>,
    /// Estimated cost for this node
    pub cost: f64,
    /// Estimated cardinality (rows produced)
    pub cardinality: u64,
    /// Estimated selectivity (fraction of rows passing)
    pub selectivity: f64,
    /// Resource requirements
    pub resources: ResourceRequirements,
}

/// Plan operation types
#[derive(Debug, Clone)]
pub enum PlanOperation {
    /// Table scan
    TableScan {
        table: String,
        filter: Option<FilterExpression>,
        projection: Vec<String>,
    },
    /// Index scan
    IndexScan {
        table: String,
        index: String,
        key_conditions: Vec<WhereCondition>,
        filter: Option<FilterExpression>,
    },
    /// Hash join
    HashJoin {
        join_type: JoinType,
        left_key: String,
        right_key: String,
        conditions: Vec<WhereCondition>,
    },
    /// Nested loop join
    NestedLoopJoin {
        join_type: JoinType,
        conditions: Vec<WhereCondition>,
    },
    /// Sort operation
    Sort {
        columns: Vec<SortColumn>,
        limit: Option<usize>,
    },
    /// Aggregation
    Aggregate {
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
    },
    /// Filter (WHERE clause)
    Filter { expression: FilterExpression },
    /// Projection (SELECT columns)
    Project { columns: Vec<String> },
    /// Limit/Offset
    Limit { count: usize, offset: Option<usize> },
    /// Parallel execution wrapper
    Parallel { degree: usize },
}

/// Filter expression for optimization
#[derive(Debug, Clone)]
pub struct FilterExpression {
    /// Conjunctive normal form predicates
    pub predicates: Vec<Predicate>,
    /// Estimated selectivity
    pub selectivity: f64,
}

/// Individual predicate
#[derive(Debug, Clone)]
pub struct Predicate {
    /// Column being filtered
    pub column: String,
    /// Comparison operator
    pub operator: String,
    /// Filter value
    pub value: Value,
    /// Estimated selectivity of this predicate
    pub selectivity: f64,
}

/// Sort column specification
#[derive(Debug, Clone)]
pub struct SortColumn {
    pub column: String,
    pub ascending: bool,
}

/// Aggregate function for planning
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    pub function: String,
    pub column: Option<String>,
    pub alias: String,
}

/// Resource requirements for a plan node
#[derive(Debug, Clone)]
pub struct ResourceRequirements {
    /// Memory requirement (bytes)
    pub memory_bytes: u64,
    /// CPU cycles required
    pub cpu_cycles: u64,
    /// I/O operations required
    pub io_operations: u64,
    /// Network operations (for distributed plans)
    pub network_operations: u64,
}

/// Optimization metadata
#[derive(Debug, Clone)]
pub struct OptimizationMetadata {
    /// Time spent optimizing (ms)
    pub optimization_time_ms: u64,
    /// Number of plans considered
    pub plans_considered: usize,
    /// Optimization strategy used
    pub strategy: String,
    /// Warnings generated during optimization
    pub warnings: Vec<String>,
    /// Hints applied
    pub hints_applied: Vec<String>,
}

/// Query optimizer engine
pub struct QueryOptimizer {
    /// Configuration
    config: OptimizerConfig,
    /// Statistics provider
    stats_provider: Arc<dyn StatisticsProvider>,
    /// Plan cache for reuse
    plan_cache: Arc<RwLock<HashMap<String, CachedPlan>>>,
    /// Optimization history for adaptive learning
    #[allow(dead_code)]
    optimization_history: Arc<RwLock<Vec<OptimizationResult>>>,
}

/// Cached execution plan
#[derive(Debug, Clone)]
pub struct CachedPlan {
    /// The execution plan
    pub plan: ExecutionPlan,
    /// When it was cached
    pub cached_at: SystemTime,
    /// Cache hit count
    pub hit_count: usize,
    /// Actual execution statistics
    pub execution_stats: Vec<ActualExecutionStats>,
}

/// Actual execution statistics for adaptive optimization
#[derive(Debug, Clone)]
pub struct ActualExecutionStats {
    /// Actual execution time
    pub actual_time_ms: u64,
    /// Actual rows processed
    pub actual_rows: u64,
    /// Actual memory used
    pub actual_memory_bytes: u64,
    /// Execution timestamp
    pub executed_at: SystemTime,
}

/// Optimization result for learning
#[derive(Debug, Clone)]
pub struct OptimizationResult {
    /// Query hash
    pub query_hash: String,
    /// Chosen plan
    pub plan: ExecutionPlan,
    /// Actual execution performance
    pub actual_stats: ActualExecutionStats,
    /// Performance vs estimate accuracy
    pub accuracy: f64,
}

/// Statistics provider trait
pub trait StatisticsProvider: Send + Sync {
    /// Get table statistics
    fn get_table_stats(&self, table: &str) -> Option<TableStatistics>;
    /// Get column statistics
    fn get_column_stats(&self, table: &str, column: &str) -> Option<ColumnStatistics>;
    /// Get index statistics
    fn get_index_stats(&self, index: &str) -> Option<IndexStatistics>;
    /// Get query execution history
    fn get_query_history(&self) -> Vec<QueryExecution>;
}

impl QueryOptimizer {
    /// Create a new query optimizer
    pub fn new(config: OptimizerConfig, stats_provider: Arc<dyn StatisticsProvider>) -> Self {
        Self {
            config,
            stats_provider,
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            optimization_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Optimize a query and generate execution plan
    pub fn optimize_query(&self, query: &OptimizableQuery) -> Result<ExecutionPlan> {
        let start_time = std::time::Instant::now();

        debug!(
            "Optimizing query with {} tables, {} conditions",
            query.tables.len(),
            query.where_conditions.len()
        );

        // Check plan cache first
        let cache_key = self.generate_cache_key(query);
        if let Some(cached_plan) = self.get_cached_plan(&cache_key) {
            trace!("Using cached execution plan");
            return Ok(cached_plan.plan);
        }

        // Generate alternative plans
        let mut candidate_plans = self.generate_candidate_plans(query)?;

        // Cost each plan
        for plan in &mut candidate_plans {
            plan.estimated_cost = self.calculate_plan_cost(&plan.root, query)?;
            plan.estimated_time_ms = self.estimate_execution_time(&plan.root)?;
            plan.estimated_memory_bytes = self.estimate_memory_usage(&plan.root)?;
        }

        // Sort by cost and select best plan
        candidate_plans.sort_by(|a, b| a.estimated_cost.partial_cmp(&b.estimated_cost).unwrap());

        let mut best_plan = candidate_plans
            .into_iter()
            .next()
            .ok_or_else(|| DriftError::InvalidQuery("No valid execution plan found".to_string()))?;

        // Add optimization metadata
        let optimization_time = start_time.elapsed();
        best_plan.metadata = OptimizationMetadata {
            optimization_time_ms: optimization_time.as_millis() as u64,
            plans_considered: 1, // Simplified for now
            strategy: "CostBased".to_string(),
            warnings: Vec::new(),
            hints_applied: Vec::new(),
        };

        // Cache the plan
        self.cache_plan(cache_key, best_plan.clone());

        info!(
            "Query optimization completed in {} ms, estimated cost: {:.2}",
            optimization_time.as_millis(),
            best_plan.estimated_cost
        );

        Ok(best_plan)
    }

    /// Generate candidate execution plans
    fn generate_candidate_plans(&self, query: &OptimizableQuery) -> Result<Vec<ExecutionPlan>> {
        let mut plans = Vec::new();

        // Generate basic plan
        let basic_plan = self.generate_basic_plan(query)?;
        plans.push(basic_plan);

        // Generate index-optimized plans
        if self.config.use_index_hints {
            if let Ok(index_plan) = self.generate_index_optimized_plan(query) {
                plans.push(index_plan);
            }
        }

        // Generate parallel execution plans
        if self.config.enable_parallel_hints {
            if let Ok(parallel_plan) = self.generate_parallel_plan(query) {
                plans.push(parallel_plan);
            }
        }

        Ok(plans)
    }

    /// Generate basic execution plan
    fn generate_basic_plan(&self, query: &OptimizableQuery) -> Result<ExecutionPlan> {
        let mut current_node =
            self.create_table_scan_node(&query.tables[0], &query.where_conditions)?;

        // Add joins for multiple tables
        for table in query.tables.iter().skip(1) {
            let right_node = self.create_table_scan_node(table, &[])?;
            current_node =
                self.create_join_node(current_node, right_node, &query.join_conditions)?;
        }

        // Add remaining WHERE conditions as filter
        if !query.where_conditions.is_empty() {
            current_node = self.create_filter_node(current_node, &query.where_conditions)?;
        }

        // Add aggregation if needed
        if !query.group_by.is_empty() || !query.aggregates.is_empty() {
            current_node =
                self.create_aggregate_node(current_node, &query.group_by, &query.aggregates)?;
        }

        // Add sorting if needed
        if !query.order_by.is_empty() {
            current_node = self.create_sort_node(current_node, &query.order_by)?;
        }

        // Add projection
        if !query.select_columns.is_empty() {
            current_node = self.create_project_node(current_node, &query.select_columns)?;
        }

        // Add limit if specified
        if let Some(limit) = query.limit {
            current_node = self.create_limit_node(current_node, limit, query.offset)?;
        }

        Ok(ExecutionPlan {
            root: current_node,
            estimated_cost: 0.0,
            estimated_time_ms: 0,
            estimated_memory_bytes: 0,
            metadata: OptimizationMetadata {
                optimization_time_ms: 0,
                plans_considered: 0,
                strategy: "Basic".to_string(),
                warnings: Vec::new(),
                hints_applied: Vec::new(),
            },
        })
    }

    /// Generate index-optimized plan
    fn generate_index_optimized_plan(&self, query: &OptimizableQuery) -> Result<ExecutionPlan> {
        // TODO: Implement index selection logic
        self.generate_basic_plan(query)
    }

    /// Generate parallel execution plan
    fn generate_parallel_plan(&self, query: &OptimizableQuery) -> Result<ExecutionPlan> {
        let mut plan = self.generate_basic_plan(query)?;

        // Wrap expensive operations in parallel nodes
        plan.root = self.add_parallelism(plan.root)?;

        Ok(plan)
    }

    /// Add parallelism to a plan node
    #[allow(clippy::only_used_in_recursion)]
    fn add_parallelism(&self, mut node: PlanNode) -> Result<PlanNode> {
        match &node.operation {
            PlanOperation::TableScan { .. } if node.cardinality > 1000 => {
                // Wrap in parallel execution
                Ok(PlanNode {
                    operation: PlanOperation::Parallel { degree: 4 },
                    children: vec![node.clone()],
                    cost: node.cost * 0.7, // Assume 30% speedup
                    cardinality: node.cardinality,
                    selectivity: node.selectivity,
                    resources: node.resources,
                })
            }
            _ => {
                // Recursively process children
                for child in &mut node.children {
                    *child = self.add_parallelism(child.clone())?;
                }
                Ok(node)
            }
        }
    }

    /// Create table scan node
    fn create_table_scan_node(
        &self,
        table: &str,
        conditions: &[WhereCondition],
    ) -> Result<PlanNode> {
        let table_stats = self.stats_provider.get_table_stats(table);
        let cardinality = table_stats.as_ref().map(|s| s.row_count).unwrap_or(1000);

        let filter = if !conditions.is_empty() {
            Some(self.create_filter_expression(conditions)?)
        } else {
            None
        };

        let selectivity = filter.as_ref().map(|f| f.selectivity).unwrap_or(1.0);

        Ok(PlanNode {
            operation: PlanOperation::TableScan {
                table: table.to_string(),
                filter,
                projection: vec![], // All columns for now
            },
            children: vec![],
            cost: 0.0, // Will be calculated later
            cardinality: (cardinality as f64 * selectivity) as u64,
            selectivity,
            resources: ResourceRequirements {
                memory_bytes: (cardinality * 100) as u64, // Rough estimate
                cpu_cycles: (cardinality * 10) as u64,
                io_operations: (cardinality / 1000) as u64, // Assume 1000 rows per I/O
                network_operations: 0,
            },
        })
    }

    /// Create join node
    fn create_join_node(
        &self,
        left: PlanNode,
        right: PlanNode,
        conditions: &[WhereCondition],
    ) -> Result<PlanNode> {
        let join_type = self.choose_join_algorithm(&left, &right)?;

        let estimated_cardinality = self.estimate_join_cardinality(&left, &right, conditions);

        let operation = match join_type {
            JoinAlgorithm::Hash => PlanOperation::HashJoin {
                join_type: JoinType::Inner,  // Simplified
                left_key: "id".to_string(),  // Simplified
                right_key: "id".to_string(), // Simplified
                conditions: conditions.to_vec(),
            },
            JoinAlgorithm::NestedLoop => PlanOperation::NestedLoopJoin {
                join_type: JoinType::Inner,
                conditions: conditions.to_vec(),
            },
        };

        Ok(PlanNode {
            operation,
            children: vec![left, right],
            cost: 0.0,
            cardinality: estimated_cardinality,
            selectivity: 1.0,
            resources: ResourceRequirements {
                memory_bytes: estimated_cardinality * 200,
                cpu_cycles: estimated_cardinality * 20,
                io_operations: 0,
                network_operations: 0,
            },
        })
    }

    /// Choose optimal join algorithm
    fn choose_join_algorithm(&self, left: &PlanNode, right: &PlanNode) -> Result<JoinAlgorithm> {
        match self.config.join_strategy {
            JoinStrategy::HashJoin => Ok(JoinAlgorithm::Hash),
            JoinStrategy::NestedLoop => Ok(JoinAlgorithm::NestedLoop),
            JoinStrategy::CostBased => {
                // Use hash join for larger datasets, nested loop for smaller
                if left.cardinality > 1000 || right.cardinality > 1000 {
                    Ok(JoinAlgorithm::Hash)
                } else {
                    Ok(JoinAlgorithm::NestedLoop)
                }
            }
            JoinStrategy::Adaptive => {
                // TODO: Use execution history to choose
                Ok(JoinAlgorithm::Hash)
            }
        }
    }

    /// Estimate join cardinality
    fn estimate_join_cardinality(
        &self,
        left: &PlanNode,
        right: &PlanNode,
        _conditions: &[WhereCondition],
    ) -> u64 {
        // Simplified estimation - in practice would use detailed statistics
        (left.cardinality * right.cardinality) / 10 // Assume 10% selectivity
    }

    /// Create filter expression from conditions
    fn create_filter_expression(&self, conditions: &[WhereCondition]) -> Result<FilterExpression> {
        let predicates: Vec<Predicate> = conditions
            .iter()
            .map(|cond| {
                let selectivity = self.estimate_predicate_selectivity(cond);
                Predicate {
                    column: cond.column.clone(),
                    operator: cond.operator.clone(),
                    value: cond.value.clone(),
                    selectivity,
                }
            })
            .collect();

        // Combined selectivity (assuming independence)
        let combined_selectivity = predicates
            .iter()
            .map(|p| p.selectivity)
            .fold(1.0, |acc, sel| acc * sel);

        Ok(FilterExpression {
            predicates,
            selectivity: combined_selectivity,
        })
    }

    /// Estimate predicate selectivity
    fn estimate_predicate_selectivity(&self, condition: &WhereCondition) -> f64 {
        match condition.operator.as_str() {
            "=" => 0.1,         // 10% selectivity for equality
            ">" | "<" => 0.33,  // 33% selectivity for range
            ">=" | "<=" => 0.5, // 50% selectivity for inclusive range
            "LIKE" => 0.25,     // 25% selectivity for LIKE
            _ => 0.5,           // Default 50% selectivity
        }
    }

    /// Calculate total cost for a plan
    #[allow(clippy::only_used_in_recursion)]
    fn calculate_plan_cost(&self, node: &PlanNode, query: &OptimizableQuery) -> Result<f64> {
        let mut total_cost = 0.0;

        // Cost for this node
        total_cost += self.calculate_node_cost(node)?;

        // Cost for children
        for child in &node.children {
            total_cost += self.calculate_plan_cost(child, query)?;
        }

        Ok(total_cost)
    }

    /// Calculate cost for individual node
    fn calculate_node_cost(&self, node: &PlanNode) -> Result<f64> {
        match &node.operation {
            PlanOperation::TableScan { .. } => {
                Ok(node.cardinality as f64 * self.config.cost_model.seq_scan_cost_per_row)
            }
            PlanOperation::IndexScan { .. } => {
                Ok(node.cardinality as f64 * self.config.cost_model.index_lookup_cost)
            }
            PlanOperation::HashJoin { .. } => {
                Ok(node.cardinality as f64 * self.config.cost_model.hash_join_cost_per_probe)
            }
            PlanOperation::NestedLoopJoin { .. } => {
                Ok(node.cardinality as f64 * self.config.cost_model.nested_loop_cost_per_iteration)
            }
            PlanOperation::Sort { .. } => {
                let sort_cost = node.cardinality as f64 * self.config.cost_model.sort_cost_per_row;
                // Add O(n log n) complexity for sorting
                Ok(sort_cost * (node.cardinality as f64).log2())
            }
            _ => Ok(node.cardinality as f64), // Base cost
        }
    }

    /// Estimate execution time
    fn estimate_execution_time(&self, node: &PlanNode) -> Result<u64> {
        // Simplified estimation based on cardinality and operation type
        let base_time = match &node.operation {
            PlanOperation::TableScan { .. } => node.cardinality / 1000, // 1ms per 1000 rows
            PlanOperation::IndexScan { .. } => node.cardinality / 10000, // Faster with indexes
            PlanOperation::HashJoin { .. } => node.cardinality / 500,
            PlanOperation::Sort { .. } => node.cardinality / 200,
            _ => node.cardinality / 1000,
        };

        Ok(base_time.max(1))
    }

    /// Estimate memory usage
    fn estimate_memory_usage(&self, node: &PlanNode) -> Result<u64> {
        Ok(node.resources.memory_bytes)
    }

    /// Generate cache key for a query
    fn generate_cache_key(&self, query: &OptimizableQuery) -> String {
        // Simple hash of query structure
        format!(
            "{}_{}_{}_{}",
            query.tables.join(","),
            query.where_conditions.len(),
            query.join_conditions.len(),
            query.group_by.len()
        )
    }

    /// Get cached plan if available
    fn get_cached_plan(&self, key: &str) -> Option<CachedPlan> {
        let cache = self.plan_cache.read();
        cache.get(key).cloned()
    }

    /// Cache execution plan
    fn cache_plan(&self, key: String, plan: ExecutionPlan) {
        let mut cache = self.plan_cache.write();
        cache.insert(
            key,
            CachedPlan {
                plan,
                cached_at: SystemTime::now(),
                hit_count: 0,
                execution_stats: Vec::new(),
            },
        );
    }

    /// Create filter node
    fn create_filter_node(
        &self,
        child: PlanNode,
        conditions: &[WhereCondition],
    ) -> Result<PlanNode> {
        let filter_expr = self.create_filter_expression(conditions)?;
        let cardinality = (child.cardinality as f64 * filter_expr.selectivity) as u64;

        Ok(PlanNode {
            operation: PlanOperation::Filter {
                expression: filter_expr.clone(),
            },
            children: vec![child.clone()],
            cost: 0.0,
            cardinality,
            selectivity: filter_expr.selectivity,
            resources: ResourceRequirements {
                memory_bytes: cardinality * 50,
                cpu_cycles: cardinality * 5,
                io_operations: 0,
                network_operations: 0,
            },
        })
    }

    /// Create aggregate node
    fn create_aggregate_node(
        &self,
        child: PlanNode,
        group_by: &[String],
        aggregates: &[AggregateFunction],
    ) -> Result<PlanNode> {
        let cardinality = if group_by.is_empty() {
            1 // Single row for global aggregation
        } else {
            child.cardinality / 10 // Assume 10% unique groups
        };

        Ok(PlanNode {
            operation: PlanOperation::Aggregate {
                group_by: group_by.to_vec(),
                aggregates: aggregates.to_vec(),
            },
            children: vec![child],
            cost: 0.0,
            cardinality,
            selectivity: 1.0,
            resources: ResourceRequirements {
                memory_bytes: cardinality * 100,
                cpu_cycles: cardinality * 15,
                io_operations: 0,
                network_operations: 0,
            },
        })
    }

    /// Create sort node
    fn create_sort_node(&self, child: PlanNode, columns: &[SortColumn]) -> Result<PlanNode> {
        Ok(PlanNode {
            operation: PlanOperation::Sort {
                columns: columns.to_vec(),
                limit: None,
            },
            children: vec![child.clone()],
            cost: 0.0,
            cardinality: child.cardinality,
            selectivity: 1.0,
            resources: ResourceRequirements {
                memory_bytes: child.cardinality * 150, // Additional memory for sorting
                cpu_cycles: child.cardinality * 25,
                io_operations: 0,
                network_operations: 0,
            },
        })
    }

    /// Create project node
    fn create_project_node(&self, child: PlanNode, columns: &[String]) -> Result<PlanNode> {
        Ok(PlanNode {
            operation: PlanOperation::Project {
                columns: columns.to_vec(),
            },
            children: vec![child.clone()],
            cost: 0.0,
            cardinality: child.cardinality,
            selectivity: 1.0,
            resources: ResourceRequirements {
                memory_bytes: child.cardinality * 80, // Reduced memory after projection
                cpu_cycles: child.cardinality * 2,
                io_operations: 0,
                network_operations: 0,
            },
        })
    }

    /// Create limit node
    fn create_limit_node(
        &self,
        child: PlanNode,
        count: usize,
        offset: Option<usize>,
    ) -> Result<PlanNode> {
        let cardinality = (count as u64).min(child.cardinality);

        Ok(PlanNode {
            operation: PlanOperation::Limit { count, offset },
            children: vec![child.clone()],
            cost: 0.0,
            cardinality,
            selectivity: cardinality as f64 / child.cardinality as f64,
            resources: ResourceRequirements {
                memory_bytes: cardinality * 100,
                cpu_cycles: cardinality,
                io_operations: 0,
                network_operations: 0,
            },
        })
    }
}

/// Join algorithm types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinAlgorithm {
    Hash,
    NestedLoop,
}

/// Optimizable query representation
#[derive(Debug, Clone)]
pub struct OptimizableQuery {
    pub tables: Vec<String>,
    pub select_columns: Vec<String>,
    pub where_conditions: Vec<WhereCondition>,
    pub join_conditions: Vec<WhereCondition>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateFunction>,
    pub order_by: Vec<SortColumn>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub as_of: Option<AsOf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct MockStatsProvider;

    impl StatisticsProvider for MockStatsProvider {
        fn get_table_stats(&self, _table: &str) -> Option<TableStatistics> {
            Some(TableStatistics {
                table_name: "test".to_string(),
                row_count: 1000,
                column_count: 5,
                avg_row_size: 100,
                total_size_bytes: 100000,
                data_size_bytes: 100000,
                column_stats: HashMap::new(),
                column_statistics: HashMap::new(),
                index_stats: HashMap::new(),
                last_updated: 0,
                collection_method: "TEST".to_string(),
                collection_duration_ms: 0,
            })
        }

        fn get_column_stats(&self, _table: &str, _column: &str) -> Option<ColumnStatistics> {
            None
        }

        fn get_index_stats(&self, _index: &str) -> Option<IndexStatistics> {
            None
        }

        fn get_query_history(&self) -> Vec<QueryExecution> {
            Vec::new()
        }
    }

    #[test]
    fn test_query_optimization() {
        let stats_provider = Arc::new(MockStatsProvider);
        let optimizer = QueryOptimizer::new(OptimizerConfig::default(), stats_provider);

        let query = OptimizableQuery {
            tables: vec!["users".to_string()],
            select_columns: vec!["id".to_string(), "name".to_string()],
            where_conditions: vec![],
            join_conditions: vec![],
            group_by: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: Some(10),
            offset: None,
            as_of: None,
        };

        let plan = optimizer.optimize_query(&query).unwrap();
        assert!(plan.estimated_cost > 0.0);
        assert!(plan.estimated_time_ms > 0);
    }

    #[test]
    fn test_cost_calculation() {
        let stats_provider = Arc::new(MockStatsProvider);
        let optimizer = QueryOptimizer::new(OptimizerConfig::default(), stats_provider);

        let node = PlanNode {
            operation: PlanOperation::TableScan {
                table: "test".to_string(),
                filter: None,
                projection: vec![],
            },
            children: vec![],
            cost: 0.0,
            cardinality: 1000,
            selectivity: 1.0,
            resources: ResourceRequirements {
                memory_bytes: 100000,
                cpu_cycles: 10000,
                io_operations: 10,
                network_operations: 0,
            },
        };

        let cost = optimizer.calculate_node_cost(&node).unwrap();
        assert!(cost > 0.0);
    }
}
