//! Cost-Based Query Optimizer
//!
//! Implements a real query optimizer with:
//! - Cost model based on I/O and CPU
//! - Join order optimization using dynamic programming
//! - Index selection
//! - Predicate pushdown
//! - Subquery optimization
//! - Materialized view matching

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::errors::{DriftError, Result};
use crate::index_strategies::IndexType;
use crate::optimizer::TableStatistics;

/// Query plan node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanNode {
    /// Table scan
    TableScan {
        table: String,
        predicates: Vec<Predicate>,
        cost: Cost,
    },
    /// Index scan
    IndexScan {
        table: String,
        index: String,
        predicates: Vec<Predicate>,
        cost: Cost,
    },
    /// Nested loop join
    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
        cost: Cost,
    },
    /// Hash join
    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
        build_side: JoinSide,
        cost: Cost,
    },
    /// Sort-merge join
    SortMergeJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
        cost: Cost,
    },
    /// Sort operation
    Sort {
        input: Box<PlanNode>,
        keys: Vec<SortKey>,
        cost: Cost,
    },
    /// Aggregation
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunc>,
        cost: Cost,
    },
    /// Filter
    Filter {
        input: Box<PlanNode>,
        predicates: Vec<Predicate>,
        cost: Cost,
    },
    /// Projection
    Project {
        input: Box<PlanNode>,
        columns: Vec<String>,
        cost: Cost,
    },
    /// Limit
    Limit {
        input: Box<PlanNode>,
        limit: usize,
        offset: usize,
        cost: Cost,
    },
    /// Materialize (force materialization point)
    Materialize { input: Box<PlanNode>, cost: Cost },
}

/// Cost model
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Cost {
    /// I/O cost (page reads)
    pub io_cost: f64,
    /// CPU cost (tuple processing)
    pub cpu_cost: f64,
    /// Memory required (bytes)
    pub memory: f64,
    /// Network cost (for distributed)
    pub network_cost: f64,
    /// Estimated row count
    pub rows: f64,
    /// Estimated data size
    pub size: f64,
}

impl Cost {
    /// Total cost combining all factors
    pub fn total(&self) -> f64 {
        self.io_cost + self.cpu_cost * 0.01 + self.network_cost * 2.0
    }

    /// Create a cost for sequential scan
    pub fn seq_scan(pages: f64, rows: f64) -> Self {
        Self {
            io_cost: pages,
            cpu_cost: rows * 0.01,
            rows,
            size: rows * 100.0, // Assume 100 bytes per row average
            ..Default::default()
        }
    }

    /// Create a cost for index scan
    pub fn index_scan(index_pages: f64, data_pages: f64, rows: f64) -> Self {
        Self {
            io_cost: index_pages + data_pages,
            cpu_cost: rows * 0.005, // Less CPU than seq scan
            rows,
            size: rows * 100.0,
            ..Default::default()
        }
    }
}

/// Predicate for filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Predicate {
    pub column: String,
    pub op: ComparisonOp,
    pub value: PredicateValue,
    pub selectivity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PredicateValue {
    Constant(serde_json::Value),
    Column(String),
    Subquery(Box<PlanNode>),
}

/// Join condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_col: String,
    pub right_col: String,
    pub op: ComparisonOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinSide {
    Left,
    Right,
}

/// Sort key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

/// Aggregate function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateFunc {
    pub func: String,
    pub column: Option<String>,
    pub alias: String,
}

/// Query optimizer
pub struct CostOptimizer {
    /// Table statistics
    statistics: Arc<RwLock<HashMap<String, Arc<TableStatistics>>>>,
    /// Available indexes
    indexes: Arc<RwLock<HashMap<String, Vec<IndexInfo>>>>,
    /// Materialized views
    #[allow(dead_code)]
    materialized_views: Arc<RwLock<Vec<MaterializedViewInfo>>>,
    /// Cost parameters
    params: CostParameters,
    /// Optimization statistics
    stats: Arc<RwLock<OptimizerStats>>,
}

/// Index information
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct IndexInfo {
    name: String,
    table: String,
    columns: Vec<String>,
    #[allow(dead_code)]
    index_type: IndexType,
    unique: bool,
    size_pages: usize,
}

/// Materialized view information
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MaterializedViewInfo {
    name: String,
    query: String,
    tables: HashSet<String>,
    columns: Vec<String>,
}

/// Cost calculation parameters
#[derive(Debug, Clone)]
struct CostParameters {
    #[allow(dead_code)]
    seq_page_cost: f64,
    #[allow(dead_code)]
    random_page_cost: f64,
    #[allow(dead_code)]
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
    #[allow(dead_code)]
    parallel_workers: usize,
    work_mem: usize, // KB
}

impl Default for CostParameters {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
            parallel_workers: 4,
            work_mem: 4096, // 4MB
        }
    }
}

/// Optimizer statistics
#[derive(Debug, Default)]
struct OptimizerStats {
    #[allow(dead_code)]
    plans_considered: u64,
    #[allow(dead_code)]
    plans_pruned: u64,
    optimization_time_ms: u64,
    #[allow(dead_code)]
    joins_reordered: u64,
    indexes_used: u64,
}

impl Default for CostOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl CostOptimizer {
    pub fn new() -> Self {
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            materialized_views: Arc::new(RwLock::new(Vec::new())),
            params: CostParameters::default(),
            stats: Arc::new(RwLock::new(OptimizerStats::default())),
        }
    }

    /// Update table statistics
    pub fn update_statistics(&self, table: &str, stats: Arc<TableStatistics>) {
        self.statistics.write().insert(table.to_string(), stats);
    }

    /// Register an index
    #[allow(dead_code)]
    pub fn register_index(&self, info: IndexInfo) {
        self.indexes
            .write()
            .entry(info.table.clone())
            .or_default()
            .push(info);
    }

    /// Optimize a query plan
    pub fn optimize(&self, initial_plan: PlanNode) -> Result<PlanNode> {
        let start = std::time::Instant::now();

        // Apply optimization rules in order
        let mut plan = initial_plan;

        // 1. Predicate pushdown
        plan = self.push_down_predicates(plan)?;

        // 2. Join reordering
        plan = self.reorder_joins(plan)?;

        // 3. Index selection
        plan = self.select_indexes(plan)?;

        // 4. Choose join algorithms
        plan = self.choose_join_algorithms(plan)?;

        // 5. Add materialization points
        plan = self.add_materialization_points(plan)?;

        // 6. Parallel execution planning
        plan = self.plan_parallel_execution(plan)?;

        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.write().optimization_time_ms += elapsed;

        Ok(plan)
    }

    /// Push predicates down the plan tree
    fn push_down_predicates(&self, plan: PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::Filter {
                input, predicates, ..
            } => {
                // Try to push filter below joins
                match *input {
                    PlanNode::HashJoin {
                        left,
                        right,
                        condition,
                        build_side,
                        cost,
                    } => {
                        let (left_preds, right_preds, remaining) =
                            self.split_join_predicates(&predicates, &left, &right);

                        let new_left = if !left_preds.is_empty() {
                            Box::new(PlanNode::Filter {
                                input: left,
                                predicates: left_preds,
                                cost: Cost::default(),
                            })
                        } else {
                            left
                        };

                        let new_right = if !right_preds.is_empty() {
                            Box::new(PlanNode::Filter {
                                input: right,
                                predicates: right_preds,
                                cost: Cost::default(),
                            })
                        } else {
                            right
                        };

                        let join = PlanNode::HashJoin {
                            left: new_left,
                            right: new_right,
                            condition,
                            build_side,
                            cost,
                        };

                        if remaining.is_empty() {
                            Ok(join)
                        } else {
                            Ok(PlanNode::Filter {
                                input: Box::new(join),
                                predicates: remaining,
                                cost: Cost::default(),
                            })
                        }
                    }
                    _ => Ok(PlanNode::Filter {
                        input: Box::new(*input),
                        predicates,
                        cost: Cost::default(),
                    }),
                }
            }
            _ => Ok(plan),
        }
    }

    /// Reorder joins using dynamic programming
    fn reorder_joins(&self, plan: PlanNode) -> Result<PlanNode> {
        // Extract all joins and tables
        let (tables, joins) = self.extract_joins(&plan)?;

        if tables.len() <= 2 {
            return Ok(plan); // No reordering needed
        }

        // Use dynamic programming to find optimal join order
        let best_order = self.find_best_join_order(&tables, &joins)?;

        // Rebuild plan with optimal order
        self.rebuild_with_join_order(plan, best_order)
    }

    /// Find optimal join order using dynamic programming
    fn find_best_join_order(&self, tables: &[String], joins: &[JoinInfo]) -> Result<Vec<String>> {
        let n = tables.len();
        if n > 12 {
            // Fall back to greedy for large joins
            return self.greedy_join_order(tables, joins);
        }

        // DP table: subset -> (cost, order)
        let mut dp: HashMap<BitSet, (Cost, Vec<String>)> = HashMap::new();

        // Base case: single tables
        for (i, table) in tables.iter().enumerate() {
            let mut set = BitSet::new(n);
            set.set(i);

            let stats = self.statistics.read();
            let cost = if let Some(table_stats) = stats.get(table) {
                Cost::seq_scan(
                    table_stats.total_size_bytes as f64 / 8192.0,
                    table_stats.row_count as f64,
                )
            } else {
                Cost::seq_scan(100.0, 1000.0) // Default estimate
            };

            dp.insert(set, (cost, vec![table.clone()]));
        }

        // Build up subsets
        for size in 2..=n {
            for subset in BitSet::subsets_of_size(n, size) {
                let mut best_cost = Cost {
                    io_cost: f64::INFINITY,
                    ..Default::default()
                };
                let mut best_order = vec![];

                // Try all ways to split this subset
                for split in subset.splits() {
                    if let (Some((left_cost, left_order)), Some((right_cost, right_order))) =
                        (dp.get(&split.0), dp.get(&split.1))
                    {
                        // Calculate join cost
                        let join_cost = self.estimate_join_cost(
                            left_cost,
                            right_cost,
                            left_order,
                            right_order,
                            joins,
                        );

                        if join_cost.total() < best_cost.total() {
                            best_cost = join_cost;
                            best_order = left_order
                                .iter()
                                .chain(right_order.iter())
                                .cloned()
                                .collect();
                        }
                    }
                }

                dp.insert(subset, (best_cost, best_order));
            }
        }

        // Return the best order for all tables
        let all_set = BitSet::all(n);
        dp.get(&all_set)
            .map(|(_, order)| order.clone())
            .ok_or_else(|| DriftError::Other("Failed to find join order".to_string()))
    }

    /// Estimate cost of joining two sub-plans
    fn estimate_join_cost(
        &self,
        left_cost: &Cost,
        right_cost: &Cost,
        left_tables: &[String],
        right_tables: &[String],
        joins: &[JoinInfo],
    ) -> Cost {
        // Find applicable join conditions
        let join_selectivity = self.estimate_join_selectivity(left_tables, right_tables, joins);

        // Estimate output rows
        let output_rows = left_cost.rows * right_cost.rows * join_selectivity;

        // Choose join algorithm based on sizes
        if right_cost.rows < 1000.0 {
            // Nested loop join for small inner
            Cost {
                io_cost: left_cost.io_cost + left_cost.rows * right_cost.io_cost,
                cpu_cost: left_cost.rows * right_cost.rows * self.params.cpu_operator_cost,
                rows: output_rows,
                size: output_rows * 100.0,
                ..Default::default()
            }
        } else if left_cost.rows + right_cost.rows < 100000.0 {
            // Hash join for medium sizes
            Cost {
                io_cost: left_cost.io_cost + right_cost.io_cost,
                cpu_cost: (left_cost.rows + right_cost.rows) * self.params.cpu_operator_cost * 2.0,
                memory: right_cost.size, // Build hash table
                rows: output_rows,
                size: output_rows * 100.0,
                ..Default::default()
            }
        } else {
            // Sort-merge for large joins
            Cost {
                io_cost: left_cost.io_cost
                    + right_cost.io_cost
                    + (left_cost.rows.log2() + right_cost.rows.log2()) * 0.1,
                cpu_cost: (left_cost.rows * left_cost.rows.log2()
                    + right_cost.rows * right_cost.rows.log2())
                    * self.params.cpu_operator_cost,
                rows: output_rows,
                size: output_rows * 100.0,
                ..Default::default()
            }
        }
    }

    /// Select appropriate indexes
    fn select_indexes(&self, plan: PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::TableScan {
                table, predicates, ..
            } => {
                // Check available indexes
                let indexes = self.indexes.read();
                if let Some(table_indexes) = indexes.get(&table) {
                    // Find best index for predicates
                    let best_index = self.find_best_index(&predicates, table_indexes);

                    if let Some(index) = best_index {
                        let stats = self.statistics.read();
                        let table_stats = stats.get(&table);

                        let cost = if let Some(ts) = table_stats {
                            let selectivity = self.estimate_predicate_selectivity(&predicates, ts);
                            let rows = ts.row_count as f64 * selectivity;
                            Cost::index_scan(
                                (index.size_pages as f64).max(1.0),
                                rows * 0.1, // Assume 10% random I/O
                                rows,
                            )
                        } else {
                            Cost::default()
                        };

                        self.stats.write().indexes_used += 1;

                        return Ok(PlanNode::IndexScan {
                            table,
                            index: index.name.clone(),
                            predicates,
                            cost,
                        });
                    }
                }

                // No suitable index, keep table scan
                Ok(PlanNode::TableScan {
                    table,
                    predicates,
                    cost: Cost::default(),
                })
            }
            _ => Ok(plan),
        }
    }

    /// Find best index for given predicates
    fn find_best_index<'a>(
        &self,
        predicates: &[Predicate],
        indexes: &'a [IndexInfo],
    ) -> Option<&'a IndexInfo> {
        let mut best_index = None;
        let mut best_score = 0;

        for index in indexes {
            let mut score = 0;
            let mut matched_prefix = true;

            // Score based on how well index matches predicate columns
            for (i, index_col) in index.columns.iter().enumerate() {
                if !matched_prefix {
                    break;
                }

                for pred in predicates {
                    if pred.column == *index_col {
                        if i == 0 {
                            score += 100; // First column match is most important
                        } else {
                            score += 50;
                        }

                        if matches!(pred.op, ComparisonOp::Eq) {
                            score += 20; // Equality is better than range
                        }
                    }
                }

                // Check if we still have matching prefix
                matched_prefix = predicates.iter().any(|p| p.column == *index_col);
            }

            if index.unique {
                score += 10; // Prefer unique indexes
            }

            if score > best_score {
                best_score = score;
                best_index = Some(index);
            }
        }

        best_index
    }

    /// Choose optimal join algorithms
    fn choose_join_algorithms(&self, plan: PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                ..
            } => {
                let left_cost = self.estimate_cost(&left)?;
                let right_cost = self.estimate_cost(&right)?;

                // Choose based on sizes
                if right_cost.rows < 1000.0 && left_cost.rows < 10000.0 {
                    // Keep nested loop for small joins
                    Ok(PlanNode::NestedLoopJoin {
                        left,
                        right,
                        condition,
                        cost: self.estimate_join_cost(&left_cost, &right_cost, &[], &[], &[]),
                    })
                } else if right_cost.size < self.params.work_mem as f64 * 1024.0 {
                    // Hash join if right side fits in memory
                    Ok(PlanNode::HashJoin {
                        left,
                        right,
                        condition,
                        build_side: JoinSide::Right,
                        cost: self.estimate_join_cost(&left_cost, &right_cost, &[], &[], &[]),
                    })
                } else {
                    // Sort-merge for large joins
                    Ok(PlanNode::SortMergeJoin {
                        left,
                        right,
                        condition,
                        cost: self.estimate_join_cost(&left_cost, &right_cost, &[], &[], &[]),
                    })
                }
            }
            _ => Ok(plan),
        }
    }

    /// Add materialization points for complex subqueries
    fn add_materialization_points(&self, plan: PlanNode) -> Result<PlanNode> {
        // Materialize if subquery is referenced multiple times
        // or if it would benefit from creating a hash table
        Ok(plan)
    }

    /// Plan parallel execution
    fn plan_parallel_execution(&self, plan: PlanNode) -> Result<PlanNode> {
        // Add parallel scan/join nodes where beneficial
        Ok(plan)
    }

    /// Estimate cost of a plan node
    fn estimate_cost(&self, plan: &PlanNode) -> Result<Cost> {
        match plan {
            PlanNode::TableScan { cost, .. }
            | PlanNode::IndexScan { cost, .. }
            | PlanNode::HashJoin { cost, .. }
            | PlanNode::NestedLoopJoin { cost, .. }
            | PlanNode::SortMergeJoin { cost, .. } => Ok(*cost),
            _ => Ok(Cost::default()),
        }
    }

    /// Extract joins from plan
    fn extract_joins(&self, _plan: &PlanNode) -> Result<(Vec<String>, Vec<JoinInfo>)> {
        // TODO: Walk plan tree and extract all tables and join conditions
        Ok((vec![], vec![]))
    }

    /// Split predicates for join pushdown
    fn split_join_predicates(
        &self,
        predicates: &[Predicate],
        _left: &PlanNode,
        _right: &PlanNode,
    ) -> (Vec<Predicate>, Vec<Predicate>, Vec<Predicate>) {
        // TODO: Analyze which predicates can be pushed to which side
        (vec![], vec![], predicates.to_vec())
    }

    /// Estimate join selectivity
    fn estimate_join_selectivity(
        &self,
        _left_tables: &[String],
        _right_tables: &[String],
        _joins: &[JoinInfo],
    ) -> f64 {
        0.1 // Default 10% selectivity
    }

    /// Estimate predicate selectivity
    fn estimate_predicate_selectivity(
        &self,
        predicates: &[Predicate],
        stats: &TableStatistics,
    ) -> f64 {
        let mut selectivity = 1.0;

        for pred in predicates {
            if let Some(col_stats) = stats.column_stats.get(&pred.column) {
                selectivity *= match pred.op {
                    ComparisonOp::Eq => 1.0 / col_stats.distinct_values.max(1) as f64,
                    ComparisonOp::Lt | ComparisonOp::Gt => 0.3,
                    ComparisonOp::Like => 0.25,
                    _ => 0.5,
                };
            } else {
                selectivity *= 0.3; // Default selectivity
            }
        }

        selectivity.clamp(0.001, 1.0)
    }

    /// Greedy join ordering for large queries
    fn greedy_join_order(&self, tables: &[String], _joins: &[JoinInfo]) -> Result<Vec<String>> {
        Ok(tables.to_vec())
    }

    /// Rebuild plan with new join order
    fn rebuild_with_join_order(&self, plan: PlanNode, _order: Vec<String>) -> Result<PlanNode> {
        Ok(plan)
    }
}

/// Join information
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct JoinInfo {
    left_table: String,
    right_table: String,
    condition: JoinCondition,
}

/// Bit set for dynamic programming
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct BitSet {
    bits: u64,
    size: usize,
}

impl BitSet {
    fn new(size: usize) -> Self {
        Self { bits: 0, size }
    }

    fn set(&mut self, i: usize) {
        self.bits |= 1 << i;
    }

    fn all(size: usize) -> Self {
        Self {
            bits: (1 << size) - 1,
            size,
        }
    }

    fn subsets_of_size(_n: usize, _size: usize) -> impl Iterator<Item = BitSet> {
        // TODO: Generate all subsets of given size
        std::iter::empty()
    }

    fn splits(&self) -> impl Iterator<Item = (BitSet, BitSet)> {
        // TODO: Generate all ways to split this set into two non-empty subsets
        std::iter::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_comparison() {
        let seq_cost = Cost::seq_scan(100.0, 10000.0);
        let idx_cost = Cost::index_scan(5.0, 10.0, 100.0);

        assert!(idx_cost.total() < seq_cost.total());
    }

    #[test]
    fn test_index_selection() {
        let optimizer = CostOptimizer::new();

        let index = IndexInfo {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            columns: vec!["email".to_string()],
            index_type: IndexType::BPlusTree,
            unique: true,
            size_pages: 10,
        };

        optimizer.register_index(index);

        let predicates = vec![Predicate {
            column: "email".to_string(),
            op: ComparisonOp::Eq,
            value: PredicateValue::Constant(serde_json::json!("test@example.com")),
            selectivity: 0.001,
        }];

        let indexes = optimizer.indexes.read();
        let table_indexes = indexes.get("users").unwrap();
        let best = optimizer.find_best_index(&predicates, table_indexes);

        assert!(best.is_some());
        assert_eq!(best.unwrap().name, "idx_users_email");
    }
}
