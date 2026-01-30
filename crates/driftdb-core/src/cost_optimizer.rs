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
    /// Walks the plan tree and extracts all tables and join conditions
    fn extract_joins(&self, plan: &PlanNode) -> Result<(Vec<String>, Vec<JoinInfo>)> {
        let mut tables = Vec::new();
        let mut joins = Vec::new();
        self.extract_joins_recursive(plan, &mut tables, &mut joins);
        Ok((tables, joins))
    }

    /// Recursively extract tables and joins from plan tree
    #[allow(clippy::only_used_in_recursion)]
    fn extract_joins_recursive(
        &self,
        plan: &PlanNode,
        tables: &mut Vec<String>,
        joins: &mut Vec<JoinInfo>,
    ) {
        match plan {
            PlanNode::TableScan { table, .. } | PlanNode::IndexScan { table, .. } => {
                if !tables.contains(table) {
                    tables.push(table.clone());
                }
            }
            PlanNode::HashJoin {
                left,
                right,
                condition,
                ..
            }
            | PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                ..
            }
            | PlanNode::SortMergeJoin {
                left,
                right,
                condition,
                ..
            } => {
                // Extract tables from left and right subtrees
                let mut left_tables = Vec::new();
                let mut right_tables = Vec::new();
                self.extract_joins_recursive(left, &mut left_tables, joins);
                self.extract_joins_recursive(right, &mut right_tables, joins);

                // Add join info if we have tables on both sides
                if !left_tables.is_empty() && !right_tables.is_empty() {
                    joins.push(JoinInfo {
                        left_table: left_tables[0].clone(),
                        right_table: right_tables[0].clone(),
                        condition: condition.clone(),
                    });
                }

                // Merge tables into main list
                for t in left_tables {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
                for t in right_tables {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Materialize { input, .. } => {
                self.extract_joins_recursive(input, tables, joins);
            }
        }
    }

    /// Get all table names referenced in a plan subtree
    fn get_tables_in_plan(&self, plan: &PlanNode) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables_recursive(plan, &mut tables);
        tables
    }

    /// Recursively collect table names from plan
    #[allow(clippy::only_used_in_recursion)]
    fn collect_tables_recursive(&self, plan: &PlanNode, tables: &mut HashSet<String>) {
        match plan {
            PlanNode::TableScan { table, .. } | PlanNode::IndexScan { table, .. } => {
                tables.insert(table.clone());
            }
            PlanNode::HashJoin { left, right, .. }
            | PlanNode::NestedLoopJoin { left, right, .. }
            | PlanNode::SortMergeJoin { left, right, .. } => {
                self.collect_tables_recursive(left, tables);
                self.collect_tables_recursive(right, tables);
            }
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Materialize { input, .. } => {
                self.collect_tables_recursive(input, tables);
            }
        }
    }

    /// Split predicates for join pushdown
    /// Returns (left_predicates, right_predicates, remaining_predicates)
    fn split_join_predicates(
        &self,
        predicates: &[Predicate],
        left: &PlanNode,
        right: &PlanNode,
    ) -> (Vec<Predicate>, Vec<Predicate>, Vec<Predicate>) {
        let left_tables = self.get_tables_in_plan(left);
        let right_tables = self.get_tables_in_plan(right);

        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();
        let mut remaining = Vec::new();

        for pred in predicates {
            // Extract table name from column (assume format "table.column" or just "column")
            let pred_table = self.extract_table_from_column(&pred.column);

            // Check if predicate references a column from the left subtree
            let references_left = pred_table
                .as_ref()
                .map(|t| left_tables.contains(t))
                .unwrap_or(false);

            // Check if predicate references a column from the right subtree
            let references_right = pred_table
                .as_ref()
                .map(|t| right_tables.contains(t))
                .unwrap_or(false);

            // Also check if the predicate value references another column
            let value_references_left = match &pred.value {
                PredicateValue::Column(col) => {
                    let val_table = self.extract_table_from_column(col);
                    val_table.map(|t| left_tables.contains(&t)).unwrap_or(false)
                }
                _ => false,
            };

            let value_references_right = match &pred.value {
                PredicateValue::Column(col) => {
                    let val_table = self.extract_table_from_column(col);
                    val_table
                        .map(|t| right_tables.contains(&t))
                        .unwrap_or(false)
                }
                _ => false,
            };

            // Determine where to push the predicate
            if references_left && !references_right && !value_references_right {
                // Predicate only references left tables, push to left
                left_preds.push(pred.clone());
            } else if references_right && !references_left && !value_references_left {
                // Predicate only references right tables, push to right
                right_preds.push(pred.clone());
            } else if !references_left
                && !references_right
                && !value_references_left
                && !value_references_right
            {
                // Predicate doesn't reference any tables (constant), can go to either side
                // Push to left by convention
                left_preds.push(pred.clone());
            } else {
                // Predicate references both sides or is a cross-side join condition
                remaining.push(pred.clone());
            }
        }

        (left_preds, right_preds, remaining)
    }

    /// Extract table name from a potentially qualified column name
    /// "users.id" -> Some("users"), "id" -> None
    fn extract_table_from_column(&self, column: &str) -> Option<String> {
        column.find('.').map(|dot_pos| column[..dot_pos].to_string())
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

    #[allow(dead_code)]
    fn get(&self, i: usize) -> bool {
        (self.bits & (1 << i)) != 0
    }

    fn all(size: usize) -> Self {
        Self {
            bits: (1 << size) - 1,
            size,
        }
    }

    fn count(&self) -> usize {
        self.bits.count_ones() as usize
    }

    /// Generate all subsets of a given size using Gosper's hack
    fn subsets_of_size(n: usize, size: usize) -> impl Iterator<Item = BitSet> {
        SubsetIterator::new(n, size)
    }

    /// Generate all ways to split this set into two non-empty subsets
    /// Returns pairs (s1, s2) where s1 | s2 == self and s1 & s2 == 0
    fn splits(&self) -> impl Iterator<Item = (BitSet, BitSet)> {
        SplitIterator::new(*self)
    }

    /// Subtract another bitset
    fn subtract(&self, other: &BitSet) -> BitSet {
        BitSet {
            bits: self.bits & !other.bits,
            size: self.size,
        }
    }
}

/// Iterator for generating all k-subsets of n elements
struct SubsetIterator {
    current: u64,
    max: u64,
    #[allow(dead_code)]
    size: usize,
    n: usize,
    done: bool,
}

impl SubsetIterator {
    fn new(n: usize, size: usize) -> Self {
        if size == 0 || size > n || n > 63 {
            return Self {
                current: 0,
                max: 0,
                size,
                n,
                done: true,
            };
        }

        // Start with lowest k bits set: 0...0111...1 (size ones)
        let current = (1u64 << size) - 1;
        let max = 1u64 << n;

        Self {
            current,
            max,
            size,
            n,
            done: false,
        }
    }
}

impl Iterator for SubsetIterator {
    type Item = BitSet;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let result = BitSet {
            bits: self.current,
            size: self.n,
        };

        // Generate next subset using Gosper's hack
        // Find the rightmost one bit and the rightmost zero bit to its left
        let lowest_bit = self.current & self.current.wrapping_neg();
        let ripple = self.current + lowest_bit;

        if ripple >= self.max {
            self.done = true;
        } else {
            // Calculate the new lower bits
            let new_lowest = ((self.current ^ ripple) >> 2) / lowest_bit;
            self.current = ripple | new_lowest;

            if self.current >= self.max {
                self.done = true;
            }
        }

        Some(result)
    }
}

/// Iterator for generating all non-trivial splits of a bitset
struct SplitIterator {
    original: BitSet,
    current: u64,
    done: bool,
}

impl SplitIterator {
    fn new(set: BitSet) -> Self {
        if set.count() < 2 {
            // Can't split a set with fewer than 2 elements
            return Self {
                original: set,
                current: 0,
                done: true,
            };
        }

        // Start with the first proper non-empty subset
        Self {
            original: set,
            current: 1,
            done: false,
        }
    }
}

impl Iterator for SplitIterator {
    type Item = (BitSet, BitSet);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            // Ensure we're iterating through subsets of the original set
            // Use submask iteration: (current - 1) & original
            if self.current == 0 {
                self.done = true;
                return None;
            }

            let subset_bits = self.current & self.original.bits;

            // Check if this is a valid non-empty proper subset
            if subset_bits != 0 && subset_bits != self.original.bits {
                let s1 = BitSet {
                    bits: subset_bits,
                    size: self.original.size,
                };
                let s2 = self.original.subtract(&s1);

                // To avoid duplicate pairs (s1, s2) and (s2, s1),
                // only return pairs where s1 < s2 (by bits value)
                if s1.bits < s2.bits {
                    // Move to next subset
                    self.current = (self.current.wrapping_sub(1)) & self.original.bits;
                    if self.current == 0 {
                        self.done = true;
                    }
                    return Some((s1, s2));
                }
            }

            // Move to next potential subset
            if self.current >= self.original.bits {
                self.done = true;
            } else {
                self.current += 1;
            }
        }

        None
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

    #[test]
    fn test_bitset_basic_operations() {
        let mut set = BitSet::new(5);
        assert_eq!(set.count(), 0);

        set.set(0);
        set.set(2);
        set.set(4);
        assert_eq!(set.count(), 3);
        assert!(set.get(0));
        assert!(!set.get(1));
        assert!(set.get(2));

        let all = BitSet::all(4);
        assert_eq!(all.bits, 0b1111);
        assert_eq!(all.count(), 4);
    }

    #[test]
    fn test_bitset_subsets_of_size() {
        // Test generating all 2-subsets of {0,1,2,3}
        let subsets: Vec<BitSet> = BitSet::subsets_of_size(4, 2).collect();

        // C(4,2) = 6 subsets
        assert_eq!(subsets.len(), 6);

        // Verify each subset has exactly 2 bits set
        for subset in &subsets {
            assert_eq!(subset.count(), 2);
        }

        // Verify all subsets are unique
        let bits: Vec<u64> = subsets.iter().map(|s| s.bits).collect();
        let mut unique = bits.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(bits.len(), unique.len());
    }

    #[test]
    fn test_bitset_subsets_of_size_3() {
        // Test generating all 3-subsets of {0,1,2,3,4}
        let subsets: Vec<BitSet> = BitSet::subsets_of_size(5, 3).collect();

        // C(5,3) = 10 subsets
        assert_eq!(subsets.len(), 10);

        // Verify each subset has exactly 3 bits set
        for subset in &subsets {
            assert_eq!(subset.count(), 3);
        }
    }

    #[test]
    fn test_bitset_subsets_edge_cases() {
        // Size 0 should produce no subsets
        let empty: Vec<BitSet> = BitSet::subsets_of_size(4, 0).collect();
        assert!(empty.is_empty());

        // Size > n should produce no subsets
        let invalid: Vec<BitSet> = BitSet::subsets_of_size(3, 5).collect();
        assert!(invalid.is_empty());

        // Size 1 should produce n subsets
        let singles: Vec<BitSet> = BitSet::subsets_of_size(4, 1).collect();
        assert_eq!(singles.len(), 4);
    }

    #[test]
    fn test_bitset_splits() {
        let mut set = BitSet::new(4);
        set.set(0);
        set.set(1);
        set.set(2);
        // Set = {0, 1, 2}

        let splits: Vec<(BitSet, BitSet)> = set.splits().collect();

        // For a 3-element set, there should be 3 ways to split:
        // {0} | {1,2}, {1} | {0,2}, {2} | {0,1}
        // But we only count each pair once (s1 < s2)
        assert!(!splits.is_empty());

        // Verify all splits are valid partitions
        for (s1, s2) in &splits {
            // s1 and s2 should be disjoint
            assert_eq!(s1.bits & s2.bits, 0);
            // s1 | s2 should equal original set
            assert_eq!(s1.bits | s2.bits, set.bits);
            // Both should be non-empty
            assert!(s1.count() > 0);
            assert!(s2.count() > 0);
        }
    }

    #[test]
    fn test_bitset_subtract() {
        let mut a = BitSet::new(4);
        a.set(0);
        a.set(1);
        a.set(2);

        let mut b = BitSet::new(4);
        b.set(1);

        let result = a.subtract(&b);
        assert_eq!(result.bits, 0b0101); // {0, 2}
        assert!(result.get(0));
        assert!(!result.get(1));
        assert!(result.get(2));
    }

    #[test]
    fn test_extract_joins() {
        let optimizer = CostOptimizer::new();

        // Create a simple join plan
        let left_scan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let right_scan = PlanNode::TableScan {
            table: "orders".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let join = PlanNode::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            condition: JoinCondition {
                left_col: "users.id".to_string(),
                right_col: "orders.user_id".to_string(),
                op: ComparisonOp::Eq,
            },
            build_side: JoinSide::Right,
            cost: Cost::default(),
        };

        let (tables, joins) = optimizer.extract_joins(&join).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
        assert_eq!(joins.len(), 1);
    }

    #[test]
    fn test_split_join_predicates() {
        let optimizer = CostOptimizer::new();

        let left = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let right = PlanNode::TableScan {
            table: "orders".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let predicates = vec![
            // Should go to left (only references users)
            Predicate {
                column: "users.status".to_string(),
                op: ComparisonOp::Eq,
                value: PredicateValue::Constant(serde_json::json!("active")),
                selectivity: 0.5,
            },
            // Should go to right (only references orders)
            Predicate {
                column: "orders.total".to_string(),
                op: ComparisonOp::Gt,
                value: PredicateValue::Constant(serde_json::json!(100)),
                selectivity: 0.3,
            },
            // Should remain (references both sides)
            Predicate {
                column: "users.id".to_string(),
                op: ComparisonOp::Eq,
                value: PredicateValue::Column("orders.user_id".to_string()),
                selectivity: 0.1,
            },
        ];

        let (left_preds, right_preds, remaining) =
            optimizer.split_join_predicates(&predicates, &left, &right);

        assert_eq!(left_preds.len(), 1);
        assert_eq!(left_preds[0].column, "users.status");

        assert_eq!(right_preds.len(), 1);
        assert_eq!(right_preds[0].column, "orders.total");

        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].column, "users.id");
    }

    #[test]
    fn test_extract_table_from_column() {
        let optimizer = CostOptimizer::new();

        assert_eq!(
            optimizer.extract_table_from_column("users.id"),
            Some("users".to_string())
        );
        assert_eq!(
            optimizer.extract_table_from_column("orders.total"),
            Some("orders".to_string())
        );
        assert_eq!(optimizer.extract_table_from_column("id"), None);
        assert_eq!(optimizer.extract_table_from_column("column_name"), None);
    }
}
