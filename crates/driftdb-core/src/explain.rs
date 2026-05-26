//! EXPLAIN and EXPLAIN ANALYZE implementation
//!
//! Provides query execution plan visualization and analysis:
//! - EXPLAIN: Shows planned execution strategy with cost estimates
//! - EXPLAIN ANALYZE: Executes query and shows actual performance metrics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use crate::optimizer::{Cost, PlanNode};
use crate::errors::Result;

/// EXPLAIN output format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ExplainFormat {
    /// Human-readable text format
    #[default]
    Text,
    /// JSON format for programmatic consumption
    Json,
    /// YAML format
    Yaml,
    /// Tree-structured format
    Tree,
}

/// EXPLAIN options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainOptions {
    /// Output format
    pub format: ExplainFormat,
    /// Show verbose information
    pub verbose: bool,
    /// Show cost estimates
    pub costs: bool,
    /// Show buffer usage
    pub buffers: bool,
    /// Show actual timing (requires ANALYZE)
    pub timing: bool,
    /// Actually execute the query (EXPLAIN ANALYZE)
    pub analyze: bool,
}

impl Default for ExplainOptions {
    fn default() -> Self {
        Self {
            format: ExplainFormat::Text,
            verbose: false,
            costs: true,
            buffers: false,
            timing: false,
            analyze: false,
        }
    }
}

/// Query execution plan with cost estimates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlan {
    /// Root plan node
    pub plan: PlanNode,
    /// Planning time in milliseconds
    pub planning_time_ms: f64,
    /// Execution time (only for ANALYZE)
    pub execution_time_ms: Option<f64>,
    /// Total cost estimate
    pub total_cost: f64,
    /// Estimated rows
    pub estimated_rows: f64,
    /// Actual rows (only for ANALYZE)
    pub actual_rows: Option<usize>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl ExplainPlan {
    /// Create a new explain plan from a plan node
    pub fn new(plan: PlanNode, planning_time_ms: f64) -> Self {
        let cost = Self::extract_cost(&plan);
        let estimated_rows = cost.rows;

        Self {
            plan,
            planning_time_ms,
            execution_time_ms: None,
            total_cost: cost.total(),
            estimated_rows,
            actual_rows: None,
            metadata: HashMap::new(),
        }
    }

    /// Extract cost from plan node
    fn extract_cost(node: &PlanNode) -> Cost {
        match node {
            PlanNode::TableScan { cost, .. } => *cost,
            PlanNode::IndexScan { cost, .. } => *cost,
            PlanNode::NestedLoopJoin { cost, .. } => *cost,
            PlanNode::HashJoin { cost, .. } => *cost,
            PlanNode::SortMergeJoin { cost, .. } => *cost,
            PlanNode::Sort { cost, .. } => *cost,
            PlanNode::Aggregate { cost, .. } => *cost,
            PlanNode::Filter { cost, .. } => *cost,
            PlanNode::Project { cost, .. } => *cost,
            PlanNode::Limit { cost, .. } => *cost,
            PlanNode::Materialize { cost, .. } => *cost,
            PlanNode::Distinct { cost, .. } => *cost,
            PlanNode::SetOperation { cost, .. } => *cost,
        }
    }

    /// Set execution results (for ANALYZE)
    pub fn set_execution_results(&mut self, execution_time_ms: f64, actual_rows: usize) {
        self.execution_time_ms = Some(execution_time_ms);
        self.actual_rows = Some(actual_rows);
    }

    /// Add metadata
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Format as PostgreSQL-flavoured text. Each plan node renders on its
    /// own line; children render indented with `->` connectors:
    ///
    /// ```text
    /// Limit  (rows=5)
    ///   ->  Sort  (rows=5)
    ///         Sort Key: name DESC
    ///         ->  Seq Scan on users  (rows=100)
    ///               Filter: id = 1
    /// ```
    ///
    /// This format is what psql / JDBC / psycopg2 clients expect when they
    /// run plain `EXPLAIN`. Verbose mode (`EXPLAIN (VERBOSE)`) adds per-node
    /// detail — Output column lists, full Filter / Join Cond predicate
    /// text, Hash build-side, and so on. ANALYZE appends actual timing and
    /// row counts as a trailing block.
    pub fn format_text(&self, options: &ExplainOptions) -> String {
        let mut output = String::new();
        self.format_node_text(&self.plan, 0, true, &mut output, options);

        if options.analyze {
            // ANALYZE block — matches what previous sessions documented for
            // EXPLAIN ANALYZE output.
            if let Some(exec_time) = self.execution_time_ms {
                output.push_str(&format!("\nPlanning Time: {:.3} ms\n", self.planning_time_ms));
                output.push_str(&format!("Execution Time: {:.3} ms\n", exec_time));
                if let Some(actual) = self.actual_rows {
                    let accuracy = if self.estimated_rows > 0.0 {
                        (actual as f64 / self.estimated_rows) * 100.0
                    } else {
                        0.0
                    };
                    output.push_str(&format!(
                        "Actual Rows: {} (estimate accuracy: {:.1}%)\n",
                        actual, accuracy
                    ));
                }
            }
        }

        output
    }

    /// Format a single plan node and its children. PostgreSQL convention:
    /// the root has no connector, every other node is prefixed with `->`
    /// at two-space indent per level. Verbose detail (predicate text,
    /// sort keys, hash build-side, etc.) renders as additional indented
    /// lines beneath the node.
    #[allow(clippy::only_used_in_recursion)]
    fn format_node_text(
        &self,
        node: &PlanNode,
        depth: usize,
        is_root: bool,
        output: &mut String,
        options: &ExplainOptions,
    ) {
        let indent = "  ".repeat(depth);
        let prefix = if is_root {
            String::new()
        } else {
            format!("{}->  ", indent)
        };
        let detail_indent = if is_root {
            "  ".to_string()
        } else {
            format!("{}    ", indent)
        };

        match node {
            PlanNode::TableScan {
                table,
                predicates,
                cost,
            } => {
                output.push_str(&format!("{}Seq Scan on {}", prefix, table));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if !predicates.is_empty() {
                    let text = render_predicates(predicates);
                    output.push_str(&format!("{}Filter: {}\n", detail_indent, text));
                }
            }

            PlanNode::IndexScan {
                table,
                index,
                predicates,
                cost,
            } => {
                output.push_str(&format!("{}Index Scan using {} on {}", prefix, index, table));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if !predicates.is_empty() {
                    let text = render_predicates(predicates);
                    output.push_str(&format!("{}Index Cond: {}\n", detail_indent, text));
                }
            }

            PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                cost,
            } => {
                output.push_str(&format!("{}Nested Loop", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                output.push_str(&format!(
                    "{}Join Cond: {}\n",
                    detail_indent,
                    render_join_condition(condition)
                ));
                self.format_node_text(left, depth + 1, false, output, options);
                self.format_node_text(right, depth + 1, false, output, options);
            }

            PlanNode::HashJoin {
                left,
                right,
                condition,
                build_side,
                cost,
            } => {
                output.push_str(&format!("{}Hash Join", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                output.push_str(&format!(
                    "{}Hash Cond: {}\n",
                    detail_indent,
                    render_join_condition(condition)
                ));
                if options.verbose {
                    output.push_str(&format!("{}Build Side: {:?}\n", detail_indent, build_side));
                }
                self.format_node_text(left, depth + 1, false, output, options);
                self.format_node_text(right, depth + 1, false, output, options);
            }

            PlanNode::SortMergeJoin {
                left,
                right,
                condition,
                cost,
            } => {
                output.push_str(&format!("{}Merge Join", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                output.push_str(&format!(
                    "{}Merge Cond: {}\n",
                    detail_indent,
                    render_join_condition(condition)
                ));
                self.format_node_text(left, depth + 1, false, output, options);
                self.format_node_text(right, depth + 1, false, output, options);
            }

            PlanNode::Sort { input, keys, cost } => {
                output.push_str(&format!("{}Sort", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if !keys.is_empty() {
                    let key_strs: Vec<String> = keys
                        .iter()
                        .map(|k| {
                            format!("{} {}", k.column, if k.ascending { "ASC" } else { "DESC" })
                        })
                        .collect();
                    output.push_str(&format!(
                        "{}Sort Key: {}\n",
                        detail_indent,
                        key_strs.join(", ")
                    ));
                }
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::Aggregate {
                input,
                group_by,
                aggregates,
                cost,
            } => {
                let label = if group_by.is_empty() {
                    "Aggregate"
                } else {
                    "GroupAggregate"
                };
                output.push_str(&format!("{}{}", prefix, label));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if !group_by.is_empty() {
                    output.push_str(&format!(
                        "{}Group Key: {}\n",
                        detail_indent,
                        group_by.join(", ")
                    ));
                }
                if !aggregates.is_empty() && options.verbose {
                    let agg_strs: Vec<String> =
                        aggregates.iter().map(|a| a.alias.clone()).collect();
                    output.push_str(&format!(
                        "{}Aggregates: {}\n",
                        detail_indent,
                        agg_strs.join(", ")
                    ));
                }
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::Filter {
                input,
                predicates,
                cost,
            } => {
                output.push_str(&format!("{}Filter", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if !predicates.is_empty() {
                    output.push_str(&format!(
                        "{}Filter: {}\n",
                        detail_indent,
                        render_predicates(predicates)
                    ));
                }
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::Project {
                input,
                columns,
                cost,
            } => {
                output.push_str(&format!("{}Project", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if options.verbose && !columns.is_empty() {
                    output.push_str(&format!(
                        "{}Output: {}\n",
                        detail_indent,
                        columns.join(", ")
                    ));
                }
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::Limit {
                input,
                limit,
                offset,
                cost,
            } => {
                output.push_str(&format!("{}Limit", prefix));
                if options.costs {
                    output.push_str(&format!("  (count={}, rows={:.0})", limit, cost.rows));
                }
                output.push('\n');
                if *offset > 0 {
                    output.push_str(&format!("{}Offset: {}\n", detail_indent, offset));
                }
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::Materialize { input, cost } => {
                output.push_str(&format!("{}Materialize", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::Distinct {
                input,
                columns,
                cost,
            } => {
                output.push_str(&format!("{}Unique", prefix));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                if options.verbose && !columns.is_empty() {
                    output.push_str(&format!(
                        "{}Keys: {}\n",
                        detail_indent,
                        columns.join(", ")
                    ));
                }
                self.format_node_text(input, depth + 1, false, output, options);
            }

            PlanNode::SetOperation {
                left,
                right,
                operation,
                cost,
            } => {
                output.push_str(&format!("{}{}", prefix, operation));
                if options.costs {
                    output.push_str(&format!("  (rows={:.0})", cost.rows));
                }
                output.push('\n');
                self.format_node_text(left, depth + 1, false, output, options);
                self.format_node_text(right, depth + 1, false, output, options);
            }
        }
    }

    /// Format as JSON
    pub fn format_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Format as YAML
    pub fn format_yaml(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

impl fmt::Display for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_text(&ExplainOptions::default()))
    }
}

// ----------------------------------------------------------------------------
// Helpers for rendering predicates and join conditions as PostgreSQL-flavoured
// text. The structured `Predicate` carries (column, op, value, selectivity);
// for simple `col op const` shapes that renders cleanly. When the SQL
// predicate is richer we keep the original text in `PredicateValue::Raw` so
// the displayed Filter line still matches what the user wrote.
// ----------------------------------------------------------------------------

fn render_op(op: &crate::optimizer::ComparisonOp) -> &'static str {
    use crate::optimizer::ComparisonOp;
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Ne => "!=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Le => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Ge => ">=",
        ComparisonOp::Like => "LIKE",
        ComparisonOp::In => "IN",
    }
}

fn render_predicate_value(value: &crate::optimizer::PredicateValue) -> String {
    use crate::optimizer::PredicateValue;
    match value {
        PredicateValue::Constant(v) => match v {
            serde_json::Value::String(s) => format!("'{}'", s),
            other => other.to_string(),
        },
        PredicateValue::Column(c) => c.clone(),
        PredicateValue::Subquery(_) => "(subquery)".to_string(),
        PredicateValue::Raw(text) => text.clone(),
    }
}

fn render_predicate(p: &crate::optimizer::Predicate) -> String {
    // A predicate whose value is `Raw` came from a compound SQL expression
    // we couldn't structure further — render just the raw text and ignore
    // the placeholder column/op so the output reads like the user's SQL.
    if let crate::optimizer::PredicateValue::Raw(text) = &p.value {
        return text.clone();
    }
    format!(
        "{} {} {}",
        p.column,
        render_op(&p.op),
        render_predicate_value(&p.value)
    )
}

fn render_predicates(predicates: &[crate::optimizer::Predicate]) -> String {
    predicates
        .iter()
        .map(render_predicate)
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn render_join_condition(c: &crate::optimizer::JoinCondition) -> String {
    if let Some(text) = &c.raw_text {
        return text.clone();
    }
    format!("{} {} {}", c.left_col, render_op(&c.op), c.right_col)
}

/// EXPLAIN executor
pub struct ExplainExecutor;

impl ExplainExecutor {
    /// Create EXPLAIN plan without execution
    pub fn explain(plan: PlanNode, planning_time: Duration) -> ExplainPlan {
        ExplainPlan::new(plan, planning_time.as_secs_f64() * 1000.0)
    }

    /// Create EXPLAIN ANALYZE plan with execution
    pub fn explain_analyze<F>(
        plan: PlanNode,
        planning_time: Duration,
        execute_fn: F,
    ) -> Result<ExplainPlan>
    where
        F: FnOnce() -> Result<usize>,
    {
        let mut explain_plan = ExplainPlan::new(plan, planning_time.as_secs_f64() * 1000.0);

        let start = Instant::now();
        let row_count = execute_fn()?;
        let execution_time = start.elapsed();

        explain_plan.set_execution_results(execution_time.as_secs_f64() * 1000.0, row_count);

        Ok(explain_plan)
    }
}

// ============================================================================
// sqlparser AST → cost_optimizer::PlanNode translation
//
// Walks a `sqlparser::ast::Statement` and emits a PlanNode tree the
// `ExplainExecutor` can consume. The previous session shipped a parallel
// walker in `sql_explain.rs` because the missing piece was this
// translation; that module is retired in the same commit that adds this
// function.
//
// The translation is best-effort structural:
//   - simple `col op const` predicates → `Predicate { column, op, value }`
//     so the cost model can use selectivity if it ever wants to
//   - compound / function-call / arithmetic predicates → a single
//     `Predicate` with `PredicateValue::Raw(text)`
//   - simple `t1.a = t2.b` equi-joins → `JoinCondition { left_col, ... }`
//   - richer join conditions → `JoinCondition::raw_text`
//
// Row estimates come from `Engine::get_table_data(...)`'s actual sizes
// when available — same source the retired walker used.
// ============================================================================

use sqlparser::ast::{
    Expr as SqlExpr, GroupByExpr, JoinOperator, OrderByExpr, Query as SqlQuery, Select,
    SelectItem, SetExpr, Statement as SqlStatement, TableFactor,
};

use crate::optimizer::{
    AggregateFunc, ComparisonOp, JoinCondition, Predicate, PredicateValue, SortKey,
};
use crate::engine::Engine;
use crate::errors::DriftError;

/// Translate a sqlparser `Statement` into a `PlanNode` tree. SELECT
/// walks recursively; DML / DDL surface as a small placeholder node
/// because EXPLAIN on those primarily describes the write action rather
/// than a scan/join tree.
pub fn build_plan_from_statement(engine: &Engine, stmt: &SqlStatement) -> Result<PlanNode> {
    match stmt {
        SqlStatement::Query(q) => build_query_plan(engine, q),
        SqlStatement::Insert(insert) => Ok(PlanNode::Materialize {
            input: Box::new(PlanNode::TableScan {
                table: insert.table_name.to_string(),
                predicates: vec![Predicate {
                    column: "(action)".to_string(),
                    op: ComparisonOp::Eq,
                    value: PredicateValue::Raw(format!("INSERT INTO {}", insert.table_name)),
                    selectivity: 1.0,
                }],
                cost: scan_cost(0),
            }),
            cost: scan_cost(0),
        }),
        SqlStatement::Update { table, .. } => Ok(PlanNode::Materialize {
            input: Box::new(PlanNode::TableScan {
                table: table_factor_label(&table.relation),
                predicates: vec![Predicate {
                    column: "(action)".to_string(),
                    op: ComparisonOp::Eq,
                    value: PredicateValue::Raw("UPDATE".to_string()),
                    selectivity: 1.0,
                }],
                cost: scan_cost(0),
            }),
            cost: scan_cost(0),
        }),
        SqlStatement::Delete(_) => Ok(PlanNode::Materialize {
            input: Box::new(PlanNode::TableScan {
                table: "(delete target)".to_string(),
                predicates: vec![Predicate {
                    column: "(action)".to_string(),
                    op: ComparisonOp::Eq,
                    value: PredicateValue::Raw("DELETE".to_string()),
                    selectivity: 1.0,
                }],
                cost: scan_cost(0),
            }),
            cost: scan_cost(0),
        }),
        _ => Err(DriftError::InvalidQuery(
            "EXPLAIN supports SELECT / INSERT / UPDATE / DELETE only".to_string(),
        )),
    }
}

fn build_query_plan(engine: &Engine, query: &SqlQuery) -> Result<PlanNode> {
    let mut root = build_body_plan(engine, &query.body)?;

    if let Some(order_by) = &query.order_by {
        if !order_by.exprs.is_empty() {
            let keys: Vec<SortKey> = order_by.exprs.iter().map(sort_key_from_expr).collect();
            let rows = plan_rows(&root);
            root = PlanNode::Sort {
                input: Box::new(root),
                keys,
                cost: scan_cost(rows),
            };
        }
    }

    if let Some(limit_expr) = &query.limit {
        let count = match limit_expr {
            SqlExpr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse::<usize>().unwrap_or(0),
            _ => 0,
        };
        let rows = count.min(plan_rows(&root).max(count));
        root = PlanNode::Limit {
            input: Box::new(root),
            limit: count,
            offset: 0,
            cost: scan_cost(rows),
        };
    }

    Ok(root)
}

fn build_body_plan(engine: &Engine, body: &SetExpr) -> Result<PlanNode> {
    match body {
        SetExpr::Select(select) => build_select_plan(engine, select),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left_plan = build_body_plan(engine, left)?;
            let right_plan = build_body_plan(engine, right)?;
            let rows = plan_rows(&left_plan) + plan_rows(&right_plan);
            // Operation label honours the SQL standard names: "Union",
            // "Intersect", "Except", with " All" appended for the ALL form.
            let mut label = match op {
                sqlparser::ast::SetOperator::Union => "Union",
                sqlparser::ast::SetOperator::Intersect => "Intersect",
                sqlparser::ast::SetOperator::Except => "Except",
            }
            .to_string();
            if matches!(set_quantifier, sqlparser::ast::SetQuantifier::All) {
                label.push_str(" All");
            }
            Ok(PlanNode::SetOperation {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                operation: label,
                cost: scan_cost(rows),
            })
        }
        SetExpr::Query(inner) => build_query_plan(engine, inner),
        // VALUES lists, table-function calls, INSERT subqueries — surface as a
        // generic Materialize node rather than failing EXPLAIN.
        _ => Ok(PlanNode::Materialize {
            input: Box::new(PlanNode::TableScan {
                table: "(subquery)".to_string(),
                predicates: vec![],
                cost: scan_cost(0),
            }),
            cost: scan_cost(0),
        }),
    }
}

fn build_select_plan(engine: &Engine, select: &Select) -> Result<PlanNode> {
    // FROM clause: left table, fold each JOIN as a NestedLoop.
    let mut root = if let Some(first) = select.from.first() {
        let mut node = build_table_factor_plan(engine, &first.relation)?;
        for join in &first.joins {
            let right = build_table_factor_plan(engine, &join.relation)?;
            let condition = build_join_condition(&join.join_operator);
            let rows = plan_rows(&node).max(plan_rows(&right));
            node = PlanNode::NestedLoopJoin {
                left: Box::new(node),
                right: Box::new(right),
                condition,
                cost: scan_cost(rows),
            };
        }
        node
    } else {
        // SELECT without FROM — single result row (`SELECT 1`, etc.).
        PlanNode::Materialize {
            input: Box::new(PlanNode::TableScan {
                table: "(result)".to_string(),
                predicates: vec![],
                cost: scan_cost(1),
            }),
            cost: scan_cost(1),
        }
    };

    // WHERE: prefer pushing into the underlying scan when there's no join;
    // otherwise wrap with Filter so the predicate still surfaces visibly.
    if let Some(selection) = &select.selection {
        let preds = predicates_from_expr(selection);
        if let PlanNode::TableScan {
            ref mut predicates, ..
        } = root
        {
            predicates.extend(preds);
        } else {
            let rows = plan_rows(&root);
            root = PlanNode::Filter {
                input: Box::new(root),
                predicates: preds,
                cost: scan_cost(rows),
            };
        }
    }

    // GROUP BY / aggregates.
    let group_by_columns: Vec<String> = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs.iter().map(format_sql_expr).collect(),
        _ => Vec::new(),
    };
    let aggregates: Vec<AggregateFunc> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(SqlExpr::Function(f)) => {
                let func = f.name.to_string().to_lowercase();
                Some(AggregateFunc {
                    func: func.clone(),
                    column: None,
                    alias: f.to_string(),
                })
            }
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Function(f),
                alias,
            } => Some(AggregateFunc {
                func: f.name.to_string().to_lowercase(),
                column: None,
                alias: alias.value.clone(),
            }),
            _ => None,
        })
        .collect();
    if !group_by_columns.is_empty() || !aggregates.is_empty() {
        let rows = if group_by_columns.is_empty() {
            1
        } else {
            (plan_rows(&root) / 10).max(1)
        };
        root = PlanNode::Aggregate {
            input: Box::new(root),
            group_by: group_by_columns,
            aggregates,
            cost: scan_cost(rows),
        };
    }

    // DISTINCT.
    if select.distinct.is_some() {
        let rows = (plan_rows(&root) / 2).max(1);
        root = PlanNode::Distinct {
            input: Box::new(root),
            columns: Vec::new(),
            cost: scan_cost(rows),
        };
    }

    Ok(root)
}

fn build_table_factor_plan(engine: &Engine, tf: &TableFactor) -> Result<PlanNode> {
    let label = table_factor_label(tf);
    let lookup_name = match tf {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => label.clone(),
    };
    let rows = engine.get_table_data(&lookup_name).map(|d| d.len()).unwrap_or(0);
    Ok(PlanNode::TableScan {
        table: label,
        predicates: vec![],
        cost: scan_cost(rows),
    })
}

fn build_join_condition(op: &JoinOperator) -> JoinCondition {
    let constraint = match op {
        JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => Some(c),
        _ => None,
    };
    let Some(constraint) = constraint else {
        // CROSS JOIN or similar: no predicate.
        return JoinCondition {
            left_col: String::new(),
            right_col: String::new(),
            op: ComparisonOp::Eq,
            raw_text: Some("(cross join)".to_string()),
        };
    };
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => {
            // Simple `a.col = b.col` shape gets structured; anything more
            // complex (compound AND, function-call comparisons) is kept as
            // raw text so EXPLAIN displays exactly what was written.
            if let SqlExpr::BinaryOp { left, op, right } = expr {
                if let (Some(l), Some(r), Some(cop)) = (
                    binop_column_name(left),
                    binop_column_name(right),
                    binary_op_to_comparison(op),
                ) {
                    return JoinCondition {
                        left_col: l,
                        right_col: r,
                        op: cop,
                        raw_text: None,
                    };
                }
            }
            JoinCondition {
                left_col: String::new(),
                right_col: String::new(),
                op: ComparisonOp::Eq,
                raw_text: Some(format_sql_expr(expr)),
            }
        }
        sqlparser::ast::JoinConstraint::Using(cols) => {
            let names: Vec<String> = cols.iter().map(|c| c.value.clone()).collect();
            JoinCondition {
                left_col: String::new(),
                right_col: String::new(),
                op: ComparisonOp::Eq,
                raw_text: Some(format!("USING ({})", names.join(", "))),
            }
        }
        _ => JoinCondition {
            left_col: String::new(),
            right_col: String::new(),
            op: ComparisonOp::Eq,
            raw_text: Some("(unknown)".to_string()),
        },
    }
}

fn binop_column_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(i) => Some(i.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => Some(
            parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}

fn binary_op_to_comparison(op: &sqlparser::ast::BinaryOperator) -> Option<ComparisonOp> {
    use sqlparser::ast::BinaryOperator;
    Some(match op {
        BinaryOperator::Eq => ComparisonOp::Eq,
        BinaryOperator::NotEq => ComparisonOp::Ne,
        BinaryOperator::Lt => ComparisonOp::Lt,
        BinaryOperator::LtEq => ComparisonOp::Le,
        BinaryOperator::Gt => ComparisonOp::Gt,
        BinaryOperator::GtEq => ComparisonOp::Ge,
        _ => return None,
    })
}

/// Walk a WHERE expression and extract predicates. Compound `A AND B`
/// expands to one Predicate per leaf; everything else collapses to a
/// single Raw predicate carrying the formatted expression.
fn predicates_from_expr(expr: &SqlExpr) -> Vec<Predicate> {
    let mut out = Vec::new();
    flatten_and(expr, &mut out);
    out
}

fn flatten_and(expr: &SqlExpr, out: &mut Vec<Predicate>) {
    if let SqlExpr::BinaryOp {
        left,
        op: sqlparser::ast::BinaryOperator::And,
        right,
    } = expr
    {
        flatten_and(left, out);
        flatten_and(right, out);
        return;
    }
    out.push(predicate_from_expr(expr));
}

fn predicate_from_expr(expr: &SqlExpr) -> Predicate {
    if let SqlExpr::BinaryOp { left, op, right } = expr {
        if let (Some(col), Some(cop)) = (binop_column_name(left), binary_op_to_comparison(op)) {
            // Right-hand side: try to fit as a constant or column reference.
            let value = match right.as_ref() {
                SqlExpr::Value(v) => PredicateValue::Constant(sql_value_to_json(v)),
                SqlExpr::Identifier(i) => PredicateValue::Column(i.value.clone()),
                SqlExpr::CompoundIdentifier(parts) => PredicateValue::Column(
                    parts
                        .iter()
                        .map(|p| p.value.as_str())
                        .collect::<Vec<_>>()
                        .join("."),
                ),
                other => PredicateValue::Raw(format_sql_expr(other)),
            };
            return Predicate {
                column: col,
                op: cop,
                value,
                selectivity: 0.5,
            };
        }
    }
    Predicate {
        column: "(expr)".to_string(),
        op: ComparisonOp::Eq,
        value: PredicateValue::Raw(format_sql_expr(expr)),
        selectivity: 1.0,
    }
}

fn sql_value_to_json(v: &sqlparser::ast::Value) -> serde_json::Value {
    use sqlparser::ast::Value;
    match v {
        Value::Number(n, _) => n.parse::<f64>().map(|f| serde_json::json!(f)).unwrap_or_else(
            |_| serde_json::Value::String(n.clone()),
        ),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            serde_json::Value::String(s.clone())
        }
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        other => serde_json::Value::String(other.to_string()),
    }
}

fn sort_key_from_expr(o: &OrderByExpr) -> SortKey {
    SortKey {
        column: format_sql_expr(&o.expr),
        ascending: o.asc.unwrap_or(true),
    }
}

fn table_factor_label(tf: &TableFactor) -> String {
    match tf {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(a) => format!("{} {}", name, a.name.value),
            None => name.to_string(),
        },
        TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| "(derived)".to_string()),
        _ => format!("{:?}", tf),
    }
}

fn format_sql_expr(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Identifier(i) => i.value.clone(),
        SqlExpr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        SqlExpr::Value(v) => match v {
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => format!("'{}'", s),
            other => other.to_string(),
        },
        SqlExpr::BinaryOp { left, op, right } => format!(
            "{} {} {}",
            format_sql_expr(left),
            op,
            format_sql_expr(right)
        ),
        SqlExpr::Function(f) => f.to_string(),
        SqlExpr::IsNull(inner) => format!("{} IS NULL", format_sql_expr(inner)),
        SqlExpr::IsNotNull(inner) => format!("{} IS NOT NULL", format_sql_expr(inner)),
        _ => format!("{:?}", expr),
    }
}

fn plan_rows(node: &PlanNode) -> usize {
    match node {
        PlanNode::TableScan { cost, .. }
        | PlanNode::IndexScan { cost, .. }
        | PlanNode::NestedLoopJoin { cost, .. }
        | PlanNode::HashJoin { cost, .. }
        | PlanNode::SortMergeJoin { cost, .. }
        | PlanNode::Sort { cost, .. }
        | PlanNode::Aggregate { cost, .. }
        | PlanNode::Filter { cost, .. }
        | PlanNode::Project { cost, .. }
        | PlanNode::Limit { cost, .. }
        | PlanNode::Materialize { cost, .. }
        | PlanNode::Distinct { cost, .. }
        | PlanNode::SetOperation { cost, .. } => cost.rows as usize,
    }
}

fn scan_cost(rows: usize) -> Cost {
    // Coarse heuristic: assume 100-byte rows, sequential scan cost model.
    // A richer cost model can plug in via `CostOptimizer::optimize_select`
    // once it learns to swap nodes; today the displayed `(rows=N)` is the
    // user-visible part of the plan annotation.
    Cost::seq_scan((rows as f64 / 100.0).max(1.0), rows as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::{ComparisonOp, Predicate, PredicateValue};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use tempfile::TempDir;

    // ----- Direct PlanNode formatting (unit tests for the formatter) -------

    #[test]
    fn formatter_renders_seq_scan_with_pg_conventions() {
        // Plain `Seq Scan on users` — no `Table Scan` legacy spelling.
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 10000.0),
        };
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));
        let output = explain.format_text(&ExplainOptions::default());
        assert!(output.contains("Seq Scan on users"), "got: {}", output);
        assert!(output.contains("rows=10000"));
    }

    #[test]
    fn formatter_renders_filter_with_full_predicate_text() {
        // Filter detail should be the readable predicate, not "N conditions".
        let plan = PlanNode::Filter {
            input: Box::new(PlanNode::TableScan {
                table: "users".to_string(),
                predicates: vec![],
                cost: Cost::seq_scan(100.0, 10000.0),
            }),
            predicates: vec![Predicate {
                column: "age".to_string(),
                op: ComparisonOp::Gt,
                value: PredicateValue::Constant(serde_json::json!(18)),
                selectivity: 0.5,
            }],
            cost: Cost::seq_scan(100.0, 5000.0),
        };
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));
        let output = explain.format_text(&ExplainOptions::default());
        assert!(output.contains("Filter"));
        assert!(output.contains("Filter: age > 18"), "got: {}", output);
        assert!(output.contains("->  Seq Scan on users"));
    }

    #[test]
    fn formatter_renders_index_scan_with_index_name() {
        let plan = PlanNode::IndexScan {
            table: "users".to_string(),
            index: "idx_email".to_string(),
            predicates: vec![],
            cost: Cost::index_scan(10.0, 50.0, 1000.0),
        };
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(3));
        let output = explain.format_text(&ExplainOptions::default());
        assert!(output.contains("Index Scan using idx_email on users"));
    }

    #[test]
    fn json_format_carries_structured_plan() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 10000.0),
        };
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));
        let json = explain.format_json().unwrap();
        assert!(json.contains("\"TableScan\""));
        assert!(json.contains("\"planning_time_ms\""));
        assert!(json.contains("\"users\""));
    }

    #[test]
    fn yaml_format_carries_structured_plan() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 10000.0),
        };
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));
        let yaml = explain.format_yaml().unwrap();
        assert!(yaml.contains("TableScan"));
        assert!(yaml.contains("users"));
    }

    #[test]
    fn analyze_reports_actual_rows_and_accuracy() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 5000.0),
        };
        let explain = ExplainExecutor::explain_analyze(
            plan,
            Duration::from_millis(5),
            || Ok(4950),
        )
        .unwrap();
        assert_eq!(explain.actual_rows, Some(4950));
        assert!(explain.execution_time_ms.is_some());

        let output = explain.format_text(&ExplainOptions {
            analyze: true,
            ..Default::default()
        });
        assert!(output.contains("Execution Time"));
        assert!(output.contains("Actual Rows: 4950"));
        // Estimate accuracy = 4950/5000 ≈ 99%.
        assert!(output.contains("99."));
    }

    // ----- AST → PlanNode translation (covers sql_explain's surface) ------

    fn ast_for(sql: &str) -> SqlStatement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    fn engine_for_test() -> (TempDir, Engine) {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::init(tmp.path()).unwrap();
        (tmp, engine)
    }

    fn plan_text(sql: &str) -> String {
        let (_t, engine) = engine_for_test();
        let plan = build_plan_from_statement(&engine, &ast_for(sql)).unwrap();
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(0));
        explain.format_text(&ExplainOptions::default())
    }

    #[test]
    fn ast_select_with_where_pushes_predicate_into_scan() {
        let text = plan_text("SELECT * FROM users WHERE id = 1");
        assert!(text.contains("Seq Scan on users"), "got: {}", text);
        assert!(text.contains("Filter: id = 1"), "got: {}", text);
    }

    #[test]
    fn ast_select_order_by_limit_nests_sort_and_limit() {
        let text = plan_text("SELECT * FROM users ORDER BY name DESC LIMIT 5");
        // Root is Limit; Sort beneath it; Seq Scan below that.
        assert!(text.starts_with("Limit"), "got: {}", text);
        assert!(text.contains("->  Sort"));
        assert!(text.contains("Sort Key: name DESC"));
        assert!(text.contains("->  Seq Scan on users"));
    }

    #[test]
    fn ast_select_group_by_renders_group_aggregate() {
        let text =
            plan_text("SELECT department, COUNT(*) FROM employees GROUP BY department");
        assert!(text.contains("GroupAggregate"), "got: {}", text);
        assert!(text.contains("Group Key: department"));
    }

    #[test]
    fn ast_select_join_renders_nested_loop_with_condition() {
        let text = plan_text("SELECT * FROM users u JOIN posts p ON u.id = p.user_id");
        assert!(text.contains("Nested Loop"));
        assert!(text.contains("Join Cond: u.id = p.user_id"), "got: {}", text);
    }

    #[test]
    fn ast_set_operation_renders_union_with_two_children() {
        let text = plan_text("SELECT id FROM a UNION SELECT id FROM b");
        assert!(text.contains("Union"));
        // Two children, both Seq Scans.
        assert_eq!(text.matches("Seq Scan").count(), 2);
    }

    #[test]
    fn ast_set_operation_all_variant_shows_all_suffix() {
        let text = plan_text("SELECT id FROM a UNION ALL SELECT id FROM b");
        assert!(text.contains("Union All"));
    }

    #[test]
    fn ast_select_distinct_renders_unique() {
        let text = plan_text("SELECT DISTINCT department FROM employees");
        assert!(text.contains("Unique"), "got: {}", text);
    }

    #[test]
    fn ast_filter_above_join_keeps_join_visible() {
        // Filter above a join used to drop the join from display. The Filter
        // wrapper now reports both itself and its child.
        let text = plan_text(
            "SELECT * FROM users u JOIN posts p ON u.id = p.user_id WHERE u.status = 'active'",
        );
        assert!(text.contains("Filter"));
        assert!(text.contains("u.status = 'active'"));
        assert!(text.contains("Nested Loop"));
    }

    #[test]
    fn ast_select_with_inequality_join_uses_raw_text() {
        // Non-equality join condition can't be structured cleanly; should
        // surface as raw text in the Join Cond line.
        let text = plan_text("SELECT * FROM a JOIN b ON a.x > b.y");
        assert!(text.contains("Nested Loop"));
        assert!(text.contains("a.x > b.y"));
    }

    // ----- VERBOSE mode -----------------------------------------------------

    #[test]
    fn verbose_mode_shows_aggregate_aliases() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::TableScan {
                table: "users".to_string(),
                predicates: vec![],
                cost: Cost::seq_scan(100.0, 1000.0),
            }),
            group_by: vec!["department".to_string()],
            aggregates: vec![AggregateFunc {
                func: "count".to_string(),
                column: None,
                alias: "count(*)".to_string(),
            }],
            cost: Cost::seq_scan(10.0, 100.0),
        };
        let explain = ExplainExecutor::explain(plan, Duration::from_millis(0));
        let verbose = explain.format_text(&ExplainOptions {
            verbose: true,
            ..Default::default()
        });
        // Non-verbose elides Aggregates line; verbose shows it.
        assert!(verbose.contains("Aggregates: count(*)"), "got: {}", verbose);
        let plain = explain.format_text(&ExplainOptions::default());
        assert!(!plain.contains("Aggregates:"));
    }
}
