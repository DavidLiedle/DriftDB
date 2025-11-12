//! EXPLAIN and EXPLAIN ANALYZE implementation
//!
//! Provides query execution plan visualization and analysis:
//! - EXPLAIN: Shows planned execution strategy with cost estimates
//! - EXPLAIN ANALYZE: Executes query and shows actual performance metrics

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use crate::cost_optimizer::{Cost, PlanNode};
use crate::errors::Result;

/// EXPLAIN output format
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
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

    /// Format as text
    pub fn format_text(&self, options: &ExplainOptions) -> String {
        let mut output = String::new();

        output.push_str("Query Plan\n");
        output.push_str(&format!("{}\n", "=".repeat(60)));

        self.format_node_text(&self.plan, 0, &mut output, options);

        output.push_str(&format!("\n{}\n", "=".repeat(60)));
        output.push_str(&format!("Planning Time: {:.3} ms\n", self.planning_time_ms));

        if let Some(exec_time) = self.execution_time_ms {
            output.push_str(&format!("Execution Time: {:.3} ms\n", exec_time));
            output.push_str(&format!("Total Time: {:.3} ms\n", self.planning_time_ms + exec_time));
        }

        if options.costs {
            output.push_str(&format!("Total Cost: {:.2}\n", self.total_cost));
            output.push_str(&format!("Estimated Rows: {:.0}\n", self.estimated_rows));
        }

        if let Some(actual) = self.actual_rows {
            output.push_str(&format!("Actual Rows: {}\n", actual));
            let accuracy = if self.estimated_rows > 0.0 {
                (actual as f64 / self.estimated_rows) * 100.0
            } else {
                0.0
            };
            output.push_str(&format!("Estimate Accuracy: {:.1}%\n", accuracy));
        }

        output
    }

    /// Format a plan node as text
    #[allow(clippy::only_used_in_recursion)]
    fn format_node_text(
        &self,
        node: &PlanNode,
        depth: usize,
        output: &mut String,
        options: &ExplainOptions,
    ) {
        let indent = "  ".repeat(depth);
        let arrow = if depth > 0 { "└─ " } else { "" };

        match node {
            PlanNode::TableScan {
                table,
                predicates,
                cost,
            } => {
                output.push_str(&format!("{}{}Table Scan on {}", indent, arrow, table));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose && !predicates.is_empty() {
                    output.push_str(&format!(
                        "{}  Filter: {} predicates\n",
                        indent,
                        predicates.len()
                    ));
                }
            }

            PlanNode::IndexScan {
                table,
                index,
                predicates,
                cost,
            } => {
                output.push_str(&format!(
                    "{}{}Index Scan using {} on {}",
                    indent, arrow, index, table
                ));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose && !predicates.is_empty() {
                    output.push_str(&format!(
                        "{}  Index Cond: {} predicates\n",
                        indent,
                        predicates.len()
                    ));
                }
            }

            PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                cost,
            } => {
                output.push_str(&format!("{}{}Nested Loop Join", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose {
                    output.push_str(&format!(
                        "{}  Join Cond: {} = {}\n",
                        indent, condition.left_col, condition.right_col
                    ));
                }

                self.format_node_text(left, depth + 1, output, options);
                self.format_node_text(right, depth + 1, output, options);
            }

            PlanNode::HashJoin {
                left,
                right,
                condition,
                build_side,
                cost,
            } => {
                output.push_str(&format!("{}{}Hash Join", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose {
                    output.push_str(&format!(
                        "{}  Hash Cond: {} = {}\n",
                        indent, condition.left_col, condition.right_col
                    ));
                    output.push_str(&format!("{}  Build Side: {:?}\n", indent, build_side));
                }

                self.format_node_text(left, depth + 1, output, options);
                self.format_node_text(right, depth + 1, output, options);
            }

            PlanNode::SortMergeJoin {
                left,
                right,
                condition,
                cost,
            } => {
                output.push_str(&format!("{}{}Sort-Merge Join", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose {
                    output.push_str(&format!(
                        "{}  Merge Cond: {} = {}\n",
                        indent, condition.left_col, condition.right_col
                    ));
                }

                self.format_node_text(left, depth + 1, output, options);
                self.format_node_text(right, depth + 1, output, options);
            }

            PlanNode::Sort { input, keys, cost } => {
                output.push_str(&format!("{}{}Sort", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose && !keys.is_empty() {
                    let key_strs: Vec<String> = keys
                        .iter()
                        .map(|k| {
                            format!(
                                "{} {}",
                                k.column,
                                if k.ascending { "ASC" } else { "DESC" }
                            )
                        })
                        .collect();
                    output.push_str(&format!("{}  Sort Key: {}\n", indent, key_strs.join(", ")));
                }

                self.format_node_text(input, depth + 1, output, options);
            }

            PlanNode::Aggregate {
                input,
                group_by,
                aggregates,
                cost,
            } => {
                output.push_str(&format!("{}{}Aggregate", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose {
                    if !group_by.is_empty() {
                        output.push_str(&format!("{}  Group By: {}\n", indent, group_by.join(", ")));
                    }
                    if !aggregates.is_empty() {
                        let agg_strs: Vec<String> =
                            aggregates.iter().map(|a| a.alias.clone()).collect();
                        output.push_str(&format!(
                            "{}  Aggregates: {}\n",
                            indent,
                            agg_strs.join(", ")
                        ));
                    }
                }

                self.format_node_text(input, depth + 1, output, options);
            }

            PlanNode::Filter {
                input,
                predicates,
                cost,
            } => {
                output.push_str(&format!("{}{}Filter", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose && !predicates.is_empty() {
                    output.push_str(&format!(
                        "{}  Predicates: {} conditions\n",
                        indent,
                        predicates.len()
                    ));
                }

                self.format_node_text(input, depth + 1, output, options);
            }

            PlanNode::Project {
                input,
                columns,
                cost,
            } => {
                output.push_str(&format!("{}{}Project", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose && !columns.is_empty() {
                    output.push_str(&format!("{}  Columns: {}\n", indent, columns.join(", ")));
                }

                self.format_node_text(input, depth + 1, output, options);
            }

            PlanNode::Limit {
                input,
                limit,
                offset,
                cost,
            } => {
                output.push_str(&format!("{}{}Limit", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                if options.verbose {
                    output.push_str(&format!("{}  Limit: {}\n", indent, limit));
                    if *offset > 0 {
                        output.push_str(&format!("{}  Offset: {}\n", indent, offset));
                    }
                }

                self.format_node_text(input, depth + 1, output, options);
            }

            PlanNode::Materialize { input, cost } => {
                output.push_str(&format!("{}{}Materialize", indent, arrow));
                if options.costs {
                    output.push_str(&format!(
                        " (cost={:.2}, rows={:.0})",
                        cost.total(),
                        cost.rows
                    ));
                }
                output.push('\n');

                self.format_node_text(input, depth + 1, output, options);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_optimizer::{Predicate, ComparisonOp, PredicateValue};

    #[test]
    fn test_explain_table_scan() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 10000.0),
        };

        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));

        let output = explain.format_text(&ExplainOptions::default());
        assert!(output.contains("Table Scan on users"));
        assert!(output.contains("Planning Time: 5.000 ms"));
        assert!(output.contains("Total Cost:"));
    }

    #[test]
    fn test_explain_index_scan() {
        let plan = PlanNode::IndexScan {
            table: "users".to_string(),
            index: "idx_email".to_string(),
            predicates: vec![],
            cost: Cost::index_scan(10.0, 50.0, 1000.0),
        };

        let explain = ExplainExecutor::explain(plan, Duration::from_millis(3));

        let output = explain.format_text(&ExplainOptions::default());
        assert!(output.contains("Index Scan"));
        assert!(output.contains("idx_email"));
        assert!(output.contains("users"));
    }

    #[test]
    fn test_explain_analyze() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 10000.0),
        };

        let result = ExplainExecutor::explain_analyze(
            plan,
            Duration::from_millis(5),
            || Ok(9876), // Actual row count
        );

        assert!(result.is_ok());
        let explain = result.unwrap();

        assert_eq!(explain.actual_rows, Some(9876));
        assert!(explain.execution_time_ms.is_some());

        let output = explain.format_text(&ExplainOptions {
            analyze: true,
            ..Default::default()
        });
        assert!(output.contains("Execution Time:"));
        assert!(output.contains("Actual Rows: 9876"));
        assert!(output.contains("Estimate Accuracy:"));
    }

    #[test]
    fn test_explain_json_format() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 10000.0),
        };

        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));
        let json = explain.format_json();

        assert!(json.is_ok());
        let json_str = json.unwrap();
        assert!(json_str.contains("\"planning_time_ms\""));
        assert!(json_str.contains("\"total_cost\""));
    }

    #[test]
    fn test_explain_verbose() {
        let plan = PlanNode::Filter {
            input: Box::new(PlanNode::TableScan {
                table: "users".to_string(),
                predicates: vec![],
                cost: Cost::seq_scan(100.0, 10000.0),
            }),
            predicates: vec![
                Predicate {
                    column: "age".to_string(),
                    op: ComparisonOp::Gt,
                    value: PredicateValue::Constant(serde_json::json!(18)),
                    selectivity: 0.5,
                },
            ],
            cost: Cost::seq_scan(100.0, 5000.0),
        };

        let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));

        let output = explain.format_text(&ExplainOptions {
            verbose: true,
            ..Default::default()
        });

        assert!(output.contains("Filter"));
        assert!(output.contains("Predicates:"));
        assert!(output.contains("Table Scan"));
    }

    #[test]
    fn test_explain_cost_accuracy() {
        let plan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::seq_scan(100.0, 5000.0),
        };

        let result = ExplainExecutor::explain_analyze(
            plan,
            Duration::from_millis(5),
            || Ok(4950), // Very close to estimate
        );

        assert!(result.is_ok());
        let explain = result.unwrap();

        let output = explain.format_text(&ExplainOptions::default());
        assert!(output.contains("Estimate Accuracy:"));
        // Should be close to 99% (4950/5000)
        assert!(output.contains("99."));
    }
}
