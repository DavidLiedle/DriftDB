//! `EXPLAIN <query>` plan generation for sql_bridge.
//!
//! Walks a `sqlparser::ast::Statement` and builds a tree of `PlanNode`s
//! describing what the executor would do, then formats it as text rows
//! for the EXPLAIN result. Replaces the driftdb-server crate's parallel
//! implementation (which reparsed the SQL with hand-rolled string
//! parsers and lived inside the QueryExecutor).
//!
//! The plan shape is PostgreSQL-flavoured: a bottom-up tree displayed
//! with `->` connectors. Cost values are coarse heuristics today (no
//! real cost model wired in yet); row estimates come from the engine's
//! actual table sizes via `Engine::get_table_data`.
//!
//! This is separate from `crate::explain`, which carries a richer
//! `ExplainPlan` / `ExplainExecutor` API tied to `crate::cost_optimizer`
//! but has no sqlparser-AST → PlanNode translation. Wiring that module
//! to SQL EXPLAIN is a follow-up; for now `sql_explain` is the entry
//! point from `sql_bridge` because it operates directly on what the
//! sqlparser AST already gives us.

use std::fmt::Write;

use sqlparser::ast::{
    Expr, GroupByExpr, JoinOperator, OrderByExpr, Query as SqlQuery, Select, SelectItem, SetExpr,
    Statement, TableFactor,
};

use crate::engine::Engine;
use crate::errors::{DriftError, Result};

/// A node in the execution plan tree. Each variant carries the
/// information the formatter needs; cost/row estimates are approximate.
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Sequential scan of a base table, optionally with a filter.
    SeqScan {
        table: String,
        filter: Option<String>,
        estimated_rows: usize,
    },
    /// Nested-loop join over two inputs.
    NestedLoop {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_kind: String,
        condition: Option<String>,
        estimated_rows: usize,
    },
    /// In-memory sort step.
    Sort {
        input: Box<PlanNode>,
        keys: Vec<String>,
        estimated_rows: usize,
    },
    /// LIMIT N truncation.
    Limit {
        input: Box<PlanNode>,
        count: usize,
        estimated_rows: usize,
    },
    /// Aggregation step, possibly with GROUP BY.
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<String>,
        estimated_rows: usize,
    },
    /// SELECT DISTINCT.
    Distinct {
        input: Box<PlanNode>,
        estimated_rows: usize,
    },
    /// UNION / INTERSECT / EXCEPT.
    SetOperation {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        operation: String,
        estimated_rows: usize,
    },
    /// Filter applied above another node (used when we can't push the
    /// filter into a scan, e.g. above a join).
    Filter {
        input: Box<PlanNode>,
        predicate: String,
        estimated_rows: usize,
    },
    /// A subquery or other complex node we can't introspect further
    /// without recursing the whole executor.
    Subplan {
        label: String,
        estimated_rows: usize,
    },
}

impl PlanNode {
    fn estimated_rows(&self) -> usize {
        match self {
            PlanNode::SeqScan { estimated_rows, .. }
            | PlanNode::NestedLoop { estimated_rows, .. }
            | PlanNode::Sort { estimated_rows, .. }
            | PlanNode::Limit { estimated_rows, .. }
            | PlanNode::Aggregate { estimated_rows, .. }
            | PlanNode::Distinct { estimated_rows, .. }
            | PlanNode::SetOperation { estimated_rows, .. }
            | PlanNode::Filter { estimated_rows, .. }
            | PlanNode::Subplan { estimated_rows, .. } => *estimated_rows,
        }
    }
}

/// Build the plan tree for a `Statement`. SELECT walks recursively;
/// DML returns a synthetic single-node placeholder describing the
/// operation. DDL is intentionally unsupported — `EXPLAIN CREATE TABLE`
/// isn't meaningful.
pub fn build_plan(engine: &Engine, stmt: &Statement) -> Result<PlanNode> {
    match stmt {
        Statement::Query(q) => build_query_plan(engine, q),
        Statement::Insert(insert) => Ok(PlanNode::Subplan {
            label: format!("Insert on {}", insert.table_name),
            estimated_rows: 1,
        }),
        Statement::Update { table, .. } => Ok(PlanNode::Subplan {
            label: format!("Update on {}", extract_table_label(&table.relation)),
            estimated_rows: 0,
        }),
        Statement::Delete(_) => Ok(PlanNode::Subplan {
            label: "Delete".to_string(),
            estimated_rows: 0,
        }),
        _ => Err(DriftError::InvalidQuery(
            "EXPLAIN supports SELECT / INSERT / UPDATE / DELETE only".to_string(),
        )),
    }
}

fn build_query_plan(engine: &Engine, query: &SqlQuery) -> Result<PlanNode> {
    // Recurse into the SetExpr — handles SELECT and UNION/INTERSECT/EXCEPT.
    let mut root = build_body_plan(engine, &query.body)?;

    // ORDER BY wraps as a Sort step. sqlparser 0.51 stores ORDER BY as
    // `Option<OrderBy>` where OrderBy.exprs is the list of keys.
    if let Some(order_by) = &query.order_by {
        if !order_by.exprs.is_empty() {
            let keys: Vec<String> = order_by.exprs.iter().map(format_order_key).collect();
            let rows = root.estimated_rows();
            root = PlanNode::Sort {
                input: Box::new(root),
                keys,
                estimated_rows: rows,
            };
        }
    }

    // LIMIT wraps last.
    if let Some(limit_expr) = &query.limit {
        let count = match limit_expr {
            Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse::<usize>().unwrap_or(0),
            _ => 0,
        };
        // The LIMIT is the upper bound on rows actually returned.
        let rows = count.min(root.estimated_rows().max(count));
        root = PlanNode::Limit {
            input: Box::new(root),
            count,
            estimated_rows: rows,
        };
    }

    Ok(root)
}

fn build_body_plan(engine: &Engine, body: &SetExpr) -> Result<PlanNode> {
    match body {
        SetExpr::Select(select) => build_select_plan(engine, select),
        SetExpr::SetOperation {
            op, left, right, ..
        } => {
            let left_plan = build_body_plan(engine, left)?;
            let right_plan = build_body_plan(engine, right)?;
            let rows = left_plan.estimated_rows() + right_plan.estimated_rows();
            Ok(PlanNode::SetOperation {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                operation: format!("{:?}", op),
                estimated_rows: rows,
            })
        }
        SetExpr::Query(inner) => build_query_plan(engine, inner),
        // VALUES lists, table-function calls, INSERT subqueries — bucket
        // into a single Subplan node rather than special-casing each.
        _ => Ok(PlanNode::Subplan {
            label: "Subquery".to_string(),
            estimated_rows: 0,
        }),
    }
}

fn build_select_plan(engine: &Engine, select: &Select) -> Result<PlanNode> {
    // Start with the FROM clause — left table, then fold each JOIN.
    let mut root = if let Some(first) = select.from.first() {
        let mut node = build_table_factor_plan(engine, &first.relation)?;
        for join in &first.joins {
            let right = build_table_factor_plan(engine, &join.relation)?;
            let (join_kind, condition) = describe_join(&join.join_operator);
            // Worst-case nested-loop estimate; refinements can come later.
            let rows = node.estimated_rows().max(right.estimated_rows());
            node = PlanNode::NestedLoop {
                left: Box::new(node),
                right: Box::new(right),
                join_kind,
                condition,
                estimated_rows: rows,
            };
        }
        node
    } else {
        // SELECT without FROM (e.g. `SELECT 1`) — single result row.
        PlanNode::Subplan {
            label: "Result".to_string(),
            estimated_rows: 1,
        }
    };

    // WHERE pushes into the underlying scan when the root is still a single
    // SeqScan; over a join, we surface it as a Filter wrapper node.
    if let Some(selection) = &select.selection {
        let filter_text = format_expr(selection);
        if let PlanNode::SeqScan { ref mut filter, .. } = root {
            *filter = Some(filter_text);
        } else {
            let rows = root.estimated_rows();
            root = PlanNode::Filter {
                input: Box::new(root),
                predicate: filter_text,
                estimated_rows: rows,
            };
        }
    }

    // GROUP BY / aggregate functions.
    let group_keys = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs.iter().map(format_expr).collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let aggregates: Vec<String> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Function(f)) => Some(f.name.to_string().to_lowercase()),
            SelectItem::ExprWithAlias {
                expr: Expr::Function(f),
                ..
            } => Some(f.name.to_string().to_lowercase()),
            _ => None,
        })
        .collect();
    if !group_keys.is_empty() || !aggregates.is_empty() {
        // With GROUP BY: rough heuristic of 10:1 reduction. Without (scalar
        // aggregate over the whole input): a single row.
        let rows = if !group_keys.is_empty() {
            (root.estimated_rows() / 10).max(1)
        } else {
            1
        };
        root = PlanNode::Aggregate {
            input: Box::new(root),
            group_by: group_keys,
            aggregates,
            estimated_rows: rows,
        };
    }

    // DISTINCT.
    if select.distinct.is_some() {
        let rows = (root.estimated_rows() / 2).max(1);
        root = PlanNode::Distinct {
            input: Box::new(root),
            estimated_rows: rows,
        };
    }

    Ok(root)
}

fn build_table_factor_plan(engine: &Engine, tf: &TableFactor) -> Result<PlanNode> {
    let label = extract_table_label(tf);
    // Strip alias for the lookup; the label may include " alias" for display.
    let table_for_lookup = match tf {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => label.clone(),
    };
    let rows = engine.get_table_data(&table_for_lookup).map(|d| d.len()).unwrap_or(0);
    Ok(PlanNode::SeqScan {
        table: label,
        filter: None,
        estimated_rows: rows,
    })
}

fn extract_table_label(tf: &TableFactor) -> String {
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

fn describe_join(op: &JoinOperator) -> (String, Option<String>) {
    let cond = match op {
        JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => match c {
            sqlparser::ast::JoinConstraint::On(expr) => Some(format_expr(expr)),
            sqlparser::ast::JoinConstraint::Using(cols) => Some(format!(
                "USING ({})",
                cols.iter()
                    .map(|c| c.value.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            _ => None,
        },
        _ => None,
    };
    let kind = match op {
        JoinOperator::Inner(_) => "Inner",
        JoinOperator::LeftOuter(_) => "Left Outer",
        JoinOperator::RightOuter(_) => "Right Outer",
        JoinOperator::FullOuter(_) => "Full Outer",
        JoinOperator::CrossJoin => "Cross",
        _ => "Other",
    }
    .to_string();
    (kind, cond)
}

fn format_order_key(expr: &OrderByExpr) -> String {
    let dir = match expr.asc {
        Some(true) | None => "ASC",
        Some(false) => "DESC",
    };
    format!("{} {}", format_expr(&expr.expr), dir)
}

/// Render an expression as compact display text. Not intended to be
/// round-trippable as SQL — this is EXPLAIN output. The `{:?}` fallback
/// keeps unknown expression shapes from breaking EXPLAIN.
fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(i) => i.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|p| p.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        Expr::Value(v) => format!("{}", v),
        Expr::BinaryOp { left, op, right } => {
            format!("{} {} {}", format_expr(left), op, format_expr(right))
        }
        Expr::Function(f) => f.to_string(),
        Expr::IsNull(inner) => format!("{} IS NULL", format_expr(inner)),
        Expr::IsNotNull(inner) => format!("{} IS NOT NULL", format_expr(inner)),
        _ => format!("{:?}", expr),
    }
}

/// Format the plan as PostgreSQL-style indented text, one line per node.
/// Returns the list of lines so callers can wrap each in a result row.
pub fn format_plan(root: &PlanNode) -> Vec<String> {
    let mut out = Vec::new();
    format_node(root, &mut out, 0, true);
    out
}

fn format_node(node: &PlanNode, out: &mut Vec<String>, depth: usize, is_root: bool) {
    let indent = "  ".repeat(depth);
    let prefix = if is_root {
        String::new()
    } else {
        format!("{}->  ", indent)
    };

    match node {
        PlanNode::SeqScan {
            table,
            filter,
            estimated_rows,
        } => {
            out.push(format!(
                "{}Seq Scan on {}  (rows={})",
                prefix, table, estimated_rows
            ));
            if let Some(f) = filter {
                out.push(format!("{}  Filter: {}", indent, f));
            }
        }
        PlanNode::NestedLoop {
            left,
            right,
            join_kind,
            condition,
            estimated_rows,
        } => {
            let mut header = format!(
                "{}Nested Loop ({} Join)  (rows={})",
                prefix, join_kind, estimated_rows
            );
            if let Some(c) = condition {
                let _ = write!(header, "  Cond: {}", c);
            }
            out.push(header);
            format_node(left, out, depth + 1, false);
            format_node(right, out, depth + 1, false);
        }
        PlanNode::Sort {
            input,
            keys,
            estimated_rows,
        } => {
            out.push(format!("{}Sort  (rows={})", prefix, estimated_rows));
            out.push(format!("{}  Sort Key: {}", indent, keys.join(", ")));
            format_node(input, out, depth + 1, false);
        }
        PlanNode::Limit {
            input,
            count,
            estimated_rows,
        } => {
            out.push(format!(
                "{}Limit  (count={}, rows={})",
                prefix, count, estimated_rows
            ));
            format_node(input, out, depth + 1, false);
        }
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
            estimated_rows,
        } => {
            let header = if group_by.is_empty() {
                format!("{}Aggregate  (rows={})", prefix, estimated_rows)
            } else {
                format!("{}GroupAggregate  (rows={})", prefix, estimated_rows)
            };
            out.push(header);
            if !group_by.is_empty() {
                out.push(format!("{}  Group Key: {}", indent, group_by.join(", ")));
            }
            if !aggregates.is_empty() {
                out.push(format!(
                    "{}  Aggregates: {}",
                    indent,
                    aggregates.join(", ")
                ));
            }
            format_node(input, out, depth + 1, false);
        }
        PlanNode::Distinct {
            input,
            estimated_rows,
        } => {
            out.push(format!("{}Unique  (rows={})", prefix, estimated_rows));
            format_node(input, out, depth + 1, false);
        }
        PlanNode::SetOperation {
            left,
            right,
            operation,
            estimated_rows,
        } => {
            out.push(format!(
                "{}{}  (rows={})",
                prefix, operation, estimated_rows
            ));
            format_node(left, out, depth + 1, false);
            format_node(right, out, depth + 1, false);
        }
        PlanNode::Filter {
            input,
            predicate,
            estimated_rows,
        } => {
            out.push(format!("{}Filter  (rows={})", prefix, estimated_rows));
            out.push(format!("{}  Filter: {}", indent, predicate));
            format_node(input, out, depth + 1, false);
        }
        PlanNode::Subplan {
            label,
            estimated_rows,
        } => {
            out.push(format!("{}{}  (rows={})", prefix, label, estimated_rows));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use tempfile::TempDir;

    fn plan_for(sql: &str) -> Vec<String> {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::init(tmp.path()).unwrap();
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let plan = build_plan(&engine, &stmts[0]).unwrap();
        format_plan(&plan)
    }

    #[test]
    fn plan_select_with_filter_renders_seq_scan_with_filter() {
        let lines = plan_for("SELECT * FROM users WHERE id = 1");
        let joined = lines.join("\n");
        assert!(joined.contains("Seq Scan on users"));
        assert!(joined.contains("Filter: id = "));
    }

    #[test]
    fn plan_select_with_order_by_and_limit_nests_sort_and_limit() {
        let lines = plan_for("SELECT * FROM users ORDER BY name DESC LIMIT 5");
        let joined = lines.join("\n");
        assert!(joined.starts_with("Limit"));
        assert!(joined.contains("Sort"));
        assert!(joined.contains("Sort Key: name DESC"));
        assert!(joined.contains("->  Seq Scan on users"));
    }

    #[test]
    fn plan_select_with_group_by_renders_group_aggregate() {
        let lines = plan_for(
            "SELECT department, COUNT(*) FROM employees GROUP BY department",
        );
        let joined = lines.join("\n");
        assert!(joined.contains("GroupAggregate"));
        assert!(joined.contains("Group Key: department"));
        assert!(joined.contains("Aggregates: count"));
    }

    #[test]
    fn plan_select_with_join_renders_nested_loop() {
        let lines = plan_for(
            "SELECT * FROM users u JOIN posts p ON u.id = p.user_id WHERE u.id = 1",
        );
        let joined = lines.join("\n");
        assert!(joined.contains("Nested Loop"));
        assert!(joined.contains("Cond:"));
        // Filter applies above the join, surfaced as a Filter wrapper.
        assert!(joined.contains("Filter"));
    }

    #[test]
    fn plan_set_operation_renders_set_op_with_two_children() {
        let lines = plan_for("SELECT id FROM a UNION SELECT id FROM b");
        let joined = lines.join("\n");
        assert!(joined.contains("Union"));
        assert!(joined.matches("Seq Scan").count() == 2);
    }
}
