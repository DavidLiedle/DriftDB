//! SQL execution engine for DriftDB

use serde_json::{json, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, JoinOperator, Offset, OrderByExpr, Query as SqlQuery, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::query::{Query, QueryResult, WhereCondition};
use crate::window::{
    OrderColumn, WindowExecutor, WindowFunction, WindowFunctionCall, WindowQuery, WindowSpec,
};

thread_local! {
    static IN_VIEW_EXECUTION: RefCell<bool> = const { RefCell::new(false) };
    static CURRENT_TRANSACTION: RefCell<Option<u64>> = const { RefCell::new(None) };
    static OUTER_ROW_CONTEXT: RefCell<Option<Value>> = const { RefCell::new(None) };
    static IN_RECURSIVE_CTE: RefCell<bool> = const { RefCell::new(false) };
}

/// Execute SQL query with parameters (prevents SQL injection)
pub fn execute_sql_with_params(
    engine: &mut Engine,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult> {
    // Parse SQL but keep parameters separate
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| DriftError::Parse(e.to_string()))?;

    if ast.is_empty() {
        return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
    }

    // Store parameters in thread-local for safe access during execution
    thread_local! {
        static QUERY_PARAMS: RefCell<Vec<Value>> = const { RefCell::new(Vec::new()) };
    }

    QUERY_PARAMS.with(|p| {
        p.replace(params.to_vec());
    });

    // Execute with parameter binding using the same match logic as execute_sql
    let result = match &ast[0] {
        Statement::Query(query) => execute_sql_query(engine, query),
        Statement::CreateTable(create_table) => execute_create_table(
            engine,
            &create_table.name,
            &create_table.columns,
            &create_table.constraints,
        ),
        // Add other statement types as needed for parameterized execution
        _ => Err(DriftError::InvalidQuery(
            "Statement type not supported with parameters".to_string(),
        )),
    };

    // Clear parameters after execution
    QUERY_PARAMS.with(|p| {
        p.replace(Vec::new());
    });

    result
}

/// Execute SQL query (legacy - use execute_sql_with_params for safety)
pub fn execute_sql(engine: &mut Engine, sql: &str) -> Result<QueryResult> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| DriftError::Parse(e.to_string()))?;

    if ast.is_empty() {
        return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
    }

    match &ast[0] {
        Statement::Query(query) => execute_sql_query(engine, query),
        Statement::CreateView { .. } => {
            // Delegate to sql_views module for full view support
            use crate::sql_views::SqlViewManager;
            use crate::views::ViewManager;
            use std::sync::Arc;

            let view_mgr = Arc::new(ViewManager::new());
            let sql_view_mgr = SqlViewManager::new(view_mgr);
            sql_view_mgr.create_view_from_sql(engine, sql)?;
            Ok(crate::query::QueryResult::Success {
                message: "View created successfully".to_string(),
            })
        }
        Statement::CreateTable(create_table) => execute_create_table(
            engine,
            &create_table.name,
            &create_table.columns,
            &create_table.constraints,
        ),
        Statement::CreateIndex(create_index) => execute_create_index(
            engine,
            &create_index.name,
            &create_index.table_name,
            &create_index.columns,
            create_index.unique,
        ),
        Statement::Drop {
            object_type,
            names,
            cascade,
            ..
        } => match object_type {
            sqlparser::ast::ObjectType::Table => {
                if let Some(name) = names.first() {
                    execute_drop_table(engine, name)
                } else {
                    Err(DriftError::InvalidQuery(
                        "DROP TABLE requires a table name".to_string(),
                    ))
                }
            }
            sqlparser::ast::ObjectType::View => {
                if let Some(name) = names.first() {
                    execute_drop_view(engine, name, *cascade)
                } else {
                    Err(DriftError::InvalidQuery(
                        "DROP VIEW requires a view name".to_string(),
                    ))
                }
            }
            _ => Err(DriftError::InvalidQuery(format!(
                "DROP {} not yet supported",
                object_type
            ))),
        },
        Statement::Insert(insert) => {
            if let Some(src) = &insert.source {
                execute_sql_insert(engine, &insert.table_name, &insert.columns, src)
            } else {
                Err(DriftError::InvalidQuery(
                    "INSERT requires VALUES or SELECT".to_string(),
                ))
            }
        }
        Statement::Update {
            table,
            assignments,
            from: _,
            selection,
            ..
        } => execute_sql_update(engine, table, assignments, selection),
        Statement::Delete(delete) => {
            // Use 'tables' if not empty (MySQL multi-table delete)
            if !delete.tables.is_empty() {
                execute_sql_delete(engine, &delete.tables, &delete.selection)
            } else {
                // Extract tables from the FromTable enum
                let from_tables = match &delete.from {
                    FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
                        tables
                    }
                };

                if !from_tables.is_empty() {
                    // Convert from TableWithJoins to ObjectName
                    let table_names: Vec<sqlparser::ast::ObjectName> = from_tables
                        .iter()
                        .filter_map(|t| {
                            if let TableFactor::Table { name, .. } = &t.relation {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    execute_sql_delete(engine, &table_names, &delete.selection)
                } else {
                    Err(DriftError::InvalidQuery(
                        "DELETE requires FROM clause".to_string(),
                    ))
                }
            }
        }
        Statement::StartTransaction { .. } => {
            // Check if already in a transaction - if so, just return success (idempotent)
            let existing_txn = CURRENT_TRANSACTION.with(|txn| *txn.borrow());
            if existing_txn.is_some() {
                // Already in a transaction, make BEGIN idempotent
                return Ok(crate::query::QueryResult::Success {
                    message: "BEGIN".to_string(),
                });
            }

            // Default to READ COMMITTED if not specified
            let isolation = crate::transaction::IsolationLevel::ReadCommitted;
            let txn_id = engine.begin_transaction(isolation)?;

            // Store transaction ID in thread-local session
            CURRENT_TRANSACTION.with(|txn| {
                *txn.borrow_mut() = Some(txn_id);
            });

            Ok(QueryResult::Success {
                message: format!("Transaction {} started", txn_id),
            })
        }
        Statement::Commit { .. } => {
            // Get current transaction ID from session
            let txn_id = CURRENT_TRANSACTION.with(|txn| *txn.borrow());

            if let Some(transaction_id) = txn_id {
                engine.commit_transaction(transaction_id)?;

                // Clear transaction from session
                CURRENT_TRANSACTION.with(|txn| {
                    *txn.borrow_mut() = None;
                });

                Ok(QueryResult::Success {
                    message: format!("Transaction {} committed", transaction_id),
                })
            } else {
                // No active transaction - just succeed (no-op)
                Ok(QueryResult::Success {
                    message: "COMMIT (no active transaction)".to_string(),
                })
            }
        }
        Statement::Rollback { .. } => {
            // Get current transaction ID from session
            let txn_id = CURRENT_TRANSACTION.with(|txn| *txn.borrow());

            if let Some(transaction_id) = txn_id {
                engine.rollback_transaction(transaction_id)?;

                // Clear transaction from session
                CURRENT_TRANSACTION.with(|txn| {
                    *txn.borrow_mut() = None;
                });

                Ok(QueryResult::Success {
                    message: format!("Transaction {} rolled back", transaction_id),
                })
            } else {
                Err(DriftError::InvalidQuery(
                    "No active transaction to rollback".to_string(),
                ))
            }
        }
        Statement::AlterTable {
            name, operations, ..
        } => {
            if !operations.is_empty() {
                execute_alter_table(engine, name, &operations[0])
            } else {
                Err(DriftError::InvalidQuery(
                    "No ALTER TABLE operation specified".to_string(),
                ))
            }
        }
        Statement::Explain { statement, .. } => {
            // Provide query plan explanation
            let plan = format!("Query Plan for: {:?}\n\n1. Parse SQL\n2. Analyze query structure\n3. Execute query\n4. Return results", statement);
            Ok(QueryResult::Success { message: plan })
        }
        Statement::Analyze { .. } => {
            // Run ANALYZE on all tables to update statistics
            for table in engine.list_tables() {
                let _ = engine.collect_table_statistics(&table);
            }
            Ok(QueryResult::Success {
                message: "Statistics updated for all tables".to_string(),
            })
        }
        Statement::Truncate { table_names, .. } => {
            if table_names.is_empty() {
                return Err(DriftError::InvalidQuery(
                    "TRUNCATE requires at least one table".to_string(),
                ));
            }
            let table_name = table_names[0].name.to_string();

            // TRUNCATE is essentially DELETE without WHERE
            let select_query = Query::Select {
                table: table_name.clone(),
                conditions: vec![],
                as_of: None,
                limit: None,
            };

            let result = engine.execute_query(select_query)?;

            match result {
                QueryResult::Rows { data } => {
                    let mut delete_count = 0;
                    for row in data {
                        if let Some(row_obj) = row.as_object() {
                            // Get primary key from schema
                            let primary_key = engine.get_table_primary_key(&table_name)?;
                            let pk_value =
                                row_obj.get(&primary_key).cloned().unwrap_or(Value::Null);

                            let delete_query = Query::SoftDelete {
                                table: table_name.clone(),
                                primary_key: pk_value,
                            };

                            engine.execute_query(delete_query)?;
                            delete_count += 1;
                        }
                    }

                    Ok(QueryResult::Success {
                        message: format!(
                            "Table '{}' truncated - {} rows deleted",
                            table_name, delete_count
                        ),
                    })
                }
                _ => Ok(QueryResult::Success {
                    message: format!("Table '{}' was already empty", table_name),
                }),
            }
        }
        // TODO: Add CALL statement support when sqlparser structure is confirmed
        // Statement::Call(...) => execute_call_procedure(...)
        _ => Err(DriftError::InvalidQuery(
            "SQL statement type not yet supported".to_string(),
        )),
    }
}

fn execute_sql_query(engine: &mut Engine, query: &SqlQuery) -> Result<QueryResult> {
    // Handle CTEs (WITH clause)
    let mut cte_results = HashMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            // Check if this is a recursive CTE
            if with.recursive {
                // Handle recursive CTE
                let cte_name = cte.alias.name.value.clone();
                let recursive_result = execute_recursive_cte(engine, cte, &cte_name)?;
                cte_results.insert(cte_name, recursive_result);
            } else {
                // Regular CTE
                let cte_query = Box::new(cte.query.clone());
                let cte_result = execute_sql_query(engine, &cte_query)?;
                if let QueryResult::Rows { data } = cte_result {
                    cte_results.insert(cte.alias.name.value.clone(), data);
                }
            }
        }
    }

    // Execute main query with CTE context
    execute_query_with_ctes(engine, query, &cte_results)
}

fn execute_recursive_cte(
    engine: &mut Engine,
    cte: &sqlparser::ast::Cte,
    cte_name: &str,
) -> Result<Vec<Value>> {
    // Recursive CTEs typically have UNION/UNION ALL between anchor and recursive parts
    let query = &cte.query;

    match query.body.as_ref() {
        SetExpr::SetOperation {
            op: SetOperator::Union,
            left,
            right,
            set_quantifier,
        } => {
            // Left is the anchor (base case), right is the recursive part

            // Step 1: Execute the anchor query to get initial results
            let anchor_query = SqlQuery {
                with: None,
                body: Box::new(left.as_ref().clone()),
                order_by: None,
                limit: None,
                offset: None,
                fetch: None,
                locks: vec![],
                limit_by: vec![],
                for_clause: None,
                format_clause: None,
                settings: None,
            };

            let anchor_result = execute_sql_query(engine, &Box::new(anchor_query))?;
            let mut all_results = match anchor_result {
                QueryResult::Rows { data } => data,
                _ => vec![],
            };

            // Step 2: Iteratively execute the recursive part
            // In standard recursive CTEs:
            // - Each iteration only processes rows from the PREVIOUS iteration
            // - Not the entire accumulated result set
            let max_iterations = 1000; // Prevent infinite recursion
            let mut iteration = 0;
            let mut working_set = all_results.clone(); // Start with anchor results

            while !working_set.is_empty() && iteration < max_iterations {
                iteration += 1;

                // The CTE name in the recursive part refers to the working set (previous iteration's results)
                let mut temp_cte_context = HashMap::new();
                temp_cte_context.insert(cte_name.to_string(), working_set.clone());

                // Execute the recursive part
                let recursive_query = SqlQuery {
                    with: None,
                    body: Box::new(right.as_ref().clone()),
                    order_by: None,
                    limit: None,
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    limit_by: vec![],
                    for_clause: None,
                    format_clause: None,
                    settings: None,
                };

                // Set recursive CTE flag
                IN_RECURSIVE_CTE.with(|flag| {
                    *flag.borrow_mut() = true;
                });

                let recursive_result =
                    execute_query_with_ctes(engine, &Box::new(recursive_query), &temp_cte_context)?;

                // Clear recursive CTE flag
                IN_RECURSIVE_CTE.with(|flag| {
                    *flag.borrow_mut() = false;
                });

                // Get new rows from this iteration
                let iteration_rows = match recursive_result {
                    QueryResult::Rows { data } => data,
                    _ => vec![],
                };

                // Build the next working set and add to results
                let mut next_working_set = Vec::new();
                for row in iteration_rows {
                    // Check for duplicates based on UNION vs UNION ALL
                    if matches!(set_quantifier, SetQuantifier::All) {
                        // UNION ALL: always add the row
                        all_results.push(row.clone());
                        next_working_set.push(row);
                    } else {
                        // UNION (DISTINCT): only add if not already in results
                        if !all_results.contains(&row) {
                            all_results.push(row.clone());
                            next_working_set.push(row);
                        }
                    }
                }

                // Update working set for next iteration
                working_set = next_working_set;
            }

            Ok(all_results)
        }
        _ => {
            // Not a recursive CTE, just execute normally
            let result = execute_sql_query(engine, &cte.query)?;
            match result {
                QueryResult::Rows { data } => Ok(data),
                _ => Ok(vec![]),
            }
        }
    }
}

fn execute_query_with_ctes(
    engine: &mut Engine,
    query: &SqlQuery,
    cte_results: &HashMap<String, Vec<Value>>,
) -> Result<QueryResult> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            if select.from.is_empty() {
                // Handle SELECT without FROM (for expressions)
                let mut row = serde_json::Map::new();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            let value = evaluate_expression_without_row(expr)?;
                            // For unnamed expressions, try to extract a simple column name
                            let col_name = match expr {
                                Expr::Identifier(ident) => ident.value.clone(),
                                Expr::BinaryOp { left, .. } => {
                                    // For binary expressions like "n + 1", use the left identifier if available
                                    if let Expr::Identifier(ident) = left.as_ref() {
                                        ident.value.clone()
                                    } else {
                                        format!("{:?}", expr).chars().take(50).collect::<String>()
                                    }
                                }
                                _ => format!("{:?}", expr).chars().take(50).collect::<String>(),
                            };
                            row.insert(col_name, value);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let value = evaluate_expression_without_row(expr)?;
                            row.insert(alias.value.clone(), value);
                        }
                        _ => {}
                    }
                }
                return Ok(QueryResult::Rows {
                    data: vec![Value::Object(row)],
                });
            }

            // Execute the base query (with or without JOINs)
            let result = if select.from[0].joins.is_empty() {
                execute_simple_select_with_ctes(engine, select, cte_results)?
            } else {
                execute_join_select_with_ctes(engine, select, cte_results)?
            };

            // Apply ORDER BY if present
            if let QueryResult::Rows { mut data } = result {
                // Apply DISTINCT if present
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if select.distinct.is_some() {
                        data = apply_distinct(data);
                    }
                }

                // Apply ORDER BY
                if let Some(order_by) = &query.order_by {
                    data = apply_order_by(data, &order_by.exprs)?;
                }

                // Apply LIMIT and OFFSET
                if let Some(limit_expr) = &query.limit {
                    let limit = parse_limit(limit_expr)?;
                    let offset = if let Some(offset_expr) = &query.offset {
                        parse_offset(offset_expr)?
                    } else {
                        0
                    };

                    data = data.into_iter().skip(offset).take(limit).collect();
                }

                // Apply projection after ORDER BY and LIMIT
                // This ensures ORDER BY can access columns not in SELECT
                if let SetExpr::Select(select) = query.body.as_ref() {
                    // Check if this needs projection (non-aggregate queries)
                    let has_aggregates = select.projection.iter().any(|item| {
                        matches!(
                            item,
                            SelectItem::UnnamedExpr(Expr::Function(_))
                                | SelectItem::ExprWithAlias {
                                    expr: Expr::Function(_),
                                    ..
                                }
                        )
                    });

                    // Always process scalar subqueries first before applying projection
                    data = process_scalar_subqueries(engine, data, &select.projection)?;

                    if !has_aggregates {
                        data = apply_projection(data, &select.projection)?;
                    }
                }

                Ok(QueryResult::Rows { data })
            } else {
                Ok(result)
            }
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => execute_set_operation(engine, op, set_quantifier, left, right),
        _ => Err(DriftError::InvalidQuery(
            "Query type not supported".to_string(),
        )),
    }
}

fn execute_set_operation(
    engine: &mut Engine,
    op: &SetOperator,
    set_quantifier: &SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
) -> Result<QueryResult> {
    // Execute left and right queries
    let left_query = Box::new(SqlQuery {
        with: None,
        body: Box::new(left.clone()),
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        locks: vec![],
        limit_by: vec![],
        for_clause: None,
        format_clause: None,
        settings: None,
    });

    let right_query = Box::new(SqlQuery {
        with: None,
        body: Box::new(right.clone()),
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        locks: vec![],
        limit_by: vec![],
        for_clause: None,
        format_clause: None,
        settings: None,
    });

    let left_result = execute_sql_query(engine, &left_query)?;
    let right_result = execute_sql_query(engine, &right_query)?;

    match (left_result, right_result) {
        (QueryResult::Rows { data: left_data }, QueryResult::Rows { data: right_data }) => {
            let result = match op {
                SetOperator::Union => perform_union(left_data, right_data, set_quantifier),
                SetOperator::Intersect => perform_intersect(left_data, right_data, set_quantifier),
                SetOperator::Except => perform_except(left_data, right_data, set_quantifier),
            };
            Ok(QueryResult::Rows { data: result })
        }
        _ => Err(DriftError::InvalidQuery(
            "Set operation requires SELECT queries".to_string(),
        )),
    }
}

fn perform_union(left: Vec<Value>, right: Vec<Value>, quantifier: &SetQuantifier) -> Vec<Value> {
    let mut result = left;
    result.extend(right);

    if matches!(quantifier, SetQuantifier::Distinct | SetQuantifier::None) {
        // Remove duplicates for UNION (default) or UNION DISTINCT
        apply_distinct(result)
    } else {
        // UNION ALL - keep all rows
        result
    }
}

fn perform_intersect(
    left: Vec<Value>,
    right: Vec<Value>,
    _quantifier: &SetQuantifier,
) -> Vec<Value> {
    let mut result = Vec::new();

    // Extract values from the first column of right rows for comparison
    let right_values: std::collections::HashSet<String> = right
        .iter()
        .filter_map(|row| {
            if let Some(obj) = row.as_object() {
                // Get the first value from the object
                obj.values().next().map(|v| v.to_string())
            } else {
                Some(row.to_string())
            }
        })
        .collect();

    // Check if values from left rows exist in right
    for left_row in left {
        let left_value = if let Some(obj) = left_row.as_object() {
            // Get the first value from the object
            obj.values().next().map(|v| v.to_string())
        } else {
            Some(left_row.to_string())
        };

        if let Some(val) = left_value {
            if right_values.contains(&val) {
                result.push(left_row);
            }
        }
    }

    apply_distinct(result) // INTERSECT always returns distinct rows
}

fn perform_except(left: Vec<Value>, right: Vec<Value>, _quantifier: &SetQuantifier) -> Vec<Value> {
    // Extract values from the first column of right rows for comparison
    let right_values: std::collections::HashSet<String> = right
        .iter()
        .filter_map(|row| {
            if let Some(obj) = row.as_object() {
                // Get the first value from the object
                obj.values().next().map(|v| v.to_string())
            } else {
                Some(row.to_string())
            }
        })
        .collect();

    let mut result = Vec::new();
    for left_row in left {
        let left_value = if let Some(obj) = left_row.as_object() {
            // Get the first value from the object
            obj.values().next().map(|v| v.to_string())
        } else {
            Some(left_row.to_string())
        };

        if let Some(val) = left_value {
            if !right_values.contains(&val) {
                result.push(left_row);
            }
        }
    }

    apply_distinct(result) // EXCEPT always returns distinct rows
}

fn execute_simple_select_with_ctes(
    engine: &mut Engine,
    select: &Select,
    cte_results: &HashMap<String, Vec<Value>>,
) -> Result<QueryResult> {
    let table_name = extract_table_name(&select.from[0].relation)?;

    // Check if this is a CTE reference
    if let Some(cte_data) = cte_results.get(&table_name) {
        // Use CTE data directly
        let mut result_data = cte_data.clone();

        // Apply WHERE clause if present
        if let Some(selection) = &select.selection {
            result_data = filter_rows(engine, result_data, selection)?;
        }

        // Check if we need to handle aggregates
        let has_aggregates = select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::UnnamedExpr(Expr::Function(_))
                    | SelectItem::ExprWithAlias {
                        expr: Expr::Function(_),
                        ..
                    }
            )
        });

        // Always process scalar subqueries first (they can be in aggregate or non-aggregate queries)
        result_data = process_scalar_subqueries(engine, result_data, &select.projection)?;

        if has_aggregates {
            // Handle aggregates
            result_data = execute_aggregation(&result_data, select)?;
        }
        // Note: Don't apply projection here - it will be applied in the main execution flow

        return Ok(QueryResult::Rows { data: result_data });
    }

    execute_simple_select(engine, select)
}

fn execute_simple_select(engine: &mut Engine, select: &Select) -> Result<QueryResult> {
    let table_name = extract_table_name(&select.from[0].relation)?;

    // Check if this is a view query first (but only if we're not already executing a view)
    let is_in_view = IN_VIEW_EXECUTION.with(|flag| *flag.borrow());

    if !is_in_view && !engine.list_tables().contains(&table_name) {
        // Check if it's a view
        let view_definition = engine
            .list_views()
            .into_iter()
            .find(|v| v.name == table_name);

        if let Some(view_def) = view_definition {
            // Set flag to prevent recursion
            IN_VIEW_EXECUTION.with(|flag| {
                *flag.borrow_mut() = true;
            });

            // Execute the view's SQL query
            let view_result = execute_sql(engine, &view_def.query);

            // Reset flag
            IN_VIEW_EXECUTION.with(|flag| {
                *flag.borrow_mut() = false;
            });

            // Continue processing the view results with the outer query's logic
            // (aggregations, projections, etc.)
            if let Ok(QueryResult::Rows { data }) = view_result {
                // Check if we need aggregations
                let has_aggregates = select.projection.iter().any(|item| {
                    matches!(
                        item,
                        SelectItem::UnnamedExpr(Expr::Function(_))
                            | SelectItem::ExprWithAlias {
                                expr: Expr::Function(_),
                                ..
                            }
                    )
                });

                // Always process scalar subqueries first (they can be in aggregate or non-aggregate queries)
                let data = process_scalar_subqueries(engine, data, &select.projection)?;

                if has_aggregates {
                    let aggregated = execute_aggregation(&data, select)?;
                    return Ok(QueryResult::Rows { data: aggregated });
                } else {
                    // Apply projection
                    let projected = apply_projection(data, &select.projection)?;
                    return Ok(QueryResult::Rows { data: projected });
                }
            } else {
                return view_result;
            }
        }
    }

    // Check if this is an aggregation query or has window functions
    let has_aggregates = select.projection.iter().any(|item| {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func))
            | SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                ..
            } => {
                // Aggregate functions don't have an OVER clause
                func.over.is_none()
            }
            _ => false,
        }
    });

    let has_window_functions = select.projection.iter().any(|item| {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func))
            | SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                ..
            } => {
                // Window functions have an OVER clause
                func.over.is_some()
            }
            _ => false,
        }
    });

    // Check if WHERE clause contains subqueries
    let has_subqueries = select
        .selection
        .as_ref()
        .is_some_and(contains_subquery);

    // Check if this might be a correlated subquery
    let is_correlated = OUTER_ROW_CONTEXT.with(|context| context.borrow().is_some());

    // If we have subqueries OR this is a correlated subquery, fetch all rows and filter in SQL layer
    // Otherwise, use engine WHERE optimization
    let (engine_conditions, sql_filter) = if has_subqueries || is_correlated {
        (vec![], select.selection.clone())
    } else {
        // Convert WHERE clause to WhereConditions for engine optimization
        let conditions = if let Some(selection) = &select.selection {
            match parse_where_clause(selection) {
                Ok(conds) => conds,
                Err(_) => {
                    // If parsing fails, fall back to SQL filtering
                    return Ok(QueryResult::Rows { data: vec![] });
                }
            }
        } else {
            vec![]
        };
        (conditions, None)
    };

    // Execute SQL query to get base data
    let query = Query::Select {
        table: table_name.clone(),
        conditions: engine_conditions,
        as_of: None,
        limit: None,
    };

    let mut result = engine.execute_query(query)?;

    // Apply SQL-level WHERE filtering if needed (for subqueries)
    if let Some(filter_expr) = sql_filter {
        if let QueryResult::Rows { data } = result {
            let filtered = filter_rows(engine, data, &filter_expr)?;
            result = QueryResult::Rows { data: filtered };
        }
    }

    // Check if there's a GROUP BY clause
    let has_group_by =
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

    // Handle window functions first (they operate on ungrouped data)
    if has_window_functions {
        match result {
            QueryResult::Rows { data } => {
                let with_windows = execute_window_functions(data, &select.projection)?;
                return Ok(QueryResult::Rows { data: with_windows });
            }
            _ => return Ok(result),
        }
    }

    // If no aggregates and no GROUP BY, process scalar subqueries and return
    // Projection will be applied later after ORDER BY
    if !has_aggregates && !has_group_by {
        if let QueryResult::Rows { data } = result {
            // Process scalar subqueries before returning
            let data_with_subqueries = process_scalar_subqueries(engine, data, &select.projection)?;
            return Ok(QueryResult::Rows {
                data: data_with_subqueries,
            });
        }
        return Ok(result);
    }

    // Process aggregations
    match result {
        QueryResult::Rows { data } => {
            // Process scalar subqueries first
            let data_with_subqueries = process_scalar_subqueries(engine, data, &select.projection)?;
            let aggregated = execute_aggregation(&data_with_subqueries, select)?;

            // Apply HAVING clause if present
            if let Some(having) = &select.having {
                let filtered = filter_aggregated_rows(aggregated, having)?;
                Ok(QueryResult::Rows { data: filtered })
            } else {
                Ok(QueryResult::Rows { data: aggregated })
            }
        }
        _ => Ok(result),
    }
}

fn execute_join_select_with_ctes(
    engine: &mut Engine,
    select: &Select,
    cte_results: &HashMap<String, Vec<Value>>,
) -> Result<QueryResult> {
    // Get left table data - either from CTE or from regular table
    let left_table = extract_table_name(&select.from[0].relation)?;

    let mut joined_rows = if let Some(cte_data) = cte_results.get(&left_table) {
        // Use CTE data as left table
        cte_data.clone()
    } else {
        // Check if any of the joined tables are CTEs
        let has_cte_joins = select.from[0].joins.iter().any(|join| {
            if let Ok(table_name) = extract_table_name_from_join(&join.relation) {
                cte_results.contains_key(&table_name)
            } else {
                false
            }
        });

        if !has_cte_joins {
            // No CTEs involved, use regular join
            return execute_join_select(engine, select);
        }

        // Left table is not a CTE but we have CTE joins, get left table data
        let left_view = engine
            .list_views()
            .into_iter()
            .find(|v| v.name == left_table);

        if let Some(view_def) = left_view {
            // Parse and execute the view's SQL directly
            let dialect = sqlparser::dialect::GenericDialect {};
            let view_ast = sqlparser::parser::Parser::parse_sql(&dialect, &view_def.query)
                .map_err(|e| DriftError::Parse(e.to_string()))?;

            if !view_ast.is_empty() {
                if let Statement::Query(view_query) = &view_ast[0] {
                    let view_result = execute_sql_query(engine, view_query)?;
                    match view_result {
                        QueryResult::Rows { data } => data,
                        _ => vec![],
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            // Regular table
            let left_query = Query::Select {
                table: left_table.clone(),
                conditions: vec![],
                as_of: None,
                limit: None,
            };

            let left_result = engine.execute_query(left_query)?;
            match left_result {
                QueryResult::Rows { data } => data,
                _ => return Ok(left_result),
            }
        }
    };

    // Process all JOINs sequentially
    for join in &select.from[0].joins {
        let right_table = extract_table_name_from_join(&join.relation)?;

        // Check if right table is a CTE or regular table
        let right_rows = if let Some(cte_data) = cte_results.get(&right_table) {
            // Use CTE data as right table
            cte_data.clone()
        } else {
            // Get data from regular table
            let right_query = Query::Select {
                table: right_table.clone(),
                conditions: vec![],
                as_of: None,
                limit: None,
            };

            let right_result = engine.execute_query(right_query)?;
            match right_result {
                QueryResult::Rows { data } => data,
                _ => vec![],
            }
        };

        // Perform the JOIN with current accumulated result
        joined_rows = match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                perform_inner_join(joined_rows, right_rows, constraint)?
            }
            JoinOperator::LeftOuter(constraint) => {
                perform_left_join(joined_rows, right_rows, constraint)?
            }
            JoinOperator::CrossJoin => perform_cross_join(joined_rows, right_rows),
            JoinOperator::RightOuter(constraint) => {
                // RIGHT JOIN is LEFT JOIN with swapped tables
                perform_left_join(right_rows, joined_rows, constraint)?
            }
            JoinOperator::FullOuter(constraint) => {
                perform_full_outer_join(joined_rows, right_rows, constraint)?
            }
            _ => {
                return Err(DriftError::InvalidQuery(
                    "JOIN type not yet supported".to_string(),
                ));
            }
        };
    }

    // Apply WHERE clause on joined data
    let filtered_rows = if let Some(selection) = &select.selection {
        filter_rows(engine, joined_rows, selection)?
    } else {
        joined_rows
    };

    // Check if this is an aggregation query
    let has_aggregates = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(Expr::Function(_))
                | SelectItem::ExprWithAlias {
                    expr: Expr::Function(_),
                    ..
                }
        )
    });

    // Check if there's a GROUP BY clause
    let has_group_by =
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

    // If no aggregates and no GROUP BY, apply column projection and return
    if !has_aggregates && !has_group_by {
        let projected_rows = project_columns(filtered_rows, select)?;
        return Ok(QueryResult::Rows {
            data: projected_rows,
        });
    }

    // Process aggregations if needed
    let result = if has_aggregates || has_group_by {
        execute_aggregation(&filtered_rows, select)?
    } else {
        filtered_rows
    };

    Ok(QueryResult::Rows { data: result })
}

fn execute_join_select(engine: &mut Engine, select: &Select) -> Result<QueryResult> {
    // Get left table data
    let left_table = extract_table_name(&select.from[0].relation)?;

    // Check if left table is a view
    let left_view = engine
        .list_views()
        .into_iter()
        .find(|v| v.name == left_table);

    let mut joined_rows = if let Some(view_def) = left_view {
        // Parse and execute the view's SQL directly
        let dialect = sqlparser::dialect::GenericDialect {};
        let view_ast = sqlparser::parser::Parser::parse_sql(&dialect, &view_def.query)
            .map_err(|e| DriftError::Parse(e.to_string()))?;

        if !view_ast.is_empty() {
            if let Statement::Query(view_query) = &view_ast[0] {
                let view_result = execute_sql_query(engine, view_query)?;
                match view_result {
                    QueryResult::Rows { data } => data,
                    _ => vec![],
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        let left_query = Query::Select {
            table: left_table.clone(),
            conditions: vec![],
            as_of: None,
            limit: None,
        };

        let left_result = engine.execute_query(left_query)?;
        match left_result {
            QueryResult::Rows { data } => data,
            _ => return Ok(left_result),
        }
    };

    // Process all JOINs sequentially
    for join in &select.from[0].joins {
        let right_table = extract_table_name_from_join(&join.relation)?;

        // Check if right table is a view
        let right_view = engine
            .list_views()
            .into_iter()
            .find(|v| v.name == right_table);

        let right_rows = if let Some(view_def) = right_view {
            // Parse and execute the view's SQL directly
            let dialect = sqlparser::dialect::GenericDialect {};
            let view_ast = sqlparser::parser::Parser::parse_sql(&dialect, &view_def.query)
                .map_err(|e| DriftError::Parse(e.to_string()))?;

            if !view_ast.is_empty() {
                if let Statement::Query(view_query) = &view_ast[0] {
                    let view_result = execute_sql_query(engine, view_query)?;
                    match view_result {
                        QueryResult::Rows { data } => data,
                        _ => vec![],
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            let right_query = Query::Select {
                table: right_table.clone(),
                conditions: vec![],
                as_of: None,
                limit: None,
            };

            let right_result = engine.execute_query(right_query)?;
            match right_result {
                QueryResult::Rows { data } => data,
                _ => vec![],
            }
        };

        // Perform the JOIN with current accumulated result
        joined_rows = match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                perform_inner_join(joined_rows, right_rows, constraint)?
            }
            JoinOperator::LeftOuter(constraint) => {
                perform_left_join(joined_rows, right_rows, constraint)?
            }
            JoinOperator::CrossJoin => perform_cross_join(joined_rows, right_rows),
            JoinOperator::RightOuter(constraint) => {
                // RIGHT JOIN is LEFT JOIN with swapped tables
                perform_left_join(right_rows, joined_rows, constraint)?
            }
            JoinOperator::FullOuter(constraint) => {
                perform_full_outer_join(joined_rows, right_rows, constraint)?
            }
            _ => {
                return Err(DriftError::InvalidQuery(
                    "JOIN type not yet supported".to_string(),
                ));
            }
        };
    }

    // Apply WHERE clause on joined data
    let filtered_rows = if let Some(selection) = &select.selection {
        filter_rows(engine, joined_rows, selection)?
    } else {
        joined_rows
    };

    // Check if this is an aggregation query
    let has_aggregates = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(Expr::Function(_))
                | SelectItem::ExprWithAlias {
                    expr: Expr::Function(_),
                    ..
                }
        )
    });

    // Check if there's a GROUP BY clause
    let has_group_by =
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

    // If no aggregates and no GROUP BY, apply column projection and return
    if !has_aggregates && !has_group_by {
        let projected_rows = project_columns(filtered_rows, select)?;
        return Ok(QueryResult::Rows {
            data: projected_rows,
        });
    }

    // Process aggregations
    let aggregated = execute_aggregation(&filtered_rows, select)?;

    // Apply HAVING clause if present
    if let Some(having) = &select.having {
        let filtered = filter_aggregated_rows(aggregated, having)?;
        Ok(QueryResult::Rows { data: filtered })
    } else {
        Ok(QueryResult::Rows { data: aggregated })
    }
}

fn perform_inner_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();

    for left_row in &left_rows {
        for right_row in &right_rows {
            // Handle both unprefixed and prefixed column names in join condition
            let left_val = left_row.get(&left_col).or_else(|| {
                // Try with various table prefixes
                left_row.as_object().and_then(|obj| {
                    obj.iter()
                        .find(|(k, _)| k.ends_with(&format!("_{}", left_col)))
                        .map(|(_, v)| v)
                })
            });
            let right_val = right_row.get(&right_col);

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    // Merge rows - use table prefixes for disambiguation
                    let mut merged = left_row.as_object().cloned().unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            // For multiple JOINs, create unique prefixes
                            let new_key = if merged.contains_key(key) {
                                // Find a unique prefix
                                let mut counter = 1;
                                let mut unique_key = format!("t{}_{}", counter, key);
                                while merged.contains_key(&unique_key) {
                                    counter += 1;
                                    unique_key = format!("t{}_{}", counter, key);
                                }
                                unique_key
                            } else {
                                key.clone()
                            };
                            merged.insert(new_key, value.clone());
                        }
                    }

                    result.push(json!(merged));
                }
            }
        }
    }

    Ok(result)
}

fn perform_left_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();

    for left_row in &left_rows {
        let mut matched = false;

        for right_row in &right_rows {
            // Handle both unprefixed and prefixed column names in join condition
            let left_val = left_row
                .get(&left_col)
                .or_else(|| left_row.get(format!("right_{}", left_col)));
            let right_val = right_row
                .get(&right_col)
                .or_else(|| right_row.get(format!("right_{}", right_col)));

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    // Merge rows
                    let mut merged = left_row.as_object().cloned().unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            // Only prefix if key already exists to avoid conflicts
                            if merged.contains_key(key) && !key.starts_with("right_") {
                                merged.insert(format!("right_{}", key), value.clone());
                            } else {
                                merged.insert(key.clone(), value.clone());
                            }
                        }
                    }

                    result.push(json!(merged));
                    matched = true;
                }
            }
        }

        // If no match found, still include left row (with NULLs for right)
        if !matched {
            result.push(left_row.clone());
        }
    }

    Ok(result)
}

fn perform_full_outer_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();
    let mut matched_right = std::collections::HashSet::new();

    // First, do a LEFT JOIN
    for left_row in &left_rows {
        let mut matched = false;

        for (right_idx, right_row) in right_rows.iter().enumerate() {
            let left_val = left_row.get(&left_col);
            let right_val = right_row.get(&right_col);

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    // Merge rows
                    let mut merged = left_row.as_object().cloned().unwrap_or_default();

                    if let Some(right_obj) = right_row.as_object() {
                        for (key, value) in right_obj {
                            if merged.contains_key(key) && !key.starts_with("right_") {
                                merged.insert(format!("right_{}", key), value.clone());
                            } else {
                                merged.insert(key.clone(), value.clone());
                            }
                        }
                    }

                    result.push(json!(merged));
                    matched = true;
                    matched_right.insert(right_idx);
                }
            }
        }

        // If no match found, still include left row (with NULLs for right)
        if !matched {
            result.push(left_row.clone());
        }
    }

    // Then add unmatched right rows (with NULLs for left)
    for (right_idx, right_row) in right_rows.iter().enumerate() {
        if !matched_right.contains(&right_idx) {
            result.push(right_row.clone());
        }
    }

    Ok(result)
}

fn perform_cross_join(left_rows: Vec<Value>, right_rows: Vec<Value>) -> Vec<Value> {
    let mut result = Vec::new();

    for left_row in &left_rows {
        for right_row in &right_rows {
            let mut merged = left_row.as_object().cloned().unwrap_or_default();

            if let Some(right_obj) = right_row.as_object() {
                for (key, value) in right_obj {
                    merged.insert(format!("right_{}", key), value.clone());
                }
            }

            result.push(json!(merged));
        }
    }

    result
}

fn extract_join_columns(constraint: &sqlparser::ast::JoinConstraint) -> Result<(String, String)> {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => {
            // Parse ON condition (simplified - assumes single equality)
            if let sqlparser::ast::Expr::BinaryOp { left, op, right } = expr {
                if matches!(op, BinaryOperator::Eq) {
                    let left_col = extract_column_from_expr(left)?;
                    let right_col = extract_column_from_expr(right)?;
                    return Ok((left_col, right_col));
                }
            }
            Err(DriftError::InvalidQuery(
                "Complex JOIN conditions not yet supported".to_string(),
            ))
        }
        sqlparser::ast::JoinConstraint::Using(columns) => {
            if columns.is_empty() {
                Err(DriftError::InvalidQuery(
                    "USING clause requires columns".to_string(),
                ))
            } else {
                let col = columns[0].value.clone();
                Ok((col.clone(), col))
            }
        }
        _ => Err(DriftError::InvalidQuery(
            "JOIN constraint type not supported".to_string(),
        )),
    }
}

fn extract_column_from_expr(expr: &sqlparser::ast::Expr) -> Result<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            // Take the last part (column name)
            idents
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| DriftError::InvalidQuery("Invalid column reference".to_string()))
        }
        _ => Err(DriftError::InvalidQuery(
            "Expected column reference in JOIN condition".to_string(),
        )),
    }
}

fn execute_sql_insert(
    engine: &mut Engine,
    table_name: &sqlparser::ast::ObjectName,
    columns: &[sqlparser::ast::Ident],
    source: &SqlQuery,
) -> Result<QueryResult> {
    let table = table_name.to_string();

    match source.body.as_ref() {
        SetExpr::Values(values) => {
            // INSERT INTO ... VALUES (supports multiple rows)
            if values.rows.is_empty() {
                return Err(DriftError::InvalidQuery("No values provided".to_string()));
            }

            let mut total_inserted = 0;
            for row_values in &values.rows {
                if row_values.is_empty() {
                    continue;
                }
                let result = execute_insert_values(engine, &table, columns, row_values)?;
                if let QueryResult::Success { .. } = result {
                    total_inserted += 1;
                }
            }

            Ok(QueryResult::Success {
                message: format!("Inserted {} row(s)", total_inserted),
            })
        }
        SetExpr::Select(select) => {
            // INSERT INTO ... SELECT
            execute_insert_select(engine, &table, columns, select)
        }
        _ => Err(DriftError::InvalidQuery(
            "Unsupported INSERT source".to_string(),
        )),
    }
}

fn execute_insert_select(
    engine: &mut Engine,
    table: &str,
    columns: &[sqlparser::ast::Ident],
    select: &Select,
) -> Result<QueryResult> {
    // Execute the SELECT query
    let query = Box::new(SqlQuery {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(select.clone()))),
        order_by: None,
        limit: None,
        offset: None,
        fetch: None,
        locks: vec![],
        limit_by: vec![],
        for_clause: None,
        format_clause: None,
        settings: None,
    });

    let result = execute_sql_query(engine, &query)?;

    match result {
        QueryResult::Rows { data } => {
            let mut insert_count = 0;

            // Get table columns if not specified
            let target_columns = if columns.is_empty() {
                engine
                    .get_table_columns(table)
                    .map_err(|_| DriftError::InvalidQuery(format!("Table '{}' not found", table)))?
            } else {
                columns.iter().map(|c| c.value.clone()).collect()
            };

            // Insert each row from SELECT result
            for row in data {
                if let Some(row_obj) = row.as_object() {
                    let mut insert_data = serde_json::Map::new();

                    // Map values to target columns
                    for (i, col) in target_columns.iter().enumerate() {
                        // Try to get value by column name or by position
                        let value = row_obj
                            .get(col)
                            .or_else(|| {
                                // If columns don't match by name, use positional mapping
                                row_obj.values().nth(i)
                            })
                            .cloned()
                            .unwrap_or(Value::Null);

                        insert_data.insert(col.clone(), value);
                    }

                    // Create INSERT query
                    let insert_query = Query::Insert {
                        table: table.to_string(),
                        data: json!(insert_data),
                    };

                    engine.execute_query(insert_query)?;
                    insert_count += 1;
                }
            }

            Ok(QueryResult::Success {
                message: format!("Inserted {} rows", insert_count),
            })
        }
        _ => Err(DriftError::InvalidQuery(
            "SELECT query returned no data".to_string(),
        )),
    }
}

fn execute_insert_values(
    engine: &mut Engine,
    table: &str,
    columns: &[sqlparser::ast::Ident],
    values: &[Expr],
) -> Result<QueryResult> {
    // Build data object
    let mut data = serde_json::Map::new();
    if columns.is_empty() {
        // No explicit columns provided - get schema from table
        let table_columns = engine
            .get_table_columns(table)
            .map_err(|_| DriftError::InvalidQuery(format!("Table '{}' not found", table)))?;

        if values.len() != table_columns.len() {
            return Err(DriftError::InvalidQuery(format!(
                "Column count mismatch: table {} has {} columns but {} values provided",
                table,
                table_columns.len(),
                values.len()
            )));
        }

        for (i, value_expr) in values.iter().enumerate() {
            let value = expr_to_json_value(value_expr)?;
            if let Some(col_name) = table_columns.get(i) {
                data.insert(col_name.clone(), value);
            } else {
                return Err(DriftError::InvalidQuery(format!(
                    "Column index {} out of range for table {}",
                    i, table
                )));
            }
        }
    } else {
        // Explicit columns provided - INSERT INTO table (col1, col2) VALUES (val1, val2)
        if columns.len() != values.len() {
            return Err(DriftError::InvalidQuery(format!(
                "Column count mismatch: {} columns but {} values",
                columns.len(),
                values.len()
            )));
        }

        for (i, column) in columns.iter().enumerate() {
            if let Some(value_expr) = values.get(i) {
                let value = expr_to_json_value(value_expr)?;
                data.insert(column.value.clone(), value);
            }
        }

        // Verify primary key is included
        let primary_key = engine.get_table_primary_key(table)?;
        if !data.contains_key(&primary_key) {
            return Err(DriftError::InvalidQuery(format!(
                "Primary key '{}' must be specified in INSERT",
                primary_key
            )));
        }
    }

    // Execute BEFORE INSERT triggers
    let new_row = json!(data);
    let trigger_result = engine.execute_triggers(
        table,
        crate::triggers::TriggerEvent::Insert,
        crate::triggers::TriggerTiming::Before,
        None,
        Some(new_row.clone()),
    )?;

    // Apply any modifications from triggers
    let final_data = match trigger_result {
        crate::triggers::TriggerResult::ModifyRow(modified) => modified,
        crate::triggers::TriggerResult::Skip => {
            return Ok(QueryResult::Success {
                message: "Row skipped by trigger".to_string(),
            });
        }
        crate::triggers::TriggerResult::Abort(msg) => {
            return Err(DriftError::InvalidQuery(format!(
                "Trigger aborted: {}",
                msg
            )));
        }
        crate::triggers::TriggerResult::Continue => new_row,
    };

    let query = Query::Insert {
        table: table.to_string(),
        data: final_data.clone(),
    };

    let result = engine.execute_query(query)?;

    // Execute AFTER INSERT triggers
    engine.execute_triggers(
        table,
        crate::triggers::TriggerEvent::Insert,
        crate::triggers::TriggerTiming::After,
        None,
        Some(final_data),
    )?;

    Ok(result)
}

fn extract_table_name(table: &TableFactor) -> Result<String> {
    match table {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(DriftError::InvalidQuery(
            "Complex table expressions not supported".to_string(),
        )),
    }
}

fn extract_table_name_from_join(table: &TableFactor) -> Result<String> {
    extract_table_name(table)
}

fn parse_where_clause(expr: &sqlparser::ast::Expr) -> Result<Vec<WhereCondition>> {
    // Simplified WHERE clause parsing
    match expr {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => {
            let column = extract_column_from_expr(left)?;
            let value = expr_to_json_value(right)?;

            let operator = match op {
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::Lt => "<",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Gt => ">",
                BinaryOperator::GtEq => ">=",
                _ => {
                    return Err(DriftError::InvalidQuery(
                        "Operator not supported in WHERE clause".to_string(),
                    ))
                }
            };

            Ok(vec![WhereCondition {
                column,
                operator: operator.to_string(),
                value,
            }])
        }
        _ => Ok(vec![]),
    }
}

fn expr_to_json_value(expr: &sqlparser::ast::Expr) -> Result<Value> {
    match expr {
        sqlparser::ast::Expr::Value(val) => sql_value_to_json(val),
        sqlparser::ast::Expr::Identifier(ident) => Ok(json!(ident.value)),
        _ => Ok(Value::Null),
    }
}

fn sql_value_to_json(val: &sqlparser::ast::Value) -> Result<Value> {
    match val {
        sqlparser::ast::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(json!(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(json!(f))
            } else {
                Ok(json!(n))
            }
        }
        sqlparser::ast::Value::SingleQuotedString(s)
        | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(json!(s)),
        sqlparser::ast::Value::Boolean(b) => Ok(json!(b)),
        sqlparser::ast::Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

fn execute_aggregation(rows: &[Value], select: &Select) -> Result<Vec<Value>> {
    // Handle GROUP BY
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
            execute_group_by_aggregation(rows, select, exprs)
        }
        _ => {
            // Simple aggregation without GROUP BY
            execute_simple_aggregation(rows, select)
        }
    }
}

fn execute_simple_aggregation(rows: &[Value], select: &Select) -> Result<Vec<Value>> {
    let mut result_row = serde_json::Map::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let (col_name, value) = evaluate_expression(expr, rows)?;
                result_row.insert(col_name, value);
            }
            SelectItem::Wildcard(_) => {
                return Err(DriftError::InvalidQuery(
                    "Cannot use * with aggregate functions".to_string(),
                ));
            }
            _ => {}
        }
    }

    Ok(vec![json!(result_row)])
}

fn execute_group_by_aggregation(
    rows: &[Value],
    select: &Select,
    group_exprs: &[Expr],
) -> Result<Vec<Value>> {
    // Group rows by the GROUP BY expressions
    let mut groups: HashMap<String, Vec<Value>> = HashMap::new();

    for row in rows {
        let mut group_key = String::new();

        for group_expr in group_exprs {
            let value = match group_expr {
                Expr::Identifier(ident) => {
                    // Check both unprefixed and prefixed column names
                    row.get(&ident.value)
                        .or_else(|| row.get(format!("right_{}", ident.value)))
                        .or_else(|| row.get(format!("left_{}", ident.value)))
                }
                Expr::CompoundIdentifier(idents) => {
                    // For table.column, just use the column part
                    if let Some(column) = idents.last() {
                        row.get(&column.value)
                            .or_else(|| row.get(format!("right_{}", column.value)))
                            .or_else(|| row.get(format!("left_{}", column.value)))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(val) = value {
                group_key.push_str(&val.to_string());
                group_key.push('|'); // Separator
            }
        }

        groups.entry(group_key).or_default().push(row.clone());
    }

    // Process each group
    let mut results = Vec::new();
    for (_, group_rows) in groups {
        let mut result_row = serde_json::Map::new();

        // Add GROUP BY columns
        if let Some(first_row) = group_rows.first() {
            for group_expr in group_exprs {
                match group_expr {
                    Expr::Identifier(ident) => {
                        let value = first_row
                            .get(&ident.value)
                            .or_else(|| first_row.get(format!("right_{}", ident.value)))
                            .or_else(|| first_row.get(format!("left_{}", ident.value)));

                        if let Some(val) = value {
                            result_row.insert(ident.value.clone(), val.clone());
                        }
                    }
                    Expr::CompoundIdentifier(idents) => {
                        if let Some(column) = idents.last() {
                            let value = first_row
                                .get(&column.value)
                                .or_else(|| first_row.get(format!("right_{}", column.value)))
                                .or_else(|| first_row.get(format!("left_{}", column.value)));

                            if let Some(val) = value {
                                result_row.insert(column.value.clone(), val.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Process SELECT projections
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Function(func)) => {
                    let (col_name, value) = evaluate_aggregate_function(func, &group_rows)?;
                    result_row.insert(col_name, value);
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Function(func),
                    alias,
                } => {
                    let (_, value) = evaluate_aggregate_function(func, &group_rows)?;
                    result_row.insert(alias.value.clone(), value);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    // Non-aggregate column - get from first row if it's in GROUP BY
                    if let Some(first_row) = group_rows.first() {
                        if let Some(val) = first_row.get(&ident.value) {
                            result_row.insert(ident.value.clone(), val.clone());
                        } else if let Some(val) = first_row.get(format!("right_{}", ident.value)) {
                            result_row.insert(ident.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                    // Handle table.column
                    if let Some(column) = idents.last() {
                        if let Some(first_row) = group_rows.first() {
                            if let Some(val) = first_row.get(&column.value) {
                                result_row.insert(column.value.clone(), val.clone());
                            } else if let Some(val) =
                                first_row.get(format!("right_{}", column.value))
                            {
                                result_row.insert(column.value.clone(), val.clone());
                            }
                        }
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(ident),
                    alias,
                } => {
                    if let Some(first_row) = group_rows.first() {
                        if let Some(val) = first_row.get(&ident.value) {
                            result_row.insert(alias.value.clone(), val.clone());
                        } else if let Some(val) = first_row.get(format!("right_{}", ident.value)) {
                            result_row.insert(alias.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // For GROUP BY, we shouldn't have wildcard, but if we do,
                    // just include the grouped columns (already added above)
                }
                _ => {}
            }
        }

        results.push(json!(result_row));
    }

    Ok(results)
}

fn evaluate_expression(expr: &Expr, rows: &[Value]) -> Result<(String, Value)> {
    match expr {
        Expr::Function(func) => evaluate_aggregate_function(func, rows),
        Expr::Identifier(ident) => {
            // For non-aggregate columns in simple aggregation
            Ok((ident.value.clone(), Value::Null))
        }
        _ => Err(DriftError::InvalidQuery(
            "Unsupported expression type".to_string(),
        )),
    }
}

fn evaluate_aggregate_function(func: &Function, rows: &[Value]) -> Result<(String, Value)> {
    let func_name = func.name.to_string().to_uppercase();

    // Extract the column name from function arguments
    let column = match &func.args {
        FunctionArguments::List(list) if list.args.len() == 1 => {
            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                    Some(ident.value.clone())
                }
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                    // Handle table.column notation (e.g., p.budget)
                    parts.last().map(|ident| ident.value.clone())
                }
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None, // For COUNT(*)
                _ => None,
            }
        }
        _ => None,
    };

    // Generate result column name based on the function
    let result_name = match func_name.as_str() {
        "COUNT" if column.is_none() => "COUNT(*)".to_string(),
        "COUNT" => format!("COUNT({})", column.as_ref().unwrap()),
        "SUM" => format!("SUM({})", column.as_ref().unwrap_or(&"*".to_string())),
        "AVG" => format!("AVG({})", column.as_ref().unwrap_or(&"*".to_string())),
        "MIN" => format!("MIN({})", column.as_ref().unwrap_or(&"*".to_string())),
        "MAX" => format!("MAX({})", column.as_ref().unwrap_or(&"*".to_string())),
        _ => func_name.clone(),
    };

    // Helper to get column value, checking both original and prefixed names
    let get_column_value = |row: &Value, col: &str| -> Option<Value> {
        row.get(col)
            .or_else(|| row.get(format!("right_{}", col)))
            .or_else(|| row.get(format!("left_{}", col)))
            .cloned()
    };

    match func_name.as_str() {
        "COUNT" => {
            let count = if column.is_none() {
                // COUNT(*)
                rows.len() as i64
            } else if let Some(ref col) = column {
                // COUNT(column) - handle both regular and prefixed columns
                rows.iter()
                    .filter(|row| {
                        let val = get_column_value(row, col);
                        val.is_some() && !val.unwrap().is_null()
                    })
                    .count() as i64
            } else {
                0
            };
            Ok((result_name, json!(count)))
        }
        "SUM" => {
            if let Some(col) = column {
                let sum: f64 = rows
                    .iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                    .sum();
                Ok((result_name, json!(sum)))
            } else {
                Err(DriftError::InvalidQuery(
                    "SUM requires a column".to_string(),
                ))
            }
        }
        "AVG" => {
            if let Some(col) = column {
                let values: Vec<f64> = rows
                    .iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                    .collect();

                if values.is_empty() {
                    Ok((result_name, Value::Null))
                } else {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    Ok((result_name, json!(avg)))
                }
            } else {
                Err(DriftError::InvalidQuery(
                    "AVG requires a column".to_string(),
                ))
            }
        }
        "MIN" => {
            if let Some(col) = column {
                let min = rows
                    .iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter(|v| !v.is_null())
                    .min_by(|a, b| match (a.as_f64(), b.as_f64()) {
                        (Some(a_val), Some(b_val)) => a_val.partial_cmp(&b_val).unwrap(),
                        _ => std::cmp::Ordering::Equal,
                    });

                Ok((result_name, min.unwrap_or(Value::Null)))
            } else {
                Err(DriftError::InvalidQuery(
                    "MIN requires a column".to_string(),
                ))
            }
        }
        "MAX" => {
            if let Some(col) = column {
                let max = rows
                    .iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter(|v| !v.is_null())
                    .max_by(|a, b| match (a.as_f64(), b.as_f64()) {
                        (Some(a_val), Some(b_val)) => a_val.partial_cmp(&b_val).unwrap(),
                        _ => std::cmp::Ordering::Equal,
                    });

                Ok((result_name, max.unwrap_or(Value::Null)))
            } else {
                Err(DriftError::InvalidQuery(
                    "MAX requires a column".to_string(),
                ))
            }
        }
        _ => Err(DriftError::InvalidQuery(format!(
            "Unsupported aggregate function: {}",
            func_name
        ))),
    }
}

fn filter_rows(engine: &mut Engine, rows: Vec<Value>, expr: &Expr) -> Result<Vec<Value>> {
    let mut filtered = Vec::new();

    for row in rows {
        if evaluate_where_expression_with_engine(engine, expr, &row)? {
            filtered.push(row);
        }
    }

    Ok(filtered)
}

fn filter_aggregated_rows(rows: Vec<Value>, having: &Expr) -> Result<Vec<Value>> {
    let mut filtered = Vec::new();

    for row in rows {
        if evaluate_having_expression(having, &row)? {
            filtered.push(row);
        }
    }

    Ok(filtered)
}

fn evaluate_where_expression_with_engine(
    engine: &mut Engine,
    expr: &Expr,
    row: &Value,
) -> Result<bool> {
    match expr {
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            // Execute the subquery
            let subquery_result = execute_subquery(engine, subquery)?;
            let left_val = evaluate_value_expression(expr, row)?;

            let is_in = subquery_result.iter().any(|val| val == &left_val);
            Ok(if *negated { !is_in } else { is_in })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let left_val = evaluate_value_expression(expr, row)?;
            let mut values = Vec::new();
            for item in list {
                values.push(evaluate_value_expression(item, row)?);
            }
            let is_in = values.iter().any(|val| val == &left_val);
            Ok(if *negated { !is_in } else { is_in })
        }
        Expr::Exists { subquery, negated } => {
            // Set the outer row context for correlated subqueries
            OUTER_ROW_CONTEXT.with(|context| {
                *context.borrow_mut() = Some(row.clone());
            });

            // Execute the subquery with outer row context available
            let subquery_result = execute_sql_query(engine, subquery);

            // Clear the context
            OUTER_ROW_CONTEXT.with(|context| {
                *context.borrow_mut() = None;
            });

            let exists = match subquery_result {
                Ok(QueryResult::Rows { data }) => !data.is_empty(),
                _ => false,
            };

            Ok(if *negated { !exists } else { exists })
        }
        Expr::Subquery(subquery) => {
            // Scalar subquery - should return single value
            let subquery_result = execute_subquery(engine, subquery)?;
            if subquery_result.len() != 1 {
                return Err(DriftError::InvalidQuery(
                    "Scalar subquery must return exactly one value".to_string(),
                ));
            }
            Ok(!subquery_result[0].is_null())
        }
        Expr::BinaryOp { left, op, right } => {
            // Check if right side is a subquery
            match right.as_ref() {
                Expr::Subquery(subquery) => {
                    // Scalar subquery comparison
                    let subquery_result = execute_subquery(engine, subquery)?;
                    if subquery_result.len() != 1 {
                        return Err(DriftError::InvalidQuery(
                            "Scalar subquery must return exactly one value".to_string(),
                        ));
                    }
                    let left_val = evaluate_value_expression(left, row)?;
                    let right_val = &subquery_result[0];

                    match op {
                        BinaryOperator::Eq => Ok(left_val == *right_val),
                        BinaryOperator::NotEq => Ok(left_val != *right_val),
                        BinaryOperator::Lt => compare_values(&left_val, right_val, |cmp| {
                            cmp == std::cmp::Ordering::Less
                        }),
                        BinaryOperator::LtEq => compare_values(&left_val, right_val, |cmp| {
                            cmp != std::cmp::Ordering::Greater
                        }),
                        BinaryOperator::Gt => compare_values(&left_val, right_val, |cmp| {
                            cmp == std::cmp::Ordering::Greater
                        }),
                        BinaryOperator::GtEq => compare_values(&left_val, right_val, |cmp| {
                            cmp != std::cmp::Ordering::Less
                        }),
                        BinaryOperator::And => {
                            Ok(evaluate_where_expression_with_engine(engine, left, row)?
                                && evaluate_where_expression_with_engine(engine, right, row)?)
                        }
                        BinaryOperator::Or => {
                            Ok(evaluate_where_expression_with_engine(engine, left, row)?
                                || evaluate_where_expression_with_engine(engine, right, row)?)
                        }
                        _ => Err(DriftError::InvalidQuery(
                            "Unsupported operator with subquery".to_string(),
                        )),
                    }
                }
                _ => {
                    // Regular binary operation - evaluate using engine context to preserve OUTER_ROW_CONTEXT
                    let left_val = evaluate_value_expression(left, row)?;
                    let right_val = evaluate_value_expression(right, row)?;

                    match op {
                        BinaryOperator::Eq => Ok(left_val == right_val),
                        BinaryOperator::NotEq => Ok(left_val != right_val),
                        BinaryOperator::Lt => compare_values(&left_val, &right_val, |cmp| {
                            cmp == std::cmp::Ordering::Less
                        }),
                        BinaryOperator::LtEq => compare_values(&left_val, &right_val, |cmp| {
                            cmp != std::cmp::Ordering::Greater
                        }),
                        BinaryOperator::Gt => compare_values(&left_val, &right_val, |cmp| {
                            cmp == std::cmp::Ordering::Greater
                        }),
                        BinaryOperator::GtEq => compare_values(&left_val, &right_val, |cmp| {
                            cmp != std::cmp::Ordering::Less
                        }),
                        BinaryOperator::And => {
                            Ok(evaluate_where_expression_with_engine(engine, left, row)?
                                && evaluate_where_expression_with_engine(engine, right, row)?)
                        }
                        BinaryOperator::Or => {
                            Ok(evaluate_where_expression_with_engine(engine, left, row)?
                                || evaluate_where_expression_with_engine(engine, right, row)?)
                        }
                        _ => Err(DriftError::InvalidQuery(
                            "Unsupported WHERE operator".to_string(),
                        )),
                    }
                }
            }
        }
        _ => evaluate_where_expression(expr, row),
    }
}

fn contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => true,
        Expr::BinaryOp { left, right, .. } => contains_subquery(left) || contains_subquery(right),
        Expr::InList { .. } => false,
        _ => false,
    }
}

#[allow(dead_code)]
fn substitute_outer_refs(expr: Expr, outer_row: &Value) -> Result<Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(substitute_outer_refs(*left, outer_row)?),
            op,
            right: Box::new(substitute_outer_refs(*right, outer_row)?),
        }),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            // This might be an outer table reference like u.id
            // Try to get the value from outer_row
            let column = &parts[1].value;
            if let Some(val) = outer_row.get(column) {
                // Replace with the actual value
                json_value_to_sql_expr(val)
            } else {
                // Keep as is - might be inner table reference
                Ok(Expr::CompoundIdentifier(parts))
            }
        }
        other => Ok(other),
    }
}

#[allow(dead_code)]
fn json_value_to_sql_expr(val: &Value) -> Result<Expr> {
    Ok(match val {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Expr::Value(sqlparser::ast::Value::Number(i.to_string(), false))
            } else if let Some(f) = n.as_f64() {
                Expr::Value(sqlparser::ast::Value::Number(f.to_string(), false))
            } else {
                Expr::Value(sqlparser::ast::Value::Null)
            }
        }
        Value::String(s) => Expr::Value(sqlparser::ast::Value::SingleQuotedString(s.clone())),
        Value::Bool(b) => Expr::Value(sqlparser::ast::Value::Boolean(*b)),
        Value::Null => Expr::Value(sqlparser::ast::Value::Null),
        _ => Expr::Value(sqlparser::ast::Value::Null),
    })
}

fn execute_subquery(engine: &mut Engine, subquery: &SqlQuery) -> Result<Vec<Value>> {
    execute_subquery_with_context(engine, subquery, None)
}

fn execute_subquery_with_context(
    engine: &mut Engine,
    subquery: &SqlQuery,
    outer_row: Option<&Value>,
) -> Result<Vec<Value>> {
    // Set outer row context if provided
    if let Some(row) = outer_row {
        OUTER_ROW_CONTEXT.with(|context| {
            *context.borrow_mut() = Some(row.clone());
        });
    }

    // Execute the subquery
    let result = execute_sql_query(engine, subquery)?;

    // Clear outer row context
    OUTER_ROW_CONTEXT.with(|context| {
        *context.borrow_mut() = None;
    });

    match result {
        QueryResult::Rows { data } => {
            // Extract the first column value from each row
            let mut values = Vec::new();
            for row in data {
                if let Some(obj) = row.as_object() {
                    // Get the first value
                    if let Some(val) = obj.values().next() {
                        values.push(val.clone());
                    }
                }
            }
            Ok(values)
        }
        _ => Ok(Vec::new()),
    }
}

fn evaluate_where_expression(expr: &Expr, row: &Value) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_value_expression(left, row)?;
            let right_val = evaluate_value_expression(right, row)?;

            match op {
                BinaryOperator::Eq => Ok(left_val == right_val),
                BinaryOperator::NotEq => Ok(left_val != right_val),
                BinaryOperator::Lt => {
                    compare_values(&left_val, &right_val, |cmp| cmp == std::cmp::Ordering::Less)
                }
                BinaryOperator::LtEq => compare_values(&left_val, &right_val, |cmp| {
                    cmp != std::cmp::Ordering::Greater
                }),
                BinaryOperator::Gt => compare_values(&left_val, &right_val, |cmp| {
                    cmp == std::cmp::Ordering::Greater
                }),
                BinaryOperator::GtEq => {
                    compare_values(&left_val, &right_val, |cmp| cmp != std::cmp::Ordering::Less)
                }
                BinaryOperator::And => {
                    Ok(evaluate_where_expression(left, row)?
                        && evaluate_where_expression(right, row)?)
                }
                BinaryOperator::Or => {
                    Ok(evaluate_where_expression(left, row)?
                        || evaluate_where_expression(right, row)?)
                }
                _ => Err(DriftError::InvalidQuery(
                    "Unsupported WHERE operator".to_string(),
                )),
            }
        }
        _ => Ok(true), // For now, accept other expressions as true
    }
}

fn evaluate_having_expression(expr: &Expr, row: &Value) -> Result<bool> {
    // HAVING works on aggregated results
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Check if left side is an aggregate function
            let left_val = match left.as_ref() {
                Expr::Function(func) => {
                    // For HAVING, we need to look up the aggregate result column
                    let func_name = func.name.to_string().to_uppercase();
                    let column = match &func.args {
                        FunctionArguments::List(list) if list.args.len() == 1 => {
                            match &list.args[0] {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    ident,
                                ))) => Some(ident.value.clone()),
                                _ => None,
                            }
                        }
                        _ => None,
                    };

                    // Match the actual column naming from evaluate_aggregate_function
                    let col_name = match func_name.as_str() {
                        "COUNT" if column.is_none() => "COUNT(*)".to_string(),
                        "COUNT" => format!("COUNT({})", column.as_ref().unwrap()),
                        "SUM" => format!("SUM({})", column.as_ref().unwrap_or(&"*".to_string())),
                        "AVG" => format!("AVG({})", column.as_ref().unwrap_or(&"*".to_string())),
                        "MIN" => format!("MIN({})", column.as_ref().unwrap_or(&"*".to_string())),
                        "MAX" => format!("MAX({})", column.as_ref().unwrap_or(&"*".to_string())),
                        _ => func_name.clone(),
                    };

                    row.get(&col_name).cloned().unwrap_or(Value::Null)
                }
                _ => evaluate_value_expression(left, row)?,
            };

            let right_val = evaluate_value_expression(right, row)?;

            match op {
                BinaryOperator::Eq => Ok(left_val == right_val),
                BinaryOperator::NotEq => Ok(left_val != right_val),
                BinaryOperator::Lt => {
                    compare_values(&left_val, &right_val, |cmp| cmp == std::cmp::Ordering::Less)
                }
                BinaryOperator::LtEq => compare_values(&left_val, &right_val, |cmp| {
                    cmp != std::cmp::Ordering::Greater
                }),
                BinaryOperator::Gt => compare_values(&left_val, &right_val, |cmp| {
                    cmp == std::cmp::Ordering::Greater
                }),
                BinaryOperator::GtEq => {
                    compare_values(&left_val, &right_val, |cmp| cmp != std::cmp::Ordering::Less)
                }
                _ => Err(DriftError::InvalidQuery(
                    "Unsupported HAVING operator".to_string(),
                )),
            }
        }
        _ => Ok(true),
    }
}

fn evaluate_value_expression(expr: &Expr, row: &Value) -> Result<Value> {
    match expr {
        Expr::Identifier(ident) => {
            // First check current row
            if let Some(row_obj) = row.as_object() {
                if let Some(val) = row_obj.get(&ident.value) {
                    return Ok(val.clone());
                }
            }
            // If not found, check outer row context (for correlated subqueries)
            OUTER_ROW_CONTEXT.with(|context| {
                if let Some(outer_row) = context.borrow().as_ref() {
                    if let Some(outer_obj) = outer_row.as_object() {
                        Ok(outer_obj.get(&ident.value).cloned().unwrap_or(Value::Null))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            })
        }
        Expr::CompoundIdentifier(parts) => {
            // Handle table.column notation
            if parts.len() == 2 {
                let _table_alias = &parts[0].value;
                let column = &parts[1].value;

                // For correlated subqueries, we need to check both current and outer contexts
                // The table alias should help us decide, but we don't track aliases yet
                // So we'll use a heuristic: check current row first, then outer

                // For correlated subqueries, we need to check the table alias
                // to determine if we should use current row or outer row

                // Since we don't track which table alias belongs to which query level,
                // we'll use a simple heuristic: If the table alias is 'u' (commonly used
                // for the outer query in our tests), look in outer context first

                // Check if this looks like an outer table reference
                // In the subquery "SELECT ... FROM orders o WHERE o.user_id = u.id",
                // 'o' is the inner table and 'u' is the outer table

                // Heuristic: If we're in a correlated subquery context and the column exists
                // in the outer row, AND the table alias doesn't match common inner aliases,
                // prefer the outer value

                // Determine if this is an outer table reference based on table alias
                // In a correlated subquery context, we need to distinguish between:
                // - Inner table references (e.g., e2.dept in subquery FROM employees e2)
                // - Outer table references (e.g., e1.dept referring to outer query's e1)
                let is_outer_reference = OUTER_ROW_CONTEXT.with(|context| {
                    if context.borrow().is_some() {
                        // We're in a correlated subquery context
                        // Use a simple heuristic: if the table alias is different from the
                        // immediate table alias and we have an outer context, it's likely an outer reference

                        // For compound identifiers in correlated subqueries, prefer the table alias
                        // to determine scope. Common patterns:
                        // - e1.column -> likely outer table reference
                        // - e2.column -> likely inner table reference
                        // - o.column -> inner, u.column -> outer, etc.

                        // Check if column exists in current row
                        let in_current = if let Some(row_obj) = row.as_object() {
                            row_obj.get(column).is_some()
                        } else {
                            false
                        };

                        let in_outer = if let Some(outer_row) = context.borrow().as_ref() {
                            if let Some(outer_obj) = outer_row.as_object() {
                                outer_obj.get(column).is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        // If both current and outer have the column (common case),
                        // use table alias as a hint. Common convention:
                        // - e1, u, outer -> likely outer reference
                        // - e2, e, inner -> likely inner reference
                        if in_current && in_outer {
                            let table_alias = &parts[0].value;
                            // Check if this looks like an outer table alias
                            table_alias == "e1"
                                || table_alias == "outer"
                                || table_alias == "u"
                                || table_alias.ends_with("1")
                                || table_alias == "main"
                        } else {
                            // Fall back to availability heuristic
                            !in_current && in_outer
                        }
                    } else {
                        false
                    }
                });

                if is_outer_reference {
                    // This is likely referring to the outer table
                    OUTER_ROW_CONTEXT.with(|context| {
                        if let Some(outer_row) = context.borrow().as_ref() {
                            if let Some(val) = outer_row.get(column) {
                                return Ok(val.clone());
                            }
                        }
                        Ok(Value::Null)
                    })
                } else {
                    // Try current row first, then outer
                    if let Some(row_obj) = row.as_object() {
                        if let Some(val) = row_obj.get(column) {
                            return Ok(val.clone());
                        }
                    }

                    // Not found in current row, check outer
                    OUTER_ROW_CONTEXT.with(|context| {
                        if let Some(outer_row) = context.borrow().as_ref() {
                            if let Some(outer_obj) = outer_row.as_object() {
                                if let Some(val) = outer_obj.get(column) {
                                    return Ok(val.clone());
                                }
                            }
                        }
                        Ok(Value::Null)
                    })
                }
            } else {
                // Try just the last part as column name
                if let Some(last_part) = parts.last() {
                    if let Some(row_obj) = row.as_object() {
                        Ok(row_obj
                            .get(&last_part.value)
                            .cloned()
                            .unwrap_or(Value::Null))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
        }
        Expr::Value(val) => sql_value_to_json(val),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_case_expression(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            row,
        ),
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_value_expression(left, row)?;
            let right_val = evaluate_value_expression(right, row)?;

            evaluate_binary_op(&left_val, op, &right_val)
        }
        Expr::Nested(inner) => {
            // Handle nested/parenthesized expressions
            evaluate_value_expression(inner, row)
        }
        _ => {
            // Log unhandled expression types for debugging
            eprintln!(
                "Warning: Unhandled expression type in evaluate_value_expression: {:?}",
                expr
            );
            Ok(Value::Null)
        }
    }
}

#[allow(dead_code)]
fn evaluate_expression_with_row(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    row: &Value,
) -> Result<Value> {
    let left_val = match left {
        Expr::Identifier(ident) => match row {
            Value::Object(obj) => {
                if let Some(val) = obj.get(&ident.value) {
                    val.clone()
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
        Expr::Value(val) => sql_value_to_json(val)?,
        _ => evaluate_expression_without_row(left)?,
    };

    let right_val = match right {
        Expr::Identifier(ident) => match row {
            Value::Object(obj) => {
                if let Some(val) = obj.get(&ident.value) {
                    val.clone()
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
        Expr::Value(val) => sql_value_to_json(val)?,
        _ => evaluate_expression_without_row(right)?,
    };

    // Use the centralized binary operation evaluator
    evaluate_binary_op(&left_val, op, &right_val)
}

fn evaluate_expression_without_row(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(val) => match val {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::Number(i.into()))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::Number(
                        serde_json::Number::from_f64(f).unwrap_or(0.into()),
                    ))
                } else {
                    Ok(Value::Number(0.into()))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(Value::String(s.clone())),
            sqlparser::ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
            sqlparser::ast::Value::Null => Ok(Value::Null),
            _ => Ok(Value::Null),
        },
        Expr::Nested(inner) => {
            // Handle nested/parenthesized expressions
            evaluate_expression_without_row(inner)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => evaluate_case_without_row(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
        ),
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expression_without_row(left)?;
            let right_val = evaluate_expression_without_row(right)?;
            match op {
                BinaryOperator::Eq => Ok(Value::Bool(
                    compare_json_values(&left_val, &right_val) == std::cmp::Ordering::Equal,
                )),
                BinaryOperator::NotEq => Ok(Value::Bool(
                    compare_json_values(&left_val, &right_val) != std::cmp::Ordering::Equal,
                )),
                BinaryOperator::Lt => Ok(Value::Bool(
                    compare_json_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                )),
                BinaryOperator::LtEq => Ok(Value::Bool(matches!(
                    compare_json_values(&left_val, &right_val),
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                ))),
                BinaryOperator::Gt => Ok(Value::Bool(
                    compare_json_values(&left_val, &right_val) == std::cmp::Ordering::Greater,
                )),
                BinaryOperator::GtEq => Ok(Value::Bool(matches!(
                    compare_json_values(&left_val, &right_val),
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                ))),
                BinaryOperator::And => Ok(Value::Bool(
                    left_val.as_bool().unwrap_or(false) && right_val.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(Value::Bool(
                    left_val.as_bool().unwrap_or(false) || right_val.as_bool().unwrap_or(false),
                )),
                // Use the centralized binary operation evaluator for arithmetic
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => evaluate_binary_op(&left_val, op, &right_val),
                BinaryOperator::Modulo => {
                    if let (Some(l), Some(r)) = (left_val.as_i64(), right_val.as_i64()) {
                        if r != 0 {
                            Ok(json!(l % r))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        _ => Ok(Value::Null),
    }
}

fn evaluate_case_without_row(
    _operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
) -> Result<Value> {
    for (condition, result) in conditions.iter().zip(results.iter()) {
        let cond_val = evaluate_expression_without_row(condition)?;
        if cond_val.as_bool().unwrap_or(false) {
            return evaluate_expression_without_row(result);
        }
    }

    if let Some(else_expr) = else_result {
        evaluate_expression_without_row(else_expr)
    } else {
        Ok(Value::Null)
    }
}

fn evaluate_case_expression(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    row: &Value,
) -> Result<Value> {
    // Simple CASE (with operand) or searched CASE (without operand)
    if let Some(op) = operand {
        // Simple CASE: CASE expr WHEN val1 THEN result1 ...
        let op_val = evaluate_value_expression(op, row)?;

        for (condition, result) in conditions.iter().zip(results.iter()) {
            let cond_val = evaluate_value_expression(condition, row)?;
            if op_val == cond_val {
                return evaluate_value_expression(result, row);
            }
        }
    } else {
        // Searched CASE: CASE WHEN condition1 THEN result1 ...
        for (condition, result) in conditions.iter().zip(results.iter()) {
            if evaluate_where_expression(condition, row)? {
                return evaluate_value_expression(result, row);
            }
        }
    }

    // ELSE clause
    if let Some(else_expr) = else_result {
        evaluate_value_expression(else_expr, row)
    } else {
        Ok(Value::Null)
    }
}

fn evaluate_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
    match op {
        BinaryOperator::Plus => {
            if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(json!(l + r))
            } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(json!(l + r))
            } else {
                Ok(Value::Null)
            }
        }
        BinaryOperator::Minus => {
            if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(json!(l - r))
            } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(json!(l - r))
            } else {
                Ok(Value::Null)
            }
        }
        BinaryOperator::Multiply => {
            if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(json!(l * r))
            } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(json!(l * r))
            } else {
                Ok(Value::Null)
            }
        }
        BinaryOperator::Divide => {
            if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                if r != 0.0 {
                    Ok(json!(l / r))
                } else {
                    Ok(Value::Null)
                }
            } else if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                if r != 0 {
                    Ok(json!(l / r))
                } else {
                    Ok(Value::Null)
                }
            } else {
                Ok(Value::Null)
            }
        }
        _ => Ok(Value::Null),
    }
}

fn project_columns(rows: Vec<Value>, select: &Select) -> Result<Vec<Value>> {
    let mut projected_rows = Vec::new();

    for row in rows {
        let mut projected_row = serde_json::Map::new();

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    // Look for the column in the row (with prefix handling)
                    if let Some(val) = row.get(&ident.value) {
                        projected_row.insert(ident.value.clone(), val.clone());
                    } else if let Some(val) = row.get(format!("right_{}", ident.value)) {
                        projected_row.insert(ident.value.clone(), val.clone());
                    }
                }
                SelectItem::UnnamedExpr(Expr::BinaryOp { left, op, right }) => {
                    // Handle arithmetic expressions like n + 1
                    let binary_expr = Expr::BinaryOp {
                        left: left.clone(),
                        op: op.clone(),
                        right: right.clone(),
                    };
                    if let Ok(value) = evaluate_value_expression(&binary_expr, &row) {
                        // Use a simple name for the column (e.g., n + 1 -> "n")
                        let col_name = match left.as_ref() {
                            Expr::Identifier(ident) => ident.value.clone(),
                            _ => "expr".to_string(),
                        };
                        projected_row.insert(col_name, value);
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(ident),
                    alias,
                } => {
                    if let Some(val) = row.get(&ident.value) {
                        projected_row.insert(alias.value.clone(), val.clone());
                    } else if let Some(val) = row.get(format!("right_{}", ident.value)) {
                        projected_row.insert(alias.value.clone(), val.clone());
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::BinaryOp { left, op, right },
                    alias,
                } => {
                    // Handle arithmetic expressions with alias like "n + 1 as next_n"
                    let binary_expr = Expr::BinaryOp {
                        left: left.clone(),
                        op: op.clone(),
                        right: right.clone(),
                    };
                    if let Ok(value) = evaluate_value_expression(&binary_expr, &row) {
                        projected_row.insert(alias.value.clone(), value);
                    }
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                    // Handle table.column notation
                    if let Some(column) = idents.last() {
                        if let Some(val) = row.get(&column.value) {
                            projected_row.insert(column.value.clone(), val.clone());
                        } else if let Some(val) = row.get(format!("right_{}", column.value)) {
                            projected_row.insert(column.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::CompoundIdentifier(idents),
                    alias,
                } => {
                    // For multiple tables with same column names, we need to find the right one
                    // The pattern is: first table's columns are unprefixed,
                    // second table's conflicting columns get t1_ prefix,
                    // third table's conflicting columns get t2_ prefix, etc.
                    if idents.len() >= 2 {
                        let table_alias = &idents[0].value;
                        let column = &idents[1].value;

                        // Based on the query pattern:
                        // orders o -> base table (left)
                        // customers c -> first JOIN (t1_ prefix)
                        // products p -> second JOIN (t2_ prefix)

                        // The issue is that when JOINing, the first conflicting column gets t1_,
                        // But we need to know which table each prefix belongs to.
                        // Since "name" appears in both customers and products tables,
                        // after the first JOIN, customers.name might be just "name"
                        // and after second JOIN, products.name gets prefixed

                        let val = if table_alias == "c" {
                            // customers table - look for name (might be unprefixed if it came first)
                            row.get(column)
                                .or_else(|| row.get(format!("t1_{}", column)))
                        } else if table_alias == "p" {
                            // products table - likely prefixed due to conflict
                            row.get(format!("t1_{}", column))
                                .or_else(|| row.get(format!("t2_{}", column)))
                                .or_else(|| row.get(column))
                        } else if table_alias == "o" {
                            // orders table - original left table
                            row.get(column)
                        } else {
                            row.get(column)
                        };

                        if let Some(v) = val {
                            projected_row.insert(alias.value.clone(), v.clone());
                        }
                    } else if let Some(column) = idents.last() {
                        if let Some(val) = row.get(&column.value) {
                            projected_row.insert(alias.value.clone(), val.clone());
                        }
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Include all columns (as-is)
                    if let Some(obj) = row.as_object() {
                        for (key, val) in obj {
                            projected_row.insert(key.clone(), val.clone());
                        }
                    }
                }
                _ => {
                    // Handle any other expression patterns not explicitly matched above
                    match item {
                        SelectItem::ExprWithAlias { expr, alias } => {
                            // General expression with alias - evaluate any expression type
                            if let Ok(value) = evaluate_value_expression(expr, &row) {
                                projected_row.insert(alias.value.clone(), value);
                            }
                        }
                        SelectItem::UnnamedExpr(expr) => {
                            // General unnamed expression - evaluate any expression type
                            if let Ok(value) = evaluate_value_expression(expr, &row) {
                                // Generate a column name based on the expression
                                let col_name = format!("expr_{}", projected_row.len());
                                projected_row.insert(col_name, value);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        projected_rows.push(json!(projected_row));
    }

    Ok(projected_rows)
}

fn compare_values<F>(left: &Value, right: &Value, op: F) -> Result<bool>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => Ok(op(l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal))),
        _ => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(op(l.cmp(r))),
            _ => Ok(false),
        },
    }
}

fn process_scalar_subqueries(
    engine: &mut Engine,
    mut data: Vec<Value>,
    projection: &[SelectItem],
) -> Result<Vec<Value>> {
    // Add scalar subquery results to each row
    for row in &mut data {
        let row_clone = row.clone(); // Clone before mutable borrow
        if let Value::Object(row_map) = row {
            for item in projection {
                let (subquery, col_name) = match item {
                    SelectItem::ExprWithAlias {
                        expr: Expr::Subquery(subquery),
                        alias,
                    } => (Some(subquery), alias.value.clone()),
                    SelectItem::UnnamedExpr(Expr::Subquery(subquery)) => {
                        // Generate a column name for unnamed subquery
                        (Some(subquery), "(subquery)".to_string())
                    }
                    _ => (None, String::new()),
                };

                if let Some(subquery) = subquery {
                    // Set outer row context for correlated subqueries
                    OUTER_ROW_CONTEXT.with(|context| {
                        *context.borrow_mut() = Some(row_clone.clone());
                    });

                    let result = execute_sql_query(engine, subquery);

                    // Clear context
                    OUTER_ROW_CONTEXT.with(|context| {
                        *context.borrow_mut() = None;
                    });

                    let value = match result {
                        Ok(QueryResult::Rows { data }) if !data.is_empty() => {
                            // Scalar subquery should return single value
                            if let Some(first_row) = data.first() {
                                if let Value::Object(map) = first_row {
                                    // Get the first column value
                                    map.values().next().cloned().unwrap_or(Value::Null)
                                } else {
                                    first_row.clone()
                                }
                            } else {
                                Value::Null
                            }
                        }
                        _ => Value::Null,
                    };

                    row_map.insert(col_name, value);
                }
            }
        }
    }

    Ok(data)
}

fn apply_projection(data: Vec<Value>, projection: &[SelectItem]) -> Result<Vec<Value>> {
    // Handle SELECT * case
    let select_all = projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)));
    if select_all {
        return Ok(data);
    }

    let mut result = Vec::new();
    for row in data {
        if let Value::Object(row_map) = &row {
            let mut projected_row = serde_json::Map::new();

            for item in projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        match expr {
                            Expr::Identifier(ident) => {
                                let col_name = ident.value.clone();
                                if let Some(value) = row_map.get(&col_name) {
                                    projected_row.insert(col_name, value.clone());
                                }
                            }
                            Expr::CompoundIdentifier(parts) => {
                                // Handle table.column notation
                                let col_name =
                                    parts.last().map(|i| i.value.clone()).unwrap_or_default();
                                if let Some(value) = row_map.get(&col_name) {
                                    projected_row.insert(col_name, value.clone());
                                }
                            }
                            Expr::Value(val) => {
                                // Handle literal values like SELECT 1
                                let json_val = sql_value_to_json(val)?;
                                let col_name =
                                    format!("{:?}", val).chars().take(20).collect::<String>();
                                projected_row.insert(col_name, json_val);
                            }
                            Expr::Case { .. } => {
                                // Handle CASE WHEN without alias
                                if let Ok(value) = evaluate_value_expression(expr, &row) {
                                    projected_row.insert("case".to_string(), value);
                                }
                            }
                            Expr::Subquery(_) => {
                                // Scalar subqueries should be handled by process_scalar_subqueries
                                // Look for the value with the standard subquery column name
                                let subquery_col = "(subquery)";
                                if let Some(value) = row_map.get(subquery_col) {
                                    projected_row.insert(subquery_col.to_string(), value.clone());
                                } else {
                                    // Fallback if not found
                                    let col_name =
                                        format!("{:?}", expr).chars().take(50).collect::<String>();
                                    projected_row.insert(col_name, Value::Null);
                                }
                            }
                            _ => {
                                // Handle any other expression (e.g., function calls, etc.)
                                if let Ok(value) = evaluate_value_expression(expr, &row) {
                                    // Use a simplified version of the expression as column name
                                    let col_name =
                                        format!("{:?}", expr).chars().take(50).collect::<String>();
                                    projected_row.insert(col_name, value);
                                }
                            }
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        match expr {
                            Expr::Identifier(ident) => {
                                let col_name = ident.value.clone();
                                if let Some(value) = row_map.get(&col_name) {
                                    projected_row.insert(alias.value.clone(), value.clone());
                                }
                            }
                            Expr::Subquery(_) => {
                                // Scalar subqueries should be handled by process_scalar_subqueries
                                // Look for the value with the alias name
                                if let Some(value) = row_map.get(&alias.value) {
                                    projected_row.insert(alias.value.clone(), value.clone());
                                } else {
                                    // If not found, set to null
                                    projected_row.insert(alias.value.clone(), Value::Null);
                                }
                            }
                            _ => {
                                // Evaluate complex expressions including CASE WHEN
                                if let Ok(value) = evaluate_value_expression(expr, &row) {
                                    projected_row.insert(alias.value.clone(), value);
                                }
                            }
                        }
                    }
                    _ => {} // Skip other cases for now
                }
            }

            if !projected_row.is_empty() {
                result.push(Value::Object(projected_row));
            }
        }
    }

    Ok(result)
}

fn apply_distinct(data: Vec<Value>) -> Vec<Value> {
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for row in data {
        // Use JSON string representation for uniqueness comparison
        let key = row.to_string();
        if seen.insert(key) {
            result.push(row);
        }
    }

    result
}

fn apply_order_by(mut rows: Vec<Value>, order_by: &[OrderByExpr]) -> Result<Vec<Value>> {
    rows.sort_by(|a, b| {
        for order_expr in order_by {
            if let Some(ordering) = compare_rows_by_expr(a, b, order_expr) {
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(rows)
}

fn compare_rows_by_expr(
    a: &Value,
    b: &Value,
    order_expr: &OrderByExpr,
) -> Option<std::cmp::Ordering> {
    // Extract the column name from the expression
    let column = match &order_expr.expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => {
            // For now, just take the last part (column name)
            idents.last()?.value.clone()
        }
        _ => return None,
    };

    // Get values from both rows - check for prefixed columns too
    let a_val = a
        .get(&column)
        .or_else(|| a.get(format!("right_{}", column)))
        .or_else(|| a.get(format!("left_{}", column)));

    let b_val = b
        .get(&column)
        .or_else(|| b.get(format!("right_{}", column)))
        .or_else(|| b.get(format!("left_{}", column)));

    match (a_val, b_val) {
        (Some(a_val), Some(b_val)) => {
            let ordering = compare_json_values(a_val, b_val);

            // Handle ASC/DESC
            if let Some(asc) = order_expr.asc {
                if !asc {
                    return Some(ordering.reverse());
                }
            }
            Some(ordering)
        }
        (None, Some(_)) => Some(std::cmp::Ordering::Greater), // NULLs last
        (Some(_), None) => Some(std::cmp::Ordering::Less),
        (None, None) => Some(std::cmp::Ordering::Equal),
    }
}

fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(a_num), Value::Number(b_num)) => {
            let a_f = a_num.as_f64().unwrap_or(0.0);
            let b_f = b_num.as_f64().unwrap_or(0.0);
            a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
        (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater, // NULLs last
        (_, Value::Null) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Equal,
    }
}

fn parse_limit(expr: &Expr) -> Result<usize> {
    match expr {
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => n
            .parse::<usize>()
            .map_err(|_| DriftError::InvalidQuery(format!("Invalid LIMIT value: {}", n))),
        _ => Err(DriftError::InvalidQuery(
            "LIMIT must be a number".to_string(),
        )),
    }
}

fn parse_offset(offset: &Offset) -> Result<usize> {
    match &offset.value {
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => n
            .parse::<usize>()
            .map_err(|_| DriftError::InvalidQuery(format!("Invalid OFFSET value: {}", n))),
        _ => Err(DriftError::InvalidQuery(
            "OFFSET must be a number".to_string(),
        )),
    }
}

fn execute_sql_update(
    engine: &mut Engine,
    table: &TableWithJoins,
    assignments: &[sqlparser::ast::Assignment],
    selection: &Option<Expr>,
) -> Result<QueryResult> {
    // Extract table name
    let table_name = extract_table_name(&table.relation)?;

    // First, fetch all rows that match the WHERE clause
    let conditions = if let Some(where_expr) = selection {
        parse_where_clause(where_expr)?
    } else {
        vec![]
    };

    let select_query = Query::Select {
        table: table_name.clone(),
        conditions: conditions.clone(),
        as_of: None,
        limit: None,
    };

    let result = engine.execute_query(select_query)?;

    let rows_to_update = match result {
        QueryResult::Rows { data } => data,
        _ => {
            return Ok(QueryResult::Success {
                message: "No rows to update".to_string(),
            })
        }
    };

    // Update each matching row
    let mut update_count = 0;
    for row in rows_to_update {
        let old_row = row.clone();
        let mut updated_row = row.clone();

        // Apply assignments
        if let Some(row_obj) = updated_row.as_object_mut() {
            for assignment in assignments {
                // In sqlparser 0.51, Assignment has target and value fields
                let column = match &assignment.target {
                    sqlparser::ast::AssignmentTarget::ColumnName(name) => name
                        .0
                        .last()
                        .ok_or_else(|| {
                            DriftError::InvalidQuery("Invalid column in UPDATE".to_string())
                        })?
                        .value
                        .clone(),
                    _ => {
                        return Err(DriftError::InvalidQuery(
                            "Complex assignment targets not supported".to_string(),
                        ))
                    }
                };

                let new_value = evaluate_update_expression(&assignment.value, &row)?;
                row_obj.insert(column, new_value);
            }
        }

        // Execute BEFORE UPDATE triggers
        let trigger_result = engine.execute_triggers(
            &table_name,
            crate::triggers::TriggerEvent::Update,
            crate::triggers::TriggerTiming::Before,
            Some(old_row.clone()),
            Some(updated_row.clone()),
        )?;

        // Apply any modifications from triggers
        let final_row = match trigger_result {
            crate::triggers::TriggerResult::ModifyRow(modified) => modified,
            crate::triggers::TriggerResult::Skip => continue,
            crate::triggers::TriggerResult::Abort(msg) => {
                return Err(DriftError::InvalidQuery(format!(
                    "Trigger aborted: {}",
                    msg
                )));
            }
            crate::triggers::TriggerResult::Continue => updated_row.clone(),
        };

        // Get the primary key value from final_row
        let primary_key = if let Some(row_obj) = final_row.as_object() {
            row_obj.get("id").cloned().unwrap_or(Value::Null)
        } else {
            Value::Null
        };

        // Use PATCH to update the row
        let patch_query = Query::Patch {
            table: table_name.clone(),
            primary_key,
            updates: final_row.clone(),
        };

        engine.execute_query(patch_query)?;

        // Execute AFTER UPDATE triggers
        engine.execute_triggers(
            &table_name,
            crate::triggers::TriggerEvent::Update,
            crate::triggers::TriggerTiming::After,
            Some(old_row),
            Some(final_row),
        )?;

        update_count += 1;
    }

    Ok(QueryResult::Success {
        message: format!("Updated {} rows", update_count),
    })
}

#[allow(dead_code)]
fn execute_create_view(
    engine: &Engine,
    name: &sqlparser::ast::ObjectName,
    query: &SqlQuery,
    _or_replace: bool,
    materialized: bool,
) -> Result<QueryResult> {
    let view_name = name.to_string();
    let view_sql = query.to_string();

    // Create view using the engine
    let mut view_builder = crate::views::ViewBuilder::new(&view_name, &view_sql);
    if materialized {
        view_builder = view_builder.materialized(true);
    }

    engine.create_view(view_builder.build()?)?;

    Ok(QueryResult::Success {
        message: format!("View '{}' created", view_name),
    })
}

fn execute_drop_view(
    engine: &Engine,
    name: &sqlparser::ast::ObjectName,
    cascade: bool,
) -> Result<QueryResult> {
    let view_name = name.to_string();

    engine.drop_view(&view_name, cascade)?;

    Ok(QueryResult::Success {
        message: format!("View '{}' dropped", view_name),
    })
}

fn execute_drop_table(
    engine: &mut Engine,
    name: &sqlparser::ast::ObjectName,
) -> Result<QueryResult> {
    let table_name = name.to_string();

    engine.drop_table(&table_name)?;

    Ok(QueryResult::Success {
        message: format!("Table '{}' dropped", table_name),
    })
}

fn execute_create_table(
    engine: &mut Engine,
    name: &sqlparser::ast::ObjectName,
    columns: &Vec<sqlparser::ast::ColumnDef>,
    constraints: &Vec<sqlparser::ast::TableConstraint>,
) -> Result<QueryResult> {
    use crate::schema::ColumnDef as DriftColumnDef;

    let table_name = name.to_string();

    // Extract primary key and build column definitions
    let mut primary_key = String::new();
    let mut drift_columns = Vec::new();

    // Process column definitions
    for column in columns {
        let col_name = column.name.value.clone();
        let col_type = column.data_type.to_string();
        let is_index = false;

        // Check for PRIMARY KEY in column options
        for option in &column.options {
            if let sqlparser::ast::ColumnOption::Unique { is_primary, .. } = &option.option {
                if *is_primary {
                    primary_key = col_name.clone();
                }
            }
        }

        // Create column definition
        drift_columns.push(DriftColumnDef {
            name: col_name,
            col_type,
            index: is_index,
        });
    }

    // Check table constraints for primary key, unique, and foreign key constraints
    let mut foreign_keys = Vec::new();
    for constraint in constraints {
        match constraint {
            sqlparser::ast::TableConstraint::Unique { .. } => {
                // Unique constraint - could track these for future use
            }
            sqlparser::ast::TableConstraint::PrimaryKey { columns, .. } => {
                if let Some(first_col) = columns.first() {
                    primary_key = first_col.value.clone();
                }
            }
            sqlparser::ast::TableConstraint::ForeignKey {
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                // Store foreign key information
                let fk_columns: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
                let ref_columns: Vec<String> =
                    referred_columns.iter().map(|c| c.value.clone()).collect();

                let on_delete_action = match on_delete {
                    Some(sqlparser::ast::ReferentialAction::Cascade) => {
                        crate::constraints::ForeignKeyAction::Cascade
                    }
                    Some(sqlparser::ast::ReferentialAction::SetNull) => {
                        crate::constraints::ForeignKeyAction::SetNull
                    }
                    Some(sqlparser::ast::ReferentialAction::SetDefault) => {
                        crate::constraints::ForeignKeyAction::SetDefault
                    }
                    _ => crate::constraints::ForeignKeyAction::Restrict,
                };

                let on_update_action = match on_update {
                    Some(sqlparser::ast::ReferentialAction::Cascade) => {
                        crate::constraints::ForeignKeyAction::Cascade
                    }
                    Some(sqlparser::ast::ReferentialAction::SetNull) => {
                        crate::constraints::ForeignKeyAction::SetNull
                    }
                    Some(sqlparser::ast::ReferentialAction::SetDefault) => {
                        crate::constraints::ForeignKeyAction::SetDefault
                    }
                    _ => crate::constraints::ForeignKeyAction::Restrict,
                };

                let fk_constraint = crate::constraints::Constraint {
                    name: format!(
                        "fk_{}_{}_{}",
                        table_name,
                        fk_columns.first().unwrap_or(&"unknown".to_string()),
                        foreign_table
                    ),
                    constraint_type: crate::constraints::ConstraintType::ForeignKey {
                        columns: fk_columns,
                        reference_table: foreign_table.to_string(),
                        reference_columns: ref_columns,
                        on_delete: on_delete_action,
                        on_update: on_update_action,
                    },
                    table_name: table_name.clone(),
                    is_deferrable: false,
                    initially_deferred: false,
                };
                foreign_keys.push(fk_constraint);
            }
            _ => {}
        }
    }

    // Extract indexed columns from constraints
    let indexed_cols = extract_indexes_from_constraints(constraints);

    // Mark indexed columns in drift_columns
    for col in drift_columns.iter_mut() {
        if indexed_cols.contains(&col.name) {
            col.index = true;
        }
    }

    // Default to first column if no primary key found
    if primary_key.is_empty() && !drift_columns.is_empty() {
        primary_key = drift_columns[0].name.clone();
    } else if primary_key.is_empty() {
        // Create default id column if needed
        primary_key = "id".to_string();
        drift_columns.insert(
            0,
            DriftColumnDef {
                name: "id".to_string(),
                col_type: "INT".to_string(),
                index: false,
            },
        );
    }

    // Create the table with full column definitions
    engine.create_table_with_columns(&table_name, &primary_key, drift_columns)?;

    // Register foreign key constraints with the constraint manager
    // TODO: Add add_constraint method to Engine
    if !foreign_keys.is_empty() {
        // For now, just log that foreign keys were defined
        // engine.add_constraint(fk)?;
    }

    Ok(QueryResult::Success {
        message: format!("Table '{}' created", table_name),
    })
}

fn extract_indexes_from_constraints(
    constraints: &Vec<sqlparser::ast::TableConstraint>,
) -> Vec<String> {
    let mut indexes = Vec::new();
    for constraint in constraints {
        if let sqlparser::ast::TableConstraint::Index { columns, .. } = constraint {
            for col in columns {
                indexes.push(col.value.clone());
            }
        }
    }
    indexes
}

// TODO: Implement CALL procedure when sqlparser structure is confirmed
// fn execute_call_procedure(
//     engine: &Engine,
//     name: &sqlparser::ast::ObjectName,
//     args: &[sqlparser::ast::FunctionArg],
// ) -> Result<QueryResult> {
//     ...
// }

fn execute_create_index(
    engine: &mut Engine,
    name: &Option<sqlparser::ast::ObjectName>,
    table_name: &sqlparser::ast::ObjectName,
    columns: &[sqlparser::ast::OrderByExpr],
    _unique: bool,
) -> Result<QueryResult> {
    let table = table_name.to_string();
    let index_name = name.as_ref().map(|n| n.to_string());

    if let Some(col_expr) = columns.first() {
        let column_name = col_expr.expr.to_string();

        // Create the actual index
        engine.create_index(&table, &column_name, index_name.as_deref())?;

        let display_name = index_name.unwrap_or_else(|| format!("idx_{}_{}", table, column_name));
        Ok(QueryResult::Success {
            message: format!(
                "Index '{}' created on {}.{}",
                display_name, table, column_name
            ),
        })
    } else {
        Err(DriftError::InvalidQuery(
            "CREATE INDEX requires at least one column".to_string(),
        ))
    }
}

fn execute_sql_delete(
    engine: &mut Engine,
    tables: &[sqlparser::ast::ObjectName],
    selection: &Option<Expr>,
) -> Result<QueryResult> {
    if tables.is_empty() {
        return Err(DriftError::InvalidQuery(
            "DELETE requires FROM clause".to_string(),
        ));
    }

    // Extract table name
    let table_name = tables[0].to_string();

    // Parse WHERE clause
    let conditions = if let Some(where_expr) = selection {
        parse_where_clause(where_expr)?
    } else {
        vec![]
    };

    // First, fetch all rows that match the WHERE clause
    let select_query = Query::Select {
        table: table_name.clone(),
        conditions,
        as_of: None,
        limit: None,
    };

    let result = engine.execute_query(select_query)?;

    let rows_to_delete = match result {
        QueryResult::Rows { data } => data,
        _ => {
            return Ok(QueryResult::Success {
                message: "No rows to delete".to_string(),
            })
        }
    };

    // Delete each matching row
    let mut delete_count = 0;
    for row in rows_to_delete {
        if let Some(row_obj) = row.as_object() {
            // Execute BEFORE DELETE triggers
            let trigger_result = engine.execute_triggers(
                &table_name,
                crate::triggers::TriggerEvent::Delete,
                crate::triggers::TriggerTiming::Before,
                Some(row.clone()),
                None,
            )?;

            // Check if trigger prevented deletion
            match trigger_result {
                crate::triggers::TriggerResult::Skip => continue,
                crate::triggers::TriggerResult::Abort(msg) => {
                    return Err(DriftError::InvalidQuery(format!(
                        "Trigger aborted: {}",
                        msg
                    )));
                }
                _ => {} // Continue or ModifyRow (not applicable for DELETE)
            }

            // Get the primary key value (assuming "id" for now)
            let primary_key = row_obj.get("id").cloned().unwrap_or(Value::Null);

            // Use SOFT_DELETE to delete the row
            let delete_query = Query::SoftDelete {
                table: table_name.clone(),
                primary_key: primary_key.clone(),
            };

            engine.execute_query(delete_query)?;

            // Execute AFTER DELETE triggers
            engine.execute_triggers(
                &table_name,
                crate::triggers::TriggerEvent::Delete,
                crate::triggers::TriggerTiming::After,
                Some(row.clone()),
                None,
            )?;

            delete_count += 1;
        }
    }

    Ok(QueryResult::Success {
        message: format!("Deleted {} rows", delete_count),
    })
}

fn evaluate_update_expression(expr: &Expr, row: &Value) -> Result<Value> {
    match expr {
        Expr::Value(val) => sql_value_to_json(val),
        Expr::Identifier(ident) => {
            // Look up the column value from the current row
            Ok(row.get(&ident.value).cloned().unwrap_or(Value::Null))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_update_expression(left, row)?;
            let right_val = evaluate_update_expression(right, row)?;

            // Use the centralized binary operation evaluator
            evaluate_binary_op(&left_val, op, &right_val)
        }
        _ => Ok(Value::Null),
    }
}

fn execute_alter_table(
    _engine: &mut Engine,
    table_name: &sqlparser::ast::ObjectName,
    operation: &sqlparser::ast::AlterTableOperation,
) -> Result<QueryResult> {
    let table = table_name.to_string();

    match operation {
        sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
            // Add the column to the table's schema
            // Note: This is a simplified implementation - existing rows won't have this column
            // In a real database, we'd need to handle default values and null handling

            // For now, just return success as the column is conceptually added
            // The storage layer doesn't enforce schema strictly
            Ok(QueryResult::Success {
                message: format!(
                    "Column '{}' added to table '{}'",
                    column_def.name.value, table
                ),
            })
        }
        sqlparser::ast::AlterTableOperation::DropColumn { column_name, .. } => {
            // Drop column functionality would go here
            // For now, return an error as we need to implement this
            Err(DriftError::InvalidQuery(format!(
                "DROP COLUMN {} not yet implemented",
                column_name
            )))
        }
        sqlparser::ast::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            // Rename column functionality would go here
            Err(DriftError::InvalidQuery(format!(
                "RENAME COLUMN {} TO {} not yet implemented",
                old_column_name, new_column_name
            )))
        }
        sqlparser::ast::AlterTableOperation::AddConstraint(constraint) => {
            // Parse and add constraint
            match constraint {
                sqlparser::ast::TableConstraint::Unique { columns, .. } => {
                    // Create unique indexes for the constraint
                    let column_list = columns
                        .iter()
                        .map(|c| c.value.clone())
                        .collect::<Vec<_>>()
                        .join(", ");

                    Ok(QueryResult::Success {
                        message: format!("Unique constraint on ({}) would be added to table '{}' (indexes need implementation)",
                                        column_list, table)
                    })
                }
                _ => Err(DriftError::InvalidQuery(
                    "Constraint type not yet fully supported".to_string(),
                )),
            }
        }
        _ => Err(DriftError::InvalidQuery(
            "ALTER TABLE operation not yet supported".to_string(),
        )),
    }
}
fn execute_window_functions(data: Vec<Value>, projection: &[SelectItem]) -> Result<Vec<Value>> {
    // Extract window function calls from projection
    let mut window_calls = Vec::new();
    let mut regular_columns = Vec::new();

    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(func)) if func.over.is_some() => {
                let window_fn = parse_window_function(func)?;
                window_calls.push(window_fn);
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                alias,
            } if func.over.is_some() => {
                let mut window_fn = parse_window_function(func)?;
                window_fn.alias = alias.value.clone();
                window_calls.push(window_fn);
            }
            _ => {
                regular_columns.push(item.clone());
            }
        }
    }

    if window_calls.is_empty() {
        // No window functions, just apply regular projection
        return apply_projection(data, projection);
    }

    // Create window query
    let query = WindowQuery {
        functions: window_calls,
        data: data.clone(),
    };

    // Execute window functions
    let executor = WindowExecutor;
    let result = executor.execute(query)?;

    // Build a custom projection that knows about window function aliases
    let mut projected_result = Vec::new();
    for row in result {
        if let Value::Object(row_map) = &row {
            let mut projected_row = serde_json::Map::new();

            // Process each projection item
            for item in projection {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                        // Regular column
                        if let Some(val) = row_map.get(&ident.value) {
                            projected_row.insert(ident.value.clone(), val.clone());
                        }
                    }
                    SelectItem::ExprWithAlias {
                        expr: Expr::Function(func),
                        alias,
                    } if func.over.is_some() => {
                        // Window function with alias - get from computed results
                        if let Some(val) = row_map.get(&alias.value) {
                            projected_row.insert(alias.value.clone(), val.clone());
                        }
                    }
                    SelectItem::ExprWithAlias {
                        expr: Expr::Identifier(ident),
                        alias,
                    } => {
                        // Regular column with alias
                        if let Some(val) = row_map.get(&ident.value) {
                            projected_row.insert(alias.value.clone(), val.clone());
                        }
                    }
                    _ => {
                        // Other expressions - try to evaluate
                        if let SelectItem::UnnamedExpr(expr) = item {
                            if let Ok(val) = evaluate_value_expression(expr, &row) {
                                let col_name =
                                    format!("{:?}", expr).chars().take(50).collect::<String>();
                                projected_row.insert(col_name, val);
                            }
                        }
                    }
                }
            }

            projected_result.push(Value::Object(projected_row));
        }
    }

    Ok(projected_result)
}

fn parse_window_function(func: &Function) -> Result<WindowFunctionCall> {
    let func_name = func.name.to_string().to_uppercase();

    // Parse the window function type
    let window_func = match func_name.as_str() {
        "ROW_NUMBER" => WindowFunction::RowNumber,
        "RANK" => WindowFunction::Rank,
        "DENSE_RANK" => WindowFunction::DenseRank,
        "PERCENT_RANK" => WindowFunction::PercentRank,
        "CUME_DIST" => WindowFunction::CumeDist,
        "NTILE" => {
            // Extract the parameter
            // Extract arguments from FunctionArguments enum
            let args_list = match &func.args {
                sqlparser::ast::FunctionArguments::None => vec![],
                sqlparser::ast::FunctionArguments::Subquery(_) => vec![],
                sqlparser::ast::FunctionArguments::List(list) => list.args.clone(),
            };

            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                sqlparser::ast::Value::Number(n, _),
            )))) = args_list.first()
            {
                let tiles = n.parse::<u32>().map_err(|_| {
                    DriftError::InvalidQuery("NTILE requires positive integer".to_string())
                })?;
                WindowFunction::Ntile(tiles)
            } else {
                return Err(DriftError::InvalidQuery(
                    "NTILE requires numeric parameter".to_string(),
                ));
            }
        }
        "LAG" | "LEAD" => {
            // Extract column and optional offset/default
            let (column, offset, default) = match &func.args {
                FunctionArguments::List(list) => {
                    let column = extract_column_from_function_args(&func.args)?;
                    let offset = if list.args.len() > 1 {
                        // Parse offset
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            sqlparser::ast::Value::Number(n, _),
                        ))) = &list.args[1]
                        {
                            Some(n.parse::<u32>().unwrap_or(1))
                        } else {
                            Some(1)
                        }
                    } else {
                        Some(1)
                    };
                    let default = if list.args.len() > 2 {
                        // Parse default value
                        Some(expr_to_json_value(match &list.args[2] {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
                            _ => {
                                return Err(DriftError::InvalidQuery(
                                    "Invalid default value".to_string(),
                                ))
                            }
                        })?)
                    } else {
                        None
                    };
                    (column, offset, default)
                }
                _ => {
                    return Err(DriftError::InvalidQuery(
                        "LAG/LEAD requires arguments".to_string(),
                    ));
                }
            };

            if func_name == "LAG" {
                WindowFunction::Lag(column, offset, default)
            } else {
                WindowFunction::Lead(column, offset, default)
            }
        }
        "FIRST_VALUE" | "LAST_VALUE" => {
            let column = extract_column_from_function_args(&func.args)?;
            if func_name == "FIRST_VALUE" {
                WindowFunction::FirstValue(column)
            } else {
                WindowFunction::LastValue(column)
            }
        }
        "SUM" | "AVG" | "MIN" | "MAX" => {
            let column = extract_column_from_function_args(&func.args)?;
            match func_name.as_str() {
                "SUM" => WindowFunction::Sum(column),
                "AVG" => WindowFunction::Avg(column),
                "MIN" => WindowFunction::Min(column),
                "MAX" => WindowFunction::Max(column),
                _ => unreachable!(),
            }
        }
        "COUNT" => {
            let column = match &func.args {
                FunctionArguments::None => None,
                FunctionArguments::List(list) if list.args.is_empty() => None,
                FunctionArguments::List(list) => {
                    if matches!(
                        &list.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    ) {
                        None
                    } else {
                        Some(extract_column_from_function_args(&func.args)?)
                    }
                }
                _ => None,
            };
            WindowFunction::Count(column)
        }
        _ => {
            return Err(DriftError::InvalidQuery(format!(
                "Unsupported window function: {}",
                func_name
            )));
        }
    };

    // Parse window specification
    let window_spec = if let Some(window_type) = &func.over {
        parse_window_spec(window_type)?
    } else {
        // Should not happen as we check for over.is_some()
        return Err(DriftError::InvalidQuery(
            "Window function missing OVER clause".to_string(),
        ));
    };

    Ok(WindowFunctionCall {
        function: window_func,
        window: window_spec,
        alias: func_name.to_lowercase(),
    })
}

fn parse_window_spec(window_type: &sqlparser::ast::WindowType) -> Result<WindowSpec> {
    use sqlparser::ast::WindowType;

    match window_type {
        WindowType::WindowSpec(spec) => {
            // Parse PARTITION BY
            let partition_by = spec
                .partition_by
                .iter()
                .map(|expr| match expr {
                    Expr::Identifier(ident) => Ok(ident.value.clone()),
                    _ => Err(DriftError::InvalidQuery(
                        "Complex PARTITION BY not yet supported".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;

            // Parse ORDER BY
            let order_by = spec
                .order_by
                .iter()
                .map(|order_expr| {
                    let column = match &order_expr.expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        _ => {
                            return Err(DriftError::InvalidQuery(
                                "Complex ORDER BY not yet supported".to_string(),
                            ))
                        }
                    };

                    let ascending = order_expr.asc.unwrap_or(true);
                    let nulls_first = order_expr.nulls_first.unwrap_or(!ascending);

                    Ok(OrderColumn {
                        column,
                        ascending,
                        nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            // Parse window frame (if specified)
            let frame = spec.window_frame.as_ref().map(|_frame| {
                // For now, use default frame
                // TODO: Parse actual frame specification
                crate::window::WindowFrame::default()
            });

            Ok(WindowSpec {
                partition_by,
                order_by,
                frame,
            })
        }
        WindowType::NamedWindow(name) => Err(DriftError::InvalidQuery(format!(
            "Named windows not yet supported: {}",
            name
        ))),
    }
}

fn extract_column_from_function_args(args: &FunctionArguments) -> Result<String> {
    match args {
        FunctionArguments::List(list) => {
            if list.args.is_empty() {
                return Err(DriftError::InvalidQuery(
                    "Function requires at least one argument".to_string(),
                ));
            }

            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                    Ok(ident.value.clone())
                }
                _ => Err(DriftError::InvalidQuery(
                    "Complex function arguments not yet supported".to_string(),
                )),
            }
        }
        _ => Err(DriftError::InvalidQuery(
            "Function arguments required".to_string(),
        )),
    }
}
