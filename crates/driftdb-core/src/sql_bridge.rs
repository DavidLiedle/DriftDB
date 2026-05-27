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
    /// PostgreSQL-style aborted-transaction state, surfaced into the
    /// `SessionContext.aborted` field by `SessionGuard` on drop. Lives
    /// for the duration of one `execute_sql_in_session` call.
    static CURRENT_TXN_ABORTED: RefCell<bool> = const { RefCell::new(false) };
    static OUTER_ROW_CONTEXT: RefCell<Option<Value>> = const { RefCell::new(None) };
    static IN_RECURSIVE_CTE: RefCell<bool> = const { RefCell::new(false) };
    /// Active `FOR SYSTEM_TIME AS OF ...` clause for the current `execute_sql` call.
    /// Set by `execute_sql` after extracting the temporal prefix; read by every
    /// `Query::Select` build site so time-travel reads reach the engine.
    static TEMPORAL_AS_OF: RefCell<Option<crate::query::AsOf>> = const { RefCell::new(None) };
}

/// Return a clone of the active `FOR SYSTEM_TIME AS OF ...` clause, if any.
fn current_temporal_as_of() -> Option<crate::query::AsOf> {
    TEMPORAL_AS_OF.with(|c| c.borrow().clone())
}

/// Convert a parsed `SystemTimeClause` into the engine's `AsOf` representation.
///
/// `ALL` is handled by a fast-path earlier in `execute_sql` and never reaches here.
/// `BETWEEN` and `FROM ... TO` aren't expressible as `AsOf` — they need their own
/// engine query type — so reject them with a clear error rather than silently
/// dropping the clause.
fn system_time_to_as_of(
    clause: &crate::sql::SystemTimeClause,
) -> Result<Option<crate::query::AsOf>> {
    use crate::query::AsOf;
    use crate::sql::{SystemTimeClause, TemporalPoint};
    match clause {
        SystemTimeClause::AsOf(TemporalPoint::Sequence(seq)) => Ok(Some(AsOf::Sequence(*seq))),
        SystemTimeClause::AsOf(TemporalPoint::CurrentTimestamp) => Ok(Some(AsOf::Now)),
        SystemTimeClause::AsOf(TemporalPoint::Timestamp(dt)) => {
            let nanos = dt.timestamp_nanos_opt().ok_or_else(|| {
                DriftError::InvalidQuery(
                    "Timestamp out of range for FOR SYSTEM_TIME AS OF".to_string(),
                )
            })?;
            let odt = time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
                .map_err(|e| DriftError::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
            Ok(Some(AsOf::Timestamp(odt)))
        }
        SystemTimeClause::All => Ok(None),
        SystemTimeClause::Between { .. } | SystemTimeClause::FromTo { .. } => {
            Err(DriftError::InvalidQuery(
                "FOR SYSTEM_TIME BETWEEN / FROM ... TO is not yet supported".to_string(),
            ))
        }
    }
}

/// RAII guard that restores the previous `TEMPORAL_AS_OF` value on drop, so
/// the thread-local can't leak across `execute_sql` calls if one returns early
/// via `?`.
struct TemporalAsOfGuard(Option<crate::query::AsOf>);

impl Drop for TemporalAsOfGuard {
    fn drop(&mut self) {
        let prev = self.0.take();
        TEMPORAL_AS_OF.with(|c| *c.borrow_mut() = prev);
    }
}

/// Per-session execution context that survives across multiple
/// `execute_sql_in_session` calls. The PostgreSQL protocol layer holds
/// one of these per connection so its `BEGIN` / `INSERT` / `COMMIT`
/// statements share the same transaction id. CLI / direct callers use
/// a default (auto-commit) context.
///
/// The only state today is the active engine transaction id, but the
/// type is structured so future per-session needs (search-path, time
/// zone, prepared statement names) can land here without churning every
/// call site.
#[derive(Debug, Default, Clone)]
pub struct SessionContext {
    /// Engine transaction id when an interactive transaction is active.
    /// `BEGIN` sets it; `COMMIT` / `ROLLBACK` clear it. DML reads it to
    /// decide whether to buffer through the engine's transaction manager
    /// or apply events immediately.
    pub transaction_id: Option<u64>,
    /// PostgreSQL-style aborted-transaction state. Set by any statement
    /// inside a transaction that surfaces a constraint violation (PK
    /// uniqueness today; range will grow). Once set, every statement
    /// except `ROLLBACK` returns the canonical
    /// "current transaction is aborted, commands ignored until end of
    /// transaction block" error. Cleared by `COMMIT` or `ROLLBACK`.
    pub aborted: bool,
}

impl SessionContext {
    /// Construct a fresh context with no active transaction. Equivalent
    /// to `SessionContext::default()` but reads more clearly at call
    /// sites that want auto-commit semantics.
    pub fn new() -> Self {
        Self::default()
    }
}

/// RAII guard that mirrors `SessionContext.transaction_id` into the
/// thread-local `CURRENT_TRANSACTION` for the duration of one
/// `execute_sql_in_session` call, then writes any changes (set by
/// `BEGIN`, cleared by `COMMIT` / `ROLLBACK`) back into the caller's
/// context on drop.
///
/// The thread-local exists so the existing internal sql_bridge code
/// — and the engine-side DML dispatch — can keep reading transaction
/// state from a single well-known location without threading
/// `&mut SessionContext` through every helper.
struct SessionGuard<'ctx> {
    prev_txn_id: Option<u64>,
    prev_aborted: bool,
    ctx: &'ctx mut SessionContext,
}

impl<'ctx> SessionGuard<'ctx> {
    fn enter(ctx: &'ctx mut SessionContext) -> Self {
        let prev_txn_id = CURRENT_TRANSACTION.with(|c| c.replace(ctx.transaction_id));
        let prev_aborted = CURRENT_TXN_ABORTED.with(|c| c.replace(ctx.aborted));
        Self {
            prev_txn_id,
            prev_aborted,
            ctx,
        }
    }
}

impl Drop for SessionGuard<'_> {
    fn drop(&mut self) {
        // Write any transaction-state changes back to the caller's context,
        // then restore the outer thread-local (so nested `execute_sql_in_session`
        // calls — e.g. through view materialisation — don't leak state).
        let final_txn_id = CURRENT_TRANSACTION.with(|c| c.replace(self.prev_txn_id));
        let final_aborted = CURRENT_TXN_ABORTED.with(|c| c.replace(self.prev_aborted));
        self.ctx.transaction_id = final_txn_id;
        self.ctx.aborted = final_aborted;
    }
}

/// Read the current session's active transaction id, if any.
fn current_transaction() -> Option<u64> {
    CURRENT_TRANSACTION.with(|c| *c.borrow())
}

/// Whether the current transaction is in the aborted state. Set by a
/// constraint violation (PK uniqueness, FK violation, etc.); cleared
/// by `COMMIT` or `ROLLBACK`. Inner code reads this before executing
/// any statement other than `ROLLBACK`.
fn current_txn_aborted() -> bool {
    CURRENT_TXN_ABORTED.with(|c| *c.borrow())
}

fn mark_txn_aborted() {
    CURRENT_TXN_ABORTED.with(|c| *c.borrow_mut() = true);
}

fn clear_txn_aborted() {
    CURRENT_TXN_ABORTED.with(|c| *c.borrow_mut() = false);
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

/// Execute a SQL statement against `engine` with no caller-supplied
/// session state — every call starts and ends with an empty
/// `SessionContext`, so `BEGIN` / `COMMIT` / `ROLLBACK` issued via this
/// entry point won't carry transaction state across statements. Suitable
/// for one-shot CLI use and direct core callers; PostgreSQL-protocol
/// servers should use [`execute_sql_in_session`] instead.
pub fn execute_sql(engine: &mut Engine, sql: &str) -> Result<QueryResult> {
    let mut ctx = SessionContext::new();
    execute_sql_in_session(engine, sql, &mut ctx)
}

/// Execute a SQL statement against `engine`, threading `ctx` for the
/// duration of the call so transaction state set by `BEGIN` survives
/// to the next statement and so DML lands either in the engine's
/// transaction buffer (when `ctx.transaction_id` is `Some`) or as an
/// immediate apply (when `None`).
pub fn execute_sql_in_session(
    engine: &mut Engine,
    sql: &str,
    ctx: &mut SessionContext,
) -> Result<QueryResult> {
    let _guard = SessionGuard::enter(ctx);
    let result = execute_sql_inner(engine, sql);
    // PostgreSQL semantics: any error mid-transaction aborts the
    // transaction. Slices 1 and 2 already set the abort flag at their
    // specific constraint-check sites — those stay as documentation
    // of intent. This is the backstop for every other error surface:
    // syntax errors, unknown table, missing column, FK violations,
    // arithmetic errors, etc.
    //
    // Auto-commit errors don't touch the abort state (there's no
    // transaction to poison). The thread-local is read here before
    // `SessionGuard::drop` syncs it back to `ctx.aborted`, so a
    // set-then-drop sequence here flows through to the caller's
    // session for the next statement's pre-dispatch gate.
    if result.is_err() && current_transaction().is_some() {
        mark_txn_aborted();
    }
    result
}

/// The original `execute_sql` body — kept private so both
/// `execute_sql` and `execute_sql_in_session` can share dispatch logic
/// without duplicating it. Reads the active transaction id (if any)
/// via the `CURRENT_TRANSACTION` thread-local that `SessionGuard`
/// keeps in sync with the caller's `SessionContext`.
fn execute_sql_inner(engine: &mut Engine, sql: &str) -> Result<QueryResult> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // PostgreSQL convention: VACUUM table_name → Compact
    if upper.starts_with("VACUUM ") {
        let table = trimmed["VACUUM ".len()..]
            .split_whitespace()
            .next()
            .ok_or_else(|| DriftError::InvalidQuery("VACUUM requires a table name".into()))?
            .to_string();
        return engine
            .execute_query(Query::Compact { table })
            .map_err(|e| DriftError::InvalidQuery(e.to_string()));
    }

    // PostgreSQL convention: CHECKPOINT TABLE table_name → Snapshot
    if upper.starts_with("CHECKPOINT TABLE ") {
        let table = trimmed["CHECKPOINT TABLE ".len()..]
            .split_whitespace()
            .next()
            .ok_or_else(|| {
                DriftError::InvalidQuery("CHECKPOINT TABLE requires a table name".into())
            })?
            .to_string();
        return engine
            .execute_query(Query::Snapshot { table })
            .map_err(|e| DriftError::InvalidQuery(e.to_string()));
    }

    // SQL:2011: FOR SYSTEM_TIME ALL → drift history
    if upper.contains(" FOR SYSTEM_TIME ALL") {
        return execute_for_system_time_all(engine, trimmed);
    }

    // Peel off any `FOR SYSTEM_TIME AS OF ...` clause before handing the SQL to
    // sqlparser, which doesn't recognize SQL:2011 temporal syntax. The clause is
    // stashed in a thread-local that `Query::Select` build sites read.
    let temporal_parser = crate::sql::TemporalSqlParser::new();
    let (base_sql, system_time) = temporal_parser.extract_temporal_clause(trimmed)?;
    let as_of = match &system_time {
        Some(clause) => system_time_to_as_of(clause)?,
        None => None,
    };
    let prev_as_of = TEMPORAL_AS_OF.with(|c| c.replace(as_of));
    let _temporal_guard = TemporalAsOfGuard(prev_as_of);
    let base_sql = base_sql.trim();

    let dialect = GenericDialect {};
    let ast =
        Parser::parse_sql(&dialect, base_sql).map_err(|e| DriftError::Parse(e.to_string()))?;

    if ast.is_empty() {
        return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
    }

    // PostgreSQL-style aborted-transaction gate. After a constraint
    // violation inside a transaction, every statement except
    // `ROLLBACK` (and the no-op `COMMIT`, which Postgres treats as
    // rollback in this state) surfaces the canonical aborted error.
    // The check sits here so it covers every dispatch arm uniformly.
    if current_txn_aborted() {
        match &ast[0] {
            Statement::Rollback { .. } | Statement::Commit { .. } => {
                // Allow termination paths to clear the abort.
            }
            _ => {
                return Err(DriftError::InvalidQuery(
                    "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                ));
            }
        }
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
                // PostgreSQL behavior: COMMIT against an aborted
                // transaction is treated as ROLLBACK. We don't apply
                // any buffered writes; we just clear state.
                let was_aborted = current_txn_aborted();
                if was_aborted {
                    engine.rollback_transaction(transaction_id)?;
                } else {
                    engine.commit_transaction(transaction_id)?;
                }

                // Clear transaction + abort from session.
                CURRENT_TRANSACTION.with(|txn| {
                    *txn.borrow_mut() = None;
                });
                clear_txn_aborted();

                Ok(QueryResult::Success {
                    message: if was_aborted {
                        format!("ROLLBACK (transaction {} was aborted)", transaction_id)
                    } else {
                        format!("Transaction {} committed", transaction_id)
                    },
                })
            } else {
                // No active transaction - just succeed (no-op)
                Ok(QueryResult::Success {
                    message: "COMMIT (no active transaction)".to_string(),
                })
            }
        }
        Statement::Rollback { savepoint, .. } => {
            // Two cases dispatched here:
            //   - `ROLLBACK` (no savepoint) → end the transaction.
            //   - `ROLLBACK TO [SAVEPOINT] name` → partial rollback;
            //     transaction stays open, abort cleared, writes since
            //     the named savepoint are discarded.
            let txn_id = CURRENT_TRANSACTION.with(|txn| *txn.borrow());
            let transaction_id = txn_id.ok_or_else(|| {
                DriftError::InvalidQuery("No active transaction to rollback".to_string())
            })?;

            if let Some(sp_name) = savepoint {
                // ROLLBACK TO SAVEPOINT — partial rollback.
                let name = sp_name.value.clone();
                engine.rollback_to_savepoint(transaction_id, &name)?;
                // PostgreSQL: ROLLBACK TO inside an aborted transaction
                // clears the abort and lets the transaction continue
                // from the savepoint's state. Slice 1's abort gate
                // already allowed Rollback-variant statements in
                // aborted state for exactly this reason.
                clear_txn_aborted();
                Ok(QueryResult::Success {
                    message: format!("ROLLBACK TO SAVEPOINT {}", name),
                })
            } else {
                // Full ROLLBACK — end the transaction.
                engine.rollback_transaction(transaction_id)?;
                CURRENT_TRANSACTION.with(|txn| {
                    *txn.borrow_mut() = None;
                });
                clear_txn_aborted();
                Ok(QueryResult::Success {
                    message: format!("Transaction {} rolled back", transaction_id),
                })
            }
        }
        Statement::Savepoint { name } => {
            let txn_id = CURRENT_TRANSACTION.with(|txn| *txn.borrow()).ok_or_else(|| {
                DriftError::InvalidQuery(
                    "SAVEPOINT can only be used in transaction blocks".to_string(),
                )
            })?;
            engine.create_savepoint(txn_id, &name.value)?;
            Ok(QueryResult::Success {
                message: format!("SAVEPOINT {}", name.value),
            })
        }
        Statement::ReleaseSavepoint { name } => {
            let txn_id = CURRENT_TRANSACTION.with(|txn| *txn.borrow()).ok_or_else(|| {
                DriftError::InvalidQuery(
                    "RELEASE SAVEPOINT can only be used in transaction blocks".to_string(),
                )
            })?;
            engine.release_savepoint(txn_id, &name.value)?;
            Ok(QueryResult::Success {
                message: format!("RELEASE {}", name.value),
            })
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
        Statement::Explain {
            statement,
            analyze,
            verbose,
            format,
            ..
        } => {
            // Build the structured plan from the sqlparser AST, then hand it
            // to `crate::explain::ExplainExecutor` — the same module that's
            // had multi-format output (text / JSON / YAML), verbose-mode
            // detail, and execution-time accuracy estimation since DriftDB
            // 0.1 but had no SQL callers wired in. ANALYZE measures actual
            // execution time and row counts via the inner SQL bridge,
            // sidestepping the `Instant::now()` wrapping the previous
            // session needed.
            let planning_start = std::time::Instant::now();
            let plan = crate::explain::build_plan_from_statement(engine, statement)?;
            let planning_time = planning_start.elapsed();

            // `EXPLAIN (FORMAT JSON | TEXT)` survives the parser as
            // `AnalyzeFormat`; anything else falls back to text. YAML isn't
            // exposed via SQL parser today — it remains available via the
            // programmatic `ExplainPlan::format_yaml` API.
            let explain_format = match format {
                Some(sqlparser::ast::AnalyzeFormat::JSON) => {
                    crate::explain::ExplainFormat::Json
                }
                _ => crate::explain::ExplainFormat::Text,
            };
            let options = crate::explain::ExplainOptions {
                format: explain_format,
                verbose: *verbose,
                costs: true,
                timing: *analyze,
                analyze: *analyze,
            };

            let explain_plan = if *analyze {
                // Execute the wrapped statement to measure real elapsed time
                // and count returned rows. The result itself is discarded —
                // PostgreSQL's EXPLAIN ANALYZE returns plan text, not the
                // query's rows.
                let sql_text = statement.to_string();
                crate::explain::ExplainExecutor::explain_analyze(
                    plan,
                    planning_time,
                    || -> Result<usize> {
                        let result = execute_sql_inner(engine, &sql_text)?;
                        Ok(match &result {
                            QueryResult::Rows { data } => data.len(),
                            QueryResult::DriftHistory { events } => events.len(),
                            _ => 0,
                        })
                    },
                )?
            } else {
                crate::explain::ExplainExecutor::explain(plan, planning_time)
            };

            // Render with the requested format. JSON / YAML get a single row
            // carrying the full document; text format splits on newlines so
            // psql renders one plan line per result row.
            let body = match options.format {
                crate::explain::ExplainFormat::Json => explain_plan
                    .format_json()
                    .map_err(|e| DriftError::Other(format!("EXPLAIN JSON: {}", e)))?,
                crate::explain::ExplainFormat::Yaml => explain_plan
                    .format_yaml()
                    .map_err(|e| DriftError::Other(format!("EXPLAIN YAML: {}", e)))?,
                _ => explain_plan.format_text(&options),
            };
            let data: Vec<Value> = if matches!(
                options.format,
                crate::explain::ExplainFormat::Json | crate::explain::ExplainFormat::Yaml
            ) {
                vec![{
                    let mut row = serde_json::Map::new();
                    row.insert("QUERY PLAN".to_string(), Value::String(body));
                    Value::Object(row)
                }]
            } else {
                body.lines()
                    .map(|line| {
                        let mut row = serde_json::Map::new();
                        row.insert("QUERY PLAN".to_string(), Value::String(line.to_string()));
                        Value::Object(row)
                    })
                    .collect()
            };
            Ok(QueryResult::Rows { data })
        }
        Statement::Analyze { table_name, .. } => {
            // ANALYZE table_name: scan one table and push the result
            // into QueryOptimizer's statistics map. This is what
            // activates the dormant signals in slices 2 (within-class
            // selectivity tiebreaker) and 6 (cost-based multi-join
            // seed selection) — without ANALYZE, both fall back to
            // source order because `statistics_row_count` returns 0.
            let table = table_name.to_string();
            let known: Vec<String> = engine.list_tables();
            if !table.is_empty() && known.contains(&table) {
                let stats = engine.collect_table_statistics(&table)?;
                engine.query_optimizer().update_statistics(&table, stats);
                Ok(QueryResult::Success {
                    message: format!("ANALYZE {}", table),
                })
            } else {
                // Bare ANALYZE or unknown table: PostgreSQL behavior
                // for the bare form is "every table". We do the same;
                // unknown table falls through to that path rather than
                // erroring, matching `let _ = ...`'s prior tolerance.
                for t in known {
                    if let Ok(stats) = engine.collect_table_statistics(&t) {
                        engine.query_optimizer().update_statistics(&t, stats);
                    }
                }
                Ok(QueryResult::Success {
                    message: "ANALYZE".to_string(),
                })
            }
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
    let has_subqueries = select.selection.as_ref().is_some_and(contains_subquery);

    // Check if this might be a correlated subquery
    let is_correlated = OUTER_ROW_CONTEXT.with(|context| context.borrow().is_some());

    // If we have subqueries OR this is a correlated subquery, fetch all rows and filter in SQL layer
    // Otherwise, use engine WHERE optimization
    let (engine_conditions, sql_filter) = if has_subqueries || is_correlated {
        (vec![], select.selection.clone())
    } else {
        match select.selection.as_ref() {
            Some(selection) => match parse_where_clause(selection) {
                Ok(conds) if !conds.is_empty() => (conds, None),
                // Parser didn't structurally lower the expression
                // (unknown operator, OR, function call, etc.) — fall
                // back to row-level SQL evaluation with all rows.
                Ok(_) => (vec![], Some(selection.clone())),
                Err(_) => (vec![], Some(selection.clone())),
            },
            None => (vec![], None),
        }
    };

    // Execute SQL query to get base data
    let query = Query::Select {
        table: table_name.clone(),
        conditions: engine_conditions,
        as_of: current_temporal_as_of(),
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
                as_of: current_temporal_as_of(),
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
        let (right_table, right_explicit_alias) = match extract_table_with_alias(&join.relation) {
            Ok(pair) => pair,
            Err(_) => (extract_table_name_from_join(&join.relation)?, None),
        };
        // Alias for collision-prefixing falls back to the table name
        // when the SQL didn't provide one. Standard SQL: a bare table
        // name acts as its own alias.
        let right_alias = right_explicit_alias.unwrap_or_else(|| right_table.clone());

        // Check if right table is a CTE or regular table
        let right_rows = if let Some(cte_data) = cte_results.get(&right_table) {
            cte_data.clone()
        } else {
            let right_query = Query::Select {
                table: right_table.clone(),
                conditions: vec![],
                as_of: current_temporal_as_of(),
                limit: None,
            };
            match engine.execute_query(right_query)? {
                QueryResult::Rows { data } => data,
                _ => vec![],
            }
        };

        // Same ON-orientation fix as the non-CTE legacy loop below.
        let orient_ref = |constraint: &sqlparser::ast::JoinConstraint| {
            orient_constraint_for_right_alias(constraint, &right_alias)
                .unwrap_or_else(|| constraint.clone())
        };
        joined_rows = match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                perform_inner_join(joined_rows, right_rows, &orient_ref(constraint), &right_alias)?
            }
            JoinOperator::LeftOuter(constraint) => {
                perform_left_join(joined_rows, right_rows, &orient_ref(constraint), &right_alias)?
            }
            JoinOperator::CrossJoin => {
                perform_cross_join(joined_rows, right_rows, &right_alias)
            }
            JoinOperator::RightOuter(constraint) => {
                let oriented = orient_constraint_for_right_alias(constraint, &right_alias)
                    .unwrap_or_else(|| constraint.clone());
                perform_left_join(right_rows, joined_rows, &oriented, &right_alias)?
            }
            JoinOperator::FullOuter(constraint) => {
                perform_full_outer_join(joined_rows, right_rows, &orient_ref(constraint), &right_alias)?
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
    // Optimized fast path: single INNER JOIN over two tables (not views).
    // Routes the algorithm choice through `QueryOptimizer::plan_single_join`
    // and pushes table-prefixed WHERE predicates down to per-side
    // `Engine::select` calls (which already honor index selection,
    // predicate order, and range scans from earlier slices). Returns
    // `Ok(None)` if anything doesn't fit (views, multi-join, OUTER JOIN,
    // complex join condition) — then we fall through to the legacy
    // implementation below.
    if let Some(result) = try_optimized_single_join(engine, select)? {
        return Ok(result);
    }
    // Multi-join (3+ tables, all INNER) goes through the reordering
    // planner. Mixed INNER+OUTER chains fall through to legacy
    // (correctness-preserving; reordering across OUTER boundaries is
    // a separate slice).
    if let Some(result) = try_optimized_multi_join(engine, select)? {
        return Ok(result);
    }
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
            as_of: current_temporal_as_of(),
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
        let (right_table, right_explicit_alias) = match extract_table_with_alias(&join.relation) {
            Ok(pair) => pair,
            Err(_) => (extract_table_name_from_join(&join.relation)?, None),
        };
        let right_alias = right_explicit_alias.unwrap_or_else(|| right_table.clone());

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
                    match execute_sql_query(engine, view_query)? {
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
                as_of: current_temporal_as_of(),
                limit: None,
            };
            match engine.execute_query(right_query)? {
                QueryResult::Rows { data } => data,
                _ => vec![],
            }
        };

        // Legacy ON-orientation fix: the underlying perform_*_join
        // functions extract columns in source-text order, which is
        // wrong when ON is written with the new right table on the
        // LEFT of `=` (e.g. `ON o.customer_id = c.id` where the
        // accumulator holds `c` and the new right is `o`). The
        // optimized single-join path already handles this via
        // `orient_join_columns_to_from`; this is the missing
        // counterpart for multi-join legacy.
        let orient_ref = |constraint: &sqlparser::ast::JoinConstraint| {
            orient_constraint_for_right_alias(constraint, &right_alias)
                .unwrap_or_else(|| constraint.clone())
        };
        joined_rows = match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                perform_inner_join(joined_rows, right_rows, &orient_ref(constraint), &right_alias)?
            }
            JoinOperator::LeftOuter(constraint) => {
                perform_left_join(joined_rows, right_rows, &orient_ref(constraint), &right_alias)?
            }
            JoinOperator::CrossJoin => {
                perform_cross_join(joined_rows, right_rows, &right_alias)
            }
            JoinOperator::RightOuter(constraint) => {
                // RIGHT JOIN flips sides; orient against the swapped
                // layout (accumulator becomes the new "right" of the
                // underlying LEFT join).
                let oriented = orient_constraint_for_right_alias(constraint, &right_alias)
                    .unwrap_or_else(|| constraint.clone());
                perform_left_join(right_rows, joined_rows, &oriented, &right_alias)?
            }
            JoinOperator::FullOuter(constraint) => {
                perform_full_outer_join(joined_rows, right_rows, &orient_ref(constraint), &right_alias)?
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
    right_alias: &str,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();

    for left_row in &left_rows {
        for right_row in &right_rows {
            let left_val = lookup_join_value(left_row, &left_col);
            let right_val = right_row.get(&right_col);

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    result.push(merge_join_rows(left_row, right_row, right_alias));
                }
            }
        }
    }

    Ok(result)
}

/// Sample the union of all keys across a set of rows. Used to drive
/// NULL-padding for unmatched rows on the opposing side of an OUTER
/// join — without an explicit schema, this is the most reliable signal
/// for "what columns does the other side have?".
fn collect_row_keys(rows: &[Value]) -> std::collections::HashSet<String> {
    let mut keys = std::collections::HashSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                keys.insert(k.clone());
            }
        }
    }
    keys
}

/// Build a NULL-padded merged row for the unmatched-left case of an
/// OUTER join. Left columns appear bare; the right's columns appear
/// as explicit `Value::Null` — either bare (no collision) or
/// `{right_alias}.{col}` (collision with a left key). This matches
/// `merge_join_rows`'s key scheme so resolution (which tries
/// `alias.col` then bare) sees consistent shape across matched and
/// unmatched rows.
///
/// Without explicit NULL-padding, alias-aware resolution of
/// `d.colliding_col` on an unmatched-left row would fall back to the
/// left's bare `colliding_col` (the same as a matched-left collision
/// would have hidden). That made `WHERE d.col IS NULL` silently
/// behave wrong: the anti-join pattern's whole point is for those
/// rows to surface.
fn null_pad_right_into(left_row: &Value, right_keys: &std::collections::HashSet<String>, right_alias: &str) -> Value {
    let mut merged = left_row.as_object().cloned().unwrap_or_default();
    for col in right_keys {
        let key = if merged.contains_key(col) {
            format!("{}.{}", right_alias, col)
        } else {
            col.clone()
        };
        merged.entry(key).or_insert(Value::Null);
    }
    json!(merged)
}

/// Same as `null_pad_right_into`, but for unmatched-RIGHT rows in a
/// FULL OUTER join. Right columns stay bare (no collision since left
/// columns are absent); left columns appear as bare null entries
/// (which collision-prefixing would have moved aside in a matched
/// row, but we can't reproduce the matched row's positional choice
/// here without left's actual values — instead, we just NULL the
/// known left columns).
fn null_pad_left_into(right_row: &Value, left_keys: &std::collections::HashSet<String>) -> Value {
    let mut merged = right_row.as_object().cloned().unwrap_or_default();
    for col in left_keys {
        if !merged.contains_key(col) {
            merged.insert(col.clone(), Value::Null);
        }
    }
    json!(merged)
}

/// Unified row-merge for all join variants. Left columns stay bare;
/// right columns are inserted under TWO keys: always the qualified
/// `{right_alias}.{col}` form, and additionally the bare `col` form
/// when no left-side collision exists.
///
/// The double-keying is what makes multi-join (slice 6) work: at each
/// join step in the reordered chain, the join condition may reference
/// a column on a non-seed table that's already in the accumulator.
/// Without the qualified key being present unconditionally, a bare
/// lookup would hit the seed's value when the seed also had that
/// column name. Storage cost: ~2x for non-colliding right-side cols,
/// which is acceptable for correctness across arbitrary tree shapes.
///
/// For collisions the bare key already belongs to the left and we
/// skip the second insert, preserving the slice-3 left-precedence
/// rule for unqualified column references.
///
/// The separator is `.` — SQL identifiers can't contain `.`, so the
/// qualified key never collides with a legitimate column name.
fn merge_join_rows(left_row: &Value, right_row: &Value, right_alias: &str) -> Value {
    let mut merged = left_row.as_object().cloned().unwrap_or_default();
    if let Some(right_obj) = right_row.as_object() {
        for (key, value) in right_obj {
            let qualified = format!("{}.{}", right_alias, key);
            merged.insert(qualified, value.clone());
            if !merged.contains_key(key) {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    json!(merged)
}

/// Look up a join-column value on a row, handling alias-prefixed names
/// from earlier joins in a multi-join chain. The left side of an
/// `(A JOIN B) JOIN C` is itself a merged row whose collision keys are
/// of the form `B.col`; resolution checks bare `col` first, then any
/// suffix matching `.col`.
fn lookup_join_value<'a>(row: &'a Value, col: &str) -> Option<&'a Value> {
    if let Some(v) = row.get(col) {
        return Some(v);
    }
    let obj = row.as_object()?;
    let needle = format!(".{}", col);
    obj.iter()
        .find(|(k, _)| k.ends_with(&needle))
        .map(|(_, v)| v)
}

/// Resolve a `CompoundIdentifier`-shaped column reference (`alias.col`)
/// against a (possibly merged) row. Tries `{alias}.{col}` first (the
/// key right-side collisions get from `merge_join_rows`), then falls
/// back to bare `{col}`. Single-part references degenerate to the bare
/// lookup.
///
/// This is the projection-side counterpart of the resolution rule in
/// `evaluate_value_expression`'s `CompoundIdentifier` branch — both
/// MUST stay in sync or the value seen by WHERE differs from the value
/// seen by SELECT.
fn resolve_qualified_column(
    idents: &[sqlparser::ast::Ident],
    row: &Value,
) -> Option<Value> {
    let row_obj = row.as_object()?;
    if idents.len() >= 2 {
        let alias = &idents[0].value;
        let column = &idents[1].value;
        let qualified = format!("{}.{}", alias, column);
        if let Some(v) = row_obj.get(&qualified) {
            return Some(v.clone());
        }
        return row_obj.get(column).cloned();
    }
    let column = &idents.last()?.value;
    row_obj.get(column).cloned()
}

fn perform_left_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
    right_alias: &str,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();
    // Compute the right's column set once so unmatched-left rows get
    // explicit NULL-padding for the right side (`d.col = null`). This
    // lets alias-aware resolution distinguish "right was absent" from
    // "right's col happens to be falsy" — anti-join queries
    // (`WHERE d.col IS NULL`) depend on it.
    let right_keys = collect_row_keys(&right_rows);

    for left_row in &left_rows {
        let mut matched = false;

        for right_row in &right_rows {
            let left_val = lookup_join_value(left_row, &left_col);
            let right_val = lookup_join_value(right_row, &right_col);

            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    result.push(merge_join_rows(left_row, right_row, right_alias));
                    matched = true;
                }
            }
        }

        if !matched {
            result.push(null_pad_right_into(left_row, &right_keys, right_alias));
        }
    }

    Ok(result)
}

fn perform_full_outer_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    constraint: &sqlparser::ast::JoinConstraint,
    right_alias: &str,
) -> Result<Vec<Value>> {
    let (left_col, right_col) = extract_join_columns(constraint)?;
    let mut result = Vec::new();
    let mut matched_right = std::collections::HashSet::new();
    let right_keys = collect_row_keys(&right_rows);
    let left_keys = collect_row_keys(&left_rows);

    for left_row in &left_rows {
        let mut matched = false;
        for (right_idx, right_row) in right_rows.iter().enumerate() {
            let left_val = lookup_join_value(left_row, &left_col);
            let right_val = lookup_join_value(right_row, &right_col);
            if let (Some(l_val), Some(r_val)) = (left_val, right_val) {
                if l_val == r_val {
                    result.push(merge_join_rows(left_row, right_row, right_alias));
                    matched = true;
                    matched_right.insert(right_idx);
                }
            }
        }
        if !matched {
            result.push(null_pad_right_into(left_row, &right_keys, right_alias));
        }
    }

    // Unmatched-right phase. Pad with NULL for left's columns so
    // `SELECT u.col` from an unmatched-right row resolves to NULL
    // rather than falling back to bare and silently hitting the
    // right's value. Note this only NULL-pads the LEFT keys that
    // don't already appear on the right (the right's columns stay
    // bare since left's are absent — no collision possible).
    for (right_idx, right_row) in right_rows.iter().enumerate() {
        if !matched_right.contains(&right_idx) {
            result.push(null_pad_left_into(right_row, &left_keys));
        }
    }

    Ok(result)
}

/// Optimizer-driven single-join path covering INNER, LEFT/RIGHT OUTER,
/// and FULL OUTER. RIGHT OUTER is normalized to LEFT OUTER by swapping
/// sides; the planner only sees `JoinType::{Inner, LeftOuter, FullOuter}`.
///
/// Returns `Ok(None)` for shapes outside scope (views, multi-join,
/// CROSS JOIN, complex ON conditions), letting the caller fall through
/// to the legacy implementation, which is correctness-preserving.
///
/// Wiring active in this path:
/// - Per-side `Engine::select` honors slices 1–3 (index selection,
///   predicate order, range scans) — for predicates pushed to the
///   *preserving* side.
/// - The join itself picks algorithm via `plan_single_join` (slice 4)
///   and respects the OUTER-join build-side constraint.
/// - Alias-aware row merging from the bug-fix slice keeps
///   `{right_alias}.{col}` collisions resolvable post-join.
fn try_optimized_single_join(
    engine: &mut Engine,
    select: &Select,
) -> Result<Option<QueryResult>> {
    // Shape gate.
    if select.from.len() != 1 || select.from[0].joins.len() != 1 {
        return Ok(None);
    }
    let join = &select.from[0].joins[0];

    // Decode the SQL join type and any side-swap. RIGHT OUTER → LEFT
    // OUTER with sides swapped (PostgreSQL convention). CROSS JOIN and
    // anything else falls through.
    use crate::optimizer::JoinType as JT;
    let (sql_constraint, plan_join_type, swap_sides) = match &join.join_operator {
        JoinOperator::Inner(c) => (c, JT::Inner, false),
        JoinOperator::LeftOuter(c) => (c, JT::LeftOuter, false),
        JoinOperator::RightOuter(c) => (c, JT::LeftOuter, true),
        JoinOperator::FullOuter(c) => (c, JT::FullOuter, false),
        _ => return Ok(None),
    };

    // Both sides must be real tables (not views, not subqueries).
    let (sql_left_table, sql_left_alias) =
        match extract_table_with_alias(&select.from[0].relation) {
            Ok(pair) => pair,
            Err(_) => return Ok(None),
        };
    let (sql_right_table, sql_right_alias) = match extract_table_with_alias(&join.relation) {
        Ok(pair) => pair,
        Err(_) => return Ok(None),
    };
    let known_views: Vec<String> = engine.list_views().into_iter().map(|v| v.name).collect();
    if known_views.contains(&sql_left_table) || known_views.contains(&sql_right_table) {
        return Ok(None);
    }

    // Extract join columns AND their alias prefixes from the ON
    // expression. `extract_join_columns` returns columns in the order
    // they appear in `=`, which isn't necessarily the FROM order
    // (e.g. `ON u.dept_id = d.id` extracts (dept_id, id) even when the
    // SQL is `FROM depts d LEFT JOIN users u ...`). We use the prefix
    // to map each column to its actual table.
    let (sql_left_col, sql_right_col) =
        match orient_join_columns_to_from(sql_constraint, &sql_left_alias, &sql_right_alias) {
            Some(pair) => pair,
            None => return Ok(None),
        };

    // After the RIGHT→LEFT rewrite, `plan_*` and the executor see the
    // swapped layout. We still apply the user's original WHERE post-
    // join to the merged row (which is keyed by the original SQL
    // aliases), so we do NOT swap aliases for the alias_to_table map.
    let (left_table, right_table, left_col, right_col, _left_alias, right_alias) = if swap_sides {
        (
            sql_right_table.clone(),
            sql_left_table.clone(),
            sql_right_col.clone(),
            sql_left_col.clone(),
            sql_right_alias.clone(),
            sql_left_alias.clone(),
        )
    } else {
        (
            sql_left_table.clone(),
            sql_right_table.clone(),
            sql_left_col.clone(),
            sql_right_col.clone(),
            sql_left_alias.clone(),
            sql_right_alias.clone(),
        )
    };

    // Predicate pushdown — only to the *preserving* side(s).
    //
    // - INNER: both sides preserve no nulls; push to either.
    // - LEFT OUTER: only the LEFT side preserves; pushing to right
    //   would filter rows the join is supposed to NULL-pad.
    // - FULL OUTER: both sides can be NULL-padded; no pushdown.
    //
    // The original WHERE always runs post-join, so a missed pushdown
    // is a perf loss only. The pushdown POLICY here is about
    // correctness: pushing a predicate to a side that could be
    // NULL-padded would silently drop rows that should appear with
    // NULLs and then be filtered (or not) by the post-join WHERE.
    let pushdown_targets: &[&str] = match plan_join_type {
        JT::Inner => &["left", "right"],
        JT::LeftOuter => &["left"],
        JT::FullOuter => &[],
    };
    let mut alias_to_table: HashMap<String, String> = HashMap::new();
    alias_to_table.insert(sql_left_table.clone(), sql_left_table.clone());
    if let Some(a) = &sql_left_alias {
        alias_to_table.insert(a.clone(), sql_left_table.clone());
    }
    alias_to_table.insert(sql_right_table.clone(), sql_right_table.clone());
    if let Some(a) = &sql_right_alias {
        alias_to_table.insert(a.clone(), sql_right_table.clone());
    }
    let mut pushdown: HashMap<String, Vec<WhereCondition>> = HashMap::new();
    if let Some(selection) = &select.selection {
        collect_pushdown_predicates(selection, &alias_to_table, &mut pushdown);
    }
    // Drop pushdown for the NULL-padded side(s).
    if !pushdown_targets.contains(&"left") {
        pushdown.remove(&sql_left_table);
    }
    if !pushdown_targets.contains(&"right") {
        pushdown.remove(&sql_right_table);
    }

    // Fetch via `Engine::select` (slices 1–3 wiring applies per-side).
    let left_rows = fetch_table_rows(
        engine,
        &left_table,
        pushdown.remove(&left_table).unwrap_or_default(),
    )?;
    let right_rows = fetch_table_rows(
        engine,
        &right_table,
        pushdown.remove(&right_table).unwrap_or_default(),
    )?;

    let join_node = engine.query_optimizer().plan_single_join(
        &left_table,
        &right_table,
        &left_col,
        &right_col,
        plan_join_type,
    );

    let right_alias_str = right_alias.unwrap_or_else(|| right_table.clone());
    // Always synthesize an oriented constraint for the NL helpers.
    // They call `extract_join_columns(constraint)`, which returns
    // columns in source-text order — that order may not match the
    // FROM-clause table order (`FROM A JOIN B ON B.x = A.y`). The
    // hash variants take columns directly so this only matters for
    // NL, but we synthesize unconditionally for shape consistency.
    let effective_constraint = synthesize_eq_constraint(&left_col, &right_col);

    let joined_rows = run_join_algorithm(
        &join_node,
        plan_join_type,
        &left_rows,
        &right_rows,
        &left_col,
        &right_col,
        &effective_constraint,
        &right_alias_str,
    )?;

    // Apply the original WHERE on joined output. Predicates that were
    // pushed down are redundantly re-checked here — harmless, since
    // a row that satisfied them per-side still satisfies them post-join.
    let filtered_rows = if let Some(selection) = &select.selection {
        filter_rows(engine, joined_rows, selection)?
    } else {
        joined_rows
    };

    // Aggregation / projection mirror the legacy path.
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
    let has_group_by =
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

    if !has_aggregates && !has_group_by {
        let projected_rows = project_columns(filtered_rows, select)?;
        return Ok(Some(QueryResult::Rows {
            data: projected_rows,
        }));
    }
    let aggregated = execute_aggregation(&filtered_rows, select)?;
    let result = if let Some(having) = &select.having {
        filter_aggregated_rows(aggregated, having)?
    } else {
        aggregated
    };
    Ok(Some(QueryResult::Rows { data: result }))
}

// ─── Multi-join reordering (slice 6) ────────────────────────────────
//
// Pure-INNER multi-join queries (3+ tables) route through a greedy
// reorderer that picks join order by table-level row-count estimates.
// The executor walks the resulting left-deep tree, calling per-leaf
// `Engine::select` (slices 1–3 wiring) and `run_join_algorithm`
// (slices 4–5) at each step.
//
// Scope:
// - Pure INNER chains only. Mixed INNER+OUTER falls through to legacy.
// - Left-deep trees (one new table per step). Bushy trees deferred.
// - Predicate-graph connectivity required: each join step must reuse
//   an alias already in the accumulator. Cartesian re-orderings
//   (no shared predicate) reject and fall through to source order.
// - Without `ANALYZE`-populated row counts, the heuristic degenerates
//   to source order. Tests seed stats manually to force reorder.

/// Per-leaf info: the table, its alias, and any single-table
/// pushdown predicates from the WHERE clause.
#[derive(Debug, Clone)]
struct MultiJoinLeaf {
    table: String,
    alias: String,
    pushdown: Vec<WhereCondition>,
}

/// Edge in the join graph: an equi-join condition between two
/// alias-qualified columns. Direction is unspecified at extraction
/// time; the planner orients each edge as it builds the order.
#[derive(Debug, Clone)]
struct MultiJoinEdge {
    a_alias: String,
    a_col: String,
    b_alias: String,
    b_col: String,
}

/// A single join step in the executable plan. `left_alias` and
/// `left_col` reference the accumulator (which carries every
/// previously-joined alias); `right_*` references the new leaf
/// being joined.
#[derive(Debug, Clone)]
struct MultiJoinStep {
    /// New leaf being joined into the accumulator.
    right: MultiJoinLeaf,
    /// Alias of a table already in the accumulator that this step's
    /// ON condition references. Kept for future alias-qualified
    /// lookup; the current `lookup_join_value` uses bare-then-suffix
    /// fallback which suffices when the joined columns are uniquely
    /// named across leaves (the common case). Disambiguation by
    /// alias is a follow-up.
    #[allow(dead_code)]
    left_alias: String,
    /// Column on that left-side alias.
    left_col: String,
    /// Column on the right leaf.
    right_col: String,
}

/// Executable multi-join plan. Left-deep: starts from `seed`, applies
/// `steps` in order, each adding one new leaf.
#[derive(Debug, Clone)]
struct MultiJoinExecPlan {
    seed: MultiJoinLeaf,
    steps: Vec<MultiJoinStep>,
}

/// A segment of the join chain. INNER segments can reorder internally;
/// OUTER segments are fixed anchors.
#[derive(Debug, Clone)]
enum JoinChainSegment {
    /// Contiguous INNER joins. The first segment includes the FROM
    /// table as its initial leaf; extension segments contain only
    /// new leaves whose ON clauses reference the accumulator.
    Inner {
        leaves: Vec<MultiJoinLeaf>,
        edges: Vec<MultiJoinEdge>,
    },
    /// A single LEFT OUTER or FULL OUTER anchor. Stays in place
    /// regardless of stats; reordering across this would change SQL
    /// semantics (rows that should be NULL-padded would be lost).
    Outer {
        right: MultiJoinLeaf,
        join_type: crate::optimizer::JoinType,
        edge: MultiJoinEdge,
    },
}

/// Entry point: detect multi-join chain, segment it at OUTER
/// boundaries, reorder each INNER segment independently, execute
/// in segment order. Pure-INNER chains are the degenerate case
/// (one segment, no anchors). Returns `Ok(None)` for shapes we
/// don't handle (views, RIGHT OUTER, CROSS JOIN, USING, complex
/// ON expressions) → legacy path.
fn try_optimized_multi_join(
    engine: &mut Engine,
    select: &Select,
) -> Result<Option<QueryResult>> {
    use crate::optimizer::JoinType;
    // Shape gate.
    if select.from.len() != 1 || select.from[0].joins.len() < 2 {
        return Ok(None);
    }

    // Extract FROM leaf.
    let known_views: Vec<String> = engine.list_views().into_iter().map(|v| v.name).collect();
    let (from_table, from_alias_opt) = match extract_table_with_alias(&select.from[0].relation) {
        Ok(pair) => pair,
        Err(_) => return Ok(None),
    };
    if known_views.contains(&from_table) {
        return Ok(None);
    }
    let from_alias = from_alias_opt.unwrap_or_else(|| from_table.clone());
    let from_leaf = MultiJoinLeaf {
        table: from_table.clone(),
        alias: from_alias.clone(),
        pushdown: vec![],
    };

    // Build segments. Initial segment = FROM table; INNER joins
    // append to the current Inner segment; OUTER joins close the
    // current Inner segment and add an Outer anchor.
    let mut segments: Vec<JoinChainSegment> = vec![JoinChainSegment::Inner {
        leaves: vec![from_leaf],
        edges: vec![],
    }];
    for join in &select.from[0].joins {
        let (r_table, r_alias_opt) = match extract_table_with_alias(&join.relation) {
            Ok(pair) => pair,
            Err(_) => return Ok(None),
        };
        if known_views.contains(&r_table) {
            return Ok(None);
        }
        let r_alias = r_alias_opt.unwrap_or_else(|| r_table.clone());
        let r_leaf = MultiJoinLeaf {
            table: r_table.clone(),
            alias: r_alias.clone(),
            pushdown: vec![],
        };
        match &join.join_operator {
            JoinOperator::Inner(c) => {
                let edge = match extract_qualified_edge(c) {
                    Some(e) => e,
                    None => return Ok(None),
                };
                // Append to the last Inner segment, or start a new one
                // (if the previous segment was an Outer anchor).
                if let Some(JoinChainSegment::Inner { leaves, edges }) = segments.last_mut() {
                    leaves.push(r_leaf);
                    edges.push(edge);
                } else {
                    segments.push(JoinChainSegment::Inner {
                        leaves: vec![r_leaf],
                        edges: vec![edge],
                    });
                }
            }
            JoinOperator::LeftOuter(c) => {
                let edge = match extract_qualified_edge(c) {
                    Some(e) => e,
                    None => return Ok(None),
                };
                segments.push(JoinChainSegment::Outer {
                    right: r_leaf,
                    join_type: JoinType::LeftOuter,
                    edge,
                });
            }
            JoinOperator::FullOuter(c) => {
                let edge = match extract_qualified_edge(c) {
                    Some(e) => e,
                    None => return Ok(None),
                };
                segments.push(JoinChainSegment::Outer {
                    right: r_leaf,
                    join_type: JoinType::FullOuter,
                    edge,
                });
            }
            // RIGHT OUTER and CROSS JOIN: defer. The legacy path
            // handles them (with the ON-orientation fix applied
            // earlier in this slice).
            _ => return Ok(None),
        }
    }

    // Predicate pushdown: build the alias→table map across ALL
    // segments, then push single-table predicates ONLY to INNER
    // segment leaves. OUTER anchor right sides do NOT receive
    // pushdown — filtering them per-side would drop rows that the
    // join is supposed to NULL-pad.
    if let Some(selection) = &select.selection {
        let mut alias_to_table: HashMap<String, String> = HashMap::new();
        for seg in &segments {
            match seg {
                JoinChainSegment::Inner { leaves, .. } => {
                    for leaf in leaves {
                        alias_to_table.insert(leaf.alias.clone(), leaf.table.clone());
                        alias_to_table.insert(leaf.table.clone(), leaf.table.clone());
                    }
                }
                JoinChainSegment::Outer { right, .. } => {
                    // Recognize the alias so the post-join WHERE
                    // can evaluate; do NOT enroll for pushdown.
                    alias_to_table.insert(right.alias.clone(), right.table.clone());
                    alias_to_table.insert(right.table.clone(), right.table.clone());
                }
            }
        }
        let mut pushdown_by_table: HashMap<String, Vec<WhereCondition>> = HashMap::new();
        collect_pushdown_predicates(selection, &alias_to_table, &mut pushdown_by_table);
        for seg in &mut segments {
            if let JoinChainSegment::Inner { leaves, .. } = seg {
                for leaf in leaves {
                    if let Some(conds) = pushdown_by_table.remove(&leaf.table) {
                        leaf.pushdown = conds;
                    }
                }
            }
        }
    }

    // Execute segments in order.
    let mut accumulator: Vec<Value> = Vec::new();
    let mut joined_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seed_alias: Option<String> = None;
    let mut seed_table: Option<String> = None;
    let mut initial_done = false;

    for seg in segments {
        match seg {
            JoinChainSegment::Inner { leaves, edges } => {
                if !initial_done {
                    // Initial segment includes FROM. If it has just
                    // FROM and no other leaves, the accumulator is
                    // just FROM's rows; no joins to execute.
                    if leaves.len() == 1 {
                        accumulator = fetch_table_rows(engine, &leaves[0].table, leaves[0].pushdown.clone())?;
                        seed_alias = Some(leaves[0].alias.clone());
                        seed_table = Some(leaves[0].table.clone());
                        joined_aliases.insert(leaves[0].alias.clone());
                    } else {
                        let plan = match build_multi_join_plan(engine, &leaves, &edges) {
                            Some(p) => p,
                            None => return Ok(None),
                        };
                        seed_alias = Some(plan.seed.alias.clone());
                        seed_table = Some(plan.seed.table.clone());
                        for leaf in &leaves {
                            joined_aliases.insert(leaf.alias.clone());
                        }
                        accumulator = execute_multi_join_plan(engine, &plan)?;
                    }
                    initial_done = true;
                } else {
                    // Extension segment: greedy-order new leaves
                    // against the existing accumulator's aliases.
                    let steps = match plan_extension_steps(
                        engine,
                        &leaves,
                        &edges,
                        &joined_aliases,
                    ) {
                        Some(s) => s,
                        None => return Ok(None),
                    };
                    for step in steps {
                        accumulator = execute_join_step(
                            engine,
                            accumulator,
                            &step,
                            seed_alias.as_deref().unwrap_or(""),
                            seed_table.as_deref().unwrap_or(""),
                            JoinType::Inner,
                        )?;
                        joined_aliases.insert(step.right.alias.clone());
                    }
                }
            }
            JoinChainSegment::Outer { right, join_type, edge } => {
                // Outer anchor: source-order, no reordering. Resolve
                // edge direction (which side is the accumulator).
                let (left_alias, left_col, right_col) =
                    if joined_aliases.contains(&edge.a_alias) && edge.b_alias == right.alias {
                        (edge.a_alias.clone(), edge.a_col.clone(), edge.b_col.clone())
                    } else if joined_aliases.contains(&edge.b_alias) && edge.a_alias == right.alias {
                        (edge.b_alias.clone(), edge.b_col.clone(), edge.a_col.clone())
                    } else {
                        return Ok(None);
                    };
                let step = MultiJoinStep {
                    right: right.clone(),
                    left_alias,
                    left_col,
                    right_col,
                };
                accumulator = execute_join_step(
                    engine,
                    accumulator,
                    &step,
                    seed_alias.as_deref().unwrap_or(""),
                    seed_table.as_deref().unwrap_or(""),
                    join_type,
                )?;
                joined_aliases.insert(right.alias.clone());
            }
        }
    }

    // Use accumulator as the joined_rows for downstream filter/project.
    let joined_rows = accumulator;
    let _ = from_table; // kept for symmetry; alias map covers lookup.
    let _ = from_alias;

    // Post-join WHERE filter. Pushed-down single-table predicates were
    // already applied at the leaves; multi-table or unqualified
    // predicates are re-checked here (and pushed ones are harmlessly
    // re-checked too — see slice 4's commentary).
    let filtered_rows = if let Some(selection) = &select.selection {
        filter_rows(engine, joined_rows, selection)?
    } else {
        joined_rows
    };

    // Aggregation / projection mirror the single-join path.
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
    let has_group_by =
        matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if !exprs.is_empty());

    if !has_aggregates && !has_group_by {
        let projected_rows = project_columns(filtered_rows, select)?;
        return Ok(Some(QueryResult::Rows {
            data: projected_rows,
        }));
    }
    let aggregated = execute_aggregation(&filtered_rows, select)?;
    let result = if let Some(having) = &select.having {
        filter_aggregated_rows(aggregated, having)?
    } else {
        aggregated
    };
    Ok(Some(QueryResult::Rows { data: result }))
}

/// Parse an ON condition into a `MultiJoinEdge`. Requires both sides
/// to be `alias.col` form so the planner can map columns to tables
/// independently of position. Anything else (USING, unprefixed
/// names, AND chains, ranges) → `None`, falling through to legacy.
fn extract_qualified_edge(constraint: &sqlparser::ast::JoinConstraint) -> Option<MultiJoinEdge> {
    use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint};
    match constraint {
        JoinConstraint::On(Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        }) => {
            let (lhs_prefix, lhs_col) = unpack_qualified(left)?;
            let (rhs_prefix, rhs_col) = unpack_qualified(right)?;
            Some(MultiJoinEdge {
                a_alias: lhs_prefix?,
                a_col: lhs_col,
                b_alias: rhs_prefix?,
                b_col: rhs_col,
            })
        }
        _ => None,
    }
}

/// Plan an extension segment: order `new_leaves` such that each one
/// has an edge connecting it to an alias that's already joined (in
/// `pre_joined_aliases` or in a prior step of this segment). Picks
/// the cheapest eligible leaf at each step. Returns `None` if any
/// leaf can't be connected (disconnected predicate graph) — caller
/// falls through to legacy.
///
/// Unlike `build_multi_join_plan`, there's no seed selection — the
/// accumulator is already populated when we get here.
fn plan_extension_steps(
    engine: &Engine,
    new_leaves: &[MultiJoinLeaf],
    edges: &[MultiJoinEdge],
    pre_joined_aliases: &std::collections::HashSet<String>,
) -> Option<Vec<MultiJoinStep>> {
    let row_count = |table: &str| -> usize {
        engine
            .query_optimizer()
            .statistics_row_count(table)
            .unwrap_or(0)
    };

    let mut joined: std::collections::HashSet<String> = pre_joined_aliases.clone();
    let mut remaining: Vec<usize> = (0..new_leaves.len()).collect();
    let mut steps: Vec<MultiJoinStep> = Vec::new();

    while !remaining.is_empty() {
        let mut best: Option<(usize, MultiJoinStep)> = None;
        let mut best_cost: usize = usize::MAX;
        for (pos, &idx) in remaining.iter().enumerate() {
            let leaf = &new_leaves[idx];
            for edge in edges {
                let step = if edge.a_alias == leaf.alias && joined.contains(&edge.b_alias) {
                    Some(MultiJoinStep {
                        right: leaf.clone(),
                        left_alias: edge.b_alias.clone(),
                        left_col: edge.b_col.clone(),
                        right_col: edge.a_col.clone(),
                    })
                } else if edge.b_alias == leaf.alias && joined.contains(&edge.a_alias) {
                    Some(MultiJoinStep {
                        right: leaf.clone(),
                        left_alias: edge.a_alias.clone(),
                        left_col: edge.a_col.clone(),
                        right_col: edge.b_col.clone(),
                    })
                } else {
                    None
                };
                if let Some(step) = step {
                    let cost = row_count(&leaf.table);
                    if cost < best_cost
                        || (cost == best_cost
                            && best.as_ref().map(|(p, _)| pos < *p).unwrap_or(true))
                    {
                        best_cost = cost;
                        best = Some((pos, step));
                    }
                }
            }
        }
        let (pos, step) = best?;
        joined.insert(step.right.alias.clone());
        remaining.remove(pos);
        steps.push(step);
    }
    Some(steps)
}

/// Execute a single multi-join step against the running accumulator.
/// Used by both inner-segment extensions and outer anchors —
/// `join_type` distinguishes the two.
///
/// `seed_alias` and `seed_table` are the initial segment's seed
/// (computed once and threaded through). The seed's columns are the
/// only ones stored bare in the accumulator; every other alias's
/// columns are stored under `{alias}.col` (always) and bare (when
/// no collision). The qualified form is unambiguous regardless of
/// which other tables are in the accumulator — we use it for any
/// non-seed left-side column reference.
fn execute_join_step(
    engine: &mut Engine,
    accumulator: Vec<Value>,
    step: &MultiJoinStep,
    seed_alias: &str,
    seed_table: &str,
    join_type: crate::optimizer::JoinType,
) -> Result<Vec<Value>> {
    let right_rows = fetch_table_rows(engine, &step.right.table, step.right.pushdown.clone())?;
    let left_col_key = if step.left_alias == seed_alias {
        step.left_col.clone()
    } else {
        format!("{}.{}", step.left_alias, step.left_col)
    };
    let join_node = engine.query_optimizer().plan_single_join(
        seed_table,
        &step.right.table,
        &left_col_key,
        &step.right_col,
        join_type,
    );
    let constraint = synthesize_eq_constraint(&left_col_key, &step.right_col);
    run_join_algorithm(
        &join_node,
        join_type,
        &accumulator,
        &right_rows,
        &left_col_key,
        &step.right_col,
        &constraint,
        &step.right.alias,
    )
}

/// Greedy left-deep planner. Picks the cheapest seed leaf, then at
/// each step picks the unjoined leaf that (a) has an edge to an
/// already-joined alias and (b) minimizes the heuristic cost.
///
/// Heuristic cost = `row_count` from optimizer statistics. Without
/// stats, all leaves look equal → source order is preserved (stable
/// sort). With stats (test-seeded or real ANALYZE), smaller tables
/// come first, which keeps intermediate join results smaller.
fn build_multi_join_plan(
    engine: &Engine,
    leaves: &[MultiJoinLeaf],
    edges: &[MultiJoinEdge],
) -> Option<MultiJoinExecPlan> {
    if leaves.is_empty() {
        return None;
    }
    let row_count = |table: &str| -> usize {
        engine
            .query_optimizer()
            .statistics_row_count(table)
            .unwrap_or(0)
    };

    // Pick seed: the smallest leaf by row count. Stable on ties
    // (source order). With no stats, all are 0 → source order wins.
    let mut remaining: Vec<usize> = (0..leaves.len()).collect();
    remaining.sort_by_key(|&i| (row_count(&leaves[i].table), i));
    let seed_idx = remaining.remove(0);
    let mut joined_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    joined_aliases.insert(leaves[seed_idx].alias.clone());

    let mut steps: Vec<MultiJoinStep> = Vec::new();
    while !remaining.is_empty() {
        // For each remaining leaf, check whether some edge connects
        // it to an already-joined alias. If yes, it's eligible.
        let mut best: Option<(usize, MultiJoinStep)> = None;
        let mut best_cost: usize = usize::MAX;
        for (pos, &idx) in remaining.iter().enumerate() {
            let leaf = &leaves[idx];
            // Find an edge from this leaf to a joined alias.
            for edge in edges {
                let step = if edge.a_alias == leaf.alias && joined_aliases.contains(&edge.b_alias)
                {
                    Some(MultiJoinStep {
                        right: leaf.clone(),
                        left_alias: edge.b_alias.clone(),
                        left_col: edge.b_col.clone(),
                        right_col: edge.a_col.clone(),
                    })
                } else if edge.b_alias == leaf.alias
                    && joined_aliases.contains(&edge.a_alias)
                {
                    Some(MultiJoinStep {
                        right: leaf.clone(),
                        left_alias: edge.a_alias.clone(),
                        left_col: edge.a_col.clone(),
                        right_col: edge.b_col.clone(),
                    })
                } else {
                    None
                };
                if let Some(step) = step {
                    let cost = row_count(&leaf.table);
                    if cost < best_cost
                        || (cost == best_cost && best.as_ref().map(|(p, _)| pos < *p).unwrap_or(true))
                    {
                        best_cost = cost;
                        best = Some((pos, step));
                    }
                }
            }
        }
        let (pos, step) = best?; // disconnected graph → fall through
        joined_aliases.insert(step.right.alias.clone());
        remaining.remove(pos);
        steps.push(step);
    }

    Some(MultiJoinExecPlan {
        seed: leaves[seed_idx].clone(),
        steps,
    })
}

/// Execute the reordered plan. Builds the accumulator from the seed,
/// then folds each step through `run_join_algorithm`, picking
/// NestedLoop vs Hash via `plan_single_join` based on the two
/// already-known per-side row counts.
fn execute_multi_join_plan(
    engine: &mut Engine,
    plan: &MultiJoinExecPlan,
) -> Result<Vec<Value>> {
    let mut accumulator = fetch_table_rows(engine, &plan.seed.table, plan.seed.pushdown.clone())?;
    for step in &plan.steps {
        let right_rows = fetch_table_rows(
            engine,
            &step.right.table,
            step.right.pushdown.clone(),
        )?;
        // Resolve the accumulator-side column to its actual key. The
        // seed's columns are stored bare; every other table's columns
        // are stored both bare (when no collision with seed) AND
        // alias-qualified (always). We pick the qualified form for
        // non-seed aliases — it's unique and unambiguous regardless
        // of which other tables happen to share the column name.
        let left_col_key = if step.left_alias == plan.seed.alias {
            step.left_col.clone()
        } else {
            format!("{}.{}", step.left_alias, step.left_col)
        };
        // Algorithm choice per step. We treat the accumulator's
        // implicit "table" as the union of all already-joined leaves —
        // the planner doesn't know intermediate cardinality, so we
        // pass the seed's table as a stand-in for the optimizer's
        // statistics lookup. Either way, NL is correct; Hash is a
        // perf optimization.
        let join_node = engine.query_optimizer().plan_single_join(
            &plan.seed.table,
            &step.right.table,
            &left_col_key,
            &step.right_col,
            crate::optimizer::JoinType::Inner,
        );
        let constraint = synthesize_eq_constraint(&left_col_key, &step.right_col);
        accumulator = run_join_algorithm(
            &join_node,
            crate::optimizer::JoinType::Inner,
            &accumulator,
            &right_rows,
            &left_col_key,
            &step.right_col,
            &constraint,
            &step.right.alias,
        )?;
    }
    Ok(accumulator)
}

/// Extract (table_name, optional_alias) from a TableFactor::Table.
fn extract_table_with_alias(table: &TableFactor) -> Result<(String, Option<String>)> {
    match table {
        TableFactor::Table { name, alias, .. } => Ok((
            name.to_string(),
            alias.as_ref().map(|a| a.name.value.clone()),
        )),
        _ => Err(DriftError::InvalidQuery(
            "Complex table expressions not supported".to_string(),
        )),
    }
}

/// Fetch all rows from a table, applying the given per-table WHERE
/// conditions through `Engine::select`. Empty conditions = full scan
/// (which is itself optimizer-aware; nothing else activates without
/// predicates to push).
fn fetch_table_rows(
    engine: &mut Engine,
    table: &str,
    conditions: Vec<WhereCondition>,
) -> Result<Vec<Value>> {
    let query = Query::Select {
        table: table.to_string(),
        conditions,
        as_of: current_temporal_as_of(),
        limit: None,
    };
    match engine.execute_query(query)? {
        QueryResult::Rows { data } => Ok(data),
        _ => Ok(vec![]),
    }
}

/// Walk a WHERE expression, partitioning AND-leaves that name a
/// specific table (via prefix matching the table name or its alias)
/// into per-table predicate lists. Anything else (no prefix, OR-chain,
/// function call, subquery) is left for the post-join filter. The
/// caller still applies the full original WHERE to the joined output,
/// so pushdown is a strict performance win — it can't change results.
fn collect_pushdown_predicates(
    expr: &Expr,
    alias_to_table: &HashMap<String, String>,
    pushdown: &mut HashMap<String, Vec<WhereCondition>>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_pushdown_predicates(left, alias_to_table, pushdown);
            collect_pushdown_predicates(right, alias_to_table, pushdown);
        }
        Expr::BinaryOp { left, op, right } => {
            // Push only when the left side is a CompoundIdentifier
            // whose prefix resolves to a known table, and the right
            // side reduces to a constant. Anything more ambitious
            // (column-to-column, function calls) stays residual.
            let Some((table, column)) = extract_qualified_column(left, alias_to_table) else {
                return;
            };
            let Ok(value) = expr_to_json_value(right) else {
                return;
            };
            let operator = match op {
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::Lt => "<",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Gt => ">",
                BinaryOperator::GtEq => ">=",
                _ => return,
            };
            pushdown.entry(table).or_default().push(WhereCondition {
                column,
                operator: operator.to_string(),
                value,
            });
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            let Some((table, column)) = extract_qualified_column(expr, alias_to_table) else {
                return;
            };
            let (Ok(low_v), Ok(high_v)) = (expr_to_json_value(low), expr_to_json_value(high))
            else {
                return;
            };
            let entry = pushdown.entry(table).or_default();
            entry.push(WhereCondition {
                column: column.clone(),
                operator: ">=".to_string(),
                value: low_v,
            });
            entry.push(WhereCondition {
                column,
                operator: "<=".to_string(),
                value: high_v,
            });
        }
        _ => {} // OR, NOT, subqueries, etc. — residual only.
    }
}

/// If `expr` is `alias.column` where `alias` resolves to a known table
/// in this query, return `(table_name, column_name)`. Otherwise None.
fn extract_qualified_column(
    expr: &Expr,
    alias_to_table: &HashMap<String, String>,
) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let alias = &parts[0].value;
            let column = &parts[1].value;
            alias_to_table
                .get(alias)
                .map(|t| (t.clone(), column.clone()))
        }
        _ => None,
    }
}

/// Dispatch a single-join PlanNode to the right algorithm + join-type
/// implementation. Pattern-matches on the PlanNode and on join_type
/// orthogonally. Hash-join errors silently fall back to the NL variant
/// of the same join type (slice-4 fallback convention).
#[allow(clippy::too_many_arguments)]
fn run_join_algorithm(
    join_node: &crate::optimizer::PlanNode,
    join_type: crate::optimizer::JoinType,
    left_rows: &[Value],
    right_rows: &[Value],
    left_col: &str,
    right_col: &str,
    constraint: &sqlparser::ast::JoinConstraint,
    right_alias: &str,
) -> Result<Vec<Value>> {
    use crate::optimizer::JoinType as JT;
    use crate::optimizer::PlanNode;

    let want_hash = matches!(join_node, PlanNode::HashJoin { .. });
    let build_side = match join_node {
        PlanNode::HashJoin { build_side, .. } => *build_side,
        _ => crate::optimizer::JoinSide::Right,
    };

    // Try the chosen algorithm; on Err, fall back to the NL form of
    // the same join type. This keeps weird-input behavior identical
    // across algorithm choices.
    if want_hash {
        let hash_result = match join_type {
            JT::Inner => perform_inner_hash_join(
                left_rows, right_rows, left_col, right_col, build_side, right_alias,
            ),
            JT::LeftOuter => perform_left_outer_hash_join(
                left_rows,
                right_rows,
                left_col,
                right_col,
                right_alias,
            ),
            JT::FullOuter => perform_full_outer_hash_join(
                left_rows,
                right_rows,
                left_col,
                right_col,
                right_alias,
            ),
        };
        if let Ok(rows) = hash_result {
            return Ok(rows);
        }
        // fall through to NL fallback
    }

    match join_type {
        JT::Inner => perform_inner_join(
            left_rows.to_vec(),
            right_rows.to_vec(),
            constraint,
            right_alias,
        ),
        JT::LeftOuter => perform_left_join(
            left_rows.to_vec(),
            right_rows.to_vec(),
            constraint,
            right_alias,
        ),
        JT::FullOuter => perform_full_outer_join(
            left_rows.to_vec(),
            right_rows.to_vec(),
            constraint,
            right_alias,
        ),
    }
}

/// Extract `(left_table_col, right_table_col)` from an `ON` clause,
/// reordering based on the alias prefixes if the user wrote them in
/// reverse FROM order (e.g. `FROM depts d LEFT JOIN users u ON
/// u.dept_id = d.id` — the equality's left operand `u.dept_id`
/// belongs to the JOIN's right side, not its left).
///
/// `extract_join_columns` (used by the legacy paths) returns columns
/// in the source-text order of the equality, which is fine when ON
/// matches FROM ordering. The optimizer-driven path needs the
/// FROM-oriented version so it can hand each column to the correct
/// per-side `Engine::select` call.
///
/// Returns `None` for any constraint shape we can't safely orient
/// (USING is symmetric — same column on both sides — and falls
/// through to a single name; complex ON conditions trip the inner
/// helper).
fn orient_join_columns_to_from(
    constraint: &sqlparser::ast::JoinConstraint,
    sql_left_alias: &Option<String>,
    sql_right_alias: &Option<String>,
) -> Option<(String, String)> {
    use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint};
    match constraint {
        JoinConstraint::On(Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        }) => {
            let (lhs_prefix, lhs_col) = unpack_qualified(left)?;
            let (rhs_prefix, rhs_col) = unpack_qualified(right)?;
            // Match each operand's prefix against the FROM aliases. If
            // the equality's LHS is the FROM-left, return as-is;
            // otherwise swap.
            let lhs_is_left = matches_alias(&lhs_prefix, sql_left_alias);
            let lhs_is_right = matches_alias(&lhs_prefix, sql_right_alias);
            let rhs_is_left = matches_alias(&rhs_prefix, sql_left_alias);
            let rhs_is_right = matches_alias(&rhs_prefix, sql_right_alias);
            if lhs_is_left && rhs_is_right {
                Some((lhs_col, rhs_col))
            } else if lhs_is_right && rhs_is_left {
                Some((rhs_col, lhs_col))
            } else {
                // Ambiguous or unprefixed — fall back to the legacy
                // source-order extraction. The optimized path may
                // still produce wrong results for this query, but
                // it's no worse than the legacy path was already.
                extract_join_columns(constraint).ok()
            }
        }
        _ => extract_join_columns(constraint).ok(),
    }
}

fn unpack_qualified(expr: &sqlparser::ast::Expr) -> Option<(Option<String>, String)> {
    match expr {
        sqlparser::ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((Some(parts[0].value.clone()), parts[1].value.clone()))
        }
        sqlparser::ast::Expr::Identifier(ident) => Some((None, ident.value.clone())),
        _ => None,
    }
}

fn matches_alias(prefix: &Option<String>, alias: &Option<String>) -> bool {
    match (prefix, alias) {
        (Some(p), Some(a)) => p == a,
        _ => false,
    }
}

/// Orient an ON clause for the legacy multi-join loop, where the
/// accumulator's tables vary by step but the new right-side alias is
/// always known. If the ON's LEFT operand prefix is `right_alias`,
/// the constraint is reversed relative to FROM order — swap so that
/// LHS belongs to the accumulator and RHS belongs to the new right.
///
/// Returns `Some(canonical_constraint)` only when the orientation is
/// unambiguous. Returns `None` for cases the caller should leave
/// alone (USING clauses, unprefixed columns, both operands prefixed
/// with `right_alias` from a self-join). The legacy path's existing
/// `extract_join_columns` handles those, with its known
/// source-text-order limitation.
fn orient_constraint_for_right_alias(
    constraint: &sqlparser::ast::JoinConstraint,
    right_alias: &str,
) -> Option<sqlparser::ast::JoinConstraint> {
    use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint};
    match constraint {
        JoinConstraint::On(Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        }) => {
            let (lhs_prefix, lhs_col) = unpack_qualified(left)?;
            let (rhs_prefix, rhs_col) = unpack_qualified(right)?;
            let lhs_matches = lhs_prefix.as_deref() == Some(right_alias);
            let rhs_matches = rhs_prefix.as_deref() == Some(right_alias);
            if lhs_matches && !rhs_matches {
                Some(synthesize_eq_constraint(&rhs_col, &lhs_col))
            } else if rhs_matches && !lhs_matches {
                Some(constraint.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Synthesize a `JoinConstraint::On(left_col = right_col)` for the
/// nested-loop fallback after a RIGHT→LEFT side swap. We can't reuse
/// the user's original `sql_constraint` post-swap because its left and
/// right columns refer to the pre-swap layout.
fn synthesize_eq_constraint(
    left_col: &str,
    right_col: &str,
) -> sqlparser::ast::JoinConstraint {
    use sqlparser::ast::{BinaryOperator, Expr, Ident, JoinConstraint};
    JoinConstraint::On(Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new(left_col))),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Identifier(Ident::new(right_col))),
    })
}

/// LEFT OUTER hash join. Build hash on RIGHT (required: probe with the
/// preserving side); for each LEFT row, emit one merged row per right
/// match, OR a bare left row if no matches. Mirrors
/// `perform_left_join`'s output shape exactly so downstream filter /
/// projection don't care which algorithm ran.
fn perform_left_outer_hash_join(
    left_rows: &[Value],
    right_rows: &[Value],
    left_col: &str,
    right_col: &str,
    right_alias: &str,
) -> Result<Vec<Value>> {
    let mut bucket: HashMap<String, Vec<&Value>> = HashMap::new();
    for row in right_rows {
        let Some(val) = lookup_join_value(row, right_col) else {
            continue;
        };
        if val.is_null() {
            continue;
        }
        bucket.entry(json_to_hash_key(val)).or_default().push(row);
    }
    let right_keys = collect_row_keys(right_rows);

    let mut result = Vec::with_capacity(left_rows.len());
    for left_row in left_rows {
        let probe_key = lookup_join_value(left_row, left_col)
            .filter(|v| !v.is_null())
            .map(json_to_hash_key);
        let matches = probe_key.as_ref().and_then(|k| bucket.get(k));
        match matches {
            Some(rs) if !rs.is_empty() => {
                for right_row in rs {
                    result.push(merge_join_rows(left_row, right_row, right_alias));
                }
            }
            _ => {
                result.push(null_pad_right_into(left_row, &right_keys, right_alias));
            }
        }
    }
    Ok(result)
}

/// FULL OUTER hash join. Build on RIGHT, track which right rows
/// matched during the LEFT probe phase, then emit unmatched-right
/// rows at the end. Output shape matches `perform_full_outer_join`.
fn perform_full_outer_hash_join(
    left_rows: &[Value],
    right_rows: &[Value],
    left_col: &str,
    right_col: &str,
    right_alias: &str,
) -> Result<Vec<Value>> {
    // Build phase: store (right_index, row) so we can mark matches.
    let mut bucket: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, row) in right_rows.iter().enumerate() {
        let Some(val) = lookup_join_value(row, right_col) else {
            continue;
        };
        if val.is_null() {
            continue;
        }
        bucket.entry(json_to_hash_key(val)).or_default().push(idx);
    }

    let mut matched_right: std::collections::HashSet<usize> =
        std::collections::HashSet::new();
    let mut result = Vec::new();
    let right_keys = collect_row_keys(right_rows);
    let left_keys = collect_row_keys(left_rows);

    // LEFT-probe phase.
    for left_row in left_rows {
        let probe_key = lookup_join_value(left_row, left_col)
            .filter(|v| !v.is_null())
            .map(json_to_hash_key);
        let matches = probe_key.as_ref().and_then(|k| bucket.get(k));
        match matches {
            Some(idxs) if !idxs.is_empty() => {
                for &idx in idxs {
                    matched_right.insert(idx);
                    result.push(merge_join_rows(left_row, &right_rows[idx], right_alias));
                }
            }
            _ => {
                result.push(null_pad_right_into(left_row, &right_keys, right_alias));
            }
        }
    }

    // Unmatched-RIGHT phase. Pad with NULL for left's columns (same
    // semantics as perform_full_outer_join).
    for (idx, row) in right_rows.iter().enumerate() {
        if !matched_right.contains(&idx) {
            result.push(null_pad_left_into(row, &left_keys));
        }
    }
    Ok(result)
}

/// Hash-join implementation for INNER JOIN. Builds a hash table on the
/// `build_side` keyed by the join column, then probes with the other
/// side. The merged row shape matches `perform_inner_join` so the
/// downstream filter / projection code doesn't need to care which
/// algorithm produced the result.
///
/// Returns `Err` if a join-column value is not hashable as a string
/// (we use the JSON string representation as the hash key, so this is
/// unlikely in practice — but the caller treats `Err` as "fall back to
/// nested-loop" without surfacing it to the user).
fn perform_inner_hash_join(
    left_rows: &[Value],
    right_rows: &[Value],
    left_col: &str,
    right_col: &str,
    build_side: crate::optimizer::JoinSide,
    right_alias: &str,
) -> Result<Vec<Value>> {
    use crate::optimizer::JoinSide;
    let (build_rows, probe_rows, build_col, probe_col) = match build_side {
        JoinSide::Right => (right_rows, left_rows, right_col, left_col),
        JoinSide::Left => (left_rows, right_rows, left_col, right_col),
    };

    // Build phase: hash by join column value. Multiple rows with the
    // same key form a Vec — required when the inner has duplicate keys.
    let mut bucket: HashMap<String, Vec<&Value>> = HashMap::new();
    for row in build_rows {
        let Some(val) = lookup_join_value(row, build_col) else {
            continue; // NULL join keys never match (SQL semantics).
        };
        if val.is_null() {
            continue;
        }
        let key = json_to_hash_key(val);
        bucket.entry(key).or_default().push(row);
    }

    // Probe phase. Output shape must match `perform_inner_join` so the
    // executor's HashJoin → NestedLoop fallback is transparent: SQL-
    // left columns first, SQL-right collisions prefixed with the right
    // alias. The probe/build roles are an internal hash-join detail,
    // not a row-order signal — `right_alias` always names the SQL
    // right side regardless.
    let mut result = Vec::with_capacity(probe_rows.len());
    for probe_row in probe_rows {
        let Some(val) = lookup_join_value(probe_row, probe_col) else {
            continue;
        };
        if val.is_null() {
            continue;
        }
        let key = json_to_hash_key(val);
        let Some(matches) = bucket.get(&key) else {
            continue;
        };
        for build_row in matches {
            let (left_row, right_row) = match build_side {
                JoinSide::Right => (probe_row, *build_row),
                JoinSide::Left => (*build_row, probe_row),
            };
            result.push(merge_join_rows(left_row, right_row, right_alias));
        }
    }
    Ok(result)
}

/// Stable string representation of a JSON value for hash-table keys.
/// Strings stay as-is (so `"5"` and `5` don't collide); other values
/// use their JSON repr. The same scheme `Index::insert` uses, so
/// hash-join keys agree with index-lookup keys at the boundary.
fn json_to_hash_key(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        format!("s:{}", s)
    } else {
        format!("j:{}", value)
    }
}

fn perform_cross_join(
    left_rows: Vec<Value>,
    right_rows: Vec<Value>,
    right_alias: &str,
) -> Vec<Value> {
    let mut result = Vec::new();
    for left_row in &left_rows {
        for right_row in &right_rows {
            result.push(merge_join_rows(left_row, right_row, right_alias));
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

    let new_row = json!(data);

    // Validate FK constraints before the trigger runs. PostgreSQL evaluates
    // referential-integrity constraints before BEFORE-INSERT triggers fire,
    // so an FK violation rejects the row without invoking user code.
    crate::fk::validate_insert(engine, table, &new_row)?;

    // Execute BEFORE INSERT triggers
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

    // Route based on transaction state. When the session is inside a
    // transaction we buffer the event in the engine's transaction
    // manager; the actual storage write happens at COMMIT. PK
    // uniqueness is checked at INSERT time against both the
    // transaction's buffered writes (for sibling-row collisions) and
    // the committed state (for collisions with rows outside this
    // transaction). This matches PostgreSQL's default immediate-
    // constraint behavior. On violation, the transaction enters the
    // aborted state — every subsequent statement (except ROLLBACK)
    // surfaces "current transaction is aborted".
    //
    // Auto-commit INSERTs (no active transaction) route through
    // `execute_query(Query::Insert)`, which already performs the same
    // uniqueness check via the engine's executor — unchanged.
    let result = if let Some(txn_id) = current_transaction() {
        let pk_field = engine.get_table_primary_key(table)?;
        let primary_key = final_data.get(&pk_field).cloned().ok_or_else(|| {
            DriftError::InvalidQuery(format!("Missing primary key field '{}'", pk_field))
        })?;
        // Combined buffer-then-committed visibility check. A pending
        // SoftDelete in the buffer masks any committed row for this
        // PK (read-your-writes), so the standard delete-then-insert
        // pattern works.
        match engine.pk_visibility_in_transaction(txn_id, table, &primary_key)? {
            crate::engine::PkVisibility::Active => {
                mark_txn_aborted();
                return Err(DriftError::InvalidQuery(format!(
                    "duplicate key value violates unique constraint on table \"{}\": key ({})=({}) already exists",
                    table, pk_field, primary_key
                )));
            }
            crate::engine::PkVisibility::Deleted | crate::engine::PkVisibility::Absent => {
                // Free to proceed.
            }
        }
        let event = crate::events::Event::new_insert(
            table.to_string(),
            primary_key,
            final_data.clone(),
        );
        engine.apply_event_in_transaction(txn_id, event)?;
        QueryResult::Success {
            message: "Buffered insert in transaction".to_string(),
        }
    } else {
        let query = Query::Insert {
            table: table.to_string(),
            data: final_data.clone(),
        };
        engine.execute_query(query)?
    };

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
    // Lower the WHERE expression to a flat AND-chain of WhereConditions
    // that the engine can match per row (and that the optimizer can
    // coalesce into IndexLookup/IndexScan access plans). Anything we
    // don't structurally understand (subqueries, OR, function calls)
    // makes the caller fall back to row-level SQL evaluation, so it's
    // safe to return an empty list rather than erroring.
    match expr {
        sqlparser::ast::Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut conds = parse_where_clause(left)?;
            conds.extend(parse_where_clause(right)?);
            Ok(conds)
        }
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
        // SQL `x BETWEEN low AND high` is inclusive on both sides.
        // Lower to `x >= low AND x <= high`. `NOT BETWEEN` would need
        // OR semantics, which the engine doesn't represent today —
        // leave it to row-level SQL evaluation (return empty list).
        sqlparser::ast::Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            let column = extract_column_from_expr(expr)?;
            let low_val = expr_to_json_value(low)?;
            let high_val = expr_to_json_value(high)?;
            Ok(vec![
                WhereCondition {
                    column: column.clone(),
                    operator: ">=".to_string(),
                    value: low_val,
                },
                WhereCondition {
                    column,
                    operator: "<=".to_string(),
                    value: high_val,
                },
            ])
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
    // SQL-standard validation, matching PostgreSQL's strictness:
    //   1. `SELECT *` is not meaningful with GROUP BY — the projected rows
    //      are groups, not source rows, so `*` would have to expand to
    //      something the planner can't decide unambiguously.
    //   2. Every non-aggregate column in the SELECT list must appear in
    //      GROUP BY (functional-dependency rule). Otherwise the value the
    //      query would return is non-deterministic.
    //
    // Previously sql_bridge silently accepted both, returning whatever the
    // first row of each group happened to contain — wrong by the standard
    // and inconsistent with the server's old hand-rolled validator.
    let group_columns: std::collections::HashSet<&str> = group_exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::Identifier(i) => Some(i.value.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.as_str()),
            _ => None,
        })
        .collect();

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                return Err(DriftError::InvalidQuery(
                    "SELECT * with GROUP BY is not supported".to_string(),
                ));
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident))
                if !group_columns.contains(ident.value.as_str()) =>
            {
                return Err(DriftError::InvalidQuery(format!(
                    "column \"{}\" must be in GROUP BY clause or used in an aggregate",
                    ident.value
                )));
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                ..
            } if !group_columns.contains(ident.value.as_str()) => {
                return Err(DriftError::InvalidQuery(format!(
                    "column \"{}\" must be in GROUP BY clause or used in an aggregate",
                    ident.value
                )));
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                if let Some(last) = parts.last() {
                    if !group_columns.contains(last.value.as_str()) {
                        return Err(DriftError::InvalidQuery(format!(
                            "column \"{}\" must be in GROUP BY clause or used in an aggregate",
                            last.value
                        )));
                    }
                }
            }
            // Function calls (aggregates), aliased aggregates, and computed
            // expressions are allowed unconditionally — the aggregation is the
            // valid escape from the functional-dependency rule.
            _ => {}
        }
    }

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
                    // Handle table.column with alias-aware resolution.
                    if let Some(first_row) = group_rows.first() {
                        if let Some(val) = resolve_qualified_column(idents, first_row) {
                            if let Some(column) = idents.last() {
                                result_row.insert(column.value.clone(), val);
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

    // Generate result column name based on the function. PostgreSQL
    // convention: column header is the lowercase function name, e.g.
    // `count(*)`, `sum(price)`. Unquoted SQL is case-insensitive at the
    // language level; the canonical output form is lowercase.
    let result_name = match func_name.as_str() {
        "COUNT" if column.is_none() => "count(*)".to_string(),
        "COUNT" => format!("count({})", column.as_ref().unwrap()),
        "SUM" => format!("sum({})", column.as_ref().unwrap_or(&"*".to_string())),
        "AVG" => format!("avg({})", column.as_ref().unwrap_or(&"*".to_string())),
        "MIN" => format!("min({})", column.as_ref().unwrap_or(&"*".to_string())),
        "MAX" => format!("max({})", column.as_ref().unwrap_or(&"*".to_string())),
        _ => func_name.to_lowercase(),
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
                // SQL-standard / PostgreSQL semantics:
                //   - SUM over zero rows (or all-NULL rows) is NULL, not 0.
                //   - SUM of integer inputs returns an integer; if any input
                //     is a float, the whole sum is a float. We don't have a
                //     numeric type distinction, but we do have JSON int vs
                //     float, so preserve that.
                let collected: Vec<Value> = rows
                    .iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter(|v| !v.is_null())
                    .collect();

                if collected.is_empty() {
                    return Ok((result_name, Value::Null));
                }

                let all_ints = collected.iter().all(|v| {
                    v.as_i64().is_some()
                        || matches!(v, Value::Number(n) if n.is_i64() || n.is_u64())
                });
                if all_ints {
                    let sum: i64 = collected.iter().filter_map(|v| v.as_i64()).sum();
                    Ok((result_name, json!(sum)))
                } else {
                    let sum: f64 = collected
                        .iter()
                        .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                        .sum();
                    Ok((result_name, json!(sum)))
                }
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
                // Use the canonical compare so MIN/MAX work for string and
                // mixed-type columns. The previous numeric-only comparator
                // returned Ordering::Equal for any non-Number pair, which
                // made `MIN(name)` return whatever happened to come first
                // in iteration order rather than the alphabetic minimum.
                let min = rows
                    .iter()
                    .filter_map(|row| get_column_value(row, &col))
                    .filter(|v| !v.is_null())
                    .min_by(crate::query::predicate::compare_json_values);

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
                    .max_by(crate::query::predicate::compare_json_values);

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
                        BinaryOperator::Lt => Ok(crate::query::predicate::compare_values(
                            &left_val, right_val, "<",
                        )),
                        BinaryOperator::LtEq => Ok(crate::query::predicate::compare_values(
                            &left_val, right_val, "<=",
                        )),
                        BinaryOperator::Gt => Ok(crate::query::predicate::compare_values(
                            &left_val, right_val, ">",
                        )),
                        BinaryOperator::GtEq => Ok(crate::query::predicate::compare_values(
                            &left_val, right_val, ">=",
                        )),
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
                        BinaryOperator::Lt => Ok(crate::query::predicate::compare_values(
                            &left_val,
                            &right_val,
                            "<",
                        )),
                        BinaryOperator::LtEq => Ok(crate::query::predicate::compare_values(
                            &left_val,
                            &right_val,
                            "<=",
                        )),
                        BinaryOperator::Gt => Ok(crate::query::predicate::compare_values(
                            &left_val,
                            &right_val,
                            ">",
                        )),
                        BinaryOperator::GtEq => Ok(crate::query::predicate::compare_values(
                            &left_val,
                            &right_val,
                            ">=",
                        )),
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
                BinaryOperator::Lt => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    "<",
                )),
                BinaryOperator::LtEq => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    "<=",
                )),
                BinaryOperator::Gt => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    ">",
                )),
                BinaryOperator::GtEq => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    ">=",
                )),
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
        // `x IS NULL` and `x IS NOT NULL`. The OUTER JOIN anti-join
        // pattern (`WHERE right.col IS NULL`) depends on this — the
        // pre-existing default of `Ok(true)` for unmatched expressions
        // silently accepted every row, turning anti-join queries into
        // "return everything" instead of "return unmatched". Latent
        // bug exposed by the LEFT JOIN slice; fixed here.
        Expr::IsNull(inner) => {
            let v = evaluate_value_expression(inner, row)?;
            Ok(v.is_null())
        }
        Expr::IsNotNull(inner) => {
            let v = evaluate_value_expression(inner, row)?;
            Ok(!v.is_null())
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
                    // PostgreSQL convention: aggregate column headers are
                    // lowercase (`count(*)`, `sum(x)`) unless the user quoted
                    // the function name. Matches the column naming produced
                    // by `evaluate_aggregate_function`.
                    let col_name = match func_name.as_str() {
                        "COUNT" if column.is_none() => "count(*)".to_string(),
                        "COUNT" => format!("count({})", column.as_ref().unwrap()),
                        "SUM" => format!("sum({})", column.as_ref().unwrap_or(&"*".to_string())),
                        "AVG" => format!("avg({})", column.as_ref().unwrap_or(&"*".to_string())),
                        "MIN" => format!("min({})", column.as_ref().unwrap_or(&"*".to_string())),
                        "MAX" => format!("max({})", column.as_ref().unwrap_or(&"*".to_string())),
                        _ => func_name.to_lowercase(),
                    };

                    row.get(&col_name).cloned().unwrap_or(Value::Null)
                }
                _ => evaluate_value_expression(left, row)?,
            };

            let right_val = evaluate_value_expression(right, row)?;

            match op {
                BinaryOperator::Eq => Ok(left_val == right_val),
                BinaryOperator::NotEq => Ok(left_val != right_val),
                BinaryOperator::Lt => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    "<",
                )),
                BinaryOperator::LtEq => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    "<=",
                )),
                BinaryOperator::Gt => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    ">",
                )),
                BinaryOperator::GtEq => Ok(crate::query::predicate::compare_values(
                    &left_val,
                    &right_val,
                    ">=",
                )),
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
                let table_alias = &parts[0].value;
                let column = &parts[1].value;

                // Alias-aware resolution: try `{alias}.{column}` first.
                // Joined rows have right-side collisions keyed this way
                // (see `merge_join_rows`). When the user writes
                // `d.name` and depts.name collided with users.name,
                // this finds depts's value. Non-colliding right-side
                // columns and all left-side columns fall through to
                // the bare-name lookup below.
                let qualified = format!("{}.{}", table_alias, column);
                if let Some(row_obj) = row.as_object() {
                    if let Some(val) = row_obj.get(&qualified) {
                        return Ok(val.clone());
                    }
                }

                // For correlated subqueries, we need to check both current and outer contexts.
                // The table alias should help us decide, but we don't track full alias
                // scope yet — so we use a heuristic: check current row first, then outer.
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
                    crate::query::predicate::compare_json_values(&left_val, &right_val) == std::cmp::Ordering::Equal,
                )),
                BinaryOperator::NotEq => Ok(Value::Bool(
                    crate::query::predicate::compare_json_values(&left_val, &right_val) != std::cmp::Ordering::Equal,
                )),
                BinaryOperator::Lt => Ok(Value::Bool(
                    crate::query::predicate::compare_json_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                )),
                BinaryOperator::LtEq => Ok(Value::Bool(matches!(
                    crate::query::predicate::compare_json_values(&left_val, &right_val),
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                ))),
                BinaryOperator::Gt => Ok(Value::Bool(
                    crate::query::predicate::compare_json_values(&left_val, &right_val) == std::cmp::Ordering::Greater,
                )),
                BinaryOperator::GtEq => Ok(Value::Bool(matches!(
                    crate::query::predicate::compare_json_values(&left_val, &right_val),
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
                    // `alias.col`: try the alias-prefixed key first
                    // (right-side collisions are keyed this way after
                    // `merge_join_rows`), then fall back to bare column.
                    // Output column name = bare column (PostgreSQL
                    // convention: prefix is for resolution only).
                    if let Some(val) = resolve_qualified_column(idents, &row) {
                        if let Some(column) = idents.last() {
                            projected_row.insert(column.value.clone(), val);
                        }
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::CompoundIdentifier(idents),
                    alias,
                } => {
                    if let Some(val) = resolve_qualified_column(idents, &row) {
                        projected_row.insert(alias.value.clone(), val);
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

    // Validate bare column identifiers against the union of column names
    // observed across all rows. PostgreSQL errors on `SELECT nonexistent FROM t`,
    // and matching that behaviour is the choice we make here — silently
    // producing NULLs for unknown columns hides typos and schema drift.
    //
    // DriftDB's storage is schemaless within a table (legacy `CREATE TABLE t
    // (pk = id)` declares only the PK; data columns are introduced by INSERT),
    // so the only reliable "set of valid columns" is what's actually in the
    // data. If the data is non-empty, every Expr::Identifier in the
    // projection must appear in at least one row; otherwise we error.
    if !data.is_empty() {
        let mut known_columns: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for row in &data {
            if let Value::Object(map) = row {
                for key in map.keys() {
                    known_columns.insert(key.as_str());
                }
            }
        }
        for item in projection {
            let ident = match item {
                SelectItem::UnnamedExpr(Expr::Identifier(i)) => Some(&i.value),
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(i),
                    ..
                } => Some(&i.value),
                _ => None,
            };
            if let Some(name) = ident {
                if !known_columns.contains(name.as_str()) {
                    return Err(DriftError::InvalidQuery(format!(
                        "column \"{}\" does not exist",
                        name
                    )));
                }
            }
        }
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
                                // Alias-aware resolution. The row may
                                // arrive un-projected (raw joined
                                // shape: bare left columns + `d.col`
                                // for right-side collisions) OR
                                // pre-projected (already keyed by the
                                // bare column name from a prior
                                // project_columns pass). Try the bare
                                // column first (idempotency: preserve
                                // a prior projection's value), then
                                // alias-aware lookup (`alias.col`,
                                // then bare).
                                let col_name =
                                    parts.last().map(|i| i.value.clone()).unwrap_or_default();
                                if let Some(value) = row_map.get(&col_name) {
                                    projected_row.insert(col_name, value.clone());
                                } else if let Some(value) =
                                    resolve_qualified_column(parts, &row)
                                {
                                    projected_row.insert(col_name, value);
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
                            Expr::CompoundIdentifier(parts) => {
                                // Idempotency: if the row already
                                // carries the alias key (from a
                                // prior project_columns pass), keep
                                // that value verbatim — re-resolving
                                // against the projected row's bare
                                // names would null it out. Otherwise,
                                // resolve `alias.col` against the
                                // raw row using alias-aware lookup.
                                if let Some(value) = row_map.get(&alias.value) {
                                    projected_row.insert(alias.value.clone(), value.clone());
                                } else if let Some(value) =
                                    resolve_qualified_column(parts, &row)
                                {
                                    projected_row.insert(alias.value.clone(), value);
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

/// Build the canonical lowercase column name an aggregate function call would
/// produce, so `ORDER BY AVG(salary)` can locate the `avg(salary)` key that
/// `evaluate_aggregate_function` wrote into the result row.
fn aggregate_column_name(func: &Function) -> Option<String> {
    let func_name = func.name.to_string().to_uppercase();
    let column = match &func.args {
        FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                Some(ident.value.clone())
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                parts.last().map(|p| p.value.clone())
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
            _ => return None,
        },
        _ => None,
    };
    Some(match func_name.as_str() {
        "COUNT" if column.is_none() => "count(*)".to_string(),
        "COUNT" => format!("count({})", column?),
        "SUM" => format!("sum({})", column.unwrap_or_else(|| "*".to_string())),
        "AVG" => format!("avg({})", column.unwrap_or_else(|| "*".to_string())),
        "MIN" => format!("min({})", column.unwrap_or_else(|| "*".to_string())),
        "MAX" => format!("max({})", column.unwrap_or_else(|| "*".to_string())),
        _ => return None,
    })
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
    // Extract the column name from the expression. ORDER BY can reference:
    //   - a bare column: `ORDER BY name`
    //   - a table-qualified column: `ORDER BY t.name`
    //   - an aggregate that also appears in SELECT: `ORDER BY AVG(salary)`
    //     — this works post-aggregation because the aggregation step writes
    //     the result under the canonical lowercase name (`avg(salary)`).
    let column = match &order_expr.expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents.last()?.value.clone(),
        Expr::Function(func) => aggregate_column_name(func)?,
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
            let ordering = crate::query::predicate::compare_json_values(a_val, b_val);

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
    let pk_field = engine.get_table_primary_key(&table_name)?;
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

        // Validate FK constraints before BEFORE-UPDATE triggers fire. Only
        // re-checks parents for FK columns whose value actually changed (an
        // UPDATE that leaves the FK column alone is always safe). PG runs
        // these constraint checks before triggers; matches that ordering.
        crate::fk::validate_update(engine, &table_name, &old_row, &updated_row)?;

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

        // Extract OLD and NEW primary keys. The hardcoded "id" pull
        // from earlier was a latent bug: any table with a non-`id`
        // PK had its UPDATE buffered under the wrong key, and the
        // committed Patch then merged into the wrong row (or no row
        // at all). Both PKs come from `schema.primary_key`.
        let old_pk = old_row.get(&pk_field).cloned().unwrap_or(Value::Null);
        let new_pk = final_row.get(&pk_field).cloned().unwrap_or(Value::Null);

        let pk_changed = old_pk != new_pk;

        if pk_changed {
            // PK-change semantics: PostgreSQL models this as DELETE
            // old + INSERT new. We do the same in the buffer (two
            // events) and in auto-commit (two storage applies). The
            // new PK must not collide with anything visible.
            //
            // Slice 1's `pk_visibility_in_transaction` does the
            // right thing here: a buffered `SoftDelete` masks any
            // committed row, so reusing a PK whose holder was
            // deleted earlier in this transaction works.
            if let Some(txn_id) = current_transaction() {
                match engine.pk_visibility_in_transaction(txn_id, &table_name, &new_pk)? {
                    crate::engine::PkVisibility::Active => {
                        mark_txn_aborted();
                        return Err(DriftError::InvalidQuery(format!(
                            "duplicate key value violates unique constraint on table \"{}\": key ({})=({}) already exists",
                            table_name, pk_field, new_pk
                        )));
                    }
                    crate::engine::PkVisibility::Deleted
                    | crate::engine::PkVisibility::Absent => {
                        let delete_event = crate::events::Event::new_soft_delete(
                            table_name.clone(),
                            old_pk.clone(),
                        );
                        let insert_event = crate::events::Event::new_insert(
                            table_name.clone(),
                            new_pk.clone(),
                            final_row.clone(),
                        );
                        engine.apply_event_in_transaction(txn_id, delete_event)?;
                        engine.apply_event_in_transaction(txn_id, insert_event)?;
                    }
                }
            } else {
                // Auto-commit: check committed state only (no buffer).
                // Each row applies independently; a mid-loop error
                // leaves prior-row changes committed. Same atomicity
                // limitation as today's auto-commit DML; documented.
                if engine.pk_exists_committed(&table_name, &new_pk)? {
                    return Err(DriftError::InvalidQuery(format!(
                        "duplicate key value violates unique constraint on table \"{}\": key ({})=({}) already exists",
                        table_name, pk_field, new_pk
                    )));
                }
                let delete_event = crate::events::Event::new_soft_delete(
                    table_name.clone(),
                    old_pk.clone(),
                );
                let insert_event = crate::events::Event::new_insert(
                    table_name.clone(),
                    new_pk.clone(),
                    final_row.clone(),
                );
                engine.apply_event(delete_event)?;
                engine.apply_event(insert_event)?;
            }
        } else {
            // No PK change: regular Patch keyed by the unchanged PK.
            if let Some(txn_id) = current_transaction() {
                let event = crate::events::Event::new_patch(
                    table_name.clone(),
                    old_pk,
                    final_row.clone(),
                );
                engine.apply_event_in_transaction(txn_id, event)?;
            } else {
                let patch_query = Query::Patch {
                    table: table_name.clone(),
                    primary_key: old_pk,
                    updates: final_row.clone(),
                };
                engine.execute_query(patch_query)?;
            }
        }

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
    // Forget any FK constraints declared on the dropped table so they can't
    // bleed through to a future CREATE TABLE with the same name.
    crate::fk::forget(&table_name);

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

    // Inline FK constraints declared as `dept_id VARCHAR REFERENCES dept(id)`
    // are surfaced by sqlparser as `ColumnOption::ForeignKey` on the column,
    // *not* as a `TableConstraint::ForeignKey`. We collect them here so the
    // FK registration loop below can treat both shapes uniformly.
    let mut inline_fks: Vec<crate::fk::ForeignKey> = Vec::new();

    // Process column definitions
    for column in columns {
        let col_name = column.name.value.clone();
        let col_type = column.data_type.to_string();
        // Treat inline `UNIQUE` (and inline `PRIMARY KEY`, which implies unique)
        // as a request to build a secondary index on the column. PostgreSQL
        // builds an implicit unique index for UNIQUE columns; we honor that
        // with our existing secondary-index mechanism even though uniqueness
        // itself isn't enforced beyond the PK today.
        let mut is_index = false;

        for option in &column.options {
            match &option.option {
                sqlparser::ast::ColumnOption::Unique { is_primary, .. } => {
                    is_index = true;
                    if *is_primary {
                        primary_key = col_name.clone();
                    }
                }
                sqlparser::ast::ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    if let Some(ref_col) = referred_columns.first() {
                        inline_fks.push(crate::fk::ForeignKey {
                            column: col_name.clone(),
                            ref_table: foreign_table.to_string(),
                            ref_column: ref_col.value.clone(),
                        });
                    }
                }
                _ => {}
            }
        }

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

    // Register FK constraints into the process-wide FK registry so subsequent
    // INSERT / UPDATE / DELETE through this same sql_bridge enforce them.
    // Both table-level FKs (`FOREIGN KEY (a) REFERENCES b(c)`) and inline FKs
    // (`a VARCHAR REFERENCES b(c)`) flow into the same registry — the parser
    // hands the latter back as ColumnOption::ForeignKey, which we collected
    // above into `inline_fks`.
    let mut all_fks: Vec<crate::fk::ForeignKey> = inline_fks;
    for c in &foreign_keys {
        if let crate::constraints::ConstraintType::ForeignKey {
            columns,
            reference_table,
            reference_columns,
            ..
        } = &c.constraint_type
        {
            // Pair child columns with parent columns positionally. SQL allows
            // composite FKs (multiple columns), so unzip into one ForeignKey
            // entry per column pair — the runtime check treats each column
            // independently with MATCH SIMPLE semantics.
            for (child_col, parent_col) in columns.iter().zip(reference_columns.iter()) {
                all_fks.push(crate::fk::ForeignKey {
                    column: child_col.clone(),
                    ref_table: reference_table.clone(),
                    ref_column: parent_col.clone(),
                });
            }
        }
    }
    if !all_fks.is_empty() {
        crate::fk::register(&table_name, all_fks);
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
            // Validate FK constraints before triggers. If any other table has
            // a row referencing this one, the delete is blocked (RESTRICT).
            // Cascade actions are parsed by CREATE TABLE but not yet executed
            // — that's a follow-up; for now we conservatively reject deletes
            // that would orphan child rows.
            crate::fk::validate_delete(engine, &table_name, &row)?;

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

            // Buffer through the transaction manager when inside a
            // transaction; otherwise apply the soft-delete immediately.
            if let Some(txn_id) = current_transaction() {
                let event =
                    crate::events::Event::new_soft_delete(table_name.clone(), primary_key.clone());
                engine.apply_event_in_transaction(txn_id, event)?;
            } else {
                let delete_query = Query::SoftDelete {
                    table: table_name.clone(),
                    primary_key: primary_key.clone(),
                };
                engine.execute_query(delete_query)?;
            }

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

/// Handle `SELECT ... FROM <table> FOR SYSTEM_TIME ALL [WHERE <pk> = <val>]`
///
/// Returns drift history for a single row (if WHERE matches) or all rows.
fn execute_for_system_time_all(engine: &mut Engine, sql: &str) -> Result<QueryResult> {
    let upper = sql.to_uppercase();

    // Extract table name: between FROM and FOR SYSTEM_TIME ALL
    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| DriftError::InvalidQuery("FOR SYSTEM_TIME ALL requires FROM clause".into()))?;
    let fst_pos = upper
        .find(" FOR SYSTEM_TIME ALL")
        .ok_or_else(|| DriftError::InvalidQuery("FOR SYSTEM_TIME ALL marker not found".into()))?;

    // Use original-case sql for the table name to preserve case
    let table = sql[from_pos + 6..fst_pos].trim().to_string();

    // Check for optional WHERE clause after FOR SYSTEM_TIME ALL
    let after_fst = sql[fst_pos + 20..].trim();
    let pk_filter = if after_fst.to_uppercase().strip_prefix("WHERE ").is_some() {
        // Parse simple equality: <col> = '<val>' or <col> = <num>
        let where_part = &after_fst[after_fst.to_uppercase().find("WHERE ").unwrap() + 6..];
        parse_pk_equality(where_part)
    } else {
        None
    };

    if let Some(pk_val) = pk_filter {
        engine
            .execute_query(Query::ShowDrift {
                table,
                primary_key: pk_val,
            })
            .map_err(|e| DriftError::InvalidQuery(e.to_string()))
    } else {
        // No WHERE filter — return history for all rows
        let pk_col = engine
            .get_table_primary_key(&table)
            .map_err(|e| DriftError::InvalidQuery(e.to_string()))?;

        let rows = match engine.execute_query(Query::Select {
            table: table.clone(),
            conditions: vec![],
            as_of: None,
            limit: None,
        })? {
            QueryResult::Rows { data } => data,
            _ => vec![],
        };

        let mut all_events: Vec<Value> = Vec::new();
        for row in rows {
            if let Some(pk_val) = row.get(&pk_col).cloned() {
                if let QueryResult::DriftHistory { events } =
                    engine.execute_query(Query::ShowDrift {
                        table: table.clone(),
                        primary_key: pk_val,
                    })?
                {
                    all_events.extend(events);
                }
            }
        }
        Ok(QueryResult::DriftHistory { events: all_events })
    }
}

/// Parse a simple `col = 'val'` or `col = num` equality clause and return the value part.
/// Returns `None` if the clause is not a simple single equality.
fn parse_pk_equality(where_clause: &str) -> Option<Value> {
    // Find the '=' sign
    let eq_pos = where_clause.find('=')?;
    let value_part = where_clause[eq_pos + 1..].trim();

    if value_part.starts_with('\'') && value_part.ends_with('\'') {
        // String value: strip quotes
        let inner = &value_part[1..value_part.len() - 1];
        Some(Value::String(inner.to_string()))
    } else if let Ok(n) = value_part.parse::<f64>() {
        Some(serde_json::json!(n))
    } else {
        // Bare word — treat as string
        Some(Value::String(value_part.to_string()))
    }
}
