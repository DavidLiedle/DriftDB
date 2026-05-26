//! Query Executor for PostgreSQL Protocol
//!
//! Executes SQL queries directly against the DriftDB engine

use anyhow::{anyhow, Result};
use driftdb_core::{Engine, EngineGuard};
use parking_lot::{Mutex as ParkingMutex, RwLock as SyncRwLock};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// `crate::transaction` was retired alongside the server's hand-rolled DML
// path — every BEGIN / COMMIT / ROLLBACK now routes through sql_bridge,
// which holds transaction state in the per-session SessionContext.

#[cfg(test)]
#[path = "executor_subquery_tests.rs"]
mod executor_subquery_tests;

/// Result types for different SQL operations
#[derive(Debug, Clone)]
pub enum QueryResult {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Insert {
        count: usize,
    },
    Update {
        count: usize,
    },
    Delete {
        count: usize,
    },
    CreateTable,
    DropTable,
    CreateIndex,
    #[allow(dead_code)]
    Begin,
    Commit,
    Rollback,
    Empty,
}


/// Prepared statement storage
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PreparedStatement {
    pub name: String,
    pub sql: String,
    pub parsed_query: ParsedQuery,
    pub param_types: Vec<ParamType>,
    pub created_at: std::time::Instant,
}

/// Parsed query structure for prepared statements
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ParsedQuery {
    pub query_type: QueryType,
    pub base_sql: String,
    pub param_positions: Vec<usize>, // Positions of $1, $2, etc.
}

/// Query type enum
#[derive(Debug, Clone)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

/// Parameter type for prepared statements
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ParamType {
    Integer,
    String,
    Boolean,
    Float,
    Unknown,
}

pub struct QueryExecutor<'a> {
    engine_guard: Option<&'a EngineGuard>,
    engine: Option<Arc<SyncRwLock<Engine>>>,
    // subquery_cache / cte_tables / use_indexes are reserved for future
    // server-local caching hooks; they're no longer touched by execute()
    // (all SQL flows through sql_bridge). Kept on the struct to preserve
    // the constructor signature and so reinstating per-connection caches
    // doesn't require re-plumbing.
    #[allow(dead_code)]
    subquery_cache: Arc<Mutex<HashMap<String, QueryResult>>>,
    #[allow(dead_code)]
    cte_tables: Arc<Mutex<HashMap<String, Vec<Value>>>>,
    #[allow(dead_code)]
    use_indexes: bool,
    prepared_statements: Arc<ParkingMutex<HashMap<String, PreparedStatement>>>,
    session_id: String,
    /// Per-connection sql_bridge session, holding the active transaction
    /// id (if any). Wrapped in a parking_lot::Mutex so the existing
    /// `&self`-everywhere API survives — each statement locks for the
    /// duration of execution, which matches PG's serial-per-connection
    /// statement model.
    session: Arc<ParkingMutex<driftdb_core::sql_bridge::SessionContext>>,
}

#[allow(dead_code)]
impl<'a> QueryExecutor<'a> {
    /// Convert core sql::QueryResult to server QueryResult
    fn convert_sql_result(
        &self,
        core_result: driftdb_core::query::QueryResult,
        table_columns: Option<Vec<String>>,
    ) -> Result<QueryResult> {
        use driftdb_core::query::QueryResult as CoreResult;
        use serde_json::Value;

        match core_result {
            CoreResult::Success { message } => {
                debug!("SQL execution success: {}", message);
                // Parse the message to determine the proper response type
                if message.contains("Index") && message.contains("created") {
                    Ok(QueryResult::CreateIndex)
                } else if message.starts_with("Table") && message.contains("created") {
                    Ok(QueryResult::CreateTable)
                } else if message.starts_with("Table") && message.contains("dropped") {
                    Ok(QueryResult::DropTable)
                } else if message.starts_with("Inserted") {
                    let count = message
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    Ok(QueryResult::Insert { count })
                } else if message.starts_with("Updated") {
                    let count = message
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    Ok(QueryResult::Update { count })
                } else if message.starts_with("Deleted") {
                    let count = message
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    Ok(QueryResult::Delete { count })
                } else {
                    Ok(QueryResult::Empty)
                }
            }
            CoreResult::Rows { data } => {
                // Derive column list from the first row's keys when possible —
                // the workspace enables `serde_json/preserve_order`, so map
                // iteration order matches projection / aggregation order. This
                // is critical for queries like `SELECT b, a FROM t` where the
                // schema column order would shuffle the result.
                //
                // For empty result sets we fall back to `table_columns` (the
                // full schema), so PostgreSQL clients still see a column header
                // — `SELECT * FROM empty_table` returns columns even though
                // there's no row to read them from.
                let columns: Vec<String> = if let Some(Value::Object(first_row)) = data.first() {
                    first_row.keys().cloned().collect()
                } else {
                    table_columns.unwrap_or_default()
                };

                let rows: Vec<Vec<Value>> = data
                    .iter()
                    .filter_map(|row| {
                        if let Value::Object(obj) = row {
                            Some(
                                columns
                                    .iter()
                                    .map(|col| obj.get(col).cloned().unwrap_or(Value::Null))
                                    .collect(),
                            )
                        } else {
                            None
                        }
                    })
                    .collect();

                Ok(QueryResult::Select { columns, rows })
            }
            CoreResult::DriftHistory { events } => {
                // Convert history events to rows
                Ok(QueryResult::Select {
                    columns: vec!["event".to_string()],
                    rows: events.into_iter().map(|e| vec![e]).collect(),
                })
            }
            CoreResult::Plan { plan } => {
                // Convert query plan to JSON representation for display
                let plan_json = serde_json::to_value(&plan)?;
                Ok(QueryResult::Select {
                    columns: vec!["query_plan".to_string()],
                    rows: vec![vec![plan_json]],
                })
            }
            CoreResult::Error { message } => Err(anyhow!("SQL execution error: {}", message)),
        }
    }

    /// Extract table name from SELECT SQL
    fn extract_table_from_sql_static(sql: &str) -> Result<String> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!("Parse error: {}", e))?;

        if let Some(sqlparser::ast::Statement::Query(query)) = ast.first() {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                if let Some(table_with_joins) = select.from.first() {
                    if let sqlparser::ast::TableFactor::Table { name, .. } =
                        &table_with_joins.relation
                    {
                        return Ok(name.to_string());
                    }
                }
            }
        }

        Err(anyhow!("Could not extract table name from query"))
    }

    pub fn new(engine: Arc<SyncRwLock<Engine>>) -> QueryExecutor<'static> {
        QueryExecutor {
            engine_guard: None,
            engine: Some(engine),
            subquery_cache: Arc::new(Mutex::new(HashMap::new())),
            cte_tables: Arc::new(Mutex::new(HashMap::new())),
            use_indexes: true,
            prepared_statements: Arc::new(ParkingMutex::new(HashMap::new())),
            session_id: format!("session_{}", std::process::id()),
            session: Arc::new(ParkingMutex::new(
                driftdb_core::sql_bridge::SessionContext::new(),
            )),
        }
    }

    pub fn new_with_guard(engine_guard: &'a EngineGuard) -> Self {
        Self {
            engine_guard: Some(engine_guard),
            engine: None,
            subquery_cache: Arc::new(Mutex::new(HashMap::new())),
            cte_tables: Arc::new(Mutex::new(HashMap::new())),
            use_indexes: true,
            prepared_statements: Arc::new(ParkingMutex::new(HashMap::new())),
            session_id: format!("guard_session_{}", std::process::id()),
            session: Arc::new(ParkingMutex::new(
                driftdb_core::sql_bridge::SessionContext::new(),
            )),
        }
    }

    /// Create a new executor with a caller-supplied session id. Used by the
    /// PostgreSQL protocol layer so per-connection session ids are stable
    /// across statements; transaction state itself lives in `self.session`.
    pub fn new_with_guard_and_session_id(engine_guard: &'a EngineGuard, session_id: String) -> Self {
        Self {
            engine_guard: Some(engine_guard),
            engine: None,
            subquery_cache: Arc::new(Mutex::new(HashMap::new())),
            cte_tables: Arc::new(Mutex::new(HashMap::new())),
            use_indexes: true,
            prepared_statements: Arc::new(ParkingMutex::new(HashMap::new())),
            session_id,
            session: Arc::new(ParkingMutex::new(
                driftdb_core::sql_bridge::SessionContext::new(),
            )),
        }
    }

    /// Set the session ID for this executor
    pub fn set_session_id(&mut self, session_id: String) {
        self.session_id = session_id;
    }

    /// Get read access to the engine
    fn engine_read(&self) -> Result<parking_lot::RwLockReadGuard<'_, Engine>> {
        if let Some(guard) = &self.engine_guard {
            // EngineGuard provides a read() method that returns RwLockReadGuard
            Ok(guard.read())
        } else if let Some(engine) = &self.engine {
            Ok(engine.read())
        } else {
            Err(anyhow!("No engine available"))
        }
    }

    /// Get write access to the engine
    fn engine_write(&self) -> Result<parking_lot::RwLockWriteGuard<'_, Engine>> {
        if let Some(guard) = &self.engine_guard {
            // EngineGuard provides a write() method that returns RwLockWriteGuard
            Ok(guard.write())
        } else if let Some(engine) = &self.engine {
            Ok(engine.write())
        } else {
            Err(anyhow!("No engine available"))
        }
    }

    /// Parse SQL value (string, number, boolean, null)
    fn parse_sql_value(&self, value_str: &str) -> Result<Value> {
        let trimmed = value_str.trim();

        // NULL
        if trimmed.eq_ignore_ascii_case("NULL") {
            return Ok(Value::Null);
        }

        // Boolean
        if trimmed.eq_ignore_ascii_case("TRUE") {
            return Ok(Value::Bool(true));
        }
        if trimmed.eq_ignore_ascii_case("FALSE") {
            return Ok(Value::Bool(false));
        }

        // String (single quotes)
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            let content = &trimmed[1..trimmed.len() - 1];
            return Ok(Value::String(content.to_string()));
        }

        // Number
        if let Ok(n) = trimmed.parse::<i64>() {
            return Ok(Value::Number(n.into()));
        }
        if let Ok(f) = trimmed.parse::<f64>() {
            return Ok(serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null));
        }

        // Default to string without quotes
        Ok(Value::String(trimmed.to_string()))
    }

    // -------------------------------------------------------------------------
    // CASE WHEN expression evaluation
    // -------------------------------------------------------------------------

    // Note: `compare_values` and `matches_condition` previously lived here as
    // server-local helpers, with NULLs-first ordering and number-only ordered
    // comparisons. They've been replaced by `driftdb_core::query::predicate`
    // (NULLs-last, full operator set including LIKE/IN/IS NULL/etc.) so the
    // PostgreSQL protocol path produces identical results to the CLI path.

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        // Handle common PostgreSQL client queries
        if sql.eq_ignore_ascii_case("SELECT 1") || sql.eq_ignore_ascii_case("SELECT 1;") {
            return Ok(QueryResult::Select {
                columns: vec!["?column?".to_string()],
                rows: vec![vec![Value::Number(1.into())]],
            });
        }

        if sql.to_lowercase().contains("select version()") {
            return Ok(QueryResult::Select {
                columns: vec!["version".to_string()],
                rows: vec![vec![Value::String(
                    "PostgreSQL 14.0 (DriftDB 0.7.3-alpha)".to_string(),
                )]],
            });
        }

        if sql.to_lowercase().contains("select current_database()") {
            return Ok(QueryResult::Select {
                columns: vec!["current_database".to_string()],
                rows: vec![vec![Value::String("driftdb".to_string())]],
            });
        }

        // Use SQL bridge for real SQL execution

        // First check if this is a transaction command or legacy command
        let lower = sql.to_lowercase().trim().to_string();

        // Handle transaction commands first (before SQL bridge)
        if lower.starts_with("begin") || lower == "begin" {
            return self.execute_begin(sql).await;
        }
        if lower.starts_with("commit") || lower == "commit" {
            return self.execute_commit().await;
        }
        if lower.starts_with("rollback") || lower == "rollback" {
            return self.execute_rollback(sql).await;
        }
        if lower.starts_with("savepoint ") {
            return self.execute_savepoint(sql).await;
        }

        // SHOW and SET still go through the local legacy handler — they're
        // PostgreSQL-protocol housekeeping (`SHOW TABLES`, `SET search_path`)
        // that sql_bridge doesn't aim to provide.
        if lower.starts_with("show ") || lower.starts_with("set ") {
            return self.execute_legacy(sql).await;
        }
        // EXPLAIN falls through to the bridge dispatch at the bottom of this
        // function — `sql_bridge::execute_sql` calls `crate::sql_explain` to
        // build the plan tree and returns its formatted lines as a
        // single-column `QUERY PLAN` result, which `convert_sql_result`
        // turns into the QueryResult::Select shape psql expects.

        // CREATE TABLE / CREATE INDEX fall through to the bridge-backed
        // dispatch at the bottom of this function. sql_bridge::execute_sql
        // handles standard SQL CREATE TABLE (including inline PRIMARY KEY,
        // inline REFERENCES, and table-level FK constraints) and CREATE
        // INDEX uniformly — there's no need for a server-local handler.
        // The legacy `(pk = id, INDEX(col))` form is no longer supported;
        // it was deprecated as part of this migration.

        // SELECT and WITH (CTE) queries are executed through the core SQL bridge.
        // The bridge handles temporal queries, aggregation, GROUP BY/HAVING,
        // ORDER BY, JOINs, subqueries, CTEs (including recursive), set operations,
        // and CASE WHEN — what used to be two parallel implementations is now one.
        if lower.starts_with("select ") || lower.starts_with("with ") {
            return self.execute_select_via_bridge(sql).await;
        }
        // INSERT / UPDATE / DELETE now run through the core SQL bridge, with
        // the per-session SessionContext threading any active transaction
        // through. sql_bridge handles FK validation, BEFORE/AFTER triggers,
        // multi-row VALUES, and transactional buffering uniformly — what
        // used to be hand-rolled DML on this side is now one canonical path
        // shared with the CLI and direct-core callers.
        if lower.starts_with("insert into ") || lower.starts_with("insert ") {
            return self.execute_dml_via_bridge(sql).await;
        }
        if lower.starts_with("update ") {
            return self.execute_dml_via_bridge(sql).await;
        }
        if lower.starts_with("delete from ") || lower.starts_with("delete ") {
            return self.execute_dml_via_bridge(sql).await;
        }

        // For other SQL commands, use the bridge
        let mut engine = self.engine_write()?;

        // Extract table name and get columns BEFORE executing query to avoid deadlock
        let table_columns = if let Ok(table_name) = Self::extract_table_from_sql_static(sql) {
            engine.get_table_columns(&table_name).ok()
        } else {
            None
        };

        let result = driftdb_core::sql_bridge::execute_sql(&mut engine, sql);

        match result {
            Ok(core_result) => {
                // Pass the columns we extracted while holding the lock
                self.convert_sql_result(core_result, table_columns)
            }
            Err(e) => {
                debug!("SQL execution failed: {}", e);
                Err(anyhow!("SQL execution failed: {}", e))
            }
        }
    }

    /// Execute a SELECT / WITH query by delegating to the core SQL bridge.
    /// The conversion layer derives column order from the first result row
    /// (preserve_order makes that deterministic). For empty result sets we
    /// fall back to projection columns parsed from the SQL itself — that
    /// way `SELECT name FROM empty_table` still reports `name` in the
    /// column header, matching PostgreSQL.
    async fn execute_select_via_bridge(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine_write()?;

        // Best-effort projection extraction for empty-result-set headers.
        // Falls through to the table schema if SQL parsing fails or the
        // query uses `SELECT *`.
        let projection_columns = Self::projection_columns_from_sql(sql).or_else(|| {
            Self::extract_table_from_sql_static(sql)
                .ok()
                .and_then(|name| engine.get_table_columns(&name).ok())
        });

        let mut session = self.session.lock();
        let result =
            driftdb_core::sql_bridge::execute_sql_in_session(&mut engine, sql, &mut session)
                .map_err(|e| anyhow!("SQL execution failed: {}", e))?;
        drop(session);
        drop(engine);

        self.convert_sql_result(result, projection_columns)
    }

    /// Execute a DML statement (INSERT / UPDATE / DELETE) through the core
    /// SQL bridge. Locks the per-connection `SessionContext` so the active
    /// transaction id (set by `BEGIN`, cleared by `COMMIT`/`ROLLBACK`) is
    /// threaded into sql_bridge — buffered writes accumulate atomically
    /// instead of hitting storage immediately. Conversion is the same as
    /// for SELECTs but result-row metadata is absent, so the table schema
    /// is the natural column-list fallback.
    async fn execute_dml_via_bridge(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine_write()?;

        let table_columns = Self::extract_table_from_sql_static(sql)
            .ok()
            .and_then(|name| engine.get_table_columns(&name).ok());

        let mut session = self.session.lock();
        let result =
            driftdb_core::sql_bridge::execute_sql_in_session(&mut engine, sql, &mut session)
                .map_err(|e| anyhow!("SQL execution failed: {}", e))?;
        drop(session);
        drop(engine);

        self.convert_sql_result(result, table_columns)
    }

    /// Parse the SQL and return the user-visible column labels in projection
    /// order. Returns `None` for `SELECT *`, parse failures, or non-SELECT
    /// statements — callers should fall back to the table schema in those
    /// cases.
    fn projection_columns_from_sql(sql: &str) -> Option<Vec<String>> {
        use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let ast = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
        let query = match ast.into_iter().next()? {
            Statement::Query(q) => q,
            _ => return None,
        };
        let select = match *query.body {
            SetExpr::Select(s) => s,
            _ => return None,
        };

        let mut out = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let name = match item {
                SelectItem::Wildcard(_) => return None,
                SelectItem::UnnamedExpr(Expr::Identifier(i)) => i.value.clone(),
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    parts.last()?.value.clone()
                }
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::UnnamedExpr(Expr::Function(f)) => {
                    // Canonical lowercase aggregate label — matches what
                    // evaluate_aggregate_function writes into result rows.
                    let func_name = f.name.to_string().to_lowercase();
                    format!("{}(...)", func_name)
                }
                _ => return None,
            };
            out.push(name);
        }
        Some(out)
    }

    /// Server-local handler for PostgreSQL housekeeping commands that
    /// `sql_bridge` doesn't aim to provide. EXPLAIN was previously handled
    /// here too; it now flows through the bridge via `crate::sql_explain`.
    async fn execute_legacy(&self, sql: &str) -> Result<QueryResult> {
        if sql.to_lowercase().starts_with("show ") {
            return self.execute_show(sql).await;
        }
        if sql.to_lowercase().starts_with("set ") {
            // SET is a no-op today — we don't track session GUCs. Returning
            // Empty keeps psql / JDBC connection setup quiet.
            return Ok(QueryResult::Empty);
        }
        warn!("Unsupported SQL command: {}", sql);
        Err(anyhow!("Unsupported SQL command: {}", sql))
    }

    // Note: execute_insert / parse_values_list / execute_update / execute_delete
    // moved into `driftdb_core::sql_bridge`. Single canonical DML path now.





    async fn execute_show(&self, sql: &str) -> Result<QueryResult> {
        let engine = self.engine_read()?;
        let lower = sql.to_lowercase();

        if lower.starts_with("show tables") {
            let tables = engine.list_tables();
            let rows: Vec<Vec<Value>> =
                tables.into_iter().map(|t| vec![Value::String(t)]).collect();

            Ok(QueryResult::Select {
                columns: vec!["Tables_in_driftdb".to_string()],
                rows,
            })
        } else if lower.starts_with("show databases") {
            Ok(QueryResult::Select {
                columns: vec!["Database".to_string()],
                rows: vec![vec![Value::String("driftdb".to_string())]],
            })
        } else {
            warn!("Unsupported SHOW command: {}", sql);
            Err(anyhow!("Unsupported SHOW command"))
        }
    }

    /// Execute PREPARE command to create a prepared statement
    async fn execute_prepare(&self, sql: &str) -> Result<QueryResult> {
        // Parse: PREPARE stmt_name AS SELECT ...
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 4 || parts[2].to_uppercase() != "AS" {
            return Err(anyhow!(
                "Invalid PREPARE syntax. Use: PREPARE stmt_name AS SELECT ..."
            ));
        }

        let stmt_name = parts[1];
        let query_start = sql.find(" AS ").ok_or_else(|| anyhow!("Invalid PREPARE syntax: missing AS"))? + 4;
        let query_sql = sql[query_start..].trim();

        // Parse the query and identify parameters ($1, $2, etc.)
        let parsed_query = self.parse_prepared_query(query_sql)?;

        let prepared_stmt = PreparedStatement {
            name: stmt_name.to_string(),
            sql: query_sql.to_string(),
            parsed_query,
            param_types: Vec::new(), // Will be inferred on first execution
            created_at: std::time::Instant::now(),
        };

        // Store the prepared statement
        let mut statements = self.prepared_statements.lock();
        statements.insert(stmt_name.to_string(), prepared_stmt);

        info!("Prepared statement '{}' created", stmt_name);
        Ok(QueryResult::Empty)
    }

    /// Execute a prepared statement with parameters
    async fn execute_prepared(&self, sql: &str) -> Result<QueryResult> {
        // Parse: EXECUTE stmt_name (param1, param2, ...)
        let _lower = sql.to_lowercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();

        if parts.len() < 2 {
            return Err(anyhow!(
                "Invalid EXECUTE syntax. Use: EXECUTE stmt_name (params...)"
            ));
        }

        let stmt_name = parts[1].trim_end_matches('(');

        // Extract parameters if present
        let params = if sql.contains('(') {
            let param_start = sql.find('(').ok_or_else(|| anyhow!("Invalid EXECUTE syntax: missing '('"))? + 1;
            let param_end = sql
                .rfind(')')
                .ok_or_else(|| anyhow!("Missing closing parenthesis"))?;
            let param_str = &sql[param_start..param_end];
            self.parse_execute_params(param_str)?
        } else {
            Vec::new()
        };

        // Get the prepared statement
        let prepared = {
            let statements = self.prepared_statements.lock();
            statements
                .get(stmt_name)
                .ok_or_else(|| anyhow!("Prepared statement '{}' not found", stmt_name))?
                .clone()
        }; // Lock is dropped here

        // Substitute parameters into the query
        let final_sql = self.substitute_params(&prepared.sql, &params)?;

        debug!(
            "Executing prepared statement '{}' with {} parameters",
            stmt_name,
            params.len()
        );

        // Execute the final query
        Box::pin(self.execute(&final_sql)).await
    }

    /// Deallocate a prepared statement
    async fn deallocate_prepared(&self, sql: &str) -> Result<QueryResult> {
        // Parse: DEALLOCATE stmt_name
        let parts: Vec<&str> = sql.split_whitespace().collect();

        if parts.len() < 2 {
            return Err(anyhow!(
                "Invalid DEALLOCATE syntax. Use: DEALLOCATE stmt_name"
            ));
        }

        let stmt_name = parts[1];

        let mut statements = self.prepared_statements.lock();
        if statements.remove(stmt_name).is_some() {
            info!("Deallocated prepared statement '{}'", stmt_name);
            Ok(QueryResult::Empty)
        } else {
            Err(anyhow!("Prepared statement '{}' not found", stmt_name))
        }
    }

    /// Parse a query to identify parameters and structure
    fn parse_prepared_query(&self, sql: &str) -> Result<ParsedQuery> {
        let lower = sql.to_lowercase();

        // Determine query type
        let query_type = if lower.starts_with("select") {
            QueryType::Select
        } else if lower.starts_with("insert") {
            QueryType::Insert
        } else if lower.starts_with("update") {
            QueryType::Update
        } else if lower.starts_with("delete") {
            QueryType::Delete
        } else {
            QueryType::Other
        };

        // Find all parameter placeholders ($1, $2, etc.)
        let mut param_positions = Vec::new();
        let chars: Vec<char> = sql.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '$' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
                param_positions.push(i);
                // Skip the parameter number
                i += 2;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(ParsedQuery {
            query_type,
            base_sql: sql.to_string(),
            param_positions,
        })
    }

    /// Parse parameters from EXECUTE command
    fn parse_execute_params(&self, param_str: &str) -> Result<Vec<Value>> {
        if param_str.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut params = Vec::new();
        let parts = param_str.split(',');

        for part in parts {
            let trimmed = part.trim();
            params.push(self.parse_sql_value(trimmed)?);
        }

        Ok(params)
    }

    /// Substitute parameter values into the prepared query
    fn substitute_params(&self, sql: &str, params: &[Value]) -> Result<String> {
        let mut result = sql.to_string();

        // Replace parameters in reverse order to avoid index shifting
        for (i, param) in params.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            let value_str = match param {
                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => return Err(anyhow!("Unsupported parameter type")),
            };

            result = result.replace(&placeholder, &value_str);
        }

        // Check if any parameters were not replaced
        if result.contains('$') && result.chars().any(|c| c == '$') {
            // Simple check for remaining parameters
            for i in 1..=10 {
                if result.contains(&format!("${}", i)) {
                    return Err(anyhow!("Parameter ${} not provided", i));
                }
            }
        }

        Ok(result)
    }

    /// Parse FROM clause to extract table references and JOIN operations
    async fn execute_begin(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine_write()?;
        let mut session = self.session.lock();
        driftdb_core::sql_bridge::execute_sql_in_session(&mut engine, sql, &mut session)
            .map_err(|e| anyhow!("BEGIN failed: {}", e))?;
        info!(
            "Started transaction {:?} for session {}",
            session.transaction_id, self.session_id
        );
        Ok(QueryResult::Begin)
    }

    /// Execute COMMIT through the core SQL bridge. The bridge applies
    /// every event buffered since BEGIN; if any apply fails (PK conflict,
    /// FK violation deferred to commit, etc.) the failure surfaces here.
    async fn execute_commit(&self) -> Result<QueryResult> {
        let mut engine = self.engine_write()?;
        let mut session = self.session.lock();
        driftdb_core::sql_bridge::execute_sql_in_session(&mut engine, "COMMIT", &mut session)
            .map_err(|e| anyhow!("COMMIT failed: {}", e))?;
        info!("Committed transaction for session {}", self.session_id);
        Ok(QueryResult::Commit)
    }

    /// Execute ROLLBACK through the core SQL bridge. Savepoint variants
    /// (`ROLLBACK TO SAVEPOINT x`) are not yet routed through sql_bridge —
    /// sqlparser doesn't recognise them as `Statement::Rollback`, so the
    /// fallthrough here returns an error rather than silently doing the
    /// wrong thing. Full ROLLBACK works.
    async fn execute_rollback(&self, sql: &str) -> Result<QueryResult> {
        let lower = sql.to_lowercase();
        if lower.contains("to savepoint ") || lower.contains("to ") {
            return Err(anyhow!(
                "ROLLBACK TO SAVEPOINT is not yet supported via the core SQL bridge"
            ));
        }
        let mut engine = self.engine_write()?;
        let mut session = self.session.lock();
        driftdb_core::sql_bridge::execute_sql_in_session(&mut engine, "ROLLBACK", &mut session)
            .map_err(|e| anyhow!("ROLLBACK failed: {}", e))?;
        info!("Rolled back transaction for session {}", self.session_id);
        Ok(QueryResult::Rollback)
    }

    /// Execute SAVEPOINT
    /// SAVEPOINT and RELEASE SAVEPOINT — sqlparser does parse these, but
    /// sql_bridge doesn't yet route them through to the engine's
    /// transaction manager, and the savepoint state on the old server-side
    /// TransactionManager was never read by anything that mattered. These
    /// handlers acknowledge the command but don't yet implement nested
    /// rollback; documented in the rollback handler too.
    async fn execute_savepoint(&self, sql: &str) -> Result<QueryResult> {
        let savepoint_pos = sql
            .to_lowercase()
            .find("savepoint ")
            .ok_or_else(|| anyhow!("Invalid SAVEPOINT syntax"))?;
        let savepoint_name = sql[savepoint_pos + 10..]
            .trim()
            .trim_matches(';')
            .to_string();
        info!(
            "SAVEPOINT '{}' acknowledged (not yet enforced) for session {}",
            savepoint_name, self.session_id
        );
        Ok(QueryResult::Empty)
    }

    /// RELEASE SAVEPOINT — see SAVEPOINT note above.
    async fn execute_release_savepoint(&self, sql: &str) -> Result<QueryResult> {
        let savepoint_pos = sql
            .to_lowercase()
            .find("savepoint ")
            .ok_or_else(|| anyhow!("Invalid RELEASE SAVEPOINT syntax"))?;
        let savepoint_name = sql[savepoint_pos + 10..].trim().trim_matches(';');
        info!(
            "RELEASE SAVEPOINT '{}' acknowledged for session {}",
            savepoint_name, self.session_id
        );
        Ok(QueryResult::Empty)
    }

    /// Check if we're in a transaction by inspecting the per-session
    /// SessionContext. The lock is released immediately — only `Option::is_some`
    /// is observed.
    fn in_transaction(&self) -> bool {
        self.session.lock().transaction_id.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;

    /// Regression test: multi-row `INSERT INTO t VALUES (a), (b), (c)` must
    /// succeed through the PostgreSQL-protocol path. The server's old
    /// hand-rolled execute_insert parsed VALUES as a single row only — so
    /// psql clients got "Invalid VALUES syntax" while the CLI worked.
    /// After this commit DML goes through sql_bridge, which has had
    /// multi-row support since the SELECT migration.
    #[tokio::test]
    async fn test_multi_row_insert_through_pg_path() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        executor
            .execute("CREATE TABLE t (id VARCHAR, name VARCHAR, PRIMARY KEY (id))")
            .await
            .unwrap();

        let result = executor
            .execute("INSERT INTO t (id, name) VALUES ('a', 'Alice'), ('b', 'Bob'), ('c', 'Carol')")
            .await
            .unwrap();
        match result {
            QueryResult::Insert { count: _ } | QueryResult::Empty => {}
            other => panic!("expected Insert/Empty result, got {:?}", other),
        }

        let count = executor.execute("SELECT id FROM t").await.unwrap();
        match count {
            QueryResult::Select { rows, .. } => assert_eq!(rows.len(), 3),
            other => panic!("expected Select, got {:?}", other),
        }
    }

    /// Regression test: EXPLAIN through the PostgreSQL-protocol path
    /// returns the plan tree built by `crate::sql_explain` — one row per
    /// plan line, in a `QUERY PLAN` column. Confirms the server crate no
    /// longer carries its own planner.
    #[tokio::test]
    async fn test_explain_through_pg_path() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR)")
            .await
            .unwrap();
        for sql in [
            "INSERT INTO users (id, name) VALUES ('1', 'Alice')",
            "INSERT INTO users (id, name) VALUES ('2', 'Bob')",
        ] {
            executor.execute(sql).await.unwrap();
        }

        let result = executor
            .execute("EXPLAIN SELECT * FROM users WHERE id = '1' ORDER BY name DESC LIMIT 5")
            .await
            .unwrap();
        match result {
            QueryResult::Select { columns, rows } => {
                assert_eq!(columns, vec!["QUERY PLAN".to_string()]);
                let text = rows
                    .iter()
                    .filter_map(|r| r.first().and_then(|v| v.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n");
                // The plan must include the operator chain we just exercised.
                assert!(text.contains("Limit"), "missing Limit in plan:\n{}", text);
                assert!(text.contains("Sort"), "missing Sort in plan:\n{}", text);
                assert!(text.contains("Seq Scan on users"), "missing Seq Scan:\n{}", text);
                assert!(text.contains("Filter:"), "missing Filter:\n{}", text);
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    /// Regression test: BEGIN; INSERT; INSERT; COMMIT through the PG path
    /// must atomically apply both writes — and a parallel reader must see
    /// zero rows during the transaction. Mirrors
    /// test_session_context_buffers_inserts_until_commit but exercises the
    /// server's `execute()` entry point (post-DML migration), so it catches
    /// any regression where the QueryExecutor stops threading its
    /// SessionContext through sql_bridge correctly.
    #[tokio::test]
    async fn test_transactional_insert_through_pg_path() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));

        // Two QueryExecutors share the engine but each owns its own
        // SessionContext, mirroring two PostgreSQL clients on one DB.
        let writer = QueryExecutor::new(engine.clone());
        let reader = QueryExecutor::new(engine);

        writer
            .execute("CREATE TABLE accounts (id VARCHAR, bal INTEGER, PRIMARY KEY (id))")
            .await
            .unwrap();

        // Open transaction on the writer; INSERT two rows.
        writer.execute("BEGIN").await.unwrap();
        writer
            .execute("INSERT INTO accounts (id, bal) VALUES ('a', 100)")
            .await
            .unwrap();
        writer
            .execute("INSERT INTO accounts (id, bal) VALUES ('b', 200)")
            .await
            .unwrap();

        // Reader sees zero rows because the writer hasn't committed yet.
        let mid = reader.execute("SELECT id FROM accounts").await.unwrap();
        match mid {
            QueryResult::Select { rows, .. } => assert_eq!(
                rows.len(),
                0,
                "uncommitted INSERTs must not be visible to other sessions"
            ),
            other => panic!("expected Select, got {:?}", other),
        }

        writer.execute("COMMIT").await.unwrap();

        let after = reader.execute("SELECT id FROM accounts").await.unwrap();
        match after {
            QueryResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2, "both rows visible after commit")
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    /// Regression test: ROLLBACK on the PG path discards buffered writes
    /// and leaves only pre-transaction rows visible.
    #[tokio::test]
    async fn test_transactional_rollback_through_pg_path() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        executor
            .execute("CREATE TABLE items (id VARCHAR, label VARCHAR, PRIMARY KEY (id))")
            .await
            .unwrap();
        executor
            .execute("INSERT INTO items (id, label) VALUES ('keep', 'before')")
            .await
            .unwrap();

        executor.execute("BEGIN").await.unwrap();
        executor
            .execute("INSERT INTO items (id, label) VALUES ('discard', 'in-tx')")
            .await
            .unwrap();
        executor.execute("ROLLBACK").await.unwrap();

        let result = executor.execute("SELECT id FROM items").await.unwrap();
        match result {
            QueryResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::String("keep".to_string()));
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_select_one() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        let result = executor.execute("SELECT 1").await.unwrap();
        match result {
            QueryResult::Select { columns, rows } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Number(1.into()));
            }
            _ => panic!("Expected SELECT result"),
        }
    }


    #[test]
    fn test_compare_values_routes_through_canonical_predicate() {
        use driftdb_core::query::predicate::compare_json_values;
        use std::cmp::Ordering;

        // Number comparison.
        let a = Value::Number(10.into());
        let b = Value::Number(20.into());
        assert_eq!(compare_json_values(&a, &b), Ordering::Less);
        assert_eq!(compare_json_values(&b, &a), Ordering::Greater);
        assert_eq!(compare_json_values(&a, &a), Ordering::Equal);

        // String comparison.
        let a = Value::String("apple".to_string());
        let b = Value::String("banana".to_string());
        assert_eq!(compare_json_values(&a, &b), Ordering::Less);
        assert_eq!(compare_json_values(&b, &a), Ordering::Greater);

        // NULL handling — SQL-standard NULLs-last in ASC (was NULLs-first
        // in the server's deleted local impl; that disagreed with the CLI
        // path's NULLs-last and produced different orderings depending on
        // which protocol the user connected through).
        let null = Value::Null;
        let num = Value::Number(5.into());
        assert_eq!(compare_json_values(&null, &num), Ordering::Greater);
        assert_eq!(compare_json_values(&num, &null), Ordering::Less);
        assert_eq!(compare_json_values(&null, &null), Ordering::Equal);
    }

    #[tokio::test]
    async fn test_order_by_and_limit_integration() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@example.com')"#,
            r#"INSERT INTO users (id, name, age, email) VALUES (2, 'Bob', 25, 'bob@example.com')"#,
            r#"INSERT INTO users (id, name, age, email) VALUES (3, 'Charlie', 35, 'charlie@example.com')"#,
            r#"INSERT INTO users (id, name, age, email) VALUES (4, 'David', 28, 'david@example.com')"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test ORDER BY age ASC
        let result = executor
            .execute("SELECT * FROM users ORDER BY age ASC")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert!(columns.contains(&"age".to_string()));
            assert_eq!(rows.len(), 4);

            let age_index = columns.iter().position(|c| c == "age").unwrap();
            let ages: Vec<i64> = rows
                .iter()
                .map(|row| row[age_index].as_i64().unwrap())
                .collect();
            assert_eq!(ages, vec![25, 28, 30, 35]); // Should be sorted ascending
        } else {
            panic!("Expected SELECT result");
        }

        // Test ORDER BY age DESC with LIMIT
        let result = executor
            .execute("SELECT * FROM users ORDER BY age DESC LIMIT 2")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(rows.len(), 2); // Should be limited to 2 rows

            let age_index = columns.iter().position(|c| c == "age").unwrap();
            let ages: Vec<i64> = rows
                .iter()
                .map(|row| row[age_index].as_i64().unwrap())
                .collect();
            assert_eq!(ages, vec![35, 30]); // Should be sorted descending and limited
        } else {
            panic!("Expected SELECT result");
        }

        // Test ORDER BY name ASC (string sorting)
        let result = executor
            .execute("SELECT * FROM users ORDER BY name ASC")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            let name_index = columns.iter().position(|c| c == "name").unwrap();
            let names: Vec<String> = rows
                .iter()
                .map(|row| row[name_index].as_str().unwrap().to_string())
                .collect();
            assert_eq!(names, vec!["Alice", "Bob", "Charlie", "David"]); // Should be sorted alphabetically
        } else {
            panic!("Expected SELECT result");
        }

        // Test WHERE + ORDER BY + LIMIT
        let result = executor
            .execute("SELECT * FROM users WHERE age >= 28 ORDER BY age ASC LIMIT 2")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(rows.len(), 2); // Should be limited to 2 rows

            let age_index = columns.iter().position(|c| c == "age").unwrap();
            let ages: Vec<i64> = rows
                .iter()
                .map(|row| row[age_index].as_i64().unwrap())
                .collect();
            assert_eq!(ages, vec![28, 30]); // Should have ages >= 28, sorted ascending, limited to 2
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_count_star() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test COUNT(*)
        let result = executor
            .execute("SELECT COUNT(*) FROM users")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(3.into()));
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_count_column() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data with some null ages
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name) VALUES (3, 'Charlie')"#, // null age
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test COUNT(age) - should exclude null values
        let result = executor
            .execute("SELECT COUNT(age) FROM users")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(age)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(2.into())); // Only 2 non-null ages
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_sum() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 50000)"#,
            r#"INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 60000)"#,
            r#"INSERT INTO employees (id, name, salary) VALUES (3, 'Charlie', 70000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test SUM(salary)
        let result = executor
            .execute("SELECT SUM(salary) FROM employees")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "sum(salary)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(180000.into())); // 50000 + 60000 + 70000
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_avg() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE test_scores (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let scores = vec![
            r#"INSERT INTO test_scores (id, student, score) VALUES (1, 'Alice', 85)"#,
            r#"INSERT INTO test_scores (id, student, score) VALUES (2, 'Bob', 92)"#,
            r#"INSERT INTO test_scores (id, student, score) VALUES (3, 'Charlie', 78)"#,
        ];

        for sql in scores {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test AVG(score)
        let result = executor
            .execute("SELECT AVG(score) FROM test_scores")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "avg(score)");
            assert_eq!(rows.len(), 1);
            // Average of 85, 92, 78 = 255/3 = 85.0
            if let Value::Number(n) = &rows[0][0] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 85.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_min_max() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"#,
            r#"INSERT INTO users (id, name, age) VALUES (4, 'David', 28)"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test MIN(age)
        let result = executor
            .execute("SELECT MIN(age) FROM users")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "min(age)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(25.into()));
        } else {
            panic!("Expected SELECT result");
        }

        // Test MAX(age)
        let result = executor
            .execute("SELECT MAX(age) FROM users")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "max(age)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(35.into()));
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_with_where_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, salary, department) VALUES (1, 'Alice', 50000, 'Engineering')"#,
            r#"INSERT INTO employees (id, name, salary, department) VALUES (2, 'Bob', 60000, 'Engineering')"#,
            r#"INSERT INTO employees (id, name, salary, department) VALUES (3, 'Charlie', 45000, 'Sales')"#,
            r#"INSERT INTO employees (id, name, salary, department) VALUES (4, 'David', 55000, 'Engineering')"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test COUNT(*) with WHERE clause
        let result = executor
            .execute("SELECT COUNT(*) FROM employees WHERE department = 'Engineering'")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(3.into()));
        } else {
            panic!("Expected SELECT result");
        }

        // Test AVG(salary) with WHERE clause
        let result = executor
            .execute("SELECT AVG(salary) FROM employees WHERE department = 'Engineering'")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "avg(salary)");
            assert_eq!(rows.len(), 1);
            // Average of 50000, 60000, 55000 = 165000/3 = 55000.0
            if let Value::Number(n) = &rows[0][0] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 55000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_multiple_aggregations() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"#,
            r#"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"#,
            r#"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test multiple aggregations in one query
        let result = executor
            .execute("SELECT COUNT(*), MIN(age), MAX(age), AVG(age) FROM users")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 4);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(columns[1], "min(age)");
            assert_eq!(columns[2], "max(age)");
            assert_eq!(columns[3], "avg(age)");
            assert_eq!(rows.len(), 1);

            assert_eq!(rows[0][0], Value::Number(3.into())); // COUNT(*)
            assert_eq!(rows[0][1], Value::Number(25.into())); // MIN(age)
            assert_eq!(rows[0][2], Value::Number(35.into())); // MAX(age)

            // AVG(age) = (30 + 25 + 35) / 3 = 30.0
            if let Value::Number(n) = &rows[0][3] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 30.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_empty_table() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table but don't insert any data
        let result = executor
            .execute("CREATE TABLE empty_table (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Test COUNT(*) on empty table
        let result = executor
            .execute("SELECT COUNT(*) FROM empty_table")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "count(*)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Number(0.into()));
        } else {
            panic!("Expected SELECT result");
        }

        // Test SUM on empty table - should return NULL
        let result = executor
            .execute("SELECT SUM(value) FROM empty_table")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], "sum(value)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Null);
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_aggregation_string_columns() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE users (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let users = vec![
            r#"INSERT INTO users (id, name) VALUES (1, 'Alice')"#,
            r#"INSERT INTO users (id, name) VALUES (2, 'Bob')"#,
            r#"INSERT INTO users (id, name) VALUES (3, 'Charlie')"#,
        ];

        for sql in users {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test MIN/MAX on string columns
        let result = executor
            .execute("SELECT MIN(name), MAX(name) FROM users")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "min(name)");
            assert_eq!(columns[1], "max(name)");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("Alice".to_string())); // MIN alphabetically
            assert_eq!(rows[0][1], Value::String("Charlie".to_string())); // MAX alphabetically
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_basic() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 75000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 65000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test basic GROUP BY with COUNT
        let result = executor
            .execute("SELECT department, COUNT(*) FROM employees GROUP BY department")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "count(*)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            assert_eq!(sorted_rows[0][1], Value::Number(3.into()));
            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            assert_eq!(sorted_rows[1][1], Value::Number(2.into()));
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_avg() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Sales', 70000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with AVG
        let result = executor
            .execute("SELECT department, AVG(salary) FROM employees GROUP BY department")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "avg(salary)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            // Average of 70000, 80000 = 75000
            if let Value::Number(n) = &sorted_rows[0][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 75000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }

            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            // Average of 60000, 70000 = 65000
            if let Value::Number(n) = &sorted_rows[1][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 65000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_multiple_columns() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data with department and level
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (1, 'Alice', 'Engineering', 'Senior', 80000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (2, 'Bob', 'Engineering', 'Junior', 60000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (3, 'Charlie', 'Engineering', 'Senior', 85000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (4, 'David', 'Sales', 'Senior', 70000)"#,
            r#"INSERT INTO employees (id, name, department, level, salary) VALUES (5, 'Eve', 'Sales', 'Junior', 50000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with multiple columns
        let result = executor
            .execute("SELECT department, level, COUNT(*) FROM employees GROUP BY department, level")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "level");
            assert_eq!(columns[2], "count(*)");
            assert_eq!(rows.len(), 4); // 4 distinct department-level combinations
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_where() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (1, 'Alice', 'Engineering', 30, 70000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (2, 'Bob', 'Engineering', 25, 60000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (3, 'Charlie', 'Sales', 35, 65000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (4, 'David', 'Engineering', 28, 75000)"#,
            r#"INSERT INTO employees (id, name, department, age, salary) VALUES (5, 'Eve', 'Sales', 22, 50000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with WHERE clause
        let result = executor
            .execute(
                "SELECT department, COUNT(*) FROM employees WHERE age > 25 GROUP BY department",
            )
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "count(*)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            assert_eq!(sorted_rows[0][1], Value::Number(2.into())); // Alice(30) and David(28)
            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            assert_eq!(sorted_rows[1][1], Value::Number(1.into())); // Charlie(35)
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_having() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 75000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Sales', 50000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with HAVING clause
        let result = executor.execute("SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 60000").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "avg(salary)");
            assert_eq!(rows.len(), 1); // Only Engineering should match (avg = 75000)

            assert_eq!(rows[0][0], Value::String("Engineering".to_string()));
            if let Value::Number(n) = &rows[0][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 75000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_with_order_by_and_limit() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Marketing', 65000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 55000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with ORDER BY and LIMIT
        let result = executor.execute("SELECT department, AVG(salary) FROM employees GROUP BY department ORDER BY AVG(salary) DESC LIMIT 2").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "avg(salary)");
            assert_eq!(rows.len(), 2); // Limited to 2 results

            // Should be ordered by avg salary descending: Engineering (75000), Marketing (65000)
            assert_eq!(rows[0][0], Value::String("Engineering".to_string()));
            if let Value::Number(n) = &rows[0][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 75000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }

            assert_eq!(rows[1][0], Value::String("Marketing".to_string()));
            if let Value::Number(n) = &rows[1][1] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 65000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_group_by_multiple_aggregations() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 70000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 80000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Engineering', 60000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (4, 'David', 'Sales', 50000)"#,
            r#"INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 70000)"#,
        ];

        for sql in employees {
            let result = executor.execute(sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test GROUP BY with multiple aggregations
        let result = executor.execute("SELECT department, COUNT(*), MIN(salary), MAX(salary), AVG(salary) FROM employees GROUP BY department").await.unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns.len(), 5);
            assert_eq!(columns[0], "department");
            assert_eq!(columns[1], "count(*)");
            assert_eq!(columns[2], "min(salary)");
            assert_eq!(columns[3], "max(salary)");
            assert_eq!(columns[4], "avg(salary)");
            assert_eq!(rows.len(), 2);

            // Sort rows for consistent testing
            let mut sorted_rows = rows.clone();
            sorted_rows.sort_by(|a, b| a[0].as_str().unwrap().cmp(b[0].as_str().unwrap()));

            // Check Engineering department
            assert_eq!(sorted_rows[0][0], Value::String("Engineering".to_string()));
            assert_eq!(sorted_rows[0][1], Value::Number(3.into())); // COUNT
            assert_eq!(sorted_rows[0][2], Value::Number(60000.into())); // MIN
            assert_eq!(sorted_rows[0][3], Value::Number(80000.into())); // MAX
                                                                        // AVG = (70000 + 80000 + 60000) / 3 = 70000
            if let Value::Number(n) = &sorted_rows[0][4] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 70000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }

            // Check Sales department
            assert_eq!(sorted_rows[1][0], Value::String("Sales".to_string()));
            assert_eq!(sorted_rows[1][1], Value::Number(2.into())); // COUNT
            assert_eq!(sorted_rows[1][2], Value::Number(50000.into())); // MIN
            assert_eq!(sorted_rows[1][3], Value::Number(70000.into())); // MAX
                                                                        // AVG = (50000 + 70000) / 2 = 60000
            if let Value::Number(n) = &sorted_rows[1][4] {
                let avg = n.as_f64().unwrap();
                assert!((avg - 60000.0).abs() < 0.0001);
            } else {
                panic!("Expected numeric result for AVG");
            }
        } else {
            panic!("Expected SELECT result");
        }
    }

    #[tokio::test]
    async fn test_column_selection() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Insert some test data
        let employees = vec![
            r#"INSERT INTO employees (id, name, department, salary, age) VALUES (1, 'Alice', 'Engineering', 75000, 30)"#,
            r#"INSERT INTO employees (id, name, department, salary, age) VALUES (2, 'Bob', 'Sales', 65000, 28)"#,
            r#"INSERT INTO employees (id, name, department, salary, age) VALUES (3, 'Charlie', 'Engineering', 85000, 35)"#,
        ];

        for insert_sql in employees {
            let result = executor.execute(insert_sql).await.unwrap();
            assert!(matches!(result, QueryResult::Insert { count: 1 }));
        }

        // Test single column selection
        let result = executor
            .execute("SELECT name FROM employees")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 3);
            // Check that all expected names are present (order may vary)
            let names: Vec<String> = rows
                .iter()
                .map(|row| {
                    if let serde_json::Value::String(name) = &row[0] {
                        name.clone()
                    } else {
                        panic!("Expected string name");
                    }
                })
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Bob".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
        } else {
            panic!("Expected Select result");
        }

        // Test multiple column selection
        let result = executor
            .execute("SELECT name, department FROM employees")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name", "department"]);
            assert_eq!(rows.len(), 3);
            // Check that all expected combinations are present
            let results: Vec<(String, String)> = rows
                .iter()
                .map(|row| {
                    if let (serde_json::Value::String(name), serde_json::Value::String(dept)) =
                        (&row[0], &row[1])
                    {
                        (name.clone(), dept.clone())
                    } else {
                        panic!("Expected string values");
                    }
                })
                .collect();
            assert!(results.contains(&("Alice".to_string(), "Engineering".to_string())));
            assert!(results.contains(&("Bob".to_string(), "Sales".to_string())));
            assert!(results.contains(&("Charlie".to_string(), "Engineering".to_string())));
        } else {
            panic!("Expected Select result");
        }

        // Test column selection with WHERE clause
        let result = executor
            .execute("SELECT name, salary FROM employees WHERE department = 'Engineering'")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name", "salary"]);
            assert_eq!(rows.len(), 2);
            // Check that both engineering employees are present
            let results: Vec<(String, i64)> = rows
                .iter()
                .map(|row| {
                    if let (serde_json::Value::String(name), serde_json::Value::Number(salary)) =
                        (&row[0], &row[1])
                    {
                        (name.clone(), salary.as_i64().unwrap())
                    } else {
                        panic!("Expected string and number values");
                    }
                })
                .collect();
            assert!(results.contains(&("Alice".to_string(), 75000)));
            assert!(results.contains(&("Charlie".to_string(), 85000)));
        } else {
            panic!("Expected Select result");
        }

        // Test column selection with ORDER BY
        let result = executor
            .execute("SELECT name, salary FROM employees ORDER BY salary DESC")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name", "salary"]);
            assert_eq!(rows.len(), 3);
            // First row should be Charlie with highest salary (85000)
            if let (serde_json::Value::String(name), serde_json::Value::Number(salary)) =
                (&rows[0][0], &rows[0][1])
            {
                assert_eq!(name, "Charlie");
                assert_eq!(salary.as_i64().unwrap(), 85000);
            } else {
                panic!("Expected string and number values");
            }
        } else {
            panic!("Expected Select result");
        }

        // Test column selection with LIMIT
        let result = executor
            .execute("SELECT name FROM employees LIMIT 2")
            .await
            .unwrap();
        if let QueryResult::Select { columns, rows } = result {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select result");
        }

        // Test error case: non-existent column
        let result = executor
            .execute("SELECT nonexistent_column FROM employees")
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_group_by_error_cases() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine.clone());

        // Create a test table
        let result = executor
            .execute("CREATE TABLE employees (id VARCHAR PRIMARY KEY)")
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::CreateTable));

        // Test error: column not in GROUP BY
        let result = executor
            .execute("SELECT name, department, COUNT(*) FROM employees GROUP BY department")
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be in GROUP BY clause"));

        // Test error: SELECT * with GROUP BY
        let result = executor
            .execute("SELECT * FROM employees GROUP BY department")
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SELECT * with GROUP BY is not supported"));

        // Test success: columns without GROUP BY should now work
        let result = executor
            .execute("SELECT name, department FROM employees")
            .await;
        assert!(result.is_ok());
        if let Ok(QueryResult::Select { columns, rows: _ }) = result {
            assert_eq!(columns, vec!["name", "department"]);
        }
    }
}
