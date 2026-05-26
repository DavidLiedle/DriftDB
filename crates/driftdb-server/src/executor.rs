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

/// Order direction
#[derive(Debug, Clone)]
enum OrderDirection {
    Asc,
    Desc,
}

// The query-planner types below describe SELECTs for EXPLAIN. Execution itself
// goes through `crate::query::predicate` + `driftdb_core::sql_bridge`, so some
// of the fields here are populated for future plan-display features but not
// yet rendered. `dead_code` is allowed at the type level rather than per-field
// so the intent — "this is the structured description of a SELECT" — stays
// expressed in the type definition.

/// Order by specification
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct OrderBy {
    column: String,
    direction: OrderDirection,
}

/// Aggregation function types
#[derive(Debug, Clone, PartialEq)]
enum AggregationFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Aggregation specification
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Aggregation {
    function: AggregationFunction,
    column: Option<String>, // None for COUNT(*)
}

/// Group by specification
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct GroupBy {
    columns: Vec<String>,
}

/// Having clause specification (similar to WHERE but for groups)
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Having {
    conditions: Vec<(String, String, Value)>, // (function_expression, operator, value)
}

/// Select clause specification
#[allow(dead_code)]
#[derive(Debug, Clone)]
enum SelectClause {
    All,                                  // SELECT *
    AllDistinct,                          // SELECT DISTINCT *
    Columns(Vec<String>),                 // SELECT column1, column2, etc.
    ColumnsDistinct(Vec<String>),         // SELECT DISTINCT column1, column2, etc.
    Aggregations(Vec<Aggregation>),       // SELECT COUNT(*), SUM(column), etc.
    Mixed(Vec<String>, Vec<Aggregation>), // SELECT column1, column2, COUNT(*), SUM(column3)
}

/// JOIN types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

/// Temporal clause types for SQL:2011 temporal queries
#[derive(Debug, Clone)]
enum TemporalClause {
    AsOf(TemporalPoint),
    All,
}

/// A point in time for temporal queries
#[derive(Debug, Clone)]
enum TemporalPoint {
    Sequence(u64),
    Timestamp(String),
    CurrentTimestamp,
}

/// JOIN condition
#[derive(Debug, Clone)]
struct JoinCondition {
    left_table: String,
    left_column: String,
    right_table: String,
    right_column: String,
    operator: String, // "=", "!=", "<", ">", etc.
}

/// JOIN specification
#[derive(Debug, Clone)]
struct Join {
    join_type: JoinType,
    table: String,
    table_alias: Option<String>,
    condition: Option<JoinCondition>, // None for CROSS JOIN
}

/// Table reference with optional alias
#[derive(Debug, Clone)]
struct TableRef {
    name: String,
    alias: Option<String>,
}

/// FROM clause specification
#[derive(Debug, Clone)]
enum FromClause {
    Single(TableRef),
    MultipleImplicit(Vec<TableRef>), // Comma-separated tables for implicit JOIN
    WithJoins {
        base_table: TableRef,
        joins: Vec<Join>,
    },
    DerivedTable(DerivedTable), // Subquery used as table
    DerivedTableWithJoins {
        base_table: DerivedTable,
        joins: Vec<Join>,
    },
}

/// Subquery expression types
#[derive(Debug, Clone)]
pub struct Subquery {
    pub sql: String,
    pub is_correlated: bool,
    #[allow(dead_code)]
    pub referenced_columns: Vec<String>, // Columns from outer query referenced in subquery
}

/// Subquery expression in WHERE clauses
#[derive(Debug, Clone)]
pub enum SubqueryExpression {
    In {
        column: String,
        subquery: Subquery,
        negated: bool, // true for NOT IN
    },
    Exists {
        subquery: Subquery,
        negated: bool, // true for NOT EXISTS
    },
    Comparison {
        column: String,
        operator: String,                       // "=", ">", "<", etc.
        quantifier: Option<SubqueryQuantifier>, // ANY, ALL, or None for scalar
        subquery: Subquery,
    },
}

/// Quantifiers for subquery comparisons
#[derive(Debug, Clone, PartialEq)]
pub enum SubqueryQuantifier {
    Any,
    All,
}

/// Scalar subquery in SELECT clause
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ScalarSubquery {
    pub subquery: Subquery,
    pub alias: Option<String>,
}

/// Extended SELECT clause to support scalar subqueries
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ExtendedSelectItem {
    Column(String),
    Aggregation(Aggregation),
    ScalarSubquery(ScalarSubquery),
}

/// Derived table in FROM clause (subquery used as table)
#[derive(Debug, Clone)]
pub struct DerivedTable {
    pub subquery: Subquery,
    pub alias: String, // Required for derived tables
}

/// Enhanced WHERE condition to support subqueries
#[derive(Debug, Clone)]
pub enum WhereCondition {
    Simple {
        column: String,
        operator: String,
        value: Value,
    },
    Subquery(SubqueryExpression),
}

/// Set operation types
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperation {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

/// Set operation specification
#[derive(Debug, Clone)]
pub struct SetOperationQuery {
    pub left: String,  // Left SELECT query
    pub right: String, // Right SELECT query
    pub operation: SetOperation,
}

/// Query execution plan node types
#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan {
        table: String,
        filter: Option<String>,
        estimated_rows: usize,
    },
    #[allow(dead_code)]
    IndexScan {
        table: String,
        index: String,
        condition: String,
        estimated_rows: usize,
    },
    NestedLoop {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        condition: Option<String>,
        estimated_rows: usize,
    },
    #[allow(dead_code)]
    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        hash_keys: Vec<String>,
        estimated_rows: usize,
    },
    Sort {
        input: Box<PlanNode>,
        keys: Vec<String>,
        estimated_rows: usize,
    },
    Limit {
        input: Box<PlanNode>,
        count: usize,
        estimated_rows: usize,
    },
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<String>,
        estimated_rows: usize,
    },
    SetOperation {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        operation: SetOperation,
        estimated_rows: usize,
    },
    Distinct {
        input: Box<PlanNode>,
        columns: Vec<String>,
        estimated_rows: usize,
    },
    Subquery {
        #[allow(dead_code)]
        query: String,
        correlated: bool,
        estimated_rows: usize,
    },
}

/// Query execution plan
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub root: PlanNode,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
}

impl PlanNode {
    /// Get the estimated number of rows for this node
    fn get_estimated_rows(&self) -> usize {
        match self {
            PlanNode::SeqScan { estimated_rows, .. }
            | PlanNode::IndexScan { estimated_rows, .. }
            | PlanNode::NestedLoop { estimated_rows, .. }
            | PlanNode::HashJoin { estimated_rows, .. }
            | PlanNode::Sort { estimated_rows, .. }
            | PlanNode::Limit { estimated_rows, .. }
            | PlanNode::Aggregate { estimated_rows, .. }
            | PlanNode::SetOperation { estimated_rows, .. }
            | PlanNode::Distinct { estimated_rows, .. }
            | PlanNode::Subquery { estimated_rows, .. } => *estimated_rows,
        }
    }
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
    subquery_cache: Arc<Mutex<HashMap<String, QueryResult>>>, // Cache for non-correlated subqueries
    cte_tables: Arc<Mutex<HashMap<String, Vec<Value>>>>,      // CTE result cache for current query
    use_indexes: bool,                                        // Enable/disable index optimization
    prepared_statements: Arc<ParkingMutex<HashMap<String, PreparedStatement>>>, // Prepared statements cache
    session_id: String,                           // Session identifier for transaction tracking
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

    /// Parse WHERE clause into conditions
    fn parse_where_clause(&self, where_clause: &str) -> Result<Vec<(String, String, Value)>> {
        let mut conditions = Vec::new();

        // Split by AND (simple parser for now)
        let parts: Vec<&str> = where_clause.split(" AND ").collect();

        for part in parts {
            let trimmed = part.trim();

            // Parse column = value (support =, >, <, >=, <=, !=)
            let operators = ["!=", ">=", "<=", "=", ">", "<"];
            let mut found = false;

            for op in &operators {
                if let Some(op_pos) = trimmed.find(op) {
                    let column = trimmed[..op_pos].trim();
                    let value_str = trimmed[op_pos + op.len()..].trim();

                    // Parse value
                    let value = self.parse_sql_value(value_str)?;

                    conditions.push((column.to_string(), op.to_string(), value));
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(anyhow!("Invalid WHERE condition: {}", trimmed));
            }
        }

        Ok(conditions)
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

    /// Evaluate a CASE WHEN expression against a row map.
    /// Supports both searched form: CASE WHEN cond THEN val … [ELSE val] END
    /// and simple form:            CASE expr WHEN val THEN result … END
    fn evaluate_case_expression(
        &self,
        expr: &str,
        row: &serde_json::Map<String, Value>,
    ) -> Result<Value> {
        let trimmed = expr.trim();

        // Must start with CASE (case-insensitive)
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("CASE") {
            return Ok(Value::Null);
        }

        // Strip leading CASE and trailing END
        let inner = trimmed[4..].trim();
        let inner = if inner.to_uppercase().ends_with("END") {
            inner[..inner.len() - 3].trim()
        } else {
            inner
        };

        // Determine form: searched (starts with WHEN) vs. simple (has a subject expr)
        let inner_upper = inner.to_uppercase();
        if inner_upper.starts_with("WHEN") {
            // Searched CASE: CASE WHEN cond THEN val [WHEN cond THEN val]* [ELSE val] END
            self.evaluate_searched_case(inner, row)
        } else {
            // Simple CASE: CASE subject_expr WHEN val THEN result … [ELSE result] END
            self.evaluate_simple_case(inner, row)
        }
    }

    /// Evaluate searched CASE: WHEN cond THEN val … [ELSE val]
    fn evaluate_searched_case(
        &self,
        inner: &str,
        row: &serde_json::Map<String, Value>,
    ) -> Result<Value> {
        // Tokenise into WHEN/THEN/ELSE/END segments (case-insensitive)
        let mut rest = inner;
        loop {
            let upper = rest.to_uppercase();
            if upper.starts_with("WHEN") {
                let after_when = rest[4..].trim();
                // Find the matching THEN
                let then_pos = Self::find_keyword_pos(after_when, "THEN")?;
                let condition_str = after_when[..then_pos].trim();
                let after_then = after_when[then_pos + 4..].trim();

                // Find next WHEN, ELSE, or end
                let next_pos = Self::find_next_case_keyword(after_then);
                let then_value_str = after_then[..next_pos].trim();

                // Evaluate the condition
                if self.evaluate_case_condition(condition_str, row)? {
                    return self.evaluate_case_value(then_value_str, row);
                }
                rest = after_then[next_pos..].trim();
            } else if upper.starts_with("ELSE") {
                let else_value_str = rest[4..].trim();
                return self.evaluate_case_value(else_value_str, row);
            } else {
                break;
            }
        }
        Ok(Value::Null)
    }

    /// Evaluate simple CASE: subject WHEN val THEN result … [ELSE result]
    fn evaluate_simple_case(
        &self,
        inner: &str,
        row: &serde_json::Map<String, Value>,
    ) -> Result<Value> {
        // Split off subject from first WHEN
        let when_pos = Self::find_keyword_pos(inner, "WHEN")?;
        let subject_str = inner[..when_pos].trim();
        let subject_val = self.resolve_value_or_column(subject_str, row);

        let mut rest = inner[when_pos..].trim();
        loop {
            let upper = rest.to_uppercase();
            if upper.starts_with("WHEN") {
                let after_when = rest[4..].trim();
                let then_pos = Self::find_keyword_pos(after_when, "THEN")?;
                let compare_str = after_when[..then_pos].trim();
                let compare_val = self.parse_sql_value(compare_str)?;

                let after_then = after_when[then_pos + 4..].trim();
                let next_pos = Self::find_next_case_keyword(after_then);
                let result_str = after_then[..next_pos].trim();

                if subject_val == compare_val {
                    return self.evaluate_case_value(result_str, row);
                }
                rest = after_then[next_pos..].trim();
            } else if upper.starts_with("ELSE") {
                let else_value_str = rest[4..].trim();
                return self.evaluate_case_value(else_value_str, row);
            } else {
                break;
            }
        }
        Ok(Value::Null)
    }

    /// Find the byte position of a keyword (whole-word, case-insensitive) in `s`.
    fn find_keyword_pos(s: &str, keyword: &str) -> Result<usize> {
        let upper = s.to_uppercase();
        let kw_upper = keyword.to_uppercase();
        let mut depth = 0usize;
        let mut i = 0;
        while i + kw_upper.len() <= upper.len() {
            if upper[i..].starts_with("CASE") {
                depth += 1;
                i += 4;
                continue;
            }
            if upper[i..].starts_with("END") && depth > 0 {
                depth -= 1;
                i += 3;
                continue;
            }
            if depth == 0 && upper[i..].starts_with(kw_upper.as_str()) {
                // Verify word boundary after keyword
                let end = i + kw_upper.len();
                let after = upper.get(end..end + 1).unwrap_or(" ");
                if after == " " || after == "(" || end == upper.len() {
                    return Ok(i);
                }
            }
            i += 1;
        }
        Err(anyhow!("Keyword '{}' not found in CASE expression", keyword))
    }

    /// Find the position of the next top-level WHEN, ELSE, or end of string.
    fn find_next_case_keyword(s: &str) -> usize {
        let upper = s.to_uppercase();
        let mut depth = 0usize;
        let mut i = 0;
        while i < upper.len() {
            if upper[i..].starts_with("CASE") {
                depth += 1;
                i += 4;
                continue;
            }
            if upper[i..].starts_with("END") && depth > 0 {
                depth -= 1;
                i += 3;
                continue;
            }
            if depth == 0
                && (upper[i..].starts_with("WHEN ") || upper[i..].starts_with("ELSE"))
            {
                return i;
            }
            i += 1;
        }
        upper.len()
    }

    /// Evaluate a CASE condition string ("column op value" or boolean).
    fn evaluate_case_condition(
        &self,
        condition_str: &str,
        row: &serde_json::Map<String, Value>,
    ) -> Result<bool> {
        let s = condition_str.trim();
        // Try simple "col op val" patterns
        let operators = ["!=", "<>", ">=", "<=", "=", ">", "<"];
        for op in &operators {
            if let Some(op_pos) = s.find(op) {
                let lhs = s[..op_pos].trim();
                let rhs = s[op_pos + op.len()..].trim();
                let lhs_val = self.resolve_value_or_column(lhs, row);
                let rhs_val = self.parse_sql_value(rhs)?;
                let canonical_op = if *op == "<>" { "!=" } else { op };
                return Ok(driftdb_core::query::predicate::compare_values(
                    &lhs_val,
                    &rhs_val,
                    canonical_op,
                ));
            }
        }
        // IS NULL / IS NOT NULL
        let su = s.to_uppercase();
        if su.ends_with("IS NOT NULL") {
            let col = s[..s.len() - 11].trim();
            let val = self.resolve_value_or_column(col, row);
            return Ok(!val.is_null());
        }
        if su.ends_with("IS NULL") {
            let col = s[..s.len() - 7].trim();
            let val = self.resolve_value_or_column(col, row);
            return Ok(val.is_null());
        }
        Ok(false)
    }

    /// Evaluate a CASE result value (could be a literal, column ref, or nested CASE).
    fn evaluate_case_value(
        &self,
        value_str: &str,
        row: &serde_json::Map<String, Value>,
    ) -> Result<Value> {
        let s = value_str.trim();
        if s.to_uppercase().starts_with("CASE") {
            return self.evaluate_case_expression(s, row);
        }
        Ok(self.resolve_value_or_column(s, row))
    }

    /// Resolve a string as either a column reference or a SQL literal.
    fn resolve_value_or_column(&self, s: &str, row: &serde_json::Map<String, Value>) -> Value {
        let s = s.trim();
        // String literal
        if s.starts_with('\'') && s.ends_with('\'') {
            return Value::String(s[1..s.len() - 1].replace("''", "'"));
        }
        // Number literal
        if let Ok(n) = s.parse::<i64>() {
            return Value::Number(n.into());
        }
        if let Ok(f) = s.parse::<f64>() {
            if let Some(num) = serde_json::Number::from_f64(f) {
                return Value::Number(num);
            }
        }
        // Boolean literal
        if s.eq_ignore_ascii_case("TRUE") {
            return Value::Bool(true);
        }
        if s.eq_ignore_ascii_case("FALSE") {
            return Value::Bool(false);
        }
        if s.eq_ignore_ascii_case("NULL") {
            return Value::Null;
        }
        // Column reference: try exact match, then table.col prefix match
        if let Some(v) = row.get(s) {
            return v.clone();
        }
        for (key, val) in row {
            if key.ends_with(&format!(".{}", s)) || key == s {
                return val.clone();
            }
        }
        Value::Null
    }

    // -------------------------------------------------------------------------
    // CASE WHEN — select-clause pre-processing
    // -------------------------------------------------------------------------

    /// Split a SELECT column list by comma, respecting CASE…END and parentheses.
    fn split_select_cols(select_part: &str) -> Vec<String> {
        let mut items = Vec::new();
        let mut current = String::new();
        let mut depth_paren: usize = 0;
        let mut case_depth: usize = 0;
        let chars: Vec<char> = select_part.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let ch = chars[i];
            // Detect CASE keyword (word-boundary, case-insensitive)
            if depth_paren == 0 {
                let remaining: String = chars[i..].iter().collect();
                let ru = remaining.to_uppercase();
                if ru.starts_with("CASE")
                    && (chars.get(i + 4).map(|c| !c.is_alphanumeric() && *c != '_').unwrap_or(true))
                {
                    case_depth += 1;
                    current.push_str("CASE");
                    i += 4;
                    continue;
                }
                if ru.starts_with("END")
                    && case_depth > 0
                    && (chars.get(i + 3).map(|c| !c.is_alphanumeric() && *c != '_').unwrap_or(true))
                {
                    case_depth -= 1;
                    current.push_str("END");
                    i += 3;
                    continue;
                }
            }
            match ch {
                '(' => {
                    depth_paren += 1;
                    current.push(ch);
                }
                ')' => {
                    depth_paren = depth_paren.saturating_sub(1);
                    current.push(ch);
                }
                ',' if depth_paren == 0 && case_depth == 0 => {
                    items.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
            i += 1;
        }
        if !current.trim().is_empty() {
            items.push(current.trim().to_string());
        }
        items
    }

    /// Scan a SELECT clause for CASE WHEN expressions; return:
    /// - the modified clause (CASE expressions replaced by their alias/placeholder)
    /// - a map from alias -> original CASE expression
    fn extract_case_expressions(select_part: &str) -> (String, HashMap<String, String>) {
        let items = Self::split_select_cols(select_part);
        let mut case_map: HashMap<String, String> = HashMap::new();
        let mut new_items: Vec<String> = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            let item_upper = item.to_uppercase();
            if item_upper.trim_start().starts_with("CASE") {
                // Extract alias from "… AS alias" suffix
                let (alias, expr) = Self::extract_alias_from_case(item, idx);
                case_map.insert(alias.clone(), expr);
                new_items.push(alias);
            } else {
                new_items.push(item.clone());
            }
        }
        (new_items.join(", "), case_map)
    }

    /// Split a CASE expression item into (alias, full_expression).
    fn extract_alias_from_case(item: &str, idx: usize) -> (String, String) {
        // Find " AS " after the END keyword (case-insensitive)
        let upper = item.to_uppercase();
        // Search from the right for " AS " pattern after END
        if let Some(end_pos) = upper.rfind("END") {
            let after_end = &item[end_pos + 3..].trim().to_string();
            let after_upper = after_end.to_uppercase();
            if after_upper.starts_with("AS ") {
                let alias = after_end[3..].trim().to_string();
                let expr = item[..end_pos + 3].trim().to_string();
                return (alias, expr);
            }
        }
        // No alias — generate one
        let alias = format!("_case_{}", idx);
        (alias, item.trim().to_string())
    }

    // -------------------------------------------------------------------------
    // CTE (WITH clause) support
    // -------------------------------------------------------------------------

    /// Parse and execute CTEs from a query starting with WITH.
    /// Returns the main SELECT SQL (without the WITH prefix) after populating
    /// `self.cte_tables`.
    async fn extract_and_execute_ctes(&self, sql: &str) -> Result<String> {
        // Strip leading "WITH" keyword
        let sql = sql.trim();
        let after_with = if sql.to_uppercase().starts_with("WITH ") {
            sql[5..].trim()
        } else {
            return Ok(sql.to_string());
        };

        // We need to find the final SELECT that terminates the CTE block.
        // Strategy: scan through comma-separated CTE definitions looking for the
        // pattern:  cte_name AS ( ... ), cte2 AS ( ... ), ... SELECT ...
        let mut rest = after_with;
        let mut is_recursive = false;

        // Check for RECURSIVE keyword
        if rest.to_uppercase().starts_with("RECURSIVE ") {
            is_recursive = true;
            rest = rest[9..].trim();
        }

        loop {
            // Each iteration parses one CTE: name AS (body)
            let name_end = rest
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(rest.len());
            let cte_name = rest[..name_end].trim().to_string();
            if cte_name.is_empty() {
                break;
            }
            rest = rest[name_end..].trim();

            // Expect AS
            if !rest.to_uppercase().starts_with("AS") {
                break;
            }
            rest = rest[2..].trim();

            // Expect opening paren
            if !rest.starts_with('(') {
                break;
            }

            // Find the matching closing paren
            let body_start = 1;
            let mut depth = 1usize;
            let mut idx = body_start;
            let bytes = rest.as_bytes();
            while idx < bytes.len() && depth > 0 {
                match bytes[idx] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                if depth > 0 {
                    idx += 1;
                }
            }
            let cte_body = rest[body_start..idx].trim();
            rest = rest[idx + 1..].trim(); // skip the closing paren

            // Execute the CTE body
            let cte_result = if is_recursive {
                Box::pin(self.execute_recursive_cte(&cte_name, cte_body)).await?
            } else {
                Box::pin(self.execute(cte_body)).await?
            };

            // Convert QueryResult to Vec<Value> and store
            let rows: Vec<Value> = match cte_result {
                QueryResult::Select { columns, rows } => rows
                    .into_iter()
                    .map(|row| {
                        let mut map = serde_json::Map::new();
                        for (col, val) in columns.iter().zip(row) {
                            map.insert(col.clone(), val);
                        }
                        Value::Object(map)
                    })
                    .collect(),
                _ => vec![],
            };

            {
                let mut ctes = self.cte_tables.lock().await;
                ctes.insert(cte_name, rows);
            }

            // After the closing paren, there's either a comma (more CTEs) or the main SELECT
            if rest.starts_with(',') {
                rest = rest[1..].trim();
            } else {
                break;
            }
        }

        Ok(rest.to_string())
    }

    /// Execute a RECURSIVE CTE by iterating until fixed-point.
    async fn execute_recursive_cte(&self, cte_name: &str, body: &str) -> Result<QueryResult> {
        // Recursive CTEs have the form:
        //   anchor_term UNION [ALL] recursive_term
        // We execute the anchor first, then iteratively execute the recursive term
        // using the previous iteration's output as the CTE, until no new rows appear.

        let upper = body.to_uppercase();
        let union_pos = if let Some(p) = upper.find(" UNION ALL ") {
            Some((p, p + 11, true))
        } else {
            upper.find(" UNION ").map(|p| (p, p + 7, false))
        };

        if let Some((split_at, rest_start, _all)) = union_pos {
            let anchor_sql = body[..split_at].trim();
            let recursive_sql = body[rest_start..].trim();

            // Execute anchor
            let anchor_result = Box::pin(self.execute(anchor_sql)).await?;
            let (columns, mut accumulated) = match anchor_result {
                QueryResult::Select { columns, rows } => (columns, rows),
                _ => return Ok(QueryResult::Select { columns: vec![], rows: vec![] }),
            };

            // Convert accumulated to CTE format
            let to_value_rows = |rows: &[Vec<Value>]| -> Vec<Value> {
                rows.iter()
                    .map(|row| {
                        let mut map = serde_json::Map::new();
                        for (col, val) in columns.iter().zip(row.iter()) {
                            map.insert(col.clone(), val.clone());
                        }
                        Value::Object(map)
                    })
                    .collect()
            };

            // Iterate
            const MAX_ITERATIONS: usize = 1000;
            for _ in 0..MAX_ITERATIONS {
                {
                    let mut ctes = self.cte_tables.lock().await;
                    ctes.insert(cte_name.to_string(), to_value_rows(&accumulated));
                }

                let iter_result = Box::pin(self.execute(recursive_sql)).await;
                match iter_result {
                    Ok(QueryResult::Select { rows: new_rows, .. }) => {
                        if new_rows.is_empty() {
                            break;
                        }
                        let prev_len = accumulated.len();
                        accumulated.extend(new_rows);
                        if accumulated.len() == prev_len {
                            break; // Fixed point
                        }
                    }
                    _ => break,
                }
            }

            Ok(QueryResult::Select {
                columns,
                rows: accumulated,
            })
        } else {
            // No UNION — not really recursive, just execute body
            Box::pin(self.execute(body)).await
        }
    }

    // -------------------------------------------------------------------------
    // FK constraint helpers
    // -------------------------------------------------------------------------

    /// Parse REFERENCES clauses from a CREATE TABLE body and register FKs.
    /// Parse `col REFERENCES other_table(other_col)` patterns out of the
    /// raw column-defs body of a CREATE TABLE and register the resulting
    /// FKs in the canonical `core::fk` registry. INSERTs / UPDATEs /
    /// DELETEs through sql_bridge read from the same registry, so FKs
    /// declared via the server's PostgreSQL-protocol CREATE TABLE
    /// enforce uniformly regardless of which client wrote the data.
    fn register_fk_constraints(&self, table_name: &str, column_defs_str: &str) {
        let mut fks: Vec<driftdb_core::fk::ForeignKey> = Vec::new();
        for part in column_defs_str.split(',') {
            let part = part.trim();
            let upper = part.to_uppercase();
            let Some(ref_pos) = upper.find("REFERENCES") else {
                continue;
            };
            let col_name = part.split_whitespace().next().unwrap_or("").to_string();
            let after_ref = part[ref_pos + 10..].trim();
            let (Some(paren_open), Some(paren_close)) = (after_ref.find('('), after_ref.find(')'))
            else {
                continue;
            };
            let ref_table = after_ref[..paren_open].trim().to_string();
            let ref_col = after_ref[paren_open + 1..paren_close].trim().to_string();
            if !col_name.is_empty() && !ref_table.is_empty() && !ref_col.is_empty() {
                fks.push(driftdb_core::fk::ForeignKey {
                    column: col_name,
                    ref_table,
                    ref_column: ref_col,
                });
            }
        }
        if !fks.is_empty() {
            let count = fks.len();
            driftdb_core::fk::register(table_name, fks);
            info!(
                "Registered {} FK constraint(s) for table '{}'",
                count, table_name
            );
        }
    }

    // Note: validate_fk_insert / validate_fk_delete moved into
    // `driftdb_core::fk` so the same FK logic runs for every DML path.

    /// Parse GROUP BY clause
    fn parse_group_by_clause(&self, group_by_clause: &str) -> Result<GroupBy> {
        let columns: Vec<String> = group_by_clause
            .split(',')
            .map(|col| col.trim().to_string())
            .collect();

        if columns.is_empty() || columns.iter().any(|col| col.is_empty()) {
            return Err(anyhow!("Invalid GROUP BY clause: {}", group_by_clause));
        }

        Ok(GroupBy { columns })
    }

    /// Parse HAVING clause
    fn parse_having_clause(&self, having_clause: &str) -> Result<Having> {
        let mut conditions = Vec::new();

        // Split by AND (simple parser for now)
        let parts: Vec<&str> = having_clause.split(" AND ").collect();

        for part in parts {
            let trimmed = part.trim();

            // Parse aggregation function conditions like AVG(salary) > 50000
            let operators = ["!=", ">=", "<=", "=", ">", "<"];
            let mut found = false;

            for op in &operators {
                if let Some(op_pos) = trimmed.find(op) {
                    let function_expr = trimmed[..op_pos].trim();
                    let value_str = trimmed[op_pos + op.len()..].trim();

                    // Validate that left side is an aggregation function
                    if !self.is_aggregation_function(function_expr) {
                        return Err(anyhow!(
                            "HAVING clause must use aggregation functions: {}",
                            function_expr
                        ));
                    }

                    // Parse value
                    let value = self.parse_sql_value(value_str)?;

                    conditions.push((function_expr.to_string(), op.to_string(), value));
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(anyhow!("Invalid HAVING condition: {}", trimmed));
            }
        }

        Ok(Having { conditions })
    }

    /// Check if an expression is an aggregation function
    fn is_aggregation_function(&self, expr: &str) -> bool {
        let expr = expr.trim().to_uppercase();
        expr.starts_with("COUNT(")
            || expr.starts_with("SUM(")
            || expr.starts_with("AVG(")
            || expr.starts_with("MIN(")
            || expr.starts_with("MAX(")
    }

    /// Parse ORDER BY clause
    fn parse_order_by_clause(&self, order_by_clause: &str) -> Result<OrderBy> {
        let parts: Vec<&str> = order_by_clause.split_whitespace().collect();

        if parts.is_empty() {
            return Err(anyhow!("Empty ORDER BY clause"));
        }

        let column = parts[0].to_string();
        let direction = if parts.len() > 1 {
            match parts[1].to_uppercase().as_str() {
                "ASC" => OrderDirection::Asc,
                "DESC" => OrderDirection::Desc,
                _ => {
                    return Err(anyhow!(
                        "Invalid ORDER BY direction: {}. Use ASC or DESC",
                        parts[1]
                    ))
                }
            }
        } else {
            OrderDirection::Asc // Default to ascending
        };

        Ok(OrderBy { column, direction })
    }

    /// Parse LIMIT clause
    fn parse_limit_clause(&self, limit_clause: &str) -> Result<usize> {
        let trimmed = limit_clause.trim();
        trimmed
            .parse::<usize>()
            .map_err(|_| anyhow!("Invalid LIMIT value: {}", trimmed))
    }

    /// Parse SELECT clause to determine if it's SELECT *, aggregation functions, or mixed
    fn parse_select_clause(&self, select_part: &str) -> Result<SelectClause> {
        let trimmed = select_part.trim();

        // Check for DISTINCT
        let (is_distinct, columns_part) = if trimmed.to_uppercase().starts_with("DISTINCT ") {
            (true, trimmed[9..].trim())
        } else {
            (false, trimmed)
        };

        // Check for SELECT * or SELECT DISTINCT *
        if columns_part == "*" {
            return Ok(if is_distinct {
                SelectClause::AllDistinct
            } else {
                SelectClause::All
            });
        }

        // Parse columns and aggregation functions
        let mut columns = Vec::new();
        let mut aggregations = Vec::new();

        // Split by comma (simple parser for now)
        let parts: Vec<&str> = columns_part.split(',').collect();

        for part in parts {
            let part = part.trim();

            // Try to parse as aggregation function first
            if let Some(aggregation) = self.parse_aggregation_function(part)? {
                aggregations.push(aggregation);
            } else {
                // Treat as regular column
                columns.push(part.to_string());
            }
        }

        // DISTINCT cannot be used with aggregations
        if is_distinct && !aggregations.is_empty() {
            return Err(anyhow!(
                "DISTINCT cannot be used with aggregation functions"
            ));
        }

        // Determine the type of SELECT clause
        match (columns.is_empty(), aggregations.is_empty(), is_distinct) {
            (true, true, _) => Err(anyhow!("No valid columns or aggregation functions found")),
            (true, false, _) => Ok(SelectClause::Aggregations(aggregations)),
            (false, true, false) => {
                // Just regular columns - return Columns variant for column selection
                Ok(SelectClause::Columns(columns))
            }
            (false, true, true) => {
                // DISTINCT columns
                Ok(SelectClause::ColumnsDistinct(columns))
            }
            (false, false, _) => Ok(SelectClause::Mixed(columns, aggregations)),
        }
    }

    /// Parse a single aggregation function like COUNT(*), SUM(column), etc.
    fn parse_aggregation_function(&self, expr: &str) -> Result<Option<Aggregation>> {
        let expr = expr.trim();

        // Check for function call pattern: FUNCTION(argument)
        if !expr.contains('(') || !expr.ends_with(')') {
            return Ok(None);
        }

        let Some(paren_pos) = expr.find('(') else { return Ok(None); };
        let function_name = expr[..paren_pos].trim().to_uppercase();
        let argument = expr[paren_pos + 1..expr.len() - 1].trim();

        let function = match function_name.as_str() {
            "COUNT" => AggregationFunction::Count,
            "SUM" => AggregationFunction::Sum,
            "AVG" => AggregationFunction::Avg,
            "MIN" => AggregationFunction::Min,
            "MAX" => AggregationFunction::Max,
            _ => return Ok(None),
        };

        let column = if argument == "*" {
            // Only COUNT supports *
            if function != AggregationFunction::Count {
                return Err(anyhow!(
                    "{} function does not support * argument",
                    function_name
                ));
            }
            None
        } else {
            Some(argument.to_string())
        };

        Ok(Some(Aggregation { function, column }))
    }

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

        if lower.starts_with("show ") || lower.starts_with("explain ") || lower.starts_with("set ")
        {
            return self.execute_legacy(sql).await;
        }

        // Route CREATE INDEX and CREATE TABLE to dedicated handlers
        if lower.starts_with("create index") {
            return self.execute_create_index(sql).await;
        }
        if lower.starts_with("create table") {
            return self.execute_create_table(sql).await;
        }

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

    // Legacy execution path for backward compatibility
    async fn execute_legacy(&self, sql: &str) -> Result<QueryResult> {
        // Handle SHOW commands
        if sql.to_lowercase().starts_with("show ") {
            return self.execute_show(sql).await;
        }

        // Handle EXPLAIN commands
        if sql.to_lowercase().starts_with("explain ") {
            return self.execute_explain(sql).await;
        }

        // Handle SET commands (ignore for now)
        if sql.to_lowercase().starts_with("set ") {
            return Ok(QueryResult::Empty);
        }

        warn!("Unsupported SQL command: {}", sql);
        Err(anyhow!("Unsupported SQL command: {}", sql))
    }

    // Note: execute_insert / parse_values_list / execute_update / execute_delete
    // moved into `driftdb_core::sql_bridge`. Single canonical DML path now.


    async fn execute_create_table(&self, sql: &str) -> Result<QueryResult> {
        use driftdb_core::schema::ColumnDef;
        use sqlparser::ast::{ColumnOption, Statement, TableConstraint};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // Try standard SQL parsing first; fall back to legacy pk=id syntax if it fails
        let dialect = GenericDialect {};
        if let Ok(mut ast) = Parser::parse_sql(&dialect, sql) {
            if let Some(Statement::CreateTable(create)) = ast.pop() {
                let table_name = create.name.to_string();

                // Extract PRIMARY KEY: table-level constraint takes precedence
                let mut primary_key = "id".to_string();
                for constraint in &create.constraints {
                    if let TableConstraint::PrimaryKey { columns, .. } = constraint {
                        if let Some(col) = columns.first() {
                            primary_key = col.value.clone();
                            break;
                        }
                    }
                }
                // Column-level inline PRIMARY KEY (only if no table-level PK found yet)
                if primary_key == "id" {
                    'outer: for col in &create.columns {
                        for opt in &col.options {
                            if let ColumnOption::Unique { is_primary: true, .. } = opt.option {
                                primary_key = col.name.value.clone();
                                break 'outer;
                            }
                        }
                    }
                }

                // Build engine ColumnDef list
                let columns: Vec<ColumnDef> = create.columns.iter().map(|c| {
                    let indexed = c.options.iter().any(|o| matches!(o.option, ColumnOption::Unique { .. }));
                    ColumnDef {
                        name: c.name.value.clone(),
                        col_type: c.data_type.to_string(),
                        index: indexed,
                    }
                }).collect();

                let mut engine = self.engine_write()?;
                engine
                    .create_table_with_columns(&table_name, &primary_key, columns)
                    .map_err(|e| anyhow!("Create table failed: {}", e))?;
                drop(engine);

                // Register FK constraints if REFERENCES keyword present in SQL
                let lower = sql.to_lowercase();
                if lower.contains("references") {
                    if let (Some(open), Some(close)) = (sql.find('('), sql.rfind(')')) {
                        self.register_fk_constraints(&table_name, &sql[open + 1..close]);
                    }
                }

                info!("Table {} created (pk={})", table_name, primary_key);
                return Ok(QueryResult::CreateTable);
            }
        }

        // Legacy fallback: pk=id syntax (e.g. CREATE TABLE t (pk=id, INDEX(col)))
        self.execute_create_table_legacy(sql).await
    }

    async fn execute_create_table_legacy(&self, sql: &str) -> Result<QueryResult> {
        let lower = sql.to_lowercase();
        if !lower.starts_with("create table") {
            return Err(anyhow!("Not a CREATE TABLE statement"));
        }

        // Find table name
        let after_create = sql[12..].trim(); // Skip "CREATE TABLE"
        let table_end = after_create
            .find('(')
            .ok_or_else(|| anyhow!("Invalid CREATE TABLE syntax"))?;
        let table_name = after_create[..table_end].trim();

        // Extract the column definitions body (inside the outer parentheses)
        let paren_open = table_end;
        let col_body = {
            let s = &after_create[paren_open + 1..];
            let mut depth = 1usize;
            let mut end = 0;
            for (i, ch) in s.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            end = i;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            &s[..end]
        };

        // Parse pk=id or PRIMARY KEY syntax
        let mut primary_key = "id".to_string();
        for part in col_body.split(',') {
            let trimmed = part.trim();
            let trimmed_upper = trimmed.to_uppercase();
            // Legacy: pk=col_name
            if trimmed_upper.starts_with("PK=") || trimmed.to_lowercase().starts_with("pk=") {
                let pk_val = trimmed.split_once('=').map(|x| x.1).unwrap_or("id").trim();
                primary_key = pk_val.to_string();
                break;
            }
            // Standard: col_name ... PRIMARY KEY
            if trimmed_upper.contains("PRIMARY KEY") {
                let col_name = trimmed.split_whitespace().next().unwrap_or("id");
                primary_key = col_name.to_string();
                break;
            }
        }

        let mut engine = self.engine_write()?;
        engine
            .create_table(table_name, &primary_key, vec![])
            .map_err(|e| anyhow!("Create table failed: {}", e))?;
        drop(engine);

        // Register FK constraints if present
        self.register_fk_constraints(table_name, col_body);

        info!("Table {} created (pk={}, legacy syntax)", table_name, primary_key);
        Ok(QueryResult::CreateTable)
    }

    async fn execute_create_index(&self, sql: &str) -> Result<QueryResult> {
        // Handles: CREATE INDEX ON table_name (col1, col2)
        //          CREATE INDEX idx_name ON table_name (col)
        let lower = sql.to_lowercase();
        let on_pos = lower
            .find(" on ")
            .ok_or_else(|| anyhow!("CREATE INDEX missing ON clause"))?;
        let after_on = sql[on_pos + 4..].trim();
        let paren_pos = after_on
            .find('(')
            .ok_or_else(|| anyhow!("CREATE INDEX missing column list"))?;
        let table_name = after_on[..paren_pos].trim();
        let cols_str = after_on[paren_pos + 1..]
            .trim_end_matches([')', ';'])
            .trim();

        let mut engine = self.engine_write()?;
        for col in cols_str.split(',') {
            let col = col.trim();
            if !col.is_empty() {
                engine
                    .create_index(table_name, col, None)
                    .map_err(|e| anyhow!("CREATE INDEX failed: {}", e))?;
            }
        }

        info!("Index created on {}", table_name);
        Ok(QueryResult::CreateIndex)
    }

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

    /// Execute EXPLAIN command to show query plan
    async fn execute_explain(&self, sql: &str) -> Result<QueryResult> {
        // Remove "EXPLAIN " prefix
        let query = sql[8..].trim();

        // Check for ANALYZE option
        let (analyze, actual_query) = if query.to_lowercase().starts_with("analyze ") {
            (true, query[8..].trim())
        } else {
            (false, query)
        };

        // Generate query plan
        let plan = self.generate_query_plan(actual_query).await?;

        // Format plan as readable output
        let plan_text = self.format_query_plan(&plan, 0);

        // If ANALYZE is requested, also execute the query and add timing info
        let output = if analyze {
            let start = std::time::Instant::now();
            let _result = Box::pin(self.execute(actual_query)).await?;
            let elapsed = start.elapsed();

            format!(
                "{}\n\nExecution Time: {:.3} ms",
                plan_text,
                elapsed.as_secs_f64() * 1000.0
            )
        } else {
            plan_text
        };

        // Return as a single-column result
        Ok(QueryResult::Select {
            columns: vec!["QUERY PLAN".to_string()],
            rows: output
                .lines()
                .map(|line| vec![Value::String(line.to_string())])
                .collect(),
        })
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

    /// Generate a query execution plan
    async fn generate_query_plan(&self, sql: &str) -> Result<QueryPlan> {
        let lower = sql.to_lowercase();

        // Check for set operations first
        if let Some(set_op) = self.parse_set_operation(sql)? {
            return self.generate_set_operation_plan(&set_op).await;
        }

        // Handle SELECT queries
        if lower.starts_with("select") {
            self.generate_select_plan(sql).await
        } else if lower.starts_with("insert") {
            Ok(QueryPlan {
                root: PlanNode::SeqScan {
                    table: "INSERT operation".to_string(),
                    filter: None,
                    estimated_rows: 1,
                },
                estimated_cost: 1.0,
                estimated_rows: 1,
            })
        } else if lower.starts_with("update") {
            self.generate_update_plan(sql).await
        } else if lower.starts_with("delete") {
            self.generate_delete_plan(sql).await
        } else {
            Err(anyhow!("Cannot generate plan for this query type"))
        }
    }

    /// Generate plan for SELECT queries
    async fn generate_select_plan(&self, sql: &str) -> Result<QueryPlan> {
        let lower = sql.to_lowercase();

        // Parse the query components
        let select_start = if lower.starts_with("select ") { 7 } else { 0 };
        let from_pos = lower
            .find(" from ")
            .ok_or_else(|| anyhow!("Missing FROM clause"))?;
        let select_part = &sql[select_start..from_pos];
        let select_clause = self.parse_select_clause(select_part)?;

        // Parse FROM clause
        let from_pos = lower.find(" from ").unwrap() + 6;
        let remaining = &sql[from_pos..];
        let from_end = remaining
            .find(" WHERE ")
            .or_else(|| remaining.find(" where "))
            .or_else(|| remaining.find(" GROUP BY "))
            .or_else(|| remaining.find(" group by "))
            .or_else(|| remaining.find(" ORDER BY "))
            .or_else(|| remaining.find(" order by "))
            .or_else(|| remaining.find(" LIMIT "))
            .or_else(|| remaining.find(" limit "))
            .unwrap_or(remaining.len());

        let from_part = remaining[..from_end].trim();
        let from_clause = self.parse_from_clause(from_part)?;

        // Build plan nodes bottom-up
        let mut root = self.generate_from_plan(&from_clause).await?;

        // Add WHERE filter if present
        let after_from = &remaining[from_end..];
        if let Some(where_pos) = after_from.to_lowercase().find(" where ") {
            let where_start = where_pos + 7;
            let where_clause = &after_from[where_start..];
            let where_end = where_clause
                .to_lowercase()
                .find(" group by ")
                .or_else(|| where_clause.to_lowercase().find(" order by "))
                .or_else(|| where_clause.to_lowercase().find(" limit "))
                .unwrap_or(where_clause.len());
            let conditions_str = where_clause[..where_end].trim();

            // Update the root node with filter information
            if let PlanNode::SeqScan { ref mut filter, .. } = root {
                *filter = Some(conditions_str.to_string());
            }
        }

        // Add DISTINCT if needed
        match &select_clause {
            SelectClause::AllDistinct | SelectClause::ColumnsDistinct(_) => {
                let columns = match &select_clause {
                    SelectClause::AllDistinct => vec!["*".to_string()],
                    SelectClause::ColumnsDistinct(cols) => cols.clone(),
                    _ => vec![],
                };
                root = PlanNode::Distinct {
                    input: Box::new(root.clone()),
                    columns,
                    estimated_rows: root.get_estimated_rows() / 2, // Rough estimate
                };
            }
            _ => {}
        }

        // Add GROUP BY if present
        if let Some(group_pos) = after_from.to_lowercase().find(" group by ") {
            let group_start = group_pos + 10;
            let group_clause = &after_from[group_start..];
            let group_end = group_clause
                .to_lowercase()
                .find(" having ")
                .or_else(|| group_clause.to_lowercase().find(" order by "))
                .or_else(|| group_clause.to_lowercase().find(" limit "))
                .unwrap_or(group_clause.len());
            let group_str = group_clause[..group_end].trim();

            let aggregates = match &select_clause {
                SelectClause::Aggregations(aggs) => {
                    aggs.iter().map(|a| format!("{:?}", a.function)).collect()
                }
                SelectClause::Mixed(_, aggs) => {
                    aggs.iter().map(|a| format!("{:?}", a.function)).collect()
                }
                _ => vec![],
            };

            root = PlanNode::Aggregate {
                input: Box::new(root.clone()),
                group_by: group_str.split(',').map(|s| s.trim().to_string()).collect(),
                aggregates,
                estimated_rows: root.get_estimated_rows() / 10, // Rough estimate
            };
        }

        // Add ORDER BY if present
        if let Some(order_pos) = after_from.to_lowercase().find(" order by ") {
            let order_start = order_pos + 10;
            let order_clause = &after_from[order_start..];
            let order_end = order_clause
                .to_lowercase()
                .find(" limit ")
                .unwrap_or(order_clause.len());
            let order_str = order_clause[..order_end].trim();

            root = PlanNode::Sort {
                input: Box::new(root.clone()),
                keys: vec![order_str.to_string()],
                estimated_rows: root.get_estimated_rows(),
            };
        }

        // Add LIMIT if present
        if let Some(limit_pos) = after_from.to_lowercase().find(" limit ") {
            let limit_start = limit_pos + 7;
            let limit_clause = &after_from[limit_start..];
            let limit_end = limit_clause.find(' ').unwrap_or(limit_clause.len());
            let limit_str = limit_clause[..limit_end].trim();

            if let Ok(limit_count) = limit_str.parse::<usize>() {
                root = PlanNode::Limit {
                    input: Box::new(root.clone()),
                    count: limit_count,
                    estimated_rows: limit_count.min(root.get_estimated_rows()),
                };
            }
        }

        let estimated_rows = root.get_estimated_rows();
        Ok(QueryPlan {
            root,
            estimated_cost: estimated_rows as f64 * 0.01, // Simple cost model
            estimated_rows,
        })
    }

    /// Generate plan for FROM clause
    async fn generate_from_plan(&self, from_clause: &FromClause) -> Result<PlanNode> {
        match from_clause {
            FromClause::Single(table_ref) => {
                // Check if we can use an index
                if self.use_indexes {
                    if let Ok(engine) = self.engine_read() {
                        let indexed_columns = engine.get_indexed_columns(&table_ref.name);
                        // For now, just note if indexes exist
                        // In a real optimizer, we'd check if WHERE conditions use these columns
                        if !indexed_columns.is_empty() {
                            debug!(
                                "Table {} has indexes on: {:?}",
                                table_ref.name, indexed_columns
                            );
                        }
                    }
                }

                Ok(PlanNode::SeqScan {
                    table: table_ref.name.clone(),
                    filter: None,
                    estimated_rows: 1000, // Default estimate
                })
            }
            FromClause::WithJoins { base_table, joins } => {
                let mut left = PlanNode::SeqScan {
                    table: base_table.name.clone(),
                    filter: None,
                    estimated_rows: 1000,
                };

                for join in joins {
                    let right = PlanNode::SeqScan {
                        table: join.table.clone(),
                        filter: None,
                        estimated_rows: 1000,
                    };

                    let condition = join.condition.as_ref().map(|c| {
                        format!(
                            "{}.{} {} {}.{}",
                            c.left_table, c.left_column, c.operator, c.right_table, c.right_column
                        )
                    });

                    left = PlanNode::NestedLoop {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type: join.join_type.clone(),
                        condition,
                        estimated_rows: 1000, // Should calculate based on join selectivity
                    };
                }

                Ok(left)
            }
            FromClause::DerivedTable(dt) => Ok(PlanNode::Subquery {
                query: dt.subquery.sql.clone(),
                correlated: dt.subquery.is_correlated,
                estimated_rows: 100,
            }),
            _ => Ok(PlanNode::SeqScan {
                table: "complex_from".to_string(),
                filter: None,
                estimated_rows: 1000,
            }),
        }
    }

    /// Generate plan for set operations
    async fn generate_set_operation_plan(&self, set_op: &SetOperationQuery) -> Result<QueryPlan> {
        let left_plan = Box::pin(self.generate_query_plan(&set_op.left)).await?;
        let right_plan = Box::pin(self.generate_query_plan(&set_op.right)).await?;

        let estimated_rows = match &set_op.operation {
            SetOperation::Union | SetOperation::UnionAll => {
                left_plan.estimated_rows + right_plan.estimated_rows
            }
            SetOperation::Intersect | SetOperation::IntersectAll => {
                left_plan.estimated_rows.min(right_plan.estimated_rows)
            }
            SetOperation::Except | SetOperation::ExceptAll => left_plan.estimated_rows,
        };

        Ok(QueryPlan {
            root: PlanNode::SetOperation {
                left: Box::new(left_plan.root),
                right: Box::new(right_plan.root),
                operation: set_op.operation.clone(),
                estimated_rows,
            },
            estimated_cost: (left_plan.estimated_cost + right_plan.estimated_cost) * 1.1,
            estimated_rows,
        })
    }

    /// Generate plan for UPDATE queries
    async fn generate_update_plan(&self, _sql: &str) -> Result<QueryPlan> {
        Ok(QueryPlan {
            root: PlanNode::SeqScan {
                table: "UPDATE target".to_string(),
                filter: Some("WHERE conditions".to_string()),
                estimated_rows: 100,
            },
            estimated_cost: 10.0,
            estimated_rows: 100,
        })
    }

    /// Generate plan for DELETE queries
    async fn generate_delete_plan(&self, _sql: &str) -> Result<QueryPlan> {
        Ok(QueryPlan {
            root: PlanNode::SeqScan {
                table: "DELETE target".to_string(),
                filter: Some("WHERE conditions".to_string()),
                estimated_rows: 100,
            },
            estimated_cost: 10.0,
            estimated_rows: 100,
        })
    }

    /// Format query plan as text
    fn format_query_plan(&self, plan: &QueryPlan, _indent: usize) -> String {
        let mut output = Vec::new();
        output.push(format!(
            "Query Plan (estimated cost: {:.2}, rows: {})",
            plan.estimated_cost, plan.estimated_rows
        ));
        output.push("-".repeat(60));
        self.format_plan_node(&plan.root, &mut output, "", true);
        output.join("\n")
    }

    /// Format a plan node recursively
    #[allow(clippy::only_used_in_recursion)]
    fn format_plan_node(
        &self,
        node: &PlanNode,
        output: &mut Vec<String>,
        prefix: &str,
        is_last: bool,
    ) {
        let connector = if is_last { "└── " } else { "├── " };
        let node_desc = match node {
            PlanNode::SeqScan {
                table,
                filter,
                estimated_rows,
            } => {
                if let Some(f) = filter {
                    format!(
                        "Seq Scan on {} (filter: {}) [rows: {}]",
                        table, f, estimated_rows
                    )
                } else {
                    format!("Seq Scan on {} [rows: {}]", table, estimated_rows)
                }
            }
            PlanNode::IndexScan {
                table,
                index,
                condition,
                estimated_rows,
            } => {
                format!(
                    "Index Scan on {} using {} ({}), [rows: {}]",
                    table, index, condition, estimated_rows
                )
            }
            PlanNode::NestedLoop {
                join_type,
                condition,
                estimated_rows,
                ..
            } => {
                let cond_str = condition
                    .as_ref()
                    .map(|c| format!(" on {}", c))
                    .unwrap_or_default();
                format!(
                    "Nested Loop {:?} Join{} [rows: {}]",
                    join_type, cond_str, estimated_rows
                )
            }
            PlanNode::HashJoin {
                join_type,
                hash_keys,
                estimated_rows,
                ..
            } => {
                format!(
                    "Hash {:?} Join on ({}) [rows: {}]",
                    join_type,
                    hash_keys.join(", "),
                    estimated_rows
                )
            }
            PlanNode::Sort {
                keys,
                estimated_rows,
                ..
            } => {
                format!("Sort by {} [rows: {}]", keys.join(", "), estimated_rows)
            }
            PlanNode::Limit {
                count,
                estimated_rows,
                ..
            } => {
                format!("Limit {} [rows: {}]", count, estimated_rows)
            }
            PlanNode::Aggregate {
                group_by,
                aggregates,
                estimated_rows,
                ..
            } => {
                format!(
                    "Aggregate ({}) group by {} [rows: {}]",
                    aggregates.join(", "),
                    group_by.join(", "),
                    estimated_rows
                )
            }
            PlanNode::SetOperation {
                operation,
                estimated_rows,
                ..
            } => {
                format!("{:?} [rows: {}]", operation, estimated_rows)
            }
            PlanNode::Distinct {
                columns,
                estimated_rows,
                ..
            } => {
                format!(
                    "Distinct on {} [rows: {}]",
                    columns.join(", "),
                    estimated_rows
                )
            }
            PlanNode::Subquery {
                correlated,
                estimated_rows,
                ..
            } => {
                let corr_str = if *correlated { "Correlated " } else { "" };
                format!("{}Subquery [rows: {}]", corr_str, estimated_rows)
            }
        };

        output.push(format!("{}{}{}", prefix, connector, node_desc));

        let new_prefix = if is_last {
            format!("{}    ", prefix)
        } else {
            format!("{}│   ", prefix)
        };

        // Recursively format children
        let children: Vec<&PlanNode> = match node {
            PlanNode::NestedLoop { left, right, .. }
            | PlanNode::HashJoin { left, right, .. }
            | PlanNode::SetOperation { left, right, .. } => vec![left, right],
            PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. } => vec![input],
            _ => vec![],
        };

        for (i, child) in children.iter().enumerate() {
            let is_last_child = i == children.len() - 1;
            self.format_plan_node(child, output, &new_prefix, is_last_child);
        }
    }

    /// Parse FROM clause to extract table references and JOIN operations
    fn parse_from_clause(&self, from_part: &str) -> Result<FromClause> {
        let from_trimmed = from_part.trim();
        let lower = from_trimmed.to_lowercase();

        // Check for derived table (starts with parenthesized SELECT)
        if let Some(derived_table) = self.parse_derived_table(from_trimmed)? {
            // Check if derived table has JOINs
            if lower.contains(" join ")
                || lower.contains(" inner join ")
                || lower.contains(" left join ")
                || lower.contains(" left outer join ")
                || lower.contains(" right join ")
                || lower.contains(" right outer join ")
                || lower.contains(" full join ")
                || lower.contains(" full outer join ")
                || lower.contains(" cross join ")
            {
                self.parse_derived_table_with_joins(from_trimmed)
            } else {
                Ok(FromClause::DerivedTable(derived_table))
            }
        }
        // Check for JOIN keywords
        else if lower.contains(" join ")
            || lower.contains(" inner join ")
            || lower.contains(" left join ")
            || lower.contains(" left outer join ")
            || lower.contains(" right join ")
            || lower.contains(" right outer join ")
            || lower.contains(" full join ")
            || lower.contains(" full outer join ")
            || lower.contains(" cross join ")
        {
            self.parse_explicit_joins(from_trimmed)
        } else if from_trimmed.contains(',') {
            // Comma-separated tables (implicit JOIN)
            self.parse_implicit_joins(from_trimmed)
        } else {
            // Single table
            self.parse_single_table(from_trimmed)
        }
    }

    /// Parse single table reference
    fn parse_single_table(&self, table_part: &str) -> Result<FromClause> {
        let parts: Vec<&str> = table_part.split_whitespace().collect();

        if parts.is_empty() {
            return Err(anyhow!("Empty table specification"));
        }

        let table_name = parts[0].to_string();

        // Check for alias
        let table_alias = if parts.len() >= 2 {
            // Handle both "table alias" and "table AS alias" forms
            if parts.len() >= 3 && parts[1].to_lowercase() == "as" {
                Some(parts[2].to_string())
            } else {
                Some(parts[1].to_string())
            }
        } else {
            None
        };

        Ok(FromClause::Single(TableRef {
            name: table_name,
            alias: table_alias,
        }))
    }

    /// Parse comma-separated tables for implicit JOIN
    fn parse_implicit_joins(&self, from_part: &str) -> Result<FromClause> {
        let table_parts: Vec<&str> = from_part.split(',').collect();
        let mut tables = Vec::new();

        for part in table_parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let table_parts: Vec<&str> = part.split_whitespace().collect();
            let table_name = table_parts[0].to_string();

            let table_alias = if table_parts.len() >= 2 {
                if table_parts.len() >= 3 && table_parts[1].to_lowercase() == "as" {
                    Some(table_parts[2].to_string())
                } else {
                    Some(table_parts[1].to_string())
                }
            } else {
                None
            };

            tables.push(TableRef {
                name: table_name,
                alias: table_alias,
            });
        }

        if tables.is_empty() {
            return Err(anyhow!("No tables specified"));
        }

        Ok(FromClause::MultipleImplicit(tables))
    }

    /// Parse explicit JOIN syntax
    fn parse_explicit_joins(&self, from_part: &str) -> Result<FromClause> {
        let lower = from_part.to_lowercase();

        // Find the base table (before first JOIN)
        let first_join_pos = lower
            .find(" join ")
            .or_else(|| lower.find(" inner join "))
            .or_else(|| lower.find(" left join "))
            .or_else(|| lower.find(" left outer join "))
            .or_else(|| lower.find(" right join "))
            .or_else(|| lower.find(" right outer join "))
            .or_else(|| lower.find(" full join "))
            .or_else(|| lower.find(" full outer join "))
            .or_else(|| lower.find(" cross join "))
            .unwrap_or(from_part.len());

        let base_table_part = from_part[..first_join_pos].trim();
        let joins_part = if first_join_pos < from_part.len() {
            from_part[first_join_pos..].trim()
        } else {
            ""
        };

        // Parse base table
        let base_table = match self.parse_single_table(base_table_part)? {
            FromClause::Single(table_ref) => table_ref,
            _ => return Err(anyhow!("Invalid base table")),
        };

        // Parse JOINs
        let joins = if joins_part.is_empty() {
            Vec::new()
        } else {
            self.parse_joins(joins_part)?
        };

        Ok(FromClause::WithJoins { base_table, joins })
    }

    /// Parse individual JOIN clauses
    fn parse_joins(&self, joins_part: &str) -> Result<Vec<Join>> {
        let mut joins = Vec::new();
        let mut remaining = joins_part;

        while !remaining.is_empty() {
            remaining = remaining.trim();

            // Determine JOIN type
            let (join_type, after_join_type) =
                if remaining.to_lowercase().starts_with("inner join ") {
                    (JoinType::Inner, &remaining[11..])
                } else if remaining.to_lowercase().starts_with("left outer join ") {
                    (JoinType::LeftOuter, &remaining[16..])
                } else if remaining.to_lowercase().starts_with("left join ") {
                    (JoinType::LeftOuter, &remaining[10..])
                } else if remaining.to_lowercase().starts_with("right outer join ") {
                    (JoinType::RightOuter, &remaining[17..])
                } else if remaining.to_lowercase().starts_with("right join ") {
                    (JoinType::RightOuter, &remaining[11..])
                } else if remaining.to_lowercase().starts_with("full outer join ") {
                    (JoinType::FullOuter, &remaining[16..])
                } else if remaining.to_lowercase().starts_with("full join ") {
                    (JoinType::FullOuter, &remaining[10..])
                } else if remaining.to_lowercase().starts_with("cross join ") {
                    (JoinType::Cross, &remaining[11..])
                } else if remaining.to_lowercase().starts_with("join ") {
                    (JoinType::Inner, &remaining[5..])
                } else {
                    break; // No more JOINs
                };

            // Find the end of this JOIN clause (next JOIN or end of string)
            let next_join_pos = after_join_type
                .to_lowercase()
                .find(" join ")
                .or_else(|| after_join_type.to_lowercase().find(" inner join "))
                .or_else(|| after_join_type.to_lowercase().find(" left join "))
                .or_else(|| after_join_type.to_lowercase().find(" left outer join "))
                .or_else(|| after_join_type.to_lowercase().find(" right join "))
                .or_else(|| after_join_type.to_lowercase().find(" right outer join "))
                .or_else(|| after_join_type.to_lowercase().find(" full join "))
                .or_else(|| after_join_type.to_lowercase().find(" full outer join "))
                .or_else(|| after_join_type.to_lowercase().find(" cross join "));

            let (join_clause, next_remaining) = if let Some(pos) = next_join_pos {
                (&after_join_type[..pos], &after_join_type[pos..])
            } else {
                (after_join_type, "")
            };

            // Parse this JOIN
            let join = self.parse_single_join(join_type, join_clause)?;
            joins.push(join);

            remaining = next_remaining;
        }

        Ok(joins)
    }

    /// Parse a single JOIN clause
    fn parse_single_join(&self, join_type: JoinType, join_clause: &str) -> Result<Join> {
        let join_clause = join_clause.trim();

        // For CROSS JOIN, there's no ON clause
        if join_type == JoinType::Cross {
            // Parse table name and alias
            let parts: Vec<&str> = join_clause.split_whitespace().collect();
            if parts.is_empty() {
                return Err(anyhow!("Missing table name in CROSS JOIN"));
            }

            let table_name = parts[0].to_string();
            let table_alias = if parts.len() >= 2 {
                if parts.len() >= 3 && parts[1].to_lowercase() == "as" {
                    Some(parts[2].to_string())
                } else {
                    Some(parts[1].to_string())
                }
            } else {
                None
            };

            return Ok(Join {
                join_type,
                table: table_name,
                table_alias,
                condition: None,
            });
        }

        // Find ON clause
        let on_pos = join_clause.to_lowercase().find(" on ")
            .ok_or_else(|| anyhow!("Missing ON clause in JOIN"))?;
        let table_part = join_clause[..on_pos].trim();
        let on_part = join_clause[on_pos + 4..].trim(); // Skip " on "

        // Parse table name and alias
        let parts: Vec<&str> = table_part.split_whitespace().collect();
        if parts.is_empty() {
            return Err(anyhow!("Missing table name in JOIN"));
        }

        let table_name = parts[0].to_string();
        let table_alias = if parts.len() >= 2 {
            if parts.len() >= 3 && parts[1].to_lowercase() == "as" {
                Some(parts[2].to_string())
            } else {
                Some(parts[1].to_string())
            }
        } else {
            None
        };

        // Parse ON condition
        let condition = self.parse_join_condition(on_part)?;

        Ok(Join {
            join_type,
            table: table_name,
            table_alias,
            condition: Some(condition),
        })
    }

    /// Parse JOIN condition (e.g., "t1.id = t2.user_id")
    fn parse_join_condition(&self, on_clause: &str) -> Result<JoinCondition> {
        let on_clause = on_clause.trim();

        // Simple condition parsing - supports =, !=, <, >, <=, >=
        let operators = ["!=", "<=", ">=", "=", "<", ">"];

        for op in &operators {
            if let Some(op_pos) = on_clause.find(op) {
                let left_part = on_clause[..op_pos].trim();
                let right_part = on_clause[op_pos + op.len()..].trim();

                // Parse left side (table.column)
                let (left_table, left_column) = self.parse_column_reference(left_part)?;

                // Parse right side (table.column)
                let (right_table, right_column) = self.parse_column_reference(right_part)?;

                return Ok(JoinCondition {
                    left_table,
                    left_column,
                    right_table,
                    right_column,
                    operator: op.to_string(),
                });
            }
        }

        Err(anyhow!("Invalid JOIN condition: {}", on_clause))
    }

    /// Parse column reference (table.column or just column)
    fn parse_column_reference(&self, column_ref: &str) -> Result<(String, String)> {
        let column_ref = column_ref.trim();

        if let Some(dot_pos) = column_ref.find('.') {
            let table_part = column_ref[..dot_pos].trim();
            let column_part = column_ref[dot_pos + 1..].trim();

            if table_part.is_empty() || column_part.is_empty() {
                return Err(anyhow!("Invalid column reference: {}", column_ref));
            }

            Ok((table_part.to_string(), column_part.to_string()))
        } else {
            // No table specified - we'll need to infer it during execution
            Ok(("".to_string(), column_ref.to_string()))
        }
    }

    /// Parse enhanced WHERE clause with subquery support
    fn parse_enhanced_where_clause(&self, where_clause: &str) -> Result<Vec<WhereCondition>> {
        let mut conditions = Vec::new();

        // Split by AND (simple parser for now - could be enhanced for OR support)
        let parts: Vec<&str> = where_clause.split(" AND ").collect();

        for part in parts {
            let trimmed = part.trim();

            // Check for subquery patterns
            if let Some(condition) = self.try_parse_subquery_condition(trimmed)? {
                conditions.push(WhereCondition::Subquery(condition));
            } else {
                // Fall back to simple condition parsing
                let simple_condition = self.parse_simple_condition(trimmed)?;
                conditions.push(WhereCondition::Simple {
                    column: simple_condition.0,
                    operator: simple_condition.1,
                    value: simple_condition.2,
                });
            }
        }

        Ok(conditions)
    }

    /// Try to parse a subquery condition (IN, EXISTS, ANY/ALL comparisons)
    fn try_parse_subquery_condition(&self, condition: &str) -> Result<Option<SubqueryExpression>> {
        let condition_lower = condition.to_lowercase();

        // Check for EXISTS pattern
        if condition_lower.starts_with("exists (") || condition_lower.starts_with("not exists (") {
            return self.parse_exists_condition(condition);
        }

        // Check for IN pattern
        if condition_lower.contains(" in (") || condition_lower.contains(" not in (") {
            return self.parse_in_condition(condition);
        }

        // Check for ANY/ALL patterns
        if condition_lower.contains(" any (") || condition_lower.contains(" all (") {
            return self.parse_any_all_condition(condition);
        }

        // Check for scalar subquery comparisons (contains parenthesized SELECT)
        if condition_lower.contains("(select ") {
            return self.parse_scalar_subquery_condition(condition);
        }

        Ok(None)
    }

    /// Parse simple WHERE condition (non-subquery)
    fn parse_simple_condition(&self, condition: &str) -> Result<(String, String, Value)> {
        // Parse column = value (support =, >, <, >=, <=, !=)
        let operators = ["!=", ">=", "<=", "=", ">", "<"];

        for op in &operators {
            if let Some(op_pos) = condition.find(op) {
                let column = condition[..op_pos].trim();
                let value_str = condition[op_pos + op.len()..].trim();

                // Parse value
                let value = self.parse_sql_value(value_str)?;

                return Ok((column.to_string(), op.to_string(), value));
            }
        }

        Err(anyhow!("Invalid WHERE condition: {}", condition))
    }

    /// Parse EXISTS/NOT EXISTS condition
    fn parse_exists_condition(&self, condition: &str) -> Result<Option<SubqueryExpression>> {
        let condition_lower = condition.to_lowercase();
        let negated = condition_lower.starts_with("not exists");

        let start_pattern = if negated { "not exists (" } else { "exists (" };
        let start_pos = condition_lower
            .find(start_pattern)
            .ok_or_else(|| anyhow!("Invalid EXISTS condition"))?;

        // Point to the '(' so extract_parenthesized_subquery can find it
        let subquery_start = start_pos + start_pattern.len() - 1;
        let subquery_sql = self.extract_parenthesized_subquery(&condition[subquery_start..])?;

        let subquery = self.parse_subquery(&subquery_sql)?;

        Ok(Some(SubqueryExpression::Exists { subquery, negated }))
    }

    /// Parse IN/NOT IN condition
    fn parse_in_condition(&self, condition: &str) -> Result<Option<SubqueryExpression>> {
        let condition_lower = condition.to_lowercase();
        let negated = condition_lower.contains(" not in (");

        let in_pattern = if negated { " not in (" } else { " in (" };
        let in_pos = condition_lower
            .find(in_pattern)
            .ok_or_else(|| anyhow!("Invalid IN condition"))?;

        let column = condition[..in_pos].trim().to_string();
        // Point to the '(' so extract_parenthesized_subquery can find it
        let subquery_start = in_pos + in_pattern.len() - 1;
        let subquery_sql = self.extract_parenthesized_subquery(&condition[subquery_start..])?;

        let subquery = self.parse_subquery(&subquery_sql)?;

        Ok(Some(SubqueryExpression::In {
            column,
            subquery,
            negated,
        }))
    }

    /// Parse ANY/ALL comparison condition
    fn parse_any_all_condition(&self, condition: &str) -> Result<Option<SubqueryExpression>> {
        let condition_lower = condition.to_lowercase();

        let (quantifier, pattern) = if condition_lower.contains(" any (") {
            (SubqueryQuantifier::Any, " any (")
        } else if condition_lower.contains(" all (") {
            (SubqueryQuantifier::All, " all (")
        } else {
            return Ok(None);
        };

        let quantifier_pos = condition_lower
            .find(pattern)
            .ok_or_else(|| anyhow!("Invalid ANY/ALL condition"))?;

        // Parse the left side: column operator
        let left_part = condition[..quantifier_pos].trim();
        let operators = ["!=", ">=", "<=", "=", ">", "<"];

        let mut column = String::new();
        let mut operator = String::new();

        for op in &operators {
            if let Some(op_pos) = left_part.rfind(op) {
                column = left_part[..op_pos].trim().to_string();
                operator = op.to_string();
                break;
            }
        }

        if column.is_empty() || operator.is_empty() {
            return Err(anyhow!("Invalid ANY/ALL condition format"));
        }

        // Point to the '(' so extract_parenthesized_subquery can find it
        let subquery_start = quantifier_pos + pattern.len() - 1;
        let subquery_sql = self.extract_parenthesized_subquery(&condition[subquery_start..])?;

        let subquery = self.parse_subquery(&subquery_sql)?;

        Ok(Some(SubqueryExpression::Comparison {
            column,
            operator,
            quantifier: Some(quantifier),
            subquery,
        }))
    }

    /// Parse scalar subquery comparison condition
    fn parse_scalar_subquery_condition(
        &self,
        condition: &str,
    ) -> Result<Option<SubqueryExpression>> {
        let operators = ["!=", ">=", "<=", "=", ">", "<"];

        for op in &operators {
            if let Some(op_pos) = condition.find(op) {
                let left_part = condition[..op_pos].trim();
                let right_part = condition[op_pos + op.len()..].trim();

                // Check if right side is a subquery
                if right_part.starts_with('(') && right_part.to_lowercase().contains("select ") {
                    let subquery_sql = self.extract_parenthesized_subquery(right_part)?;
                    let subquery = self.parse_subquery(&subquery_sql)?;

                    return Ok(Some(SubqueryExpression::Comparison {
                        column: left_part.to_string(),
                        operator: op.to_string(),
                        quantifier: None, // Scalar subquery
                        subquery,
                    }));
                }
            }
        }

        Ok(None)
    }

    /// Extract SQL from parentheses, handling nested parentheses
    fn extract_parenthesized_subquery(&self, text: &str) -> Result<String> {
        let text = text.trim();
        if !text.starts_with('(') {
            return Err(anyhow!("Expected parenthesized subquery"));
        }

        let mut paren_count = 0;
        let mut end_pos = 0;

        for (i, ch) in text.char_indices() {
            match ch {
                '(' => paren_count += 1,
                ')' => {
                    paren_count -= 1;
                    if paren_count == 0 {
                        end_pos = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if paren_count != 0 {
            return Err(anyhow!("Unmatched parentheses in subquery"));
        }

        Ok(text[1..end_pos].to_string())
    }

    /// Parse a subquery and determine if it's correlated
    fn parse_subquery(&self, sql: &str) -> Result<Subquery> {
        let sql = sql.trim();

        // Simple correlation detection - look for unqualified column references
        // that might reference outer query tables
        let referenced_columns = self.extract_potential_correlation_columns(sql);
        let is_correlated = !referenced_columns.is_empty();

        Ok(Subquery {
            sql: sql.to_string(),
            is_correlated,
            referenced_columns,
        })
    }

    /// Extract potential correlation columns from subquery
    fn extract_potential_correlation_columns(&self, _sql: &str) -> Vec<String> {
        // This is a simplified implementation
        // In practice, you'd want more sophisticated parsing to distinguish
        // between columns from subquery tables vs outer query references
        Vec::new() // For now, assume non-correlated
    }

    /// Parse scalar subqueries in SELECT clause
    fn parse_select_clause_with_subqueries(
        &self,
        select_part: &str,
    ) -> Result<Vec<ExtendedSelectItem>> {
        let mut items = Vec::new();

        // Split by commas, but be careful of commas inside subqueries
        let parts = self.split_select_items(select_part)?;

        for part in parts {
            let part = part.trim();

            // Check if this is a scalar subquery
            if part.starts_with('(') && part.to_lowercase().contains("select ") {
                let subquery_sql = self.extract_parenthesized_subquery(part)?;
                let subquery = self.parse_subquery(&subquery_sql)?;

                items.push(ExtendedSelectItem::ScalarSubquery(ScalarSubquery {
                    subquery,
                    alias: None, // Could be enhanced to parse AS alias
                }));
            } else if let Ok(Some(agg)) = self.parse_aggregation_function(part) {
                items.push(ExtendedSelectItem::Aggregation(agg));
            } else {
                items.push(ExtendedSelectItem::Column(part.to_string()));
            }
        }

        Ok(items)
    }

    /// Split SELECT items by commas, respecting parentheses
    fn split_select_items(&self, select_part: &str) -> Result<Vec<String>> {
        let mut items = Vec::new();
        let mut current_item = String::new();
        let mut paren_count = 0;
        let mut in_quotes = false;
        let mut quote_char = '\0';

        for ch in select_part.chars() {
            match ch {
                '"' | '\'' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                    current_item.push(ch);
                }
                c if in_quotes && c == quote_char => {
                    in_quotes = false;
                    current_item.push(ch);
                }
                '(' if !in_quotes => {
                    paren_count += 1;
                    current_item.push(ch);
                }
                ')' if !in_quotes => {
                    paren_count -= 1;
                    current_item.push(ch);
                }
                ',' if !in_quotes && paren_count == 0 => {
                    items.push(current_item.trim().to_string());
                    current_item.clear();
                }
                _ => {
                    current_item.push(ch);
                }
            }
        }

        if !current_item.trim().is_empty() {
            items.push(current_item.trim().to_string());
        }

        Ok(items)
    }

    /// Parse derived table in FROM clause
    fn parse_derived_table(&self, from_part: &str) -> Result<Option<DerivedTable>> {
        let from_part = from_part.trim();

        // Check if it starts with a parenthesized SELECT
        if from_part.starts_with('(') && from_part.to_lowercase().contains("select ") {
            // Find the end of the subquery and extract alias
            let subquery_sql = self.extract_parenthesized_subquery(from_part)?;

            // Look for alias after the closing parenthesis
            let remaining = &from_part[subquery_sql.len() + 2..].trim(); // +2 for parentheses
            let alias = if remaining.to_lowercase().starts_with("as ") {
                remaining[3..]
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow!("Missing alias for derived table"))?
                    .to_string()
            } else {
                remaining
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow!("Missing alias for derived table"))?
                    .to_string()
            };

            let subquery = self.parse_subquery(&subquery_sql)?;

            Ok(Some(DerivedTable { subquery, alias }))
        } else {
            Ok(None)
        }
    }

    /// Execute a subquery and return its results (with caching for non-correlated subqueries)
    async fn execute_subquery(
        &self,
        subquery: &Subquery,
        outer_row: Option<&Value>,
    ) -> Result<QueryResult> {
        // If an outer row is provided, always run through the correlated path so that
        // references like "alias.col" are substituted with actual values.  This is safe
        // for non-correlated subqueries too: if no keys match, the SQL is left unchanged.
        if let Some(row) = outer_row {
            self.execute_correlated_subquery(subquery, row).await
        } else {
            // Non-correlated subquery - check cache first
            let cache_key = subquery.sql.clone();

            // Check if we have a cached result
            {
                let cache = self.subquery_cache.lock().await;
                if let Some(cached_result) = cache.get(&cache_key) {
                    debug!("Using cached result for subquery: {}", cache_key);
                    return Ok(cached_result.clone());
                }
            }

            // Execute the subquery - box the future to avoid recursion issues
            let result = Box::pin(self.execute(subquery.sql.as_str())).await?;

            // Cache the result for future use
            {
                let mut cache = self.subquery_cache.lock().await;
                cache.insert(cache_key, result.clone());
                debug!("Cached result for subquery: {}", subquery.sql);
            }

            Ok(result)
        }
    }

    /// Execute a correlated subquery with outer row context.
    ///
    /// Substitutes outer-row column values into the subquery SQL.  The outer
    /// row may contain either:
    ///   a) prefixed keys like `"u.id"` — when the JOIN already embeds the alias, or
    ///   b) bare keys like `"id"` — when the engine row has no alias prefix.
    ///
    /// For (a): exact string replacement is safe (keys are already fully qualified).
    /// For (b): we must handle two cases in order:
    ///   1. `alias.col` references in the SQL (e.g. `u.id`) — find `.{col}` suffixes
    ///      and replace the whole `alias.col` token.
    ///   2. Bare `col` references — replace with word-boundary checking to avoid
    ///      corrupting compound identifiers like `customer_id`.
    async fn execute_correlated_subquery(
        &self,
        subquery: &Subquery,
        outer_row: &Value,
    ) -> Result<QueryResult> {
        let mut sql = subquery.sql.clone();

        if let Value::Object(map) = outer_row {
            // Sort keys longest-first so longer keys are replaced before shorter ones.
            let mut pairs: Vec<(&String, &Value)> = map.iter().collect();
            pairs.sort_by_key(|b| std::cmp::Reverse(b.0.len()));

            // Word-boundary replacement helper (avoids matching substrings of identifiers).
            let replace_word_bounded = |haystack: &str, pattern: &str, replacement: &str| -> String {
                let mut result = String::new();
                let mut search = haystack;
                while let Some(pos) = search.find(pattern) {
                    let before = if pos > 0 { search.as_bytes()[pos - 1] } else { b' ' };
                    let after = search
                        .as_bytes()
                        .get(pos + pattern.len())
                        .copied()
                        .unwrap_or(b' ');
                    let is_boundary = |c: u8| !c.is_ascii_alphanumeric() && c != b'_';
                    result.push_str(&search[..pos]);
                    if is_boundary(before) && is_boundary(after) {
                        result.push_str(replacement);
                    } else {
                        result.push_str(pattern);
                    }
                    search = &search[pos + pattern.len()..];
                }
                result.push_str(search);
                result
            };

            for (key, value) in &pairs {
                let literal = match value {
                    Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                    Value::Null => "NULL".to_string(),
                    _ => "NULL".to_string(),
                };

                if key.contains('.') {
                    // Prefixed key (e.g. "u.id"): safe exact replacement.
                    sql = sql.replace(key.as_str(), &literal);
                } else {
                    // Bare key (e.g. "id").
                    //
                    // Step 1: replace "alias.key" patterns → literal.
                    // This handles the common case WHERE SQL uses `u.id` but the
                    // outer row only contains the bare key `id`.
                    let dot_key = format!(".{}", key);
                    let mut result = String::new();
                    let mut search = sql.as_str();
                    while let Some(dot_pos) = search.find(&dot_key) {
                        let after_pos = dot_pos + dot_key.len();
                        let after = search.as_bytes().get(after_pos).copied().unwrap_or(b' ');
                        let is_boundary = |c: u8| !c.is_ascii_alphanumeric() && c != b'_';
                        if is_boundary(after) {
                            // Walk back to find the start of the alias token.
                            let alias_start = search[..dot_pos]
                                .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                                .map(|p| p + 1)
                                .unwrap_or(0);
                            result.push_str(&search[..alias_start]);
                            result.push_str(&literal);
                            search = &search[after_pos..];
                        } else {
                            // Not a word boundary after the key — skip past this occurrence.
                            result.push_str(&search[..dot_pos + dot_key.len()]);
                            search = &search[dot_pos + dot_key.len()..];
                        }
                    }
                    result.push_str(search);
                    sql = result;

                    // Step 2: replace any remaining bare "key" occurrences using
                    // word-boundary matching.
                    sql = replace_word_bounded(&sql, key, &literal);
                }
            }
        }

        Box::pin(self.execute(&sql)).await
    }

    /// Evaluate WHERE conditions with subquery support
    async fn evaluate_where_conditions(
        &self,
        conditions: &[WhereCondition],
        row: &Value,
    ) -> Result<bool> {
        for condition in conditions {
            match condition {
                WhereCondition::Simple {
                    column,
                    operator,
                    value,
                } => {
                    if let Value::Object(map) = row {
                        let field_value = map
                            .get(column)
                            .or_else(|| {
                                // If column has a table prefix (e.g. "p.customer_id"),
                                // strip it and retry with the bare name ("customer_id").
                                if let Some(dot) = column.rfind('.') {
                                    map.get(&column[dot + 1..])
                                } else {
                                    None
                                }
                            })
                            .or_else(|| {
                                // Try to find column with any table prefix
                                map.iter()
                                    .find(|(key, _)| {
                                        key.ends_with(&format!(".{}", column)) || key == &column
                                    })
                                    .map(|(_, v)| v)
                            })
                            .cloned()
                            .unwrap_or(Value::Null);

                        if field_value.is_null() {
                            return Ok(false);
                        }

                        if !driftdb_core::query::predicate::compare_values(
                            &field_value,
                            value,
                            operator,
                        ) {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                WhereCondition::Subquery(subquery_expr) => {
                    if !self.evaluate_subquery_condition(subquery_expr, row).await? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// Evaluate a subquery condition
    async fn evaluate_subquery_condition(
        &self,
        expr: &SubqueryExpression,
        row: &Value,
    ) -> Result<bool> {
        match expr {
            SubqueryExpression::In {
                column,
                subquery,
                negated,
            } => {
                let result = self.evaluate_in_subquery(column, subquery, row).await?;
                Ok(if *negated { !result } else { result })
            }
            SubqueryExpression::Exists { subquery, negated } => {
                let result = self.evaluate_exists_subquery(subquery, row).await?;
                Ok(if *negated { !result } else { result })
            }
            SubqueryExpression::Comparison {
                column,
                operator,
                quantifier,
                subquery,
            } => {
                self.evaluate_comparison_subquery(
                    column,
                    operator,
                    quantifier.as_ref(),
                    subquery,
                    row,
                )
                .await
            }
        }
    }

    /// Evaluate IN subquery condition
    async fn evaluate_in_subquery(
        &self,
        column: &str,
        subquery: &Subquery,
        row: &Value,
    ) -> Result<bool> {
        // Get the column value from the row
        let column_value = if let Value::Object(map) = row {
            map.get(column)
                .or_else(|| {
                    map.iter()
                        .find(|(key, _)| key.ends_with(&format!(".{}", column)) || key == &column)
                        .map(|(_, v)| v)
                })
                .cloned()
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };

        if column_value.is_null() {
            return Ok(false);
        }

        // Execute the subquery
        let subquery_result = self.execute_subquery(subquery, Some(row)).await?;

        // Check if the column value is in the subquery results
        match subquery_result {
            QueryResult::Select { rows, .. } => {
                for result_row in rows {
                    if !result_row.is_empty() && result_row[0] == column_value {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Err(anyhow!("Subquery in IN clause must return a result set")),
        }
    }

    /// Evaluate EXISTS subquery condition
    async fn evaluate_exists_subquery(&self, subquery: &Subquery, row: &Value) -> Result<bool> {
        let subquery_result = self.execute_subquery(subquery, Some(row)).await?;

        match subquery_result {
            QueryResult::Select { rows, .. } => Ok(!rows.is_empty()),
            _ => Err(anyhow!(
                "Subquery in EXISTS clause must return a result set"
            )),
        }
    }

    /// Evaluate comparison subquery with ANY/ALL or scalar
    async fn evaluate_comparison_subquery(
        &self,
        column: &str,
        operator: &str,
        quantifier: Option<&SubqueryQuantifier>,
        subquery: &Subquery,
        row: &Value,
    ) -> Result<bool> {
        // Get the column value from the row
        let column_value = if let Value::Object(map) = row {
            map.get(column)
                .or_else(|| {
                    map.iter()
                        .find(|(key, _)| key.ends_with(&format!(".{}", column)) || key == &column)
                        .map(|(_, v)| v)
                })
                .cloned()
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        };

        if column_value.is_null() {
            return Ok(false);
        }

        // Execute the subquery
        let subquery_result = self.execute_subquery(subquery, Some(row)).await?;

        match subquery_result {
            QueryResult::Select { rows, .. } => {
                match quantifier {
                    Some(SubqueryQuantifier::Any) => {
                        // ANY: true if comparison is true for at least one value
                        for result_row in rows {
                            if !result_row.is_empty()
                                && driftdb_core::query::predicate::compare_values(
                                    &column_value,
                                    &result_row[0],
                                    operator,
                                )
                            {
                                return Ok(true);
                            }
                        }
                        Ok(false)
                    }
                    Some(SubqueryQuantifier::All) => {
                        // ALL: true if comparison is true for all values
                        for result_row in rows {
                            if !result_row.is_empty()
                                && !driftdb_core::query::predicate::compare_values(
                                    &column_value,
                                    &result_row[0],
                                    operator,
                                )
                            {
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    }
                    None => {
                        // Scalar subquery: must return exactly one value
                        if rows.len() != 1 || rows[0].is_empty() {
                            return Err(anyhow!("Scalar subquery must return exactly one value"));
                        }
                        Ok(driftdb_core::query::predicate::compare_values(
                            &column_value,
                            &rows[0][0],
                            operator,
                        ))
                    }
                }
            }
            _ => Err(anyhow!("Subquery in comparison must return a result set")),
        }
    }

    /// Execute scalar subquery in SELECT clause
    async fn execute_scalar_subquery(
        &self,
        scalar_subquery: &ScalarSubquery,
        outer_row: Option<&Value>,
    ) -> Result<Value> {
        let subquery_result = self
            .execute_subquery(&scalar_subquery.subquery, outer_row)
            .await?;

        match subquery_result {
            QueryResult::Select { mut rows, .. } => {
                if rows.len() != 1 || rows[0].is_empty() {
                    return Err(anyhow!("Scalar subquery must return exactly one value"));
                }
                let mut row = rows.pop().ok_or_else(|| anyhow!("Scalar subquery returned no rows"))?;
                row.pop().ok_or_else(|| anyhow!("Scalar subquery returned empty row"))
            }
            _ => Err(anyhow!("Scalar subquery must return a result set")),
        }
    }

    /// Execute derived table (subquery in FROM clause)
    async fn execute_derived_table(
        &self,
        derived_table: &DerivedTable,
    ) -> Result<(Vec<Value>, Vec<String>)> {
        let subquery_result = self.execute_subquery(&derived_table.subquery, None).await?;

        match subquery_result {
            QueryResult::Select { rows, columns } => {
                // Prefix column names with the alias
                let prefixed_columns: Vec<String> = columns
                    .iter()
                    .map(|col| format!("{}.{}", derived_table.alias, col))
                    .collect();

                Ok((
                    rows.into_iter()
                        .map(|row| {
                            // Convert each row to an object with prefixed column names
                            let mut map = serde_json::Map::new();
                            for (i, value) in row.into_iter().enumerate() {
                                if i < prefixed_columns.len() {
                                    map.insert(prefixed_columns[i].clone(), value);
                                }
                            }
                            Value::Object(map)
                        })
                        .collect(),
                    prefixed_columns,
                ))
            }
            _ => Err(anyhow!("Derived table subquery must return a result set")),
        }
    }

    /// Parse derived table with JOINs
    fn parse_derived_table_with_joins(&self, from_part: &str) -> Result<FromClause> {
        // Extract the derived table part and the JOIN parts
        let subquery_end_pos = self.find_subquery_end_position(from_part)?;

        // Find the alias
        let after_subquery = &from_part[subquery_end_pos..].trim();
        let alias_end = after_subquery
            .find(" JOIN ")
            .or_else(|| after_subquery.find(" INNER JOIN "))
            .or_else(|| after_subquery.find(" LEFT JOIN "))
            .or_else(|| after_subquery.find(" RIGHT JOIN "))
            .or_else(|| after_subquery.find(" FULL JOIN "))
            .or_else(|| after_subquery.find(" CROSS JOIN "))
            .unwrap_or(after_subquery.len());

        let alias_part = after_subquery[..alias_end].trim();
        let joins_part = &after_subquery[alias_end..];

        // Parse the derived table
        let derived_table_with_alias = format!("{}{}", &from_part[..subquery_end_pos], alias_part);
        let derived_table = self
            .parse_derived_table(&derived_table_with_alias)?
            .ok_or_else(|| anyhow!("Failed to parse derived table"))?;

        // Parse the JOINs
        let joins = self.parse_joins(joins_part)?;

        Ok(FromClause::DerivedTableWithJoins {
            base_table: derived_table,
            joins,
        })
    }

    /// Find the end position of a subquery in FROM clause
    fn find_subquery_end_position(&self, from_part: &str) -> Result<usize> {
        let mut paren_count = 0;
        let mut in_quotes = false;
        let mut quote_char = '\0';

        for (i, ch) in from_part.char_indices() {
            match ch {
                '"' | '\'' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                }
                c if in_quotes && c == quote_char => {
                    in_quotes = false;
                }
                '(' if !in_quotes => {
                    paren_count += 1;
                }
                ')' if !in_quotes => {
                    paren_count -= 1;
                    if paren_count == 0 {
                        return Ok(i + 1);
                    }
                }
                _ => {}
            }
        }

        Err(anyhow!("Unmatched parentheses in derived table"))
    }

    /// Execute JOIN operation
    /// Parse temporal clause from SQL query
    fn parse_temporal_clause(&self, sql_part: &str) -> Result<Option<TemporalClause>> {
        let lower = sql_part.to_lowercase();

        // Look for FOR SYSTEM_TIME ALL (must check before AS OF since it's a prefix match)
        if lower.contains(" for system_time all") {
            return Ok(Some(TemporalClause::All));
        }

        // Look for FOR SYSTEM_TIME AS OF
        if let Some(pos) = lower.find(" for system_time as of ") {
            let temporal_start = pos + 23; // Length of " for system_time as of "
            let remaining = &sql_part[temporal_start..];

            // Find the end of the temporal clause
            let temporal_end = remaining
                .find(" WHERE ")
                .or_else(|| remaining.find(" where "))
                .or_else(|| remaining.find(" GROUP BY "))
                .or_else(|| remaining.find(" group by "))
                .or_else(|| remaining.find(" ORDER BY "))
                .or_else(|| remaining.find(" order by "))
                .or_else(|| remaining.find(" LIMIT "))
                .or_else(|| remaining.find(" limit "))
                .unwrap_or(remaining.len());

            let temporal_value = remaining[..temporal_end].trim();
            debug!("Parsing temporal value: {}", temporal_value);

            // Parse the temporal point
            let point = self.parse_temporal_point(temporal_value)?;
            return Ok(Some(TemporalClause::AsOf(point)));
        }

        Ok(None)
    }

    /// Parse a temporal point (timestamp or sequence number)
    fn parse_temporal_point(&self, value: &str) -> Result<TemporalPoint> {
        let trimmed = value.trim();

        // Check for @SEQ:N / @seq:N (DriftDB sequence number extension)
        if let Some(rest) = trimmed.to_uppercase().strip_prefix("@SEQ:") {
            let seq = rest
                .trim()
                .parse::<u64>()
                .map_err(|_| anyhow!("Invalid sequence number: {}", rest))?;
            return Ok(TemporalPoint::Sequence(seq));
        }

        // Check for CURRENT_TIMESTAMP
        if trimmed.to_uppercase() == "CURRENT_TIMESTAMP" {
            return Ok(TemporalPoint::CurrentTimestamp);
        }

        // Check for sequence number (e.g., "SEQUENCE 100" or just "100")
        if trimmed.to_uppercase().starts_with("SEQUENCE ") {
            let seq_str = &trimmed[9..]; // Skip "SEQUENCE "
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| anyhow!("Invalid sequence number: {}", seq_str))?;
            return Ok(TemporalPoint::Sequence(seq));
        }

        // Try to parse as a plain number (sequence)
        if let Ok(seq) = trimmed.parse::<u64>() {
            return Ok(TemporalPoint::Sequence(seq));
        }

        // Try to parse as timestamp
        // Support various formats: 'YYYY-MM-DD HH:MM:SS' or ISO8601
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            let timestamp_str = &trimmed[1..trimmed.len() - 1];
            // For now, we'll use a simple approach
            // In a real implementation, we'd parse the timestamp properly
            return Ok(TemporalPoint::Timestamp(timestamp_str.to_string()));
        }

        Err(anyhow!(
            "Invalid temporal point: {}. Expected CURRENT_TIMESTAMP, sequence number, or timestamp",
            value
        ))
    }

    /// Execute `FOR SYSTEM_TIME ALL` — returns full drift history for a table.
    ///
    /// If `after_from` contains a WHERE clause with a simple pk equality, returns
    /// history for that single row only; otherwise returns history for all rows.
    async fn execute_system_time_all(
        &self,
        from_clause: &FromClause,
        after_from: &str,
    ) -> Result<QueryResult> {
        let table_name = match from_clause {
            FromClause::Single(t) => t.name.clone(),
            _ => return Err(anyhow!("FOR SYSTEM_TIME ALL only supports single tables")),
        };

        // Try to extract a simple pk equality from the WHERE clause
        let pk_val = if let Some(where_pos) = after_from.to_lowercase().find(" where ") {
            let where_clause = &after_from[where_pos + 7..];
            // Find the end of WHERE (stop at GROUP BY / ORDER BY / LIMIT)
            let where_end = where_clause
                .to_lowercase()
                .find(" group by ")
                .or_else(|| where_clause.to_lowercase().find(" order by "))
                .or_else(|| where_clause.to_lowercase().find(" limit "))
                .unwrap_or(where_clause.len());
            let condition = where_clause[..where_end].trim();
            parse_simple_equality_value(condition)
        } else {
            None
        };

        use driftdb_core::query::Query as CoreQuery;

        let events: Vec<Value> = if let Some(pk) = pk_val {
            let mut engine = self.engine_write()?;
            match engine.execute_query(CoreQuery::ShowDrift {
                table: table_name,
                primary_key: pk,
            })? {
                driftdb_core::query::QueryResult::DriftHistory { events } => events,
                _ => vec![],
            }
        } else {
            // All rows
            let mut engine = self.engine_write()?;
            let pk_col = engine
                .get_table_primary_key(&table_name)
                .map_err(|e| anyhow!("{}", e))?;
            let rows = match engine.execute_query(CoreQuery::Select {
                table: table_name.clone(),
                conditions: vec![],
                as_of: None,
                limit: None,
            })? {
                driftdb_core::query::QueryResult::Rows { data } => data,
                _ => vec![],
            };
            let mut all_events: Vec<Value> = Vec::new();
            for row in rows {
                if let Some(pk) = row.get(&pk_col).cloned() {
                    if let driftdb_core::query::QueryResult::DriftHistory { events } =
                        engine.execute_query(CoreQuery::ShowDrift {
                            table: table_name.clone(),
                            primary_key: pk,
                        })?
                    {
                        all_events.extend(events);
                    }
                }
            }
            all_events
        };

        Ok(QueryResult::Select {
            columns: vec!["event".to_string()],
            rows: events.into_iter().map(|e| vec![e]).collect(),
        })
    }

    /// Execute JOIN with temporal support
    async fn execute_temporal_join(
        &self,
        from_clause: &FromClause,
        temporal_clause: &Option<TemporalClause>,
    ) -> Result<(Vec<Value>, Vec<String>)> {
        match from_clause {
            FromClause::Single(table_ref) => {
                // Single table with temporal support - get all data synchronously
                let engine = self.engine_read()?;
                let table_data = match temporal_clause {
                    Some(TemporalClause::AsOf(point)) => {
                        match point {
                            TemporalPoint::Sequence(seq) => engine
                                .get_table_data_at(&table_ref.name, *seq)
                                .map_err(|e| anyhow!("Failed to get temporal table data: {}", e))?,
                            TemporalPoint::CurrentTimestamp => {
                                // Current timestamp means latest data
                                engine
                                    .get_table_data(&table_ref.name)
                                    .map_err(|e| anyhow!("Failed to get table data: {}", e))?
                            }
                            TemporalPoint::Timestamp(ts) => {
                                // Parse the timestamp string to OffsetDateTime
                                let timestamp = time::OffsetDateTime::parse(
                                    ts,
                                    &time::format_description::well_known::Rfc3339,
                                )
                                .or_else(|_| {
                                    // Try ISO 8601 format
                                    time::PrimitiveDateTime::parse(
                                        ts,
                                        &time::format_description::well_known::Iso8601::DEFAULT,
                                    )
                                    .map(|dt| dt.assume_utc())
                                })
                                .map_err(|e| anyhow!("Invalid timestamp format '{}': {}", ts, e))?;

                                // Get table data at the specific timestamp
                                engine
                                    .get_table_data_at_timestamp(&table_ref.name, timestamp)
                                    .map_err(|e| {
                                        anyhow!(
                                            "Failed to get table data at timestamp {}: {}",
                                            ts,
                                            e
                                        )
                                    })?
                            }
                        }
                    }
                    // All is handled before execute_temporal_join is called;
                    // this arm is a defensive fallback that returns current data.
                    Some(TemporalClause::All) | None => engine
                        .get_table_data(&table_ref.name)
                        .map_err(|e| anyhow!("Failed to get table data: {}", e))?,
                };

                // Create column names with table prefix if alias exists
                let columns = engine
                    .get_table_columns(&table_ref.name)
                    .map_err(|e| anyhow!("{}", e))?;
                let prefixed_columns = if let Some(alias) = &table_ref.alias {
                    columns
                        .iter()
                        .map(|col| format!("{}.{}", alias, col))
                        .collect()
                } else {
                    columns
                        .iter()
                        .map(|col| format!("{}.{}", table_ref.name, col))
                        .collect()
                };

                Ok((table_data, prefixed_columns))
            }
            _ => Err(anyhow!("Temporal queries only support single tables")),
        }
    }

    async fn execute_join(&self, from_clause: &FromClause) -> Result<(Vec<Value>, Vec<String>)> {
        match from_clause {
            FromClause::Single(table_ref) => {
                // Check CTE tables first
                {
                    let ctes = self.cte_tables.lock().await;
                    if let Some(cte_rows) = ctes.get(&table_ref.name) {
                        let cte_rows = cte_rows.clone();
                        drop(ctes);
                        // Derive column list from the CTE data
                        let mut columns: Vec<String> = Vec::new();
                        let mut seen = std::collections::HashSet::new();
                        for row in &cte_rows {
                            if let Value::Object(map) = row {
                                for key in map.keys() {
                                    if seen.insert(key.clone()) {
                                        columns.push(key.clone());
                                    }
                                }
                            }
                        }
                        let prefix = table_ref.alias.as_ref().unwrap_or(&table_ref.name);
                        let prefixed_columns: Vec<String> = columns
                            .iter()
                            .map(|col| format!("{}.{}", prefix, col))
                            .collect();
                        return Ok((cte_rows, prefixed_columns));
                    }
                }

                // Single table - no JOIN needed. Get all data synchronously to avoid
                // holding a non-Send RwLockReadGuard across await points.
                let engine = self.engine_read()?;
                let table_data = engine
                    .get_table_data(&table_ref.name)
                    .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

                // Get schema columns, then discover additional columns from actual data
                // (DriftDB is schema-flexible, so inserts can add columns not in the schema)
                let mut columns: Vec<String> = engine
                    .get_table_columns(&table_ref.name)
                    .unwrap_or_default();

                // Discover additional columns from actual data records
                let mut seen: std::collections::HashSet<String> = columns.iter().cloned().collect();
                for record in &table_data {
                    if let Value::Object(map) = record {
                        for key in map.keys() {
                            if seen.insert(key.clone()) {
                                columns.push(key.clone());
                            }
                        }
                    }
                }

                let prefix = table_ref.alias.as_ref().unwrap_or(&table_ref.name);
                let prefixed_columns = columns
                    .iter()
                    .map(|col| format!("{}.{}", prefix, col))
                    .collect();

                Ok((table_data, prefixed_columns))
            }
            FromClause::MultipleImplicit(tables) => {
                // Implicit JOIN (CROSS JOIN with WHERE clause filtering)
                self.execute_implicit_join(tables).await
            }
            FromClause::WithJoins { base_table, joins } => {
                // Explicit JOINs
                self.execute_explicit_joins(base_table, joins).await
            }
            FromClause::DerivedTable(derived_table) => {
                // Derived table (subquery in FROM clause)
                self.execute_derived_table(derived_table).await
            }
            FromClause::DerivedTableWithJoins { base_table, joins } => {
                // Derived table with JOINs
                let (mut base_data, mut all_columns) =
                    self.execute_derived_table(base_table).await?;

                // Execute each JOIN
                for join in joins {
                    let (joined_data, joined_columns) = self
                        .execute_single_join(&base_data, &all_columns, join)
                        .await?;
                    base_data = joined_data;
                    all_columns.extend(joined_columns);
                }

                Ok((base_data, all_columns))
            }
        }
    }

    /// Execute implicit JOIN (cross product of multiple tables)
    async fn execute_implicit_join(
        &self,
        tables: &[TableRef],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        if tables.is_empty() {
            return Err(anyhow!("No tables specified for implicit JOIN"));
        }

        // Get data for all tables synchronously, then drop the guard
        let (table_data, all_columns) = {
            let engine = self.engine_read()?;
            let mut table_data = Vec::new();
            let mut all_columns = Vec::new();

            for table_ref in tables {
                let data = engine.get_table_data(&table_ref.name).map_err(|e| {
                    anyhow!("Failed to get table data for {}: {}", table_ref.name, e)
                })?;

                let columns = engine
                    .get_table_columns(&table_ref.name)
                    .map_err(|e| anyhow!("{}", e))?;
                let table_prefix = table_ref.alias.as_ref().unwrap_or(&table_ref.name);
                let prefixed_columns: Vec<String> = columns
                    .iter()
                    .map(|col| format!("{}.{}", table_prefix, col))
                    .collect();

                table_data.push(data);
                all_columns.extend(prefixed_columns);
            }
            (table_data, all_columns)
        };

        // Create Cartesian product
        let joined_rows = self.create_cartesian_product(&table_data);

        Ok((joined_rows, all_columns))
    }

    /// Execute explicit JOINs
    async fn execute_explicit_joins(
        &self,
        base_table: &TableRef,
        joins: &[Join],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        // Get base table data synchronously, then drop the guard
        let (mut current_data, mut current_columns) = {
            let engine = self.engine_read()?;
            let data = engine
                .get_table_data(&base_table.name)
                .map_err(|e| anyhow!("Failed to get base table data: {}", e))?;

            let columns = engine
                .get_table_columns(&base_table.name)
                .map_err(|e| anyhow!("{}", e))?;
            let base_prefix = base_table.alias.as_ref().unwrap_or(&base_table.name);
            let prefixed: Vec<String> = columns
                .iter()
                .map(|col| format!("{}.{}", base_prefix, col))
                .collect();
            (data, prefixed)
        };

        // Apply each JOIN in sequence
        for join in joins {
            let (new_data, new_columns) = self
                .execute_single_join(&current_data, &current_columns, join)
                .await?;

            current_data = new_data;
            current_columns = new_columns;
        }

        Ok((current_data, current_columns))
    }

    /// Execute a single JOIN operation
    async fn execute_single_join(
        &self,
        left_data: &[Value],
        left_columns: &[String],
        join: &Join,
    ) -> Result<(Vec<Value>, Vec<String>)> {
        // Get right table data synchronously, then drop the guard
        let (right_data, right_columns) = {
            let engine = self.engine_read()?;
            let data = engine
                .get_table_data(&join.table)
                .map_err(|e| anyhow!("Failed to get table data for {}: {}", join.table, e))?;

            let columns = engine
                .get_table_columns(&join.table)
                .map_err(|e| anyhow!("{}", e))?;
            let right_prefix = join.table_alias.as_ref().unwrap_or(&join.table);
            let prefixed: Vec<String> = columns
                .iter()
                .map(|col| format!("{}.{}", right_prefix, col))
                .collect();
            (data, prefixed)
        };

        // Combine column lists
        let mut combined_columns = left_columns.to_vec();
        combined_columns.extend(right_columns.clone());

        match join.join_type {
            JoinType::Inner => {
                self.execute_inner_join(left_data, &right_data, join, &combined_columns)
            }
            JoinType::LeftOuter => {
                self.execute_left_join(left_data, &right_data, join, &combined_columns)
            }
            JoinType::RightOuter => {
                self.execute_right_join(left_data, &right_data, join, &combined_columns)
            }
            JoinType::FullOuter => {
                self.execute_full_outer_join(left_data, &right_data, join, &combined_columns)
            }
            JoinType::Cross => self.execute_cross_join(left_data, &right_data, &combined_columns),
        }
    }

    /// Execute INNER JOIN
    fn execute_inner_join(
        &self,
        left_data: &[Value],
        right_data: &[Value],
        join: &Join,
        combined_columns: &[String],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        let mut result_rows = Vec::new();

        for left_row in left_data {
            for right_row in right_data {
                if self.join_condition_matches(left_row, right_row, join)? {
                    let combined_row = self.combine_rows(left_row, right_row)?;
                    result_rows.push(combined_row);
                }
            }
        }

        Ok((result_rows, combined_columns.to_vec()))
    }

    /// Execute LEFT JOIN
    fn execute_left_join(
        &self,
        left_data: &[Value],
        right_data: &[Value],
        join: &Join,
        combined_columns: &[String],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        let mut result_rows = Vec::new();

        for left_row in left_data {
            let mut found_match = false;

            for right_row in right_data {
                if self.join_condition_matches(left_row, right_row, join)? {
                    let combined_row = self.combine_rows(left_row, right_row)?;
                    result_rows.push(combined_row);
                    found_match = true;
                }
            }

            // If no match found, add left row with NULLs for right side
            if !found_match {
                let null_right =
                    self.create_null_row_for_table(&join.table, join.table_alias.as_ref())?;
                let combined_row = self.combine_rows(left_row, &null_right)?;
                result_rows.push(combined_row);
            }
        }

        Ok((result_rows, combined_columns.to_vec()))
    }

    /// Execute RIGHT JOIN
    fn execute_right_join(
        &self,
        left_data: &[Value],
        right_data: &[Value],
        join: &Join,
        combined_columns: &[String],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        let mut result_rows = Vec::new();

        for right_row in right_data {
            let mut found_match = false;

            for left_row in left_data {
                if self.join_condition_matches(left_row, right_row, join)? {
                    let combined_row = self.combine_rows(left_row, right_row)?;
                    result_rows.push(combined_row);
                    found_match = true;
                }
            }

            // If no match found, add right row with NULLs for left side
            if !found_match {
                // Create a null row for the left side - we need to determine left table info
                let null_left = self.create_null_row_from_columns(
                    &combined_columns[..combined_columns.len() - self.count_right_columns(join)?],
                )?;
                let combined_row = self.combine_rows(&null_left, right_row)?;
                result_rows.push(combined_row);
            }
        }

        Ok((result_rows, combined_columns.to_vec()))
    }

    /// Execute FULL OUTER JOIN
    fn execute_full_outer_join(
        &self,
        left_data: &[Value],
        right_data: &[Value],
        join: &Join,
        combined_columns: &[String],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        let mut result_rows = Vec::new();
        let mut matched_right_indices = std::collections::HashSet::new();

        // Process left side (like LEFT JOIN)
        for left_row in left_data {
            let mut found_match = false;

            for (right_idx, right_row) in right_data.iter().enumerate() {
                if self.join_condition_matches(left_row, right_row, join)? {
                    let combined_row = self.combine_rows(left_row, right_row)?;
                    result_rows.push(combined_row);
                    matched_right_indices.insert(right_idx);
                    found_match = true;
                }
            }

            if !found_match {
                let null_right =
                    self.create_null_row_for_table(&join.table, join.table_alias.as_ref())?;
                let combined_row = self.combine_rows(left_row, &null_right)?;
                result_rows.push(combined_row);
            }
        }

        // Process unmatched right side
        for (right_idx, right_row) in right_data.iter().enumerate() {
            if !matched_right_indices.contains(&right_idx) {
                let null_left = self.create_null_row_from_columns(
                    &combined_columns[..combined_columns.len() - self.count_right_columns(join)?],
                )?;
                let combined_row = self.combine_rows(&null_left, right_row)?;
                result_rows.push(combined_row);
            }
        }

        Ok((result_rows, combined_columns.to_vec()))
    }

    /// Execute CROSS JOIN
    fn execute_cross_join(
        &self,
        left_data: &[Value],
        right_data: &[Value],
        combined_columns: &[String],
    ) -> Result<(Vec<Value>, Vec<String>)> {
        let result_rows = self.create_cartesian_product(&[left_data.to_vec(), right_data.to_vec()]);
        Ok((result_rows, combined_columns.to_vec()))
    }

    /// Create Cartesian product of multiple tables
    fn create_cartesian_product(&self, table_data: &[Vec<Value>]) -> Vec<Value> {
        if table_data.is_empty() {
            return Vec::new();
        }

        if table_data.len() == 1 {
            return table_data[0].clone();
        }

        let _result: Vec<Value> = Vec::new();

        // Start with first table
        let mut current_product = table_data[0].clone();

        // Combine with each subsequent table
        for table in &table_data[1..] {
            let mut new_product = Vec::new();

            for current_row in &current_product {
                for table_row in table {
                    if let Ok(combined) = self.combine_rows(current_row, table_row) {
                        new_product.push(combined);
                    }
                }
            }

            current_product = new_product;
        }

        current_product
    }

    /// Check if JOIN condition matches between two rows
    fn join_condition_matches(
        &self,
        left_row: &Value,
        right_row: &Value,
        join: &Join,
    ) -> Result<bool> {
        let condition = match &join.condition {
            Some(cond) => cond,
            None => return Ok(true), // CROSS JOIN always matches
        };

        // Get values from both rows
        let left_value = self.get_column_value_from_row(
            left_row,
            &condition.left_table,
            &condition.left_column,
        )?;
        let right_value = self.get_column_value_from_row(
            right_row,
            &condition.right_table,
            &condition.right_column,
        )?;

        // Compare based on operator
        match condition.operator.as_str() {
            "=" => Ok(left_value == right_value),
            "!=" => Ok(left_value != right_value),
            "<" => {
                let ordering = driftdb_core::query::predicate::compare_json_values(&left_value, &right_value);
                Ok(ordering == std::cmp::Ordering::Less)
            }
            ">" => {
                let ordering = driftdb_core::query::predicate::compare_json_values(&left_value, &right_value);
                Ok(ordering == std::cmp::Ordering::Greater)
            }
            "<=" => {
                let ordering = driftdb_core::query::predicate::compare_json_values(&left_value, &right_value);
                Ok(ordering != std::cmp::Ordering::Greater)
            }
            ">=" => {
                let ordering = driftdb_core::query::predicate::compare_json_values(&left_value, &right_value);
                Ok(ordering != std::cmp::Ordering::Less)
            }
            _ => Err(anyhow!("Unsupported JOIN operator: {}", condition.operator)),
        }
    }

    /// Get column value from row with table qualification
    fn get_column_value_from_row(
        &self,
        row: &Value,
        table_name: &str,
        column_name: &str,
    ) -> Result<Value> {
        if let Value::Object(map) = row {
            // Try table.column format first
            let qualified_column = if table_name.is_empty() {
                column_name.to_string()
            } else {
                format!("{}.{}", table_name, column_name)
            };

            if let Some(value) = map.get(&qualified_column) {
                return Ok(value.clone());
            }

            // Try just column name as fallback
            if let Some(value) = map.get(column_name) {
                return Ok(value.clone());
            }

            // Try to find any column ending with this name
            for (key, value) in map {
                if key.ends_with(&format!(".{}", column_name)) {
                    return Ok(value.clone());
                }
            }
        }

        Ok(Value::Null)
    }

    /// Combine two rows into a single row
    fn combine_rows(&self, left_row: &Value, right_row: &Value) -> Result<Value> {
        if let (Value::Object(left_map), Value::Object(right_map)) = (left_row, right_row) {
            let mut combined = left_map.clone();
            combined.extend(right_map.clone());
            Ok(Value::Object(combined))
        } else {
            Err(anyhow!("Cannot combine non-object rows"))
        }
    }

    /// Create a row of NULL values for a table
    fn create_null_row_for_table(
        &self,
        table_name: &str,
        table_alias: Option<&String>,
    ) -> Result<Value> {
        // This is a simplified implementation
        // In a real system, we'd get the actual schema for the table
        let mut null_row = serde_json::Map::new();

        // For now, we'll create common column names
        // A more complete implementation would query the table schema
        let common_columns = ["id", "name", "value", "created_at", "updated_at"];
        let prefix = table_alias.as_ref().map_or(table_name, |alias| alias);

        for col in &common_columns {
            null_row.insert(format!("{}.{}", prefix, col), Value::Null);
        }

        Ok(Value::Object(null_row))
    }

    /// Create a row of NULL values from column list
    fn create_null_row_from_columns(&self, columns: &[String]) -> Result<Value> {
        let mut null_row = serde_json::Map::new();

        for col in columns {
            null_row.insert(col.clone(), Value::Null);
        }

        Ok(Value::Object(null_row))
    }

    /// Count columns for the right table in a join
    fn count_right_columns(&self, _join: &Join) -> Result<usize> {
        // Simplified implementation - in reality we'd check the table schema
        Ok(5) // Assuming 5 columns per table for now
    }

    /// Get table columns in schema order
    async fn get_table_columns(&self, engine: &Engine, table_name: &str) -> Result<Vec<String>> {
        // Get column names from the engine's schema in the order they were defined
        engine
            .get_table_columns(table_name)
            .map_err(|e| anyhow!("{}", e))
    }

    /// Use indexes to optimize WHERE clause filtering
    async fn get_filtered_table_data(
        &self,
        table_name: &str,
        conditions: &[(String, String, Value)],
    ) -> Result<Vec<Value>> {
        let engine = self.engine_read()?;

        // Check if we should use indexes
        if !self.use_indexes || conditions.is_empty() {
            // No index optimization, fetch all data
            return engine
                .get_table_data(table_name)
                .map_err(|e| anyhow!("Failed to get table data: {}", e));
        }

        // Try to find an indexed column in the conditions
        let indexed_columns = engine.get_indexed_columns(table_name);

        for (column, operator, value) in conditions {
            // Check if this column is indexed
            if indexed_columns.contains(column) && operator == "=" {
                // Use index for equality lookup
                debug!("Using index on column {} for table {}", column, table_name);

                if let Ok(matching_keys) = engine.lookup_by_index(table_name, column, value) {
                    let num_candidates = matching_keys.len();

                    // Fetch only the matching rows
                    let mut rows = Vec::new();
                    for key in matching_keys {
                        if let Ok(Some(row)) = engine.get_row(table_name, &key) {
                            rows.push(row);
                        }
                    }

                    // Apply remaining conditions to the index results
                    let mut filtered = Vec::new();
                    for row in rows {
                        let mut matches = true;
                        for (col, op, val) in conditions {
                            if col != column || op != "=" {
                                // Apply non-index conditions
                                if let Value::Object(map) = &row {
                                    if let Some(field_value) = map.get(col) {
                                        if !driftdb_core::query::predicate::compare_values(
                                            field_value,
                                            val,
                                            op,
                                        ) {
                                            matches = false;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if matches {
                            filtered.push(row);
                        }
                    }

                    info!(
                        "Index scan on {} returned {} rows (filtered from {} candidates)",
                        column,
                        filtered.len(),
                        num_candidates
                    );
                    return Ok(filtered);
                }
            }
        }

        // No suitable index found, fall back to full table scan
        debug!("No suitable index found for WHERE clause, performing full table scan");
        engine
            .get_table_data(table_name)
            .map_err(|e| anyhow!("Failed to get table data: {}", e))
    }

    /// Check if we can use an index for the given conditions
    fn can_use_index(&self, conditions: &[WhereCondition]) -> Option<String> {
        // Look for simple equality conditions that can use an index
        for condition in conditions {
            if let WhereCondition::Simple {
                column, operator, ..
            } = condition
            {
                if operator == "=" {
                    return Some(column.clone());
                }
            }
        }
        None
    }

    /// Apply DISTINCT to remove duplicate rows
    fn apply_distinct(&self, data: &[Value], columns: Option<&[String]>) -> Result<Vec<Value>> {
        use std::collections::HashSet;

        if data.is_empty() {
            return Ok(vec![]);
        }

        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for row in data {
            if let Value::Object(map) = row {
                // Create a key based on specified columns or all columns
                let key = if let Some(cols) = columns {
                    // DISTINCT on specific columns
                    cols.iter()
                        .map(|col| {
                            // Try direct column access or with table prefix
                            map.get(col)
                                .or_else(|| {
                                    map.iter()
                                        .find(|(k, _)| k.ends_with(&format!(".{}", col)))
                                        .map(|(_, v)| v)
                                })
                                .cloned()
                                .unwrap_or(Value::Null)
                        })
                        .collect::<Vec<_>>()
                } else {
                    // DISTINCT on all columns
                    let mut values: Vec<(String, Value)> =
                        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    values.sort_by_key(|(k, _)| k.clone());
                    values.into_iter().map(|(_, v)| v).collect()
                };

                // Convert to string for HashSet comparison (simple approach)
                let key_str = format!("{:?}", key);

                if seen.insert(key_str) {
                    result.push(row.clone());
                }
            } else {
                // For non-object rows, just add them (shouldn't happen in normal case)
                result.push(row.clone());
            }
        }

        Ok(result)
    }

    /// Parse set operations (UNION, INTERSECT, EXCEPT)
    fn parse_set_operation(&self, sql: &str) -> Result<Option<SetOperationQuery>> {
        let lower = sql.to_lowercase();

        // Check for set operations
        let operations = [
            (" union all ", SetOperation::UnionAll),
            (" union ", SetOperation::Union),
            (" intersect all ", SetOperation::IntersectAll),
            (" intersect ", SetOperation::Intersect),
            (" except all ", SetOperation::ExceptAll),
            (" except ", SetOperation::Except),
        ];

        for (keyword, operation) in operations.iter() {
            if let Some(pos) = lower.find(keyword) {
                // Split the query into left and right parts
                let left = sql[..pos].trim();
                let right = sql[pos + keyword.len()..].trim();

                // Both sides must be SELECT statements
                if !left.to_lowercase().starts_with("select")
                    || !right.to_lowercase().starts_with("select")
                {
                    if left.starts_with("(")
                        && left.ends_with(")")
                        && right.starts_with("(")
                        && right.ends_with(")")
                    {
                        // Handle parenthesized subqueries
                        let left_inner = &left[1..left.len() - 1];
                        let right_inner = &right[1..right.len() - 1];
                        if left_inner.to_lowercase().starts_with("select")
                            && right_inner.to_lowercase().starts_with("select")
                        {
                            return Ok(Some(SetOperationQuery {
                                left: left_inner.to_string(),
                                right: right_inner.to_string(),
                                operation: operation.clone(),
                            }));
                        }
                    }
                    continue;
                }

                return Ok(Some(SetOperationQuery {
                    left: left.to_string(),
                    right: right.to_string(),
                    operation: operation.clone(),
                }));
            }
        }

        Ok(None)
    }

    // ==================== TRANSACTION METHODS ====================

    /// Execute BEGIN through the core SQL bridge. Isolation-level hints
    /// (`BEGIN ISOLATION LEVEL SERIALIZABLE`, etc.) parse cleanly via
    /// sqlparser but are not yet honored by the engine's transaction
    /// manager — the engine treats every transaction as READ COMMITTED
    /// today. That matched the server's previous TransactionManager
    /// behaviour (which stored the isolation level but never read it).
    /// Documented as a known limitation; the parser keeps the AST so
    /// honoring isolation levels later doesn't require a re-plumb.
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

/// Parse a simple `col = 'val'` or `col = num` equality clause.
/// Returns only the value part (ignores the column name).
/// Returns `None` if the clause cannot be parsed as a simple single equality.
fn parse_simple_equality_value(condition: &str) -> Option<Value> {
    let eq_pos = condition.find('=')?;
    let value_part = condition[eq_pos + 1..].trim();

    if value_part.starts_with('\'') && value_part.ends_with('\'') {
        let inner = &value_part[1..value_part.len() - 1];
        Some(Value::String(inner.to_string()))
    } else if let Ok(n) = value_part.parse::<f64>() {
        Some(serde_json::json!(n))
    } else if !value_part.is_empty() {
        // Bare word — treat as string
        Some(Value::String(value_part.to_string()))
    } else {
        None
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

    #[tokio::test]
    async fn test_parse_where_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        let conditions = executor
            .parse_where_clause("name = 'Alice' AND age > 25")
            .unwrap();
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].0, "name");
        assert_eq!(conditions[0].1, "=");
        assert_eq!(conditions[0].2, Value::String("Alice".to_string()));
        assert_eq!(conditions[1].0, "age");
        assert_eq!(conditions[1].1, ">");
        assert_eq!(conditions[1].2, Value::Number(25.into()));
    }

    #[tokio::test]
    async fn test_parse_order_by_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        // Test ascending order (default)
        let order_by = executor.parse_order_by_clause("name").unwrap();
        assert_eq!(order_by.column, "name");
        assert!(matches!(order_by.direction, OrderDirection::Asc));

        // Test explicit ascending
        let order_by = executor.parse_order_by_clause("name ASC").unwrap();
        assert_eq!(order_by.column, "name");
        assert!(matches!(order_by.direction, OrderDirection::Asc));

        // Test descending
        let order_by = executor.parse_order_by_clause("age DESC").unwrap();
        assert_eq!(order_by.column, "age");
        assert!(matches!(order_by.direction, OrderDirection::Desc));
    }

    #[tokio::test]
    async fn test_parse_limit_clause() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        let limit = executor.parse_limit_clause("10").unwrap();
        assert_eq!(limit, 10);

        let limit = executor.parse_limit_clause("100").unwrap();
        assert_eq!(limit, 100);

        // Test invalid limit
        assert!(executor.parse_limit_clause("invalid").is_err());
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
            .execute("CREATE TABLE users (pk = id)")
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
            .execute("CREATE TABLE users (pk = id)")
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
            .execute("CREATE TABLE users (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE test_scores (pk = id)")
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
            .execute("CREATE TABLE users (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE users (pk = id)")
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
            .execute("CREATE TABLE empty_table (pk = id)")
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
            .execute("CREATE TABLE users (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
            .execute("CREATE TABLE employees (pk = id)")
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
