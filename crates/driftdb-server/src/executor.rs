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

use crate::transaction::{IsolationLevel, PendingWrite, TransactionManager, WriteOperation};

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

/// Order by specification
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
#[derive(Debug, Clone)]
pub struct Aggregation {
    function: AggregationFunction,
    column: Option<String>, // None for COUNT(*)
}

/// Group by specification
#[derive(Debug, Clone)]
struct GroupBy {
    columns: Vec<String>,
}

/// Having clause specification (similar to WHERE but for groups)
#[derive(Debug, Clone)]
struct Having {
    conditions: Vec<(String, String, Value)>, // (function_expression, operator, value)
}

/// Select clause specification
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
    // Future: Between, FromTo, All
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
    transaction_manager: Arc<TransactionManager>, // Transaction management
    session_id: String,                           // Session identifier for transaction tracking
}

/// Foreign key constraint definition (process-global registry)
#[derive(Debug, Clone)]
pub struct ForeignKeyConstraint {
    /// Column in this table that holds the FK value
    pub column: String,
    /// Referenced table name
    pub ref_table: String,
    /// Referenced column in the target table
    pub ref_column: String,
}

fn fk_registry() -> &'static parking_lot::RwLock<HashMap<String, Vec<ForeignKeyConstraint>>> {
    static FK_REGISTRY: std::sync::OnceLock<
        parking_lot::RwLock<HashMap<String, Vec<ForeignKeyConstraint>>>,
    > = std::sync::OnceLock::new();
    FK_REGISTRY.get_or_init(|| parking_lot::RwLock::new(HashMap::new()))
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
                // Convert data: Vec<Value> to columnar format
                if data.is_empty() {
                    Ok(QueryResult::Select {
                        columns: vec![],
                        rows: vec![],
                    })
                } else {
                    // Use provided columns from schema, or fall back to HashMap keys
                    let columns: Vec<String> = if let Some(cols) = table_columns {
                        cols
                    } else {
                        // Fallback to HashMap keys
                        if let Some(Value::Object(first_row)) = data.first() {
                            first_row.keys().cloned().collect()
                        } else {
                            vec![]
                        }
                    };

                    // Convert rows
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
        let transaction_manager = Arc::new(TransactionManager::new(engine.clone()));
        QueryExecutor {
            engine_guard: None,
            engine: Some(engine),
            subquery_cache: Arc::new(Mutex::new(HashMap::new())),
            cte_tables: Arc::new(Mutex::new(HashMap::new())),
            use_indexes: true, // Enable index optimization by default
            prepared_statements: Arc::new(ParkingMutex::new(HashMap::new())),
            transaction_manager,
            session_id: format!("session_{}", std::process::id()),
        }
    }

    pub fn new_with_guard(engine_guard: &'a EngineGuard) -> Self {
        // Use the engine from the guard for transaction management
        let engine_for_txn = engine_guard.get_engine_ref();
        let transaction_manager = Arc::new(TransactionManager::new(engine_for_txn));

        Self {
            engine_guard: Some(engine_guard),
            engine: None,
            subquery_cache: Arc::new(Mutex::new(HashMap::new())),
            cte_tables: Arc::new(Mutex::new(HashMap::new())),
            use_indexes: true,
            prepared_statements: Arc::new(ParkingMutex::new(HashMap::new())),
            transaction_manager,
            session_id: format!("guard_session_{}", std::process::id()),
        }
    }

    /// Create a new executor with a shared transaction manager
    pub fn new_with_guard_and_transaction_manager(
        engine_guard: &'a EngineGuard,
        transaction_manager: Arc<TransactionManager>,
        session_id: String,
    ) -> Self {
        Self {
            engine_guard: Some(engine_guard),
            engine: None,
            subquery_cache: Arc::new(Mutex::new(HashMap::new())),
            cte_tables: Arc::new(Mutex::new(HashMap::new())),
            use_indexes: true,
            prepared_statements: Arc::new(ParkingMutex::new(HashMap::new())),
            transaction_manager,
            session_id,
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
            return Ok(Value::Number(serde_json::Number::from_f64(f).unwrap()));
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
                return Ok(self.matches_condition(&lhs_val, canonical_op, &rhs_val));
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
                        for (col, val) in columns.iter().zip(row.into_iter()) {
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
    fn register_fk_constraints(&self, table_name: &str, column_defs_str: &str) {
        // Look for patterns: col_name ... REFERENCES other_table(other_col)
        let mut registry = fk_registry().write();
        let mut constraints = Vec::new();

        // Simple per-column scan
        for part in column_defs_str.split(',') {
            let part = part.trim();
            let upper = part.to_uppercase();
            if let Some(ref_pos) = upper.find("REFERENCES") {
                // Extract the column name (first token of this definition)
                let col_name = part.split_whitespace().next().unwrap_or("").to_string();
                let after_ref = part[ref_pos + 10..].trim();
                // Parse ref_table(ref_col)
                if let Some(paren_open) = after_ref.find('(') {
                    if let Some(paren_close) = after_ref.find(')') {
                        let ref_table = after_ref[..paren_open].trim().to_string();
                        let ref_col = after_ref[paren_open + 1..paren_close].trim().to_string();
                        if !col_name.is_empty() && !ref_table.is_empty() && !ref_col.is_empty() {
                            constraints.push(ForeignKeyConstraint {
                                column: col_name,
                                ref_table,
                                ref_column: ref_col,
                            });
                        }
                    }
                }
            }
        }

        if !constraints.is_empty() {
            registry.insert(table_name.to_string(), constraints);
            info!("Registered {} FK constraint(s) for table '{}'", registry[table_name].len(), table_name);
        }
    }

    /// Validate FK constraints on INSERT: for each FK column in the inserted doc,
    /// verify a row with the referenced PK value exists in the referenced table.
    fn validate_fk_insert(&self, table_name: &str, doc: &Value) -> Result<()> {
        let registry = fk_registry().read();
        let constraints = match registry.get(table_name) {
            Some(c) => c.clone(),
            None => return Ok(()),
        };
        drop(registry);

        if constraints.is_empty() {
            return Ok(());
        }

        let engine = self.engine_read()?;
        for fk in &constraints {
            let fk_val = if let Value::Object(map) = doc {
                map.get(&fk.column).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            if fk_val.is_null() {
                continue; // NULL FK values are allowed (no referential check needed)
            }
            // Scan the referenced table for a matching row
            let ref_data = engine
                .get_table_data(&fk.ref_table)
                .map_err(|e| anyhow!("FK check: failed to read table '{}': {}", fk.ref_table, e))?;
            let found = ref_data.iter().any(|row| {
                if let Value::Object(map) = row {
                    map.get(&fk.ref_column).map(|v| v == &fk_val).unwrap_or(false)
                } else {
                    false
                }
            });
            if !found {
                return Err(anyhow!(
                    "Foreign key violation: value {:?} in column '{}' does not exist in '{}'.'{}'",
                    fk_val, fk.column, fk.ref_table, fk.ref_column
                ));
            }
        }
        Ok(())
    }

    /// Validate FK constraints on DELETE: ensure no other table has a FK pointing
    /// at the row being deleted.
    fn validate_fk_delete(&self, table_name: &str, deleted_row: &Value) -> Result<()> {
        let registry = fk_registry().read();
        let engine = self.engine_read()?;

        for (referencing_table, constraints) in registry.iter() {
            for fk in constraints {
                if fk.ref_table != table_name {
                    continue;
                }
                // fk.ref_column in deleted_row gives the PK value we're deleting
                let pk_val = if let Value::Object(map) = deleted_row {
                    map.get(&fk.ref_column).cloned().unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                if pk_val.is_null() {
                    continue;
                }
                // Check if any row in the referencing table uses this PK value
                let ref_data = engine
                    .get_table_data(referencing_table)
                    .map_err(|e| anyhow!("FK check failed: {}", e))?;
                let blocked = ref_data.iter().any(|row| {
                    if let Value::Object(map) = row {
                        map.get(&fk.column).map(|v| v == &pk_val).unwrap_or(false)
                    } else {
                        false
                    }
                });
                if blocked {
                    return Err(anyhow!(
                        "Foreign key violation: cannot delete row from '{}' because '{}' has a \
                         reference via column '{}'",
                        table_name, referencing_table, fk.column
                    ));
                }
            }
        }
        Ok(())
    }

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

        let paren_pos = expr.find('(').unwrap();
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

    /// Compute aggregation results from filtered data
    fn compute_aggregations(
        &self,
        data: &[Value],
        aggregations: &[Aggregation],
    ) -> Result<(Vec<String>, Vec<Value>)> {
        let mut columns = Vec::new();
        let mut values = Vec::new();

        for aggregation in aggregations {
            let (column_name, result) = self.compute_single_aggregation(data, aggregation)?;
            columns.push(column_name);
            values.push(result);
        }

        Ok((columns, values))
    }

    /// Group data by specified columns
    fn group_data(
        &self,
        data: Vec<Value>,
        group_by: &GroupBy,
    ) -> Result<HashMap<Vec<Value>, Vec<Value>>> {
        let mut groups: HashMap<Vec<Value>, Vec<Value>> = HashMap::new();

        for row in data {
            if let Value::Object(ref map) = row {
                // Extract group key values
                let mut group_key = Vec::new();
                for col in &group_by.columns {
                    let value = map.get(col).cloned().unwrap_or(Value::Null);
                    group_key.push(value);
                }

                // Add row to the appropriate group
                groups.entry(group_key).or_default().push(row);
            } else {
                return Err(anyhow!("Invalid row format for grouping"));
            }
        }

        Ok(groups)
    }

    /// Compute aggregations for grouped data, returning results for each group
    fn compute_grouped_aggregations(
        &self,
        groups: &HashMap<Vec<Value>, Vec<Value>>,
        group_by: &GroupBy,
        aggregations: &[Aggregation],
    ) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
        let mut all_columns = group_by.columns.clone();
        let mut agg_columns = Vec::new();

        // Add aggregation column names
        for aggregation in aggregations {
            let column_name = match (&aggregation.function, &aggregation.column) {
                (AggregationFunction::Count, None) => "count(*)".to_string(),
                (AggregationFunction::Count, Some(col)) => format!("count({})", col),
                (AggregationFunction::Sum, Some(col)) => format!("sum({})", col),
                (AggregationFunction::Avg, Some(col)) => format!("avg({})", col),
                (AggregationFunction::Min, Some(col)) => format!("min({})", col),
                (AggregationFunction::Max, Some(col)) => format!("max({})", col),
                _ => return Err(anyhow!("Invalid aggregation configuration")),
            };
            agg_columns.push(column_name);
        }

        all_columns.extend(agg_columns);

        let mut all_rows = Vec::new();

        // Process each group
        for (group_key, group_data) in groups {
            let mut row = group_key.clone(); // Start with group key values

            // Compute aggregations for this group
            for aggregation in aggregations {
                let (_, result) = self.compute_single_aggregation(group_data, aggregation)?;
                row.push(result);
            }

            all_rows.push(row);
        }

        Ok((all_columns, all_rows))
    }

    /// Apply HAVING clause to filter groups
    fn apply_having_filter(
        &self,
        groups: HashMap<Vec<Value>, Vec<Value>>,
        having: &Having,
        _aggregations: &[Aggregation],
    ) -> Result<HashMap<Vec<Value>, Vec<Value>>> {
        let mut filtered_groups = HashMap::new();

        for (group_key, group_data) in groups {
            let mut matches_having = true;

            // Check each HAVING condition
            for (function_expr, operator, expected_value) in &having.conditions {
                // Parse the aggregation function from the expression
                if let Some(aggregation) = self.parse_aggregation_function(function_expr)? {
                    let (_, actual_value) =
                        self.compute_single_aggregation(&group_data, &aggregation)?;

                    if !self.matches_condition(&actual_value, operator, expected_value) {
                        matches_having = false;
                        break;
                    }
                } else {
                    return Err(anyhow!(
                        "Invalid aggregation function in HAVING: {}",
                        function_expr
                    ));
                }
            }

            if matches_having {
                filtered_groups.insert(group_key, group_data);
            }
        }

        Ok(filtered_groups)
    }

    /// Compute a single aggregation function
    fn compute_single_aggregation(
        &self,
        data: &[Value],
        aggregation: &Aggregation,
    ) -> Result<(String, Value)> {
        let column_name = match (&aggregation.function, &aggregation.column) {
            (AggregationFunction::Count, None) => "count(*)".to_string(),
            (AggregationFunction::Count, Some(col)) => format!("count({})", col),
            (AggregationFunction::Sum, Some(col)) => format!("sum({})", col),
            (AggregationFunction::Avg, Some(col)) => format!("avg({})", col),
            (AggregationFunction::Min, Some(col)) => format!("min({})", col),
            (AggregationFunction::Max, Some(col)) => format!("max({})", col),
            _ => return Err(anyhow!("Invalid aggregation configuration")),
        };

        let result = match aggregation.function {
            AggregationFunction::Count => self.compute_count(data, &aggregation.column)?,
            AggregationFunction::Sum => {
                self.compute_sum(data, aggregation.column.as_ref().unwrap())?
            }
            AggregationFunction::Avg => {
                self.compute_avg(data, aggregation.column.as_ref().unwrap())?
            }
            AggregationFunction::Min => {
                self.compute_min(data, aggregation.column.as_ref().unwrap())?
            }
            AggregationFunction::Max => {
                self.compute_max(data, aggregation.column.as_ref().unwrap())?
            }
        };

        Ok((column_name, result))
    }

    /// Compute COUNT(*) or COUNT(column)
    fn compute_count(&self, data: &[Value], column: &Option<String>) -> Result<Value> {
        match column {
            None => {
                // COUNT(*) - count all rows
                Ok(Value::Number((data.len() as i64).into()))
            }
            Some(col) => {
                // COUNT(column) - count non-null values
                let count = data
                    .iter()
                    .filter_map(|row| {
                        if let Value::Object(map) = row {
                            map.get(col)
                        } else {
                            None
                        }
                    })
                    .filter(|&value| value != &Value::Null)
                    .count();
                Ok(Value::Number((count as i64).into()))
            }
        }
    }

    /// Compute SUM(column)
    fn compute_sum(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut sum = 0.0;
        let mut has_values = false;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    match value {
                        Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                sum += f;
                                has_values = true;
                            }
                        }
                        Value::Null => {
                            // Skip null values
                            continue;
                        }
                        _ => {
                            return Err(anyhow!(
                                "Cannot compute SUM on non-numeric column: {}",
                                column
                            ));
                        }
                    }
                }
            }
        }

        if !has_values {
            Ok(Value::Null)
        } else if sum.fract() == 0.0 && sum >= i64::MIN as f64 && sum <= i64::MAX as f64 {
            // Return as integer if it's a whole number within i64 range
            Ok(Value::Number((sum as i64).into()))
        } else {
            // Return as float
            Ok(Value::Number(
                serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()),
            ))
        }
    }

    /// Compute AVG(column)
    fn compute_avg(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut sum = 0.0;
        let mut count = 0;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    match value {
                        Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                sum += f;
                                count += 1;
                            }
                        }
                        Value::Null => {
                            // Skip null values
                            continue;
                        }
                        _ => {
                            return Err(anyhow!(
                                "Cannot compute AVG on non-numeric column: {}",
                                column
                            ));
                        }
                    }
                }
            }
        }

        if count == 0 {
            Ok(Value::Null)
        } else {
            let avg = sum / count as f64;
            Ok(Value::Number(
                serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
            ))
        }
    }

    /// Compute MIN(column)
    fn compute_min(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut min_value: Option<Value> = None;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    if value == &Value::Null {
                        continue; // Skip null values
                    }

                    match &min_value {
                        None => min_value = Some(value.clone()),
                        Some(current_min) => {
                            if self.compare_values(value, current_min) == std::cmp::Ordering::Less {
                                min_value = Some(value.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(min_value.unwrap_or(Value::Null))
    }

    /// Compute MAX(column)
    fn compute_max(&self, data: &[Value], column: &str) -> Result<Value> {
        let mut max_value: Option<Value> = None;

        for row in data {
            if let Value::Object(map) = row {
                if let Some(value) = map.get(column) {
                    if value == &Value::Null {
                        continue; // Skip null values
                    }

                    match &max_value {
                        None => max_value = Some(value.clone()),
                        Some(current_max) => {
                            if self.compare_values(value, current_max)
                                == std::cmp::Ordering::Greater
                            {
                                max_value = Some(value.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(max_value.unwrap_or(Value::Null))
    }

    /// Sort rows based on ORDER BY specification
    fn sort_rows(
        &self,
        rows: &mut [Vec<Value>],
        columns: &[String],
        order_by: &OrderBy,
    ) -> Result<()> {
        // Find the column index (case-insensitive for aggregation functions)
        // Also check for table-prefixed columns
        let column_index = columns
            .iter()
            .position(|col| {
                // Direct match
                col == &order_by.column || col.to_lowercase() == order_by.column.to_lowercase() ||
            // Check if column matches the suffix after the table prefix (e.g., "table.column" matches "column")
            col.split('.').next_back().is_some_and(|suffix| {
                suffix == order_by.column || suffix.to_lowercase() == order_by.column.to_lowercase()
            })
            })
            .ok_or_else(|| {
                anyhow!(
                    "ORDER BY column '{}' not found in result set",
                    order_by.column
                )
            })?;

        rows.sort_by(|a, b| {
            let val_a = a.get(column_index).unwrap_or(&Value::Null);
            let val_b = b.get(column_index).unwrap_or(&Value::Null);

            let comparison = self.compare_values(val_a, val_b);

            match order_by.direction {
                OrderDirection::Asc => comparison,
                OrderDirection::Desc => comparison.reverse(),
            }
        });

        Ok(())
    }

    /// Compare two JSON values for sorting
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            // Null values sort first
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // Numbers
            (Value::Number(na), Value::Number(nb)) => {
                let fa = na.as_f64().unwrap_or(0.0);
                let fb = nb.as_f64().unwrap_or(0.0);
                fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
            }

            // Strings
            (Value::String(sa), Value::String(sb)) => sa.cmp(sb),

            // Booleans
            (Value::Bool(ba), Value::Bool(bb)) => ba.cmp(bb),

            // Mixed types: convert to strings and compare
            _ => {
                let sa = match a {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(a).unwrap_or_default(),
                };
                let sb = match b {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(b).unwrap_or_default(),
                };
                sa.cmp(&sb)
            }
        }
    }

    /// Check if a value matches a condition
    fn matches_condition(
        &self,
        field_value: &Value,
        operator: &str,
        condition_value: &Value,
    ) -> bool {
        match operator {
            "=" => field_value == condition_value,
            "!=" => field_value != condition_value,
            ">" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) > b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            }
            "<" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) < b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            }
            ">=" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) >= b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            }
            "<=" => {
                if let (Value::Number(a), Value::Number(b)) = (field_value, condition_value) {
                    a.as_f64().unwrap_or(0.0) <= b.as_f64().unwrap_or(0.0)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

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

        // Route DriftDB custom CREATE TABLE syntax (e.g., "CREATE TABLE users (pk = id)")
        // to the dedicated handler before it falls through to sqlparser-rs
        if lower.starts_with("create table") {
            return self.execute_create_table(sql).await;
        }

        // Route DML/DQL operations to the executor's own handlers which support
        // aggregation, GROUP BY, ORDER BY, JOINs, subqueries, etc.
        if lower.starts_with("select ") {
            return self.execute_select(sql).await;
        }
        // CTE queries start with WITH and contain a final SELECT
        if lower.starts_with("with ") {
            return self.execute_select(sql).await;
        }
        if lower.starts_with("insert into ") || lower.starts_with("insert ") {
            return self.execute_insert(sql).await;
        }
        if lower.starts_with("update ") {
            return self.execute_update(sql).await;
        }
        if lower.starts_with("delete from ") || lower.starts_with("delete ") {
            return self.execute_delete(sql).await;
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

    async fn execute_select(&self, sql: &str) -> Result<QueryResult> {
        // ---------------------------------------------------------------
        // Handle CTEs: WITH cte_name AS (…) SELECT …
        // ---------------------------------------------------------------
        let sql_owned: String;
        let sql = if sql.trim().to_uppercase().starts_with("WITH ") {
            let main_sql = self.extract_and_execute_ctes(sql).await?;
            sql_owned = main_sql;
            sql_owned.as_str()
        } else {
            sql
        };

        let lower = sql.to_lowercase();

        // Note: parse_result is not available in this scope
        // We'll handle multi-line SQL support through better parsing below

        // Now trim for our legacy string-based parsing (fallback path)
        let sql = sql.trim();

        // Check for set operations (UNION, INTERSECT, EXCEPT)
        if let Some(set_op) = self.parse_set_operation(sql)? {
            return self.execute_set_operation(&set_op).await;
        }

        // Parse SELECT clause to determine what kind of query this is
        let select_start = if lower.starts_with("select ") { 7 } else { 0 };
        let from_pos = lower
            .find(" from ")
            .ok_or_else(|| anyhow!("Missing FROM clause"))?;
        let raw_select_part = &sql[select_start..from_pos];

        // ---------------------------------------------------------------
        // Extract CASE WHEN expressions from the SELECT clause
        // ---------------------------------------------------------------
        let (modified_select_part, case_expressions) =
            Self::extract_case_expressions(raw_select_part);
        let select_part = if case_expressions.is_empty() {
            raw_select_part
        } else {
            &modified_select_part
        };

        let select_clause = self.parse_select_clause(select_part)?;

        // Parse FROM clause (handles JOINs)
        let from_pos = lower.find(" from ").unwrap() + 6;
        let remaining = &sql[from_pos..];

        // Find the end of the FROM clause
        let from_end = remaining
            .find(" WHERE ")
            .or_else(|| remaining.find(" where "))
            .or_else(|| remaining.find(" GROUP BY "))
            .or_else(|| remaining.find(" group by "))
            .or_else(|| remaining.find(" ORDER BY "))
            .or_else(|| remaining.find(" order by "))
            .or_else(|| remaining.find(" LIMIT "))
            .or_else(|| remaining.find(" limit "))
            .or_else(|| remaining.find(" FOR SYSTEM_TIME "))
            .or_else(|| remaining.find(" for system_time "))
            .or_else(|| remaining.find(" AS OF "))
            .or_else(|| remaining.find(" as of "))
            .unwrap_or(remaining.len());

        let from_part = remaining[..from_end].trim();
        let after_from = &remaining[from_end..];

        // Parse FROM clause to get table references and JOINs
        let from_clause = self.parse_from_clause(from_part)?;
        debug!("Parsed FROM clause: {:?}", from_clause);

        // Execute JOIN operations to get initial dataset
        let (mut joined_data, mut all_columns) = self.execute_join(&from_clause).await?;
        debug!(
            "JOIN execution produced {} rows with {} columns",
            joined_data.len(),
            all_columns.len()
        );

        // Inject computed CASE WHEN columns into every row so they are
        // available during column projection
        if !case_expressions.is_empty() {
            for row in &mut joined_data {
                if let Value::Object(ref mut map) = row {
                    for (alias, case_expr) in &case_expressions {
                        let computed = self.evaluate_case_expression(case_expr, map)?;
                        map.insert(alias.clone(), computed);
                    }
                }
            }
            // Also extend the column list so validation passes
            for alias in case_expressions.keys() {
                if !all_columns.contains(alias) {
                    all_columns.push(alias.clone());
                }
            }
        }

        // Parse WHERE conditions if present (with subquery support)
        let _where_conditions = if let Some(where_pos) = after_from.to_lowercase().find(" where ") {
            let where_start = where_pos + 7;
            let where_clause = &after_from[where_start..];

            let where_end = where_clause
                .to_lowercase()
                .find(" group by ")
                .or_else(|| where_clause.to_lowercase().find(" order by "))
                .or_else(|| where_clause.to_lowercase().find(" limit "))
                .or_else(|| where_clause.to_lowercase().find(" for system_time "))
                .or_else(|| where_clause.to_lowercase().find(" as of "))
                .unwrap_or(where_clause.len());

            let conditions_str = where_clause[..where_end].trim();
            debug!("Parsing WHERE clause: {}", conditions_str);
            let enhanced_conditions = self.parse_enhanced_where_clause(conditions_str)?;
            debug!(
                "Parsed enhanced WHERE conditions: {:?}",
                enhanced_conditions
            );

            // Filter joined data based on enhanced WHERE conditions
            let initial_count = joined_data.len();
            let mut filtered_data = Vec::new();

            for row in joined_data {
                if self
                    .evaluate_where_conditions(&enhanced_conditions, &row)
                    .await?
                {
                    filtered_data.push(row);
                }
            }

            joined_data = filtered_data;
            debug!(
                "WHERE clause filtered from {} to {} rows",
                initial_count,
                joined_data.len()
            );

            enhanced_conditions
        } else {
            Vec::new()
        };

        // Parse GROUP BY clause if present
        let group_by = if let Some(group_pos) = after_from.to_lowercase().find(" group by ") {
            let group_start = group_pos + 10; // " group by " is 10 characters
            let group_clause = &after_from[group_start..];

            let group_end = group_clause
                .to_lowercase()
                .find(" having ")
                .or_else(|| group_clause.to_lowercase().find(" order by "))
                .or_else(|| group_clause.to_lowercase().find(" limit "))
                .or_else(|| group_clause.to_lowercase().find(" for system_time "))
                .or_else(|| group_clause.to_lowercase().find(" as of "))
                .unwrap_or(group_clause.len());

            let group_str = group_clause[..group_end].trim();
            debug!("Parsing GROUP BY clause: {}", group_str);
            Some(self.parse_group_by_clause(group_str)?)
        } else {
            None
        };

        // Parse HAVING clause if present
        let having = if let Some(having_pos) = after_from.to_lowercase().find(" having ") {
            let having_start = having_pos + 8; // " having " is 8 characters
            let having_clause = &after_from[having_start..];

            let having_end = having_clause
                .to_lowercase()
                .find(" order by ")
                .or_else(|| having_clause.to_lowercase().find(" limit "))
                .or_else(|| having_clause.to_lowercase().find(" for system_time "))
                .or_else(|| having_clause.to_lowercase().find(" as of "))
                .unwrap_or(having_clause.len());

            let having_str = having_clause[..having_end].trim();
            debug!("Parsing HAVING clause: {}", having_str);
            Some(self.parse_having_clause(having_str)?)
        } else {
            None
        };

        // Parse ORDER BY clause if present
        let order_by = if let Some(order_pos) = after_from.to_lowercase().find(" order by ") {
            let order_start = order_pos + 10; // " order by " is 10 characters
            let order_clause = &after_from[order_start..];

            let order_end = order_clause
                .to_lowercase()
                .find(" limit ")
                .or_else(|| order_clause.to_lowercase().find(" for system_time "))
                .or_else(|| order_clause.to_lowercase().find(" as of "))
                .unwrap_or(order_clause.len());

            let order_str = order_clause[..order_end].trim();
            debug!("Parsing ORDER BY clause: {}", order_str);
            Some(self.parse_order_by_clause(order_str)?)
        } else {
            None
        };

        // Parse LIMIT clause if present
        let limit = if let Some(limit_pos) = after_from.to_lowercase().find(" limit ") {
            let limit_start = limit_pos + 7; // " limit " is 7 characters
            let limit_clause = &after_from[limit_start..];

            let limit_end = limit_clause
                .to_lowercase()
                .find(" for system_time ")
                .or_else(|| limit_clause.to_lowercase().find(" as of "))
                .unwrap_or(limit_clause.len());

            let limit_str = limit_clause[..limit_end].trim();
            debug!("Parsing LIMIT clause: {}", limit_str);
            Some(self.parse_limit_clause(limit_str)?)
        } else {
            None
        };

        // Parse temporal clause (FOR SYSTEM_TIME AS OF)
        let temporal_clause = self.parse_temporal_clause(after_from)?;

        // Apply temporal filtering to the data if specified
        if temporal_clause.is_some() {
            // For now, temporal queries only work with single tables
            if !matches!(from_clause, FromClause::Single(_)) {
                warn!("Temporal queries (FOR SYSTEM_TIME) are only supported for single table queries");
                return Err(anyhow!(
                    "Temporal queries are not supported with JOINs or multiple tables"
                ));
            }

            // Re-execute the query with temporal support
            let (temporal_data, temporal_columns) = self
                .execute_temporal_join(&from_clause, &temporal_clause)
                .await?;
            joined_data = temporal_data;
            all_columns = temporal_columns;
            debug!("Temporal query produced {} rows", joined_data.len());
        }

        // Handle different types of queries based on SELECT clause and GROUP BY
        match (select_clause, group_by) {
            // Pure aggregation without GROUP BY
            (SelectClause::Aggregations(aggregations), None) => {
                // Compute aggregation results on joined data
                let (agg_columns, agg_values) =
                    self.compute_aggregations(&joined_data, &aggregations)?;

                debug!(
                    "Returning aggregation result with columns: {:?}",
                    agg_columns
                );
                Ok(QueryResult::Select {
                    columns: agg_columns,
                    rows: vec![agg_values],
                })
            }
            // Mixed columns and aggregations with GROUP BY
            (SelectClause::Mixed(columns, aggregations), Some(group_by_clause)) => {
                // Validate that all non-aggregation columns are in GROUP BY
                for col in &columns {
                    if !group_by_clause.columns.contains(col) {
                        return Err(anyhow!(
                            "Column '{}' must be in GROUP BY clause or be an aggregate function",
                            col
                        ));
                    }
                }

                // Group the joined data
                let mut groups = self.group_data(joined_data, &group_by_clause)?;
                debug!("Grouped joined data into {} groups", groups.len());

                // Apply HAVING clause if present
                if let Some(having_clause) = having {
                    groups = self.apply_having_filter(groups, &having_clause, &aggregations)?;
                    debug!("After HAVING filter: {} groups", groups.len());
                }

                // Compute aggregations for each group
                let (group_columns, mut group_rows) =
                    self.compute_grouped_aggregations(&groups, &group_by_clause, &aggregations)?;

                // Apply ORDER BY if specified
                if let Some(ref order_by) = order_by {
                    debug!(
                        "Applying ORDER BY: {} {:?}",
                        order_by.column, order_by.direction
                    );
                    self.sort_rows(&mut group_rows, &group_columns, order_by)?;
                }

                // Apply LIMIT if specified
                if let Some(limit_count) = limit {
                    debug!("Applying LIMIT: {}", limit_count);
                    group_rows.truncate(limit_count);
                }

                debug!(
                    "Returning {} grouped rows with columns: {:?}",
                    group_rows.len(),
                    group_columns
                );
                Ok(QueryResult::Select {
                    columns: group_columns,
                    rows: group_rows,
                })
            }
            // Aggregations with GROUP BY
            (SelectClause::Aggregations(aggregations), Some(group_by_clause)) => {
                // Group the joined data
                let mut groups = self.group_data(joined_data, &group_by_clause)?;
                debug!("Grouped joined data into {} groups", groups.len());

                // Apply HAVING clause if present
                if let Some(having_clause) = having {
                    groups = self.apply_having_filter(groups, &having_clause, &aggregations)?;
                    debug!("After HAVING filter: {} groups", groups.len());
                }

                // Compute aggregations for each group
                let (group_columns, mut group_rows) =
                    self.compute_grouped_aggregations(&groups, &group_by_clause, &aggregations)?;

                // Apply ORDER BY if specified
                if let Some(ref order_by) = order_by {
                    debug!(
                        "Applying ORDER BY: {} {:?}",
                        order_by.column, order_by.direction
                    );
                    self.sort_rows(&mut group_rows, &group_columns, order_by)?;
                }

                // Apply LIMIT if specified
                if let Some(limit_count) = limit {
                    debug!("Applying LIMIT: {}", limit_count);
                    group_rows.truncate(limit_count);
                }

                debug!(
                    "Returning {} grouped rows with columns: {:?}",
                    group_rows.len(),
                    group_columns
                );
                Ok(QueryResult::Select {
                    columns: group_columns,
                    rows: group_rows,
                })
            }
            // Column selection without GROUP BY
            (SelectClause::Columns(columns), None) => {
                // Filter and format results for specific column selection from joined data
                self.format_join_column_selection_results(
                    joined_data,
                    &all_columns,
                    &columns,
                    order_by,
                    limit,
                )
            }
            // DISTINCT column selection without GROUP BY
            (SelectClause::ColumnsDistinct(columns), None) => {
                // Apply DISTINCT to column selection from joined data
                let distinct_data = self.apply_distinct(&joined_data, Some(&columns))?;
                self.format_join_column_selection_results(
                    distinct_data,
                    &all_columns,
                    &columns,
                    order_by,
                    limit,
                )
            }
            // Regular SELECT *
            (SelectClause::All, None) => {
                // Format results for SELECT * from joined data
                self.format_join_select_all_results(joined_data, all_columns, order_by, limit)
            }
            // SELECT DISTINCT *
            (SelectClause::AllDistinct, None) => {
                // Apply DISTINCT to all columns from joined data
                let distinct_data = self.apply_distinct(&joined_data, None)?;
                self.format_join_select_all_results(distinct_data, all_columns, order_by, limit)
            }
            // Error cases
            (SelectClause::All | SelectClause::AllDistinct, Some(_)) => {
                Err(anyhow!("SELECT * with GROUP BY is not supported"))
            }
            (SelectClause::Columns(_) | SelectClause::ColumnsDistinct(_), Some(_)) => Err(anyhow!(
                "Column selection with GROUP BY is not yet supported - use Mixed clause instead"
            )),
            (SelectClause::Mixed(_, _), None) => Err(anyhow!(
                "Column selection without aggregations requires GROUP BY clause"
            )),
        }
    }

    /// Filter columns from the result set and format for specific column selection
    fn format_column_selection_results(
        &self,
        filtered_data: Vec<Value>,
        selected_columns: &[String],
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        if filtered_data.is_empty() {
            return Ok(QueryResult::Select {
                columns: selected_columns.to_vec(),
                rows: vec![],
            });
        }

        // Validate that all requested columns exist in at least one record
        if filtered_data.is_empty() {
            return Ok(QueryResult::Select {
                columns: Vec::new(),
                rows: Vec::new(),
            });
        }
        let first = &filtered_data[0];
        if let Value::Object(map) = first {
            for col in selected_columns {
                if !map.contains_key(col) {
                    // Check if the column exists in any record (for safety)
                    let column_exists = filtered_data.iter().any(|record| {
                        if let Value::Object(obj) = record {
                            obj.contains_key(col)
                        } else {
                            false
                        }
                    });
                    if !column_exists {
                        return Err(anyhow!("Column '{}' does not exist in the table", col));
                    }
                }
            }
        }

        // Extract only the requested columns from each row
        let mut rows: Vec<Vec<Value>> = filtered_data
            .into_iter()
            .map(|record| {
                if let Value::Object(map) = record {
                    selected_columns
                        .iter()
                        .map(|col| map.get(col).cloned().unwrap_or(Value::Null))
                        .collect()
                } else {
                    // For non-object records, return null for all columns
                    vec![Value::Null; selected_columns.len()]
                }
            })
            .collect();

        // Apply ORDER BY if specified
        if let Some(ref order_by) = order_by {
            debug!(
                "Applying ORDER BY: {} {:?}",
                order_by.column, order_by.direction
            );
            self.sort_rows(&mut rows, selected_columns, order_by)?;
        }

        // Apply LIMIT if specified
        if let Some(limit_count) = limit {
            debug!("Applying LIMIT: {}", limit_count);
            rows.truncate(limit_count);
        }

        debug!(
            "Returning {} rows with {} selected columns: {:?}",
            rows.len(),
            selected_columns.len(),
            selected_columns
        );
        Ok(QueryResult::Select {
            columns: selected_columns.to_vec(),
            rows,
        })
    }

    /// Format results for SELECT * queries
    fn format_select_all_results(
        &self,
        filtered_data: Vec<Value>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        // Format results - ensure consistent column ordering
        if !filtered_data.is_empty() {
            let first = &filtered_data[0];
            // Extract columns without sorting to preserve natural order
            let columns: Vec<String> = if let Value::Object(map) = first {
                // Use the iteration order of the first record
                map.keys().cloned().collect()
            } else {
                vec!["value".to_string()]
            };

            let mut rows: Vec<Vec<Value>> = filtered_data
                .into_iter()
                .map(|record| {
                    if let Value::Object(map) = record {
                        columns
                            .iter()
                            .map(|col| map.get(col).cloned().unwrap_or(Value::Null))
                            .collect()
                    } else {
                        vec![record]
                    }
                })
                .collect();

            // Apply ORDER BY if specified
            if let Some(ref order_by) = order_by {
                debug!(
                    "Applying ORDER BY: {} {:?}",
                    order_by.column, order_by.direction
                );
                self.sort_rows(&mut rows, &columns, order_by)?;
            }

            // Apply LIMIT if specified
            if let Some(limit_count) = limit {
                debug!("Applying LIMIT: {}", limit_count);
                rows.truncate(limit_count);
            }

            debug!("Returning {} rows with columns: {:?}", rows.len(), columns);
            Ok(QueryResult::Select { columns, rows })
        } else {
            // Return empty result set with proper column names from table schema if possible
            // For now, just return empty columns and rows
            debug!("Query returned empty result set");
            Ok(QueryResult::Select {
                columns: vec![],
                rows: vec![],
            })
        }
    }

    async fn execute_insert(&self, sql: &str) -> Result<QueryResult> {
        // Parse INSERT INTO table_name (columns) VALUES (values)
        let sql_clean = sql.trim().trim_end_matches(';');
        let lower = sql_clean.to_lowercase();

        if !lower.starts_with("insert into ") {
            return Err(anyhow!("Invalid INSERT syntax"));
        }

        // Extract table name
        let after_into = &sql_clean[12..]; // Skip "INSERT INTO "
        let table_end = after_into
            .find(' ')
            .or_else(|| after_into.find('('))
            .ok_or_else(|| anyhow!("Cannot find table name"))?;
        let table_name = after_into[..table_end].trim();

        // Find VALUES clause
        let values_pos = lower
            .find(" values")
            .ok_or_else(|| anyhow!("Missing VALUES clause"))?;

        // Extract column names if present
        let columns_str = sql_clean[12 + table_end..values_pos].trim();
        let columns = if columns_str.starts_with('(') && columns_str.ends_with(')') {
            columns_str[1..columns_str.len() - 1]
                .split(',')
                .map(|c| c.trim())
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        // Extract values
        let values_str = sql_clean[values_pos + 7..].trim(); // Skip " VALUES"
        if !values_str.starts_with('(') || !values_str.ends_with(')') {
            return Err(anyhow!("Invalid VALUES syntax"));
        }

        let values = values_str[1..values_str.len() - 1].trim();

        // Create JSON document
        let mut json_obj = serde_json::Map::new();

        if !columns.is_empty() && !values.is_empty() {
            // Parse values (simple comma-separated parser)
            let value_parts = self.parse_values_list(values)?;

            for (i, col) in columns.iter().enumerate() {
                if i < value_parts.len() {
                    json_obj.insert(col.to_string(), value_parts[i].clone());
                }
            }
        } else {
            return Err(anyhow!("Column names and values required"));
        }

        // Insert the document
        let doc = Value::Object(json_obj);

        // Validate foreign key constraints before inserting
        self.validate_fk_insert(table_name, &doc)?;

        // Check if we're in a transaction - if so, buffer the write
        if let Ok(true) = self.transaction_manager.is_in_transaction(&self.session_id) {
            // Buffer the write for later commit
            use crate::transaction::{PendingWrite, WriteOperation};
            let write = PendingWrite {
                table: table_name.to_string(),
                operation: WriteOperation::Insert,
                data: doc,
            };

            self.transaction_manager
                .add_pending_write(&self.session_id, write)
                .await
                .map_err(|e| anyhow!("Failed to buffer transaction write: {}", e))?;

            info!(
                "Buffered INSERT for transaction in session {}",
                self.session_id
            );
        } else {
            // Not in transaction, apply immediately (acquire lock only when needed)
            let mut engine = self.engine_write()?;
            engine
                .insert_record(table_name, doc)
                .map_err(|e| anyhow!("Insert failed: {}", e))?;
        }

        Ok(QueryResult::Insert { count: 1 })
    }

    /// Parse comma-separated values, handling quoted strings
    fn parse_values_list(&self, values: &str) -> Result<Vec<Value>> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = values.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '\'' => {
                    if in_quotes && chars.peek() == Some(&'\'') {
                        // Escaped quote
                        current.push('\'');
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                        if !in_quotes {
                            // End of string value
                            result.push(Value::String(current.clone()));
                            current.clear();
                        }
                    }
                }
                ',' if !in_quotes => {
                    // End of value
                    if !current.is_empty() {
                        result.push(self.parse_sql_value(&current)?);
                        current.clear();
                    }
                }
                #[allow(clippy::if_same_then_else)]
                _ => {
                    if in_quotes || ch != ' ' {
                        current.push(ch);
                    } else if !current.is_empty() {
                        current.push(ch);
                    }
                }
            }
        }

        // Add last value
        if !current.is_empty() {
            result.push(self.parse_sql_value(&current)?);
        }

        Ok(result)
    }

    async fn execute_update(&self, sql: &str) -> Result<QueryResult> {
        // Parse UPDATE table_name SET column = value [WHERE condition]
        let sql_clean = sql.trim().trim_end_matches(';');
        let lower = sql_clean.to_lowercase();

        if !lower.starts_with("update ") {
            return Err(anyhow!("Invalid UPDATE syntax"));
        }

        // Extract table name
        let after_update = &sql_clean[7..].trim(); // Skip "UPDATE "
        let set_pos = lower[7..]
            .find(" set ")
            .ok_or_else(|| anyhow!("Missing SET clause"))?;
        let table_name = after_update[..set_pos].trim();

        // Extract SET clause
        let after_set = &after_update[set_pos + 5..].trim(); // Skip " SET "
        let where_pos = after_set.to_lowercase().find(" where ");

        let set_clause = if let Some(pos) = where_pos {
            &after_set[..pos]
        } else {
            after_set
        };

        // Parse SET assignments (column = value)
        let mut updates = HashMap::new();
        for assignment in set_clause.split(',') {
            let parts: Vec<&str> = assignment.trim().split('=').collect();
            if parts.len() != 2 {
                return Err(anyhow!("Invalid SET assignment: {}", assignment));
            }
            let column = parts[0].trim();
            let value = self.parse_sql_value(parts[1].trim())?;
            updates.insert(column.to_string(), value);
        }

        // Check if we're in a transaction
        let in_transaction = self
            .transaction_manager
            .is_in_transaction(&self.session_id)?;
        info!(
            "UPDATE: in_transaction={}, session={}",
            in_transaction, self.session_id
        );

        if in_transaction {
            // Buffer UPDATE operations for transaction
            // Collect all pending writes first, then buffer them after dropping the lock
            let pending_writes = {
                let engine = self.engine_read()?;

                // Get all data from table
                let all_data = engine
                    .get_table_data(table_name)
                    .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

                // Parse WHERE conditions if present
                let conditions = if let Some(pos) = where_pos {
                    let where_clause = &after_set[pos + 7..].trim();
                    Some(self.parse_where_clause(where_clause)?)
                } else {
                    None
                };

                // Collect matching rows to update
                let mut writes = Vec::new();
                for row in all_data {
                    if let Value::Object(mut map) = row {
                        // Check if row matches WHERE conditions (if any)
                        let matches = if let Some(ref conds) = conditions {
                            conds.iter().all(|(column, operator, value)| {
                                if let Some(field_value) = map.get(column) {
                                    self.matches_condition(field_value, operator, value)
                                } else {
                                    false
                                }
                            })
                        } else {
                            true // No WHERE clause means update all
                        };

                        if matches {
                            // Get primary key for this row
                            let primary_key = map
                                .get("id")
                                .cloned()
                                .ok_or_else(|| anyhow!("Row missing primary key"))?;

                            // Apply updates to the row
                            for (col, val) in &updates {
                                map.insert(col.clone(), val.clone());
                            }

                            // Collect the update operation
                            let write = PendingWrite {
                                table: table_name.to_string(),
                                operation: WriteOperation::Update { id: primary_key },
                                data: Value::Object(map.clone()),
                            };
                            writes.push(write);
                        }
                    }
                }
                writes
            }; // engine guard is dropped here

            // Now buffer all the writes (can safely await now)
            let updated_count = pending_writes.len();
            for write in pending_writes {
                self.transaction_manager
                    .add_pending_write(&self.session_id, write)
                    .await
                    .map_err(|e| anyhow!("Failed to buffer transaction write: {}", e))?;
            }

            info!(
                "Buffered {} UPDATE operations for transaction in session {}",
                updated_count, self.session_id
            );
            Ok(QueryResult::Update {
                count: updated_count,
            })
        } else {
            // Not in transaction, apply immediately
            let mut engine = self.engine_write()?;

            // Get all data from table
            let all_data = engine
                .get_table_data(table_name)
                .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

            // Parse WHERE conditions if present
            let conditions = if let Some(pos) = where_pos {
                let where_clause = &after_set[pos + 7..].trim();
                Some(self.parse_where_clause(where_clause)?)
            } else {
                None
            };

            // Count updated rows
            let mut updated_count = 0;

            // Apply updates to matching rows
            for row in all_data {
                if let Value::Object(mut map) = row {
                    // Check if row matches WHERE conditions (if any)
                    let matches = if let Some(ref conds) = conditions {
                        conds.iter().all(|(column, operator, value)| {
                            if let Some(field_value) = map.get(column) {
                                self.matches_condition(field_value, operator, value)
                            } else {
                                false
                            }
                        })
                    } else {
                        true // No WHERE clause means update all
                    };

                    if matches {
                        // Get primary key for this row
                        let primary_key = map
                            .get("id")
                            .cloned()
                            .ok_or_else(|| anyhow!("Row missing primary key"))?;

                        // Apply updates to the row
                        for (col, val) in &updates {
                            map.insert(col.clone(), val.clone());
                        }

                        // Save updated row back to database
                        engine.update_record(table_name, primary_key, Value::Object(map))?;
                        updated_count += 1;
                    }
                }
            }

            Ok(QueryResult::Update {
                count: updated_count,
            })
        }
    }

    async fn execute_delete(&self, sql: &str) -> Result<QueryResult> {
        // Parse DELETE FROM table_name [WHERE condition]
        let sql_clean = sql.trim().trim_end_matches(';');
        let lower = sql_clean.to_lowercase();

        if !lower.starts_with("delete from ") {
            return Err(anyhow!("Invalid DELETE syntax"));
        }

        // Extract table name
        let after_delete = &sql_clean[12..].trim(); // Skip "DELETE FROM "
        let where_pos = after_delete.to_lowercase().find(" where ");

        let table_name = if let Some(pos) = where_pos {
            &after_delete[..pos].trim()
        } else {
            after_delete
        };

        // Check if we're in a transaction
        let in_transaction = self
            .transaction_manager
            .is_in_transaction(&self.session_id)?;
        info!(
            "DELETE: in_transaction={}, session={}",
            in_transaction, self.session_id
        );

        if in_transaction {
            // Buffer DELETE operations for transaction
            // Collect all pending writes first, then buffer them after dropping the lock
            let pending_writes = {
                let engine = self.engine_read()?;

                // Get all data from table
                let all_data = engine
                    .get_table_data(table_name)
                    .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

                // Parse WHERE conditions if present
                let conditions = if let Some(pos) = where_pos {
                    let where_clause = &after_delete[pos + 7..].trim();
                    Some(self.parse_where_clause(where_clause)?)
                } else {
                    None
                };

                // Collect matching rows to delete
                let mut writes = Vec::new();
                for row in all_data {
                    if let Value::Object(map) = row {
                        // Check if row matches WHERE conditions (if any)
                        let matches = if let Some(ref conds) = conditions {
                            conds.iter().all(|(column, operator, value)| {
                                if let Some(field_value) = map.get(column) {
                                    self.matches_condition(field_value, operator, value)
                                } else {
                                    false
                                }
                            })
                        } else {
                            true // No WHERE clause means delete all
                        };

                        if matches {
                            // Get primary key for this row
                            let primary_key = map
                                .get("id")
                                .cloned()
                                .ok_or_else(|| anyhow!("Row missing primary key"))?;

                            // Collect the delete operation
                            let write = PendingWrite {
                                table: table_name.to_string(),
                                operation: WriteOperation::Delete { id: primary_key },
                                data: Value::Object(map.clone()),
                            };
                            writes.push(write);
                        }
                    }
                }
                writes
            }; // engine guard is dropped here

            // Now buffer all the writes (can safely await now)
            let deleted_count = pending_writes.len();
            for write in pending_writes {
                self.transaction_manager
                    .add_pending_write(&self.session_id, write)
                    .await
                    .map_err(|e| anyhow!("Failed to buffer transaction write: {}", e))?;
            }

            info!(
                "Buffered {} DELETE operations for transaction in session {}",
                deleted_count, self.session_id
            );
            Ok(QueryResult::Delete {
                count: deleted_count,
            })
        } else {
            // Not in transaction, apply immediately
            let mut engine = self.engine_write()?;

            // Get all data from table
            let all_data = engine
                .get_table_data(table_name)
                .map_err(|e| anyhow!("Failed to get table data: {}", e))?;

            // Parse WHERE conditions if present
            let conditions = if let Some(pos) = where_pos {
                let where_clause = &after_delete[pos + 7..].trim();
                Some(self.parse_where_clause(where_clause)?)
            } else {
                None
            };

            // Count deleted rows
            let mut deleted_count = 0;

            // Delete matching rows
            for row in all_data {
                if let Value::Object(map) = row {
                    // Check if row matches WHERE conditions (if any)
                    let matches = if let Some(ref conds) = conditions {
                        conds.iter().all(|(column, operator, value)| {
                            if let Some(field_value) = map.get(column) {
                                self.matches_condition(field_value, operator, value)
                            } else {
                                false
                            }
                        })
                    } else {
                        true // No WHERE clause means delete all (dangerous!)
                    };

                    if matches {
                        // Get primary key for this row
                        let primary_key = map
                            .get("id")
                            .cloned()
                            .ok_or_else(|| anyhow!("Row missing primary key"))?;

                        // Validate FK referential integrity before deleting
                        self.validate_fk_delete(table_name, &Value::Object(map.clone()))?;

                        // Delete the row (soft delete for audit trail)
                        engine.delete_record(table_name, primary_key)?;
                        deleted_count += 1;
                    }
                }
            }

            Ok(QueryResult::Delete {
                count: deleted_count,
            })
        }
    }

    async fn execute_create_table(&self, sql: &str) -> Result<QueryResult> {
        let mut engine = self.engine_write()?;

        // Parse CREATE TABLE name (columns...)
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
        let paren_open = after_create.find('(').unwrap();
        // Find matching close paren
        let col_body = {
            let s = &after_create[paren_open + 1..];
            let mut depth = 1usize;
            let mut end = 0;
            for (i, ch) in s.char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
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

        // Parse column definitions and determine primary key
        let mut primary_key = "id".to_string();
        for part in col_body.split(',') {
            let part_upper = part.trim().to_uppercase();
            // Detect explicit PRIMARY KEY constraint: "col_name ... PRIMARY KEY"
            if part_upper.contains("PRIMARY KEY") {
                let col_name = part.split_whitespace().next().unwrap_or("id");
                primary_key = col_name.to_string();
                break;
            }
        }

        engine
            .create_table(table_name, &primary_key, vec![])
            .map_err(|e| anyhow!("Create table failed: {}", e))?;

        // Register FK constraints if present
        self.register_fk_constraints(table_name, col_body);

        info!("Table {} created successfully", table_name);
        Ok(QueryResult::CreateTable)
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
        let query_start = sql.find(" AS ").unwrap() + 4;
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
            let param_start = sql.find('(').unwrap() + 1;
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
        let on_pos = join_clause.to_lowercase().find(" on ");
        if on_pos.is_none() {
            return Err(anyhow!("Missing ON clause in JOIN"));
        }

        let on_pos = on_pos.unwrap();
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
            pairs.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

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

                        if !self.matches_condition(&field_value, operator, value) {
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
                                && self.matches_condition(&column_value, operator, &result_row[0])
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
                                && !self.matches_condition(&column_value, operator, &result_row[0])
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
                        Ok(self.matches_condition(&column_value, operator, &rows[0][0]))
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
                Ok(rows.pop().unwrap().pop().unwrap())
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

        // Look for simpler AS OF syntax (legacy DriftDB)
        if let Some(pos) = lower.find(" as of ") {
            let temporal_start = pos + 7; // Length of " as of "
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
            debug!("Parsing temporal value (legacy): {}", temporal_value);

            // Parse the temporal point
            let point = self.parse_temporal_point(temporal_value)?;
            return Ok(Some(TemporalClause::AsOf(point)));
        }

        Ok(None)
    }

    /// Parse a temporal point (timestamp or sequence number)
    fn parse_temporal_point(&self, value: &str) -> Result<TemporalPoint> {
        let trimmed = value.trim();

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
                    None => engine
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
                let ordering = self.compare_values(&left_value, &right_value);
                Ok(ordering == std::cmp::Ordering::Less)
            }
            ">" => {
                let ordering = self.compare_values(&left_value, &right_value);
                Ok(ordering == std::cmp::Ordering::Greater)
            }
            "<=" => {
                let ordering = self.compare_values(&left_value, &right_value);
                Ok(ordering != std::cmp::Ordering::Greater)
            }
            ">=" => {
                let ordering = self.compare_values(&left_value, &right_value);
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
                                        if !self.matches_condition(field_value, op, val) {
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

    /// Execute set operations
    async fn execute_set_operation(&self, set_op: &SetOperationQuery) -> Result<QueryResult> {
        debug!("Executing set operation: {:?}", set_op.operation);

        // Execute both SELECT queries (using Box::pin for recursion)
        let left_result = Box::pin(self.execute_select(&set_op.left)).await?;
        let right_result = Box::pin(self.execute_select(&set_op.right)).await?;

        // Extract columns and rows from results
        let (left_columns, left_rows) = match left_result {
            QueryResult::Select { columns, rows } => (columns, rows),
            _ => return Err(anyhow!("Left side of set operation must be a SELECT query")),
        };

        let (right_columns, right_rows) = match right_result {
            QueryResult::Select { columns, rows } => (columns, rows),
            _ => {
                return Err(anyhow!(
                    "Right side of set operation must be a SELECT query"
                ))
            }
        };

        // Validate that column counts match
        if left_columns.len() != right_columns.len() {
            return Err(anyhow!(
                "Set operation requires equal number of columns: {} vs {}",
                left_columns.len(),
                right_columns.len()
            ));
        }

        // Apply the set operation
        let result_rows = match set_op.operation {
            SetOperation::Union => self.apply_union(left_rows, right_rows, false),
            SetOperation::UnionAll => self.apply_union(left_rows, right_rows, true),
            SetOperation::Intersect => self.apply_intersect(left_rows, right_rows, false),
            SetOperation::IntersectAll => self.apply_intersect(left_rows, right_rows, true),
            SetOperation::Except => self.apply_except(left_rows, right_rows, false),
            SetOperation::ExceptAll => self.apply_except(left_rows, right_rows, true),
        };

        Ok(QueryResult::Select {
            columns: left_columns, // Use left columns as the result columns
            rows: result_rows,
        })
    }

    /// Apply UNION operation
    fn apply_union(
        &self,
        left: Vec<Vec<Value>>,
        right: Vec<Vec<Value>>,
        keep_duplicates: bool,
    ) -> Vec<Vec<Value>> {
        use std::collections::HashSet;

        if keep_duplicates {
            // UNION ALL - just concatenate
            let mut result = left;
            result.extend(right);
            result
        } else {
            // UNION - remove duplicates
            let mut seen = HashSet::new();
            let mut result = Vec::new();

            for row in left.into_iter().chain(right) {
                let key = format!("{:?}", row);
                if seen.insert(key) {
                    result.push(row);
                }
            }

            result
        }
    }

    /// Apply INTERSECT operation
    fn apply_intersect(
        &self,
        left: Vec<Vec<Value>>,
        right: Vec<Vec<Value>>,
        keep_duplicates: bool,
    ) -> Vec<Vec<Value>> {
        use std::collections::{HashMap, HashSet};

        if keep_duplicates {
            // INTERSECT ALL - keep minimum count of duplicates
            let mut right_counts = HashMap::new();
            for row in &right {
                let key = format!("{:?}", row);
                *right_counts.entry(key).or_insert(0) += 1;
            }

            let mut result = Vec::new();
            for row in left {
                let key = format!("{:?}", row);
                if let Some(count) = right_counts.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        result.push(row);
                    }
                }
            }

            result
        } else {
            // INTERSECT - only distinct common rows
            let right_set: HashSet<String> = right.iter().map(|row| format!("{:?}", row)).collect();

            let mut seen = HashSet::new();
            let mut result = Vec::new();

            for row in left {
                let key = format!("{:?}", row);
                if right_set.contains(&key) && seen.insert(key) {
                    result.push(row);
                }
            }

            result
        }
    }

    /// Apply EXCEPT operation
    fn apply_except(
        &self,
        left: Vec<Vec<Value>>,
        right: Vec<Vec<Value>>,
        keep_duplicates: bool,
    ) -> Vec<Vec<Value>> {
        use std::collections::{HashMap, HashSet};

        if keep_duplicates {
            // EXCEPT ALL - subtract counts
            let mut right_counts = HashMap::new();
            for row in &right {
                let key = format!("{:?}", row);
                *right_counts.entry(key).or_insert(0) += 1;
            }

            let mut result = Vec::new();
            for row in left {
                let key = format!("{:?}", row);
                let count = right_counts.get_mut(&key).map(|c| *c).unwrap_or(0);
                if count == 0 {
                    result.push(row);
                } else {
                    right_counts.insert(key, count - 1);
                }
            }

            result
        } else {
            // EXCEPT - remove any rows that appear in right
            let right_set: HashSet<String> = right.iter().map(|row| format!("{:?}", row)).collect();

            let mut seen = HashSet::new();
            let mut result = Vec::new();

            for row in left {
                let key = format!("{:?}", row);
                if !right_set.contains(&key) && seen.insert(key) {
                    result.push(row);
                }
            }

            result
        }
    }

    /// Format results for specific column selection from joined data
    fn format_join_column_selection_results(
        &self,
        joined_data: Vec<Value>,
        all_columns: &[String],
        selected_columns: &[String],
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        if joined_data.is_empty() {
            return Ok(QueryResult::Select {
                columns: selected_columns.to_vec(),
                rows: vec![],
            });
        }

        // Helper: check whether a SELECT item is a literal constant (number, quoted
        // string, NULL, TRUE, FALSE).  Literals are always "valid" — they don't
        // need to exist as a column in the joined data.
        let is_literal = |col: &str| -> bool {
            let trimmed = col.trim();
            if trimmed.eq_ignore_ascii_case("null")
                || trimmed.eq_ignore_ascii_case("true")
                || trimmed.eq_ignore_ascii_case("false")
            {
                return true;
            }
            if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
                return true;
            }
            trimmed.parse::<f64>().is_ok()
        };

        // Helper: evaluate a literal constant SELECT item.
        let eval_literal = |col: &str| -> Value {
            let trimmed = col.trim();
            if trimmed.eq_ignore_ascii_case("null") {
                return Value::Null;
            }
            if trimmed.eq_ignore_ascii_case("true") {
                return Value::Bool(true);
            }
            if trimmed.eq_ignore_ascii_case("false") {
                return Value::Bool(false);
            }
            if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
                return Value::String(trimmed[1..trimmed.len() - 1].to_string());
            }
            if let Ok(n) = trimmed.parse::<i64>() {
                return Value::Number(n.into());
            }
            if let Ok(f) = trimmed.parse::<f64>() {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    return Value::Number(n);
                }
            }
            Value::Null
        };

        // Validate that all requested columns exist (literals are always valid).
        let first = &joined_data[0];
        if let Value::Object(map) = first {
            for col in selected_columns {
                if is_literal(col) {
                    continue; // constants don't need to exist as columns
                }
                // Check if column exists directly, with a table prefix, or stripped prefix.
                let bare_col = col.rfind('.').map(|p| &col[p + 1..]).unwrap_or(col.as_str());
                let column_exists = map.contains_key(col)
                    || map.contains_key(bare_col)
                    || map.keys().any(|key| key.ends_with(&format!(".{}", col)))
                    || all_columns.contains(col)
                    || all_columns
                        .iter()
                        .any(|c| c.ends_with(&format!(".{}", col)));

                if !column_exists {
                    return Err(anyhow!(
                        "Column '{}' does not exist in the joined result",
                        col
                    ));
                }
            }
        }

        // Extract only the requested columns from each row
        let mut rows: Vec<Vec<Value>> = joined_data
            .into_iter()
            .map(|record| {
                if let Value::Object(map) = record {
                    selected_columns
                        .iter()
                        .map(|col| {
                            // Literal constant: return the value directly.
                            if is_literal(col) {
                                return eval_literal(col);
                            }

                            // Try exact match first
                            if let Some(value) = map.get(col) {
                                return value.clone();
                            }

                            // Strip table prefix (e.g. "p.customer_id" → "customer_id")
                            if let Some(dot) = col.rfind('.') {
                                let bare = &col[dot + 1..];
                                if let Some(value) = map.get(bare) {
                                    return value.clone();
                                }
                            }

                            // Try to find column with any table prefix
                            for (key, value) in &map {
                                if key.ends_with(&format!(".{}", col)) {
                                    return value.clone();
                                }
                            }

                            Value::Null
                        })
                        .collect()
                } else {
                    vec![Value::Null; selected_columns.len()]
                }
            })
            .collect();

        // Apply ORDER BY if specified
        if let Some(ref order_by) = order_by {
            debug!(
                "Applying ORDER BY: {} {:?}",
                order_by.column, order_by.direction
            );
            self.sort_rows(&mut rows, selected_columns, order_by)?;
        }

        // Apply LIMIT if specified
        if let Some(limit_count) = limit {
            debug!("Applying LIMIT: {}", limit_count);
            rows.truncate(limit_count);
        }

        debug!(
            "Returning {} rows with {} selected columns: {:?}",
            rows.len(),
            selected_columns.len(),
            selected_columns
        );
        Ok(QueryResult::Select {
            columns: selected_columns.to_vec(),
            rows,
        })
    }

    /// Format results for SELECT * from joined data
    fn format_join_select_all_results(
        &self,
        joined_data: Vec<Value>,
        all_columns: Vec<String>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
    ) -> Result<QueryResult> {
        if joined_data.is_empty() {
            return Ok(QueryResult::Select {
                columns: all_columns,
                rows: vec![],
            });
        }

        // Use the columns from the JOIN operation
        let columns = all_columns;

        let mut rows: Vec<Vec<Value>> = joined_data
            .into_iter()
            .map(|record| {
                if let Value::Object(map) = record {
                    columns
                        .iter()
                        .map(|col| {
                            // Try full prefixed name first, then unprefixed (after the dot)
                            map.get(col)
                                .or_else(|| {
                                    col.split('.')
                                        .next_back()
                                        .and_then(|suffix| map.get(suffix))
                                })
                                .cloned()
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                } else {
                    vec![Value::Null; columns.len()]
                }
            })
            .collect();

        // Strip table prefix from column names for cleaner output
        let columns: Vec<String> = columns
            .into_iter()
            .map(|col| col.split('.').next_back().unwrap_or(&col).to_string())
            .collect();

        // Apply ORDER BY if specified
        if let Some(ref order_by) = order_by {
            debug!(
                "Applying ORDER BY: {} {:?}",
                order_by.column, order_by.direction
            );
            self.sort_rows(&mut rows, &columns, order_by)?;
        }

        // Apply LIMIT if specified
        if let Some(limit_count) = limit {
            debug!("Applying LIMIT: {}", limit_count);
            rows.truncate(limit_count);
        }

        debug!(
            "Returning {} joined rows with {} columns: {:?}",
            rows.len(),
            columns.len(),
            columns
        );
        Ok(QueryResult::Select { columns, rows })
    }

    // ==================== TRANSACTION METHODS ====================

    /// Execute BEGIN transaction
    async fn execute_begin(&self, sql: &str) -> Result<QueryResult> {
        let lower = sql.to_lowercase();

        // Parse isolation level if specified
        let isolation_level = if lower.contains("read uncommitted") {
            IsolationLevel::ReadUncommitted
        } else if lower.contains("read committed") {
            IsolationLevel::ReadCommitted
        } else if lower.contains("repeatable read") {
            IsolationLevel::RepeatableRead
        } else if lower.contains("serializable") {
            IsolationLevel::Serializable
        } else {
            IsolationLevel::default()
        };

        // Check for read-only mode
        let is_read_only = lower.contains("read only");

        // Begin transaction
        let txn_id = self
            .transaction_manager
            .begin_transaction(&self.session_id, isolation_level, is_read_only)
            .await?;

        info!(
            "Started transaction {} for session {}",
            txn_id, self.session_id
        );
        Ok(QueryResult::Begin)
    }

    /// Execute COMMIT transaction
    async fn execute_commit(&self) -> Result<QueryResult> {
        self.transaction_manager
            .commit_transaction(&self.session_id)
            .await?;

        info!("Committed transaction for session {}", self.session_id);
        Ok(QueryResult::Commit)
    }

    /// Execute ROLLBACK transaction
    async fn execute_rollback(&self, sql: &str) -> Result<QueryResult> {
        let lower = sql.to_lowercase();

        // Check for ROLLBACK TO SAVEPOINT
        if lower.contains("to savepoint ") || lower.contains("to ") {
            let savepoint_pos = lower
                .find("to savepoint ")
                .map(|p| p + 13)
                .or_else(|| lower.find("to ").map(|p| p + 3))
                .unwrap();

            let savepoint_name = sql[savepoint_pos..].trim().trim_matches(';');

            self.transaction_manager
                .rollback_to_savepoint(&self.session_id, savepoint_name)?;

            info!(
                "Rolled back to savepoint {} for session {}",
                savepoint_name, self.session_id
            );
        } else {
            // Full rollback
            self.transaction_manager
                .rollback_transaction(&self.session_id)
                .await?;

            info!("Rolled back transaction for session {}", self.session_id);
        }

        Ok(QueryResult::Rollback)
    }

    /// Execute SAVEPOINT
    async fn execute_savepoint(&self, sql: &str) -> Result<QueryResult> {
        let savepoint_pos = sql
            .to_lowercase()
            .find("savepoint ")
            .ok_or_else(|| anyhow!("Invalid SAVEPOINT syntax"))?;

        let savepoint_name = sql[savepoint_pos + 10..]
            .trim()
            .trim_matches(';')
            .to_string();

        self.transaction_manager
            .create_savepoint(&self.session_id, savepoint_name.clone())?;

        info!(
            "Created savepoint {} for session {}",
            savepoint_name, self.session_id
        );
        Ok(QueryResult::Empty)
    }

    /// Execute RELEASE SAVEPOINT
    async fn execute_release_savepoint(&self, sql: &str) -> Result<QueryResult> {
        let savepoint_pos = sql
            .to_lowercase()
            .find("savepoint ")
            .ok_or_else(|| anyhow!("Invalid RELEASE SAVEPOINT syntax"))?;

        let savepoint_name = sql[savepoint_pos + 10..].trim().trim_matches(';');

        // For now, just log - actual release would remove from savepoint list
        info!(
            "Released savepoint {} for session {}",
            savepoint_name, self.session_id
        );
        Ok(QueryResult::Empty)
    }

    /// Check if we're in a transaction
    fn in_transaction(&self) -> bool {
        self.transaction_manager.has_transaction(&self.session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;

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

    #[tokio::test]
    async fn test_compare_values() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        let executor = QueryExecutor::new(engine);

        use std::cmp::Ordering;

        // Test number comparison
        let a = Value::Number(10.into());
        let b = Value::Number(20.into());
        assert_eq!(executor.compare_values(&a, &b), Ordering::Less);
        assert_eq!(executor.compare_values(&b, &a), Ordering::Greater);
        assert_eq!(executor.compare_values(&a, &a), Ordering::Equal);

        // Test string comparison
        let a = Value::String("apple".to_string());
        let b = Value::String("banana".to_string());
        assert_eq!(executor.compare_values(&a, &b), Ordering::Less);
        assert_eq!(executor.compare_values(&b, &a), Ordering::Greater);

        // Test null handling
        let null = Value::Null;
        let num = Value::Number(5.into());
        assert_eq!(executor.compare_values(&null, &num), Ordering::Less);
        assert_eq!(executor.compare_values(&num, &null), Ordering::Greater);
        assert_eq!(executor.compare_values(&null, &null), Ordering::Equal);
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
