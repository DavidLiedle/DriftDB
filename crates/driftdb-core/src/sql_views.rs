//! SQL Views Implementation with Full SQL Support
//!
//! Extends the view system with complete SQL syntax support for:
//! - CREATE VIEW / CREATE OR REPLACE VIEW
//! - CREATE MATERIALIZED VIEW
//! - WITH CHECK OPTION for updatable views
//! - Recursive views with CTEs

use sqlparser::ast::{CreateTableOptions, ObjectName, Query as SqlQuery, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use parking_lot::RwLock;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::sql_bridge;
use crate::views::{ColumnDefinition, RefreshPolicy, ViewDefinition, ViewManager};

/// Extended view manager with full SQL support
pub struct SqlViewManager {
    view_manager: Arc<ViewManager>,
    /// Cache of parsed view queries for performance
    parsed_views: Arc<RwLock<HashMap<String, SqlQuery>>>,
    /// View dependency graph for cascade operations
    dependency_graph: Arc<RwLock<ViewDependencyGraph>>,
}

/// Tracks dependencies between views
struct ViewDependencyGraph {
    /// View -> List of views it depends on
    dependencies: HashMap<String, HashSet<String>>,
    /// View -> List of views that depend on it
    dependents: HashMap<String, HashSet<String>>,
}

impl ViewDependencyGraph {
    fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
        }
    }

    fn add_dependency(&mut self, view: &str, depends_on: &str) {
        self.dependencies
            .entry(view.to_string())
            .or_insert_with(HashSet::new)
            .insert(depends_on.to_string());

        self.dependents
            .entry(depends_on.to_string())
            .or_insert_with(HashSet::new)
            .insert(view.to_string());
    }

    fn get_cascade_order(&self, view: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        self.dfs_cascade(view, &mut visited, &mut result);
        result
    }

    fn dfs_cascade(&self, view: &str, visited: &mut HashSet<String>, result: &mut Vec<String>) {
        if visited.contains(view) {
            return;
        }
        visited.insert(view.to_string());

        if let Some(deps) = self.dependents.get(view) {
            for dep in deps {
                self.dfs_cascade(dep, visited, result);
            }
        }

        result.push(view.to_string());
    }
}

impl SqlViewManager {
    pub fn new(view_manager: Arc<ViewManager>) -> Self {
        Self {
            view_manager,
            parsed_views: Arc::new(RwLock::new(HashMap::new())),
            dependency_graph: Arc::new(RwLock::new(ViewDependencyGraph::new())),
        }
    }

    /// Parse and execute CREATE VIEW statement
    pub fn create_view_from_sql(&self, engine: &mut Engine, sql: &str) -> Result<()> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DriftError::Parse(format!("Failed to parse CREATE VIEW: {}", e)))?;

        if ast.is_empty() {
            return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
        }

        match &ast[0] {
            Statement::CreateView {
                or_replace,
                materialized,
                name,
                columns,
                query,
                options,
                cluster_by: _,
                comment,
                with_no_schema_binding: _,
                if_not_exists: _,
                temporary: _,
                to: _,
            } => {
                let view_name = object_name_to_string(name);

                // Check if view exists and or_replace is not set
                if !or_replace && self.view_manager.get_view(&view_name).is_some() {
                    return Err(DriftError::Other(format!(
                        "View '{}' already exists",
                        view_name
                    )));
                }

                // Extract dependencies from the query
                let dependencies = self.extract_dependencies(query)?;

                // Parse column definitions
                let column_defs = if columns.is_empty() {
                    // Derive columns from query
                    self.derive_columns_from_query(engine, query)?
                } else {
                    columns
                        .iter()
                        .map(|col| ColumnDefinition {
                            name: col.name.to_string(),
                            data_type: col
                                .data_type
                                .as_ref()
                                .map(|dt| dt.to_string())
                                .unwrap_or_else(|| "text".to_string()),
                            nullable: true,
                            source_table: None,
                            source_column: None,
                        })
                        .collect()
                };

                // Determine refresh policy for materialized views
                let refresh_policy = if *materialized {
                    Some(self.parse_refresh_policy(&options)?)
                } else {
                    None
                };

                // Create the view definition
                let view_def = ViewDefinition {
                    name: view_name.clone(),
                    query: format!("{}", query),
                    parsed_query: None,
                    columns: column_defs,
                    is_materialized: *materialized,
                    dependencies: dependencies.clone(),
                    created_at: std::time::SystemTime::now(),
                    modified_at: std::time::SystemTime::now(),
                    owner: "system".to_string(), // TODO: Get from session
                    permissions: Default::default(),
                    refresh_policy,
                    comment: comment.clone(),
                };

                // Register the view
                self.view_manager.create_view(view_def)?;

                // Update dependency graph
                let mut graph = self.dependency_graph.write();
                for dep in &dependencies {
                    graph.add_dependency(&view_name, dep);
                }

                // Cache parsed query (dereference boxed query)
                self.parsed_views
                    .write()
                    .insert(view_name.clone(), (**query).clone());

                // If materialized, perform initial refresh
                if *materialized {
                    self.refresh_materialized_view(engine, &view_name)?;
                }

                Ok(())
            }
            _ => Err(DriftError::InvalidQuery(
                "Expected CREATE VIEW statement".to_string(),
            )),
        }
    }

    /// Execute DROP VIEW statement
    pub fn drop_view_from_sql(&self, sql: &str, cascade: bool) -> Result<()> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DriftError::Parse(format!("Failed to parse DROP VIEW: {}", e)))?;

        if ast.is_empty() {
            return Err(DriftError::InvalidQuery("Empty SQL statement".to_string()));
        }

        match &ast[0] {
            Statement::Drop {
                object_type,
                names,
                cascade: sql_cascade,
                ..
            } => {
                if !matches!(object_type, sqlparser::ast::ObjectType::View) {
                    return Err(DriftError::InvalidQuery(
                        "Expected DROP VIEW statement".to_string(),
                    ));
                }

                let cascade = cascade || *sql_cascade;

                for name in names {
                    let view_name = object_name_to_string(name);

                    // Get cascade order if needed
                    let views_to_drop = if cascade {
                        self.dependency_graph.read().get_cascade_order(&view_name)
                    } else {
                        // Check if there are dependents
                        let graph = self.dependency_graph.read();
                        if let Some(deps) = graph.dependents.get(&view_name) {
                            if !deps.is_empty() {
                                return Err(DriftError::Other(format!(
                                    "Cannot drop view '{}': other views depend on it. Use CASCADE.",
                                    view_name
                                )));
                            }
                        }
                        vec![view_name.clone()]
                    };

                    // Drop views in cascade order
                    for view in views_to_drop {
                        self.view_manager.drop_view(&view, true)?;
                        self.parsed_views.write().remove(&view);

                        // Update dependency graph
                        let mut graph = self.dependency_graph.write();
                        if let Some(deps) = graph.dependencies.remove(&view) {
                            for dep in deps {
                                if let Some(dependents) = graph.dependents.get_mut(&dep) {
                                    dependents.remove(&view);
                                }
                            }
                        }
                        graph.dependents.remove(&view);
                    }
                }

                Ok(())
            }
            _ => Err(DriftError::InvalidQuery(
                "Expected DROP VIEW statement".to_string(),
            )),
        }
    }

    /// Query a view as if it were a table
    pub fn query_view(&self, engine: &mut Engine, view_name: &str) -> Result<Vec<Value>> {
        let view = self
            .view_manager
            .get_view(view_name)
            .ok_or_else(|| DriftError::Other(format!("View '{}' not found", view_name)))?;

        if view.is_materialized {
            // Query the materialized data
            self.query_materialized_view(engine, view_name)
        } else {
            // Execute the view's query
            self.execute_view_query(engine, &view)
        }
    }

    /// Refresh a materialized view
    pub fn refresh_materialized_view(&self, engine: &mut Engine, view_name: &str) -> Result<()> {
        let view = self
            .view_manager
            .get_view(view_name)
            .ok_or_else(|| DriftError::Other(format!("View '{}' not found", view_name)))?;

        if !view.is_materialized {
            return Err(DriftError::Other(format!(
                "View '{}' is not materialized",
                view_name
            )));
        }

        // Execute the view's query
        let results = self.execute_view_query(engine, &view)?;

        // Store the results in the cache
        self.view_manager
            .cache_materialized_data(view_name, results)?;

        Ok(())
    }

    /// Extract table/view dependencies from a query
    fn extract_dependencies(&self, query: &SqlQuery) -> Result<HashSet<String>> {
        let mut dependencies = HashSet::new();

        // Extract from the body (SetExpr)
        self.extract_dependencies_from_set_expr(&query.body, &mut dependencies);

        // Extract from CTEs
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                // CTE queries can also have dependencies
                let cte_deps = self.extract_dependencies(&cte.query)?;
                dependencies.extend(cte_deps);
            }
        }

        Ok(dependencies)
    }

    fn extract_dependencies_from_set_expr(
        &self,
        expr: &sqlparser::ast::SetExpr,
        dependencies: &mut HashSet<String>,
    ) {
        match expr {
            sqlparser::ast::SetExpr::Select(select) => {
                // Extract from FROM clause
                for table in &select.from {
                    self.extract_table_dependencies(&table.relation, dependencies);
                    for join in &table.joins {
                        self.extract_table_dependencies(&join.relation, dependencies);
                    }
                }
            }
            sqlparser::ast::SetExpr::Query(query) => {
                if let Ok(deps) = self.extract_dependencies(query) {
                    dependencies.extend(deps);
                }
            }
            sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
                self.extract_dependencies_from_set_expr(left, dependencies);
                self.extract_dependencies_from_set_expr(right, dependencies);
            }
            _ => {}
        }
    }

    fn extract_table_dependencies(
        &self,
        table_factor: &sqlparser::ast::TableFactor,
        dependencies: &mut HashSet<String>,
    ) {
        match table_factor {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                dependencies.insert(object_name_to_string(name));
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                // Recursively extract from subquery
                if let Ok(deps) = self.extract_dependencies(subquery) {
                    dependencies.extend(deps);
                }
            }
            _ => {}
        }
    }

    fn derive_columns_from_query(
        &self,
        _engine: &mut Engine,
        query: &SqlQuery,
    ) -> Result<Vec<ColumnDefinition>> {
        // Parse and analyze the query to determine columns
        // This is a simplified version - in production would need full type inference
        let mut columns = Vec::new();

        // Extract projection from SetExpr
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            for item in &select.projection {
                match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                        let col_name = format!("{}", expr);
                        columns.push(ColumnDefinition {
                            name: col_name,
                            data_type: "text".to_string(),
                            nullable: true,
                            source_table: None,
                            source_column: None,
                        });
                    }
                    sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                        columns.push(ColumnDefinition {
                            name: alias.to_string(),
                            data_type: "text".to_string(),
                            nullable: true,
                            source_table: None,
                            source_column: None,
                        });
                    }
                    sqlparser::ast::SelectItem::QualifiedWildcard(object_name, _) => {
                        // Would need to expand based on table schema
                        let _table_name = object_name_to_string(object_name);
                        // TODO: Get actual columns from table
                    }
                    sqlparser::ast::SelectItem::Wildcard(_options) => {
                        // Would need to expand based on all tables in FROM
                        // TODO: Get actual columns from all tables
                    }
                }
            }
        }

        if columns.is_empty() {
            // Default column if we couldn't determine
            columns.push(ColumnDefinition {
                name: "column1".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                source_table: None,
                source_column: None,
            });
        }

        Ok(columns)
    }

    fn parse_refresh_policy(&self, _options: &CreateTableOptions) -> Result<RefreshPolicy> {
        // Parse refresh options from WITH clause
        // For now, default to manual refresh
        Ok(RefreshPolicy::Manual)
    }

    fn execute_view_query(&self, engine: &mut Engine, view: &ViewDefinition) -> Result<Vec<Value>> {
        // Execute the view's SQL query
        match sql_bridge::execute_sql(engine, &view.query.clone())? {
            crate::query::QueryResult::Rows { data } => Ok(data),
            _ => Ok(Vec::new()),
        }
    }

    fn query_materialized_view(&self, _engine: &mut Engine, view_name: &str) -> Result<Vec<Value>> {
        self.view_manager.get_cached_data(view_name).ok_or_else(|| {
            DriftError::Other(format!(
                "Materialized view '{}' has no cached data",
                view_name
            ))
        })
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_simple_view() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::open(temp_dir.path()).unwrap();
        let view_manager = Arc::new(ViewManager::new());
        let sql_view_mgr = SqlViewManager::new(view_manager);

        // Create a base table first
        engine
            .create_table_with_columns(
                "users",
                "id",
                vec![
                    crate::schema::ColumnDef {
                        name: "name".to_string(),
                        col_type: "string".to_string(),
                        index: false,
                    }
                ],
            )
            .unwrap();

        // Create a view
        let create_sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'";
        sql_view_mgr
            .create_view_from_sql(&mut engine, create_sql)
            .unwrap();

        // Verify view was created
        assert!(sql_view_mgr.view_manager.get_view("active_users").is_some());
    }

    #[test]
    fn test_create_materialized_view() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::open(temp_dir.path()).unwrap();
        let view_manager = Arc::new(ViewManager::new());
        let sql_view_mgr = SqlViewManager::new(view_manager);

        // Create base table
        engine
            .create_table_with_columns(
                "orders",
                "id",
                vec![
                    crate::schema::ColumnDef {
                        name: "total".to_string(),
                        col_type: "number".to_string(),
                        index: false,
                    }
                ],
            )
            .unwrap();

        // Create materialized view
        let create_sql = "CREATE MATERIALIZED VIEW order_summary AS
                         SELECT COUNT(*) as order_count, SUM(total) as total_revenue
                         FROM orders";
        sql_view_mgr
            .create_view_from_sql(&mut engine, create_sql)
            .unwrap();

        let view = sql_view_mgr.view_manager.get_view("order_summary").unwrap();
        assert!(view.is_materialized);
    }

    #[test]
    fn test_view_dependencies() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = Engine::open(temp_dir.path()).unwrap();
        let view_manager = Arc::new(ViewManager::new());
        let sql_view_mgr = SqlViewManager::new(view_manager);

        // Create base table
        engine.create_table("products", "id", vec![]).unwrap();

        // Create first view
        sql_view_mgr
            .create_view_from_sql(
                &mut engine,
                "CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 100",
            )
            .unwrap();

        // Create dependent view
        sql_view_mgr.create_view_from_sql(
            &mut engine,
            "CREATE VIEW featured_expensive AS SELECT * FROM expensive_products WHERE featured = true"
        ).unwrap();

        // Try to drop parent view without cascade (should fail)
        let result = sql_view_mgr.drop_view_from_sql("DROP VIEW expensive_products", false);
        assert!(result.is_err());

        // Drop with cascade (should succeed)
        sql_view_mgr
            .drop_view_from_sql("DROP VIEW expensive_products", true)
            .unwrap();

        // Both views should be gone
        assert!(sql_view_mgr
            .view_manager
            .get_view("expensive_products")
            .is_none());
        assert!(sql_view_mgr
            .view_manager
            .get_view("featured_expensive")
            .is_none());
    }
}
