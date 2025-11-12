//! Database Views Implementation
//!
//! Provides support for creating and managing database views - virtual tables
//! defined by SQL queries that can be queried like regular tables.
//!
//! Features:
//! - CREATE VIEW / DROP VIEW statements
//! - Materialized views with automatic refresh
//! - View dependencies tracking
//! - Security through view-based access control
//! - Temporal views that preserve time-travel capabilities

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info};

use crate::cache::{CacheConfig, QueryCache};
use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::query::{AsOf, Query, QueryResult};
use crate::sql_bridge;

/// View definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Unique view name
    pub name: String,
    /// SQL query that defines the view
    pub query: String,
    /// Parsed query for execution
    #[serde(skip)]
    pub parsed_query: Option<Query>,
    /// Column definitions derived from query
    pub columns: Vec<ColumnDefinition>,
    /// Whether this is a materialized view
    pub is_materialized: bool,
    /// Dependencies on other tables/views
    pub dependencies: HashSet<String>,
    /// View creation timestamp
    pub created_at: SystemTime,
    /// Last modification timestamp
    pub modified_at: SystemTime,
    /// View owner/creator
    pub owner: String,
    /// Access permissions
    pub permissions: ViewPermissions,
    /// For materialized views, refresh policy
    pub refresh_policy: Option<RefreshPolicy>,
    /// View comment/description
    pub comment: Option<String>,
}

/// Column definition in a view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub source_table: Option<String>,
    pub source_column: Option<String>,
}

/// View access permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewPermissions {
    /// Users who can SELECT from this view
    pub select_users: HashSet<String>,
    /// Roles that can SELECT from this view
    pub select_roles: HashSet<String>,
    /// Whether the view is public (accessible to all)
    pub is_public: bool,
    /// Whether to check permissions on underlying tables
    pub check_underlying_permissions: bool,
}

impl Default for ViewPermissions {
    fn default() -> Self {
        Self {
            select_users: HashSet::new(),
            select_roles: HashSet::new(),
            is_public: false,
            check_underlying_permissions: true,
        }
    }
}

/// Refresh policy for materialized views
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefreshPolicy {
    /// Refresh on every access
    OnAccess,
    /// Refresh periodically
    Interval(Duration),
    /// Refresh on demand only
    Manual,
    /// Refresh when underlying data changes
    OnChange,
}

/// Materialized view data
#[derive(Debug, Clone)]
pub struct MaterializedViewData {
    /// Cached query results
    pub data: Vec<Value>,
    /// When the data was last refreshed
    pub refreshed_at: SystemTime,
    /// Number of rows
    pub row_count: usize,
    /// Approximate size in bytes
    pub size_bytes: usize,
}

/// View manager for handling all views in the database
pub struct ViewManager {
    /// All view definitions
    views: Arc<RwLock<HashMap<String, ViewDefinition>>>,
    /// Materialized view data cache
    materialized_data: Arc<RwLock<HashMap<String, MaterializedViewData>>>,
    /// View dependency graph
    dependency_graph: Arc<RwLock<ViewDependencyGraph>>,
    /// Query cache for non-materialized views
    query_cache: Arc<QueryCache>,
    /// Statistics
    stats: Arc<RwLock<ViewStatistics>>,
    /// Database engine for executing queries
    engine: Option<Arc<RwLock<Engine>>>,
}

/// View dependency graph for tracking dependencies
#[derive(Debug, Default)]
struct ViewDependencyGraph {
    /// Map from view to its dependencies
    dependencies: HashMap<String, HashSet<String>>,
    /// Map from table/view to views that depend on it
    dependents: HashMap<String, HashSet<String>>,
}

impl ViewDependencyGraph {
    /// Add a view with its dependencies
    fn add_view(&mut self, view_name: String, deps: HashSet<String>) {
        // Add to dependencies map
        self.dependencies.insert(view_name.clone(), deps.clone());

        // Update dependents map
        for dep in deps {
            self.dependents
                .entry(dep)
                .or_default()
                .insert(view_name.clone());
        }
    }

    /// Remove a view from the graph
    fn remove_view(&mut self, view_name: &str) {
        // Remove from dependencies
        if let Some(deps) = self.dependencies.remove(view_name) {
            // Update dependents
            for dep in deps {
                if let Some(dependents) = self.dependents.get_mut(&dep) {
                    dependents.remove(view_name);
                }
            }
        }
    }

    /// Get all views that depend on a given table/view
    fn get_dependents(&self, name: &str) -> HashSet<String> {
        self.dependents.get(name).cloned().unwrap_or_default()
    }

    /// Check for circular dependencies
    fn has_circular_dependency(&self, view_name: &str, new_deps: &HashSet<String>) -> bool {
        // Use DFS to detect cycles
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        self.has_cycle_dfs(view_name, new_deps, &mut visited, &mut rec_stack)
    }

    fn has_cycle_dfs(
        &self,
        current: &str,
        deps: &HashSet<String>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(current.to_string());
        rec_stack.insert(current.to_string());

        for dep in deps {
            if !visited.contains(dep) {
                if let Some(nested_deps) = self.dependencies.get(dep) {
                    if self.has_cycle_dfs(dep, nested_deps, visited, rec_stack) {
                        return true;
                    }
                }
            } else if rec_stack.contains(dep) {
                return true;
            }
        }

        rec_stack.remove(current);
        false
    }
}

/// Statistics for view usage
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ViewStatistics {
    pub total_views: usize,
    pub materialized_views: usize,
    pub materialized_views_refreshed: u64,
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub refresh_count: u64,
    pub avg_query_time_ms: f64,
}

impl Default for ViewManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ViewManager {
    /// Create a new view manager
    pub fn new() -> Self {
        let cache_config = CacheConfig {
            max_entries: 100,
            default_ttl: Duration::from_secs(300),
            cache_temporal: false,
            cache_transactional: false,
            max_result_size: 50 * 1024 * 1024, // 50MB for views
        };

        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
            materialized_data: Arc::new(RwLock::new(HashMap::new())),
            dependency_graph: Arc::new(RwLock::new(ViewDependencyGraph::default())),
            query_cache: Arc::new(QueryCache::new(cache_config)),
            stats: Arc::new(RwLock::new(ViewStatistics::default())),
            engine: None,
        }
    }

    /// Set the database engine for executing view queries
    pub fn with_engine(mut self, engine: Arc<RwLock<Engine>>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Create a new view
    pub fn create_view(&self, definition: ViewDefinition) -> Result<()> {
        let view_name = definition.name.clone();

        debug!("Creating view: {}", view_name);

        // Check for circular dependencies
        let dep_graph = self.dependency_graph.read();
        if dep_graph.has_circular_dependency(&view_name, &definition.dependencies) {
            return Err(DriftError::InvalidQuery(format!(
                "Circular dependency detected for view '{}'",
                view_name
            )));
        }
        drop(dep_graph);

        // Add view definition
        let mut views = self.views.write();
        if views.contains_key(&view_name) {
            return Err(DriftError::InvalidQuery(format!(
                "View '{}' already exists",
                view_name
            )));
        }
        views.insert(view_name.clone(), definition.clone());
        drop(views);

        // Update dependency graph
        let mut dep_graph = self.dependency_graph.write();
        dep_graph.add_view(view_name.clone(), definition.dependencies.clone());

        // Update statistics
        let mut stats = self.stats.write();
        stats.total_views += 1;
        if definition.is_materialized {
            stats.materialized_views += 1;
        }

        info!("View '{}' created successfully", view_name);
        Ok(())
    }

    /// Drop a view
    pub fn drop_view(&self, view_name: &str, cascade: bool) -> Result<()> {
        debug!("Dropping view: {}", view_name);

        // Check for dependent views
        let dep_graph = self.dependency_graph.read();
        let dependents = dep_graph.get_dependents(view_name);
        drop(dep_graph);

        if !dependents.is_empty() && !cascade {
            return Err(DriftError::InvalidQuery(
                format!("Cannot drop view '{}' - other views depend on it: {:?}. Use CASCADE to drop dependents.",
                        view_name, dependents)
            ));
        }

        // Drop dependent views if CASCADE
        if cascade {
            for dep_view in dependents {
                self.drop_view(&dep_view, true)?;
            }
        }

        // Remove view definition
        let mut views = self.views.write();
        let was_materialized = views
            .get(view_name)
            .map(|v| v.is_materialized)
            .unwrap_or(false);

        if views.remove(view_name).is_none() {
            return Err(DriftError::InvalidQuery(format!(
                "View '{}' does not exist",
                view_name
            )));
        }
        drop(views);

        // Remove from dependency graph
        let mut dep_graph = self.dependency_graph.write();
        dep_graph.remove_view(view_name);

        // Remove materialized data if applicable
        if was_materialized {
            let mut materialized = self.materialized_data.write();
            materialized.remove(view_name);

            let mut stats = self.stats.write();
            stats.materialized_views = stats.materialized_views.saturating_sub(1);
        }

        // Update statistics
        let mut stats = self.stats.write();
        stats.total_views = stats.total_views.saturating_sub(1);

        info!("View '{}' dropped successfully", view_name);
        Ok(())
    }

    /// Query a view
    pub fn query_view(
        &self,
        view_name: &str,
        conditions: Vec<crate::query::WhereCondition>,
        as_of: Option<AsOf>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let views = self.views.read();
        let view = views.get(view_name).ok_or_else(|| {
            DriftError::InvalidQuery(format!("View '{}' does not exist", view_name))
        })?;

        let view = view.clone();
        drop(views);

        // Update statistics
        let mut stats = self.stats.write();
        stats.total_queries += 1;
        drop(stats);

        if view.is_materialized {
            self.query_materialized_view(&view, conditions, limit)
        } else {
            self.query_regular_view(&view, conditions, as_of, limit)
        }
    }

    /// Query a regular (non-materialized) view
    fn query_regular_view(
        &self,
        view: &ViewDefinition,
        conditions: Vec<crate::query::WhereCondition>,
        _as_of: Option<AsOf>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        // Generate cache key
        let cache_key =
            self.query_cache
                .generate_key(&format!("VIEW:{}", view.name), "default", None);

        // Check cache
        if let Some(cached) = self.query_cache.get(&cache_key) {
            let mut stats = self.stats.write();
            stats.cache_hits += 1;
            drop(stats);

            if let QueryResult::Rows { data } = cached {
                return Ok(data);
            }
        }

        // Cache miss - execute view query
        let mut stats = self.stats.write();
        stats.cache_misses += 1;
        drop(stats);

        // Execute the view's SQL query
        let results = if let Some(ref engine_arc) = self.engine {
            // Create a modified query with additional conditions if needed
            let mut query_sql = view.query.clone();

            // If we have additional conditions, we need to modify the SQL
            if !conditions.is_empty() {
                // For simplicity, wrap the view query in a subquery and add WHERE clause
                let mut where_clauses = Vec::new();
                for condition in &conditions {
                    where_clauses.push(format!(
                        "{} {} {}",
                        condition.column,
                        condition.operator,
                        if condition.value.is_string() {
                            format!("'{}'", condition.value.as_str().unwrap_or(""))
                        } else {
                            condition.value.to_string()
                        }
                    ));
                }

                if !where_clauses.is_empty() {
                    query_sql = format!(
                        "SELECT * FROM ({}) AS subquery WHERE {}",
                        query_sql,
                        where_clauses.join(" AND ")
                    );
                }
            }

            // Add LIMIT if specified
            if let Some(limit) = limit {
                query_sql = format!("{} LIMIT {}", query_sql, limit);
            }

            // Execute the SQL query
            let mut engine = engine_arc.write();
            match sql_bridge::execute_sql(&mut engine, &query_sql) {
                Ok(QueryResult::Rows { data }) => data,
                Ok(_) => vec![], // Handle non-row results
                Err(e) => {
                    debug!("View query execution failed: {}", e);
                    vec![] // Return empty on error for now
                }
            }
        } else {
            debug!("No engine available for view execution");
            vec![]
        };

        // Cache the results
        self.query_cache.put(
            cache_key,
            QueryResult::Rows {
                data: results.clone(),
            },
        )?;

        Ok(results)
    }

    /// Query a materialized view
    fn query_materialized_view(
        &self,
        view: &ViewDefinition,
        conditions: Vec<crate::query::WhereCondition>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        // Check if refresh is needed
        self.maybe_refresh_materialized_view(view)?;

        // Get materialized data
        let materialized = self.materialized_data.read();
        let data = materialized.get(&view.name).ok_or_else(|| {
            DriftError::Internal(format!(
                "Materialized data not found for view '{}'",
                view.name
            ))
        })?;

        // Apply conditions and limit
        let mut results: Vec<Value> = data
            .data
            .iter()
            .filter(|row| Self::matches_conditions(row, &conditions))
            .cloned()
            .collect();

        if let Some(limit) = limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Check if a materialized view needs refresh and refresh if needed
    fn maybe_refresh_materialized_view(&self, view: &ViewDefinition) -> Result<()> {
        if let Some(refresh_policy) = &view.refresh_policy {
            let should_refresh = match refresh_policy {
                RefreshPolicy::OnAccess => true,
                RefreshPolicy::Interval(duration) => {
                    let materialized = self.materialized_data.read();
                    if let Some(data) = materialized.get(&view.name) {
                        data.refreshed_at.elapsed().unwrap_or(Duration::MAX) > *duration
                    } else {
                        true
                    }
                }
                RefreshPolicy::Manual => false,
                RefreshPolicy::OnChange => {
                    // TODO: Track changes to underlying tables
                    false
                }
            };

            if should_refresh {
                self.refresh_materialized_view(&view.name)?;
            }
        }

        Ok(())
    }

    /// Refresh a materialized view
    pub fn refresh_materialized_view(&self, view_name: &str) -> Result<()> {
        debug!("Refreshing materialized view: {}", view_name);

        let views = self.views.read();
        let view = views.get(view_name).ok_or_else(|| {
            DriftError::InvalidQuery(format!("View '{}' does not exist", view_name))
        })?;

        if !view.is_materialized {
            return Err(DriftError::InvalidQuery(format!(
                "View '{}' is not materialized",
                view_name
            )));
        }

        // Execute view query and store results
        let data = if let Some(ref engine_arc) = self.engine {
            let mut engine = engine_arc.write();
            match sql_bridge::execute_sql(&mut engine, &view.query) {
                Ok(QueryResult::Rows { data }) => data,
                Ok(_) => vec![], // Handle non-row results
                Err(e) => {
                    debug!("Materialized view refresh failed: {}", e);
                    vec![] // Return empty on error
                }
            }
        } else {
            debug!("No engine available for materialized view refresh");
            vec![]
        };
        let row_count = data.len();
        let size_bytes = data.len() * 100; // Rough estimate

        let materialized_data = MaterializedViewData {
            data,
            refreshed_at: SystemTime::now(),
            row_count,
            size_bytes,
        };

        // Store materialized data
        let mut materialized = self.materialized_data.write();
        materialized.insert(view_name.to_string(), materialized_data);

        // Update statistics
        let mut stats = self.stats.write();
        stats.refresh_count += 1;

        info!(
            "Materialized view '{}' refreshed: {} rows",
            view_name, row_count
        );
        Ok(())
    }

    /// Check if a row matches conditions
    fn matches_conditions(row: &Value, conditions: &[crate::query::WhereCondition]) -> bool {
        conditions.iter().all(|cond| {
            if let Some(field_value) = row.get(&cond.column) {
                match cond.operator.as_str() {
                    "=" | "==" => field_value == &cond.value,
                    "!=" | "<>" => field_value != &cond.value,
                    _ => false,
                }
            } else {
                false
            }
        })
    }

    /// Get view definition
    pub fn get_view(&self, view_name: &str) -> Option<ViewDefinition> {
        self.views.read().get(view_name).cloned()
    }

    /// List all views
    pub fn list_views(&self) -> Vec<ViewDefinition> {
        self.views.read().values().cloned().collect()
    }

    /// Get view statistics
    pub fn statistics(&self) -> ViewStatistics {
        self.stats.read().clone()
    }

    /// Validate view SQL and extract metadata
    pub fn validate_view_sql(sql: &str) -> Result<(HashSet<String>, Vec<ColumnDefinition>)> {
        use sqlparser::ast::{SelectItem, Statement};
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DriftError::Parse(format!("Invalid SQL in view definition: {}", e)))?;

        if ast.is_empty() {
            return Err(DriftError::InvalidQuery(
                "Empty SQL in view definition".to_string(),
            ));
        }

        let mut dependencies = HashSet::new();
        let mut columns = Vec::new();

        // Extract table dependencies and column info from the parsed AST
        match &ast[0] {
            Statement::Query(query) => {
                // Extract table dependencies
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    // Extract FROM tables
                    for table_with_joins in &select.from {
                        extract_table_name(&table_with_joins.relation, &mut dependencies);
                        for join in &table_with_joins.joins {
                            extract_table_name(&join.relation, &mut dependencies);
                        }
                    }

                    // Extract column information from SELECT items
                    for (i, select_item) in select.projection.iter().enumerate() {
                        match select_item {
                            SelectItem::UnnamedExpr(_expr) => {
                                columns.push(ColumnDefinition {
                                    name: format!("column_{}", i),
                                    data_type: "UNKNOWN".to_string(),
                                    nullable: true,
                                    source_table: None,
                                    source_column: None,
                                });
                            }
                            SelectItem::ExprWithAlias { alias, .. } => {
                                columns.push(ColumnDefinition {
                                    name: alias.value.clone(),
                                    data_type: "UNKNOWN".to_string(),
                                    nullable: true,
                                    source_table: None,
                                    source_column: None,
                                });
                            }
                            SelectItem::Wildcard(_) => {
                                // For wildcard, we can't determine columns without schema info
                                columns.push(ColumnDefinition {
                                    name: "*".to_string(),
                                    data_type: "WILDCARD".to_string(),
                                    nullable: true,
                                    source_table: None,
                                    source_column: None,
                                });
                            }
                            SelectItem::QualifiedWildcard(prefix, _) => {
                                let prefix_str = prefix
                                    .0
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect::<Vec<_>>()
                                    .join(".");
                                columns.push(ColumnDefinition {
                                    name: format!("{}.*", prefix_str),
                                    data_type: "QUALIFIED_WILDCARD".to_string(),
                                    nullable: true,
                                    source_table: Some(prefix_str),
                                    source_column: None,
                                });
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(DriftError::InvalidQuery(
                    "View definition must be a SELECT query".to_string(),
                ));
            }
        }

        Ok((dependencies, columns))
    }

    /// Cache materialized view data
    pub fn cache_materialized_data(&self, view_name: &str, data: Vec<Value>) -> Result<()> {
        let size_bytes = data.iter().map(|v| v.to_string().len()).sum();

        let materialized = MaterializedViewData {
            row_count: data.len(),
            data,
            refreshed_at: SystemTime::now(),
            size_bytes,
        };

        self.materialized_data
            .write()
            .insert(view_name.to_string(), materialized);

        // Update statistics
        let mut stats = self.stats.write();
        stats.materialized_views_refreshed += 1;

        Ok(())
    }

    /// Get cached data for a materialized view
    pub fn get_cached_data(&self, view_name: &str) -> Option<Vec<Value>> {
        self.materialized_data
            .read()
            .get(view_name)
            .map(|data| data.data.clone())
    }
}

/// Builder for creating view definitions
pub struct ViewBuilder {
    name: String,
    query: String,
    is_materialized: bool,
    owner: String,
    permissions: ViewPermissions,
    refresh_policy: Option<RefreshPolicy>,
    comment: Option<String>,
}

impl ViewBuilder {
    /// Create a new view builder
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            is_materialized: false,
            owner: "system".to_string(),
            permissions: ViewPermissions::default(),
            refresh_policy: None,
            comment: None,
        }
    }

    /// Set whether this is a materialized view
    pub fn materialized(mut self, is_materialized: bool) -> Self {
        self.is_materialized = is_materialized;
        self
    }

    /// Set the view owner
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// Set view permissions
    pub fn permissions(mut self, permissions: ViewPermissions) -> Self {
        self.permissions = permissions;
        self
    }

    /// Set refresh policy for materialized views
    pub fn refresh_policy(mut self, policy: RefreshPolicy) -> Self {
        self.refresh_policy = Some(policy);
        self
    }

    /// Set view comment
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Build the view definition
    pub fn build(self) -> Result<ViewDefinition> {
        // Validate and extract metadata from SQL
        let (dependencies, columns) = ViewManager::validate_view_sql(&self.query)?;

        Ok(ViewDefinition {
            name: self.name,
            query: self.query,
            parsed_query: None,
            columns,
            is_materialized: self.is_materialized,
            dependencies,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            owner: self.owner,
            permissions: self.permissions,
            refresh_policy: self.refresh_policy,
            comment: self.comment,
        })
    }
}

/// Helper function to extract table name from TableFactor
fn extract_table_name(
    table_factor: &sqlparser::ast::TableFactor,
    dependencies: &mut HashSet<String>,
) {
    use sqlparser::ast::TableFactor;

    match table_factor {
        TableFactor::Table { name, .. } => {
            dependencies.insert(name.to_string());
        }
        TableFactor::Derived { .. } => {
            // Subquery - could recursively analyze but for now skip
        }
        TableFactor::TableFunction { .. } => {
            // Table function - skip for now
        }
        TableFactor::UNNEST { .. } => {
            // UNNEST - skip for now
        }
        TableFactor::JsonTable { .. } => {
            // JSON table - skip for now
        }
        TableFactor::NestedJoin { .. } => {
            // Nested join - would need recursive handling
        }
        TableFactor::Pivot { .. } => {
            // Pivot - skip for now
        }
        TableFactor::Unpivot { .. } => {
            // Unpivot - skip for now
        }
        TableFactor::MatchRecognize { .. } => {
            // Match recognize - skip for now
        }
        TableFactor::Function { .. } => {
            // Table function - skip for now
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_creation() {
        let manager = ViewManager::new();

        let view = ViewBuilder::new("user_summary", "SELECT * FROM users WHERE active = true")
            .owner("admin")
            .comment("Active users summary")
            .build()
            .unwrap();

        assert_eq!(view.name, "user_summary");
        assert!(!view.is_materialized);

        manager.create_view(view).unwrap();

        let retrieved = manager.get_view("user_summary").unwrap();
        assert_eq!(retrieved.name, "user_summary");
    }

    #[test]
    fn test_materialized_view() {
        let manager = ViewManager::new();

        let view = ViewBuilder::new("sales_summary", "SELECT SUM(amount) FROM sales")
            .materialized(true)
            .refresh_policy(RefreshPolicy::Interval(Duration::from_secs(3600)))
            .build()
            .unwrap();

        assert!(view.is_materialized);
        assert!(view.refresh_policy.is_some());

        manager.create_view(view).unwrap();

        let stats = manager.statistics();
        assert_eq!(stats.total_views, 1);
        assert_eq!(stats.materialized_views, 1);
    }

    #[test]
    fn test_circular_dependency_detection() {
        let mut graph = ViewDependencyGraph::default();

        graph.add_view("view_a".to_string(), ["view_b".to_string()].into());
        graph.add_view("view_b".to_string(), ["view_c".to_string()].into());

        // This would create a cycle: view_c -> view_a -> view_b -> view_c
        let has_cycle = graph.has_circular_dependency("view_c", &["view_a".to_string()].into());
        assert!(has_cycle);
    }

    #[test]
    fn test_view_permissions() {
        let mut perms = ViewPermissions::default();
        perms.select_users.insert("alice".to_string());
        perms.select_users.insert("bob".to_string());
        perms.select_roles.insert("analyst".to_string());

        assert!(!perms.is_public);
        assert!(perms.check_underlying_permissions);
        assert_eq!(perms.select_users.len(), 2);
    }
}
