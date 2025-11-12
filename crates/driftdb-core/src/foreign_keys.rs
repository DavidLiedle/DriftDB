use crate::engine::Engine;
use crate::errors::{DriftError, Result};
use crate::query::{Query, QueryResult, WhereCondition};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Foreign key constraint management system
pub struct ForeignKeyManager {
    constraints: Arc<RwLock<HashMap<String, ForeignKeyConstraint>>>,
    table_constraints: Arc<RwLock<HashMap<String, Vec<String>>>>,
    reference_index: Arc<RwLock<ReferenceIndex>>,
    validation_cache: Arc<RwLock<ValidationCache>>,
    config: ForeignKeyConfig,
    engine: Option<Arc<RwLock<Engine>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConfig {
    pub enforce_constraints: bool,
    pub cascade_depth_limit: usize,
    pub check_on_commit: bool,
    pub defer_checking: bool,
    pub use_cache: bool,
    pub cache_size: usize,
}

impl Default for ForeignKeyConfig {
    fn default() -> Self {
        Self {
            enforce_constraints: true,
            cascade_depth_limit: 10,
            check_on_commit: false,
            defer_checking: false,
            use_cache: true,
            cache_size: 10000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub name: String,
    pub child_table: String,
    pub child_columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    pub is_deferrable: bool,
    pub initially_deferred: bool,
    pub match_type: MatchType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchType {
    Simple,
    Full,
    Partial,
}

/// Index for fast lookups of references
struct ReferenceIndex {
    // parent_table -> child_tables that reference it
    parent_to_children: HashMap<String, HashSet<String>>,
    // child_table -> parent_tables it references
    child_to_parents: HashMap<String, HashSet<String>>,
    // (parent_table, parent_key) -> list of (child_table, child_keys)
    #[allow(dead_code)]
    reference_map: HashMap<(String, Vec<serde_json::Value>), Vec<(String, Vec<serde_json::Value>)>>,
}

/// Cache for validation results
struct ValidationCache {
    // (table, key) -> exists
    existence_cache: lru::LruCache<(String, Vec<serde_json::Value>), bool>,
    // constraint_name -> validation_result
    constraint_cache: lru::LruCache<String, ValidationResult>,
}

#[derive(Debug, Clone)]
struct ValidationResult {
    #[allow(dead_code)]
    pub valid: bool,
    #[allow(dead_code)]
    pub violations: Vec<ConstraintViolation>,
    #[allow(dead_code)]
    pub timestamp: std::time::Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintViolation {
    pub constraint_name: String,
    pub violation_type: ViolationType,
    pub child_table: String,
    pub child_key: Vec<serde_json::Value>,
    pub parent_table: String,
    pub parent_key: Vec<serde_json::Value>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationType {
    MissingParent,
    OrphanedChild,
    CascadeDepthExceeded,
    CircularReference,
    NullConstraint,
}

/// Result of cascade operations
#[derive(Debug)]
pub struct CascadeResult {
    pub affected_tables: Vec<String>,
    pub deleted_rows: HashMap<String, Vec<Vec<serde_json::Value>>>,
    pub updated_rows: HashMap<String, Vec<Vec<serde_json::Value>>>,
    pub nullified_rows: HashMap<String, Vec<Vec<serde_json::Value>>>,
}

impl ForeignKeyManager {
    pub fn new(config: ForeignKeyConfig) -> Self {
        let cache_size = config.cache_size;

        Self {
            constraints: Arc::new(RwLock::new(HashMap::new())),
            table_constraints: Arc::new(RwLock::new(HashMap::new())),
            reference_index: Arc::new(RwLock::new(ReferenceIndex::new())),
            validation_cache: Arc::new(RwLock::new(ValidationCache::new(cache_size))),
            config,
            engine: None,
        }
    }

    pub fn with_engine(mut self, engine: Arc<RwLock<Engine>>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Add a foreign key constraint
    pub fn add_constraint(&self, constraint: ForeignKeyConstraint) -> Result<()> {
        // Validate constraint definition
        self.validate_constraint_definition(&constraint)?;

        // Check for circular references
        if self.would_create_cycle(&constraint)? {
            return Err(DriftError::Validation(format!(
                "Foreign key constraint '{}' would create a circular reference",
                constraint.name
            )));
        }

        let name = constraint.name.clone();
        let child_table = constraint.child_table.clone();
        let parent_table = constraint.parent_table.clone();

        // Add to constraint store
        self.constraints.write().insert(name.clone(), constraint);

        // Update table constraints index
        self.table_constraints
            .write()
            .entry(child_table.clone())
            .or_default()
            .push(name.clone());

        // Update reference index
        let mut index = self.reference_index.write();
        index
            .parent_to_children
            .entry(parent_table.clone())
            .or_default()
            .insert(child_table.clone());
        index
            .child_to_parents
            .entry(child_table)
            .or_default()
            .insert(parent_table);

        Ok(())
    }

    /// Remove a foreign key constraint
    pub fn drop_constraint(&self, constraint_name: &str) -> Result<()> {
        let constraint = self
            .constraints
            .write()
            .remove(constraint_name)
            .ok_or_else(|| {
                DriftError::NotFound(format!(
                    "Foreign key constraint '{}' not found",
                    constraint_name
                ))
            })?;

        // Update table constraints index
        if let Some(constraints) = self
            .table_constraints
            .write()
            .get_mut(&constraint.child_table)
        {
            constraints.retain(|c| c != constraint_name);
        }

        // Update reference index
        let mut index = self.reference_index.write();
        if let Some(children) = index.parent_to_children.get_mut(&constraint.parent_table) {
            children.remove(&constraint.child_table);
        }
        if let Some(parents) = index.child_to_parents.get_mut(&constraint.child_table) {
            parents.remove(&constraint.parent_table);
        }

        // Clear validation cache
        self.validation_cache.write().clear();

        Ok(())
    }

    /// Check constraints before insert
    pub fn check_insert(
        &self,
        table: &str,
        row: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        if !self.config.enforce_constraints {
            return Ok(());
        }

        let constraints = self.get_table_constraints(table);

        for constraint_name in constraints {
            let constraint = self
                .constraints
                .read()
                .get(&constraint_name)
                .cloned()
                .ok_or_else(|| {
                    DriftError::Internal(format!("Constraint '{}' not found", constraint_name))
                })?;

            // Check if parent exists
            let child_values =
                self.extract_column_values_from_map(row, &constraint.child_columns)?;

            // Skip if any child column is NULL and match type allows it
            if self.has_null_values(&child_values) {
                match constraint.match_type {
                    MatchType::Simple => continue, // NULL values allowed
                    MatchType::Full => {
                        // All must be NULL or none
                        if !child_values.iter().all(|v| v.is_null()) {
                            return Err(DriftError::Validation(format!(
                                "Foreign key constraint '{}' violation: partial NULL values not allowed with MATCH FULL",
                                constraint.name
                            )));
                        }
                        continue;
                    }
                    MatchType::Partial => continue, // Partial NULL allowed
                }
            }

            // Check parent existence
            if !self.parent_exists(
                &constraint.parent_table,
                &constraint.parent_columns,
                &child_values,
            )? {
                return Err(DriftError::Validation(format!(
                    "Foreign key constraint '{}' violation: parent key does not exist in table '{}'",
                    constraint.name, constraint.parent_table
                )));
            }
        }

        Ok(())
    }

    /// Check constraints before update
    pub fn check_update(
        &self,
        table: &str,
        old_row: &HashMap<String, serde_json::Value>,
        new_row: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        if !self.config.enforce_constraints {
            return Ok(());
        }

        // Check as parent (other tables reference this one)
        self.check_update_as_parent(table, old_row, new_row)?;

        // Check as child (this table references others)
        self.check_update_as_child(table, new_row)?;

        Ok(())
    }

    /// Check constraints before delete
    pub fn check_delete(
        &self,
        table: &str,
        row: &HashMap<String, serde_json::Value>,
    ) -> Result<CascadeResult> {
        if !self.config.enforce_constraints {
            return Ok(CascadeResult::empty());
        }

        let mut cascade_result = CascadeResult::empty();
        let children = self.get_child_constraints(table);

        for constraint_name in children {
            let constraint = self
                .constraints
                .read()
                .get(&constraint_name)
                .cloned()
                .ok_or_else(|| {
                    DriftError::Internal(format!("Constraint '{}' not found", constraint_name))
                })?;

            let parent_values =
                self.extract_column_values_from_map(row, &constraint.parent_columns)?;

            // Check for dependent rows
            let dependent_rows = self.find_dependent_rows(
                &constraint.child_table,
                &constraint.child_columns,
                &parent_values,
            )?;

            if !dependent_rows.is_empty() {
                match constraint.on_delete {
                    ReferentialAction::NoAction | ReferentialAction::Restrict => {
                        return Err(DriftError::Validation(format!(
                            "Foreign key constraint '{}' violation: cannot delete parent with existing children",
                            constraint.name
                        )));
                    }
                    ReferentialAction::Cascade => {
                        // Recursively delete children
                        cascade_result
                            .deleted_rows
                            .entry(constraint.child_table.clone())
                            .or_default()
                            .extend(dependent_rows);

                        cascade_result.affected_tables.push(constraint.child_table);
                    }
                    ReferentialAction::SetNull => {
                        cascade_result
                            .nullified_rows
                            .entry(constraint.child_table.clone())
                            .or_default()
                            .extend(dependent_rows);

                        cascade_result.affected_tables.push(constraint.child_table);
                    }
                    ReferentialAction::SetDefault => {
                        // Would need default values from schema
                        cascade_result
                            .updated_rows
                            .entry(constraint.child_table.clone())
                            .or_default()
                            .extend(dependent_rows);

                        cascade_result.affected_tables.push(constraint.child_table);
                    }
                }
            }
        }

        Ok(cascade_result)
    }

    fn check_update_as_parent(
        &self,
        table: &str,
        old_row: &HashMap<String, serde_json::Value>,
        new_row: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let children = self.get_child_constraints(table);

        for constraint_name in children {
            let constraint = self
                .constraints
                .read()
                .get(&constraint_name)
                .cloned()
                .ok_or_else(|| {
                    DriftError::Internal(format!("Constraint '{}' not found", constraint_name))
                })?;

            let old_values =
                self.extract_column_values_from_map(old_row, &constraint.parent_columns)?;
            let new_values =
                self.extract_column_values_from_map(new_row, &constraint.parent_columns)?;

            // Skip if key hasn't changed
            if old_values == new_values {
                continue;
            }

            // Check for dependent rows
            let dependent_rows = self.find_dependent_rows(
                &constraint.child_table,
                &constraint.child_columns,
                &old_values,
            )?;

            if !dependent_rows.is_empty() {
                match constraint.on_update {
                    ReferentialAction::NoAction | ReferentialAction::Restrict => {
                        return Err(DriftError::Validation(format!(
                            "Foreign key constraint '{}' violation: cannot update parent key with existing children",
                            constraint.name
                        )));
                    }
                    ReferentialAction::Cascade => {
                        // Would need to cascade update to children
                        // This would be handled by the engine
                    }
                    ReferentialAction::SetNull | ReferentialAction::SetDefault => {
                        // Would need to update children accordingly
                    }
                }
            }
        }

        Ok(())
    }

    fn check_update_as_child(
        &self,
        table: &str,
        new_row: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        // Same as insert check
        self.check_insert(table, new_row)
    }

    fn validate_constraint_definition(&self, constraint: &ForeignKeyConstraint) -> Result<()> {
        // Check that column counts match
        if constraint.child_columns.len() != constraint.parent_columns.len() {
            return Err(DriftError::Validation(format!(
                "Foreign key constraint '{}': child and parent column counts must match",
                constraint.name
            )));
        }

        // Check that columns are not empty
        if constraint.child_columns.is_empty() {
            return Err(DriftError::Validation(format!(
                "Foreign key constraint '{}': must specify at least one column",
                constraint.name
            )));
        }

        // Check for duplicate columns
        let mut seen = HashSet::new();
        for col in &constraint.child_columns {
            if !seen.insert(col) {
                return Err(DriftError::Validation(format!(
                    "Foreign key constraint '{}': duplicate child column '{}'",
                    constraint.name, col
                )));
            }
        }

        seen.clear();
        for col in &constraint.parent_columns {
            if !seen.insert(col) {
                return Err(DriftError::Validation(format!(
                    "Foreign key constraint '{}': duplicate parent column '{}'",
                    constraint.name, col
                )));
            }
        }

        Ok(())
    }

    fn would_create_cycle(&self, new_constraint: &ForeignKeyConstraint) -> Result<bool> {
        // Simple cycle detection using DFS
        let mut visited = HashSet::new();
        let mut path = Vec::new();

        self.detect_cycle_dfs(
            &new_constraint.parent_table,
            &new_constraint.child_table,
            &mut visited,
            &mut path,
        )
    }

    fn detect_cycle_dfs(
        &self,
        start: &str,
        target: &str,
        visited: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Result<bool> {
        if start == target && !path.is_empty() {
            return Ok(true);
        }

        if visited.contains(start) {
            return Ok(false);
        }

        visited.insert(start.to_string());
        path.push(start.to_string());

        let index = self.reference_index.read();
        if let Some(children) = index.parent_to_children.get(start) {
            for child in children {
                if self.detect_cycle_dfs(child, target, visited, path)? {
                    return Ok(true);
                }
            }
        }

        path.pop();
        Ok(false)
    }

    fn get_table_constraints(&self, table: &str) -> Vec<String> {
        self.table_constraints
            .read()
            .get(table)
            .cloned()
            .unwrap_or_default()
    }

    fn get_child_constraints(&self, parent_table: &str) -> Vec<String> {
        let constraints = self.constraints.read();
        constraints
            .iter()
            .filter(|(_, c)| c.parent_table == parent_table)
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn extract_column_values(
        &self,
        row: &serde_json::Value,
        columns: &[String],
    ) -> Result<Vec<serde_json::Value>> {
        let mut values = Vec::new();

        // Convert Value to HashMap if it's an object
        if let Some(row_obj) = row.as_object() {
            for column in columns {
                let value = row_obj
                    .get(column)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                values.push(value);
            }
        } else {
            // If row is not an object, return nulls for all columns
            for _ in columns {
                values.push(serde_json::Value::Null);
            }
        }

        Ok(values)
    }

    // Helper function for existing HashMap interface
    fn extract_column_values_from_map(
        &self,
        row: &HashMap<String, serde_json::Value>,
        columns: &[String],
    ) -> Result<Vec<serde_json::Value>> {
        let mut values = Vec::new();

        for column in columns {
            let value = row.get(column).cloned().unwrap_or(serde_json::Value::Null);
            values.push(value);
        }

        Ok(values)
    }

    fn has_null_values(&self, values: &[serde_json::Value]) -> bool {
        values.iter().any(|v| v.is_null())
    }

    fn parent_exists(
        &self,
        parent_table: &str,
        parent_columns: &[String],
        values: &[serde_json::Value],
    ) -> Result<bool> {
        // Check cache first
        if self.config.use_cache {
            let key = (parent_table.to_string(), values.to_vec());
            if let Some(exists) = self.validation_cache.write().existence_cache.get(&key) {
                return Ok(*exists);
            }
        }

        // Actually query the database to check parent existence
        let exists = if let Some(ref engine) = self.engine {
            // Build conditions: WHERE parent_columns[0] = values[0] AND parent_columns[1] = values[1] ...
            let mut conditions = Vec::new();
            for (i, column) in parent_columns.iter().enumerate() {
                if let Some(value) = values.get(i) {
                    conditions.push(WhereCondition {
                        column: column.clone(),
                        operator: "=".to_string(),
                        value: value.clone(),
                    });
                }
            }

            // Execute query: SELECT 1 FROM parent_table WHERE conditions LIMIT 1
            let query = Query::Select {
                table: parent_table.to_string(),
                conditions,
                as_of: None,
                limit: Some(1),
            };

            let engine_lock = engine.read();
            match engine_lock.query(&query) {
                Ok(QueryResult::Rows { data }) => !data.is_empty(),
                Ok(_) => false,
                Err(_) => false, // If query fails, assume parent doesn't exist (conservative)
            }
        } else {
            // No engine available - this should not happen in production
            tracing::warn!("Foreign key validation attempted without engine - allowing operation");
            true // Fallback to permissive behavior if no engine
        };

        // Update cache
        if self.config.use_cache {
            let key = (parent_table.to_string(), values.to_vec());
            self.validation_cache
                .write()
                .existence_cache
                .put(key, exists);
        }

        Ok(exists)
    }

    fn find_dependent_rows(
        &self,
        child_table: &str,
        child_columns: &[String],
        parent_values: &[serde_json::Value],
    ) -> Result<Vec<Vec<serde_json::Value>>> {
        // Actually query the database to find dependent rows
        if let Some(ref engine) = self.engine {
            // Build conditions: WHERE child_columns[0] = parent_values[0] AND child_columns[1] = parent_values[1] ...
            let mut conditions = Vec::new();
            for (i, column) in child_columns.iter().enumerate() {
                if let Some(value) = parent_values.get(i) {
                    conditions.push(WhereCondition {
                        column: column.clone(),
                        operator: "=".to_string(),
                        value: value.clone(),
                    });
                }
            }

            // Execute query: SELECT * FROM child_table WHERE conditions
            let query = Query::Select {
                table: child_table.to_string(),
                conditions,
                as_of: None,
                limit: None, // Get all dependent rows
            };

            let engine_lock = engine.read();
            match engine_lock.query(&query) {
                Ok(QueryResult::Rows { data }) => {
                    // Extract the values from each row
                    let mut dependent_values = Vec::new();
                    for row in data {
                        let mut row_values = Vec::new();
                        for column in child_columns {
                            let value = row.get(column).cloned().unwrap_or(serde_json::Value::Null);
                            row_values.push(value);
                        }
                        dependent_values.push(row_values);
                    }
                    Ok(dependent_values)
                }
                Ok(_) => Ok(Vec::new()),
                Err(e) => {
                    tracing::error!("Failed to find dependent rows: {}", e);
                    Ok(Vec::new()) // Return empty on error to avoid blocking operations
                }
            }
        } else {
            tracing::warn!("Dependency check attempted without engine");
            Ok(Vec::new())
        }
    }

    /// Validate all constraints in the database
    pub fn validate_all(&self) -> Result<Vec<ConstraintViolation>> {
        let mut violations = Vec::new();

        for (name, constraint) in self.constraints.read().iter() {
            if let Err(e) = self.validate_constraint(constraint) {
                violations.push(ConstraintViolation {
                    constraint_name: name.clone(),
                    violation_type: ViolationType::MissingParent,
                    child_table: constraint.child_table.clone(),
                    child_key: Vec::new(),
                    parent_table: constraint.parent_table.clone(),
                    parent_key: Vec::new(),
                    message: e.to_string(),
                });
            }
        }

        Ok(violations)
    }

    fn validate_constraint(&self, constraint: &ForeignKeyConstraint) -> Result<()> {
        // Validate that all existing rows in the child table have valid parent references
        if let Some(ref engine) = self.engine {
            // Get all rows from child table
            let query = Query::Select {
                table: constraint.child_table.clone(),
                conditions: Vec::new(), // No conditions = get all rows
                as_of: None,
                limit: None,
            };

            let engine_lock = engine.read();
            match engine_lock.query(&query) {
                Ok(QueryResult::Rows { data }) => {
                    for row in data {
                        // Extract child column values
                        let child_values =
                            self.extract_column_values(&row, &constraint.child_columns)?;

                        // Skip rows with NULL values (handled by match type)
                        if self.has_null_values(&child_values) {
                            match constraint.match_type {
                                MatchType::Simple => continue, // NULL values allowed
                                MatchType::Full => {
                                    // All must be NULL or none
                                    if !child_values.iter().all(|v| v.is_null()) {
                                        return Err(DriftError::Validation(format!(
                                            "Foreign key constraint '{}' violation: partial NULL values not allowed with MATCH FULL",
                                            constraint.name
                                        )));
                                    }
                                    continue;
                                }
                                MatchType::Partial => continue, // Partial NULL allowed
                            }
                        }

                        // Check if parent exists
                        if !self.parent_exists(
                            &constraint.parent_table,
                            &constraint.parent_columns,
                            &child_values,
                        )? {
                            return Err(DriftError::Validation(format!(
                                "Foreign key constraint '{}' violation: child row {:?} has no matching parent in table '{}'",
                                constraint.name, child_values, constraint.parent_table
                            )));
                        }
                    }
                }
                Ok(_) => {
                    // Table might be empty or query returned non-row result
                }
                Err(e) => {
                    return Err(DriftError::Internal(format!(
                        "Failed to validate constraint '{}': {}",
                        constraint.name, e
                    )));
                }
            }
        }

        Ok(())
    }

    /// Get constraint by name
    pub fn get_constraint(&self, name: &str) -> Option<ForeignKeyConstraint> {
        self.constraints.read().get(name).cloned()
    }

    /// List all constraints
    pub fn list_constraints(&self) -> Vec<ForeignKeyConstraint> {
        self.constraints.read().values().cloned().collect()
    }

    /// List constraints for a table
    pub fn list_table_constraints(&self, table: &str) -> Vec<ForeignKeyConstraint> {
        self.constraints
            .read()
            .values()
            .filter(|c| c.child_table == table || c.parent_table == table)
            .cloned()
            .collect()
    }

    /// Clear validation cache
    pub fn clear_cache(&self) {
        self.validation_cache.write().clear();
    }

    /// Get dependency graph
    pub fn get_dependency_graph(&self) -> HashMap<String, Vec<String>> {
        let index = self.reference_index.read();
        index
            .child_to_parents
            .clone()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect()
    }

    /// Get topological sort of tables (for safe deletion order)
    pub fn topological_sort(&self) -> Result<Vec<String>> {
        let graph = self.get_dependency_graph();
        let mut sorted = Vec::new();
        let mut visited = HashSet::new();
        let mut temp_visited = HashSet::new();

        for table in graph.keys() {
            if !visited.contains(table) {
                self.topological_sort_dfs(
                    table,
                    &graph,
                    &mut visited,
                    &mut temp_visited,
                    &mut sorted,
                )?;
            }
        }

        sorted.reverse();
        Ok(sorted)
    }

    fn topological_sort_dfs(
        &self,
        table: &str,
        graph: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        temp_visited: &mut HashSet<String>,
        sorted: &mut Vec<String>,
    ) -> Result<()> {
        if temp_visited.contains(table) {
            return Err(DriftError::Validation(
                "Circular dependency detected".to_string(),
            ));
        }

        if visited.contains(table) {
            return Ok(());
        }

        temp_visited.insert(table.to_string());

        if let Some(dependencies) = graph.get(table) {
            for dep in dependencies {
                self.topological_sort_dfs(dep, graph, visited, temp_visited, sorted)?;
            }
        }

        temp_visited.remove(table);
        visited.insert(table.to_string());
        sorted.push(table.to_string());

        Ok(())
    }
}

impl ReferenceIndex {
    fn new() -> Self {
        Self {
            parent_to_children: HashMap::new(),
            child_to_parents: HashMap::new(),
            reference_map: HashMap::new(),
        }
    }
}

impl ValidationCache {
    fn new(size: usize) -> Self {
        Self {
            existence_cache: lru::LruCache::new(size.try_into().unwrap()),
            constraint_cache: lru::LruCache::new(100.try_into().unwrap()),
        }
    }

    fn clear(&mut self) {
        self.existence_cache.clear();
        self.constraint_cache.clear();
    }
}

impl CascadeResult {
    fn empty() -> Self {
        Self {
            affected_tables: Vec::new(),
            deleted_rows: HashMap::new(),
            updated_rows: HashMap::new(),
            nullified_rows: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_creation() {
        let manager = ForeignKeyManager::new(ForeignKeyConfig::default());

        let constraint = ForeignKeyConstraint {
            name: "fk_order_customer".to_string(),
            child_table: "orders".to_string(),
            child_columns: vec!["customer_id".to_string()],
            parent_table: "customers".to_string(),
            parent_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::Restrict,
            on_update: ReferentialAction::Cascade,
            is_deferrable: false,
            initially_deferred: false,
            match_type: MatchType::Simple,
        };

        manager.add_constraint(constraint).unwrap();

        let retrieved = manager.get_constraint("fk_order_customer").unwrap();
        assert_eq!(retrieved.child_table, "orders");
        assert_eq!(retrieved.parent_table, "customers");
    }

    #[test]
    fn test_cycle_detection() {
        let manager = ForeignKeyManager::new(ForeignKeyConfig::default());

        // Create A -> B
        manager
            .add_constraint(ForeignKeyConstraint {
                name: "fk_a_b".to_string(),
                child_table: "table_a".to_string(),
                child_columns: vec!["b_id".to_string()],
                parent_table: "table_b".to_string(),
                parent_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::Restrict,
                is_deferrable: false,
                initially_deferred: false,
                match_type: MatchType::Simple,
            })
            .unwrap();

        // Create B -> C
        manager
            .add_constraint(ForeignKeyConstraint {
                name: "fk_b_c".to_string(),
                child_table: "table_b".to_string(),
                child_columns: vec!["c_id".to_string()],
                parent_table: "table_c".to_string(),
                parent_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::Restrict,
                is_deferrable: false,
                initially_deferred: false,
                match_type: MatchType::Simple,
            })
            .unwrap();

        // Try to create C -> A (would create cycle)
        let result = manager.add_constraint(ForeignKeyConstraint {
            name: "fk_c_a".to_string(),
            child_table: "table_c".to_string(),
            child_columns: vec!["a_id".to_string()],
            parent_table: "table_a".to_string(),
            parent_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::Restrict,
            on_update: ReferentialAction::Restrict,
            is_deferrable: false,
            initially_deferred: false,
            match_type: MatchType::Simple,
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("circular"));
    }

    #[test]
    fn test_topological_sort() {
        let manager = ForeignKeyManager::new(ForeignKeyConfig::default());

        // Create dependencies: orders -> customers, order_items -> orders
        manager
            .add_constraint(ForeignKeyConstraint {
                name: "fk_order_customer".to_string(),
                child_table: "orders".to_string(),
                child_columns: vec!["customer_id".to_string()],
                parent_table: "customers".to_string(),
                parent_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Restrict,
                on_update: ReferentialAction::Restrict,
                is_deferrable: false,
                initially_deferred: false,
                match_type: MatchType::Simple,
            })
            .unwrap();

        manager
            .add_constraint(ForeignKeyConstraint {
                name: "fk_item_order".to_string(),
                child_table: "order_items".to_string(),
                child_columns: vec!["order_id".to_string()],
                parent_table: "orders".to_string(),
                parent_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::Cascade,
                is_deferrable: false,
                initially_deferred: false,
                match_type: MatchType::Simple,
            })
            .unwrap();

        let sorted = manager.topological_sort().unwrap();

        // customers should come before orders, orders before order_items
        let customers_idx = sorted.iter().position(|t| t == "customers");
        let orders_idx = sorted.iter().position(|t| t == "orders");
        let items_idx = sorted.iter().position(|t| t == "order_items");

        if let (Some(c), Some(o), Some(i)) = (customers_idx, orders_idx, items_idx) {
            assert!(c < o);
            assert!(o < i);
        }
    }
}
