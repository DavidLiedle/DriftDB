use crate::engine::Engine;
use crate::schema::Schema;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// SQL Constraint Types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintType {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        reference_table: String,
        reference_columns: Vec<String>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
    Unique {
        columns: Vec<String>,
    },
    Check {
        expression: String,
        compiled_expr: CheckExpression,
    },
    NotNull {
        column: String,
    },
    Default {
        column: String,
        value: Value,
    },
}

/// Foreign key referential actions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

/// Compiled check constraint expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckExpression {
    Comparison {
        column: String,
        operator: ComparisonOp,
        value: Value,
    },
    Between {
        column: String,
        min: Value,
        max: Value,
    },
    In {
        column: String,
        values: Vec<Value>,
    },
    And(Box<CheckExpression>, Box<CheckExpression>),
    Or(Box<CheckExpression>, Box<CheckExpression>),
    Not(Box<CheckExpression>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Constraint definition with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub table_name: String,
    pub is_deferrable: bool,
    pub initially_deferred: bool,
}

/// Constraint Manager handles all constraint validation and enforcement
pub struct ConstraintManager {
    constraints: HashMap<String, Vec<Constraint>>, // table_name -> constraints
    #[allow(dead_code)]
    unique_indexes: HashMap<String, HashSet<String>>, // table_name.column -> unique values
    foreign_key_graph: ForeignKeyGraph,
}

/// Tracks foreign key relationships for cascade operations
struct ForeignKeyGraph {
    // parent_table -> (child_table, constraint)
    dependencies: HashMap<String, Vec<(String, Constraint)>>,
}

impl ConstraintManager {
    pub fn new() -> Self {
        Self {
            constraints: HashMap::new(),
            unique_indexes: HashMap::new(),
            foreign_key_graph: ForeignKeyGraph::new(),
        }
    }

    /// Add a constraint to a table
    pub fn add_constraint(&mut self, constraint: Constraint) -> Result<()> {
        // Validate constraint definition
        self.validate_constraint_definition(&constraint)?;

        // Add to foreign key graph if applicable
        if let ConstraintType::ForeignKey {
            reference_table, ..
        } = &constraint.constraint_type
        {
            self.foreign_key_graph.add_dependency(
                reference_table.clone(),
                constraint.table_name.clone(),
                constraint.clone(),
            );
        }

        // Store constraint
        self.constraints
            .entry(constraint.table_name.clone())
            .or_insert_with(Vec::new)
            .push(constraint);

        Ok(())
    }

    /// Validate a record before insert
    pub fn validate_insert(
        &self,
        table: &Schema,
        record: &mut Value,
        engine: &Engine,
    ) -> Result<()> {
        let table_constraints = self.constraints.get(&table.name);
        if table_constraints.is_none() {
            return Ok(());
        }

        for constraint in table_constraints.unwrap() {
            match &constraint.constraint_type {
                ConstraintType::NotNull { column } => {
                    if record.get(column).map_or(true, |v| v.is_null()) {
                        return Err(anyhow!(
                            "NOT NULL constraint violation: column '{}' cannot be null",
                            column
                        ));
                    }
                }
                ConstraintType::Check { compiled_expr, .. } => {
                    if !self.evaluate_check_expression(compiled_expr, record)? {
                        return Err(anyhow!("CHECK constraint violation: {}", constraint.name));
                    }
                }
                ConstraintType::Unique { columns } => {
                    self.validate_unique_constraint(table, columns, record, engine)?;
                }
                ConstraintType::ForeignKey {
                    columns,
                    reference_table,
                    reference_columns,
                    ..
                } => {
                    self.validate_foreign_key(
                        columns,
                        reference_table,
                        reference_columns,
                        record,
                        engine,
                    )?;
                }
                ConstraintType::Default { column, value } => {
                    // Apply default if column is missing or null
                    if record.get(column).map_or(true, |v| v.is_null()) {
                        if let Some(obj) = record.as_object_mut() {
                            obj.insert(column.clone(), value.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Validate a record before update
    pub fn validate_update(
        &self,
        table: &Schema,
        old_record: &Value,
        new_record: &Value,
        engine: &Engine,
    ) -> Result<()> {
        let table_constraints = self.constraints.get(&table.name);
        if table_constraints.is_none() {
            return Ok(());
        }

        for constraint in table_constraints.unwrap() {
            match &constraint.constraint_type {
                ConstraintType::Check { compiled_expr, .. } => {
                    if !self.evaluate_check_expression(compiled_expr, new_record)? {
                        return Err(anyhow!("CHECK constraint violation: {}", constraint.name));
                    }
                }
                ConstraintType::Unique { columns } => {
                    // Only validate if unique columns changed
                    if self.columns_changed(columns, old_record, new_record) {
                        self.validate_unique_constraint(table, columns, new_record, engine)?;
                    }
                }
                ConstraintType::ForeignKey {
                    columns,
                    reference_table,
                    reference_columns,
                    ..
                } => {
                    // Only validate if foreign key columns changed
                    if self.columns_changed(columns, old_record, new_record) {
                        self.validate_foreign_key(
                            columns,
                            reference_table,
                            reference_columns,
                            new_record,
                            engine,
                        )?;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Validate before delete, handling cascades
    pub fn validate_delete(
        &self,
        table: &Schema,
        record: &Value,
        engine: &mut Engine,
    ) -> Result<Vec<CascadeAction>> {
        let mut cascade_actions = Vec::new();

        // Check for foreign key references to this record
        if let Some(dependencies) = self.foreign_key_graph.get_dependencies(&table.name) {
            for (child_table, constraint) in dependencies {
                if let ConstraintType::ForeignKey {
                    columns,
                    reference_columns,
                    on_delete,
                    ..
                } = &constraint.constraint_type
                {
                    // Check if any child records reference this record
                    let child_refs = self.find_referencing_records(
                        &child_table,
                        columns,
                        record,
                        reference_columns,
                        engine,
                    )?;

                    if !child_refs.is_empty() {
                        match on_delete {
                            ForeignKeyAction::Cascade => {
                                // Add cascade delete action
                                cascade_actions.push(CascadeAction::Delete {
                                    table: child_table.clone(),
                                    records: child_refs,
                                });
                            }
                            ForeignKeyAction::SetNull => {
                                // Add set null action
                                cascade_actions.push(CascadeAction::SetNull {
                                    table: child_table.clone(),
                                    columns: columns.clone(),
                                    records: child_refs,
                                });
                            }
                            ForeignKeyAction::Restrict => {
                                return Err(anyhow!(
                                    "Cannot delete: record is referenced by foreign key in table '{}'",
                                    child_table
                                ));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(cascade_actions)
    }

    /// Evaluate a check constraint expression
    fn evaluate_check_expression(&self, expr: &CheckExpression, record: &Value) -> Result<bool> {
        match expr {
            CheckExpression::Comparison {
                column,
                operator,
                value,
            } => {
                let record_value = record
                    .get(column)
                    .ok_or_else(|| anyhow!("Column '{}' not found", column))?;
                Ok(self.compare_values(record_value, operator, value))
            }
            CheckExpression::Between { column, min, max } => {
                let record_value = record
                    .get(column)
                    .ok_or_else(|| anyhow!("Column '{}' not found", column))?;
                Ok(
                    self.compare_values(record_value, &ComparisonOp::GreaterThanOrEqual, min)
                        && self.compare_values(record_value, &ComparisonOp::LessThanOrEqual, max),
                )
            }
            CheckExpression::In { column, values } => {
                let record_value = record
                    .get(column)
                    .ok_or_else(|| anyhow!("Column '{}' not found", column))?;
                Ok(values.iter().any(|v| v == record_value))
            }
            CheckExpression::And(left, right) => Ok(self
                .evaluate_check_expression(left, record)?
                && self.evaluate_check_expression(right, record)?),
            CheckExpression::Or(left, right) => Ok(self.evaluate_check_expression(left, record)?
                || self.evaluate_check_expression(right, record)?),
            CheckExpression::Not(inner) => Ok(!self.evaluate_check_expression(inner, record)?),
        }
    }

    /// Compare two JSON values
    fn compare_values(&self, left: &Value, op: &ComparisonOp, right: &Value) -> bool {
        match (left, right) {
            (Value::Number(l), Value::Number(r)) => {
                let l_val = l.as_f64().unwrap_or(0.0);
                let r_val = r.as_f64().unwrap_or(0.0);
                match op {
                    ComparisonOp::Equal => l_val == r_val,
                    ComparisonOp::NotEqual => l_val != r_val,
                    ComparisonOp::LessThan => l_val < r_val,
                    ComparisonOp::LessThanOrEqual => l_val <= r_val,
                    ComparisonOp::GreaterThan => l_val > r_val,
                    ComparisonOp::GreaterThanOrEqual => l_val >= r_val,
                }
            }
            (Value::String(l), Value::String(r)) => match op {
                ComparisonOp::Equal => l == r,
                ComparisonOp::NotEqual => l != r,
                ComparisonOp::LessThan => l < r,
                ComparisonOp::LessThanOrEqual => l <= r,
                ComparisonOp::GreaterThan => l > r,
                ComparisonOp::GreaterThanOrEqual => l >= r,
            },
            (Value::Bool(l), Value::Bool(r)) => match op {
                ComparisonOp::Equal => l == r,
                ComparisonOp::NotEqual => l != r,
                _ => false,
            },
            _ => left == right && matches!(op, ComparisonOp::Equal),
        }
    }

    /// Validate unique constraint
    fn validate_unique_constraint(
        &self,
        table: &Schema,
        columns: &[String],
        record: &Value,
        _engine: &Engine,
    ) -> Result<()> {
        // Build unique key from column values
        let mut key_parts = Vec::new();
        for col in columns {
            let val = record
                .get(col)
                .ok_or_else(|| anyhow!("Column '{}' not found", col))?;
            key_parts.push(val.to_string());
        }
        let unique_key = format!("{}.{}", table.name, key_parts.join("_"));

        // Check if value already exists
        // In production, this would query the actual data
        // For now, we'll return Ok as a placeholder
        _ = unique_key; // Suppress warning
        Ok(())
    }

    /// Validate foreign key constraint
    fn validate_foreign_key(
        &self,
        columns: &[String],
        reference_table: &str,
        _reference_columns: &[String],
        record: &Value,
        _engine: &Engine,
    ) -> Result<()> {
        // Extract foreign key values from record
        let mut fk_values = Vec::new();
        for col in columns {
            let val = record
                .get(col)
                .ok_or_else(|| anyhow!("Column '{}' not found", col))?;
            if !val.is_null() {
                fk_values.push(val.clone());
            }
        }

        // If all FK values are null, constraint is satisfied
        if fk_values.is_empty() {
            return Ok(());
        }

        // Check if referenced record exists
        // In production, this would query the reference table
        // For now, we'll return Ok as a placeholder
        _ = reference_table; // Suppress warning
        Ok(())
    }

    /// Check if columns changed between old and new record
    fn columns_changed(&self, columns: &[String], old_record: &Value, new_record: &Value) -> bool {
        for col in columns {
            let old_val = old_record.get(col);
            let new_val = new_record.get(col);
            if old_val != new_val {
                return true;
            }
        }
        false
    }

    /// Find records that reference a given record
    fn find_referencing_records(
        &self,
        child_table: &str,
        _child_columns: &[String],
        _parent_record: &Value,
        _parent_columns: &[String],
        _engine: &Engine,
    ) -> Result<Vec<Value>> {
        // In production, this would query the child table for matching records
        // For now, return empty vec as placeholder
        _ = child_table; // Suppress warning
        Ok(Vec::new())
    }

    /// Validate constraint definition
    fn validate_constraint_definition(&self, constraint: &Constraint) -> Result<()> {
        match &constraint.constraint_type {
            ConstraintType::ForeignKey {
                columns,
                reference_columns,
                ..
            } => {
                if columns.len() != reference_columns.len() {
                    return Err(anyhow!(
                        "Foreign key column count mismatch: {} local columns vs {} reference columns",
                        columns.len(),
                        reference_columns.len()
                    ));
                }
            }
            ConstraintType::Unique { columns } | ConstraintType::PrimaryKey { columns } => {
                if columns.is_empty() {
                    return Err(anyhow!("Constraint must specify at least one column"));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl ForeignKeyGraph {
    fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
        }
    }

    fn add_dependency(
        &mut self,
        parent_table: String,
        child_table: String,
        constraint: Constraint,
    ) {
        self.dependencies
            .entry(parent_table)
            .or_insert_with(Vec::new)
            .push((child_table, constraint));
    }

    fn get_dependencies(&self, table: &str) -> Option<&Vec<(String, Constraint)>> {
        self.dependencies.get(table)
    }
}

/// Actions to perform as result of cascade operations
#[derive(Debug)]
pub enum CascadeAction {
    Delete {
        table: String,
        records: Vec<Value>,
    },
    SetNull {
        table: String,
        columns: Vec<String>,
        records: Vec<Value>,
    },
}

/// Parse CHECK constraint expression from SQL
pub fn parse_check_expression(expr_str: &str) -> Result<CheckExpression> {
    // Simple parser for basic CHECK expressions
    // In production, this would use a proper SQL expression parser

    let expr_str = expr_str.trim();

    // Handle simple comparisons: column op value
    if expr_str.contains(">=") {
        let parts: Vec<&str> = expr_str.split(">=").collect();
        if parts.len() == 2 {
            return Ok(CheckExpression::Comparison {
                column: parts[0].trim().to_string(),
                operator: ComparisonOp::GreaterThanOrEqual,
                value: parse_value(parts[1].trim())?,
            });
        }
    } else if expr_str.contains("<=") {
        let parts: Vec<&str> = expr_str.split("<=").collect();
        if parts.len() == 2 {
            return Ok(CheckExpression::Comparison {
                column: parts[0].trim().to_string(),
                operator: ComparisonOp::LessThanOrEqual,
                value: parse_value(parts[1].trim())?,
            });
        }
    } else if expr_str.contains('>') {
        let parts: Vec<&str> = expr_str.split('>').collect();
        if parts.len() == 2 {
            return Ok(CheckExpression::Comparison {
                column: parts[0].trim().to_string(),
                operator: ComparisonOp::GreaterThan,
                value: parse_value(parts[1].trim())?,
            });
        }
    } else if expr_str.contains('<') {
        let parts: Vec<&str> = expr_str.split('<').collect();
        if parts.len() == 2 {
            return Ok(CheckExpression::Comparison {
                column: parts[0].trim().to_string(),
                operator: ComparisonOp::LessThan,
                value: parse_value(parts[1].trim())?,
            });
        }
    } else if expr_str.contains('=') {
        let parts: Vec<&str> = expr_str.split('=').collect();
        if parts.len() == 2 {
            return Ok(CheckExpression::Comparison {
                column: parts[0].trim().to_string(),
                operator: ComparisonOp::Equal,
                value: parse_value(parts[1].trim())?,
            });
        }
    } else if expr_str.contains("BETWEEN") {
        // Handle BETWEEN expressions
        let parts: Vec<&str> = expr_str.split("BETWEEN").collect();
        if parts.len() == 2 {
            let column = parts[0].trim().to_string();
            let range_parts: Vec<&str> = parts[1].split("AND").collect();
            if range_parts.len() == 2 {
                return Ok(CheckExpression::Between {
                    column,
                    min: parse_value(range_parts[0].trim())?,
                    max: parse_value(range_parts[1].trim())?,
                });
            }
        }
    } else if expr_str.contains("IN") {
        // Handle IN expressions
        let parts: Vec<&str> = expr_str.split("IN").collect();
        if parts.len() == 2 {
            let column = parts[0].trim().to_string();
            let values_str = parts[1]
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')');
            let values: Result<Vec<Value>> = values_str
                .split(',')
                .map(|v| parse_value(v.trim()))
                .collect();
            return Ok(CheckExpression::In {
                column,
                values: values?,
            });
        }
    }

    Err(anyhow!("Cannot parse CHECK expression: {}", expr_str))
}

/// Parse a value from a string
fn parse_value(s: &str) -> Result<Value> {
    let s = s.trim();

    // String literal
    if s.starts_with('\'') && s.ends_with('\'') {
        return Ok(Value::String(s[1..s.len() - 1].to_string()));
    }

    // Boolean
    if s.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }

    // Number
    if let Ok(n) = s.parse::<i64>() {
        return Ok(Value::Number(serde_json::Number::from(n)));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Ok(Value::Number(serde_json::Number::from_f64(n).unwrap()));
    }

    // Default to string without quotes
    Ok(Value::String(s.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_check_expression() {
        // Test simple comparison
        let expr = parse_check_expression("age >= 18").unwrap();
        match expr {
            CheckExpression::Comparison {
                column,
                operator,
                value,
            } => {
                assert_eq!(column, "age");
                assert!(matches!(operator, ComparisonOp::GreaterThanOrEqual));
                assert_eq!(value, Value::Number(serde_json::Number::from(18)));
            }
            _ => panic!("Wrong expression type"),
        }

        // Test BETWEEN
        let expr = parse_check_expression("price BETWEEN 10 AND 100").unwrap();
        match expr {
            CheckExpression::Between { column, min, max } => {
                assert_eq!(column, "price");
                assert_eq!(min, Value::Number(serde_json::Number::from(10)));
                assert_eq!(max, Value::Number(serde_json::Number::from(100)));
            }
            _ => panic!("Wrong expression type"),
        }

        // Test IN
        let expr = parse_check_expression("status IN ('active', 'pending')").unwrap();
        match expr {
            CheckExpression::In { column, values } => {
                assert_eq!(column, "status");
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::String("active".to_string()));
                assert_eq!(values[1], Value::String("pending".to_string()));
            }
            _ => panic!("Wrong expression type"),
        }
    }

    #[test]
    fn test_constraint_validation() {
        use tempfile::tempdir;

        let mut mgr = ConstraintManager::new();

        // Add NOT NULL constraint
        let constraint = Constraint {
            name: "email_not_null".to_string(),
            constraint_type: ConstraintType::NotNull {
                column: "email".to_string(),
            },
            table_name: "users".to_string(),
            is_deferrable: false,
            initially_deferred: false,
        };
        mgr.add_constraint(constraint).unwrap();

        // Test record with null email
        let mut record = serde_json::json!({
            "id": 1,
            "name": "John",
            "email": null
        });

        let table = Schema {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            columns: vec![],
        };

        // This should fail
        let temp_dir = tempdir().unwrap();
        let engine = Engine::init(temp_dir.path()).unwrap();
        let result = mgr.validate_insert(&table, &mut record, &engine);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NOT NULL"));
    }
}
