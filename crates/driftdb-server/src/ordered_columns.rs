//! Ordered column system to fix HashMap ordering issues

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Ordered row that maintains column order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedRow {
    /// Column names in order
    columns: Vec<String>,
    /// Column values mapped by name
    values: HashMap<String, Value>,
}

impl OrderedRow {
    /// Create a new ordered row
    pub fn new(columns: Vec<String>, values: HashMap<String, Value>) -> Self {
        Self { columns, values }
    }

    /// Create from unordered HashMap using schema
    pub fn from_hashmap(data: HashMap<String, Value>, schema: &TableSchema) -> Self {
        let columns = schema.get_column_order();
        Self {
            columns: columns.clone(),
            values: data,
        }
    }

    /// Get columns in order
    pub fn get_columns(&self) -> &[String] {
        &self.columns
    }

    /// Get value by column name
    pub fn get_value(&self, column: &str) -> Option<&Value> {
        self.values.get(column)
    }

    /// Get all values in column order
    pub fn get_ordered_values(&self) -> Vec<Option<&Value>> {
        self.columns.iter()
            .map(|col| self.values.get(col))
            .collect()
    }

    /// Convert to JSON array in column order
    pub fn to_ordered_array(&self) -> Vec<Value> {
        self.columns.iter()
            .map(|col| {
                self.values.get(col)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect()
    }

    /// Convert to JSON object
    pub fn to_object(&self) -> Value {
        Value::Object(
            self.columns.iter()
                .filter_map(|col| {
                    self.values.get(col).map(|v| (col.clone(), v.clone()))
                })
                .collect()
        )
    }
}

/// Table schema that defines column order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    table_name: String,
    /// Ordered list of columns
    columns: Vec<ColumnDefinition>,
    /// Primary key column
    primary_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    name: String,
    /// Column data type
    data_type: ColumnType,
    /// Is nullable
    nullable: bool,
    /// Default value
    default_value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Boolean,
    Json,
    Timestamp,
}

impl TableSchema {
    /// Create a new table schema
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            columns: Vec::new(),
            primary_key: None,
        }
    }

    /// Add a column to the schema
    pub fn add_column(&mut self, column: ColumnDefinition) {
        self.columns.push(column);
    }

    /// Set primary key
    pub fn set_primary_key(&mut self, column_name: String) {
        self.primary_key = Some(column_name);
    }

    /// Get column order
    pub fn get_column_order(&self) -> Vec<String> {
        self.columns.iter()
            .map(|col| col.name.clone())
            .collect()
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns.iter()
            .find(|col| col.name == name)
    }

    /// Validate row data against schema
    pub fn validate_row(&self, data: &HashMap<String, Value>) -> Result<(), String> {
        for column in &self.columns {
            if let Some(value) = data.get(&column.name) {
                // Validate data type
                if !self.validate_type(value, &column.data_type) {
                    return Err(format!(
                        "Invalid type for column '{}': expected {:?}",
                        column.name, column.data_type
                    ));
                }
            } else if !column.nullable && column.default_value.is_none() {
                return Err(format!(
                    "Missing required column '{}'",
                    column.name
                ));
            }
        }
        Ok(())
    }

    /// Validate value type
    fn validate_type(&self, value: &Value, expected_type: &ColumnType) -> bool {
        match expected_type {
            ColumnType::Integer => value.is_i64() || value.is_u64(),
            ColumnType::Float => value.is_f64() || value.is_i64(),
            ColumnType::String => value.is_string(),
            ColumnType::Boolean => value.is_boolean(),
            ColumnType::Json => true, // Any JSON value is valid
            ColumnType::Timestamp => value.is_string() || value.is_i64(),
        }
    }

    /// Apply defaults to row data
    pub fn apply_defaults(&self, data: &mut HashMap<String, Value>) {
        for column in &self.columns {
            if !data.contains_key(&column.name) {
                if let Some(default) = &column.default_value {
                    data.insert(column.name.clone(), default.clone());
                } else if column.nullable {
                    data.insert(column.name.clone(), Value::Null);
                }
            }
        }
    }
}

/// Schema registry for all tables
pub struct SchemaRegistry {
    schemas: HashMap<String, TableSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register a table schema
    pub fn register_schema(&mut self, schema: TableSchema) {
        self.schemas.insert(schema.table_name.clone(), schema);
    }

    /// Get schema for a table
    pub fn get_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    /// Update schema
    pub fn update_schema(&mut self, table_name: &str, schema: TableSchema) {
        self.schemas.insert(table_name.to_string(), schema);
    }

    /// Remove schema
    pub fn remove_schema(&mut self, table_name: &str) {
        self.schemas.remove(table_name);
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
}

/// Convert unordered query results to ordered format
pub fn order_query_results(
    rows: Vec<HashMap<String, Value>>,
    schema: &TableSchema,
) -> Vec<OrderedRow> {
    rows.into_iter()
        .map(|row| OrderedRow::from_hashmap(row, schema))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordered_row() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::from(1));
        values.insert("name".to_string(), Value::from("Alice"));
        values.insert("age".to_string(), Value::from(30));

        let row = OrderedRow::new(columns.clone(), values);

        // Check column order is preserved
        assert_eq!(row.get_columns(), &columns);

        // Check ordered array
        let array = row.to_ordered_array();
        assert_eq!(array[0], Value::from(1));
        assert_eq!(array[1], Value::from("Alice"));
        assert_eq!(array[2], Value::from(30));
    }

    #[test]
    fn test_table_schema() {
        let mut schema = TableSchema::new("users".to_string());

        schema.add_column(ColumnDefinition {
            name: "id".to_string(),
            data_type: ColumnType::Integer,
            nullable: false,
            default_value: None,
        });

        schema.add_column(ColumnDefinition {
            name: "name".to_string(),
            data_type: ColumnType::String,
            nullable: false,
            default_value: None,
        });

        schema.add_column(ColumnDefinition {
            name: "email".to_string(),
            data_type: ColumnType::String,
            nullable: true,
            default_value: None,
        });

        schema.set_primary_key("id".to_string());

        // Test column order
        let order = schema.get_column_order();
        assert_eq!(order, vec!["id", "name", "email"]);

        // Test validation
        let mut data = HashMap::new();
        data.insert("id".to_string(), Value::from(1));
        data.insert("name".to_string(), Value::from("Bob"));

        assert!(schema.validate_row(&data).is_ok());

        // Test missing required field
        let mut invalid_data = HashMap::new();
        invalid_data.insert("id".to_string(), Value::from(1));
        // Missing required 'name'

        assert!(schema.validate_row(&invalid_data).is_err());
    }

    #[test]
    fn test_schema_registry() {
        let mut registry = SchemaRegistry::new();

        let schema = TableSchema::new("products".to_string());
        registry.register_schema(schema);

        assert!(registry.get_schema("products").is_some());
        assert!(registry.get_schema("nonexistent").is_none());

        let tables = registry.list_tables();
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"products".to_string()));
    }

    #[test]
    fn test_apply_defaults() {
        let mut schema = TableSchema::new("items".to_string());

        schema.add_column(ColumnDefinition {
            name: "id".to_string(),
            data_type: ColumnType::Integer,
            nullable: false,
            default_value: None,
        });

        schema.add_column(ColumnDefinition {
            name: "status".to_string(),
            data_type: ColumnType::String,
            nullable: false,
            default_value: Some(Value::from("active")),
        });

        let mut data = HashMap::new();
        data.insert("id".to_string(), Value::from(1));
        // 'status' is missing but has default

        schema.apply_defaults(&mut data);

        assert_eq!(data.get("status"), Some(&Value::from("active")));
    }
}