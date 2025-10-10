#![allow(dead_code)]

use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A prepared statement with parameter placeholders
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The name of the statement (empty string for unnamed)
    pub name: String,
    /// The original SQL query
    pub sql: String,
    /// The parsed SQL with placeholders replaced
    pub parsed_sql: String,
    /// Number of parameters expected
    pub param_count: usize,
    /// Parameter types (if known)
    pub param_types: Vec<Option<i32>>,
}

/// A portal represents a prepared statement with bound parameters
#[derive(Debug, Clone)]
pub struct Portal {
    /// The name of the portal (empty string for unnamed)
    pub name: String,
    /// The prepared statement this portal is based on
    pub statement: PreparedStatement,
    /// The bound parameter values
    pub params: Vec<Option<Value>>,
}

/// Manages prepared statements and portals for a session
pub struct PreparedStatementManager {
    /// Prepared statements by name
    statements: Arc<RwLock<HashMap<String, PreparedStatement>>>,
    /// Portals by name
    portals: Arc<RwLock<HashMap<String, Portal>>>,
}

impl PreparedStatementManager {
    pub fn new() -> Self {
        Self {
            statements: Arc::new(RwLock::new(HashMap::new())),
            portals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Parse a SQL query and create a prepared statement
    pub fn parse_statement(
        &self,
        name: String,
        sql: String,
        param_types: Vec<i32>,
    ) -> Result<PreparedStatement> {
        // Count parameter placeholders ($1, $2, etc.)
        let mut max_param = 0;
        let mut i = 0;
        let bytes = sql.as_bytes();

        while i < bytes.len() {
            if bytes[i] == b'$' {
                // Check if this is followed by a number
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }

                if j > i + 1 {
                    // We found a parameter placeholder
                    let param_str = std::str::from_utf8(&bytes[i + 1..j])
                        .map_err(|e| anyhow!("Invalid UTF-8 in parameter: {}", e))?;
                    let param_num: usize = param_str
                        .parse()
                        .map_err(|e| anyhow!("Invalid parameter number: {}", e))?;
                    max_param = max_param.max(param_num);
                }
                i = j;
            } else if bytes[i] == b'\'' {
                // Skip string literals to avoid treating $ inside strings as parameters
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        // Check for escaped quote ''
                        if i < bytes.len() && bytes[i] == b'\'' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        let param_count = max_param;

        // Convert param_types to Option<i32> vec, padding with None if necessary
        let mut param_types_opt = Vec::with_capacity(param_count);
        for i in 0..param_count {
            if i < param_types.len() && param_types[i] != 0 {
                param_types_opt.push(Some(param_types[i]));
            } else {
                param_types_opt.push(None);
            }
        }

        let stmt = PreparedStatement {
            name: name.clone(),
            sql: sql.clone(),
            parsed_sql: sql, // For now, we keep the same SQL
            param_count,
            param_types: param_types_opt,
        };

        // Store the prepared statement
        self.statements.write().insert(name, stmt.clone());

        Ok(stmt)
    }

    /// Bind parameters to a prepared statement to create a portal
    pub fn bind_portal(
        &self,
        portal_name: String,
        statement_name: String,
        params: Vec<Option<Vec<u8>>>,
        param_formats: Vec<i16>,
        _result_formats: Vec<i16>,
    ) -> Result<Portal> {
        let statements = self.statements.read();
        let statement = statements
            .get(&statement_name)
            .ok_or_else(|| anyhow!("Prepared statement '{}' not found", statement_name))?
            .clone();

        // Validate parameter count
        if params.len() != statement.param_count {
            return Err(anyhow!(
                "Expected {} parameters, got {}",
                statement.param_count,
                params.len()
            ));
        }

        // Convert parameters to JSON values
        let mut param_values = Vec::with_capacity(params.len());
        for (i, param_opt) in params.iter().enumerate() {
            if let Some(param_bytes) = param_opt {
                if param_bytes.is_empty() {
                    param_values.push(None);
                } else {
                    // Get format code (0 = text, 1 = binary)
                    let format = if i < param_formats.len() {
                        param_formats[i]
                    } else if !param_formats.is_empty() {
                        param_formats[0] // Use first format for all if only one provided
                    } else {
                        0 // Default to text
                    };

                    let value = if format == 0 {
                        // Text format
                        let text = std::str::from_utf8(param_bytes)
                            .map_err(|e| anyhow!("Invalid UTF-8 in parameter {}: {}", i + 1, e))?;

                        // Try to parse as JSON, otherwise treat as string
                        if let Ok(json_val) = serde_json::from_str(text) {
                            json_val
                        } else {
                            Value::String(text.to_string())
                        }
                    } else {
                        // Binary format - for now we'll convert to hex string
                        // In a real implementation, we'd decode based on the type OID
                        let hex = hex::encode(param_bytes);
                        Value::String(format!("\\x{}", hex))
                    };

                    param_values.push(Some(value));
                }
            } else {
                // NULL parameter
                param_values.push(None);
            }
        }

        let portal = Portal {
            name: portal_name.clone(),
            statement,
            params: param_values,
        };

        // Store the portal
        self.portals.write().insert(portal_name, portal.clone());

        Ok(portal)
    }

    /// Execute a portal and return the SQL with parameters substituted
    pub fn execute_portal(&self, portal_name: &str, _max_rows: i32) -> Result<String> {
        let portals = self.portals.read();
        let portal = portals
            .get(portal_name)
            .ok_or_else(|| anyhow!("Portal '{}' not found", portal_name))?;

        // Substitute parameters in the SQL
        let mut sql = portal.statement.parsed_sql.clone();

        // Replace parameters in reverse order to avoid index shifting
        for i in (0..portal.params.len()).rev() {
            let param_placeholder = format!("${}", i + 1);
            let param_value = match &portal.params[i] {
                None => "NULL".to_string(),
                Some(Value::Null) => "NULL".to_string(),
                Some(Value::String(s)) => format!("'{}'", s.replace('\'', "''")),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                Some(v) => format!("'{}'", serde_json::to_string(v)?.replace('\'', "''")),
            };

            sql = sql.replace(&param_placeholder, &param_value);
        }

        Ok(sql)
    }

    /// Describe a prepared statement
    pub fn describe_statement(&self, name: &str) -> Result<PreparedStatement> {
        let statements = self.statements.read();
        statements
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("Prepared statement '{}' not found", name))
    }

    /// Describe a portal
    pub fn describe_portal(&self, name: &str) -> Result<Portal> {
        let portals = self.portals.read();
        portals
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("Portal '{}' not found", name))
    }

    /// Close a prepared statement
    pub fn close_statement(&self, name: &str) -> Result<()> {
        let mut statements = self.statements.write();
        statements
            .remove(name)
            .ok_or_else(|| anyhow!("Prepared statement '{}' not found", name))?;
        Ok(())
    }

    /// Close a portal
    pub fn close_portal(&self, name: &str) -> Result<()> {
        let mut portals = self.portals.write();
        portals
            .remove(name)
            .ok_or_else(|| anyhow!("Portal '{}' not found", name))?;
        Ok(())
    }

    /// Clear all prepared statements and portals
    pub fn clear(&self) {
        self.statements.write().clear();
        self.portals.write().clear();
    }
}

impl Default for PreparedStatementManager {
    fn default() -> Self {
        Self::new()
    }
}
