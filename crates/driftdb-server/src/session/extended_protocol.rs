use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use serde_json::Value;
use parking_lot::Mutex;
use crate::protocol::Message;
use crate::executor::QueryExecutor;

/// A prepared statement with parameters
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: String,
    pub query: String,
    pub parameter_types: Vec<i32>,
}

/// A portal represents a ready-to-execute statement
#[derive(Debug, Clone)]
pub struct Portal {
    pub name: String,
    pub statement: PreparedStatement,
    pub parameters: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<i16>,
}

/// Extended Query Protocol handler
pub struct ExtendedProtocol {
    prepared_statements: Arc<Mutex<HashMap<String, PreparedStatement>>>,
    portals: Arc<Mutex<HashMap<String, Portal>>>,
}

impl ExtendedProtocol {
    pub fn new() -> Self {
        Self {
            prepared_statements: Arc::new(Mutex::new(HashMap::new())),
            portals: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Handle Parse message - create a prepared statement
    pub fn handle_parse(
        &self,
        statement_name: String,
        query: String,
        parameter_types: Vec<i32>,
    ) -> Result<Message> {
        let stmt = PreparedStatement {
            name: statement_name.clone(),
            query,
            parameter_types,
        };

        self.prepared_statements.lock().insert(statement_name, stmt);
        Ok(Message::ParseComplete)
    }

    /// Handle Bind message - create a portal from a prepared statement
    pub fn handle_bind(
        &self,
        portal_name: String,
        statement_name: String,
        parameter_formats: Vec<i16>,
        parameters: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<Message> {
        let statements = self.prepared_statements.lock();
        let statement = statements
            .get(&statement_name)
            .ok_or_else(|| anyhow!("Prepared statement '{}' not found", statement_name))?
            .clone();

        // Validate parameter count
        if parameters.len() != statement.parameter_types.len() {
            return Err(anyhow!(
                "Parameter count mismatch: expected {}, got {}",
                statement.parameter_types.len(),
                parameters.len()
            ));
        }

        let portal = Portal {
            name: portal_name.clone(),
            statement,
            parameters,
            result_formats,
        };

        self.portals.lock().insert(portal_name, portal);
        Ok(Message::BindComplete)
    }

    /// Handle Execute message - run a portal
    pub async fn handle_execute(
        &self,
        portal_name: &str,
        max_rows: i32,
        executor: &QueryExecutor<'_>,
    ) -> Result<Vec<Message>> {
        let portal = {
            let portals = self.portals.lock();
            portals
                .get(portal_name)
                .ok_or_else(|| anyhow!("Portal '{}' not found", portal_name))?
                .clone()
        };

        // Substitute parameters into the query
        let query = self.substitute_parameters(&portal.statement.query, &portal.parameters)?;

        // Execute the query
        let result = executor.execute(&query).await?;

        // Convert result to messages
        let messages = self.format_result(result, &portal.result_formats)?;
        Ok(messages)
    }

    /// Handle Describe message - get metadata about a statement or portal
    pub fn handle_describe(&self, typ: u8, name: &str) -> Result<Vec<Message>> {
        match typ {
            b'S' => {
                // Describe prepared statement
                let statements = self.prepared_statements.lock();
                let stmt = statements
                    .get(name)
                    .ok_or_else(|| anyhow!("Prepared statement '{}' not found", name))?;

                // Return parameter description
                let param_desc = Message::ParameterDescription {
                    types: stmt.parameter_types.clone(),
                };

                // For now, return NoData for row description
                // In a full implementation, we'd parse the query to determine columns
                Ok(vec![param_desc, Message::NoData])
            }
            b'P' => {
                // Describe portal
                let portals = self.portals.lock();
                let portal = portals
                    .get(name)
                    .ok_or_else(|| anyhow!("Portal '{}' not found", name))?;

                // Return parameter description
                let param_desc = Message::ParameterDescription {
                    types: portal.statement.parameter_types.clone(),
                };

                // For now, return NoData for row description
                Ok(vec![param_desc, Message::NoData])
            }
            _ => Err(anyhow!("Invalid describe type: {}", typ as char)),
        }
    }

    /// Handle Close message - close a statement or portal
    pub fn handle_close(&self, typ: u8, name: &str) -> Result<Message> {
        match typ {
            b'S' => {
                // Close prepared statement
                self.prepared_statements.lock().remove(name);
                Ok(Message::CloseComplete)
            }
            b'P' => {
                // Close portal
                self.portals.lock().remove(name);
                Ok(Message::CloseComplete)
            }
            _ => Err(anyhow!("Invalid close type: {}", typ as char)),
        }
    }

    /// Substitute parameters into a query
    fn substitute_parameters(
        &self,
        query: &str,
        parameters: &[Option<Vec<u8>>],
    ) -> Result<String> {
        let mut result = query.to_string();

        // Replace $1, $2, etc. with actual parameter values
        for (i, param) in parameters.iter().enumerate() {
            let placeholder = format!("${}", i + 1);
            let value = match param {
                Some(bytes) => {
                    // Convert bytes to string representation
                    // This is simplified - real implementation would handle types properly
                    String::from_utf8_lossy(bytes).to_string()
                }
                None => "NULL".to_string(),
            };

            // Quote string values (simplified)
            let quoted_value = if value == "NULL" {
                value
            } else if value.parse::<f64>().is_ok() {
                // Numeric value
                value
            } else {
                // String value - needs quoting
                format!("'{}'", value.replace('\'', "''"))
            };

            result = result.replace(&placeholder, &quoted_value);
        }

        Ok(result)
    }

    /// Format query result as protocol messages
    fn format_result(
        &self,
        result: crate::executor::QueryResult,
        _result_formats: &[i16],
    ) -> Result<Vec<Message>> {
        use crate::executor::QueryResult;

        match result {
            QueryResult::Select { columns, rows } => {
                let mut messages = Vec::new();

                // Send row description
                let fields = columns
                    .iter()
                    .map(|col| crate::protocol::FieldDescription::new(
                        col.clone(),
                        crate::protocol::DataType::Text,
                    ))
                    .collect();

                messages.push(Message::RowDescription { fields });

                // Send data rows
                for row in rows {
                    let values = row
                        .into_iter()
                        .map(|val| match val {
                            Value::Null => None,
                            v => Some(v.to_string().into_bytes()),
                        })
                        .collect();
                    messages.push(Message::DataRow { values });
                }

                Ok(messages)
            }
            QueryResult::Insert { count } => {
                Ok(vec![Message::CommandComplete {
                    tag: format!("INSERT 0 {}", count),
                }])
            }
            QueryResult::Update { count } => {
                Ok(vec![Message::CommandComplete {
                    tag: format!("UPDATE {}", count),
                }])
            }
            QueryResult::Delete { count } => {
                Ok(vec![Message::CommandComplete {
                    tag: format!("DELETE {}", count),
                }])
            }
            QueryResult::CreateTable => {
                Ok(vec![Message::CommandComplete {
                    tag: "CREATE TABLE".to_string(),
                }])
            }
            QueryResult::Begin => {
                Ok(vec![Message::CommandComplete {
                    tag: "BEGIN".to_string(),
                }])
            }
            QueryResult::Commit => {
                Ok(vec![Message::CommandComplete {
                    tag: "COMMIT".to_string(),
                }])
            }
            QueryResult::Rollback => {
                Ok(vec![Message::CommandComplete {
                    tag: "ROLLBACK".to_string(),
                }])
            }
            QueryResult::Empty => Ok(vec![Message::EmptyQueryResponse]),
        }
    }

    /// Clear all prepared statements and portals
    pub fn reset(&self) {
        self.prepared_statements.lock().clear();
        self.portals.lock().clear();
    }
}