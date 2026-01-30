//! DriftDB client connection and query execution

use crate::error::{Error, Result};
use crate::query::Query;
use crate::transaction::Transaction;
use crate::types::{Row, Value};
use tokio_postgres::{Client as PgClient, NoTls};
use tracing::{debug, info};

/// DriftDB client for executing queries
///
/// The client maintains a connection to a DriftDB server and provides
/// methods for executing queries, transactions, and time-travel operations.
pub struct Client {
    inner: PgClient,
}

impl Client {
    /// Connect to a DriftDB server
    ///
    /// # Arguments
    ///
    /// * `host` - The host and port (e.g., "localhost:5433")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("localhost:5433").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(host: &str) -> Result<Self> {
        info!("Connecting to DriftDB at {}", host);

        // Parse host:port
        let connection_string =
            if host.starts_with("postgresql://") || host.starts_with("postgres://") {
                host.to_string()
            } else {
                // Default to PostgreSQL connection string format
                format!("postgresql://{}/?sslmode=disable", host)
            };

        debug!("Connection string: {}", connection_string);

        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        info!("Successfully connected to DriftDB");
        Ok(Self { inner: client })
    }

    /// Execute a SQL statement that doesn't return rows
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// client.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)").await?;
    /// client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        debug!("Executing SQL: {}", sql);

        // NOTE: We use simple_query() instead of the prepared statement protocol
        // (query/execute with parameters) because the DriftDB server currently
        // has incomplete support for the PostgreSQL extended query protocol
        // (Parse/Bind/Describe/Execute/Sync message sequence). The simple query
        // protocol sends SQL directly and works reliably for all operations.
        let messages = self
            .inner
            .simple_query(sql)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        // Count affected rows from CommandComplete messages
        let mut rows = 0u64;
        for msg in messages {
            if let tokio_postgres::SimpleQueryMessage::CommandComplete(count) = msg {
                rows = count;
            }
        }
        debug!("Affected {} rows", rows);
        Ok(rows)
    }

    /// Execute a SQL statement with parameters (safe from SQL injection)
    ///
    /// Uses PostgreSQL-style placeholders ($1, $2, etc.) to safely interpolate values.
    /// This method uses the extended query protocol and may have compatibility issues
    /// with some DriftDB server versions.
    ///
    /// **Note**: Currently not working due to incomplete server support for extended
    /// query protocol. Use this method in the future when server support is complete.
    /// For now, consider using client-side escaping with caution.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// client.execute_params(
    ///     "INSERT INTO users (id, name) VALUES ($1, $2)",
    ///     &[&1i64, &"Alice"],
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_params(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64> {
        debug!("Executing SQL with {} params: {}", params.len(), sql);

        let rows = self
            .inner
            .execute(sql, params)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        debug!("Affected {} rows", rows);
        Ok(rows)
    }

    /// Execute a SQL statement with escaped string parameters
    ///
    /// **SECURITY WARNING**: This method uses client-side escaping which is less secure
    /// than true parameterized queries. Only use this for trusted input or when the
    /// server doesn't support the extended query protocol. The proper `execute_params()`
    /// method should be preferred once server support is available.
    ///
    /// Replaces $1, $2, etc. placeholders with escaped string values.
    /// Only supports string, integer, and boolean values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// use driftdb_client::types::Value;
    /// client.execute_escaped(
    ///     "INSERT INTO users (id, name) VALUES ($1, $2)",
    ///     &[Value::Int(1), Value::Text("O'Reilly".to_string())],
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_escaped(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let escaped_sql = Self::escape_params(sql, params)?;
        debug!("Executing escaped SQL: {}", escaped_sql);
        self.execute(&escaped_sql).await
    }

    /// Execute a query and return all rows
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let rows = client.query("SELECT * FROM users").await?;
    /// for row in rows {
    ///     println!("Row: {:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>> {
        debug!("Querying: {}", sql);

        // NOTE: We use simple_query() instead of the prepared statement protocol.
        // See execute() method for detailed explanation.
        let messages = self
            .inner
            .simple_query(sql)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        // Extract rows from messages
        let mut rows = Vec::new();
        for msg in messages {
            if let tokio_postgres::SimpleQueryMessage::Row(simple_row) = msg {
                rows.push(self.simple_row_to_row(simple_row));
            }
        }
        debug!("Returned {} rows", rows.len());
        Ok(rows)
    }

    /// Execute a query with parameters and return all rows (safe from SQL injection)
    ///
    /// Uses PostgreSQL-style placeholders ($1, $2, etc.) to safely interpolate values.
    /// This method uses the extended query protocol and may have compatibility issues
    /// with some DriftDB server versions.
    ///
    /// **Note**: Currently not working due to incomplete server support for extended
    /// query protocol. Use `query_escaped()` as a temporary workaround.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let rows = client.query_params(
    ///     "SELECT * FROM users WHERE id = $1",
    ///     &[&1i64],
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_params(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        debug!("Querying with {} params: {}", params.len(), sql);

        let pg_rows = self
            .inner
            .query(sql, params)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let rows: Vec<Row> = pg_rows
            .into_iter()
            .map(|pg_row| self.pg_row_to_row(pg_row))
            .collect();

        debug!("Returned {} rows", rows.len());
        Ok(rows)
    }

    /// Execute a query with escaped parameters and return all rows
    ///
    /// **SECURITY WARNING**: This method uses client-side escaping which is less secure
    /// than true parameterized queries. Only use this for trusted input or when the
    /// server doesn't support the extended query protocol.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// use driftdb_client::types::Value;
    /// let rows = client.query_escaped(
    ///     "SELECT * FROM users WHERE name = $1",
    ///     &[Value::Text("O'Reilly".to_string())],
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_escaped(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let escaped_sql = Self::escape_params(sql, params)?;
        debug!("Querying with escaped SQL: {}", escaped_sql);
        self.query(&escaped_sql).await
    }

    /// Execute a query and deserialize results into a typed struct
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)]
    /// # struct User { id: i64, name: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let users: Vec<User> = client.query_as("SELECT * FROM users").await?;
    /// for user in users {
    ///     println!("User: {} - {}", user.id, user.name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_as<T: serde::de::DeserializeOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let rows = self.query(sql).await?;
        rows.into_iter()
            .map(|row| row.deserialize())
            .collect::<std::result::Result<Vec<T>, _>>()
            .map_err(Error::from)
    }

    /// Start building a query with builder pattern
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::{Client, TimeTravel};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let rows = client
    ///     .query_builder("SELECT * FROM users")
    ///     .as_of(TimeTravel::Sequence(42))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_builder(&self, sql: impl Into<String>) -> Query<'_> {
        Query::new(self, sql.into())
    }

    /// Begin a transaction
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let mut tx = client.begin().await?;
    /// tx.execute("INSERT INTO users VALUES (2, 'Bob')").await?;
    /// tx.execute("INSERT INTO users VALUES (3, 'Charlie')").await?;
    /// tx.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn begin(&self) -> Result<Transaction> {
        Transaction::begin(&self.inner).await
    }

    /// Helper function to escape parameters and replace $N placeholders
    fn escape_params(sql: &str, params: &[Value]) -> Result<String> {
        let mut result = sql.to_string();

        // Replace placeholders in reverse order to avoid issues with $1 vs $10
        for (idx, param) in params.iter().enumerate().rev() {
            let placeholder = format!("${}", idx + 1);
            let escaped_value = match param {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Text(s) => {
                    // Escape single quotes by doubling them (SQL standard)
                    let escaped = s.replace('\'', "''");
                    format!("'{}'", escaped)
                }
                Value::Bytes(_) => {
                    return Err(Error::Query(
                        "Byte arrays are not supported in escaped queries".to_string(),
                    ));
                }
                Value::Json(j) => {
                    // Serialize JSON and escape it as a string
                    let json_str = serde_json::to_string(j)
                        .map_err(|e| Error::Query(format!("Failed to serialize JSON: {}", e)))?;
                    let escaped = json_str.replace('\'', "''");
                    format!("'{}'", escaped)
                }
            };

            result = result.replace(&placeholder, &escaped_value);
        }

        Ok(result)
    }

    /// Convert a SimpleQueryRow to our Row type
    fn simple_row_to_row(&self, simple_row: tokio_postgres::SimpleQueryRow) -> Row {
        let columns: Vec<String> = simple_row
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let values: Vec<Value> = (0..simple_row.len())
            .map(|idx| {
                match simple_row.get(idx) {
                    Some(s) => {
                        // Try to parse as different types
                        if s == "t" || s == "true" {
                            Value::Bool(true)
                        } else if s == "f" || s == "false" {
                            Value::Bool(false)
                        } else if let Ok(i) = s.parse::<i64>() {
                            Value::Int(i)
                        } else if let Ok(f) = s.parse::<f64>() {
                            Value::Float(f)
                        } else {
                            Value::Text(s.to_string())
                        }
                    }
                    None => Value::Null,
                }
            })
            .collect();

        Row::new(columns, values)
    }

    /// Convert a PostgreSQL Row from extended query protocol to our Row type
    fn pg_row_to_row(&self, pg_row: tokio_postgres::Row) -> Row {
        let columns: Vec<String> = pg_row
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let values: Vec<Value> = (0..pg_row.len())
            .map(|idx| {
                // Try to get the value as different types
                if let Ok(v) = pg_row.try_get::<_, Option<bool>>(idx) {
                    v.map(Value::Bool).unwrap_or(Value::Null)
                } else if let Ok(v) = pg_row.try_get::<_, Option<i64>>(idx) {
                    v.map(Value::Int).unwrap_or(Value::Null)
                } else if let Ok(v) = pg_row.try_get::<_, Option<f64>>(idx) {
                    v.map(Value::Float).unwrap_or(Value::Null)
                } else if let Ok(v) = pg_row.try_get::<_, Option<String>>(idx) {
                    v.map(Value::Text).unwrap_or(Value::Null)
                } else if let Ok(v) = pg_row.try_get::<_, Option<Vec<u8>>>(idx) {
                    v.map(Value::Bytes).unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            })
            .collect();

        Row::new(columns, values)
    }

    /// Get the current sequence number
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let seq = client.current_sequence().await?;
    /// println!("Current sequence: {}", seq);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn current_sequence(&self) -> Result<u64> {
        let rows = self
            .query("SELECT MAX(sequence) FROM __driftdb_metadata__")
            .await?;

        rows.first()
            .and_then(|row| row.get_idx(0))
            .and_then(|v| v.as_i64())
            .map(|i| i as u64)
            .ok_or_else(|| Error::Query("Failed to get current sequence".to_string()))
    }

    /// Close the connection gracefully
    pub async fn close(self) -> Result<()> {
        // The connection will be closed when client is dropped
        // This is here for explicit API
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Connection tests require running server - see integration_tests.rs
}
