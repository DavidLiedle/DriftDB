//! Query builder for DriftDB with time-travel support

use crate::client::Client;
use crate::error::Result;
use crate::types::{Row, TimeTravel};
use tracing::debug;

/// Query builder with support for time-travel queries
///
/// Provides a fluent API for building and executing queries with optional
/// time-travel specifications.
pub struct Query<'a> {
    client: &'a Client,
    sql: String,
    time_travel: Option<TimeTravel>,
}

impl<'a> Query<'a> {
    /// Create a new query builder
    pub(crate) fn new(client: &'a Client, sql: String) -> Self {
        Self {
            client,
            sql,
            time_travel: None,
        }
    }

    /// Execute query at a specific point in time
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::{Client, TimeTravel};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// // Query at sequence 42
    /// let rows = client
    ///     .query_builder("SELECT * FROM users")
    ///     .as_of(TimeTravel::Sequence(42))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_of(mut self, time_travel: TimeTravel) -> Self {
        self.time_travel = Some(time_travel);
        self
    }

    /// Execute the query and return all rows
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::{Client, TimeTravel};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let rows = client
    ///     .query_builder("SELECT * FROM users WHERE active = true")
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(self) -> Result<Vec<Row>> {
        let sql = self.build_sql();
        debug!("Executing query: {}", sql);
        self.client.query(&sql).await
    }

    /// Execute the query and deserialize into typed structs
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use driftdb_client::{Client, TimeTravel};
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)]
    /// # struct User { id: i64, name: String }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("localhost:5433").await?;
    /// let users: Vec<User> = client
    ///     .query_builder("SELECT * FROM users")
    ///     .as_of(TimeTravel::Sequence(100))
    ///     .execute_as()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_as<T: serde::de::DeserializeOwned>(self) -> Result<Vec<T>> {
        let sql = self.build_sql();
        debug!("Executing typed query: {}", sql);
        self.client.query_as(&sql).await
    }

    /// Build the final SQL with time-travel clause
    fn build_sql(&self) -> String {
        match &self.time_travel {
            Some(tt) => {
                // Insert time-travel clause before WHERE/ORDER BY/LIMIT
                // Simple approach: append after FROM clause
                let sql = &self.sql;

                // Find FROM clause and insert time-travel after it
                if let Some(from_pos) = sql.to_uppercase().find(" FROM ") {
                    // Find the end of the table name (before WHERE, ORDER, LIMIT, semicolon, or end)
                    let search_start = from_pos + 6; // " FROM ".len()
                    let remaining = &sql[search_start..];

                    let end_pos = [
                        remaining.find(" WHERE"),
                        remaining.find(" ORDER"),
                        remaining.find(" LIMIT"),
                        remaining.find(" GROUP"),
                        remaining.find(";"),
                    ]
                    .into_iter()
                    .flatten()
                    .min()
                    .unwrap_or(remaining.len());

                    let table_part = &remaining[..end_pos];
                    let rest = &remaining[end_pos..];

                    format!(
                        "{} FROM {} {} {}",
                        &sql[..from_pos],
                        table_part,
                        tt.to_sql(),
                        rest
                    )
                } else {
                    // No FROM clause, just append (might not make sense but won't break)
                    format!("{} {}", sql, tt.to_sql())
                }
            }
            None => self.sql.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require an actual DriftDB server connection
    // For now, we test the SQL building logic with a different approach

    #[test]
    fn test_time_travel_sql_generation() {
        // Test that TimeTravel generates correct SQL
        assert_eq!(TimeTravel::Sequence(42).to_sql(), "FOR SYSTEM_TIME AS OF @SEQ:42");
        assert_eq!(TimeTravel::All.to_sql(), "FOR SYSTEM_TIME ALL");
    }
}
