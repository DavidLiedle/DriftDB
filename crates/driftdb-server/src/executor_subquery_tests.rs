#[cfg(test)]
mod subquery_tests {

    use crate::executor::{QueryExecutor, SubqueryExpression, SubqueryQuantifier, WhereCondition};
    use driftdb_core::Engine;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_executor() -> QueryExecutor<'static> {
        // Create a simple in-memory engine for testing
        // Note: This leaks the TempDir but that's acceptable for tests
        let temp_dir = Box::leak(Box::new(TempDir::new().unwrap()));
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        QueryExecutor::new(engine)
    }

    #[tokio::test]
    async fn test_parse_in_subquery() {
        let executor = create_test_executor();

        let condition = "id IN (SELECT user_id FROM orders)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::In {
                column,
                subquery,
                negated,
            } => {
                assert_eq!(column, "id");
                assert_eq!(subquery.sql, "SELECT user_id FROM orders");
                assert!(!negated);
            }
            _ => panic!("Expected IN subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_not_in_subquery() {
        let executor = create_test_executor();

        let condition = "id NOT IN (SELECT user_id FROM orders)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::In {
                column,
                subquery,
                negated,
            } => {
                assert_eq!(column, "id");
                assert_eq!(subquery.sql, "SELECT user_id FROM orders");
                assert!(negated);
            }
            _ => panic!("Expected NOT IN subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_exists_subquery() {
        let executor = create_test_executor();

        let condition = "EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::Exists { subquery, negated } => {
                assert_eq!(
                    subquery.sql,
                    "SELECT 1 FROM orders WHERE user_id = users.id"
                );
                assert!(!negated);
            }
            _ => panic!("Expected EXISTS subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_not_exists_subquery() {
        let executor = create_test_executor();

        let condition = "NOT EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::Exists { subquery, negated } => {
                assert_eq!(
                    subquery.sql,
                    "SELECT 1 FROM orders WHERE user_id = users.id"
                );
                assert!(negated);
            }
            _ => panic!("Expected NOT EXISTS subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_any_subquery() {
        let executor = create_test_executor();

        let condition = "price > ANY (SELECT amount FROM orders)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::Comparison {
                column,
                operator,
                quantifier,
                subquery,
            } => {
                assert_eq!(column, "price");
                assert_eq!(operator, ">");
                assert_eq!(quantifier, Some(SubqueryQuantifier::Any));
                assert_eq!(subquery.sql, "SELECT amount FROM orders");
            }
            _ => panic!("Expected ANY comparison subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_all_subquery() {
        let executor = create_test_executor();

        let condition = "price > ALL (SELECT amount FROM orders)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::Comparison {
                column,
                operator,
                quantifier,
                subquery,
            } => {
                assert_eq!(column, "price");
                assert_eq!(operator, ">");
                assert_eq!(quantifier, Some(SubqueryQuantifier::All));
                assert_eq!(subquery.sql, "SELECT amount FROM orders");
            }
            _ => panic!("Expected ALL comparison subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_scalar_subquery() {
        let executor = create_test_executor();

        let condition = "amount > (SELECT AVG(amount) FROM orders)";
        let subquery_expr = executor.try_parse_subquery_condition(condition).unwrap();

        assert!(subquery_expr.is_some());
        match subquery_expr.unwrap() {
            SubqueryExpression::Comparison {
                column,
                operator,
                quantifier,
                subquery,
            } => {
                assert_eq!(column, "amount");
                assert_eq!(operator, ">");
                assert_eq!(quantifier, None); // No quantifier for scalar subquery
                assert_eq!(subquery.sql, "SELECT AVG(amount) FROM orders");
            }
            _ => panic!("Expected scalar comparison subquery expression"),
        }
    }

    #[tokio::test]
    async fn test_parse_derived_table() {
        let executor = create_test_executor();

        let from_part = "(SELECT * FROM users WHERE status = 'active') AS active_users";
        let derived_table = executor.parse_derived_table(from_part).unwrap();

        assert!(derived_table.is_some());
        let dt = derived_table.unwrap();
        assert_eq!(dt.alias, "active_users");
        assert_eq!(
            dt.subquery.sql,
            "SELECT * FROM users WHERE status = 'active'"
        );
    }

    #[tokio::test]
    async fn test_extract_parenthesized_subquery() {
        let executor = create_test_executor();

        let text = "(SELECT user_id FROM orders WHERE status = 'completed')";
        let extracted = executor.extract_parenthesized_subquery(text).unwrap();

        assert_eq!(
            extracted,
            "SELECT user_id FROM orders WHERE status = 'completed'"
        );
    }

    #[tokio::test]
    async fn test_nested_parentheses() {
        let executor = create_test_executor();

        let text = "(SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders))";
        let extracted = executor.extract_parenthesized_subquery(text).unwrap();

        assert_eq!(
            extracted,
            "SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)"
        );
    }

    #[tokio::test]
    async fn test_enhanced_where_clause_parsing() {
        let executor = create_test_executor();

        let where_clause = "status = 'active' AND id IN (SELECT user_id FROM orders)";
        let conditions = executor.parse_enhanced_where_clause(where_clause).unwrap();

        assert_eq!(conditions.len(), 2);

        // First condition should be simple
        match &conditions[0] {
            WhereCondition::Simple {
                column,
                operator,
                value,
            } => {
                assert_eq!(column, "status");
                assert_eq!(operator, "=");
                assert_eq!(value, &serde_json::Value::String("active".to_string()));
            }
            _ => panic!("Expected simple condition"),
        }

        // Second condition should be subquery
        match &conditions[1] {
            WhereCondition::Subquery(SubqueryExpression::In {
                column,
                subquery,
                negated,
            }) => {
                assert_eq!(column, "id");
                assert_eq!(subquery.sql, "SELECT user_id FROM orders");
                assert!(!negated);
            }
            _ => panic!("Expected IN subquery condition"),
        }
    }
}

#[cfg(test)]
mod cte_tests {
    use crate::executor::QueryExecutor;
    use driftdb_core::Engine;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_executor() -> QueryExecutor<'static> {
        let temp_dir = Box::leak(Box::new(TempDir::new().unwrap()));
        let engine = Arc::new(RwLock::new(Engine::init(temp_dir.path()).unwrap()));
        QueryExecutor::new(engine)
    }

    /// Seed a table with data for testing.
    async fn seed_table(executor: &QueryExecutor<'_>, create_sql: &str, inserts: &[&str]) {
        executor.execute(create_sql).await.expect("CREATE TABLE");
        for sql in inserts {
            executor.execute(sql).await.expect("INSERT");
        }
    }

    // ── CASE WHEN ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_case_when_in_select() {
        let executor = create_test_executor();
        seed_table(
            &executor,
            "CREATE TABLE users (id TEXT PRIMARY KEY, status TEXT)",
            &[
                "INSERT INTO users (id, status) VALUES ('1', 'active')",
                "INSERT INTO users (id, status) VALUES ('2', 'inactive')",
            ],
        )
        .await;

        let result = executor
            .execute("SELECT id, CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END AS is_active FROM users ORDER BY id")
            .await
            .expect("SELECT with CASE WHEN");

        match result {
            crate::executor::QueryResult::Select { columns, rows } => {
                assert!(
                    columns.iter().any(|c| c == "is_active" || c == "_case_0"),
                    "expected CASE alias in columns, got {:?}",
                    columns
                );
                assert_eq!(rows.len(), 2);
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_case_when_simple_form() {
        let executor = create_test_executor();
        seed_table(
            &executor,
            "CREATE TABLE items (id TEXT PRIMARY KEY, score TEXT)",
            &[
                "INSERT INTO items (id, score) VALUES ('a', '1')",
                "INSERT INTO items (id, score) VALUES ('b', '2')",
                "INSERT INTO items (id, score) VALUES ('c', '3')",
            ],
        )
        .await;

        // Simple CASE expr WHEN val THEN result form
        let result = executor
            .execute(
                "SELECT id, CASE score WHEN '1' THEN 'low' WHEN '2' THEN 'mid' ELSE 'high' END AS tier FROM items ORDER BY id",
            )
            .await
            .expect("SELECT with simple CASE");

        match result {
            crate::executor::QueryResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    // ── CTEs ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_basic_cte() {
        let executor = create_test_executor();
        seed_table(
            &executor,
            "CREATE TABLE orders (id TEXT PRIMARY KEY, status TEXT)",
            &[
                "INSERT INTO orders (id, status) VALUES ('o1', 'shipped')",
                "INSERT INTO orders (id, status) VALUES ('o2', 'shipped')",
                "INSERT INTO orders (id, status) VALUES ('o3', 'pending')",
            ],
        )
        .await;

        // CTE filters shipped orders; outer query selects from the CTE result.
        let result = executor
            .execute(
                "WITH shipped AS (SELECT id FROM orders WHERE status = 'shipped') \
                 SELECT id FROM shipped",
            )
            .await
            .expect("WITH ... SELECT");

        match result {
            crate::executor::QueryResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2, "expected 2 rows with status = shipped");
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_cte_chained() {
        let executor = create_test_executor();
        seed_table(
            &executor,
            "CREATE TABLE products (id TEXT PRIMARY KEY, price TEXT, category TEXT)",
            &[
                "INSERT INTO products (id, price, category) VALUES ('p1', '10', 'A')",
                "INSERT INTO products (id, price, category) VALUES ('p2', '20', 'A')",
                "INSERT INTO products (id, price, category) VALUES ('p3', '30', 'B')",
            ],
        )
        .await;

        // Two CTEs chained — second references real table (first CTE name resolution
        // is the main thing we test here)
        let result = executor
            .execute(
                "WITH cat_a AS (SELECT id FROM products WHERE category = 'A'), \
                      cat_b AS (SELECT id FROM products WHERE category = 'B') \
                 SELECT id FROM cat_a",
            )
            .await
            .expect("chained CTEs");

        match result {
            crate::executor::QueryResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2, "expected 2 rows in cat_a");
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    // ── FOREIGN KEYS ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_fk_insert_valid_reference() {
        let executor = create_test_executor();
        // Parent table
        seed_table(
            &executor,
            "CREATE TABLE departments (id TEXT PRIMARY KEY, name TEXT)",
            &["INSERT INTO departments (id, name) VALUES ('dept1', 'Engineering')"],
        )
        .await;

        // Child table with FK
        executor
            .execute(
                "CREATE TABLE employees (id TEXT PRIMARY KEY, dept_id TEXT REFERENCES departments(id))",
            )
            .await
            .expect("CREATE TABLE with FK");

        // Valid insert — referenced row exists
        let result = executor
            .execute(
                "INSERT INTO employees (id, dept_id) VALUES ('emp1', 'dept1')",
            )
            .await;

        assert!(
            result.is_ok(),
            "expected valid FK insert to succeed, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_fk_insert_invalid_reference() {
        let executor = create_test_executor();
        seed_table(
            &executor,
            "CREATE TABLE depts2 (id TEXT PRIMARY KEY, name TEXT)",
            &["INSERT INTO depts2 (id, name) VALUES ('d1', 'Sales')"],
        )
        .await;

        executor
            .execute(
                "CREATE TABLE emps2 (id TEXT PRIMARY KEY, dept_id TEXT REFERENCES depts2(id))",
            )
            .await
            .expect("CREATE TABLE with FK");

        // Invalid insert — 'nonexistent' does not exist in depts2
        let result = executor
            .execute("INSERT INTO emps2 (id, dept_id) VALUES ('e1', 'nonexistent')")
            .await;

        assert!(
            result.is_err(),
            "expected FK violation to return an error, got Ok"
        );
    }

    // ── CORRELATED SUBQUERIES ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_correlated_exists_subquery() {
        let executor = create_test_executor();
        seed_table(
            &executor,
            "CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT)",
            &[
                "INSERT INTO customers (id, name) VALUES ('c1', 'Alice')",
                "INSERT INTO customers (id, name) VALUES ('c2', 'Bob')",
            ],
        )
        .await;
        seed_table(
            &executor,
            "CREATE TABLE purchases (id TEXT PRIMARY KEY, customer_id TEXT)",
            &["INSERT INTO purchases (id, customer_id) VALUES ('p1', 'c1')"],
        )
        .await;

        // Alice has a purchase, Bob does not
        let result = executor
            .execute(
                "SELECT name FROM customers u \
                 WHERE EXISTS (SELECT 1 FROM purchases p WHERE p.customer_id = u.id)",
            )
            .await
            .expect("correlated EXISTS");

        match result {
            crate::executor::QueryResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1, "expected only Alice (has a purchase)");
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }
}
