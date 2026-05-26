
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
