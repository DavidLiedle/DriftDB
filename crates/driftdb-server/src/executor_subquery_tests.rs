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
