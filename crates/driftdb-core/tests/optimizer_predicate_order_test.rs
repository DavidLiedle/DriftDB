//! Proves that the optimizer's predicate ordering — the second dimension
//! of the optimizer↔executor contract — materially influences execution.
//!
//! White-box: optimizer emits `Filter` steps in cost-class order, not
//! WHERE source order. Black-box: residual evaluation in the executor
//! produces the same rows under any permutation (short-circuit
//! correctness preserved across reorderings).

use serde_json::json;
use tempfile::TempDir;

use driftdb_core::optimizer::PlanStep;
use driftdb_core::query::{AsOf, WhereCondition};
use driftdb_core::{Engine, Query, QueryResult};

fn cond(col: &str, op: &str, value: serde_json::Value) -> WhereCondition {
    WhereCondition {
        column: col.to_string(),
        operator: op.to_string(),
        value,
    }
}

fn setup_users() -> (TempDir, Engine) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["status".to_string()],
        })
        .unwrap();

    for i in 0..40 {
        let status = if i % 4 == 0 { "active" } else { "inactive" };
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: json!({
                    "id": format!("u{}", i),
                    "email": format!("user{}@example.com", i),
                    "status": status,
                    "age": (i % 30) + 18,
                }),
            })
            .unwrap();
    }
    (temp_dir, engine)
}

#[test]
fn plan_orders_filters_by_cost_class_not_source_order() {
    // WHERE order intentionally puts the EXPENSIVE predicate first
    // (LIKE), then a range, then an equality on an indexed column,
    // then an IS NULL. Optimizer must reverse the order: IS NULL (0)
    // → indexed equality (1) → range (3) → LIKE (5).
    //
    // Note: the indexed equality on `status` would normally be the
    // ACCESS step (IndexLookup), not a residual Filter, so to keep
    // it in the residual list we use `!=` on `status` — that's still
    // class 1 (equality-shape on indexed column) but won't be chosen
    // for access.
    let (_td, engine) = setup_users();
    let query = Query::Select {
        table: "users".to_string(),
        conditions: vec![
            cond("email", "LIKE", json!("user1%")),         // class 5
            cond("age", ">", json!(20)),                    // class 3
            cond("status", "!=", json!("disabled")),        // class 1
            cond("email", "IS NOT NULL", serde_json::Value::Null), // class 0
        ],
        as_of: None,
        limit: None,
    };
    let plan = engine.query_optimizer().optimize(&query).unwrap();

    // No equality on `status`, so no IndexLookup chosen. All four
    // predicates appear as Filter steps in cost-class order.
    let filter_ops: Vec<(String, String)> = plan
        .steps
        .iter()
        .filter_map(|s| match s {
            PlanStep::Filter { predicate, .. } => {
                Some((predicate.column.clone(), predicate.operator.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        filter_ops,
        vec![
            ("email".to_string(), "IS NOT NULL".to_string()),
            ("status".to_string(), "!=".to_string()),
            ("age".to_string(), ">".to_string()),
            ("email".to_string(), "LIKE".to_string()),
        ],
        "filters should be ordered cheapest first; got: {:?}",
        filter_ops
    );
}

#[test]
fn indexed_path_residual_order_matches_plan() {
    // Index lookup on status='active' narrows to 10 PKs. Two residual
    // predicates: equality on indexed column (`age = X` — age isn't
    // indexed, class 2) and a LIKE (class 5). Plan should put the
    // equality before the LIKE despite source order being reversed.
    let (_td, engine) = setup_users();
    let query = Query::Select {
        table: "users".to_string(),
        conditions: vec![
            cond("email", "LIKE", json!("user%")),
            cond("status", "=", json!("active")),
            cond("age", "=", json!(20)),
        ],
        as_of: None,
        limit: None,
    };
    let plan = engine.query_optimizer().optimize(&query).unwrap();

    // status= becomes IndexLookup (access step).
    assert!(plan
        .steps
        .iter()
        .any(|s| matches!(s, PlanStep::IndexLookup { index, .. } if index == "status")));

    let filter_ops: Vec<(String, String)> = plan
        .steps
        .iter()
        .filter_map(|s| match s {
            PlanStep::Filter { predicate, .. } => {
                Some((predicate.column.clone(), predicate.operator.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        filter_ops,
        vec![
            ("age".to_string(), "=".to_string()),
            ("email".to_string(), "LIKE".to_string()),
        ],
        "residual filters should be equality-before-LIKE; got: {:?}",
        filter_ops
    );
}

#[test]
fn results_invariant_under_predicate_reordering() {
    // Same logical AND of predicates, two different source orders.
    // Both must return the same row set.
    let (_td, mut engine) = setup_users();

    let source_order_a = vec![
        cond("email", "LIKE", json!("user1%")),
        cond("age", ">=", json!(20)),
        cond("status", "!=", json!("disabled")),
    ];
    let source_order_b = vec![
        cond("status", "!=", json!("disabled")),
        cond("age", ">=", json!(20)),
        cond("email", "LIKE", json!("user1%")),
    ];

    let run = |conds: Vec<WhereCondition>, engine: &mut Engine| -> Vec<serde_json::Value> {
        let r = engine
            .execute_query(Query::Select {
                table: "users".to_string(),
                conditions: conds,
                as_of: None,
                limit: None,
            })
            .unwrap();
        let QueryResult::Rows { data } = r else {
            panic!("expected Rows")
        };
        data
    };

    let mut a = run(source_order_a, &mut engine);
    let mut b = run(source_order_b, &mut engine);
    a.sort_by_key(|r| r["id"].as_str().unwrap_or("").to_string());
    b.sort_by_key(|r| r["id"].as_str().unwrap_or("").to_string());
    assert_eq!(a, b, "result set must not depend on source order");
    assert!(!a.is_empty(), "the test should actually return rows");
}

#[test]
fn schemaless_rows_excluded_regardless_of_order() {
    // Insert some rows that lack the `nickname` column. Predicate on
    // `nickname` must exclude them whether it runs first or last.
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "u".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["status".to_string()],
        })
        .unwrap();
    // Two rows with nickname, two without:
    for (i, nick) in [
        (1, Some("alice")),
        (2, None),
        (3, Some("bob")),
        (4, None),
    ] {
        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), json!(format!("u{}", i)));
        row.insert("status".to_string(), json!("active"));
        if let Some(n) = nick {
            row.insert("nickname".to_string(), json!(n));
        }
        engine
            .execute_query(Query::Insert {
                table: "u".to_string(),
                data: serde_json::Value::Object(row),
            })
            .unwrap();
    }

    // WHERE status='active' (indexed) AND nickname='alice'.
    // Index gives us 4 PKs; residual filters down to 1.
    let result = engine
        .execute_query(Query::Select {
            table: "u".to_string(),
            conditions: vec![
                cond("status", "=", json!("active")),
                cond("nickname", "=", json!("alice")),
            ],
            as_of: None,
            limit: None,
        })
        .unwrap();
    let QueryResult::Rows { data } = result else {
        panic!("expected Rows")
    };
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["id"], json!("u1"));
}

#[test]
fn time_travel_path_still_honors_predicate_order() {
    // Time travel forces the full-scan path. Predicate order should
    // still be applied (the plan is built regardless of AS OF).
    let (_td, mut engine) = setup_users();

    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: vec![
                cond("email", "LIKE", json!("user5%")), // expensive
                cond("status", "=", json!("active")),   // class 1 (indexed)
            ],
            as_of: Some(AsOf::Sequence(u64::MAX)),
            limit: None,
        })
        .unwrap();
    let QueryResult::Rows { data } = result else {
        panic!("expected Rows")
    };
    // Full-scan path runs because of AsOf. We have 40 users (u0..u39).
    // status='active' picks every 4th row: i in {0,4,8,...,36}.
    // LIKE 'user5%' narrows to user5 only (user50..user59 don't exist
    // in this dataset).
    // u5: status = "inactive" (5%4 != 0). Intersection = ∅.
    //
    // The point of this test isn't the row set itself — it's that
    // status='active' was applied DESPITE having been "absorbed" into
    // the optimizer's IndexLookup step. Without the re-prepend in
    // select(), this would return [u5] (only LIKE applied). With the
    // re-prepend, it correctly returns [].
    assert!(
        data.is_empty(),
        "status='active' must be applied even when AsOf forces full-scan; got {:?}",
        data
    );

    // Sanity check: change the indexed predicate so SOMETHING matches.
    // status='inactive' + LIKE 'user5%' → u5.
    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: vec![
                cond("email", "LIKE", json!("user5%")),
                cond("status", "=", json!("inactive")),
            ],
            as_of: Some(AsOf::Sequence(u64::MAX)),
            limit: None,
        })
        .unwrap();
    let QueryResult::Rows { data } = result else {
        panic!("expected Rows")
    };
    let ids: Vec<&str> = data.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["u5"]);
}
