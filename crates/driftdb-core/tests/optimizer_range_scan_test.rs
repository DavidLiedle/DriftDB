//! Third slice of the optimizer-to-executor wiring arc: range IndexScan
//! execution. Proves that for single-table SELECT with WHERE, range
//! predicates on an indexed column drive an `IndexScan` access step
//! (with proper bounds + inclusivity) and the executor honors it.

use serde_json::json;
use tempfile::TempDir;

use driftdb_core::optimizer::{PlanStep, RangeBound};
use driftdb_core::query::{AsOf, WhereCondition};
use driftdb_core::{Engine, Query, QueryResult};

fn cond(col: &str, op: &str, value: serde_json::Value) -> WhereCondition {
    WhereCondition {
        column: col.to_string(),
        operator: op.to_string(),
        value,
    }
}

/// 60 users with numeric `age` indexed. Age values: 18..78 (one per user).
fn setup_users_age_indexed() -> (TempDir, Engine) {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["age".to_string()],
        })
        .unwrap();
    for i in 0..60 {
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: json!({
                    "id": format!("u{}", i),
                    "name": format!("user{}", i),
                    "age": (18 + i),
                    "status": if i % 3 == 0 { "active" } else { "inactive" },
                }),
            })
            .unwrap();
    }
    (temp_dir, engine)
}

fn select_rows(engine: &mut Engine, conds: Vec<WhereCondition>) -> Vec<serde_json::Value> {
    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: conds,
            as_of: None,
            limit: None,
        })
        .unwrap();
    let QueryResult::Rows { data } = result else {
        panic!("expected Rows")
    };
    data
}

fn extract_index_scan(plan: &driftdb_core::optimizer::QueryPlan) -> Option<(Option<RangeBound>, Option<RangeBound>)> {
    plan.steps.iter().find_map(|s| match s {
        PlanStep::IndexScan { start, end, .. } => Some((start.clone(), end.clone())),
        _ => None,
    })
}

#[test]
fn plan_emits_index_scan_for_two_sided_range() {
    let (_td, engine) = setup_users_age_indexed();
    let plan = engine
        .query_optimizer()
        .optimize(&Query::Select {
            table: "users".to_string(),
            conditions: vec![cond("age", ">", json!(30)), cond("age", "<", json!(50))],
            as_of: None,
            limit: None,
        })
        .unwrap();

    assert!(plan.uses_index, "two-sided range should use index");
    let (start, end) = extract_index_scan(&plan).expect("expected IndexScan step");
    let start = start.expect("expected lower bound");
    let end = end.expect("expected upper bound");
    assert_eq!(start.value, json!(30));
    assert!(!start.inclusive, "> 30 is exclusive");
    assert_eq!(end.value, json!(50));
    assert!(!end.inclusive, "< 50 is exclusive");
}

#[test]
fn plan_emits_index_scan_for_single_sided_range() {
    let (_td, engine) = setup_users_age_indexed();
    let plan = engine
        .query_optimizer()
        .optimize(&Query::Select {
            table: "users".to_string(),
            conditions: vec![cond("age", ">=", json!(40))],
            as_of: None,
            limit: None,
        })
        .unwrap();
    assert!(plan.uses_index);
    let (start, end) = extract_index_scan(&plan).expect("expected IndexScan");
    let start = start.expect("expected lower bound");
    assert!(end.is_none(), "no upper bound expected");
    assert_eq!(start.value, json!(40));
    assert!(start.inclusive, ">= 40 is inclusive");
}

#[test]
fn range_returns_rows_within_bounds_exclusive() {
    // age > 30 AND age < 50  → ages 31..49 inclusive of endpoints?  No,
    // both exclusive. Users with age 31..49.
    let (_td, mut engine) = setup_users_age_indexed();
    let rows = select_rows(
        &mut engine,
        vec![cond("age", ">", json!(30)), cond("age", "<", json!(50))],
    );
    let mut ages: Vec<i64> = rows
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .collect();
    ages.sort();
    let expected: Vec<i64> = (31..=49).collect();
    assert_eq!(ages, expected);
}

#[test]
fn range_returns_rows_within_bounds_inclusive() {
    // age >= 30 AND age <= 50  → ages 30..50 inclusive.
    let (_td, mut engine) = setup_users_age_indexed();
    let rows = select_rows(
        &mut engine,
        vec![cond("age", ">=", json!(30)), cond("age", "<=", json!(50))],
    );
    let mut ages: Vec<i64> = rows
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .collect();
    ages.sort();
    let expected: Vec<i64> = (30..=50).collect();
    assert_eq!(ages, expected);
}

#[test]
fn range_single_sided_lower_bound() {
    // age >= 70 → users with age 70..77 (only 60 users, ages 18..77).
    let (_td, mut engine) = setup_users_age_indexed();
    let rows = select_rows(&mut engine, vec![cond("age", ">=", json!(70))]);
    let mut ages: Vec<i64> = rows
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .collect();
    ages.sort();
    assert_eq!(ages, vec![70, 71, 72, 73, 74, 75, 76, 77]);
}

#[test]
fn range_single_sided_upper_bound() {
    // age < 20 → ages 18, 19.
    let (_td, mut engine) = setup_users_age_indexed();
    let rows = select_rows(&mut engine, vec![cond("age", "<", json!(20))]);
    let mut ages: Vec<i64> = rows
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .collect();
    ages.sort();
    assert_eq!(ages, vec![18, 19]);
}

#[test]
fn empty_range_returns_no_rows_no_panic() {
    // age > 100 AND age < 50  — empty range. Must not panic.
    let (_td, mut engine) = setup_users_age_indexed();
    let rows = select_rows(
        &mut engine,
        vec![cond("age", ">", json!(100)), cond("age", "<", json!(50))],
    );
    assert!(rows.is_empty());
}

#[test]
fn range_plus_residual_predicate_filters_both() {
    // age > 30 AND age < 50 AND status='active'.
    // Index drives the age range, residual filters status.
    // active = i % 3 == 0; ages 18+i. Index gives ages 31..49,
    // residual keeps only those where (age-18) % 3 == 0 → ages
    // 33, 36, 39, 42, 45, 48.
    let (_td, mut engine) = setup_users_age_indexed();
    let rows = select_rows(
        &mut engine,
        vec![
            cond("age", ">", json!(30)),
            cond("age", "<", json!(50)),
            cond("status", "=", json!("active")),
        ],
    );
    let mut ages: Vec<i64> = rows
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .collect();
    ages.sort();
    assert_eq!(ages, vec![33, 36, 39, 42, 45, 48]);
}

#[test]
fn boundary_rows_respected_exclusive_vs_inclusive() {
    // age = 30 exists (i=12 → age 30). > 30 excludes it; >= 30 includes it.
    let (_td, mut engine) = setup_users_age_indexed();

    let strict = select_rows(&mut engine, vec![cond("age", ">", json!(30))]);
    let strict_min = strict
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .min()
        .unwrap();
    assert_eq!(strict_min, 31);

    let inclusive = select_rows(&mut engine, vec![cond("age", ">=", json!(30))]);
    let inclusive_min = inclusive
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .min()
        .unwrap();
    assert_eq!(inclusive_min, 30);
}

#[test]
fn time_travel_range_falls_back_correctly() {
    // AS OF forces full-scan; the range predicates that were "absorbed"
    // into the IndexScan must be re-applied as filters.
    let (_td, mut engine) = setup_users_age_indexed();
    let result = engine
        .execute_query(Query::Select {
            table: "users".to_string(),
            conditions: vec![cond("age", ">", json!(30)), cond("age", "<", json!(35))],
            as_of: Some(AsOf::Sequence(u64::MAX)),
            limit: None,
        })
        .unwrap();
    let QueryResult::Rows { data } = result else {
        panic!("expected Rows")
    };
    let mut ages: Vec<i64> = data
        .iter()
        .map(|r| r["age"].as_i64().unwrap())
        .collect();
    ages.sort();
    assert_eq!(ages, vec![31, 32, 33, 34]);
}

#[test]
fn tighter_bound_wins_when_two_predicates_same_side() {
    // age > 25 AND age > 40  → tightens to age > 40.
    let (_td, engine) = setup_users_age_indexed();
    let plan = engine
        .query_optimizer()
        .optimize(&Query::Select {
            table: "users".to_string(),
            conditions: vec![cond("age", ">", json!(25)), cond("age", ">", json!(40))],
            as_of: None,
            limit: None,
        })
        .unwrap();
    let (start, _) = extract_index_scan(&plan).expect("IndexScan expected");
    let start = start.expect("lower bound expected");
    assert_eq!(start.value, json!(40), "expected tightening to 40");
    assert!(!start.inclusive);
}

#[test]
fn equal_value_exclusive_beats_inclusive_when_tightening() {
    // age >= 30 AND age > 30  → both same value, exclusive is stricter.
    let (_td, engine) = setup_users_age_indexed();
    let plan = engine
        .query_optimizer()
        .optimize(&Query::Select {
            table: "users".to_string(),
            conditions: vec![cond("age", ">=", json!(30)), cond("age", ">", json!(30))],
            as_of: None,
            limit: None,
        })
        .unwrap();
    let (start, _) = extract_index_scan(&plan).expect("IndexScan expected");
    let start = start.expect("lower bound expected");
    assert_eq!(start.value, json!(30));
    assert!(!start.inclusive, "exclusive wins over inclusive at same value");
}
