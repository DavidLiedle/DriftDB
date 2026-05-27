//! Fourth slice of the optimizer↔executor wiring arc: join algorithm
//! selection for single INNER JOIN. White-box: assert PlanNode variant.
//! Black-box: assert correctness across algorithms and composition
//! with slices 1–3 (per-side index selection, predicate order, range
//! scans).

use serde_json::json;
use tempfile::TempDir;

use driftdb_core::optimizer::{JoinSide, PlanNode};
use driftdb_core::sql_bridge::execute_sql;
use driftdb_core::{Engine, Query, QueryResult};

fn rows(engine: &mut Engine, sql: &str) -> Vec<serde_json::Value> {
    match execute_sql(engine, sql).unwrap() {
        QueryResult::Rows { data } => data,
        other => panic!("expected Rows, got {:?}", other),
    }
}

/// Two tables: users (id, name, age, dept_id), depts (id, name).
/// `users.dept_id` is indexed → composes with slice 1.
/// `users.age` is indexed → composes with slice 3 (range).
fn setup_users_depts() -> (TempDir, Engine) {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();

    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["dept_id".to_string(), "age".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "depts".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    // Use `dept_name` rather than `name` so the post-join row doesn't
    // collide with users.name — see the latent CompoundIdentifier-
    // resolution bug noted at the test cases below.
    for (id, dept_name) in &[("d1", "Engineering"), ("d2", "Sales"), ("d3", "Ops")] {
        engine
            .execute_query(Query::Insert {
                table: "depts".to_string(),
                data: json!({ "id": id, "dept_name": dept_name }),
            })
            .unwrap();
    }
    // 30 users distributed across 3 departments, ages 20..49.
    for i in 0..30 {
        let dept = match i % 3 {
            0 => "d1",
            1 => "d2",
            _ => "d3",
        };
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: json!({
                    "id": format!("u{}", i),
                    "name": format!("user{}", i),
                    "age": 20 + i,
                    "dept_id": dept,
                }),
            })
            .unwrap();
    }
    (temp, engine)
}

#[test]
fn optimizer_emits_plan_node_for_join() {
    // White-box: ask the optimizer for a join plan and assert the
    // returned PlanNode is one of the join variants with TableScan
    // children carrying the right table names.
    let (_t, engine) = setup_users_depts();
    let node = engine
        .query_optimizer()
        .plan_single_join("users", "depts", "dept_id", "id", driftdb_core::optimizer::JoinType::Inner);
    match node {
        PlanNode::NestedLoopJoin { left, right, condition, .. }
        | PlanNode::HashJoin { left, right, condition, .. } => {
            // SQL order preserved: left = users, right = depts.
            match *left {
                PlanNode::TableScan { table, .. } => assert_eq!(table, "users"),
                other => panic!("expected TableScan(users), got {:?}", other),
            }
            match *right {
                PlanNode::TableScan { table, .. } => assert_eq!(table, "depts"),
                other => panic!("expected TableScan(depts), got {:?}", other),
            }
            assert_eq!(condition.left_col, "dept_id");
            assert_eq!(condition.right_col, "id");
        }
        other => panic!("expected join node, got {:?}", other),
    }
}

#[test]
fn nested_loop_chosen_when_inner_side_indexed() {
    // Heuristic rule 1: indexed inner-side join column → NestedLoop.
    // Here `depts` (right) is indexed on its PK `id`? Actually PKs
    // aren't auto-registered as separate indexes. Set up so that the
    // RIGHT table has the column indexed and the left doesn't.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "outer_t".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "inner_t".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["key".to_string()],
        })
        .unwrap();
    let node = engine
        .query_optimizer()
        .plan_single_join("outer_t", "inner_t", "key", "key", driftdb_core::optimizer::JoinType::Inner);
    assert!(
        matches!(node, PlanNode::NestedLoopJoin { .. }),
        "indexed inner-side join column should pick NestedLoop"
    );
}

#[test]
fn hash_chosen_when_both_sides_large_and_unindexed() {
    // Heuristic rule 3: row count > NL_THRESHOLD on either side →
    // Hash. After `register_table_indexes` runs, the row-count hint
    // is 10_000, which is well above the 1000 threshold — but
    // index registration only fires if the table has indexed columns.
    // Tables without indexed columns aren't registered at all, so
    // row_count defaults to 0. To get the Hash path deterministically,
    // we need to seed via Engine::update_statistics or use indexed
    // columns. Use indexed columns that AREN'T the join keys.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "big_a".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other_a".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "big_b".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other_b".to_string()],
        })
        .unwrap();
    // join_key is NOT indexed on either side.
    let node = engine
        .query_optimizer()
        .plan_single_join("big_a", "big_b", "join_key", "join_key", driftdb_core::optimizer::JoinType::Inner);
    assert!(
        matches!(node, PlanNode::HashJoin { .. }),
        "large unindexed-join-key tables should pick Hash; got {:?}",
        node
    );
}

#[test]
fn inner_join_correctness_via_sql() {
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.name, d.name FROM users u JOIN depts d ON u.dept_id = d.id",
    );
    assert_eq!(rs.len(), 30, "30 users × 1 dept each = 30 joined rows");
    // Every row has both a u.name (or `name`) and a t1_name (collision).
    for row in &rs {
        assert!(row.get("name").is_some(), "row missing name: {:?}", row);
    }
}

#[test]
fn composition_with_slice_1_index_selection() {
    // Push an equality on the indexed `dept_id` column down to one
    // side. Slice-1 wiring should activate at that leaf, even though
    // we're in a join. Set-equality is the right check here (order
    // doesn't matter once we're confirming index-driven row selection).
    use std::collections::HashSet;
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u JOIN depts d ON u.dept_id = d.id WHERE u.dept_id = 'd2'",
    );
    let ids: HashSet<String> = rs
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect();
    // dept_id='d2' → i%3==1 → users 1,4,7,...,28. 10 rows.
    let expected: HashSet<String> = (0..30)
        .filter(|i| i % 3 == 1)
        .map(|i| format!("u{}", i))
        .collect();
    assert_eq!(ids, expected);
}

#[test]
fn composition_with_slice_3_range_scan() {
    // Push a range predicate on indexed `age` down to the users side.
    // Slice-3 wiring should activate there. Project u.age so we can
    // assert the actual range membership rather than just row count.
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.age FROM users u JOIN depts d ON u.dept_id = d.id WHERE u.age >= 40 AND u.age < 45",
    );
    let mut ages: Vec<i64> = rs.iter().map(|r| r["age"].as_i64().unwrap()).collect();
    ages.sort();
    // Ages 40,41,42,43,44 — five rows (one dept match each).
    assert_eq!(ages, vec![40, 41, 42, 43, 44]);
}

#[test]
fn duplicate_join_keys_on_inner_no_duplicates_or_drops() {
    // Build a setup where the right side has duplicate join keys.
    // For an inner-join, EACH left row should produce one output per
    // matching right row — so duplicates multiply correctly.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "left_t".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "right_t".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    // left has one row with k=1.
    engine
        .execute_query(Query::Insert {
            table: "left_t".to_string(),
            data: json!({"id": "L1", "k": 1, "name": "left-one"}),
        })
        .unwrap();
    // right has THREE rows with k=1.
    for n in &["A", "B", "C"] {
        engine
            .execute_query(Query::Insert {
                table: "right_t".to_string(),
                data: json!({"id": format!("R{}", n), "k": 1, "tag": n}),
            })
            .unwrap();
    }
    let rs = rows(
        &mut engine,
        "SELECT * FROM left_t l JOIN right_t r ON l.k = r.k",
    );
    assert_eq!(rs.len(), 3, "1 × 3 duplicates = 3 output rows");
}

#[test]
fn empty_side_returns_no_rows_no_panic() {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "non_empty".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "empty_t".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    for i in 0..5 {
        engine
            .execute_query(Query::Insert {
                table: "non_empty".to_string(),
                data: json!({"id": format!("n{}", i), "k": i}),
            })
            .unwrap();
    }
    let rs = rows(
        &mut engine,
        "SELECT * FROM non_empty a JOIN empty_t b ON a.k = b.k",
    );
    assert!(rs.is_empty());
}

#[test]
fn hash_join_with_string_keys() {
    // The hash join's key encoding uses JSON repr with a type tag.
    // Strings ("d1") and numbers (1) must NOT collide as hash keys.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "ord".to_string(),
            primary_key: "id".to_string(),
            // dept_id not indexed → ensures Hash path under default heuristic.
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "dpt".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "dpt".to_string(),
            data: json!({"id": "1", "other": "x", "name": "string-id-dept"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "ord".to_string(),
            // numeric dept_id = 1, NOT the string "1".
            data: json!({"id": "o1", "other": "x", "dept_id": 1}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT * FROM ord o JOIN dpt d ON o.dept_id = d.id",
    );
    // String "1" must NOT match number 1 — they're different types.
    assert!(
        rs.is_empty(),
        "type-distinct hash keys must not collide; got {:?}",
        rs
    );
}

#[test]
fn predicate_pushdown_to_both_sides() {
    // Push predicates on BOTH sides, with no column-name collision
    // (depts uses `dept_name`, users uses `name`). Each side's
    // Engine::select call gets its own conditions list; the join
    // operates on the narrowed rows.
    //
    // Note: post-join `filter_rows` re-applies the full WHERE for
    // correctness. There's a latent bug where post-join
    // `CompoundIdentifier` resolution doesn't follow
    // perform_inner_join's t1_*/t2_* collision-prefix scheme — i.e.
    // `d.colliding_name` would find the LEFT row's `colliding_name`
    // after merge. Using `dept_name` sidesteps that. The structural
    // fix is bigger scope and queued for a follow-on slice.
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u JOIN depts d ON u.dept_id = d.id \
         WHERE u.dept_id = 'd1' AND d.dept_name = 'Engineering'",
    );
    // d1 = Engineering; users with dept_id='d1' = i%3==0 → 10 rows.
    assert_eq!(rs.len(), 10);
}

#[test]
fn build_side_advisory_does_not_break_output_shape() {
    // When the optimizer picks Hash with build_side=Left, the join
    // output should still place SQL-left columns first. Verify by
    // looking at the column shape of the merged row.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    // small left, large right → likely build_side=Left.
    engine
        .execute_query(Query::CreateTable {
            name: "small_left".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()], // gets row_count_hint=10000 too
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "large_right".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    // Category-B test scaffolding (slice 9): forcing a HashJoin
    // build-side decision requires row counts > NL_THRESHOLD (1000)
    // on one side. Inserting 1000+ real rows per table just to
    // trigger the algorithmic threshold is prohibitively slow for a
    // unit test. The `register_table_indexes` row_count_hint is
    // the intended pattern for forcing specific planner decisions
    // without the cost of populating real data — kept here
    // deliberately. ANALYZE is exercised end-to-end by
    // `tests/optimizer_analyze_test.rs` and category-A tests.
    engine
        .query_optimizer()
        .register_table_indexes("small_left", &["other".to_string()], 10);
    engine
        .query_optimizer()
        .register_table_indexes("large_right", &["other".to_string()], 100_000);
    let node = engine
        .query_optimizer()
        .plan_single_join("small_left", "large_right", "k", "k", driftdb_core::optimizer::JoinType::Inner);
    match node {
        PlanNode::HashJoin { build_side, .. } => {
            assert_eq!(build_side, JoinSide::Left, "smaller side should build");
        }
        other => panic!("expected HashJoin, got {:?}", other),
    }

    // Now insert one row each and verify the join still works correctly.
    engine
        .execute_query(Query::Insert {
            table: "small_left".to_string(),
            data: json!({"id": "L1", "k": 7, "left_col": "from-left"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "large_right".to_string(),
            data: json!({"id": "R1", "k": 7, "right_col": "from-right"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT * FROM small_left l JOIN large_right r ON l.k = r.k",
    );
    assert_eq!(rs.len(), 1);
    // SQL-left columns must come from small_left.
    let row = &rs[0];
    assert_eq!(row.get("left_col").unwrap(), &json!("from-left"));
    assert!(row.get("right_col").is_some());
}
