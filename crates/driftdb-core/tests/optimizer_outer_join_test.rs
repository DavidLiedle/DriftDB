//! Slice 5: OUTER JOIN handling under the optimizer. Second join shape
//! after INNER (slice 4); first slice where row shape changes via
//! null-padding for unmatched rows.

use serde_json::json;
use tempfile::TempDir;

use driftdb_core::optimizer::{JoinType, PlanNode};
use driftdb_core::sql_bridge::execute_sql;
use driftdb_core::{Engine, Query, QueryResult};

fn rows(engine: &mut Engine, sql: &str) -> Vec<serde_json::Value> {
    match execute_sql(engine, sql).unwrap() {
        QueryResult::Rows { data } => data,
        other => panic!("expected Rows, got {:?}", other),
    }
}

/// Two tables. users has rows that won't join (u_lonely without a
/// dept), and depts has rows nothing joins to (d_empty department).
fn setup_users_depts() -> (TempDir, Engine) {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["dept_id".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "depts".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();

    for (id, name) in &[("d1", "Engineering"), ("d2", "Sales"), ("d_empty", "Empty")] {
        engine
            .execute_query(Query::Insert {
                table: "depts".to_string(),
                data: json!({"id": id, "dept_name": name}),
            })
            .unwrap();
    }
    for (id, name, dept) in &[
        ("u1", "Alice", Some("d1")),
        ("u2", "Bob", Some("d1")),
        ("u3", "Carol", Some("d2")),
        ("u_lonely", "Dave", None),
    ] {
        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), json!(id));
        row.insert("name".to_string(), json!(name));
        if let Some(d) = dept {
            row.insert("dept_id".to_string(), json!(d));
        }
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: serde_json::Value::Object(row),
            })
            .unwrap();
    }
    (temp, engine)
}

// ─── plan-shape (white-box) ─────────────────────────────────────────

#[test]
fn plan_carries_left_outer_join_type() {
    let (_t, engine) = setup_users_depts();
    let node = engine.query_optimizer().plan_single_join(
        "users",
        "depts",
        "dept_id",
        "id",
        JoinType::LeftOuter,
    );
    let jt = match &node {
        PlanNode::NestedLoopJoin { join_type, .. } | PlanNode::HashJoin { join_type, .. } => {
            *join_type
        }
        other => panic!("expected join node, got {:?}", other),
    };
    assert_eq!(jt, JoinType::LeftOuter);
}

#[test]
fn outer_join_hash_forces_build_side_right() {
    // Even though slice-4's tiebreaker would pick `Right` here anyway,
    // explicitly verify that the OUTER constraint is honored. Test
    // configuration: both sides large-and-unindexed (Hash path), so
    // build_side defaults to smaller; we set left smaller to force
    // build_side=Left under INNER, then re-plan as LEFT OUTER and
    // assert it flips to Right.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "a".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "b".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    // Category-B test scaffolding (slice 9): forces a Hash path by
    // crossing NL_THRESHOLD on the right side without inserting
    // 100_000 rows. The test asserts an algorithmic choice
    // (build_side override for OUTER), not a stats-path outcome —
    // inserting real data here would only multiply test runtime.
    engine
        .query_optimizer()
        .register_table_indexes("a", &["other".to_string()], 10);
    engine
        .query_optimizer()
        .register_table_indexes("b", &["other".to_string()], 100_000);

    let inner = engine
        .query_optimizer()
        .plan_single_join("a", "b", "k", "k", JoinType::Inner);
    match inner {
        PlanNode::HashJoin { build_side, .. } => {
            // INNER picks the smaller side (left in this setup).
            assert_eq!(build_side, driftdb_core::optimizer::JoinSide::Left);
        }
        other => panic!("expected HashJoin, got {:?}", other),
    }

    let left_outer = engine
        .query_optimizer()
        .plan_single_join("a", "b", "k", "k", JoinType::LeftOuter);
    match left_outer {
        PlanNode::HashJoin { build_side, .. } => {
            // LEFT OUTER must build on Right regardless of sizes.
            assert_eq!(build_side, driftdb_core::optimizer::JoinSide::Right);
        }
        other => panic!("expected HashJoin, got {:?}", other),
    }

    let full = engine
        .query_optimizer()
        .plan_single_join("a", "b", "k", "k", JoinType::FullOuter);
    match full {
        PlanNode::HashJoin { build_side, .. } => {
            assert_eq!(build_side, driftdb_core::optimizer::JoinSide::Right);
        }
        other => panic!("expected HashJoin, got {:?}", other),
    }
}

// ─── correctness: LEFT OUTER (NL path) ──────────────────────────────

#[test]
fn left_join_emits_unmatched_left_with_null_right() {
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.id, d.dept_name FROM users u LEFT JOIN depts d ON u.dept_id = d.id",
    );
    // 4 users; u_lonely has no dept → still appears with d.dept_name absent.
    let mut found: Vec<(String, Option<String>)> = rs
        .iter()
        .map(|r| {
            let id = r["id"].as_str().unwrap().to_string();
            let dn = r.get("dept_name").and_then(|v| v.as_str()).map(String::from);
            (id, dn)
        })
        .collect();
    found.sort();
    let mut expected = vec![
        ("u1".to_string(), Some("Engineering".to_string())),
        ("u2".to_string(), Some("Engineering".to_string())),
        ("u3".to_string(), Some("Sales".to_string())),
        ("u_lonely".to_string(), None),
    ];
    expected.sort();
    assert_eq!(found, expected);
}

#[test]
fn anti_join_pattern_finds_lonely_left_rows() {
    // The canonical "users with no orders" pattern: WHERE right_col IS NULL.
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u LEFT JOIN depts d ON u.dept_id = d.id \
         WHERE d.id IS NULL",
    );
    let ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["u_lonely"]);
}

// ─── correctness: LEFT OUTER + algorithm equivalence ────────────────

#[test]
fn left_join_nl_and_hash_produce_identical_results() {
    // Run the same LEFT JOIN query twice. First setup has both sides
    // small → NL. Second setup forces Hash via large row_count_hint.
    // Both must return the same row set.
    fn run(force_hash: bool) -> Vec<serde_json::Value> {
        let temp = TempDir::new().unwrap();
        let mut engine = Engine::init(temp.path()).unwrap();
        engine
            .execute_query(Query::CreateTable {
                name: "users".to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
        engine
            .execute_query(Query::CreateTable {
                name: "depts".to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
        if force_hash {
            // Category-B test scaffolding (slice 9): forces Hash
            // algorithm via NL_THRESHOLD crossing. The test
            // verifies that NL and Hash produce identical row
            // shapes; populating 100_000 real rows in a unit test
            // would be prohibitively slow.
            engine
                .query_optimizer()
                .register_table_indexes("users", &["dept_id".to_string()], 100_000);
            engine
                .query_optimizer()
                .register_table_indexes("depts", &["dept_name".to_string()], 100_000);
        }
        engine
            .execute_query(Query::Insert {
                table: "depts".to_string(),
                data: json!({"id": "d1", "dept_name": "Eng"}),
            })
            .unwrap();
        for (id, dept) in &[("u1", Some("d1")), ("u2", None::<&str>)] {
            let mut row = serde_json::Map::new();
            row.insert("id".to_string(), json!(id));
            if let Some(d) = dept {
                row.insert("dept_id".to_string(), json!(d));
            }
            engine
                .execute_query(Query::Insert {
                    table: "users".to_string(),
                    data: serde_json::Value::Object(row),
                })
                .unwrap();
        }
        rows(
            &mut engine,
            "SELECT u.id, d.dept_name FROM users u LEFT JOIN depts d ON u.dept_id = d.id",
        )
    }
    let mut nl = run(false);
    let mut hj = run(true);
    nl.sort_by_key(|r| r["id"].as_str().unwrap().to_string());
    hj.sort_by_key(|r| r["id"].as_str().unwrap().to_string());
    assert_eq!(nl, hj);
}

// ─── pushdown policy ────────────────────────────────────────────────

#[test]
fn pushdown_to_preserving_left_side_works() {
    // WHERE on the LEFT side of a LEFT OUTER can push down — the left
    // is preserving, so filtering it doesn't lose rows.
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u LEFT JOIN depts d ON u.dept_id = d.id \
         WHERE u.dept_id = 'd1'",
    );
    let mut ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec!["u1", "u2"]);
}

#[test]
fn no_pushdown_to_null_padded_right_side() {
    // WHERE on the RIGHT side of a LEFT OUTER must NOT push down to
    // the right's Engine::select call — that would filter out depts
    // that should appear as null-padded in unmatched left rows.
    // Correctness: the join still produces the right result whether
    // pushdown was applied or not (post-join WHERE catches it).
    //
    // This query says: every user, with their dept_name if the dept
    // name is "Engineering"; otherwise NULL right side. NOT the same
    // as filtering to depts with name="Engineering" — that would lose
    // u3 and u_lonely.
    //
    // Note: SQL semantics of `WHERE d.dept_name = 'Engineering'`
    // after LEFT JOIN actually does turn it into an INNER JOIN —
    // because NULL = anything is false. So u3 and u_lonely get
    // filtered. That's expected SQL behavior. What we're testing here
    // is that the result is the SAME regardless of pushdown — i.e.
    // we don't accidentally drop matching rows by over-pushing.
    let (_t, mut engine) = setup_users_depts();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u LEFT JOIN depts d ON u.dept_id = d.id \
         WHERE d.dept_name = 'Engineering'",
    );
    let mut ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort();
    // u1, u2 join to d1 (Engineering). u3 → d2 (Sales) filtered. u_lonely → no dept → filtered by NULL = X.
    assert_eq!(ids, vec!["u1", "u2"]);
}

// ─── FULL OUTER ─────────────────────────────────────────────────────

#[test]
fn full_outer_includes_unmatched_from_both_sides() {
    let (_t, mut engine) = setup_users_depts();
    // 3 matched (u1, u2, u3) + 1 unmatched left (u_lonely) + 1
    // unmatched right (d_empty) = 5 rows.
    let rs = rows(
        &mut engine,
        "SELECT * FROM users u FULL OUTER JOIN depts d ON u.dept_id = d.id",
    );
    assert_eq!(rs.len(), 5);

    // Check categories present.
    let user_ids: std::collections::HashSet<&str> = rs
        .iter()
        .filter_map(|r| r.get("id").and_then(|v| v.as_str()))
        .collect();
    // 'id' is the left's id (users.id) for matched + unmatched-left
    // rows; for unmatched-right rows it's depts.id (passed through
    // bare).
    assert!(user_ids.contains("u1"));
    assert!(user_ids.contains("u_lonely"));
    assert!(user_ids.contains("d_empty"));
}

#[test]
fn full_outer_hash_path_matches_nl_path() {
    fn run(force_hash: bool) -> usize {
        let temp = TempDir::new().unwrap();
        let mut engine = Engine::init(temp.path()).unwrap();
        engine
            .execute_query(Query::CreateTable {
                name: "users".to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
        engine
            .execute_query(Query::CreateTable {
                name: "depts".to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
        if force_hash {
            // Category-B test scaffolding (slice 9): same as the
            // sibling NL/Hash equivalence test — forces the Hash
            // algorithm via NL_THRESHOLD crossing. Inserting real
            // 100k-row data per side is impractical for a unit
            // test asserting algorithm equivalence.
            engine.query_optimizer().register_table_indexes(
                "users",
                &["dept_id".to_string()],
                100_000,
            );
            engine.query_optimizer().register_table_indexes(
                "depts",
                &["dept_name".to_string()],
                100_000,
            );
        }
        for (id, name) in &[("d1", "Eng"), ("d_empty", "Empty")] {
            engine
                .execute_query(Query::Insert {
                    table: "depts".to_string(),
                    data: json!({"id": id, "dept_name": name}),
                })
                .unwrap();
        }
        for (id, dept) in &[("u1", Some("d1")), ("u_lonely", None::<&str>)] {
            let mut row = serde_json::Map::new();
            row.insert("id".to_string(), json!(id));
            if let Some(d) = dept {
                row.insert("dept_id".to_string(), json!(d));
            }
            engine
                .execute_query(Query::Insert {
                    table: "users".to_string(),
                    data: serde_json::Value::Object(row),
                })
                .unwrap();
        }
        rows(
            &mut engine,
            "SELECT * FROM users u FULL OUTER JOIN depts d ON u.dept_id = d.id",
        )
        .len()
    }
    let nl = run(false);
    let hj = run(true);
    assert_eq!(nl, hj);
    // 1 matched + 1 unmatched-left + 1 unmatched-right = 3.
    assert_eq!(nl, 3);
}

// ─── RIGHT JOIN ─────────────────────────────────────────────────────

#[test]
fn right_join_equals_swapped_left_join() {
    let (_t, mut engine) = setup_users_depts();
    let right_join_rs = rows(
        &mut engine,
        "SELECT u.id, d.id AS d_id FROM users u RIGHT JOIN depts d ON u.dept_id = d.id",
    );
    let swapped_left_rs = rows(
        &mut engine,
        "SELECT u.id, d.id AS d_id FROM depts d LEFT JOIN users u ON u.dept_id = d.id",
    );
    let mut a: Vec<_> = right_join_rs
        .iter()
        .map(|r| {
            (
                r.get("id").cloned().unwrap_or(serde_json::Value::Null),
                r.get("d_id").cloned().unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    let mut b: Vec<_> = swapped_left_rs
        .iter()
        .map(|r| {
            (
                r.get("id").cloned().unwrap_or(serde_json::Value::Null),
                r.get("d_id").cloned().unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    a.sort_by(|x, y| x.0.to_string().cmp(&y.0.to_string()));
    b.sort_by(|x, y| x.0.to_string().cmp(&y.0.to_string()));
    assert_eq!(a, b);
    // Right-driven: every dept appears, plus matched-on-the-left.
    // 2 matches (u1-d1, u2-d1, u3-d2 = 3) + 1 unmatched right (d_empty) = 4 rows.
    assert_eq!(right_join_rs.len(), 4);
}

// ─── empty side ─────────────────────────────────────────────────────

#[test]
fn left_join_empty_right_preserves_all_left() {
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
    for i in 0..5 {
        engine
            .execute_query(Query::Insert {
                table: "left_t".to_string(),
                data: json!({"id": format!("l{}", i), "k": i}),
            })
            .unwrap();
    }
    let rs = rows(
        &mut engine,
        "SELECT * FROM left_t l LEFT JOIN right_t r ON l.k = r.k",
    );
    assert_eq!(rs.len(), 5);
}

// ─── composition with alias-resolution slice ────────────────────────

#[test]
fn outer_join_alias_resolution_for_matched_and_unmatched() {
    // Both tables have a colliding `name` column. After LEFT JOIN:
    // matched rows have `name` (left) and `d.name` (right). Unmatched-
    // left rows have only `name` (no d.name). `SELECT d.name` must
    // resolve correctly in both cases — matched returns the right's
    // name, unmatched returns NULL (bare fallback finds nothing).
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "depts".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "depts".to_string(),
            data: json!({"id": "d1", "name": "Engineering"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({"id": "u1", "name": "Alice", "dept_id": "d1"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({"id": "u_lonely", "name": "Dave"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT u.id, u.name AS user_name, d.name AS dept_name \
         FROM users u LEFT JOIN depts d ON u.dept_id = d.id",
    );
    let mut by_id: std::collections::HashMap<&str, &serde_json::Value> =
        std::collections::HashMap::new();
    for r in &rs {
        by_id.insert(r["id"].as_str().unwrap(), r);
    }
    assert_eq!(by_id["u1"]["user_name"], json!("Alice"));
    assert_eq!(by_id["u1"]["dept_name"], json!("Engineering"));
    assert_eq!(by_id["u_lonely"]["user_name"], json!("Dave"));
    // The bug-fix slice's bare-fallback limitation is gone: unmatched
    // rows now get explicit NULL-padding for the other side's column
    // set (via `null_pad_right_into`). `d.name` on an unmatched-left
    // row resolves to NULL, not the left's `name`. This is what
    // enables the anti-join pattern (`WHERE d.col IS NULL`).
    assert_eq!(by_id["u_lonely"]["dept_name"], serde_json::Value::Null);
}
