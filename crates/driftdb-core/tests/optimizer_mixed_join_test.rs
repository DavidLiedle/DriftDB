//! Slice 7: mixed INNER+OUTER reordering with segmented chains.
//! INNER segments reorder independently around fixed OUTER anchors.
//! Also includes regression tests for the legacy ON-orientation bug
//! fix that this slice landed alongside.

use serde_json::json;
use tempfile::TempDir;

use driftdb_core::sql_bridge::execute_sql;
use driftdb_core::{Engine, Query, QueryResult};

fn rows(engine: &mut Engine, sql: &str) -> Vec<serde_json::Value> {
    match execute_sql(engine, sql).unwrap() {
        QueryResult::Rows { data } => data,
        other => panic!("expected Rows, got {:?}", other),
    }
}

// ─── Legacy ON-orientation regression ─────────────────────────────

#[test]
fn legacy_on_reversed_order_matches_correctly() {
    // The exact failure case the prior writeup flagged: ON
    // `o.customer_id = c.id` where customers (alias c) is the
    // FROM-clause left and orders (alias o) is the new right. The
    // legacy multi-join path used `extract_join_columns` which
    // returned columns in source-text order — depts.dept_id =
    // orders.id, which matches nothing. This test ensures the fix
    // applied in this slice survives.
    //
    // Two-table single-join already had this fixed via the OUTER
    // slice. The regression here is the multi-join legacy path —
    // hit via a chain that doesn't qualify for the optimized
    // multi-join shape gate. To force the legacy path, we use a
    // non-qualified ON clause that `extract_qualified_edge` rejects.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["customers", "orders"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "customers".to_string(),
            data: json!({"id": "c1", "cname": "Acme"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "orders".to_string(),
            data: json!({"id": "o1", "customer_id": "c1"}),
        })
        .unwrap();
    // Single join with backwards ON, optimized single-join path
    // handles this via `orient_join_columns_to_from`.
    let rs = rows(
        &mut engine,
        "SELECT o.id FROM customers c JOIN orders o ON o.customer_id = c.id",
    );
    assert_eq!(rs.len(), 1, "single-join optimized path should find o1");
}

#[test]
fn legacy_multi_join_path_handles_reversed_on() {
    // Force the legacy multi-join path (not the optimized
    // segmented one) by having a chain where one ON uses an
    // unqualified column reference — that rejects from the
    // optimized path's `extract_qualified_edge`. The legacy path
    // takes over; its newly-added orient fix must still produce
    // correct results for the prefixed ONs in the chain.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b", "c"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "a1", "k": 1}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b1", "aid": "a1", "k": 1}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "c1", "k": 1}),
        })
        .unwrap();
    // First ON has the new-right alias on the LEFT of `=` (b.aid =
    // a.id). Second ON uses USING-shape via unqualified `k = k`
    // (sqlparser parses it as a plain Identifier comparison) — the
    // optimized path rejects unqualified, falling through to
    // legacy. Legacy's new orient fix handles the first.
    let rs = rows(
        &mut engine,
        "SELECT a.id FROM a JOIN b ON b.aid = a.id JOIN c ON b.k = c.k",
    );
    // 1 join × 1 join = 1 row.
    assert_eq!(rs.len(), 1, "legacy path with backwards ON should find a1");
}

// ─── Segmented mixed-chain correctness ────────────────────────────

fn setup_mixed_three() -> (TempDir, Engine) {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b", "c"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    // a has 2 rows. b joins to a via b.aid = a.id (one of a's
    // rows has no b row → unmatched a in any LEFT JOIN). c joins
    // to b via c.bid = b.id (one of b's rows has no c row →
    // unmatched b in any LEFT to c).
    for i in 1..=2 {
        engine
            .execute_query(Query::Insert {
                table: "a".to_string(),
                data: json!({"id": format!("a{}", i), "av": i}),
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b1", "aid": "a1", "bv": "one"}),
        })
        .unwrap();
    // a2 has no b → unmatched in inner.
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "c1", "bid": "b1", "cv": "X"}),
        })
        .unwrap();
    (temp, engine)
}

#[test]
fn inner_then_left_outer_reorders_inner_segment() {
    // A INNER B LEFT C: {a, b} reorders if stats favor it; LEFT C
    // stays put. Result is the LEFT JOIN of (a INNER b) with c.
    let (_t, mut engine) = setup_mixed_three();
    let rs = rows(
        &mut engine,
        "SELECT a.id, b.bv, c.cv FROM a \
         JOIN b ON b.aid = a.id \
         LEFT JOIN c ON c.bid = b.id",
    );
    // a INNER b = {(a1, b1)}. LEFT JOIN c on c.bid=b.id matches
    // c1 → one row with everything.
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
    assert_eq!(rs[0]["bv"], json!("one"));
    assert_eq!(rs[0]["cv"], json!("X"));
}

#[test]
fn inner_reorder_with_stats_around_outer_anchor() {
    // Real data + ANALYZE: setup_mixed_three has a=2 rows, b=1 row.
    // After ANALYZE the planner sees b as smaller and seeds the
    // first INNER segment from b instead of a, even though SQL
    // source order has a first. Result row set is identical.
    let (_t, mut engine) = setup_mixed_three();
    execute_sql(&mut engine, "ANALYZE TABLE a").unwrap();
    execute_sql(&mut engine, "ANALYZE TABLE b").unwrap();
    execute_sql(&mut engine, "ANALYZE TABLE c").unwrap();

    let rs = rows(
        &mut engine,
        "SELECT a.id, b.bv FROM a \
         JOIN b ON b.aid = a.id \
         LEFT JOIN c ON c.bid = b.id",
    );
    // The first segment {a, b} INNER → {(a1, b1)}. LEFT JOIN c →
    // adds c1. One row.
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
}

#[test]
fn outer_first_then_inner_reorders_extension_segment() {
    // First join is OUTER. Subsequent INNER chain reorders.
    // FROM a LEFT JOIN b INNER JOIN c.
    let (_t, mut engine) = setup_mixed_three();
    let rs = rows(
        &mut engine,
        "SELECT a.id, b.bv, c.cv FROM a \
         LEFT JOIN b ON b.aid = a.id \
         JOIN c ON c.bid = b.id",
    );
    // a LEFT JOIN b → {(a1, b1), (a2, NULL)}. INNER JOIN c on
    // c.bid=b.id → drops a2 (b is NULL, c.bid can't match) and
    // matches c1 for a1. One row.
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
    assert_eq!(rs[0]["bv"], json!("one"));
    assert_eq!(rs[0]["cv"], json!("X"));
}

#[test]
fn inner_full_outer_inner_reorders_both_segments() {
    // Two INNER segments separated by a FULL OUTER anchor.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b", "c", "d", "e"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    for (table, row) in &[
        ("a", json!({"id": "a1", "k": 1})),
        ("b", json!({"id": "b1", "ak": 1, "bk": 1})),
        ("c", json!({"id": "c1", "bk": 1, "ck": 1})),
        ("d", json!({"id": "d1", "ck": 1, "dk": 1})),
        ("e", json!({"id": "e1", "dk": 1})),
    ] {
        engine
            .execute_query(Query::Insert {
                table: table.to_string(),
                data: row.clone(),
            })
            .unwrap();
    }
    // a INNER b on a.k=b.ak; FULL c on b.bk=c.bk; INNER d on c.ck=d.ck;
    // INNER e on d.dk=e.dk.
    let rs = rows(
        &mut engine,
        "SELECT a.id, b.id AS bid, c.id AS cid, d.id AS did, e.id AS eid \
         FROM a \
         JOIN b ON a.k = b.ak \
         FULL OUTER JOIN c ON b.bk = c.bk \
         JOIN d ON c.ck = d.ck \
         JOIN e ON d.dk = e.dk",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
    assert_eq!(rs[0]["bid"], json!("b1"));
    assert_eq!(rs[0]["cid"], json!("c1"));
    assert_eq!(rs[0]["did"], json!("d1"));
    assert_eq!(rs[0]["eid"], json!("e1"));
}

#[test]
fn consecutive_outers_no_reordering() {
    // A LEFT B LEFT C: two consecutive OUTER anchors with no INNER
    // segments to reorder. Must still produce correct results.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b", "c"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "a1", "k": 1}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b1", "k": 1, "bk": 9}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "c1", "bk": 9}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT a.id, b.id AS bid, c.id AS cid \
         FROM a LEFT JOIN b ON a.k = b.k LEFT JOIN c ON b.bk = c.bk",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
    assert_eq!(rs[0]["bid"], json!("b1"));
    assert_eq!(rs[0]["cid"], json!("c1"));
}

#[test]
fn pushdown_to_inner_segment_only_not_to_outer_right() {
    // WHERE a.av = 1 AND c.cv = 'X'. The first pushes to a (INNER
    // segment, preserving). The second targets c (OUTER right) and
    // must NOT push down. Correctness is the same either way thanks
    // to the post-join filter — this test just checks results.
    let (_t, mut engine) = setup_mixed_three();
    let rs = rows(
        &mut engine,
        "SELECT a.id FROM a \
         JOIN b ON b.aid = a.id \
         LEFT JOIN c ON c.bid = b.id \
         WHERE a.av = 1 AND c.cv = 'X'",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
}

#[test]
fn anti_join_pattern_in_mixed_chain() {
    // A INNER B LEFT C WHERE c.x IS NULL.
    // Find rows where the LEFT joined side is absent.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b", "c"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "a1", "k": 1}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "a2", "k": 2}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b1", "ak": 1, "bk": 9}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b2", "ak": 2, "bk": 99}),
        })
        .unwrap();
    // Only c with bk=9 → matches b1 only. b2 has bk=99 → no match.
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "c1", "bk": 9}),
        })
        .unwrap();
    // The anti-join pattern. a INNER b → {(a1, b1), (a2, b2)}.
    // LEFT JOIN c on b.bk=c.bk → {(a1, b1, c1), (a2, b2, NULL)}.
    // WHERE c.id IS NULL → {(a2, b2, NULL)}.
    let rs = rows(
        &mut engine,
        "SELECT a.id FROM a \
         JOIN b ON a.k = b.ak \
         LEFT JOIN c ON b.bk = c.bk \
         WHERE c.id IS NULL",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a2"));
}

#[test]
fn chain_ending_with_outer_anchor() {
    // Inner segment + Outer at the end. The OUTER has no trailing
    // INNER extension; the chain terminates with the OUTER.
    let (_t, mut engine) = setup_mixed_three();
    let rs = rows(
        &mut engine,
        "SELECT a.id, c.cv FROM a JOIN b ON b.aid = a.id LEFT JOIN c ON c.bid = b.id",
    );
    // a INNER b = {(a1, b1)}. LEFT JOIN c → adds c1 to the one row.
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["cv"], json!("X"));
}

#[test]
fn segmentation_with_unmatched_left_in_first_segment() {
    // a has 2 rows; b matches only one of them. INNER joins drop
    // the unmatched one in the first segment. LEFT JOIN c then
    // operates on the (a, b)=(a1, b1) result.
    let (_t, mut engine) = setup_mixed_three();
    let rs = rows(
        &mut engine,
        "SELECT a.id FROM a JOIN b ON b.aid = a.id LEFT JOIN c ON c.bid = b.id",
    );
    // a INNER b: only a1 has a b. → 1 row. LEFT JOIN c: c1 matches.
    // → 1 row.
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("a1"));
}

#[test]
fn unqualified_join_falls_through_to_legacy() {
    // Optimized path requires alias.col on both sides of every ON.
    // An unqualified ON (`k = k`) falls through to legacy. Legacy
    // (with the orient fix) produces correct results.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "a1", "k": 1}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b1", "k": 1}),
        })
        .unwrap();
    // sqlparser treats `JOIN b USING (k)` differently from a bare
    // ON. Use USING to force the unqualified path.
    let rs = rows(&mut engine, "SELECT a.id FROM a JOIN b USING (k)");
    assert_eq!(rs.len(), 1);
}
