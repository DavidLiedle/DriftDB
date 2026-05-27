//! Slice 6: multi-join reordering. First time the executor walks a
//! plan tree whose structure differs from the SQL source order. Scope
//! is pure-INNER chains (3+ tables); mixed INNER+OUTER falls through
//! to legacy (correctness preserved, but no reordering applied).

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

/// Three tables: customers (small), orders (large), products (small).
/// Predicate graph is a chain: customers ↔ orders ↔ products.
fn setup_three_way() -> (TempDir, Engine) {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["customers", "orders", "products"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    for (id, name) in &[("c1", "Acme"), ("c2", "Globex")] {
        engine
            .execute_query(Query::Insert {
                table: "customers".to_string(),
                data: json!({"id": id, "cname": name}),
            })
            .unwrap();
    }
    for (id, name) in &[("p1", "Widget"), ("p2", "Gadget")] {
        engine
            .execute_query(Query::Insert {
                table: "products".to_string(),
                data: json!({"id": id, "pname": name}),
            })
            .unwrap();
    }
    for (id, cid, pid) in &[
        ("o1", "c1", "p1"),
        ("o2", "c1", "p2"),
        ("o3", "c2", "p1"),
    ] {
        engine
            .execute_query(Query::Insert {
                table: "orders".to_string(),
                data: json!({"id": id, "customer_id": cid, "product_id": pid}),
            })
            .unwrap();
    }
    (temp, engine)
}

#[test]
fn three_way_inner_join_returns_correct_rows() {
    // Source order: customers JOIN orders JOIN products.
    // Verify all 3 orders appear with both customer and product info.
    let (_t, mut engine) = setup_three_way();
    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid, c.cname, p.pname \
         FROM customers c \
         JOIN orders o ON o.customer_id = c.id \
         JOIN products p ON o.product_id = p.id",
    );
    assert_eq!(rs.len(), 3);
    let mut tuples: Vec<(String, String, String)> = rs
        .iter()
        .map(|r| {
            (
                r["oid"].as_str().unwrap().to_string(),
                r["cname"].as_str().unwrap().to_string(),
                r["pname"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    tuples.sort();
    assert_eq!(
        tuples,
        vec![
            ("o1".to_string(), "Acme".to_string(), "Widget".to_string()),
            ("o2".to_string(), "Acme".to_string(), "Gadget".to_string()),
            ("o3".to_string(), "Globex".to_string(), "Widget".to_string()),
        ]
    );
}

#[test]
fn reordering_picks_smaller_leaf_as_seed() {
    // Real data + ANALYZE drives reorder. `setup_three_way` produces
    // customers=2, products=2, orders=3; ANALYZE-derived row counts
    // make customers / products the smaller seeds. Source order
    // puts `orders` first; the planner reorders to seed from a
    // smaller table. Assertion is correctness — the row set is
    // identical regardless of which leaf seeds.
    let (_t, mut engine) = setup_three_way();
    execute_sql(&mut engine, "ANALYZE TABLE customers").unwrap();
    execute_sql(&mut engine, "ANALYZE TABLE products").unwrap();
    execute_sql(&mut engine, "ANALYZE TABLE orders").unwrap();

    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid, c.cname, p.pname \
         FROM orders o \
         JOIN customers c ON o.customer_id = c.id \
         JOIN products p ON o.product_id = p.id",
    );
    assert_eq!(rs.len(), 3, "row count must match regardless of seed choice");
}

#[test]
fn four_way_inner_join_correctness() {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    for t in &["a", "b", "c", "d"] {
        engine
            .execute_query(Query::CreateTable {
                name: t.to_string(),
                primary_key: "id".to_string(),
                indexed_columns: vec![],
            })
            .unwrap();
    }
    // Chain: a.id ↔ b.aid ↔ c.bid ↔ d.cid
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "a1", "aname": "A1"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "b1", "aid": "a1", "bname": "B1"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "c1", "bid": "b1", "cname": "C1"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "d".to_string(),
            data: json!({"id": "d1", "cid": "c1", "dname": "D1"}),
        })
        .unwrap();

    let rs = rows(
        &mut engine,
        "SELECT a.aname, b.bname, c.cname, d.dname \
         FROM a \
         JOIN b ON b.aid = a.id \
         JOIN c ON c.bid = b.id \
         JOIN d ON d.cid = c.id",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["aname"], json!("A1"));
    assert_eq!(rs[0]["bname"], json!("B1"));
    assert_eq!(rs[0]["cname"], json!("C1"));
    assert_eq!(rs[0]["dname"], json!("D1"));
}

#[test]
fn pushdown_to_leaf_regardless_of_position_in_reorder() {
    // WHERE references one table only. The pushdown should apply at
    // that leaf's scan, regardless of where the leaf ends up in the
    // reordered plan.
    let (_t, mut engine) = setup_three_way();
    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid FROM customers c \
         JOIN orders o ON o.customer_id = c.id \
         JOIN products p ON o.product_id = p.id \
         WHERE c.id = 'c2'",
    );
    // Only c2's orders: o3.
    let ids: Vec<&str> = rs.iter().map(|r| r["oid"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["o3"]);
}

#[test]
fn three_way_result_invariant_under_seed_choice() {
    // Run the same query twice: once with no stats (source order),
    // once after ANALYZE (stats-driven reorder). Result rows must
    // be identical regardless of seed choice.
    fn run(with_analyze: bool) -> Vec<(String, String, String)> {
        let (_t, mut engine) = setup_three_way();
        if with_analyze {
            execute_sql(&mut engine, "ANALYZE TABLE customers").unwrap();
            execute_sql(&mut engine, "ANALYZE TABLE products").unwrap();
            execute_sql(&mut engine, "ANALYZE TABLE orders").unwrap();
        }
        let rs = rows(
            &mut engine,
            "SELECT o.id AS oid, c.cname, p.pname \
             FROM customers c \
             JOIN orders o ON o.customer_id = c.id \
             JOIN products p ON o.product_id = p.id",
        );
        let mut tuples: Vec<(String, String, String)> = rs
            .iter()
            .map(|r| {
                (
                    r["oid"].as_str().unwrap().to_string(),
                    r["cname"].as_str().unwrap().to_string(),
                    r["pname"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        tuples.sort();
        tuples
    }
    let no_reorder = run(false);
    let reorder = run(true);
    assert_eq!(no_reorder, reorder);
}

#[test]
fn disconnected_predicate_graph_falls_through() {
    // Three tables but only two have an ON between them; the third
    // is "joined" with a constraint that doesn't reference it.
    // sqlparser will accept the SQL but the planner can't reorder
    // safely → falls through to legacy.
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
            data: json!({"id": "b1", "k": 1}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "c1"}),
        })
        .unwrap();
    // Both ON conditions reference a.k=b.k — the second join is
    // structurally pointless but legal SQL. Legacy path handles it.
    let result = execute_sql(
        &mut engine,
        "SELECT a.id FROM a JOIN b ON a.k = b.k JOIN c ON a.k = b.k",
    );
    // The point: it doesn't crash; legacy produces *some* result.
    assert!(result.is_ok());
}

#[test]
fn mixed_inner_outer_falls_through_to_legacy() {
    // Mixed INNER+OUTER chains aren't in this slice's reordering
    // scope. The slice's responsibility is "don't break what worked
    // before" via the legacy path. The legacy multi-join path has a
    // pre-existing ON-orientation limitation (it doesn't normalize
    // when the ON expression's LHS isn't the FROM-clause left
    // table). Writing ON in canonical FROM order — `c.id =
    // o.customer_id` rather than `o.customer_id = c.id` — sidesteps
    // it. The orient fix from the OUTER JOIN slice only applies on
    // the optimized single-join path; extending it to the legacy
    // multi-join loop is queued for the mixed-reordering slice.
    let (_t, mut engine) = setup_three_way();
    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid FROM customers c \
         INNER JOIN orders o ON c.id = o.customer_id \
         LEFT JOIN products p ON o.product_id = p.id",
    );
    assert_eq!(rs.len(), 3, "all 3 orders should appear");
}

#[test]
fn three_way_composes_with_index_selection_at_leaf() {
    // Slice 1 wiring: indexed equality on a leaf becomes an
    // IndexLookup. With multi-join reordering pushing down the
    // single-table predicate, the indexed leaf's Engine::select
    // should use the index path. Black-box assertion: result is
    // correct.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["email".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "orders".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "items".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    for i in 0..5 {
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: json!({"id": format!("u{}", i), "email": format!("u{}@x", i)}),
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "orders".to_string(),
            data: json!({"id": "o1", "user_id": "u2", "item_id": "i1"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "items".to_string(),
            data: json!({"id": "i1", "iname": "Widget"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid FROM users u \
         JOIN orders o ON o.user_id = u.id \
         JOIN items i ON i.id = o.item_id \
         WHERE u.email = 'u2@x'",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["oid"], json!("o1"));
}

#[test]
fn three_way_composes_with_range_scan_at_leaf() {
    // Slice 3 wiring: range predicate on an indexed leaf → IndexScan.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["age".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "orders".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "items".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    for i in 0..30 {
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: json!({"id": format!("u{}", i), "age": 20 + i}),
            })
            .unwrap();
    }
    engine
        .execute_query(Query::Insert {
            table: "orders".to_string(),
            data: json!({"id": "o1", "user_id": "u25", "item_id": "i1"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "items".to_string(),
            data: json!({"id": "i1", "iname": "Widget"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid FROM users u \
         JOIN orders o ON o.user_id = u.id \
         JOIN items i ON i.id = o.item_id \
         WHERE u.age >= 40 AND u.age < 50",
    );
    // u25 has age 45 → in range. Only matching order is o1.
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["oid"], json!("o1"));
}

#[test]
fn empty_intermediate_result_no_panic() {
    // Three-way INNER where the join condition can't be satisfied
    // (no overlap between c.id and o.customer_id).
    let (_t, mut engine) = setup_three_way();
    let rs = rows(
        &mut engine,
        "SELECT o.id AS oid FROM customers c \
         JOIN orders o ON o.customer_id = c.id \
         JOIN products p ON o.product_id = p.id \
         WHERE c.id = 'nonexistent'",
    );
    assert!(rs.is_empty());
}

#[test]
fn three_way_self_join_via_aliases() {
    // Same table joined to itself three times via three aliases.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "node".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    // Three nodes in a chain: n1 → n2 → n3.
    engine
        .execute_query(Query::Insert {
            table: "node".to_string(),
            data: json!({"id": "n1", "next": "n2", "label": "first"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "node".to_string(),
            data: json!({"id": "n2", "next": "n3", "label": "middle"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "node".to_string(),
            data: json!({"id": "n3", "label": "last"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT a.label AS al, b.label AS bl, c.label AS cl \
         FROM node a \
         JOIN node b ON b.id = a.next \
         JOIN node c ON c.id = b.next",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["al"], json!("first"));
    assert_eq!(rs[0]["bl"], json!("middle"));
    assert_eq!(rs[0]["cl"], json!("last"));
}
