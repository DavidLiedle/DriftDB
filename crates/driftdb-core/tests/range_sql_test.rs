//! End-to-end tests that range predicates and BETWEEN flow through
//! sql_bridge correctly — proving the parser fix (parse_where_clause
//! recursing on AND, handling BETWEEN) and the executor wiring agree
//! on the same access plan.

use tempfile::TempDir;

use driftdb_core::sql_bridge::execute_sql;
use driftdb_core::{Engine, QueryResult};

fn setup() -> (TempDir, Engine) {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    execute_sql(
        &mut engine,
        "CREATE TABLE items (id VARCHAR, name VARCHAR, price INTEGER, PRIMARY KEY (id))",
    )
    .unwrap();
    execute_sql(&mut engine, "CREATE INDEX idx_price ON items (price)").unwrap();
    for i in 0..20 {
        execute_sql(
            &mut engine,
            &format!(
                "INSERT INTO items (id, name, price) VALUES ('i{i}', 'item{i}', {price})",
                i = i,
                price = (i * 10) + 5  // 5, 15, 25, ..., 195
            ),
        )
        .unwrap();
    }
    (temp, engine)
}

fn rows(engine: &mut Engine, sql: &str) -> Vec<serde_json::Value> {
    match execute_sql(engine, sql).unwrap() {
        QueryResult::Rows { data } => data,
        other => panic!("expected Rows, got {:?}", other),
    }
}

#[test]
fn sanity_select_star_returns_all_rows() {
    let (_t, mut engine) = setup();
    let rs = rows(&mut engine, "SELECT * FROM items");
    assert_eq!(rs.len(), 20);
}

#[test]
fn sanity_equality_via_sql_works() {
    let (_t, mut engine) = setup();
    let rs = rows(&mut engine, "SELECT * FROM items WHERE price = 55");
    assert_eq!(rs.len(), 1, "got: {:?}", rs);
}

#[test]
fn sanity_single_predicate_range_via_sql() {
    let (_t, mut engine) = setup();
    let rs = rows(&mut engine, "SELECT * FROM items WHERE price > 180");
    let mut prices: Vec<i64> = rs.iter().map(|r| r["price"].as_i64().unwrap()).collect();
    prices.sort();
    assert_eq!(prices, vec![185, 195], "got: {:?}", rs);
}

#[test]
fn and_chain_with_range_returns_correct_rows() {
    let (_t, mut engine) = setup();
    let rs = rows(
        &mut engine,
        "SELECT * FROM items WHERE price > 50 AND price < 100",
    );
    let mut prices: Vec<i64> = rs.iter().map(|r| r["price"].as_i64().unwrap()).collect();
    prices.sort();
    // Prices 55, 65, 75, 85, 95.
    assert_eq!(prices, vec![55, 65, 75, 85, 95]);
}

#[test]
fn between_is_inclusive_on_both_sides() {
    let (_t, mut engine) = setup();
    let rs = rows(
        &mut engine,
        "SELECT * FROM items WHERE price BETWEEN 55 AND 95",
    );
    let mut prices: Vec<i64> = rs.iter().map(|r| r["price"].as_i64().unwrap()).collect();
    prices.sort();
    // BETWEEN 55 AND 95 → 55, 65, 75, 85, 95 inclusive of both endpoints.
    assert_eq!(prices, vec![55, 65, 75, 85, 95]);
}

#[test]
fn single_sided_range_via_sql() {
    let (_t, mut engine) = setup();
    let rs = rows(&mut engine, "SELECT * FROM items WHERE price >= 175");
    let mut prices: Vec<i64> = rs.iter().map(|r| r["price"].as_i64().unwrap()).collect();
    prices.sort();
    // Prices 175, 185, 195.
    assert_eq!(prices, vec![175, 185, 195]);
}

#[test]
fn empty_range_returns_zero_rows_via_sql() {
    let (_t, mut engine) = setup();
    let rs = rows(
        &mut engine,
        "SELECT * FROM items WHERE price > 200 AND price < 100",
    );
    assert!(rs.is_empty());
}
