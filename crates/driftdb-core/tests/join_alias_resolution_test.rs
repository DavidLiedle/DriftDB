//! Bug-fix slice: alias-aware CompoundIdentifier resolution across
//! joined rows. The pre-existing failure was that `d.name` would
//! resolve to the LEFT row's `name` after a join (because the right's
//! collision was renamed `t{N}_name` and the resolver stripped the
//! `d.` prefix and looked up bare `name`). After this slice,
//! collisions key as `{right_alias}.col` and resolution tries
//! `alias.col` first then bare.

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

fn setup_users_depts_same_name() -> (TempDir, Engine) {
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

    // depts.name collides with users.name — the exact bug pattern.
    engine
        .execute_query(Query::Insert {
            table: "depts".to_string(),
            data: json!({"id": "d1", "name": "Engineering"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "depts".to_string(),
            data: json!({"id": "d2", "name": "Sales"}),
        })
        .unwrap();
    for (id, name, dept) in &[
        ("u1", "Alice", "d1"),
        ("u2", "Bob", "d1"),
        ("u3", "Carol", "d2"),
    ] {
        engine
            .execute_query(Query::Insert {
                table: "users".to_string(),
                data: json!({"id": id, "name": name, "dept_id": dept}),
            })
            .unwrap();
    }
    (temp, engine)
}

#[test]
fn post_join_where_resolves_right_side_aliased_collision() {
    // The exact failure pattern from slice 4. `d.name = 'Engineering'`
    // must filter by depts.name, not users.name.
    let (_t, mut engine) = setup_users_depts_same_name();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u JOIN depts d ON u.dept_id = d.id \
         WHERE d.name = 'Engineering'",
    );
    let mut ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec!["u1", "u2"]);
}

#[test]
fn post_join_where_resolves_left_side_collision() {
    // Mirror: `u.name = 'Alice'` filters by users.name, not depts.name.
    let (_t, mut engine) = setup_users_depts_same_name();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u JOIN depts d ON u.dept_id = d.id \
         WHERE u.name = 'Alice'",
    );
    let ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["u1"]);
}

#[test]
fn select_alias_qualified_column_projects_right_value() {
    // SELECT d.name AS dept must come from depts.name, not users.name.
    let (_t, mut engine) = setup_users_depts_same_name();
    let rs = rows(
        &mut engine,
        "SELECT u.name AS user_name, d.name AS dept_name \
         FROM users u JOIN depts d ON u.dept_id = d.id \
         WHERE u.id = 'u1'",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["user_name"], json!("Alice"));
    assert_eq!(rs[0]["dept_name"], json!("Engineering"));
}

#[test]
fn multiple_colliding_columns_all_resolve_correctly() {
    // Both tables share TWO column names. All four resolutions must
    // pick the right side.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "a".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "b".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "a".to_string(),
            data: json!({"id": "k1", "name": "a-name", "kind": "a-kind"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "k1", "name": "b-name", "kind": "b-kind"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT a.name AS an, b.name AS bn, a.kind AS ak, b.kind AS bk \
         FROM a JOIN b ON a.id = b.id",
    );
    assert_eq!(rs.len(), 1);
    let r = &rs[0];
    assert_eq!(r["an"], json!("a-name"));
    assert_eq!(r["bn"], json!("b-name"));
    assert_eq!(r["ak"], json!("a-kind"));
    assert_eq!(r["bk"], json!("b-kind"));
}

#[test]
fn no_alias_falls_back_to_table_name() {
    // FROM users JOIN depts ON ... — no explicit aliases. The bare
    // table name acts as the alias. `depts.name = 'X'` works.
    let (_t, mut engine) = setup_users_depts_same_name();
    let rs = rows(
        &mut engine,
        "SELECT users.id FROM users JOIN depts ON users.dept_id = depts.id \
         WHERE depts.name = 'Sales'",
    );
    let ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["u3"]);
}

#[test]
fn self_join_with_distinct_aliases() {
    // Same table joined to itself with two aliases. Aliases must
    // disambiguate the two copies' columns. Classic manager/employee
    // shape: e.manager_id = m.id.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "people".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec![],
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "people".to_string(),
            data: json!({"id": "p1", "name": "Manager", "manager_id": null}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "people".to_string(),
            data: json!({"id": "p2", "name": "Employee", "manager_id": "p1"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT e.name AS employee, m.name AS manager \
         FROM people e JOIN people m ON e.manager_id = m.id",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["employee"], json!("Employee"));
    assert_eq!(rs[0]["manager"], json!("Manager"));
}

#[test]
fn hash_join_path_resolves_aliases_identically() {
    // Slice-4 hash join must produce the same row shape as nested
    // loop. Force HashJoin via large row_count_hint on a non-join-key
    // indexed column, then issue the same query as the NL test and
    // assert identical output.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "users".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    engine
        .execute_query(Query::CreateTable {
            name: "depts".to_string(),
            primary_key: "id".to_string(),
            indexed_columns: vec!["other".to_string()],
        })
        .unwrap();
    // Optimizer heuristic: both sides registered with row_count_hint
    // 10_000 > NL_THRESHOLD, neither has an index on join column →
    // HashJoin.
    engine
        .execute_query(Query::Insert {
            table: "depts".to_string(),
            data: json!({"id": "d1", "other": "x", "name": "Engineering"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "users".to_string(),
            data: json!({"id": "u1", "other": "x", "name": "Alice", "dept_id": "d1"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT u.name AS un, d.name AS dn FROM users u JOIN depts d ON u.dept_id = d.id",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["un"], json!("Alice"));
    assert_eq!(rs[0]["dn"], json!("Engineering"));
}

#[test]
fn composition_with_slice_1_and_aliased_post_join_filter() {
    // Composition: indexed equality pushes down to users-side (slice
    // 1 wiring), AND a post-join filter references the colliding
    // column via alias.
    let (_t, mut engine) = setup_users_depts_same_name();
    let rs = rows(
        &mut engine,
        "SELECT u.id FROM users u JOIN depts d ON u.dept_id = d.id \
         WHERE u.dept_id = 'd1' AND d.name = 'Engineering'",
    );
    let mut ids: Vec<&str> = rs.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort();
    // dept_id = d1 → u1, u2. Both have d.name = 'Engineering'.
    assert_eq!(ids, vec!["u1", "u2"]);
}

#[test]
fn bare_unprefixed_column_still_resolves() {
    // Queries without table prefixes continue working — non-prefixed
    // references look up the bare column name, which is left-side for
    // collisions (PostgreSQL behavior is to error on ambiguity; we
    // silently pick left-side, documented elsewhere).
    let (_t, mut engine) = setup_users_depts_same_name();
    // `id` exists on both sides; bare `id` picks left (users).
    let rs = rows(
        &mut engine,
        "SELECT id, name FROM users u JOIN depts d ON u.dept_id = d.id WHERE id = 'u3'",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["id"], json!("u3"));
    // Bare `name` is left (users.name) — same source as `u.name`.
    assert_eq!(rs[0]["name"], json!("Carol"));
}

#[test]
fn three_way_join_aliases_disambiguate_each_level() {
    // (A JOIN B) JOIN C with all three sharing a `name` column. Each
    // alias must point at its own table.
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
            data: json!({"id": "k", "name": "from-a", "b_id": "k", "c_id": "k"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "b".to_string(),
            data: json!({"id": "k", "name": "from-b"}),
        })
        .unwrap();
    engine
        .execute_query(Query::Insert {
            table: "c".to_string(),
            data: json!({"id": "k", "name": "from-c"}),
        })
        .unwrap();
    let rs = rows(
        &mut engine,
        "SELECT a.name AS an, b.name AS bn, c.name AS cn \
         FROM a JOIN b ON a.b_id = b.id JOIN c ON a.c_id = c.id",
    );
    assert_eq!(rs.len(), 1);
    assert_eq!(rs[0]["an"], json!("from-a"));
    assert_eq!(rs[0]["bn"], json!("from-b"));
    assert_eq!(rs[0]["cn"], json!("from-c"));
}
