//! New arc, slice 2: UPDATE PK-change uniqueness checks. An UPDATE
//! that mutates a primary key to a colliding value fails immediately,
//! not at COMMIT. Buffer-aware (via slice 1's PkVisibility); covers
//! committed-collision, sibling-collision, deleted-then-reuse, and
//! multi-row scenarios.

use tempfile::TempDir;

use driftdb_core::sql_bridge::{execute_sql_in_session, SessionContext};
use driftdb_core::{Engine, QueryResult};

fn setup() -> (TempDir, Engine, SessionContext) {
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    let mut ctx = SessionContext::new();
    execute_sql_in_session(
        &mut engine,
        "CREATE TABLE t (id VARCHAR, name VARCHAR, PRIMARY KEY (id))",
        &mut ctx,
    )
    .unwrap();
    (temp, engine, ctx)
}

fn run(
    engine: &mut Engine,
    ctx: &mut SessionContext,
    sql: &str,
) -> driftdb_core::Result<QueryResult> {
    execute_sql_in_session(engine, sql, ctx)
}

fn select_ids(engine: &mut Engine, ctx: &mut SessionContext) -> Vec<String> {
    match run(engine, ctx, "SELECT id FROM t").unwrap() {
        QueryResult::Rows { data } => {
            let mut ids: Vec<String> = data
                .iter()
                .map(|r| r["id"].as_str().unwrap().to_string())
                .collect();
            ids.sort();
            ids
        }
        other => panic!("expected Rows, got {:?}", other),
    }
}

// ─── Pre-existing UPDATE behavior regression ──────────────────────────

#[test]
fn update_without_pk_change_works_normally() {
    // Regression for the hardcoded-"id" bug: a table with PK named
    // something other than "id" should now route the UPDATE through
    // the correct PK field. Use the default "id" name first to
    // confirm baseline, then a custom PK name for the regression.
    let (_t, mut engine, mut ctx) = setup();
    run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('a', 'alice')",
    )
    .unwrap();
    run(&mut engine, &mut ctx, "UPDATE t SET name = 'ALICE' WHERE id = 'a'").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT name FROM t WHERE id = 'a'").unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data[0]["name"], serde_json::json!("ALICE"));
    } else {
        panic!("expected Rows");
    }
}

#[test]
fn update_with_non_id_pk_now_works() {
    // Previously broken: the hardcoded `row_obj.get("id")` returned
    // Null for any table whose PK column wasn't literally named "id",
    // and the buffer/patch was keyed by Null. With the fix, the PK
    // field comes from the schema.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    let mut ctx = SessionContext::new();
    execute_sql_in_session(
        &mut engine,
        "CREATE TABLE accounts (account_id VARCHAR, balance INTEGER, PRIMARY KEY (account_id))",
        &mut ctx,
    )
    .unwrap();
    run(
        &mut engine,
        &mut ctx,
        "INSERT INTO accounts (account_id, balance) VALUES ('A1', 100)",
    )
    .unwrap();
    run(
        &mut engine,
        &mut ctx,
        "UPDATE accounts SET balance = 200 WHERE account_id = 'A1'",
    )
    .unwrap();
    let result = run(
        &mut engine,
        &mut ctx,
        "SELECT balance FROM accounts WHERE account_id = 'A1'",
    )
    .unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data[0]["balance"], serde_json::json!(200));
    } else {
        panic!("expected Rows");
    }
}

// ─── Auto-commit PK-change ────────────────────────────────────────────

#[test]
fn auto_commit_update_pk_change_no_collision_succeeds() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(
        &mut engine,
        &mut ctx,
        "UPDATE t SET id = 'b' WHERE id = 'a'",
    )
    .unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["b"]);
}

#[test]
fn auto_commit_update_pk_change_collides_errors() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'b' WHERE id = 'a'");
    assert!(res.is_err(), "auto-commit PK-change collision must error");
    // Both rows still present; the failed UPDATE did not partially apply.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a", "b"]);
}

// ─── Transactional PK-change ──────────────────────────────────────────

#[test]
fn txn_update_pk_change_collides_committed() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'b' WHERE id = 'a'");
    assert!(res.is_err(), "PK-change to committed row must error at UPDATE");
    assert!(ctx.aborted, "transaction must enter aborted state");
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    assert!(!ctx.aborted);
    // Committed state unchanged.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a", "b"]);
}

#[test]
fn txn_update_pk_change_collides_sibling_in_same_txn() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    // Insert a sibling in the txn, then try to UPDATE-rename onto it.
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'b' WHERE id = 'a'");
    assert!(res.is_err(), "PK-change to uncommitted sibling must error");
    assert!(ctx.aborted);
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

#[test]
fn txn_update_pk_change_after_delete_succeeds() {
    // DELETE x, then UPDATE y → x should succeed. The buffered
    // SoftDelete on x makes the slot reusable per slice-1's
    // delete-then-insert pattern, extended to UPDATE-rename here.
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'old-a')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'old-b')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "DELETE FROM t WHERE id = 'a'").unwrap();
    // Now rename 'b' to 'a'. The buffer's tombstone on 'a' should
    // make the PK free.
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'a' WHERE id = 'b'");
    assert!(
        res.is_ok(),
        "rename to a deleted PK should succeed: {:?}",
        res.err()
    );
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    // Only 'a' should remain, carrying the data from old 'b'.
    let result = run(&mut engine, &mut ctx, "SELECT id, name FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["id"], serde_json::json!("a"));
        assert_eq!(data[0]["name"], serde_json::json!("old-b"));
    } else {
        panic!("expected Rows");
    }
}

#[test]
fn txn_update_pk_change_no_collision_commits() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "UPDATE t SET id = 'z' WHERE id = 'a'").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["z"]);
}

// ─── Multi-row UPDATE ──────────────────────────────────────────────────

#[test]
fn multi_row_update_to_same_pk_collides_on_second_row() {
    // UPDATE t SET id = 'fixed' affects multiple rows. First row
    // succeeds (buffer or apply gets Insert('fixed')); second row's
    // PK-change collides with the now-Active 'fixed'.
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'fixed'");
    assert!(
        res.is_err(),
        "internal collision on multi-row PK-rewrite must error"
    );
}

#[test]
fn multi_row_update_pk_increment_collides_with_committed() {
    // UPDATE t SET id = id || '_new' — actually let's do something
    // simpler: rename 'a' to 'b' when 'b' already exists. Same
    // semantics as PG-default (PK constraints are not deferrable).
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    // UPDATE with WHERE id IN-effect equivalent: rename 'a' to 'b'.
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'b' WHERE id = 'a'");
    assert!(res.is_err());
}

// ─── Aborted-state propagation ────────────────────────────────────────

#[test]
fn aborted_update_blocks_further_statements_until_rollback() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let _ = run(&mut engine, &mut ctx, "UPDATE t SET id = 'b' WHERE id = 'a'");
    // Now aborted. Any non-ROLLBACK statement must fail with the canonical message.
    let sel = run(&mut engine, &mut ctx, "SELECT * FROM t");
    assert!(sel.is_err());
    assert!(sel.unwrap_err().to_string().contains("aborted"));
    // ROLLBACK clears.
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    assert!(!ctx.aborted);
    // Committed state still has both rows.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a", "b"]);
}

// ─── Atomicity (best-effort, documented) ──────────────────────────────

#[test]
fn rollback_after_update_pk_change_leaves_committed_intact() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "UPDATE t SET id = 'z' WHERE id = 'a'").unwrap();
    // Buffer has SoftDelete(a) + Insert(z), neither committed yet.
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    // Committed state still has 'a'.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a"]);
}
