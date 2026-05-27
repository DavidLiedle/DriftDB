//! New arc, slice 1: PK uniqueness checks at INSERT time inside
//! transactions. Covers committed-state collisions, sibling-row
//! collisions within the same transaction, the delete-then-insert
//! pattern, and PostgreSQL-style aborted-transaction state.

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

fn run(engine: &mut Engine, ctx: &mut SessionContext, sql: &str) -> driftdb_core::Result<QueryResult> {
    execute_sql_in_session(engine, sql, ctx)
}

// ─── Auto-commit (baseline regression) ──────────────────────────────

#[test]
fn auto_commit_duplicate_pk_rejects_immediately() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'first')").unwrap();
    let dup = run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'second')");
    assert!(dup.is_err(), "auto-commit duplicate PK should error");
}

// ─── Inside a transaction: collision with committed data ────────────

#[test]
fn txn_insert_collides_with_committed_row() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'committed')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let dup = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('x', 'tx-attempt')",
    );
    assert!(dup.is_err(), "duplicate PK against committed must error at INSERT");
    let msg = dup.unwrap_err().to_string();
    assert!(
        msg.contains("duplicate key value")
            || msg.contains("already exists"),
        "PG-style error wording: {}",
        msg
    );
    // ROLLBACK clears the abort.
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

#[test]
fn txn_insert_collides_with_sibling_row_in_same_txn() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'first')").unwrap();
    let dup = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('x', 'second')",
    );
    assert!(
        dup.is_err(),
        "duplicate PK against uncommitted sibling must error at INSERT (no silent overwrite)"
    );
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

// ─── Delete-then-insert is allowed ──────────────────────────────────

#[test]
fn txn_insert_after_delete_of_same_pk_succeeds() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'initial')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "DELETE FROM t WHERE id = 'x'").unwrap();
    // After DELETE in the same transaction, INSERT 'x' is the
    // canonical "replace the row" pattern.
    let reinsert = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('x', 'replaced')",
    );
    assert!(
        reinsert.is_ok(),
        "delete-then-insert of same PK should succeed: {:?}",
        reinsert.err()
    );
    run(&mut engine, &mut ctx, "COMMIT").unwrap();

    // Verify the row reflects the replacement.
    let result = run(&mut engine, &mut ctx, "SELECT name FROM t WHERE id = 'x'").unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["name"], serde_json::json!("replaced"));
    } else {
        panic!("expected Rows");
    }
}

// ─── Aborted-transaction state ──────────────────────────────────────

#[test]
fn aborted_txn_rejects_further_statements_until_rollback() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'a')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let _ = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('x', 'b')",
    );
    // Now aborted. Any non-ROLLBACK statement must fail.
    let select_attempt = run(&mut engine, &mut ctx, "SELECT * FROM t");
    let err_msg = select_attempt.unwrap_err().to_string();
    assert!(
        err_msg.contains("aborted"),
        "post-violation SELECT must mention abort: {}",
        err_msg
    );

    let insert_attempt = run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('y', 'fresh')");
    assert!(
        insert_attempt.is_err(),
        "post-violation INSERT must also fail"
    );

    // ROLLBACK works and clears the abort state.
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    assert!(!ctx.aborted, "ROLLBACK must clear aborted flag");

    // A new statement (auto-commit) succeeds.
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('y', 'fresh')").unwrap();
}

#[test]
fn aborted_txn_commit_treated_as_rollback() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'a')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('y', 'b')").unwrap();
    let _ = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('x', 'c')",
    );
    // COMMIT on an aborted txn must NOT persist the y INSERT — it
    // behaves as ROLLBACK per PostgreSQL.
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT * FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        // Only the original 'x' row should be present.
        assert_eq!(data.len(), 1, "aborted COMMIT must roll back; got {:?}", data);
    } else {
        panic!("expected Rows");
    }
}

#[test]
fn new_transaction_after_aborted_one_is_clean() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'a')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let _ = run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'b')");
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    // Next transaction starts fresh.
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('z', 'fresh')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT * FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data.len(), 2, "x and z present");
    } else {
        panic!("expected Rows");
    }
}

// ─── ROLLBACK preserves committed state ─────────────────────────────

#[test]
fn rollback_after_pk_violation_leaves_db_untouched() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'committed')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    // A successful INSERT inside the txn.
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('y', 'pending')").unwrap();
    // Then a violation.
    let _ = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO t (id, name) VALUES ('x', 'conflict')",
    );
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    // 'y' must not have leaked into committed state.
    let result = run(&mut engine, &mut ctx, "SELECT id FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        let ids: Vec<&str> = data.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["x"], "only committed row should remain");
    } else {
        panic!("expected Rows");
    }
}

// ─── Successful transaction: no false positives ─────────────────────

#[test]
fn txn_inserts_distinct_pks_commit_cleanly() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    for c in ['a', 'b', 'c', 'd'] {
        run(
            &mut engine,
            &mut ctx,
            &format!("INSERT INTO t (id, name) VALUES ('{}', 'n')", c),
        )
        .unwrap();
    }
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT * FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data.len(), 4);
    } else {
        panic!("expected Rows");
    }
}

#[test]
fn cross_session_committed_visible_to_new_txn() {
    // Two SessionContexts share the same engine. Session A commits a
    // row; session B sees it and gets the duplicate-key error when
    // attempting the same PK inside a transaction.
    let temp = TempDir::new().unwrap();
    let mut engine = Engine::init(temp.path()).unwrap();
    let mut a = SessionContext::new();
    let mut b = SessionContext::new();
    execute_sql_in_session(
        &mut engine,
        "CREATE TABLE t (id VARCHAR, name VARCHAR, PRIMARY KEY (id))",
        &mut a,
    )
    .unwrap();
    execute_sql_in_session(&mut engine, "INSERT INTO t (id, name) VALUES ('x', 'a-row')", &mut a)
        .unwrap();

    execute_sql_in_session(&mut engine, "BEGIN", &mut b).unwrap();
    let dup = execute_sql_in_session(
        &mut engine,
        "INSERT INTO t (id, name) VALUES ('x', 'b-row')",
        &mut b,
    );
    assert!(dup.is_err(), "session B must see session A's committed row");
    execute_sql_in_session(&mut engine, "ROLLBACK", &mut b).unwrap();
}
