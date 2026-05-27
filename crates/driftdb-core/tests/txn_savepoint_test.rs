//! Slice 4 of the PG transaction semantics arc: real SAVEPOINT
//! support. SAVEPOINT name records a checkpoint; ROLLBACK TO SAVEPOINT
//! restores the buffer to that point AND clears the abort flag;
//! RELEASE SAVEPOINT discards a savepoint AND its nested descendants.

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

// ─── Basic SAVEPOINT mechanics ────────────────────────────────────────

#[test]
fn rollback_to_savepoint_discards_work_since_savepoint() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n')").unwrap();
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    // Only 'a' should be present; 'b' was rolled back.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a"]);
}

#[test]
fn release_savepoint_keeps_work() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "RELEASE SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    // 'a' is still there — RELEASE only discards the savepoint marker,
    // not the work it covered.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a"]);
}

#[test]
fn rollback_to_savepoint_then_continue() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'discarded')").unwrap();
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    // Transaction is still open; we can continue.
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'kept')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["b"]);
}

// ─── Recovery from abort ──────────────────────────────────────────────

#[test]
fn rollback_to_savepoint_recovers_aborted_transaction() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'committed')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'kept')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    // Trigger a PK violation → transaction aborts.
    let _ = run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'dup')");
    assert!(ctx.aborted);
    // ROLLBACK TO SAVEPOINT clears the abort and discards the
    // violating INSERT.
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    assert!(!ctx.aborted, "abort must be cleared by ROLLBACK TO");
    // Transaction continues; the earlier INSERT 'b' is still pending.
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('c', 'also-kept')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let mut expected = vec!["a", "b", "c"];
    expected.sort();
    assert_eq!(select_ids(&mut engine, &mut ctx), expected);
}

#[test]
fn rollback_to_savepoint_recovers_from_syntax_error() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'first')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    let _ = run(&mut engine, &mut ctx, "SELECT * FROM"); // syntax error
    assert!(ctx.aborted);
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    assert!(!ctx.aborted);
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'after-recovery')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let mut expected = vec!["a", "b"];
    expected.sort();
    assert_eq!(select_ids(&mut engine, &mut ctx), expected);
}

// ─── Nested savepoints ────────────────────────────────────────────────

#[test]
fn rollback_to_outer_savepoint_discards_inner() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp_outer").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'discard')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp_inner").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'discard')").unwrap();
    // Rolling back to the outer savepoint discards both inserts.
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp_outer").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT * FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        assert!(data.is_empty(), "both nested inserts discarded");
    } else {
        panic!("expected Rows");
    }
}

#[test]
fn release_outer_savepoint_releases_inner_too() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp_outer").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'kept')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp_inner").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'kept')").unwrap();
    // RELEASE of outer also releases inner (it was nested inside).
    run(&mut engine, &mut ctx, "RELEASE SAVEPOINT sp_outer").unwrap();
    // Both savepoints gone; ROLLBACK TO either now errors.
    let err = run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp_inner");
    assert!(err.is_err());
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

#[test]
fn rollback_to_inner_keeps_outer_work_to_outer() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'before-outer')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp_outer").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'between')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp_inner").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('c', 'after-inner')").unwrap();
    // Rollback to inner: discard 'c' only; 'a' and 'b' stay.
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp_inner").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a", "b"]);
}

// ─── Repeated ROLLBACK TO same savepoint ──────────────────────────────

#[test]
fn rollback_to_savepoint_keeps_savepoint_for_reuse() {
    // PG behavior: a savepoint stays available after ROLLBACK TO.
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'discarded')").unwrap();
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'second-try')").unwrap();
    // Roll back to the same savepoint a second time.
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('c', 'third-try')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    // Only 'c' should remain.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["c"]);
}

// ─── Duplicate savepoint names ────────────────────────────────────────

#[test]
fn duplicate_savepoint_name_shadows_inner() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'first')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp").unwrap(); // duplicate name
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'second')").unwrap();
    // ROLLBACK TO resolves to the most recent (innermost) sp.
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp").unwrap();
    // 'b' discarded; 'a' kept.
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a"]);
}

// ─── Error cases ──────────────────────────────────────────────────────

#[test]
fn savepoint_outside_transaction_errors() {
    let (_t, mut engine, mut ctx) = setup();
    let res = run(&mut engine, &mut ctx, "SAVEPOINT sp1");
    assert!(res.is_err());
    let msg = res.unwrap_err().to_string();
    assert!(
        msg.contains("transaction"),
        "error must mention transaction context: {}",
        msg
    );
}

#[test]
fn rollback_to_nonexistent_savepoint_errors() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT ghost");
    assert!(res.is_err());
    let msg = res.unwrap_err().to_string();
    assert!(
        msg.contains("does not exist"),
        "error must mention nonexistent savepoint: {}",
        msg
    );
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

#[test]
fn release_nonexistent_savepoint_errors() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(&mut engine, &mut ctx, "RELEASE SAVEPOINT ghost");
    assert!(res.is_err());
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

#[test]
fn savepoint_in_aborted_txn_blocked_by_gate() {
    // Per slice 1's abort gate, only ROLLBACK and COMMIT are allowed
    // when aborted. SAVEPOINT is not — you must ROLLBACK TO an
    // existing savepoint to recover before creating new ones.
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'committed')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let _ = run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'dup')");
    assert!(ctx.aborted);
    let savepoint_attempt = run(&mut engine, &mut ctx, "SAVEPOINT sp1");
    assert!(savepoint_attempt.is_err());
    let msg = savepoint_attempt.unwrap_err().to_string();
    assert!(msg.contains("aborted"));
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

// ─── COMMIT and ROLLBACK discard savepoints cleanly ───────────────────

#[test]
fn commit_discards_savepoints() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'kept')").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp2").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'kept')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    // Both inserts committed; savepoints discarded.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a", "b"]);
}

#[test]
fn full_rollback_discards_savepoints() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('committed', 'pre')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp1").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'kept')").unwrap();
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap(); // full rollback
    // Only the pre-BEGIN row remains.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["committed"]);
}

// ─── Savepoint preserves alphabet of writes correctly ─────────────────

#[test]
fn savepoint_snapshot_preserves_delete_then_insert_pattern() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'initial')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "DELETE FROM t WHERE id = 'a'").unwrap();
    run(&mut engine, &mut ctx, "SAVEPOINT sp").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'replacement')").unwrap();
    run(&mut engine, &mut ctx, "ROLLBACK TO SAVEPOINT sp").unwrap();
    // After rolling back to sp: buffer has just the DELETE.
    // Insert 'a' again now — should succeed because buffer-view says
    // 'a' is deleted.
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'second-try')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT name FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["name"], serde_json::json!("second-try"));
    } else {
        panic!("expected Rows");
    }
}
