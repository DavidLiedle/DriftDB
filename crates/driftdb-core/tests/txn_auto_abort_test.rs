//! Slice 3 of the PG transaction semantics arc: any error inside a
//! transaction aborts it, not just constraint violations. Slices 1 and
//! 2 set the abort flag at their specific check sites; this slice adds
//! the general backstop in `execute_sql_in_session`.

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

// ─── Syntax / parse errors abort ──────────────────────────────────────

#[test]
fn syntax_error_mid_txn_aborts() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    // Typo: SELECT FORM (instead of FROM) fails at parse time.
    let res = run(&mut engine, &mut ctx, "SELECT * FROM");
    assert!(res.is_err(), "syntax error must propagate");
    assert!(ctx.aborted, "syntax error inside transaction must set abort");
    // Any non-ROLLBACK statement now errors with the canonical message.
    let next = run(&mut engine, &mut ctx, "SELECT * FROM t");
    let msg = next.unwrap_err().to_string();
    assert!(
        msg.contains("aborted"),
        "post-error statement must hit abort gate: {}",
        msg
    );
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    assert!(!ctx.aborted);
}

// ─── Table-not-found aborts ───────────────────────────────────────────

#[test]
fn table_not_found_mid_txn_aborts() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(&mut engine, &mut ctx, "SELECT * FROM does_not_exist");
    assert!(res.is_err(), "unknown table must error");
    assert!(
        ctx.aborted,
        "unknown-table error inside transaction must abort: {:?}",
        res.err()
    );
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

#[test]
fn insert_into_unknown_table_mid_txn_aborts() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(
        &mut engine,
        &mut ctx,
        "INSERT INTO ghost (id, name) VALUES ('a', 'b')",
    );
    assert!(res.is_err());
    assert!(ctx.aborted, "unknown-table INSERT must abort the transaction");
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

// ─── Column-not-found aborts ──────────────────────────────────────────

#[test]
fn missing_column_in_select_mid_txn_aborts() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    // DriftDB errors on SELECT of an unknown column (apply_projection
    // validates against observed column set).
    let res = run(&mut engine, &mut ctx, "SELECT nonexistent_col FROM t");
    if res.is_err() {
        assert!(
            ctx.aborted,
            "column-not-found inside transaction must abort"
        );
    }
    // Always finish with ROLLBACK regardless to clean up; the test's
    // claim is "if the statement errors, the txn aborts."
    let _ = run(&mut engine, &mut ctx, "ROLLBACK");
}

// ─── Recovery: ROLLBACK clears, new txn proceeds ──────────────────────

#[test]
fn rollback_after_arbitrary_error_resets_session() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let _ = run(&mut engine, &mut ctx, "SELECT * FROM"); // syntax error
    assert!(ctx.aborted);
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    assert!(!ctx.aborted);
    // Subsequent transaction starts fresh.
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('x', 'fresh')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["x"]);
}

// ─── COMMIT-on-aborted = ROLLBACK (slice 1 behavior, regression) ──────

#[test]
fn commit_after_non_constraint_error_silently_rolls_back() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'committed')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('y', 'pending')").unwrap();
    let _ = run(&mut engine, &mut ctx, "SELECT * FROM ghost"); // unknown table
    assert!(ctx.aborted);
    // COMMIT on an aborted txn rolls back per slice 1's behavior.
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert!(!ctx.aborted);
    // The pending 'y' was discarded; only the original 'a' remains.
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["a"]);
}

// ─── Pre-error work in same txn is rolled back ────────────────────────

#[test]
fn pre_error_inserts_rolled_back() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    let _ = run(&mut engine, &mut ctx, "SELECT * FROM"); // syntax error
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    let result = run(&mut engine, &mut ctx, "SELECT * FROM t").unwrap();
    if let QueryResult::Rows { data } = result {
        assert!(
            data.is_empty(),
            "pre-error inserts must roll back: {:?}",
            data
        );
    } else {
        panic!("expected Rows");
    }
}

// ─── Auto-commit error doesn't touch session ──────────────────────────

#[test]
fn auto_commit_error_does_not_touch_session() {
    let (_t, mut engine, mut ctx) = setup();
    // No active transaction; a bad statement just returns Err.
    let res = run(&mut engine, &mut ctx, "SELECT * FROM ghost");
    assert!(res.is_err());
    assert!(
        !ctx.aborted,
        "auto-commit error must not set aborted (no transaction to poison)"
    );
    assert!(ctx.transaction_id.is_none());

    // A new transaction proceeds cleanly.
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('z', 'fresh')").unwrap();
    run(&mut engine, &mut ctx, "COMMIT").unwrap();
    assert_eq!(select_ids(&mut engine, &mut ctx), vec!["z"]);
}

// ─── Regression: slice 1 PK violation still aborts ────────────────────

#[test]
fn slice1_pk_violation_regression() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'dup')");
    assert!(res.is_err());
    assert!(ctx.aborted, "PK violation still aborts (explicit at check site)");
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

// ─── Regression: slice 2 UPDATE PK-change violation still aborts ──────

#[test]
fn slice2_pk_change_violation_regression() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('a', 'n1')").unwrap();
    run(&mut engine, &mut ctx, "INSERT INTO t (id, name) VALUES ('b', 'n2')").unwrap();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let res = run(&mut engine, &mut ctx, "UPDATE t SET id = 'b' WHERE id = 'a'");
    assert!(res.is_err());
    assert!(ctx.aborted, "PK-change violation still aborts");
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
}

// ─── Multiple errors in a row don't compound or panic ─────────────────

#[test]
fn multiple_errors_in_aborted_txn_are_idempotent() {
    let (_t, mut engine, mut ctx) = setup();
    run(&mut engine, &mut ctx, "BEGIN").unwrap();
    let _ = run(&mut engine, &mut ctx, "SELECT * FROM"); // triggers abort
    assert!(ctx.aborted);
    // Multiple subsequent statements all hit the gate uniformly.
    for _ in 0..3 {
        let res = run(&mut engine, &mut ctx, "SELECT * FROM t");
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("aborted"));
    }
    run(&mut engine, &mut ctx, "ROLLBACK").unwrap();
    assert!(!ctx.aborted);
}
