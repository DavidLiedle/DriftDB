//! Foreign-key registration and validation for SQL DML.
//!
//! This module holds the FK registry that `sql_bridge` populates during
//! `CREATE TABLE` and reads from during `INSERT` / `UPDATE` / `DELETE`.
//! It's the working FK system that was previously duplicated inside the
//! driftdb-server crate; the server's version had no access to the
//! sql_bridge's `CREATE TABLE` parser, so CLI users got FK constraints
//! that parsed silently and never enforced. Lifting the registry into
//! core fixes that asymmetry — every DML path now sees the same FKs.
//!
//! Two things are deliberately *not* in here:
//!
//! - `ConstraintManager` in `crate::constraints`. That module has the
//!   right architectural shape (manager + validate_insert/update/delete
//!   hooks) but its `validate_foreign_key` is a placeholder that returns
//!   Ok without checking parent rows. Migrating to that path is a
//!   separate piece of work; for now the simpler per-registry approach
//!   matches what was already shipping and tested.
//!
//! - Cascading actions (ON DELETE CASCADE, etc.). Those are parsed by
//!   sql_bridge but neither implementation actually performs them. The
//!   current behaviour is RESTRICT — block the delete if a child row
//!   references the parent.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde_json::Value;

use crate::engine::Engine;
use crate::errors::{DriftError, Result};

/// A single foreign-key column reference.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Column in the *child* table that holds the FK value.
    pub column: String,
    /// Referenced parent table name.
    pub ref_table: String,
    /// Referenced column in the parent table.
    pub ref_column: String,
}

/// Process-wide registry mapping child table → its outgoing FKs.
/// The registry persists across `Engine` instances within a single
/// process, which matches the previous server-side behaviour.
fn registry() -> &'static RwLock<HashMap<String, Vec<ForeignKey>>> {
    static REGISTRY: std::sync::OnceLock<RwLock<HashMap<String, Vec<ForeignKey>>>> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register all FK constraints declared on `table_name`. Replaces any
/// previously-registered FKs for that table.
pub fn register(table_name: &str, fks: Vec<ForeignKey>) {
    let mut reg = registry().write();
    if fks.is_empty() {
        reg.remove(table_name);
    } else {
        reg.insert(table_name.to_string(), fks);
    }
}

/// Forget all FK constraints declared on `table_name` (used by DROP TABLE).
pub fn forget(table_name: &str) {
    registry().write().remove(table_name);
}

/// Validate FK constraints before inserting `doc` into `table_name`.
///
/// For each FK column in `doc`, verify that the referenced parent row
/// exists. NULL FK values are allowed (no referential check needed —
/// matches SQL `MATCH SIMPLE` semantics, the PostgreSQL default).
pub fn validate_insert(engine: &Engine, table_name: &str, doc: &Value) -> Result<()> {
    let fks = match registry().read().get(table_name) {
        Some(v) => v.clone(),
        None => return Ok(()),
    };
    if fks.is_empty() {
        return Ok(());
    }

    for fk in &fks {
        let fk_val = match doc {
            Value::Object(map) => map.get(&fk.column).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        if fk_val.is_null() {
            continue;
        }
        // Lookup: scan the referenced table for a row whose ref_column matches.
        let ref_data = engine.get_table_data(&fk.ref_table).map_err(|e| {
            DriftError::Validation(format!(
                "Foreign key check: failed to read table '{}': {}",
                fk.ref_table, e
            ))
        })?;
        let found = ref_data.iter().any(|row| match row {
            Value::Object(map) => map.get(&fk.ref_column).map(|v| v == &fk_val).unwrap_or(false),
            _ => false,
        });
        if !found {
            return Err(DriftError::Validation(format!(
                "Foreign key violation: value {} in column '{}' does not exist in '{}'.'{}'",
                fk_val, fk.column, fk.ref_table, fk.ref_column
            )));
        }
    }
    Ok(())
}

/// Validate FK constraints before deleting `deleted_row` from
/// `table_name`. Errors if any other table has a FK row referencing
/// the parent key being removed (RESTRICT semantics).
pub fn validate_delete(engine: &Engine, table_name: &str, deleted_row: &Value) -> Result<()> {
    let reg = registry().read();
    for (referencing_table, fks) in reg.iter() {
        for fk in fks {
            if fk.ref_table != table_name {
                continue;
            }
            // Pull the parent-side key value from the row being deleted.
            let pk_val = match deleted_row {
                Value::Object(map) => map.get(&fk.ref_column).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            if pk_val.is_null() {
                continue;
            }
            let ref_data = engine.get_table_data(referencing_table).map_err(|e| {
                DriftError::Validation(format!("Foreign key check failed: {}", e))
            })?;
            let blocked = ref_data.iter().any(|row| match row {
                Value::Object(map) => map.get(&fk.column).map(|v| v == &pk_val).unwrap_or(false),
                _ => false,
            });
            if blocked {
                return Err(DriftError::Validation(format!(
                    "Foreign key violation: cannot delete row from '{}' because '{}' has a \
                     reference via column '{}'",
                    table_name, referencing_table, fk.column
                )));
            }
        }
    }
    Ok(())
}

/// Validate FK constraints before updating an existing row. The check
/// only fires for FK columns whose value actually changed — matches
/// PostgreSQL behaviour where an UPDATE that leaves an FK column alone
/// doesn't require re-validating the parent reference.
pub fn validate_update(
    engine: &Engine,
    table_name: &str,
    old_row: &Value,
    new_row: &Value,
) -> Result<()> {
    let fks = match registry().read().get(table_name) {
        Some(v) => v.clone(),
        None => return Ok(()),
    };
    if fks.is_empty() {
        return Ok(());
    }

    for fk in &fks {
        let old_val = match old_row {
            Value::Object(map) => map.get(&fk.column),
            _ => None,
        };
        let new_val = match new_row {
            Value::Object(map) => map.get(&fk.column),
            _ => None,
        };
        if old_val == new_val {
            continue;
        }
        let Some(new_val) = new_val else {
            continue;
        };
        if new_val.is_null() {
            continue;
        }
        let ref_data = engine.get_table_data(&fk.ref_table).map_err(|e| {
            DriftError::Validation(format!(
                "Foreign key check: failed to read table '{}': {}",
                fk.ref_table, e
            ))
        })?;
        let found = ref_data.iter().any(|row| match row {
            Value::Object(map) => map
                .get(&fk.ref_column)
                .map(|v| v == new_val)
                .unwrap_or(false),
            _ => false,
        });
        if !found {
            return Err(DriftError::Validation(format!(
                "Foreign key violation: value {} in column '{}' does not exist in '{}'.'{}'",
                new_val, fk.column, fk.ref_table, fk.ref_column
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn new_engine() -> (TempDir, Engine) {
        let tmp = TempDir::new().unwrap();
        let engine = Engine::init(tmp.path()).unwrap();
        (tmp, engine)
    }

    // Use unique table names per test because `registry()` is process-wide
    // static state — parallel test execution would otherwise let one test's
    // `register` race with another's `forget`. Tests don't share data across
    // their Engines, so a per-test namespace gives full isolation.

    #[test]
    fn insert_validates_parent_exists() {
        let (_t, mut engine) = new_engine();
        engine
            .create_table("parent_ins", "id", vec!["id".to_string()])
            .unwrap();
        engine
            .create_table("child_ins", "id", vec!["id".to_string()])
            .unwrap();
        register(
            "child_ins",
            vec![ForeignKey {
                column: "parent_id".to_string(),
                ref_table: "parent_ins".to_string(),
                ref_column: "id".to_string(),
            }],
        );

        engine
            .insert_record("parent_ins", json!({"id": "p1"}))
            .unwrap();

        assert!(validate_insert(&engine, "child_ins", &json!({"id": "c1", "parent_id": "p1"}))
            .is_ok());

        let err = validate_insert(
            &engine,
            "child_ins",
            &json!({"id": "c2", "parent_id": "missing"}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("Foreign key violation"));

        // NULL FK → OK (MATCH SIMPLE).
        assert!(
            validate_insert(&engine, "child_ins", &json!({"id": "c3", "parent_id": null})).is_ok()
        );

        forget("child_ins");
    }

    #[test]
    fn delete_blocks_if_referenced() {
        let (_t, mut engine) = new_engine();
        engine
            .create_table("parent_del", "id", vec!["id".to_string()])
            .unwrap();
        engine
            .create_table("child_del", "id", vec!["id".to_string()])
            .unwrap();
        register(
            "child_del",
            vec![ForeignKey {
                column: "parent_id".to_string(),
                ref_table: "parent_del".to_string(),
                ref_column: "id".to_string(),
            }],
        );

        engine
            .insert_record("parent_del", json!({"id": "p1"}))
            .unwrap();
        engine
            .insert_record("child_del", json!({"id": "c1", "parent_id": "p1"}))
            .unwrap();

        let err = validate_delete(&engine, "parent_del", &json!({"id": "p1"})).unwrap_err();
        assert!(err.to_string().contains("Foreign key violation"));

        forget("child_del");
    }

    #[test]
    fn update_only_validates_when_fk_changes() {
        let (_t, mut engine) = new_engine();
        engine
            .create_table("parent_upd", "id", vec!["id".to_string()])
            .unwrap();
        engine
            .create_table("child_upd", "id", vec!["id".to_string()])
            .unwrap();
        register(
            "child_upd",
            vec![ForeignKey {
                column: "parent_id".to_string(),
                ref_table: "parent_upd".to_string(),
                ref_column: "id".to_string(),
            }],
        );

        engine
            .insert_record("parent_upd", json!({"id": "p1"}))
            .unwrap();

        let old = json!({"id": "c1", "parent_id": "p1", "qty": 1});
        let new_same_fk = json!({"id": "c1", "parent_id": "p1", "qty": 2});
        assert!(validate_update(&engine, "child_upd", &old, &new_same_fk).is_ok());

        let new_bad_fk = json!({"id": "c1", "parent_id": "missing", "qty": 1});
        assert!(validate_update(&engine, "child_upd", &old, &new_bad_fk).is_err());

        forget("child_upd");
    }
}
