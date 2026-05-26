//! Row-level predicate evaluation shared by all Engine read paths.
//!
//! Both `Engine::query` (read-only, used by FK validation) and
//! `Engine::execute_query`'s `select()` (the main read path) need to filter
//! rows by `WhereCondition`. Keeping the logic in one place prevents the
//! two paths from diverging on operator support — a bug that was active
//! before this module existed (`Engine::query` hard-coded equality and
//! silently mismatched `<`, `>`, `!=`, etc.).
//!
//! NOTE: `crate::parallel::matches_conditions` is a third, richer copy
//! (supports `LIKE` / `IN` / non-numeric ordering via `compare_json_values`).
//! It's intentionally not unified here because the semantics differ enough
//! that consolidation needs a deliberate design pass.

use serde_json::Value;

use super::WhereCondition;

/// True if every condition in `conditions` matches `row`. An empty
/// condition list matches everything.
pub(crate) fn matches_conditions(row: &Value, conditions: &[WhereCondition]) -> bool {
    conditions.iter().all(|cond| {
        if let Value::Object(map) = row {
            if let Some(field_value) = map.get(&cond.column) {
                compare_values(field_value, &cond.value, &cond.operator)
            } else {
                false
            }
        } else {
            false
        }
    })
}

/// Compare two JSON values under the named SQL operator.
///
/// Supports `=`/`==`, `!=`/`<>`, `<`, `<=`, `>`, `>=`. NULLs match only
/// other NULLs under equality, never under ordered operators. Unknown
/// operators return `false`.
pub(crate) fn compare_values(left: &Value, right: &Value, operator: &str) -> bool {
    if left.is_null() || right.is_null() {
        return match operator {
            "=" | "==" => left.is_null() && right.is_null(),
            "!=" | "<>" => !(left.is_null() && right.is_null()),
            _ => false,
        };
    }
    match operator {
        "=" | "==" => left == right,
        "!=" | "<>" => left != right,
        "<" => match (left.as_f64(), right.as_f64()) {
            (Some(l), Some(r)) => l < r,
            _ => match (left.as_str(), right.as_str()) {
                (Some(l), Some(r)) => l < r,
                _ => false,
            },
        },
        "<=" => match (left.as_f64(), right.as_f64()) {
            (Some(l), Some(r)) => l <= r,
            _ => match (left.as_str(), right.as_str()) {
                (Some(l), Some(r)) => l <= r,
                _ => false,
            },
        },
        ">" => match (left.as_f64(), right.as_f64()) {
            (Some(l), Some(r)) => l > r,
            _ => match (left.as_str(), right.as_str()) {
                (Some(l), Some(r)) => l > r,
                _ => false,
            },
        },
        ">=" => match (left.as_f64(), right.as_f64()) {
            (Some(l), Some(r)) => l >= r,
            _ => match (left.as_str(), right.as_str()) {
                (Some(l), Some(r)) => l >= r,
                _ => false,
            },
        },
        _ => false,
    }
}
