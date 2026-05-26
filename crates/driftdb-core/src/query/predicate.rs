//! Row-level predicate evaluation shared by every Engine read path.
//!
//! Before this module existed, the predicate logic was scattered:
//!
//! - `Engine::query` hard-coded equality and ignored `cond.operator`.
//! - `Engine::execute_query`'s sequential `select()` had its own
//!   `compare_values` supporting `=`/`!=`/`<`/`<=`/`>`/`>=` only.
//! - `ParallelExecutor::matches_conditions` (used automatically for
//!   result sets above ~1000 rows) had a third implementation that
//!   also handled `LIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`,
//!   and non-numeric ordering via `compare_json_values`.
//!
//! Net effect: the same WHERE clause returned different rows depending
//! on which path executed it, and the divergence was dataset-size
//! dependent. This module is now the single source of truth — the
//! richer parallel semantics win, since silently losing operators
//! above 1000 rows is the bug, not the floor.
//!
//! Operator coverage (canonical):
//! - `=`, `==`, `!=`, `<>`
//! - `<`, `<=`, `>`, `>=` (numeric-first, with [`compare_json_values`] fallback)
//! - `LIKE` (SQL `%` and `_` wildcards via regex)
//! - `IN`, `NOT IN` (RHS is a JSON array)
//! - `IS NULL`, `IS NOT NULL`
//!
//! Missing columns are treated as NULL — i.e., they match `IS NULL` and
//! nothing else.

use std::cmp::Ordering;

use serde_json::Value;
use tracing::warn;

use super::WhereCondition;

/// True if every condition in `conditions` matches `row`. An empty
/// condition list matches everything.
pub(crate) fn matches_conditions(row: &Value, conditions: &[WhereCondition]) -> bool {
    conditions.iter().all(|cond| matches_one(row, cond))
}

fn matches_one(row: &Value, cond: &WhereCondition) -> bool {
    // Missing column behaves as a NULL value — matches only `IS NULL`,
    // mirroring SQL three-valued logic at the WHERE-clause boundary.
    let Some(field_value) = row.get(&cond.column) else {
        return cond.operator == "IS NULL";
    };
    compare_values(field_value, &cond.value, &cond.operator)
}

/// Compare two JSON values under the named SQL operator. Returns false
/// for unknown operators (with a warn trace) so callers don't get
/// surprised by silent true-matches.
pub(crate) fn compare_values(left: &Value, right: &Value, operator: &str) -> bool {
    // NULLs short-circuit: only equality and IS [NOT] NULL meaningfully
    // compare a NULL. Ordered ops on NULL always return false (SQL
    // three-valued logic collapsed to two-valued at the WHERE boundary).
    if left.is_null() || right.is_null() {
        return match operator {
            "=" | "==" => left.is_null() && right.is_null(),
            "!=" | "<>" => !(left.is_null() && right.is_null()),
            "IS NULL" => left.is_null(),
            "IS NOT NULL" => !left.is_null(),
            _ => false,
        };
    }

    match operator {
        "=" | "==" => left == right,
        "!=" | "<>" => left != right,
        "<" => ordered_cmp(left, right) == Ordering::Less,
        "<=" => matches!(ordered_cmp(left, right), Ordering::Less | Ordering::Equal),
        ">" => ordered_cmp(left, right) == Ordering::Greater,
        ">=" => matches!(
            ordered_cmp(left, right),
            Ordering::Greater | Ordering::Equal
        ),
        "LIKE" => like_matches(left, right),
        "IN" => match right.as_array() {
            Some(array) => array.contains(left),
            None => false,
        },
        "NOT IN" => match right.as_array() {
            Some(array) => !array.contains(left),
            // SQL `x NOT IN <non-list>` is structurally ill-formed; treat
            // as unmatched rather than silently matching everything.
            None => false,
        },
        "IS NULL" => left.is_null(),
        "IS NOT NULL" => !left.is_null(),
        other => {
            warn!("predicate: unknown operator '{}'", other);
            false
        }
    }
}

/// Total ordering for JSON values, used by both predicate evaluation
/// (`<`/`>`/etc.) and ORDER BY sorting. NULLs sort *last* to match
/// SQL standard / PostgreSQL default ASC behavior.
///
/// Mixed types fall back to string representation rather than
/// "always-equal" so sorts are stable and ordered predicates produce a
/// defined answer.
pub(crate) fn compare_json_values(a: &Value, b: &Value) -> Ordering {
    use Value as V;
    match (a, b) {
        (V::Null, V::Null) => Ordering::Equal,
        // NULLs last in ASC (SQL standard default).
        (V::Null, _) => Ordering::Greater,
        (_, V::Null) => Ordering::Less,
        (V::Bool(a), V::Bool(b)) => a.cmp(b),
        (V::Number(a), V::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        },
        (V::String(a), V::String(b)) => a.cmp(b),
        (V::Array(a), V::Array(b)) => a.len().cmp(&b.len()),
        (V::Object(a), V::Object(b)) => a.len().cmp(&b.len()),
        // Heterogeneous types — fall back to string representation for
        // a stable, total order rather than collapsing to Equal.
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// Numeric-first ordering used by predicate evaluation. Tries `as_f64`
/// for both sides; if either isn't numeric, falls back to
/// [`compare_json_values`].
fn ordered_cmp(left: &Value, right: &Value) -> Ordering {
    match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => l.partial_cmp(&r).unwrap_or(Ordering::Equal),
        _ => compare_json_values(left, right),
    }
}

/// Apply SQL `LIKE`: `%` matches any sequence, `_` matches a single
/// character. Both sides must be strings; non-string sides are not a
/// match. Pattern characters are escaped before substitution to avoid
/// regex injection through user-controlled patterns.
fn like_matches(left: &Value, right: &Value) -> bool {
    let (Some(text), Some(pattern)) = (left.as_str(), right.as_str()) else {
        return false;
    };
    // Regex metacharacters that need escaping when they appear literally
    // in a LIKE pattern. Note '%' and '_' are *not* listed — they're SQL
    // wildcards translated below.
    const REGEX_METAS: &[char] = &[
        '\\', '.', '+', '*', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|',
    ];
    let mut regex_pattern = String::with_capacity(pattern.len() + 4);
    regex_pattern.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            c if REGEX_METAS.contains(&c) => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            c => regex_pattern.push(c),
        }
    }
    regex_pattern.push('$');
    regex::Regex::new(&regex_pattern)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cond(column: &str, op: &str, value: Value) -> WhereCondition {
        WhereCondition {
            column: column.to_string(),
            operator: op.to_string(),
            value,
        }
    }

    #[test]
    fn equality_and_inequality() {
        let row = json!({"x": 5});
        assert!(matches_conditions(&row, &[cond("x", "=", json!(5))]));
        assert!(matches_conditions(&row, &[cond("x", "==", json!(5))]));
        assert!(!matches_conditions(&row, &[cond("x", "=", json!(6))]));
        assert!(matches_conditions(&row, &[cond("x", "!=", json!(6))]));
        assert!(matches_conditions(&row, &[cond("x", "<>", json!(6))]));
    }

    #[test]
    fn ordered_numeric() {
        let row = json!({"x": 10});
        assert!(matches_conditions(&row, &[cond("x", "<", json!(20))]));
        assert!(!matches_conditions(&row, &[cond("x", "<", json!(10))]));
        assert!(matches_conditions(&row, &[cond("x", "<=", json!(10))]));
        assert!(matches_conditions(&row, &[cond("x", ">", json!(5))]));
        assert!(matches_conditions(&row, &[cond("x", ">=", json!(10))]));
    }

    #[test]
    fn ordered_string_fallback() {
        let row = json!({"name": "bravo"});
        assert!(matches_conditions(&row, &[cond("name", "<", json!("charlie"))]));
        assert!(matches_conditions(&row, &[cond("name", ">", json!("alpha"))]));
        assert!(!matches_conditions(&row, &[cond("name", "<", json!("alpha"))]));
    }

    #[test]
    fn like_basic() {
        let row = json!({"name": "Alice"});
        assert!(matches_conditions(&row, &[cond("name", "LIKE", json!("A%"))]));
        assert!(matches_conditions(&row, &[cond("name", "LIKE", json!("%ice"))]));
        assert!(matches_conditions(&row, &[cond("name", "LIKE", json!("Al_ce"))]));
        assert!(!matches_conditions(&row, &[cond("name", "LIKE", json!("Bob%"))]));
    }

    #[test]
    fn like_escapes_regex_metacharacters_in_pattern() {
        // The pattern contains '.' and '(' which are regex metas; in SQL
        // LIKE they're literal. Without escaping, this would match too
        // much.
        let row = json!({"file": "report.txt"});
        assert!(matches_conditions(
            &row,
            &[cond("file", "LIKE", json!("report.txt"))]
        ));
        let other = json!({"file": "reportXtxt"});
        assert!(!matches_conditions(
            &other,
            &[cond("file", "LIKE", json!("report.txt"))]
        ));
    }

    #[test]
    fn in_and_not_in() {
        let row = json!({"city": "Boston"});
        assert!(matches_conditions(
            &row,
            &[cond("city", "IN", json!(["NYC", "Boston", "LA"]))]
        ));
        assert!(!matches_conditions(
            &row,
            &[cond("city", "IN", json!(["NYC", "LA"]))]
        ));
        assert!(matches_conditions(
            &row,
            &[cond("city", "NOT IN", json!(["NYC", "LA"]))]
        ));
        // NOT IN with a non-array RHS is structurally ill-formed; we
        // intentionally do not match (rather than silently matching all).
        assert!(!matches_conditions(
            &row,
            &[cond("city", "NOT IN", json!("Boston"))]
        ));
    }

    #[test]
    fn is_null_and_missing_field() {
        let row = json!({"a": 1, "b": null});
        // Explicit NULL value.
        assert!(matches_conditions(&row, &[cond("b", "IS NULL", json!(null))]));
        assert!(!matches_conditions(
            &row,
            &[cond("b", "IS NOT NULL", json!(null))]
        ));
        // Missing column is treated as NULL.
        assert!(matches_conditions(
            &row,
            &[cond("missing", "IS NULL", json!(null))]
        ));
        assert!(!matches_conditions(
            &row,
            &[cond("missing", "=", json!(1))]
        ));
    }

    #[test]
    fn compare_json_values_nulls_last() {
        // SQL ASC default puts NULLs last.
        assert_eq!(compare_json_values(&json!(null), &json!(1)), Ordering::Greater);
        assert_eq!(compare_json_values(&json!(1), &json!(null)), Ordering::Less);
        assert_eq!(compare_json_values(&json!(null), &json!(null)), Ordering::Equal);
    }

    #[test]
    fn compare_json_values_heterogeneous() {
        // Mixed types must produce a *stable* ordering, not collapse to
        // Equal — otherwise sorts are nondeterministic.
        let a = json!(5);
        let b = json!("hello");
        assert_ne!(compare_json_values(&a, &b), Ordering::Equal);
    }
}
