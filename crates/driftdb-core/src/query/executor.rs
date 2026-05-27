use serde_json::json;

use super::{AsOf, Query, QueryResult, WhereCondition};
use crate::engine::Engine;
use crate::errors::Result;
use crate::events::Event;
use crate::optimizer::PlanStep;
use crate::parallel::{ParallelConfig, ParallelExecutor};

impl Engine {
    pub fn execute_query(&mut self, query: Query) -> Result<QueryResult> {
        match query {
            Query::CreateTable {
                name,
                primary_key,
                indexed_columns,
            } => {
                self.create_table(&name, &primary_key, indexed_columns)?;
                Ok(QueryResult::Success {
                    message: format!("Table '{}' created", name),
                })
            }
            Query::Insert { table, data } => {
                let pk_field = self.get_table_primary_key(&table)?;
                let primary_key = data
                    .get(&pk_field)
                    .ok_or_else(|| {
                        crate::errors::DriftError::InvalidQuery(format!(
                            "Missing primary key field '{}'",
                            pk_field
                        ))
                    })?
                    .clone();

                // Check if primary key already exists
                let existing = self.select(
                    &table,
                    vec![super::WhereCondition {
                        column: pk_field.clone(),
                        operator: "=".to_string(),
                        value: primary_key.clone(),
                    }],
                    None,
                    Some(1),
                )?;

                if !existing.is_empty() {
                    return Err(crate::errors::DriftError::InvalidQuery(format!(
                        "Primary key violation: {} already exists",
                        primary_key
                    )));
                }

                let event = Event::new_insert(table.clone(), primary_key, data);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Inserted with sequence {}", seq),
                })
            }
            Query::Patch {
                table,
                primary_key,
                updates,
            } => {
                let event = Event::new_patch(table.clone(), primary_key, updates);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Patched with sequence {}", seq),
                })
            }
            Query::SoftDelete { table, primary_key } => {
                let event = Event::new_soft_delete(table.clone(), primary_key);
                let seq = self.apply_event(event)?;

                Ok(QueryResult::Success {
                    message: format!("Soft deleted with sequence {}", seq),
                })
            }
            Query::Select {
                table,
                conditions,
                as_of,
                limit,
            } => {
                let rows = self.select(&table, conditions, as_of, limit)?;
                Ok(QueryResult::Rows { data: rows })
            }
            Query::ShowDrift { table, primary_key } => {
                let events = self.get_drift_history(&table, primary_key)?;
                Ok(QueryResult::DriftHistory { events })
            }
            Query::Snapshot { table } => {
                self.create_snapshot(&table)?;
                Ok(QueryResult::Success {
                    message: format!("Snapshot created for table '{}'", table),
                })
            }
            Query::Compact { table } => {
                self.compact_table(&table)?;
                Ok(QueryResult::Success {
                    message: format!("Table '{}' compacted", table),
                })
            }
        }
    }

    fn select(
        &self,
        table: &str,
        conditions: Vec<WhereCondition>,
        as_of: Option<AsOf>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        // Ask the optimizer for a plan. The plan tells us:
        //   (a) which access method to use (index lookup vs. table scan);
        //   (b) the order in which residual `Filter` predicates should
        //       be evaluated against each candidate row.
        //
        // Both dimensions are honored below for single-table SELECTs with
        // no time travel. Time-travel queries skip the indexed path
        // (indexes track current state only) but still benefit from
        // ordered predicate evaluation.
        let plan_query = Query::Select {
            table: table.to_string(),
            conditions: conditions.clone(),
            as_of: as_of.clone(),
            limit,
        };
        let plan = self.query_optimizer.optimize(&plan_query).ok();

        let no_time_travel = matches!(as_of, None | Some(AsOf::Now));
        if no_time_travel {
            if let Some(plan) = plan.as_ref() {
                if let Some(rows) =
                    self.try_indexed_access_path(table, &conditions, plan, limit)?
                {
                    return Ok(rows);
                }
            }
        }

        // Full-scan path. Pull residual predicates from the plan in the
        // optimizer's chosen order. If the plan's access step was an
        // IndexLookup/IndexScan but we're not honoring it (e.g. time
        // travel), that step "absorbed" some predicates — equality for
        // IndexLookup, range bounds for IndexScan — that are therefore
        // absent from the Filter list. We re-prepend them so they still
        // get applied. Falls back to source order when no plan was
        // produced.
        let ordered_conditions: Vec<WhereCondition> = if let Some(p) = plan.as_ref() {
            let mut ordered = Vec::with_capacity(conditions.len());
            for step in &p.steps {
                match step {
                    PlanStep::IndexLookup { index, .. } => {
                        if let Some(c) = conditions
                            .iter()
                            .find(|c| c.column == *index && (c.operator == "=" || c.operator == "=="))
                        {
                            ordered.push(c.clone());
                        }
                    }
                    PlanStep::IndexScan { index, .. } => {
                        // Re-add every range predicate on the indexed
                        // column. There can be more than one (e.g.
                        // `age > 30 AND age < 50` → two predicates), all
                        // folded into a single IndexScan step.
                        for c in conditions.iter().filter(|c| {
                            c.column == *index
                                && matches!(c.operator.as_str(), ">" | ">=" | "<" | "<=")
                        }) {
                            ordered.push(c.clone());
                        }
                    }
                    _ => {}
                }
            }
            ordered.extend(predicates_from_filter_steps(p));
            if ordered.is_empty() {
                conditions.clone()
            } else {
                ordered
            }
        } else {
            conditions.clone()
        };

        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

        let sequence = match as_of {
            Some(AsOf::Sequence(seq)) => Some(seq),
            Some(AsOf::Timestamp(ts)) => {
                let events = storage.read_all_events()?;
                events
                    .iter()
                    .filter(|e| e.timestamp <= ts)
                    .map(|e| e.sequence)
                    .max()
            }
            Some(AsOf::Now) | None => None,
        };

        let state = storage.reconstruct_state_at(sequence)?;

        // Use parallel execution for large datasets
        if state.len() > 1000 {
            // Create parallel executor with default config
            let parallel_executor = ParallelExecutor::new(ParallelConfig::default())?;

            // Convert state to format expected by parallel executor
            let data: Vec<(serde_json::Value, serde_json::Value)> = state
                .into_iter()
                .map(|(pk, row)| (serde_json::Value::String(pk), row))
                .collect();

            // Execute query in parallel
            parallel_executor.parallel_select(data, &ordered_conditions, limit)
        } else {
            // Use sequential execution for small datasets
            let mut results: Vec<serde_json::Value> = state
                .into_values()
                .filter(|row| super::predicate::matches_conditions(row, &ordered_conditions))
                .collect();

            if let Some(limit) = limit {
                results.truncate(limit);
            }

            Ok(results)
        }
    }

    /// If the optimizer chose an `IndexLookup` or `IndexScan`, fetch the
    /// candidate PKs through the index and apply residual predicates.
    ///
    /// - `IndexLookup` resolves to `Engine::lookup_by_index` (point query).
    /// - `IndexScan` resolves to `Engine::range_by_index` (range walk).
    ///
    /// Residual predicates come from the plan's `Filter` steps in the
    /// optimizer's chosen order (see the second wiring slice). They
    /// exclude whatever the access step already covered.
    ///
    /// Returns `Ok(None)` to signal "fall back to full scan" for any
    /// situation we can't honor cleanly: plan didn't use an index, plan
    /// has a shape we don't recognize, or the index call errored
    /// (likely a transient race with index changes). We never surface
    /// index-side errors to the user — full scan always produces a
    /// correct answer.
    fn try_indexed_access_path(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        plan: &crate::optimizer::QueryPlan,
        limit: Option<usize>,
    ) -> Result<Option<Vec<serde_json::Value>>> {
        if !plan.uses_index {
            return Ok(None);
        }
        let access = plan.steps.iter().find(|s| {
            matches!(
                s,
                PlanStep::IndexLookup { .. } | PlanStep::IndexScan { .. }
            )
        });
        let Some(access) = access else {
            return Ok(None);
        };

        let pks: Vec<String> = match access {
            PlanStep::IndexLookup { index, .. } => {
                // Pull the JSON-typed value from the original condition
                // list rather than reparsing the plan's stringified key
                // (which would lose JSON type info, especially around
                // string-vs-number ambiguity).
                let Some(eq_cond) = conditions
                    .iter()
                    .find(|c| c.column == *index && (c.operator == "=" || c.operator == "=="))
                else {
                    return Ok(None);
                };
                match self.lookup_by_index(table, index, &eq_cond.value) {
                    Ok(set) => set.into_iter().collect(),
                    Err(_) => return Ok(None),
                }
            }
            PlanStep::IndexScan {
                index, start, end, ..
            } => {
                let start_tuple = start.as_ref().map(|b| (&b.value, b.inclusive));
                let end_tuple = end.as_ref().map(|b| (&b.value, b.inclusive));
                match self.range_by_index(table, index, start_tuple, end_tuple) {
                    Ok(set) => set.into_iter().collect(),
                    Err(_) => return Ok(None),
                }
            }
            _ => unreachable!(),
        };

        // Reconstruct current state once; PK lookups are O(1) from here.
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;
        let state = storage.reconstruct_state_at(None)?;

        let residual = predicates_from_filter_steps(plan);

        let mut results = Vec::new();
        for pk in pks {
            let Some(row) = state.get(&pk) else { continue };
            if super::predicate::matches_conditions(row, &residual) {
                results.push(row.clone());
                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break;
                    }
                }
            }
        }
        Ok(Some(results))
    }

    fn get_drift_history(
        &self,
        table: &str,
        primary_key: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>> {
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

        let events = storage.read_all_events()?;
        let pk_str = primary_key.to_string();

        let history: Vec<serde_json::Value> = events
            .into_iter()
            .filter(|e| e.primary_key == pk_str)
            .map(|e| {
                json!({
                    "sequence": e.sequence,
                    "timestamp": e.timestamp.to_string(),
                    "event_type": format!("{:?}", e.event_type),
                    "payload": e.payload,
                })
            })
            .collect();

        Ok(history)
    }

    pub fn get_table_primary_key(&self, table: &str) -> Result<String> {
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;
        Ok(storage.schema().primary_key.clone())
    }

    pub fn get_table_columns(&self, table: &str) -> Result<Vec<String>> {
        let storage = self
            .tables
            .get(table)
            .ok_or_else(|| crate::errors::DriftError::TableNotFound(table.to_string()))?;

        // Get columns from schema
        let schema = storage.schema();

        // Return all column names from the schema
        Ok(schema.columns.iter().map(|c| c.name.clone()).collect())
    }

}

/// Extract residual predicates from a plan in the order the optimizer
/// chose. Walks `plan.steps`, picking each `PlanStep::Filter`'s predicate.
/// The optimizer is responsible for ordering Filter steps cheapest-first.
fn predicates_from_filter_steps(plan: &crate::optimizer::QueryPlan) -> Vec<WhereCondition> {
    plan.steps
        .iter()
        .filter_map(|s| match s {
            PlanStep::Filter { predicate, .. } => Some(predicate.clone()),
            _ => None,
        })
        .collect()
}
