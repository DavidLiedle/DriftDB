//! Query optimizer with cost-based planning
//!
//! Optimizes query execution using:
//! - Statistics-based cost estimation
//! - Index selection
//! - Join order optimization
//! - Predicate pushdown
//! - Query plan caching

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::errors::{DriftError, Result};
use crate::index_strategies::IndexType;
use crate::query::{AsOf, Query, WhereCondition};

/// Query execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub steps: Vec<PlanStep>,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
    pub uses_index: bool,
    pub cacheable: bool,
}

/// A bound on a range scan. `inclusive == true` means the bound's value
/// itself is part of the range (`>=` or `<=`); `false` excludes it (`>`
/// or `<`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeBound {
    pub value: serde_json::Value,
    pub inclusive: bool,
}


/// Combine an existing lower bound with a new candidate, keeping the
/// stricter (greater value, or equal-and-exclusive). Used to fold
/// multiple `>`/`>=` predicates on the same column into one IndexScan.
fn tighter_start(
    existing: Option<RangeBound>,
    candidate_value: serde_json::Value,
    candidate_inclusive: bool,
) -> Option<RangeBound> {
    let candidate = RangeBound {
        value: candidate_value,
        inclusive: candidate_inclusive,
    };
    match existing {
        None => Some(candidate),
        Some(cur) => {
            let ord = crate::query::predicate::compare_json_values(&candidate.value, &cur.value);
            use std::cmp::Ordering;
            match ord {
                Ordering::Greater => Some(candidate),
                Ordering::Less => Some(cur),
                // Equal values: exclusive bound is stricter than inclusive.
                Ordering::Equal => {
                    if !candidate.inclusive {
                        Some(candidate)
                    } else {
                        Some(cur)
                    }
                }
            }
        }
    }
}

/// Combine an existing upper bound with a new candidate, keeping the
/// stricter (smaller value, or equal-and-exclusive).
fn tighter_end(
    existing: Option<RangeBound>,
    candidate_value: serde_json::Value,
    candidate_inclusive: bool,
) -> Option<RangeBound> {
    let candidate = RangeBound {
        value: candidate_value,
        inclusive: candidate_inclusive,
    };
    match existing {
        None => Some(candidate),
        Some(cur) => {
            let ord = crate::query::predicate::compare_json_values(&candidate.value, &cur.value);
            use std::cmp::Ordering;
            match ord {
                Ordering::Less => Some(candidate),
                Ordering::Greater => Some(cur),
                Ordering::Equal => {
                    if !candidate.inclusive {
                        Some(candidate)
                    } else {
                        Some(cur)
                    }
                }
            }
        }
    }
}

/// Individual step in query plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanStep {
    /// Full table scan
    TableScan {
        table: String,
        estimated_rows: usize,
        cost: f64,
    },
    /// Index scan with range bounds. Either bound may be `None` for a
    /// half-open range (`age > 30` with no upper limit). Bounds carry the
    /// JSON value AND inclusivity so the executor knows whether `> 30`
    /// (exclusive) or `>= 30` (inclusive) was meant.
    IndexScan {
        table: String,
        index: String,
        start: Option<RangeBound>,
        end: Option<RangeBound>,
        estimated_rows: usize,
        cost: f64,
    },
    /// Index lookup (point query)
    IndexLookup {
        table: String,
        index: String,
        key: String,
        estimated_rows: usize,
        cost: f64,
    },
    /// Filter rows based on predicate
    Filter {
        predicate: WhereCondition,
        selectivity: f64,
        cost: f64,
    },
    /// Sort rows
    Sort {
        column: String,
        ascending: bool,
        estimated_rows: usize,
        cost: f64,
    },
    /// Limit results
    Limit { count: usize, cost: f64 },
    /// Time travel to specific version
    TimeTravel { as_of: AsOf, cost: f64 },
    /// Load snapshot
    SnapshotLoad { sequence: u64, cost: f64 },
    /// Replay events from WAL
    EventReplay {
        from_sequence: u64,
        to_sequence: u64,
        estimated_events: usize,
        cost: f64,
    },
}

/// Table statistics for cost estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: usize,
    pub column_count: usize,
    pub avg_row_size: usize,
    pub total_size_bytes: u64,
    pub data_size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
    pub column_statistics: HashMap<String, ColumnStatistics>,
    pub index_stats: HashMap<String, IndexStatistics>,
    pub last_updated: u64,
    pub collection_method: String,
    pub collection_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub distinct_values: usize,
    pub null_count: usize,
    pub min_value: Option<serde_json::Value>,
    pub max_value: Option<serde_json::Value>,
    pub histogram: Option<Histogram>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    pub index_name: String,
    pub unique_keys: usize,
    pub depth: usize,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
    pub bucket_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower_bound: serde_json::Value,
    pub upper_bound: serde_json::Value,
    pub frequency: usize,
    pub min_value: serde_json::Value,
    pub max_value: serde_json::Value,
    pub distinct_count: usize,
}

/// Query optimizer
pub struct QueryOptimizer {
    statistics: Arc<RwLock<HashMap<String, TableStatistics>>>,
    plan_cache: Arc<RwLock<HashMap<String, QueryPlan>>>,
    cost_model: CostModel,
    snapshot_registry: Arc<RwLock<HashMap<String, Vec<SnapshotInfo>>>>,
}

/// Information about available snapshots
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub sequence: u64,
    pub timestamp: u64,
    pub size_bytes: u64,
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            cost_model: CostModel::default(),
            snapshot_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Optimize a query and produce execution plan
    #[instrument(skip(self))]
    pub fn optimize(&self, query: &Query) -> Result<QueryPlan> {
        // Check plan cache
        let cache_key = self.query_cache_key(query);
        if let Some(cached_plan) = self.plan_cache.read().get(&cache_key) {
            debug!("Using cached query plan");
            return Ok(cached_plan.clone());
        }

        let plan = match query {
            Query::Select {
                table,
                conditions,
                as_of,
                limit,
            } => self.optimize_select(table, conditions, as_of.as_ref(), limit.as_ref()),
            _ => {
                // Non-select queries don't need optimization
                Ok(QueryPlan {
                    steps: vec![],
                    estimated_cost: 1.0,
                    estimated_rows: 1,
                    uses_index: false,
                    cacheable: false,
                })
            }
        }?;

        // Cache the plan if it's cacheable
        if plan.cacheable {
            self.plan_cache.write().insert(cache_key, plan.clone());
        }

        Ok(plan)
    }

    /// Optimize SELECT query
    fn optimize_select(
        &self,
        table: &str,
        conditions: &[WhereCondition],
        as_of: Option<&AsOf>,
        limit: Option<&usize>,
    ) -> Result<QueryPlan> {
        let mut steps = Vec::new();
        let mut estimated_cost = 0.0;
        let mut estimated_rows = self.estimate_table_rows(table);
        let mut uses_index = false;
        let mut chosen_access: Option<PlanStep> = None;

        // Step 1: Handle time travel if specified
        if let Some(as_of) = as_of {
            let (snapshot_step, replay_step) = self.plan_time_travel(table, as_of);
            if let Some(step) = snapshot_step {
                estimated_cost += self.cost_of_step(&step);
                steps.push(step);
            }
            if let Some(step) = replay_step {
                estimated_cost += self.cost_of_step(&step);
                steps.push(step);
            }
        }

        // Step 2: Choose access method (index vs table scan)
        let access_plans = self.generate_access_plans(table, conditions);
        let best_access = self.choose_best_plan(&access_plans);

        if let Some(plan) = best_access {
            uses_index = matches!(
                plan,
                PlanStep::IndexScan { .. } | PlanStep::IndexLookup { .. }
            );
            chosen_access = Some(plan.clone());
            estimated_rows = self.rows_after_step(&plan, estimated_rows);
            estimated_cost += self.cost_of_step(&plan);
            steps.push(plan);
        } else {
            // Fallback to table scan
            let scan_cost = self.cost_model.table_scan_cost(estimated_rows);
            steps.push(PlanStep::TableScan {
                table: table.to_string(),
                estimated_rows,
                cost: scan_cost,
            });
            estimated_cost += scan_cost;
        }

        // Step 3: Apply remaining filters in the order the optimizer chose.
        //
        // Ordering policy (cheapest first):
        // 1. Structural cost class (operator shape + index awareness).
        //    Cheap ops like `IS NULL` / equality on indexed columns run
        //    before expensive ones like `LIKE` patterns.
        // 2. Selectivity estimate (lower selectivity = fewer rows pass
        //    through = run first to short-circuit AND chains earlier).
        //    Stats-driven; degenerates to a constant when no
        //    column_stats are present, so class is the dominant signal
        //    without ANALYZE.
        // 3. Source order — stable sort preserves it as a deterministic
        //    tiebreaker so the same query always produces the same plan.
        let indexed_columns: Vec<String> = self
            .statistics
            .read()
            .get(table)
            .map(|s| s.index_stats.keys().cloned().collect())
            .unwrap_or_default();
        let mut residual: Vec<(u8, f64, usize, &WhereCondition)> = conditions
            .iter()
            .enumerate()
            .filter(|(_, c)| !self.is_condition_covered_by_index(c, chosen_access.as_ref()))
            .map(|(i, c)| {
                let class = Self::predicate_cost_class(c, &indexed_columns);
                let selectivity = self.estimate_selectivity(table, c);
                (class, selectivity, i, c)
            })
            .collect();
        // Stable sort: equal keys retain source order.
        residual.sort_by(|a, b| {
            a.0.cmp(&b.0).then_with(|| {
                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
            })
        });
        for (_, selectivity, _, condition) in residual {
            let filter_cost = self.cost_model.filter_cost(estimated_rows);
            steps.push(PlanStep::Filter {
                predicate: condition.clone(),
                selectivity,
                cost: filter_cost,
            });
            estimated_rows = (estimated_rows as f64 * selectivity) as usize;
            estimated_cost += filter_cost;
        }

        // Step 4: Apply limit if specified
        if let Some(limit_count) = limit {
            steps.push(PlanStep::Limit {
                count: *limit_count,
                cost: 0.1, // Minimal cost for limit
            });
            estimated_rows = estimated_rows.min(*limit_count);
            estimated_cost += 0.1;
        }

        Ok(QueryPlan {
            steps,
            estimated_cost,
            estimated_rows,
            uses_index,
            cacheable: true,
        })
    }

    /// Generate possible access plans for a table.
    ///
    /// For each indexed column, considers the predicates that target it:
    /// - If any equality predicate exists, emit an `IndexLookup` (point
    ///   query — strictly more selective than a range, and the index
    ///   structure supports it directly).
    /// - Otherwise, fold all range predicates on that column into a
    ///   single `IndexScan` with combined `start`/`end` bounds. Multiple
    ///   bounds on the same side (e.g. `age > 18 AND age > 21`) tighten
    ///   to the strictest. Non-range non-equality operators (`!=`,
    ///   `LIKE`, `IN`) don't drive index access — they remain residual
    ///   `Filter` steps.
    fn generate_access_plans(&self, table: &str, conditions: &[WhereCondition]) -> Vec<PlanStep> {
        let mut plans = Vec::new();
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            for index_name in table_stats.index_stats.keys() {
                let matching: Vec<&WhereCondition> = conditions
                    .iter()
                    .filter(|c| c.column == *index_name)
                    .collect();
                if matching.is_empty() {
                    continue;
                }

                // Prefer equality.
                if let Some(eq) = matching
                    .iter()
                    .find(|c| c.operator == "=" || c.operator == "==")
                {
                    plans.push(PlanStep::IndexLookup {
                        table: table.to_string(),
                        index: index_name.clone(),
                        key: eq.value.to_string(),
                        estimated_rows: 1,
                        cost: self.cost_model.index_lookup_cost(),
                    });
                    continue;
                }

                // Otherwise coalesce ranges into a single IndexScan.
                let mut start: Option<RangeBound> = None;
                let mut end: Option<RangeBound> = None;
                for cond in &matching {
                    match cond.operator.as_str() {
                        ">" => start = tighter_start(start.take(), cond.value.clone(), false),
                        ">=" => start = tighter_start(start.take(), cond.value.clone(), true),
                        "<" => end = tighter_end(end.take(), cond.value.clone(), false),
                        "<=" => end = tighter_end(end.take(), cond.value.clone(), true),
                        _ => {} // !=, LIKE, IN — not index-usable
                    }
                }
                if start.is_some() || end.is_some() {
                    // Selectivity = product over the range predicates that
                    // contributed bounds; that's a rough estimate but good
                    // enough to make the IndexScan cheaper than a full scan
                    // for typical queries.
                    let mut selectivity = 1.0;
                    for cond in &matching {
                        if matches!(cond.operator.as_str(), ">" | ">=" | "<" | "<=") {
                            selectivity *= self.estimate_selectivity(table, cond);
                        }
                    }
                    let estimated_rows = ((table_stats.row_count as f64) * selectivity) as usize;
                    plans.push(PlanStep::IndexScan {
                        table: table.to_string(),
                        index: index_name.clone(),
                        start,
                        end,
                        estimated_rows,
                        cost: self.cost_model.index_scan_cost(estimated_rows),
                    });
                }
            }
        }

        // Always consider table scan as fallback
        let scan_rows = self.estimate_table_rows(table);
        plans.push(PlanStep::TableScan {
            table: table.to_string(),
            estimated_rows: scan_rows,
            cost: self.cost_model.table_scan_cost(scan_rows),
        });

        plans
    }

    /// Choose the best plan based on cost
    fn choose_best_plan(&self, plans: &[PlanStep]) -> Option<PlanStep> {
        plans
            .iter()
            .min_by(|a, b| {
                let cost_a = self.cost_of_step(a);
                let cost_b = self.cost_of_step(b);
                cost_a
                    .partial_cmp(&cost_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Plan time travel operations
    fn plan_time_travel(&self, table: &str, as_of: &AsOf) -> (Option<PlanStep>, Option<PlanStep>) {
        match as_of {
            AsOf::Sequence(seq) => {
                // Find closest snapshot
                let snapshot_seq = self.find_closest_snapshot(table, *seq);

                let snapshot_step = snapshot_seq.map(|s| PlanStep::SnapshotLoad {
                    sequence: s,
                    cost: self.cost_model.snapshot_load_cost(),
                });

                let replay_step = if let Some(snap_seq) = snapshot_seq {
                    if snap_seq < *seq {
                        Some(PlanStep::EventReplay {
                            from_sequence: snap_seq,
                            to_sequence: *seq,
                            estimated_events: (*seq - snap_seq) as usize,
                            cost: self
                                .cost_model
                                .event_replay_cost((*seq - snap_seq) as usize),
                        })
                    } else {
                        None
                    }
                } else {
                    Some(PlanStep::EventReplay {
                        from_sequence: 0,
                        to_sequence: *seq,
                        estimated_events: *seq as usize,
                        cost: self.cost_model.event_replay_cost(*seq as usize),
                    })
                };

                (snapshot_step, replay_step)
            }
            AsOf::Timestamp(_ts) => {
                // Convert timestamp to sequence (simplified)
                (None, None)
            }
            AsOf::Now => {
                // No time travel needed for current state
                (None, None)
            }
        }
    }

    /// Estimate selectivity of a predicate
    fn estimate_selectivity(&self, table: &str, condition: &WhereCondition) -> f64 {
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            if let Some(col_stats) = table_stats.column_stats.get(&condition.column) {
                // Account for nulls
                let null_fraction = if table_stats.row_count > 0 {
                    col_stats.null_count as f64 / table_stats.row_count as f64
                } else {
                    0.0
                };

                // Use statistics to estimate selectivity
                let non_null_selectivity = match condition.operator.as_str() {
                    "=" => {
                        // Point-query selectivity: uniform-distribution
                        // estimate, 1 / distinct_values. Histograms
                        // are NOT consulted here even when present —
                        // equi-depth histograms record bucket frequency
                        // (≈ total_rows / bucket_count), which doesn't
                        // depend on cardinality, so they can't
                        // distinguish a 2-distinct-value column from a
                        // 1000-distinct one. Use the column's
                        // distinct_values directly. PostgreSQL uses
                        // `most_common_values` for the same purpose;
                        // we don't track MCVs yet, so falling back to
                        // uniform-distribution is the right move.
                        if col_stats.distinct_values > 0 {
                            1.0 / col_stats.distinct_values as f64
                        } else {
                            0.1 // Default
                        }
                    }
                    "<" | ">" | "<=" | ">=" => {
                        // Range query selectivity using min/max or histogram
                        if let Some(histogram) = &col_stats.histogram {
                            self.estimate_range_selectivity_with_histogram(
                                &condition.value,
                                &condition.operator,
                                histogram,
                                table_stats.row_count,
                            )
                        } else if col_stats.min_value.is_some() && col_stats.max_value.is_some() {
                            self.estimate_range_selectivity_with_bounds(
                                &condition.value,
                                &condition.operator,
                                &col_stats.min_value,
                                &col_stats.max_value,
                            )
                        } else {
                            0.3 // Default 30% selectivity for range
                        }
                    }
                    "IS NULL" => null_fraction,
                    "IS NOT NULL" => 1.0 - null_fraction,
                    _ => 0.5, // Default 50% for unknown operators
                };

                // Adjust for nulls (most operators don't match nulls)
                if condition.operator != "IS NULL" && condition.operator != "IS NOT NULL" {
                    non_null_selectivity * (1.0 - null_fraction)
                } else {
                    non_null_selectivity
                }
            } else {
                0.3 // No statistics, use default
            }
        } else {
            0.3 // No table statistics
        }
    }

    // `estimate_equality_selectivity_with_histogram` lived here until
    // slice 9. Slice 8's analysis showed equi-depth histograms can't
    // distinguish cardinality for equality queries — every bucket
    // holds ~total_rows / bucket_count rows by construction, so the
    // estimate didn't depend on the column's actual cardinality.
    // Equality now uses `1 / distinct_values` directly. PostgreSQL
    // solves the same problem with most_common_values (MCV) lists;
    // adding MCV tracking is a future slice.

    /// Estimate selectivity for range queries using histogram
    fn estimate_range_selectivity_with_histogram(
        &self,
        value: &serde_json::Value,
        operator: &str,
        histogram: &Histogram,
        total_rows: usize,
    ) -> f64 {
        let mut matching_frequency = 0;

        for bucket in &histogram.buckets {
            match operator {
                "<" => {
                    if self.compare_values(&bucket.upper_bound, value) < 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                ">" => {
                    if self.compare_values(&bucket.lower_bound, value) > 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                "<=" => {
                    if self.compare_values(&bucket.upper_bound, value) <= 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                ">=" => {
                    if self.compare_values(&bucket.lower_bound, value) >= 0 {
                        matching_frequency += bucket.frequency;
                    } else if self.value_in_range(value, &bucket.lower_bound, &bucket.upper_bound) {
                        // Partial bucket match (estimate)
                        matching_frequency += bucket.frequency / 2;
                    }
                }
                _ => {}
            }
        }

        matching_frequency as f64 / total_rows as f64
    }

    /// Estimate range selectivity using min/max bounds
    fn estimate_range_selectivity_with_bounds(
        &self,
        value: &serde_json::Value,
        operator: &str,
        min_value: &Option<serde_json::Value>,
        max_value: &Option<serde_json::Value>,
    ) -> f64 {
        // Simplified linear interpolation between min and max
        if let (Some(min), Some(max)) = (min_value, max_value) {
            let position = self.interpolate_value_position(value, min, max);
            match operator {
                "<" | "<=" => position,
                ">" | ">=" => 1.0 - position,
                _ => 0.3,
            }
        } else {
            0.3
        }
    }

    /// Check if a value is within a range
    fn value_in_range(
        &self,
        value: &serde_json::Value,
        lower: &serde_json::Value,
        upper: &serde_json::Value,
    ) -> bool {
        self.compare_values(value, lower) >= 0 && self.compare_values(value, upper) <= 0
    }

    /// Compare two JSON values for cardinality-estimation interpolation.
    /// Delegates to the canonical predicate ordering so the optimizer's
    /// cost model sees the same value order as actual query execution.
    fn compare_values(&self, a: &serde_json::Value, b: &serde_json::Value) -> i32 {
        crate::query::predicate::compare_json_values(a, b) as i32
    }

    /// Interpolate value position between min and max
    fn interpolate_value_position(
        &self,
        value: &serde_json::Value,
        min: &serde_json::Value,
        max: &serde_json::Value,
    ) -> f64 {
        // Simple linear interpolation for numeric values
        if let (
            serde_json::Value::Number(v),
            serde_json::Value::Number(min_n),
            serde_json::Value::Number(max_n),
        ) = (value, min, max)
        {
            if let (Some(v_f), Some(min_f), Some(max_f)) =
                (v.as_f64(), min_n.as_f64(), max_n.as_f64())
            {
                if max_f > min_f {
                    ((v_f - min_f) / (max_f - min_f)).clamp(0.0, 1.0)
                } else {
                    0.5
                }
            } else {
                0.5
            }
        } else {
            0.5 // Default to middle
        }
    }

    /// Build a `PlanNode` tree for a single equi-join. This is the
    /// optimizer's entry point for join queries; the executor pattern-
    /// matches on the returned node to dispatch the join algorithm.
    ///
    /// The returned tree shape is:
    /// ```text
    ///   <NestedLoopJoin|HashJoin>
    ///     ├── left:  TableScan(left_table)
    ///     └── right: TableScan(right_table)
    /// ```
    ///
    /// Per-side predicates are NOT included here — they are pushed
    /// through `Engine::select` by the caller, which already honors the
    /// flat `QueryPlan` for per-side access (slices 1–3 wiring). The
    /// join-level plan only chooses the *algorithm*. This is the
    /// hybrid-contract boundary: PlanNode tree at the join site, flat
    /// QueryPlan at each leaf.
    ///
    /// Algorithm choice heuristic (in order):
    ///
    /// 1. Inner side (right) has an index on its join column → NestedLoop.
    ///    Each outer row becomes an index lookup; this composes with
    ///    slice-1 wiring at the leaves.
    /// 2. Symmetric: left has an index on its join column, right doesn't
    ///    → NestedLoop with `build_side = Left` (advisory; the executor
    ///    doesn't reorder).
    /// 3. Either estimate exceeds `NL_THRESHOLD` (1000) → Hash. Build
    ///    on the smaller side; probe with the larger.
    /// 4. Otherwise → NestedLoop. Always-correct fallback.
    ///
    /// Without `ANALYZE`, table row counts come from the
    /// `register_table_indexes` hint (10_000), which exceeds the
    /// threshold — so unindexed two-table joins default to Hash. That's
    /// the safer default for unknown-cardinality joins: nested loop is
    /// O(N*M) and explodes; hash is O(N+M).
    pub fn plan_single_join(
        &self,
        left_table: &str,
        right_table: &str,
        left_join_col: &str,
        right_join_col: &str,
        join_type: JoinType,
    ) -> PlanNode {
        let stats = self.statistics.read();
        let left_rows = stats.get(left_table).map(|s| s.row_count).unwrap_or(0);
        let right_rows = stats.get(right_table).map(|s| s.row_count).unwrap_or(0);
        let right_indexed = stats
            .get(right_table)
            .map(|s| s.index_stats.contains_key(right_join_col))
            .unwrap_or(false);
        let left_indexed = stats
            .get(left_table)
            .map(|s| s.index_stats.contains_key(left_join_col))
            .unwrap_or(false);
        drop(stats);

        let left = Box::new(PlanNode::TableScan {
            table: left_table.to_string(),
            predicates: vec![],
            cost: Cost::seq_scan((left_rows as f64 / 100.0).max(1.0), left_rows as f64),
        });
        let right = Box::new(PlanNode::TableScan {
            table: right_table.to_string(),
            predicates: vec![],
            cost: Cost::seq_scan((right_rows as f64 / 100.0).max(1.0), right_rows as f64),
        });
        let condition = JoinCondition {
            left_col: left_join_col.to_string(),
            right_col: right_join_col.to_string(),
            op: ComparisonOp::Eq,
            raw_text: None,
        };

        const NL_THRESHOLD: usize = 1000;

        // OUTER joins constrain the hash-build side: LEFT and FULL must
        // build on the RIGHT (preserving side probes; build side is the
        // one we may need to mark "matched" for unmatched-row emission).
        // NL handles every join type with a single uniform loop, so
        // these constraints only apply to the Hash branch below.

        // Rule 1: indexed inner-side join column → NestedLoop.
        if right_indexed && !left_indexed {
            return PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                cost: Cost::seq_scan(1.0, left_rows.max(1) as f64),
            };
        }
        // Rule 2: indexed left → NestedLoop (symmetric).
        if left_indexed && !right_indexed {
            return PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                cost: Cost::seq_scan(1.0, right_rows.max(1) as f64),
            };
        }
        // Rule 3: large side → Hash, build on smaller side BUT OUTER
        // joins force build_side = Right (need to probe with the
        // preserving side and mark matches for FULL OUTER).
        if left_rows > NL_THRESHOLD || right_rows > NL_THRESHOLD {
            let build_side = match join_type {
                JoinType::LeftOuter | JoinType::FullOuter => JoinSide::Right,
                JoinType::Inner => {
                    if left_rows < right_rows {
                        JoinSide::Left
                    } else {
                        JoinSide::Right
                    }
                }
            };
            return PlanNode::HashJoin {
                left,
                right,
                condition,
                build_side,
                join_type,
                cost: Cost::seq_scan(1.0, (left_rows + right_rows) as f64),
            };
        }
        // Rule 4: NestedLoop default.
        PlanNode::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
            cost: Cost::seq_scan(1.0, (left_rows * right_rows.max(1)) as f64),
        }
    }

    /// Structural cost class for a residual predicate. Lower = cheaper to
    /// evaluate, so should run first to short-circuit AND chains earlier.
    ///
    /// This is a deliberately coarse heuristic that doesn't require column
    /// statistics. It captures operator shape (`IS NULL` is a single `.get()`;
    /// `LIKE` walks a pattern) and a hint about index-eligibility — equality
    /// on an indexed column tends to be highly selective even when it ends
    /// up as a residual (e.g. when a different index was chosen for access).
    ///
    /// When real column statistics are populated by `ANALYZE`, selectivity
    /// estimates refine the order within a class; this function only
    /// determines the gross bucketing.
    fn predicate_cost_class(condition: &WhereCondition, indexed_columns: &[String]) -> u8 {
        let op = condition.operator.as_str();
        let col_is_indexed = indexed_columns.iter().any(|c| c == &condition.column);
        match op {
            "IS NULL" | "IS NOT NULL" => 0,
            "=" | "==" | "!=" | "<>" if col_is_indexed => 1,
            "=" | "==" | "!=" | "<>" => 2,
            "<" | "<=" | ">" | ">=" => 3,
            "IN" | "NOT IN" => 4,
            "LIKE" => 5,
            _ => 6,
        }
    }

    /// Check if condition is folded into the chosen access step.
    ///
    /// A condition is "covered" when its column matches the access
    /// index AND its operator matches what that access mode encodes:
    /// - `IndexLookup` covers `=` (the single key it probes).
    /// - `IndexScan` covers `<`/`<=`/`>`/`>=` (the bounds it walks).
    ///
    /// Other operators on the indexed column (`!=`, `LIKE`, `IN`) still
    /// need a residual `Filter` step.
    fn is_condition_covered_by_index(
        &self,
        condition: &WhereCondition,
        access_step: Option<&PlanStep>,
    ) -> bool {
        match access_step {
            Some(PlanStep::IndexLookup { index, .. }) => {
                condition.column == *index
                    && (condition.operator == "=" || condition.operator == "==")
            }
            Some(PlanStep::IndexScan { index, .. }) => {
                condition.column == *index
                    && matches!(condition.operator.as_str(), ">" | ">=" | "<" | "<=")
            }
            _ => false,
        }
    }

    /// Estimate rows in table
    fn estimate_table_rows(&self, table: &str) -> usize {
        self.statistics
            .read()
            .get(table)
            .map(|s| s.row_count)
            .unwrap_or(0) // Return 0 if no statistics - forces statistics collection
    }

    /// Calculate cost of a plan step
    fn cost_of_step(&self, step: &PlanStep) -> f64 {
        match step {
            PlanStep::TableScan { cost, .. } => *cost,
            PlanStep::IndexScan { cost, .. } => *cost,
            PlanStep::IndexLookup { cost, .. } => *cost,
            PlanStep::Filter { cost, .. } => *cost,
            PlanStep::Sort { cost, .. } => *cost,
            PlanStep::Limit { cost, .. } => *cost,
            PlanStep::TimeTravel { cost, .. } => *cost,
            PlanStep::SnapshotLoad { cost, .. } => *cost,
            PlanStep::EventReplay { cost, .. } => *cost,
        }
    }

    /// Estimate rows after applying a step
    fn rows_after_step(&self, step: &PlanStep, input_rows: usize) -> usize {
        match step {
            PlanStep::TableScan { estimated_rows, .. } => *estimated_rows,
            PlanStep::IndexScan { estimated_rows, .. } => *estimated_rows,
            PlanStep::IndexLookup { estimated_rows, .. } => *estimated_rows,
            PlanStep::Filter { selectivity, .. } => (input_rows as f64 * selectivity) as usize,
            PlanStep::Limit { count, .. } => input_rows.min(*count),
            _ => input_rows,
        }
    }

    /// Find closest snapshot for time travel
    fn find_closest_snapshot(&self, table: &str, sequence: u64) -> Option<u64> {
        let registry = self.snapshot_registry.read();

        if let Some(snapshots) = registry.get(table) {
            // Find the snapshot with the largest sequence that's still <= target sequence
            snapshots
                .iter()
                .filter(|s| s.sequence <= sequence)
                .max_by_key(|s| s.sequence)
                .map(|s| s.sequence)
        } else {
            None
        }
    }

    /// Register a snapshot with the optimizer
    pub fn register_snapshot(&self, table: &str, info: SnapshotInfo) {
        let mut registry = self.snapshot_registry.write();
        registry.entry(table.to_string()).or_default().push(info);
    }

    /// Generate cache key for query
    fn query_cache_key(&self, query: &Query) -> String {
        format!("{:?}", query) // Simple serialization
    }

    /// Update table statistics. Also clears the plan cache: cached
    /// plans were computed against the prior `column_stats` /
    /// `row_count`, so post-ANALYZE the same query may want a
    /// different access method or filter order. `register_table_indexes`
    /// already invalidates here for the same reason; this is the
    /// missing counterpart for the ANALYZE-driven entry point.
    pub fn update_statistics(&self, table: &str, stats: TableStatistics) {
        self.statistics.write().insert(table.to_string(), stats);
        self.plan_cache.write().clear();
    }

    /// Register the set of indexed columns for a table so the planner can
    /// propose `IndexLookup` plans without requiring prior `ANALYZE`.
    ///
    /// `row_count_hint` seeds the row count used for cost estimation. It does
    /// not need to be exact; it just needs to be large enough that a
    /// `TableScan` looks more expensive than an `IndexLookup` (the cost model
    /// at default settings flips the choice around ~700 rows). A real
    /// `ANALYZE` later will overwrite this via [`Self::update_statistics`].
    /// Row-count estimate for a table from the optimizer's statistics
    /// store. Returns `None` when the table hasn't been registered
    /// (no index registrations, no `ANALYZE`). The multi-join
    /// reordering planner consumes this as the dominant cost signal.
    pub fn statistics_row_count(&self, table: &str) -> Option<usize> {
        self.statistics.read().get(table).map(|s| s.row_count)
    }

    pub fn register_table_indexes(
        &self,
        table: &str,
        indexed_columns: &[String],
        row_count_hint: usize,
    ) {
        let mut stats_map = self.statistics.write();
        let entry = stats_map
            .entry(table.to_string())
            .or_insert_with(|| TableStatistics {
                table_name: table.to_string(),
                row_count: row_count_hint,
                column_count: 0,
                avg_row_size: 0,
                total_size_bytes: 0,
                data_size_bytes: 0,
                column_stats: HashMap::new(),
                column_statistics: HashMap::new(),
                index_stats: HashMap::new(),
                last_updated: 0,
                collection_method: "engine_register".to_string(),
                collection_duration_ms: 0,
            });
        // Bump the row count if our hint is higher than the existing value
        // (e.g., later registrations see a larger table) but never shrink it,
        // so a real ANALYZE's row_count is preserved.
        if row_count_hint > entry.row_count {
            entry.row_count = row_count_hint;
        }
        for col in indexed_columns {
            entry
                .index_stats
                .entry(col.clone())
                .or_insert_with(|| IndexStatistics {
                    index_name: col.clone(),
                    unique_keys: 0,
                    depth: 1,
                    size_bytes: 0,
                });
        }
        // A registration change invalidates cached plans for this table.
        self.plan_cache.write().clear();
    }

    /// Clear plan cache
    pub fn clear_cache(&self) {
        self.plan_cache.write().clear();
    }

    // `optimize_condition_order` lived here as a `#[allow(dead_code)]`
    // helper that sorted purely on `estimate_selectivity`. That signal
    // degenerates to a constant without column statistics, so the helper
    // would have produced source order in practice. Predicate reordering
    // now lives inline in `optimize_select` and combines a structural
    // cost class with the selectivity estimate.

    /// Analyze query patterns and suggest new indexes
    pub fn suggest_indexes(&self, table: &str) -> Vec<String> {
        let mut suggestions = Vec::new();
        let stats = self.statistics.read();

        if let Some(table_stats) = stats.get(table) {
            // Analyze column access patterns
            for column_name in table_stats.column_stats.keys() {
                // Suggest index if column is frequently used in WHERE but not indexed
                if !table_stats.index_stats.contains_key(column_name) {
                    // In production, would check query history for this column
                    suggestions.push(format!(
                        "CREATE INDEX idx_{}_{} ON {} ({})",
                        table, column_name, table, column_name
                    ));
                }
            }
        }

        suggestions
    }

    /// Estimate memory usage for query execution
    pub fn estimate_memory_usage(&self, plan: &QueryPlan) -> usize {
        let mut memory = 0;

        for step in &plan.steps {
            match step {
                PlanStep::TableScan { estimated_rows, .. }
                | PlanStep::IndexScan { estimated_rows, .. } => {
                    // Assume average row size of 1KB
                    memory = memory.max(estimated_rows * 1024);
                }
                PlanStep::Sort { estimated_rows, .. } => {
                    // Sorting requires full dataset in memory
                    memory = memory.max(estimated_rows * 1024);
                }
                PlanStep::Limit { count, .. } => {
                    // Limit only needs to buffer the limit amount
                    memory = memory.max(count * 1024);
                }
                _ => {}
            }
        }

        memory
    }
}

/// Cost model for different operations
#[derive(Debug, Clone)]
pub struct CostModel {
    pub seq_page_cost: f64,
    pub random_page_cost: f64,
    pub cpu_tuple_cost: f64,
    pub cpu_operator_cost: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.005,
        }
    }
}

impl CostModel {
    pub fn table_scan_cost(&self, rows: usize) -> f64 {
        let pages = (rows / 100).max(1); // Assume 100 rows per page
        self.seq_page_cost * pages as f64 + self.cpu_tuple_cost * rows as f64
    }

    pub fn index_scan_cost(&self, rows: usize) -> f64 {
        let pages = (rows / 200).max(1); // More rows per index page
        self.random_page_cost * pages as f64 + self.cpu_tuple_cost * rows as f64
    }

    pub fn index_lookup_cost(&self) -> f64 {
        self.random_page_cost * 2.0 + self.cpu_tuple_cost // Index + data page
    }

    pub fn filter_cost(&self, rows: usize) -> f64 {
        self.cpu_operator_cost * rows as f64
    }

    pub fn sort_cost(&self, rows: usize) -> f64 {
        let log_rows = (rows as f64).log2().max(1.0);
        rows as f64 * log_rows * self.cpu_operator_cost
    }

    pub fn snapshot_load_cost(&self) -> f64 {
        self.seq_page_cost * 10.0 // Assume 10 pages for snapshot
    }

    pub fn event_replay_cost(&self, events: usize) -> f64 {
        self.cpu_tuple_cost * events as f64 * 2.0 // Higher cost for replay
    }
}


// ============================================================================
// Tree-shaped plan / cost model
//
// The types below were `crate::cost_optimizer` until the optimizer consolidation
// commit retired that module. They model a structural execution plan (TableScan
// / IndexScan / NestedLoopJoin / HashJoin / Filter / Sort / Aggregate / Limit /
// Distinct / SetOperation / Materialize) that the EXPLAIN code in
// `crate::explain` consumes directly. `CostOptimizer` carries the
// join-reordering, predicate-pushdown, and index-selection logic; the rewrites
// it produces aren't yet plumbed back into execution — that wiring is a
// separate piece of work because the executor would need to learn to handle
// reordered joins (a row-shape-assumption change).
//
// Lives alongside the flat `QueryPlan` / `PlanStep` types above because both
// are query-planning data; keeping them in one module means EXPLAIN's type
// imports point at one place and the future executor wiring can pull from the
// same source.
// ============================================================================

/// Query plan node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanNode {
    /// Table scan
    TableScan {
        table: String,
        predicates: Vec<Predicate>,
        cost: Cost,
    },
    /// Index scan
    IndexScan {
        table: String,
        index: String,
        predicates: Vec<Predicate>,
        cost: Cost,
    },
    /// Nested loop join
    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
        #[serde(default = "default_join_type")]
        join_type: JoinType,
        cost: Cost,
    },
    /// Hash join
    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
        build_side: JoinSide,
        #[serde(default = "default_join_type")]
        join_type: JoinType,
        cost: Cost,
    },
    /// Sort-merge join
    SortMergeJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: JoinCondition,
        cost: Cost,
    },
    /// Sort operation
    Sort {
        input: Box<PlanNode>,
        keys: Vec<SortKey>,
        cost: Cost,
    },
    /// Aggregation
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunc>,
        cost: Cost,
    },
    /// Filter
    Filter {
        input: Box<PlanNode>,
        predicates: Vec<Predicate>,
        cost: Cost,
    },
    /// Projection
    Project {
        input: Box<PlanNode>,
        columns: Vec<String>,
        cost: Cost,
    },
    /// Limit
    Limit {
        input: Box<PlanNode>,
        limit: usize,
        offset: usize,
        cost: Cost,
    },
    /// Materialize (force materialization point)
    Materialize { input: Box<PlanNode>, cost: Cost },
    /// DISTINCT — duplicate elimination on the input's projection.
    Distinct {
        input: Box<PlanNode>,
        /// Distinct keys; empty means full-row deduplication (`SELECT DISTINCT *`).
        columns: Vec<String>,
        cost: Cost,
    },
    /// UNION / INTERSECT / EXCEPT.
    SetOperation {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        /// Display name for the operation: "Union", "Intersect", "Except",
        /// with " All" suffix for `ALL` variants. Free-form rather than an
        /// enum so dialect quirks (MySQL `UNION DISTINCT` etc.) don't
        /// require touching cost_optimizer when new ones land.
        operation: String,
        cost: Cost,
    },
}

/// Cost model
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Cost {
    /// I/O cost (page reads)
    pub io_cost: f64,
    /// CPU cost (tuple processing)
    pub cpu_cost: f64,
    /// Memory required (bytes)
    pub memory: f64,
    /// Network cost (for distributed)
    pub network_cost: f64,
    /// Estimated row count
    pub rows: f64,
    /// Estimated data size
    pub size: f64,
}

impl Cost {
    /// Total cost combining all factors
    pub fn total(&self) -> f64 {
        self.io_cost + self.cpu_cost * 0.01 + self.network_cost * 2.0
    }

    /// Create a cost for sequential scan
    pub fn seq_scan(pages: f64, rows: f64) -> Self {
        Self {
            io_cost: pages,
            cpu_cost: rows * 0.01,
            rows,
            size: rows * 100.0, // Assume 100 bytes per row average
            ..Default::default()
        }
    }

    /// Create a cost for index scan
    pub fn index_scan(index_pages: f64, data_pages: f64, rows: f64) -> Self {
        Self {
            io_cost: index_pages + data_pages,
            cpu_cost: rows * 0.005, // Less CPU than seq scan
            rows,
            size: rows * 100.0,
            ..Default::default()
        }
    }
}

/// Predicate for filtering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Predicate {
    pub column: String,
    pub op: ComparisonOp,
    pub value: PredicateValue,
    pub selectivity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PredicateValue {
    Constant(serde_json::Value),
    Column(String),
    Subquery(Box<PlanNode>),
    /// Free-form expression text, used when the SQL predicate doesn't
    /// reduce to one of the structured forms above (compound AND/OR,
    /// function calls, arithmetic expressions). The expression is for
    /// EXPLAIN display only — selectivity falls back to a default.
    Raw(String),
}

/// Join condition. `left_col` / `right_col` / `op` are populated for
/// simple `t1.a = t2.b` equi-joins, which cover most cases and feed the
/// cost model. When the SQL join predicate is richer (compound AND,
/// inequality, function calls), `raw_text` carries the full predicate
/// for EXPLAIN display and the structured fields are placeholders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_col: String,
    pub right_col: String,
    pub op: ComparisonOp,
    #[serde(default)]
    pub raw_text: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinSide {
    Left,
    Right,
}

/// Join type for `PlanNode::NestedLoopJoin` / `PlanNode::HashJoin`. The
/// SQL `RIGHT OUTER JOIN` shape is normalized to `LeftOuter` at the
/// executor before reaching `plan_single_join` (PostgreSQL convention:
/// swap the sides). `LeftOuter` and `FullOuter` are the only OUTER
/// shapes the planner sees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    FullOuter,
}

/// Backward-compatible default so older serialized PlanNode payloads
/// (pre-OUTER-JOIN slice) deserialize as INNER. Keeps EXPLAIN snapshots
/// stable across versions.
fn default_join_type() -> JoinType {
    JoinType::Inner
}

/// Sort key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

/// Aggregate function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateFunc {
    pub func: String,
    pub column: Option<String>,
    pub alias: String,
}

/// Query optimizer
pub struct CostOptimizer {
    /// Table statistics
    statistics: Arc<RwLock<HashMap<String, Arc<TableStatistics>>>>,
    /// Available indexes
    indexes: Arc<RwLock<HashMap<String, Vec<IndexInfo>>>>,
    /// Materialized views
    #[allow(dead_code)]
    materialized_views: Arc<RwLock<Vec<MaterializedViewInfo>>>,
    /// Cost parameters
    params: CostParameters,
    /// Optimization statistics
    stats: Arc<RwLock<OptimizerStats>>,
}

/// Index information
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct IndexInfo {
    name: String,
    table: String,
    columns: Vec<String>,
    #[allow(dead_code)]
    index_type: IndexType,
    unique: bool,
    size_pages: usize,
}

/// Materialized view information
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MaterializedViewInfo {
    name: String,
    query: String,
    tables: HashSet<String>,
    columns: Vec<String>,
}

/// Cost calculation parameters
#[derive(Debug, Clone)]
struct CostParameters {
    #[allow(dead_code)]
    seq_page_cost: f64,
    #[allow(dead_code)]
    random_page_cost: f64,
    #[allow(dead_code)]
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
    #[allow(dead_code)]
    parallel_workers: usize,
    work_mem: usize, // KB
}

impl Default for CostParameters {
    fn default() -> Self {
        Self {
            seq_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
            parallel_workers: 4,
            work_mem: 4096, // 4MB
        }
    }
}

/// Optimizer statistics
#[derive(Debug, Default)]
struct OptimizerStats {
    #[allow(dead_code)]
    plans_considered: u64,
    #[allow(dead_code)]
    plans_pruned: u64,
    optimization_time_ms: u64,
    #[allow(dead_code)]
    joins_reordered: u64,
    indexes_used: u64,
}

impl Default for CostOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl CostOptimizer {
    pub fn new() -> Self {
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            materialized_views: Arc::new(RwLock::new(Vec::new())),
            params: CostParameters::default(),
            stats: Arc::new(RwLock::new(OptimizerStats::default())),
        }
    }

    /// Update table statistics
    pub fn update_statistics(&self, table: &str, stats: Arc<TableStatistics>) {
        self.statistics.write().insert(table.to_string(), stats);
    }

    /// Register an index
    #[allow(dead_code)]
    pub fn register_index(&self, info: IndexInfo) {
        self.indexes
            .write()
            .entry(info.table.clone())
            .or_default()
            .push(info);
    }

    /// Optimize a query plan
    pub fn optimize(&self, initial_plan: PlanNode) -> Result<PlanNode> {
        let start = std::time::Instant::now();

        // Apply optimization rules in order
        let mut plan = initial_plan;

        // 1. Predicate pushdown
        plan = self.push_down_predicates(plan)?;

        // 2. Join reordering
        plan = self.reorder_joins(plan)?;

        // 3. Index selection
        plan = self.select_indexes(plan)?;

        // 4. Choose join algorithms
        plan = self.choose_join_algorithms(plan)?;

        // 5. Add materialization points
        plan = self.add_materialization_points(plan)?;

        // 6. Parallel execution planning
        plan = self.plan_parallel_execution(plan)?;

        let elapsed = start.elapsed().as_millis() as u64;
        self.stats.write().optimization_time_ms += elapsed;

        Ok(plan)
    }

    /// Push predicates down the plan tree
    fn push_down_predicates(&self, plan: PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::Filter {
                input, predicates, ..
            } => {
                // Try to push filter below joins
                match *input {
                    PlanNode::HashJoin {
                        left,
                        right,
                        condition,
                        build_side,
                        join_type,
                        cost,
                    } => {
                        let (left_preds, right_preds, remaining) =
                            self.split_join_predicates(&predicates, &left, &right);

                        let new_left = if !left_preds.is_empty() {
                            Box::new(PlanNode::Filter {
                                input: left,
                                predicates: left_preds,
                                cost: Cost::default(),
                            })
                        } else {
                            left
                        };

                        let new_right = if !right_preds.is_empty() {
                            Box::new(PlanNode::Filter {
                                input: right,
                                predicates: right_preds,
                                cost: Cost::default(),
                            })
                        } else {
                            right
                        };

                        let join = PlanNode::HashJoin {
                            left: new_left,
                            right: new_right,
                            condition,
                            build_side,
                            join_type,
                            cost,
                        };

                        if remaining.is_empty() {
                            Ok(join)
                        } else {
                            Ok(PlanNode::Filter {
                                input: Box::new(join),
                                predicates: remaining,
                                cost: Cost::default(),
                            })
                        }
                    }
                    _ => Ok(PlanNode::Filter {
                        input: Box::new(*input),
                        predicates,
                        cost: Cost::default(),
                    }),
                }
            }
            _ => Ok(plan),
        }
    }

    /// Reorder joins using dynamic programming
    fn reorder_joins(&self, plan: PlanNode) -> Result<PlanNode> {
        // Extract all joins and tables
        let (tables, joins) = self.extract_joins(&plan)?;

        if tables.len() <= 2 {
            return Ok(plan); // No reordering needed
        }

        // Use dynamic programming to find optimal join order
        let best_order = self.find_best_join_order(&tables, &joins)?;

        // Rebuild plan with optimal order
        self.rebuild_with_join_order(plan, best_order)
    }

    /// Find optimal join order using dynamic programming
    fn find_best_join_order(&self, tables: &[String], joins: &[JoinInfo]) -> Result<Vec<String>> {
        let n = tables.len();
        if n > 12 {
            // Fall back to greedy for large joins
            return self.greedy_join_order(tables, joins);
        }

        // DP table: subset -> (cost, order)
        let mut dp: HashMap<BitSet, (Cost, Vec<String>)> = HashMap::new();

        // Base case: single tables
        for (i, table) in tables.iter().enumerate() {
            let mut set = BitSet::new(n);
            set.set(i);

            let stats = self.statistics.read();
            let cost = if let Some(table_stats) = stats.get(table) {
                Cost::seq_scan(
                    table_stats.total_size_bytes as f64 / 8192.0,
                    table_stats.row_count as f64,
                )
            } else {
                Cost::seq_scan(100.0, 1000.0) // Default estimate
            };

            dp.insert(set, (cost, vec![table.clone()]));
        }

        // Build up subsets
        for size in 2..=n {
            for subset in BitSet::subsets_of_size(n, size) {
                let mut best_cost = Cost {
                    io_cost: f64::INFINITY,
                    ..Default::default()
                };
                let mut best_order = vec![];

                // Try all ways to split this subset
                for split in subset.splits() {
                    if let (Some((left_cost, left_order)), Some((right_cost, right_order))) =
                        (dp.get(&split.0), dp.get(&split.1))
                    {
                        // Calculate join cost
                        let join_cost = self.estimate_join_cost(
                            left_cost,
                            right_cost,
                            left_order,
                            right_order,
                            joins,
                        );

                        if join_cost.total() < best_cost.total() {
                            best_cost = join_cost;
                            best_order = left_order
                                .iter()
                                .chain(right_order.iter())
                                .cloned()
                                .collect();
                        }
                    }
                }

                dp.insert(subset, (best_cost, best_order));
            }
        }

        // Return the best order for all tables
        let all_set = BitSet::all(n);
        dp.get(&all_set)
            .map(|(_, order)| order.clone())
            .ok_or_else(|| DriftError::Other("Failed to find join order".to_string()))
    }

    /// Estimate cost of joining two sub-plans
    fn estimate_join_cost(
        &self,
        left_cost: &Cost,
        right_cost: &Cost,
        left_tables: &[String],
        right_tables: &[String],
        joins: &[JoinInfo],
    ) -> Cost {
        // Find applicable join conditions
        let join_selectivity = self.estimate_join_selectivity(left_tables, right_tables, joins);

        // Estimate output rows
        let output_rows = left_cost.rows * right_cost.rows * join_selectivity;

        // Choose join algorithm based on sizes
        if right_cost.rows < 1000.0 {
            // Nested loop join for small inner
            Cost {
                io_cost: left_cost.io_cost + left_cost.rows * right_cost.io_cost,
                cpu_cost: left_cost.rows * right_cost.rows * self.params.cpu_operator_cost,
                rows: output_rows,
                size: output_rows * 100.0,
                ..Default::default()
            }
        } else if left_cost.rows + right_cost.rows < 100000.0 {
            // Hash join for medium sizes
            Cost {
                io_cost: left_cost.io_cost + right_cost.io_cost,
                cpu_cost: (left_cost.rows + right_cost.rows) * self.params.cpu_operator_cost * 2.0,
                memory: right_cost.size, // Build hash table
                rows: output_rows,
                size: output_rows * 100.0,
                ..Default::default()
            }
        } else {
            // Sort-merge for large joins
            Cost {
                io_cost: left_cost.io_cost
                    + right_cost.io_cost
                    + (left_cost.rows.log2() + right_cost.rows.log2()) * 0.1,
                cpu_cost: (left_cost.rows * left_cost.rows.log2()
                    + right_cost.rows * right_cost.rows.log2())
                    * self.params.cpu_operator_cost,
                rows: output_rows,
                size: output_rows * 100.0,
                ..Default::default()
            }
        }
    }

    /// Select appropriate indexes
    fn select_indexes(&self, plan: PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::TableScan {
                table, predicates, ..
            } => {
                // Check available indexes
                let indexes = self.indexes.read();
                if let Some(table_indexes) = indexes.get(&table) {
                    // Find best index for predicates
                    let best_index = self.find_best_index(&predicates, table_indexes);

                    if let Some(index) = best_index {
                        let stats = self.statistics.read();
                        let table_stats = stats.get(&table);

                        let cost = if let Some(ts) = table_stats {
                            let selectivity = self.estimate_predicate_selectivity(&predicates, ts);
                            let rows = ts.row_count as f64 * selectivity;
                            Cost::index_scan(
                                (index.size_pages as f64).max(1.0),
                                rows * 0.1, // Assume 10% random I/O
                                rows,
                            )
                        } else {
                            Cost::default()
                        };

                        self.stats.write().indexes_used += 1;

                        return Ok(PlanNode::IndexScan {
                            table,
                            index: index.name.clone(),
                            predicates,
                            cost,
                        });
                    }
                }

                // No suitable index, keep table scan
                Ok(PlanNode::TableScan {
                    table,
                    predicates,
                    cost: Cost::default(),
                })
            }
            _ => Ok(plan),
        }
    }

    /// Find best index for given predicates
    fn find_best_index<'a>(
        &self,
        predicates: &[Predicate],
        indexes: &'a [IndexInfo],
    ) -> Option<&'a IndexInfo> {
        let mut best_index = None;
        let mut best_score = 0;

        for index in indexes {
            let mut score = 0;
            let mut matched_prefix = true;

            // Score based on how well index matches predicate columns
            for (i, index_col) in index.columns.iter().enumerate() {
                if !matched_prefix {
                    break;
                }

                for pred in predicates {
                    if pred.column == *index_col {
                        if i == 0 {
                            score += 100; // First column match is most important
                        } else {
                            score += 50;
                        }

                        if matches!(pred.op, ComparisonOp::Eq) {
                            score += 20; // Equality is better than range
                        }
                    }
                }

                // Check if we still have matching prefix
                matched_prefix = predicates.iter().any(|p| p.column == *index_col);
            }

            if index.unique {
                score += 10; // Prefer unique indexes
            }

            if score > best_score {
                best_score = score;
                best_index = Some(index);
            }
        }

        best_index
    }

    /// Choose optimal join algorithms
    fn choose_join_algorithms(&self, plan: PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                ..
            } => {
                let left_cost = self.estimate_cost(&left)?;
                let right_cost = self.estimate_cost(&right)?;

                // Choose based on sizes
                if right_cost.rows < 1000.0 && left_cost.rows < 10000.0 {
                    // Keep nested loop for small joins
                    Ok(PlanNode::NestedLoopJoin {
                        left,
                        right,
                        condition,
                        join_type,
                        cost: self.estimate_join_cost(&left_cost, &right_cost, &[], &[], &[]),
                    })
                } else if right_cost.size < self.params.work_mem as f64 * 1024.0 {
                    // Hash join if right side fits in memory.
                    // OUTER joins force build_side = Right (see
                    // `plan_single_join` for the same constraint).
                    let build_side = match join_type {
                        JoinType::LeftOuter | JoinType::FullOuter => JoinSide::Right,
                        JoinType::Inner => JoinSide::Right,
                    };
                    Ok(PlanNode::HashJoin {
                        left,
                        right,
                        condition,
                        build_side,
                        join_type,
                        cost: self.estimate_join_cost(&left_cost, &right_cost, &[], &[], &[]),
                    })
                } else {
                    // Sort-merge for large joins
                    Ok(PlanNode::SortMergeJoin {
                        left,
                        right,
                        condition,
                        cost: self.estimate_join_cost(&left_cost, &right_cost, &[], &[], &[]),
                    })
                }
            }
            _ => Ok(plan),
        }
    }

    /// Add materialization points for complex subqueries
    fn add_materialization_points(&self, plan: PlanNode) -> Result<PlanNode> {
        // Materialize if subquery is referenced multiple times
        // or if it would benefit from creating a hash table
        Ok(plan)
    }

    /// Plan parallel execution
    fn plan_parallel_execution(&self, plan: PlanNode) -> Result<PlanNode> {
        // Add parallel scan/join nodes where beneficial
        Ok(plan)
    }

    /// Estimate cost of a plan node
    fn estimate_cost(&self, plan: &PlanNode) -> Result<Cost> {
        match plan {
            PlanNode::TableScan { cost, .. }
            | PlanNode::IndexScan { cost, .. }
            | PlanNode::HashJoin { cost, .. }
            | PlanNode::NestedLoopJoin { cost, .. }
            | PlanNode::SortMergeJoin { cost, .. } => Ok(*cost),
            _ => Ok(Cost::default()),
        }
    }

    /// Extract joins from plan
    /// Walks the plan tree and extracts all tables and join conditions
    fn extract_joins(&self, plan: &PlanNode) -> Result<(Vec<String>, Vec<JoinInfo>)> {
        let mut tables = Vec::new();
        let mut joins = Vec::new();
        self.extract_joins_recursive(plan, &mut tables, &mut joins);
        Ok((tables, joins))
    }

    /// Recursively extract tables and joins from plan tree
    #[allow(clippy::only_used_in_recursion)]
    fn extract_joins_recursive(
        &self,
        plan: &PlanNode,
        tables: &mut Vec<String>,
        joins: &mut Vec<JoinInfo>,
    ) {
        match plan {
            PlanNode::TableScan { table, .. } | PlanNode::IndexScan { table, .. } => {
                if !tables.contains(table) {
                    tables.push(table.clone());
                }
            }
            PlanNode::HashJoin {
                left,
                right,
                condition,
                ..
            }
            | PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                ..
            }
            | PlanNode::SortMergeJoin {
                left,
                right,
                condition,
                ..
            } => {
                // Extract tables from left and right subtrees
                let mut left_tables = Vec::new();
                let mut right_tables = Vec::new();
                self.extract_joins_recursive(left, &mut left_tables, joins);
                self.extract_joins_recursive(right, &mut right_tables, joins);

                // Add join info if we have tables on both sides
                if !left_tables.is_empty() && !right_tables.is_empty() {
                    joins.push(JoinInfo {
                        left_table: left_tables[0].clone(),
                        right_table: right_tables[0].clone(),
                        condition: condition.clone(),
                    });
                }

                // Merge tables into main list
                for t in left_tables {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
                for t in right_tables {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Materialize { input, .. }
            | PlanNode::Distinct { input, .. } => {
                self.extract_joins_recursive(input, tables, joins);
            }
            PlanNode::SetOperation { left, right, .. } => {
                self.extract_joins_recursive(left, tables, joins);
                self.extract_joins_recursive(right, tables, joins);
            }
        }
    }

    /// Get all table names referenced in a plan subtree
    fn get_tables_in_plan(&self, plan: &PlanNode) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables_recursive(plan, &mut tables);
        tables
    }

    /// Recursively collect table names from plan
    #[allow(clippy::only_used_in_recursion)]
    fn collect_tables_recursive(&self, plan: &PlanNode, tables: &mut HashSet<String>) {
        match plan {
            PlanNode::TableScan { table, .. } | PlanNode::IndexScan { table, .. } => {
                tables.insert(table.clone());
            }
            PlanNode::HashJoin { left, right, .. }
            | PlanNode::NestedLoopJoin { left, right, .. }
            | PlanNode::SortMergeJoin { left, right, .. }
            | PlanNode::SetOperation { left, right, .. } => {
                self.collect_tables_recursive(left, tables);
                self.collect_tables_recursive(right, tables);
            }
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Materialize { input, .. }
            | PlanNode::Distinct { input, .. } => {
                self.collect_tables_recursive(input, tables);
            }
        }
    }

    /// Split predicates for join pushdown
    /// Returns (left_predicates, right_predicates, remaining_predicates)
    fn split_join_predicates(
        &self,
        predicates: &[Predicate],
        left: &PlanNode,
        right: &PlanNode,
    ) -> (Vec<Predicate>, Vec<Predicate>, Vec<Predicate>) {
        let left_tables = self.get_tables_in_plan(left);
        let right_tables = self.get_tables_in_plan(right);

        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();
        let mut remaining = Vec::new();

        for pred in predicates {
            // Extract table name from column (assume format "table.column" or just "column")
            let pred_table = self.extract_table_from_column(&pred.column);

            // Check if predicate references a column from the left subtree
            let references_left = pred_table
                .as_ref()
                .map(|t| left_tables.contains(t))
                .unwrap_or(false);

            // Check if predicate references a column from the right subtree
            let references_right = pred_table
                .as_ref()
                .map(|t| right_tables.contains(t))
                .unwrap_or(false);

            // Also check if the predicate value references another column
            let value_references_left = match &pred.value {
                PredicateValue::Column(col) => {
                    let val_table = self.extract_table_from_column(col);
                    val_table.map(|t| left_tables.contains(&t)).unwrap_or(false)
                }
                _ => false,
            };

            let value_references_right = match &pred.value {
                PredicateValue::Column(col) => {
                    let val_table = self.extract_table_from_column(col);
                    val_table
                        .map(|t| right_tables.contains(&t))
                        .unwrap_or(false)
                }
                _ => false,
            };

            // Determine where to push the predicate
            if references_left && !references_right && !value_references_right {
                // Predicate only references left tables, push to left
                left_preds.push(pred.clone());
            } else if references_right && !references_left && !value_references_left {
                // Predicate only references right tables, push to right
                right_preds.push(pred.clone());
            } else if !references_left
                && !references_right
                && !value_references_left
                && !value_references_right
            {
                // Predicate doesn't reference any tables (constant), can go to either side
                // Push to left by convention
                left_preds.push(pred.clone());
            } else {
                // Predicate references both sides or is a cross-side join condition
                remaining.push(pred.clone());
            }
        }

        (left_preds, right_preds, remaining)
    }

    /// Extract table name from a potentially qualified column name
    /// "users.id" -> Some("users"), "id" -> None
    fn extract_table_from_column(&self, column: &str) -> Option<String> {
        column
            .find('.')
            .map(|dot_pos| column[..dot_pos].to_string())
    }

    /// Estimate join selectivity
    fn estimate_join_selectivity(
        &self,
        _left_tables: &[String],
        _right_tables: &[String],
        _joins: &[JoinInfo],
    ) -> f64 {
        0.1 // Default 10% selectivity
    }

    /// Estimate predicate selectivity
    fn estimate_predicate_selectivity(
        &self,
        predicates: &[Predicate],
        stats: &TableStatistics,
    ) -> f64 {
        let mut selectivity = 1.0;

        for pred in predicates {
            if let Some(col_stats) = stats.column_stats.get(&pred.column) {
                selectivity *= match pred.op {
                    ComparisonOp::Eq => 1.0 / col_stats.distinct_values.max(1) as f64,
                    ComparisonOp::Lt | ComparisonOp::Gt => 0.3,
                    ComparisonOp::Like => 0.25,
                    _ => 0.5,
                };
            } else {
                selectivity *= 0.3; // Default selectivity
            }
        }

        selectivity.clamp(0.001, 1.0)
    }

    /// Greedy join ordering for large queries
    fn greedy_join_order(&self, tables: &[String], _joins: &[JoinInfo]) -> Result<Vec<String>> {
        Ok(tables.to_vec())
    }

    /// Rebuild plan with new join order
    fn rebuild_with_join_order(&self, plan: PlanNode, _order: Vec<String>) -> Result<PlanNode> {
        Ok(plan)
    }
}

/// Join information
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct JoinInfo {
    left_table: String,
    right_table: String,
    condition: JoinCondition,
}

/// Bit set for dynamic programming
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct BitSet {
    bits: u64,
    size: usize,
}

impl BitSet {
    fn new(size: usize) -> Self {
        Self { bits: 0, size }
    }

    fn set(&mut self, i: usize) {
        self.bits |= 1 << i;
    }

    #[allow(dead_code)]
    fn get(&self, i: usize) -> bool {
        (self.bits & (1 << i)) != 0
    }

    fn all(size: usize) -> Self {
        Self {
            bits: (1 << size) - 1,
            size,
        }
    }

    fn count(&self) -> usize {
        self.bits.count_ones() as usize
    }

    /// Generate all subsets of a given size using Gosper's hack
    fn subsets_of_size(n: usize, size: usize) -> impl Iterator<Item = BitSet> {
        SubsetIterator::new(n, size)
    }

    /// Generate all ways to split this set into two non-empty subsets
    /// Returns pairs (s1, s2) where s1 | s2 == self and s1 & s2 == 0
    fn splits(&self) -> impl Iterator<Item = (BitSet, BitSet)> {
        SplitIterator::new(*self)
    }

    /// Subtract another bitset
    fn subtract(&self, other: &BitSet) -> BitSet {
        BitSet {
            bits: self.bits & !other.bits,
            size: self.size,
        }
    }
}

/// Iterator for generating all k-subsets of n elements
struct SubsetIterator {
    current: u64,
    max: u64,
    #[allow(dead_code)]
    size: usize,
    n: usize,
    done: bool,
}

impl SubsetIterator {
    fn new(n: usize, size: usize) -> Self {
        if size == 0 || size > n || n > 63 {
            return Self {
                current: 0,
                max: 0,
                size,
                n,
                done: true,
            };
        }

        // Start with lowest k bits set: 0...0111...1 (size ones)
        let current = (1u64 << size) - 1;
        let max = 1u64 << n;

        Self {
            current,
            max,
            size,
            n,
            done: false,
        }
    }
}

impl Iterator for SubsetIterator {
    type Item = BitSet;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let result = BitSet {
            bits: self.current,
            size: self.n,
        };

        // Generate next subset using Gosper's hack
        // Find the rightmost one bit and the rightmost zero bit to its left
        let lowest_bit = self.current & self.current.wrapping_neg();
        let ripple = self.current + lowest_bit;

        if ripple >= self.max {
            self.done = true;
        } else {
            // Calculate the new lower bits
            let new_lowest = ((self.current ^ ripple) >> 2) / lowest_bit;
            self.current = ripple | new_lowest;

            if self.current >= self.max {
                self.done = true;
            }
        }

        Some(result)
    }
}

/// Iterator for generating all non-trivial splits of a bitset
struct SplitIterator {
    original: BitSet,
    current: u64,
    done: bool,
}

impl SplitIterator {
    fn new(set: BitSet) -> Self {
        if set.count() < 2 {
            // Can't split a set with fewer than 2 elements
            return Self {
                original: set,
                current: 0,
                done: true,
            };
        }

        // Start with the first proper non-empty subset
        Self {
            original: set,
            current: 1,
            done: false,
        }
    }
}

impl Iterator for SplitIterator {
    type Item = (BitSet, BitSet);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            // Ensure we're iterating through subsets of the original set
            // Use submask iteration: (current - 1) & original
            if self.current == 0 {
                self.done = true;
                return None;
            }

            let subset_bits = self.current & self.original.bits;

            // Check if this is a valid non-empty proper subset
            if subset_bits != 0 && subset_bits != self.original.bits {
                let s1 = BitSet {
                    bits: subset_bits,
                    size: self.original.size,
                };
                let s2 = self.original.subtract(&s1);

                // To avoid duplicate pairs (s1, s2) and (s2, s1),
                // only return pairs where s1 < s2 (by bits value)
                if s1.bits < s2.bits {
                    // Move to next subset
                    self.current = (self.current.wrapping_sub(1)) & self.original.bits;
                    if self.current == 0 {
                        self.done = true;
                    }
                    return Some((s1, s2));
                }
            }

            // Move to next potential subset
            if self.current >= self.original.bits {
                self.done = true;
            } else {
                self.current += 1;
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_optimization() {
        let optimizer = QueryOptimizer::new();

        // Add some statistics
        let mut stats = TableStatistics {
            table_name: "users".to_string(),
            row_count: 10000,
            column_count: 5,
            avg_row_size: 100,
            total_size_bytes: 1_000_000,
            data_size_bytes: 900_000,
            column_stats: HashMap::new(),
            column_statistics: HashMap::new(),
            index_stats: HashMap::new(),
            last_updated: 0,
            collection_method: "analyze".to_string(),
            collection_duration_ms: 100,
        };

        stats.column_stats.insert(
            "status".to_string(),
            ColumnStatistics {
                column_name: "status".to_string(),
                distinct_values: 3,
                null_count: 0,
                min_value: None,
                max_value: None,
                histogram: None,
            },
        );

        stats.index_stats.insert(
            "status".to_string(),
            IndexStatistics {
                index_name: "status_idx".to_string(),
                unique_keys: 3,
                depth: 2,
                size_bytes: 1024,
            },
        );

        optimizer.update_statistics("users", stats);

        // Create a query
        let query = Query::Select {
            table: "users".to_string(),
            conditions: vec![WhereCondition {
                column: "status".to_string(),
                operator: "=".to_string(),
                value: serde_json::json!("active"),
            }],
            as_of: None,
            limit: Some(100),
        };

        let plan = optimizer.optimize(&query).unwrap();
        assert!(plan.uses_index);
        assert!(plan.estimated_cost > 0.0);
    }

    #[test]
    fn test_cost_model() {
        let cost_model = CostModel::default();

        assert!(cost_model.table_scan_cost(1000) > cost_model.table_scan_cost(100));
        assert!(cost_model.index_lookup_cost() < cost_model.table_scan_cost(1000));
        assert!(cost_model.sort_cost(1000) > cost_model.filter_cost(1000));
    }
}
#[cfg(test)]
mod cost_tests {
    use super::*;

    #[test]
    fn test_cost_comparison() {
        let seq_cost = Cost::seq_scan(100.0, 10000.0);
        let idx_cost = Cost::index_scan(5.0, 10.0, 100.0);

        assert!(idx_cost.total() < seq_cost.total());
    }

    #[test]
    fn test_index_selection() {
        let optimizer = CostOptimizer::new();

        let index = IndexInfo {
            name: "idx_users_email".to_string(),
            table: "users".to_string(),
            columns: vec!["email".to_string()],
            index_type: IndexType::BPlusTree,
            unique: true,
            size_pages: 10,
        };

        optimizer.register_index(index);

        let predicates = vec![Predicate {
            column: "email".to_string(),
            op: ComparisonOp::Eq,
            value: PredicateValue::Constant(serde_json::json!("test@example.com")),
            selectivity: 0.001,
        }];

        let indexes = optimizer.indexes.read();
        let table_indexes = indexes.get("users").unwrap();
        let best = optimizer.find_best_index(&predicates, table_indexes);

        assert!(best.is_some());
        assert_eq!(best.unwrap().name, "idx_users_email");
    }

    #[test]
    fn test_bitset_basic_operations() {
        let mut set = BitSet::new(5);
        assert_eq!(set.count(), 0);

        set.set(0);
        set.set(2);
        set.set(4);
        assert_eq!(set.count(), 3);
        assert!(set.get(0));
        assert!(!set.get(1));
        assert!(set.get(2));

        let all = BitSet::all(4);
        assert_eq!(all.bits, 0b1111);
        assert_eq!(all.count(), 4);
    }

    #[test]
    fn test_bitset_subsets_of_size() {
        // Test generating all 2-subsets of {0,1,2,3}
        let subsets: Vec<BitSet> = BitSet::subsets_of_size(4, 2).collect();

        // C(4,2) = 6 subsets
        assert_eq!(subsets.len(), 6);

        // Verify each subset has exactly 2 bits set
        for subset in &subsets {
            assert_eq!(subset.count(), 2);
        }

        // Verify all subsets are unique
        let bits: Vec<u64> = subsets.iter().map(|s| s.bits).collect();
        let mut unique = bits.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(bits.len(), unique.len());
    }

    #[test]
    fn test_bitset_subsets_of_size_3() {
        // Test generating all 3-subsets of {0,1,2,3,4}
        let subsets: Vec<BitSet> = BitSet::subsets_of_size(5, 3).collect();

        // C(5,3) = 10 subsets
        assert_eq!(subsets.len(), 10);

        // Verify each subset has exactly 3 bits set
        for subset in &subsets {
            assert_eq!(subset.count(), 3);
        }
    }

    #[test]
    fn test_bitset_subsets_edge_cases() {
        // Size 0 should produce no subsets
        let empty: Vec<BitSet> = BitSet::subsets_of_size(4, 0).collect();
        assert!(empty.is_empty());

        // Size > n should produce no subsets
        let invalid: Vec<BitSet> = BitSet::subsets_of_size(3, 5).collect();
        assert!(invalid.is_empty());

        // Size 1 should produce n subsets
        let singles: Vec<BitSet> = BitSet::subsets_of_size(4, 1).collect();
        assert_eq!(singles.len(), 4);
    }

    #[test]
    fn test_bitset_splits() {
        let mut set = BitSet::new(4);
        set.set(0);
        set.set(1);
        set.set(2);
        // Set = {0, 1, 2}

        let splits: Vec<(BitSet, BitSet)> = set.splits().collect();

        // For a 3-element set, there should be 3 ways to split:
        // {0} | {1,2}, {1} | {0,2}, {2} | {0,1}
        // But we only count each pair once (s1 < s2)
        assert!(!splits.is_empty());

        // Verify all splits are valid partitions
        for (s1, s2) in &splits {
            // s1 and s2 should be disjoint
            assert_eq!(s1.bits & s2.bits, 0);
            // s1 | s2 should equal original set
            assert_eq!(s1.bits | s2.bits, set.bits);
            // Both should be non-empty
            assert!(s1.count() > 0);
            assert!(s2.count() > 0);
        }
    }

    #[test]
    fn test_bitset_subtract() {
        let mut a = BitSet::new(4);
        a.set(0);
        a.set(1);
        a.set(2);

        let mut b = BitSet::new(4);
        b.set(1);

        let result = a.subtract(&b);
        assert_eq!(result.bits, 0b0101); // {0, 2}
        assert!(result.get(0));
        assert!(!result.get(1));
        assert!(result.get(2));
    }

    #[test]
    fn test_extract_joins() {
        let optimizer = CostOptimizer::new();

        // Create a simple join plan
        let left_scan = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let right_scan = PlanNode::TableScan {
            table: "orders".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let join = PlanNode::HashJoin {
            left: Box::new(left_scan),
            right: Box::new(right_scan),
            condition: JoinCondition {
                left_col: "users.id".to_string(),
                right_col: "orders.user_id".to_string(),
                op: ComparisonOp::Eq,
                raw_text: None,
            },
            build_side: JoinSide::Right,
            join_type: JoinType::Inner,
            cost: Cost::default(),
        };

        let (tables, joins) = optimizer.extract_joins(&join).unwrap();

        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
        assert_eq!(joins.len(), 1);
    }

    #[test]
    fn test_split_join_predicates() {
        let optimizer = CostOptimizer::new();

        let left = PlanNode::TableScan {
            table: "users".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let right = PlanNode::TableScan {
            table: "orders".to_string(),
            predicates: vec![],
            cost: Cost::default(),
        };

        let predicates = vec![
            // Should go to left (only references users)
            Predicate {
                column: "users.status".to_string(),
                op: ComparisonOp::Eq,
                value: PredicateValue::Constant(serde_json::json!("active")),
                selectivity: 0.5,
            },
            // Should go to right (only references orders)
            Predicate {
                column: "orders.total".to_string(),
                op: ComparisonOp::Gt,
                value: PredicateValue::Constant(serde_json::json!(100)),
                selectivity: 0.3,
            },
            // Should remain (references both sides)
            Predicate {
                column: "users.id".to_string(),
                op: ComparisonOp::Eq,
                value: PredicateValue::Column("orders.user_id".to_string()),
                selectivity: 0.1,
            },
        ];

        let (left_preds, right_preds, remaining) =
            optimizer.split_join_predicates(&predicates, &left, &right);

        assert_eq!(left_preds.len(), 1);
        assert_eq!(left_preds[0].column, "users.status");

        assert_eq!(right_preds.len(), 1);
        assert_eq!(right_preds[0].column, "orders.total");

        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].column, "users.id");
    }

    #[test]
    fn test_extract_table_from_column() {
        let optimizer = CostOptimizer::new();

        assert_eq!(
            optimizer.extract_table_from_column("users.id"),
            Some("users".to_string())
        );
        assert_eq!(
            optimizer.extract_table_from_column("orders.total"),
            Some("orders".to_string())
        );
        assert_eq!(optimizer.extract_table_from_column("id"), None);
        assert_eq!(optimizer.extract_table_from_column("column_name"), None);
    }
}
