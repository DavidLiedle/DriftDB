//! Parallel Query Execution Module
//!
//! Provides parallel processing capabilities for queries to improve performance
//! on multi-core systems.
//! Features:
//! - Data partitioning for parallel processing
//! - Thread pool management
//! - Result aggregation
//! - Adaptive parallelism based on data size

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use rayon::prelude::*;
use serde_json::Value;
use tracing::{debug, trace, warn};

use crate::errors::{DriftError, Result};
use crate::query::WhereCondition;

/// Helper function to compare JSON values
fn compare_json_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use serde_json::Value as V;
    use std::cmp::Ordering;

    match (a, b) {
        (V::Null, V::Null) => Ordering::Equal,
        (V::Null, _) => Ordering::Less,
        (_, V::Null) => Ordering::Greater,
        (V::Bool(a), V::Bool(b)) => a.cmp(b),
        (V::Number(a), V::Number(b)) => {
            // Handle numeric comparison
            match (a.as_f64(), b.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            }
        }
        (V::String(a), V::String(b)) => a.cmp(b),
        (V::Array(a), V::Array(b)) => a.len().cmp(&b.len()),
        (V::Object(a), V::Object(b)) => a.len().cmp(&b.len()),
        // Mixed types - convert to string and compare
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// Configuration for parallel execution
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum number of worker threads
    pub max_threads: usize,
    /// Minimum number of rows to trigger parallel execution
    pub min_rows_for_parallel: usize,
    /// Chunk size for data partitioning
    pub chunk_size: usize,
    /// Enable parallel execution for aggregations
    pub parallel_aggregations: bool,
    /// Enable parallel execution for joins
    pub parallel_joins: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            max_threads: num_cpus,
            min_rows_for_parallel: 1000,
            chunk_size: 5000,
            parallel_aggregations: true,
            parallel_joins: true,
        }
    }
}

/// Statistics for parallel execution
#[derive(Debug, Clone, Default)]
pub struct ParallelStats {
    pub queries_executed: u64,
    pub parallel_queries: u64,
    pub sequential_queries: u64,
    pub total_rows_processed: u64,
    pub avg_speedup: f64,
}

/// Parallel query executor
pub struct ParallelExecutor {
    config: ParallelConfig,
    thread_pool: rayon::ThreadPool,
    stats: Arc<RwLock<ParallelStats>>,
}

impl ParallelExecutor {
    /// Create a new parallel executor
    pub fn new(config: ParallelConfig) -> Result<Self> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.max_threads)
            .thread_name(|i| format!("driftdb-parallel-{}", i))
            .build()
            .map_err(|e| DriftError::Internal(format!("Failed to create thread pool: {}", e)))?;

        Ok(Self {
            config,
            thread_pool,
            stats: Arc::new(RwLock::new(ParallelStats::default())),
        })
    }

    /// Execute a SELECT query in parallel
    pub fn parallel_select(
        &self,
        data: Vec<(Value, Value)>, // (primary_key, row)
        conditions: &[WhereCondition],
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let data_len = data.len();
        let mut stats = self.stats.write();
        stats.queries_executed += 1;
        stats.total_rows_processed += data_len as u64;

        // Decide whether to use parallel execution
        if data_len < self.config.min_rows_for_parallel {
            debug!("Using sequential execution for {} rows", data_len);
            stats.sequential_queries += 1;
            return self.sequential_select(data, conditions, limit);
        }

        debug!("Using parallel execution for {} rows", data_len);
        stats.parallel_queries += 1;
        let start = std::time::Instant::now();

        // Execute in parallel using rayon
        let results = self.thread_pool.install(|| {
            data.into_par_iter()
                .filter(|(_, row)| Self::matches_conditions(row, conditions))
                .map(|(_, row)| row)
                .collect::<Vec<Value>>()
        });

        // Apply limit if specified
        let mut results = results;
        if let Some(limit) = limit {
            results.truncate(limit);
        }

        let elapsed = start.elapsed();
        trace!("Parallel select completed in {:?}", elapsed);

        Ok(results)
    }

    /// Sequential select for small datasets
    fn sequential_select(
        &self,
        data: Vec<(Value, Value)>,
        conditions: &[WhereCondition],
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let mut results: Vec<Value> = data
            .into_iter()
            .filter(|(_, row)| Self::matches_conditions(row, conditions))
            .map(|(_, row)| row)
            .collect();

        if let Some(limit) = limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Execute an aggregation query in parallel
    pub fn parallel_aggregate(
        &self,
        data: Vec<Value>,
        group_by: Option<&str>,
        aggregations: &[AggregateFunction],
    ) -> Result<Vec<Value>> {
        if !self.config.parallel_aggregations || data.len() < self.config.min_rows_for_parallel {
            return self.sequential_aggregate(data, group_by, aggregations);
        }

        debug!("Executing parallel aggregation on {} rows", data.len());

        // Group data if needed
        let groups = if let Some(group_key) = group_by {
            self.parallel_group_by(data, group_key)?
        } else {
            vec![("_all".to_string(), data)]
        };

        // Perform aggregations in parallel
        let results = self.thread_pool.install(|| {
            groups
                .into_par_iter()
                .map(|(key, group_data)| {
                    let mut result = serde_json::json!({});

                    if group_by.is_some() {
                        result["group"] = Value::String(key);
                    }

                    for agg in aggregations {
                        let value = self.compute_aggregate(&group_data, agg);
                        result[&agg.output_name] = value;
                    }

                    result
                })
                .collect::<Vec<Value>>()
        });

        Ok(results)
    }

    /// Sequential aggregation for small datasets
    fn sequential_aggregate(
        &self,
        data: Vec<Value>,
        group_by: Option<&str>,
        aggregations: &[AggregateFunction],
    ) -> Result<Vec<Value>> {
        let groups = if let Some(group_key) = group_by {
            self.group_by(data, group_key)?
        } else {
            vec![("_all".to_string(), data)]
        };

        let results = groups
            .into_iter()
            .map(|(key, group_data)| {
                let mut result = serde_json::json!({});

                if group_by.is_some() {
                    result["group"] = Value::String(key);
                }

                for agg in aggregations {
                    let value = self.compute_aggregate(&group_data, agg);
                    result[&agg.output_name] = value;
                }

                result
            })
            .collect();

        Ok(results)
    }

    /// Group data by a column value (parallel)
    fn parallel_group_by(
        &self,
        data: Vec<Value>,
        group_key: &str,
    ) -> Result<Vec<(String, Vec<Value>)>> {
        let groups: HashMap<String, Vec<Value>> = self.thread_pool.install(|| {
            data.into_par_iter()
                .fold(HashMap::new, |mut acc, row| {
                    if let Some(key_value) = row.get(group_key) {
                        let key = key_value.to_string();
                        acc.entry(key).or_insert_with(Vec::new).push(row);
                    }
                    acc
                })
                .reduce(HashMap::new, |mut acc, map| {
                    for (key, mut values) in map {
                        acc.entry(key).or_insert_with(Vec::new).append(&mut values);
                    }
                    acc
                })
        });

        Ok(groups.into_iter().collect())
    }

    /// Group data by a column value (sequential)
    fn group_by(&self, data: Vec<Value>, group_key: &str) -> Result<Vec<(String, Vec<Value>)>> {
        let mut groups: HashMap<String, Vec<Value>> = HashMap::new();

        for row in data {
            if let Some(key_value) = row.get(group_key) {
                let key = key_value.to_string();
                groups.entry(key).or_default().push(row);
            }
        }

        Ok(groups.into_iter().collect())
    }

    /// Compute an aggregate function on a dataset
    fn compute_aggregate(&self, data: &[Value], func: &AggregateFunction) -> Value {
        match func.function_type {
            AggregateType::Count => Value::Number(data.len().into()),
            AggregateType::Sum => {
                let sum: f64 = data
                    .iter()
                    .filter_map(|row| row.get(&func.column))
                    .filter_map(|v| v.as_f64())
                    .sum();
                serde_json::json!(sum)
            }
            AggregateType::Avg => {
                let values: Vec<f64> = data
                    .iter()
                    .filter_map(|row| row.get(&func.column))
                    .filter_map(|v| v.as_f64())
                    .collect();

                if values.is_empty() {
                    Value::Null
                } else {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    serde_json::json!(avg)
                }
            }
            AggregateType::Min => data
                .iter()
                .filter_map(|row| row.get(&func.column))
                .min_by(|a, b| compare_json_values(a, b))
                .cloned()
                .unwrap_or(Value::Null),
            AggregateType::Max => data
                .iter()
                .filter_map(|row| row.get(&func.column))
                .max_by(|a, b| compare_json_values(a, b))
                .cloned()
                .unwrap_or(Value::Null),
        }
    }

    /// Execute a JOIN operation in parallel
    pub fn parallel_join(
        &self,
        left_data: Vec<Value>,
        right_data: Vec<Value>,
        join_type: JoinType,
        left_key: &str,
        right_key: &str,
    ) -> Result<Vec<Value>> {
        if !self.config.parallel_joins || left_data.len() < self.config.min_rows_for_parallel {
            return self.sequential_join(left_data, right_data, join_type, left_key, right_key);
        }

        debug!(
            "Executing parallel join: {} x {} rows",
            left_data.len(),
            right_data.len()
        );

        // Build hash index on the smaller dataset
        let (build_data, probe_data, build_key, probe_key) = if left_data.len() <= right_data.len()
        {
            (left_data, right_data, left_key, right_key)
        } else {
            (right_data, left_data, right_key, left_key)
        };

        // Build hash index in parallel
        let index: HashMap<String, Vec<Value>> = self.thread_pool.install(|| {
            build_data
                .into_par_iter()
                .fold(HashMap::new, |mut acc, row| {
                    if let Some(key_value) = row.get(build_key) {
                        let key = key_value.to_string();
                        acc.entry(key).or_insert_with(Vec::new).push(row);
                    }
                    acc
                })
                .reduce(HashMap::new, |mut acc, map| {
                    for (key, mut values) in map {
                        acc.entry(key).or_insert_with(Vec::new).append(&mut values);
                    }
                    acc
                })
        });

        // Probe and join in parallel
        let results = self.thread_pool.install(|| {
            probe_data
                .into_par_iter()
                .flat_map(|probe_row| {
                    let key_value = match probe_row.get(probe_key) {
                        Some(val) => val.to_string(),
                        None => return vec![],
                    };

                    if let Some(matching_rows) = index.get(&key_value) {
                        matching_rows
                            .iter()
                            .map(|build_row| {
                                let mut result = serde_json::json!({});

                                // Merge both rows
                                if let (Value::Object(left_map), Value::Object(right_map)) =
                                    (probe_row.clone(), build_row.clone())
                                {
                                    for (k, v) in left_map {
                                        result[format!("left_{}", k)] = v;
                                    }
                                    for (k, v) in right_map {
                                        result[format!("right_{}", k)] = v;
                                    }
                                }

                                result
                            })
                            .collect::<Vec<Value>>()
                    } else if join_type == JoinType::LeftOuter {
                        vec![probe_row.clone()]
                    } else {
                        vec![]
                    }
                })
                .collect()
        });

        Ok(results)
    }

    /// Sequential join for small datasets
    fn sequential_join(
        &self,
        left_data: Vec<Value>,
        right_data: Vec<Value>,
        join_type: JoinType,
        left_key: &str,
        right_key: &str,
    ) -> Result<Vec<Value>> {
        let mut results = Vec::new();

        for left_row in &left_data {
            let left_key_value = left_row.get(left_key);
            let mut matched = false;

            for right_row in &right_data {
                if let (Some(lk), Some(rk)) = (left_key_value, right_row.get(right_key)) {
                    if lk == rk {
                        matched = true;
                        let mut result = serde_json::json!({});

                        // Merge both rows
                        if let (Value::Object(left_map), Value::Object(right_map)) =
                            (left_row.clone(), right_row.clone())
                        {
                            for (k, v) in left_map {
                                result[format!("left_{}", k)] = v;
                            }
                            for (k, v) in right_map {
                                result[format!("right_{}", k)] = v;
                            }
                        }

                        results.push(result);
                    }
                }
            }

            if !matched && join_type == JoinType::LeftOuter {
                results.push(left_row.clone());
            }
        }

        Ok(results)
    }

    /// Check if a row matches the given conditions
    fn matches_conditions(row: &Value, conditions: &[WhereCondition]) -> bool {
        conditions.iter().all(|cond| {
            if let Some(field_value) = row.get(&cond.column) {
                match cond.operator.as_str() {
                    "=" | "==" => field_value == &cond.value,
                    "!=" | "<>" => field_value != &cond.value,
                    ">" => {
                        if let (Some(a), Some(b)) = (field_value.as_f64(), cond.value.as_f64()) {
                            a > b
                        } else {
                            compare_json_values(field_value, &cond.value)
                                == std::cmp::Ordering::Greater
                        }
                    }
                    "<" => {
                        if let (Some(a), Some(b)) = (field_value.as_f64(), cond.value.as_f64()) {
                            a < b
                        } else {
                            compare_json_values(field_value, &cond.value)
                                == std::cmp::Ordering::Less
                        }
                    }
                    ">=" => {
                        if let (Some(a), Some(b)) = (field_value.as_f64(), cond.value.as_f64()) {
                            a >= b
                        } else {
                            let ord = compare_json_values(field_value, &cond.value);
                            ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal
                        }
                    }
                    "<=" => {
                        if let (Some(a), Some(b)) = (field_value.as_f64(), cond.value.as_f64()) {
                            a <= b
                        } else {
                            let ord = compare_json_values(field_value, &cond.value);
                            ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal
                        }
                    }
                    "LIKE" => {
                        if let (Some(text), Some(pattern)) =
                            (field_value.as_str(), cond.value.as_str())
                        {
                            // Simple LIKE pattern matching (% = any chars, _ = single char)
                            let pattern = pattern.replace("%", ".*").replace("_", ".");
                            regex::Regex::new(&format!("^{}$", pattern))
                                .map(|re| re.is_match(text))
                                .unwrap_or(false)
                        } else {
                            false
                        }
                    }
                    "IN" => {
                        if let Some(array) = cond.value.as_array() {
                            array.contains(field_value)
                        } else {
                            false
                        }
                    }
                    "NOT IN" => {
                        if let Some(array) = cond.value.as_array() {
                            !array.contains(field_value)
                        } else {
                            true
                        }
                    }
                    "IS NULL" => field_value.is_null(),
                    "IS NOT NULL" => !field_value.is_null(),
                    _ => {
                        warn!("Unsupported operator: {}", cond.operator);
                        false
                    }
                }
            } else {
                // Field doesn't exist
                cond.operator == "IS NULL"
            }
        })
    }

    /// Get execution statistics
    pub fn statistics(&self) -> ParallelStats {
        self.stats.read().clone()
    }

    /// Reset statistics
    pub fn reset_statistics(&self) {
        *self.stats.write() = ParallelStats::default();
    }
}

/// Aggregate function definition
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    pub function_type: AggregateType,
    pub column: String,
    pub output_name: String,
}

/// Types of aggregate functions
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Types of JOIN operations
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    Full,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parallel_select() {
        let executor = ParallelExecutor::new(ParallelConfig {
            min_rows_for_parallel: 2,
            ..Default::default()
        })
        .unwrap();

        let data = vec![
            (json!(1), json!({"id": 1, "name": "Alice", "age": 30})),
            (json!(2), json!({"id": 2, "name": "Bob", "age": 25})),
            (json!(3), json!({"id": 3, "name": "Charlie", "age": 35})),
        ];

        let conditions = vec![WhereCondition {
            column: "age".to_string(),
            operator: ">".to_string(),
            value: json!(25),
        }];

        let results = executor.parallel_select(data, &conditions, None).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parallel_aggregate() {
        let executor = ParallelExecutor::new(ParallelConfig::default()).unwrap();

        let data = vec![
            json!({"category": "A", "value": 10}),
            json!({"category": "B", "value": 20}),
            json!({"category": "A", "value": 15}),
            json!({"category": "B", "value": 25}),
        ];

        let aggregations = vec![
            AggregateFunction {
                function_type: AggregateType::Sum,
                column: "value".to_string(),
                output_name: "total_value".to_string(),
            },
            AggregateFunction {
                function_type: AggregateType::Count,
                column: "value".to_string(),
                output_name: "count".to_string(),
            },
        ];

        let results = executor
            .parallel_aggregate(data, Some("category"), &aggregations)
            .unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parallel_join() {
        let executor = ParallelExecutor::new(ParallelConfig::default()).unwrap();

        let left_data = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];

        let right_data = vec![
            json!({"user_id": 1, "order": "A123"}),
            json!({"user_id": 1, "order": "A124"}),
            json!({"user_id": 2, "order": "B456"}),
        ];

        let results = executor
            .parallel_join(left_data, right_data, JoinType::Inner, "id", "user_id")
            .unwrap();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_condition_matching() {
        let row = json!({
            "name": "Alice",
            "age": 30,
            "city": "New York",
            "tags": ["developer", "rust"],
        });

        // Test equality
        assert!(ParallelExecutor::matches_conditions(
            &row,
            &[WhereCondition {
                column: "name".to_string(),
                operator: "=".to_string(),
                value: json!("Alice"),
            }]
        ));

        // Test greater than
        assert!(ParallelExecutor::matches_conditions(
            &row,
            &[WhereCondition {
                column: "age".to_string(),
                operator: ">".to_string(),
                value: json!(25),
            }]
        ));

        // Test IN operator
        assert!(ParallelExecutor::matches_conditions(
            &row,
            &[WhereCondition {
                column: "city".to_string(),
                operator: "IN".to_string(),
                value: json!(["New York", "Boston", "Chicago"]),
            }]
        ));
    }
}
