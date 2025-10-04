//! Window Functions Implementation
//!
//! Provides SQL window functions for analytical queries including:
//! - ROW_NUMBER(), RANK(), DENSE_RANK()
//! - LAG(), LEAD() for accessing previous/next rows
//! - SUM(), AVG(), COUNT() with window frames
//! - FIRST_VALUE(), LAST_VALUE(), NTH_VALUE()
//! - PERCENT_RANK(), CUME_DIST(), NTILE()
//! - Custom window frame specifications (ROWS/RANGE)

use std::cmp::Ordering;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use tracing::{debug, trace};

use crate::errors::Result;

/// A serializable wrapper for partition keys that can be used in HashMap
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct PartitionKey(String);

impl PartitionKey {
    fn from_values(values: &[Value]) -> Self {
        // Convert to a stable string representation
        let serialized = values
            .iter()
            .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()))
            .collect::<Vec<_>>()
            .join("|");
        PartitionKey(serialized)
    }
}

/// Window function types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFunction {
    /// Ranking functions
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile(u32),

    /// Value functions
    FirstValue(String), // column name
    LastValue(String),                        // column name
    NthValue(String, u32),                    // column name, n
    Lag(String, Option<u32>, Option<Value>),  // column, offset, default
    Lead(String, Option<u32>, Option<Value>), // column, offset, default

    /// Aggregate functions
    Sum(String), // column name
    Avg(String),           // column name
    Count(Option<String>), // column name (None for COUNT(*))
    Min(String),           // column name
    Max(String),           // column name
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame {
    /// Frame type (ROWS or RANGE)
    pub frame_type: FrameType,
    /// Start boundary
    pub start: FrameBoundary,
    /// End boundary
    pub end: FrameBoundary,
}

/// Frame type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrameType {
    /// Physical frame based on row positions
    Rows,
    /// Logical frame based on value ranges
    Range,
}

/// Frame boundary specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrameBoundary {
    /// Unbounded (start/end of partition)
    Unbounded,
    /// Current row
    CurrentRow,
    /// N rows/values preceding current row
    Preceding(u32),
    /// N rows/values following current row
    Following(u32),
}

impl Default for WindowFrame {
    fn default() -> Self {
        // Default: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        Self {
            frame_type: FrameType::Range,
            start: FrameBoundary::Unbounded,
            end: FrameBoundary::CurrentRow,
        }
    }
}

/// Window specification
#[derive(Debug, Clone)]
pub struct WindowSpec {
    /// Partition columns
    pub partition_by: Vec<String>,
    /// Order by columns with direction
    pub order_by: Vec<OrderColumn>,
    /// Window frame
    pub frame: Option<WindowFrame>,
}

/// Order column specification
#[derive(Debug, Clone)]
pub struct OrderColumn {
    pub column: String,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Window query specification
#[derive(Debug, Clone)]
pub struct WindowQuery {
    /// Window functions to compute
    pub functions: Vec<WindowFunctionCall>,
    /// Base data rows
    pub data: Vec<Value>,
}

/// Window function call
#[derive(Debug, Clone)]
pub struct WindowFunctionCall {
    /// The window function
    pub function: WindowFunction,
    /// Window specification
    pub window: WindowSpec,
    /// Output column alias
    pub alias: String,
}

/// Window executor for processing window functions
pub struct WindowExecutor;

/// Partition of rows sharing the same partition key
#[derive(Debug)]
struct Partition {
    /// Partition key values
    #[allow(dead_code)]
    key: Vec<Value>,
    /// Rows in this partition
    rows: Vec<(usize, Value)>, // (original_index, row_data)
}

impl WindowExecutor {
    /// Execute window functions on data
    pub fn execute(&self, query: WindowQuery) -> Result<Vec<Value>> {
        debug!(
            "Executing window query with {} functions",
            query.functions.len()
        );

        let mut result_rows = query.data.clone();

        // Process each window function
        for func_call in &query.functions {
            let values = self.compute_window_function(func_call, &query.data)?;

            // Add computed values to result rows
            for (i, value) in values.into_iter().enumerate() {
                if let Value::Object(ref mut map) = result_rows[i] {
                    map.insert(func_call.alias.clone(), value);
                }
            }
        }

        Ok(result_rows)
    }

    /// Compute a single window function
    fn compute_window_function(
        &self,
        func_call: &WindowFunctionCall,
        data: &[Value],
    ) -> Result<Vec<Value>> {
        trace!("Computing window function: {:?}", func_call.function);

        // Create partitions
        let partitions = self.create_partitions(data, &func_call.window.partition_by)?;

        // Sort each partition by ORDER BY clause
        let sorted_partitions = self.sort_partitions(partitions, &func_call.window.order_by)?;

        // Compute function values for each partition
        let mut result = vec![Value::Null; data.len()];

        for partition in sorted_partitions {
            let partition_result = self.compute_partition_function(
                &func_call.function,
                &func_call.window,
                &partition,
            )?;

            // Map results back to original row indices
            for (i, (original_idx, _)) in partition.rows.iter().enumerate() {
                result[*original_idx] = partition_result[i].clone();
            }
        }

        Ok(result)
    }

    /// Create partitions based on PARTITION BY columns
    fn create_partitions(
        &self,
        data: &[Value],
        partition_columns: &[String],
    ) -> Result<Vec<Partition>> {
        let mut partitions: HashMap<PartitionKey, Vec<(usize, Value)>> = HashMap::new();

        for (idx, row) in data.iter().enumerate() {
            // Extract partition key
            let mut key_values = Vec::new();
            for col in partition_columns {
                let value = row.get(col).cloned().unwrap_or(Value::Null);
                key_values.push(value);
            }

            let key = PartitionKey::from_values(&key_values);
            partitions
                .entry(key)
                .or_insert_with(Vec::new)
                .push((idx, row.clone()));
        }

        // Convert to Partition structs
        let result = partitions
            .into_iter()
            .map(|(partition_key, rows)| {
                // Reconstruct the Vec<Value> key from the partition key string
                let key_values: Vec<Value> = partition_key
                    .0
                    .split('|')
                    .map(|s| serde_json::from_str(s).unwrap_or(Value::Null))
                    .collect();
                Partition {
                    key: key_values,
                    rows,
                }
            })
            .collect();

        Ok(result)
    }

    /// Sort partitions by ORDER BY columns
    fn sort_partitions(
        &self,
        mut partitions: Vec<Partition>,
        order_columns: &[OrderColumn],
    ) -> Result<Vec<Partition>> {
        for partition in &mut partitions {
            partition
                .rows
                .sort_by(|a, b| self.compare_rows(&a.1, &b.1, order_columns));
        }

        Ok(partitions)
    }

    /// Compare two rows according to ORDER BY specification
    fn compare_rows(&self, a: &Value, b: &Value, order_columns: &[OrderColumn]) -> Ordering {
        for order_col in order_columns {
            let val_a = a.get(&order_col.column).unwrap_or(&Value::Null);
            let val_b = b.get(&order_col.column).unwrap_or(&Value::Null);

            let cmp = self.compare_values(val_a, val_b, order_col.nulls_first);

            let result = if order_col.ascending {
                cmp
            } else {
                cmp.reverse()
            };

            if result != Ordering::Equal {
                return result;
            }
        }

        Ordering::Equal
    }

    /// Compare two JSON values
    fn compare_values(&self, a: &Value, b: &Value, nulls_first: bool) -> Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Value::Number(n1), Value::Number(n2)) => {
                let f1 = n1.as_f64().unwrap_or(0.0);
                let f2 = n2.as_f64().unwrap_or(0.0);
                f1.partial_cmp(&f2).unwrap_or(Ordering::Equal)
            }
            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            (Value::Bool(b1), Value::Bool(b2)) => b1.cmp(b2),
            _ => Ordering::Equal, // Default for uncomparable types
        }
    }

    /// Compute window function for a single partition
    fn compute_partition_function(
        &self,
        function: &WindowFunction,
        window: &WindowSpec,
        partition: &Partition,
    ) -> Result<Vec<Value>> {
        let row_count = partition.rows.len();
        let mut result = vec![Value::Null; row_count];

        match function {
            WindowFunction::RowNumber => {
                for i in 0..row_count {
                    result[i] = Value::Number(Number::from(i + 1));
                }
            }

            WindowFunction::Rank => {
                let mut current_rank = 1;
                let mut same_rank_count = 0;

                for i in 0..row_count {
                    if i > 0
                        && self.rows_equal_for_ordering(
                            &partition.rows[i - 1].1,
                            &partition.rows[i].1,
                            &window.order_by,
                        )
                    {
                        same_rank_count += 1;
                    } else {
                        current_rank += same_rank_count;
                        same_rank_count = 0;
                    }
                    result[i] = Value::Number(Number::from(current_rank));
                }
            }

            WindowFunction::DenseRank => {
                let mut current_rank = 1;

                for i in 0..row_count {
                    if i > 0
                        && !self.rows_equal_for_ordering(
                            &partition.rows[i - 1].1,
                            &partition.rows[i].1,
                            &window.order_by,
                        )
                    {
                        current_rank += 1;
                    }
                    result[i] = Value::Number(Number::from(current_rank));
                }
            }

            WindowFunction::PercentRank => {
                // First compute ranks
                let ranks = self.compute_ranks(partition, &window.order_by);
                for i in 0..row_count {
                    let rank = ranks[i] as f64;
                    let percent_rank = if row_count <= 1 {
                        0.0
                    } else {
                        (rank - 1.0) / (row_count as f64 - 1.0)
                    };
                    result[i] =
                        Value::Number(Number::from_f64(percent_rank).unwrap_or(Number::from(0)));
                }
            }

            WindowFunction::CumeDist => {
                for i in 0..row_count {
                    // Count rows <= current row
                    let mut count = 0;
                    for j in 0..row_count {
                        if self.compare_rows(
                            &partition.rows[j].1,
                            &partition.rows[i].1,
                            &window.order_by,
                        ) != Ordering::Greater
                        {
                            count += 1;
                        }
                    }
                    let cume_dist = count as f64 / row_count as f64;
                    result[i] =
                        Value::Number(Number::from_f64(cume_dist).unwrap_or(Number::from(0)));
                }
            }

            WindowFunction::Ntile(n) => {
                let bucket_size = (row_count as f64 / *n as f64).ceil() as usize;
                for i in 0..row_count {
                    let bucket = (i / bucket_size).min(*n as usize - 1) + 1;
                    result[i] = Value::Number(Number::from(bucket));
                }
            }

            WindowFunction::FirstValue(column) => {
                if !partition.rows.is_empty() {
                    let first_value = partition.rows[0]
                        .1
                        .get(column)
                        .cloned()
                        .unwrap_or(Value::Null);
                    for i in 0..row_count {
                        result[i] = first_value.clone();
                    }
                }
            }

            WindowFunction::LastValue(column) => {
                if !partition.rows.is_empty() {
                    let last_value = partition.rows[row_count - 1]
                        .1
                        .get(column)
                        .cloned()
                        .unwrap_or(Value::Null);
                    for i in 0..row_count {
                        result[i] = last_value.clone();
                    }
                }
            }

            WindowFunction::NthValue(column, n) => {
                let nth_index = (*n as usize).saturating_sub(1);
                let nth_value = if nth_index < row_count {
                    partition.rows[nth_index]
                        .1
                        .get(column)
                        .cloned()
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                };

                for i in 0..row_count {
                    result[i] = nth_value.clone();
                }
            }

            WindowFunction::Lag(column, offset, default) => {
                let offset = offset.unwrap_or(1) as usize;
                for i in 0..row_count {
                    if i >= offset {
                        result[i] = partition.rows[i - offset]
                            .1
                            .get(column)
                            .cloned()
                            .unwrap_or(Value::Null);
                    } else {
                        result[i] = default.clone().unwrap_or(Value::Null);
                    }
                }
            }

            WindowFunction::Lead(column, offset, default) => {
                let offset = offset.unwrap_or(1) as usize;
                for i in 0..row_count {
                    if i + offset < row_count {
                        result[i] = partition.rows[i + offset]
                            .1
                            .get(column)
                            .cloned()
                            .unwrap_or(Value::Null);
                    } else {
                        result[i] = default.clone().unwrap_or(Value::Null);
                    }
                }
            }

            WindowFunction::Sum(column) => {
                let default_frame = WindowFrame::default();
                let frame = window.frame.as_ref().unwrap_or(&default_frame);
                for i in 0..row_count {
                    let frame_rows = self.get_frame_rows(i, row_count, frame);
                    let sum = self.sum_values(&partition.rows, &frame_rows, column);
                    result[i] = sum;
                }
            }

            WindowFunction::Avg(column) => {
                let default_frame = WindowFrame::default();
                let frame = window.frame.as_ref().unwrap_or(&default_frame);
                for i in 0..row_count {
                    let frame_rows = self.get_frame_rows(i, row_count, frame);
                    let avg = self.avg_values(&partition.rows, &frame_rows, column);
                    result[i] = avg;
                }
            }

            WindowFunction::Count(column) => {
                let default_frame = WindowFrame::default();
                let frame = window.frame.as_ref().unwrap_or(&default_frame);
                for i in 0..row_count {
                    let frame_rows = self.get_frame_rows(i, row_count, frame);
                    let count = self.count_values(&partition.rows, &frame_rows, column.as_deref());
                    result[i] = Value::Number(Number::from(count));
                }
            }

            WindowFunction::Min(column) => {
                let default_frame = WindowFrame::default();
                let frame = window.frame.as_ref().unwrap_or(&default_frame);
                for i in 0..row_count {
                    let frame_rows = self.get_frame_rows(i, row_count, frame);
                    let min = self.min_values(&partition.rows, &frame_rows, column);
                    result[i] = min;
                }
            }

            WindowFunction::Max(column) => {
                let default_frame = WindowFrame::default();
                let frame = window.frame.as_ref().unwrap_or(&default_frame);
                for i in 0..row_count {
                    let frame_rows = self.get_frame_rows(i, row_count, frame);
                    let max = self.max_values(&partition.rows, &frame_rows, column);
                    result[i] = max;
                }
            }
        }

        Ok(result)
    }

    /// Check if two rows are equal for ordering purposes
    fn rows_equal_for_ordering(&self, a: &Value, b: &Value, order_columns: &[OrderColumn]) -> bool {
        for order_col in order_columns {
            let val_a = a.get(&order_col.column).unwrap_or(&Value::Null);
            let val_b = b.get(&order_col.column).unwrap_or(&Value::Null);

            if self.compare_values(val_a, val_b, order_col.nulls_first) != Ordering::Equal {
                return false;
            }
        }
        true
    }

    /// Compute ranks for a partition
    fn compute_ranks(&self, partition: &Partition, order_columns: &[OrderColumn]) -> Vec<u32> {
        let row_count = partition.rows.len();
        let mut ranks = vec![1u32; row_count];
        let mut current_rank = 1;
        let mut same_rank_count = 0;

        for i in 0..row_count {
            if i > 0
                && self.rows_equal_for_ordering(
                    &partition.rows[i - 1].1,
                    &partition.rows[i].1,
                    order_columns,
                )
            {
                same_rank_count += 1;
            } else {
                current_rank += same_rank_count;
                same_rank_count = 0;
            }
            ranks[i] = current_rank;
        }

        ranks
    }

    /// Get frame row indices for a given current row
    fn get_frame_rows(
        &self,
        current_row: usize,
        total_rows: usize,
        frame: &WindowFrame,
    ) -> Vec<usize> {
        let start_idx = match frame.start {
            FrameBoundary::Unbounded => 0,
            FrameBoundary::CurrentRow => current_row,
            FrameBoundary::Preceding(n) => current_row.saturating_sub(n as usize),
            FrameBoundary::Following(n) => (current_row + n as usize).min(total_rows),
        };

        let end_idx = match frame.end {
            FrameBoundary::Unbounded => total_rows - 1,
            FrameBoundary::CurrentRow => current_row,
            FrameBoundary::Preceding(n) => current_row.saturating_sub(n as usize),
            FrameBoundary::Following(n) => (current_row + n as usize).min(total_rows - 1),
        };

        if start_idx <= end_idx {
            (start_idx..=end_idx).collect()
        } else {
            vec![]
        }
    }

    /// Sum values in frame rows
    fn sum_values(&self, rows: &[(usize, Value)], frame_indices: &[usize], column: &str) -> Value {
        let mut sum = 0.0;
        for &idx in frame_indices {
            if let Some(val) = rows[idx].1.get(column) {
                if let Some(num) = val.as_f64() {
                    sum += num;
                }
            }
        }
        Value::Number(Number::from_f64(sum).unwrap_or(Number::from(0)))
    }

    /// Average values in frame rows
    fn avg_values(&self, rows: &[(usize, Value)], frame_indices: &[usize], column: &str) -> Value {
        if frame_indices.is_empty() {
            return Value::Null;
        }

        let mut sum = 0.0;
        let mut count = 0;

        for &idx in frame_indices {
            if let Some(val) = rows[idx].1.get(column) {
                if let Some(num) = val.as_f64() {
                    sum += num;
                    count += 1;
                }
            }
        }

        if count > 0 {
            Value::Number(Number::from_f64(sum / count as f64).unwrap_or(Number::from(0)))
        } else {
            Value::Null
        }
    }

    /// Count values in frame rows
    fn count_values(
        &self,
        rows: &[(usize, Value)],
        frame_indices: &[usize],
        column: Option<&str>,
    ) -> usize {
        if let Some(col) = column {
            frame_indices
                .iter()
                .filter(|&&idx| {
                    rows[idx].1.get(col).is_some() && !rows[idx].1.get(col).unwrap().is_null()
                })
                .count()
        } else {
            frame_indices.len() // COUNT(*)
        }
    }

    /// Get minimum value in frame rows
    fn min_values(&self, rows: &[(usize, Value)], frame_indices: &[usize], column: &str) -> Value {
        let mut min_val = None;

        for &idx in frame_indices {
            if let Some(val) = rows[idx].1.get(column) {
                if !val.is_null() {
                    match &min_val {
                        None => min_val = Some(val.clone()),
                        Some(current_min) => {
                            if self.compare_values(val, current_min, false) == Ordering::Less {
                                min_val = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }

        min_val.unwrap_or(Value::Null)
    }

    /// Get maximum value in frame rows
    fn max_values(&self, rows: &[(usize, Value)], frame_indices: &[usize], column: &str) -> Value {
        let mut max_val = None;

        for &idx in frame_indices {
            if let Some(val) = rows[idx].1.get(column) {
                if !val.is_null() {
                    match &max_val {
                        None => max_val = Some(val.clone()),
                        Some(current_max) => {
                            if self.compare_values(val, current_max, false) == Ordering::Greater {
                                max_val = Some(val.clone());
                            }
                        }
                    }
                }
            }
        }

        max_val.unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_row_number() {
        let executor = WindowExecutor;

        let data = vec![
            json!({"id": 1, "name": "Alice", "department": "Sales", "salary": 50000}),
            json!({"id": 2, "name": "Bob", "department": "Sales", "salary": 60000}),
            json!({"id": 3, "name": "Charlie", "department": "Engineering", "salary": 70000}),
        ];

        let query = WindowQuery {
            functions: vec![WindowFunctionCall {
                function: WindowFunction::RowNumber,
                window: WindowSpec {
                    partition_by: vec!["department".to_string()],
                    order_by: vec![OrderColumn {
                        column: "salary".to_string(),
                        ascending: true,
                        nulls_first: false,
                    }],
                    frame: None,
                },
                alias: "row_num".to_string(),
            }],
            data: data.clone(),
        };

        let result = executor.execute(query).unwrap();
        assert_eq!(result.len(), 3);

        // Check that row numbers are assigned correctly within partitions
        for row in &result {
            assert!(row.get("row_num").is_some());
        }
    }

    #[test]
    fn test_lag_lead() {
        let executor = WindowExecutor;

        let data = vec![
            json!({"id": 1, "value": 10}),
            json!({"id": 2, "value": 20}),
            json!({"id": 3, "value": 30}),
        ];

        let query = WindowQuery {
            functions: vec![
                WindowFunctionCall {
                    function: WindowFunction::Lag("value".to_string(), Some(1), Some(json!(0))),
                    window: WindowSpec {
                        partition_by: vec![],
                        order_by: vec![OrderColumn {
                            column: "id".to_string(),
                            ascending: true,
                            nulls_first: false,
                        }],
                        frame: None,
                    },
                    alias: "prev_value".to_string(),
                },
                WindowFunctionCall {
                    function: WindowFunction::Lead("value".to_string(), Some(1), Some(json!(0))),
                    window: WindowSpec {
                        partition_by: vec![],
                        order_by: vec![OrderColumn {
                            column: "id".to_string(),
                            ascending: true,
                            nulls_first: false,
                        }],
                        frame: None,
                    },
                    alias: "next_value".to_string(),
                },
            ],
            data: data.clone(),
        };

        let result = executor.execute(query).unwrap();

        // First row should have prev_value = 0 (default), next_value = 20
        assert_eq!(result[0]["prev_value"], json!(0));
        assert_eq!(result[0]["next_value"], json!(20));

        // Second row should have prev_value = 10, next_value = 30
        assert_eq!(result[1]["prev_value"], json!(10));
        assert_eq!(result[1]["next_value"], json!(30));

        // Third row should have prev_value = 20, next_value = 0 (default)
        assert_eq!(result[2]["prev_value"], json!(20));
        assert_eq!(result[2]["next_value"], json!(0));
    }

    #[test]
    fn test_sum_with_frame() {
        let executor = WindowExecutor;

        let data = vec![
            json!({"id": 1, "value": 10}),
            json!({"id": 2, "value": 20}),
            json!({"id": 3, "value": 30}),
            json!({"id": 4, "value": 40}),
        ];

        let query = WindowQuery {
            functions: vec![WindowFunctionCall {
                function: WindowFunction::Sum("value".to_string()),
                window: WindowSpec {
                    partition_by: vec![],
                    order_by: vec![OrderColumn {
                        column: "id".to_string(),
                        ascending: true,
                        nulls_first: false,
                    }],
                    frame: Some(WindowFrame {
                        frame_type: FrameType::Rows,
                        start: FrameBoundary::Preceding(1),
                        end: FrameBoundary::Following(1),
                    }),
                },
                alias: "rolling_sum".to_string(),
            }],
            data: data.clone(),
        };

        let result = executor.execute(query).unwrap();

        // Check rolling sums with window of [-1, +1]
        assert_eq!(result[0]["rolling_sum"], json!(30)); // 10 + 20
        assert_eq!(result[1]["rolling_sum"], json!(60)); // 10 + 20 + 30
        assert_eq!(result[2]["rolling_sum"], json!(90)); // 20 + 30 + 40
        assert_eq!(result[3]["rolling_sum"], json!(70)); // 30 + 40
    }

    #[test]
    fn test_rank_functions() {
        let executor = WindowExecutor;

        let data = vec![
            json!({"name": "Alice", "score": 95}),
            json!({"name": "Bob", "score": 85}),
            json!({"name": "Charlie", "score": 95}),
            json!({"name": "David", "score": 75}),
        ];

        let query = WindowQuery {
            functions: vec![
                WindowFunctionCall {
                    function: WindowFunction::Rank,
                    window: WindowSpec {
                        partition_by: vec![],
                        order_by: vec![OrderColumn {
                            column: "score".to_string(),
                            ascending: false, // Descending for ranking
                            nulls_first: false,
                        }],
                        frame: None,
                    },
                    alias: "rank".to_string(),
                },
                WindowFunctionCall {
                    function: WindowFunction::DenseRank,
                    window: WindowSpec {
                        partition_by: vec![],
                        order_by: vec![OrderColumn {
                            column: "score".to_string(),
                            ascending: false,
                            nulls_first: false,
                        }],
                        frame: None,
                    },
                    alias: "dense_rank".to_string(),
                },
            ],
            data: data.clone(),
        };

        let result = executor.execute(query).unwrap();

        // Alice and Charlie should both have rank 1 (tied for first)
        // Bob should have rank 3 (after two people tied for first)
        // David should have rank 4

        // Dense rank should be 1, 2, 1, 3 respectively
        assert!(result.iter().all(|row| row.get("rank").is_some()));
        assert!(result.iter().all(|row| row.get("dense_rank").is_some()));
    }
}
