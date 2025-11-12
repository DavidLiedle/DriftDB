//! Database Statistics Collection and Management
//!
//! Provides comprehensive statistics collection for query optimization,
//! performance monitoring, and database management including:
//! - Table and column statistics (cardinality, histograms, null counts)
//! - Index usage and effectiveness metrics
//! - Query execution statistics and patterns
//! - System resource utilization
//! - Automatic statistics maintenance

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info, trace};

use crate::errors::Result;
use crate::optimizer::{
    ColumnStatistics, Histogram, HistogramBucket, IndexStatistics, TableStatistics,
};

/// Statistics collection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsConfig {
    /// Enable automatic statistics collection
    pub auto_collect: bool,
    /// Frequency of automatic collection
    pub collection_interval: Duration,
    /// Histogram bucket count for numeric columns
    pub histogram_buckets: usize,
    /// Sample size for large tables (percentage)
    pub sample_percentage: f64,
    /// Minimum rows before sampling kicks in
    pub sample_threshold: usize,
    /// Enable query execution tracking
    pub track_queries: bool,
    /// Maximum query history to keep
    pub max_query_history: usize,
    /// Enable system resource monitoring
    pub monitor_resources: bool,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            auto_collect: true,
            collection_interval: Duration::from_secs(3600), // 1 hour
            histogram_buckets: 50,
            sample_percentage: 10.0,
            sample_threshold: 10000,
            track_queries: true,
            max_query_history: 1000,
            monitor_resources: true,
        }
    }
}

/// Comprehensive database statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseStatistics {
    /// Global database metrics
    pub global: GlobalStatistics,
    /// Per-table statistics
    pub tables: HashMap<String, TableStatistics>,
    /// Per-index statistics
    pub indexes: HashMap<String, IndexStatistics>,
    /// Query execution statistics
    pub queries: QueryStatistics,
    /// System resource statistics
    pub system: SystemStatistics,
    /// Statistics collection metadata
    pub metadata: StatsMetadata,
}

/// Global database statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStatistics {
    /// Total number of tables
    pub table_count: usize,
    /// Total number of indexes
    pub index_count: usize,
    /// Total database size in bytes
    pub total_size_bytes: u64,
    /// Total number of rows across all tables
    pub total_rows: u64,
    /// Number of active connections
    pub active_connections: usize,
    /// Database uptime
    pub uptime_seconds: u64,
    /// Total queries executed
    pub total_queries: u64,
    /// Average query response time
    pub avg_query_time_ms: f64,
}

/// Query execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatistics {
    /// Recent query executions
    pub recent_queries: Vec<QueryExecution>,
    /// Query performance by type
    pub by_type: HashMap<String, QueryTypeStats>,
    /// Slow queries (above threshold)
    pub slow_queries: Vec<SlowQuery>,
    /// Query cache statistics
    pub cache_stats: CacheStats,
    /// Error statistics
    pub error_stats: ErrorStats,
}

/// Individual query execution record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecution {
    /// Query text (truncated for privacy)
    pub query_text: String,
    /// Execution start time
    pub start_time: SystemTime,
    /// Execution duration
    pub duration_ms: u64,
    /// Rows returned/affected
    pub rows_processed: usize,
    /// Query type (SELECT, INSERT, etc.)
    pub query_type: String,
    /// Tables accessed
    pub tables_accessed: Vec<String>,
    /// Whether query used indexes
    pub used_indexes: Vec<String>,
    /// Memory usage during execution
    pub memory_usage_bytes: Option<u64>,
}

/// Statistics for a query type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTypeStats {
    /// Total executions
    pub count: u64,
    /// Total execution time
    pub total_time_ms: u64,
    /// Average execution time
    pub avg_time_ms: f64,
    /// Minimum execution time
    pub min_time_ms: u64,
    /// Maximum execution time
    pub max_time_ms: u64,
    /// Total rows processed
    pub total_rows: u64,
    /// Average rows per query
    pub avg_rows: f64,
}

/// Slow query record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQuery {
    /// Query execution details
    pub execution: QueryExecution,
    /// Why it was slow (analysis)
    pub analysis: SlowQueryAnalysis,
}

/// Analysis of why a query was slow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryAnalysis {
    /// No index usage detected
    pub missing_indexes: Vec<String>,
    /// Full table scans performed
    pub table_scans: Vec<String>,
    /// Large result sets
    pub large_results: bool,
    /// Inefficient JOINs
    pub inefficient_joins: bool,
    /// Suboptimal WHERE clauses
    pub suboptimal_filters: bool,
}

/// Cache statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total cache requests
    pub requests: u64,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Hit rate percentage
    pub hit_rate: f64,
    /// Cache size in bytes
    pub size_bytes: u64,
    /// Number of cached items
    pub item_count: usize,
}

/// Error statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorStats {
    /// Total errors
    pub total_errors: u64,
    /// Errors by type
    pub by_type: HashMap<String, u64>,
    /// Recent errors
    pub recent_errors: Vec<ErrorRecord>,
}

/// Error record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRecord {
    /// Error timestamp
    pub timestamp: SystemTime,
    /// Error type/code
    pub error_type: String,
    /// Error message
    pub message: String,
    /// Query that caused the error (if applicable)
    pub query: Option<String>,
}

/// System resource statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatistics {
    /// CPU usage percentage
    pub cpu_usage: f64,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// Total available memory
    pub memory_total: u64,
    /// Disk usage in bytes
    pub disk_usage: u64,
    /// Available disk space
    pub disk_available: u64,
    /// Network I/O statistics
    pub network_io: NetworkStats,
    /// Disk I/O statistics
    pub disk_io: DiskStats,
}

/// Network I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Bytes received
    pub bytes_received: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Packets received
    pub packets_received: u64,
    /// Packets sent
    pub packets_sent: u64,
}

/// Disk I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskStats {
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Read operations
    pub read_ops: u64,
    /// Write operations
    pub write_ops: u64,
    /// Average read latency
    pub avg_read_latency_ms: f64,
    /// Average write latency
    pub avg_write_latency_ms: f64,
}

/// Statistics collection metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsMetadata {
    /// Last collection timestamp
    pub last_collected: SystemTime,
    /// Collection duration
    pub collection_duration_ms: u64,
    /// Statistics version
    pub version: String,
    /// Collection method used
    pub collection_method: CollectionMethod,
    /// Sample size used (if applicable)
    pub sample_size: Option<usize>,
}

/// Statistics collection method
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollectionMethod {
    /// Full table scan
    Full,
    /// Statistical sampling
    Sample,
    /// Incremental update
    Incremental,
    /// Estimated from metadata
    Estimated,
}

/// Statistics manager
pub struct StatisticsManager {
    /// Configuration
    config: StatsConfig,
    /// Current statistics
    stats: Arc<RwLock<DatabaseStatistics>>,
    /// Query execution history
    query_history: Arc<RwLock<Vec<QueryExecution>>>,
    /// Statistics collection scheduler
    last_collection: Arc<RwLock<SystemTime>>,
}

impl StatisticsManager {
    /// Create a new statistics manager
    pub fn new(config: StatsConfig) -> Self {
        let initial_stats = DatabaseStatistics {
            global: GlobalStatistics {
                table_count: 0,
                index_count: 0,
                total_size_bytes: 0,
                total_rows: 0,
                active_connections: 0,
                uptime_seconds: 0,
                total_queries: 0,
                avg_query_time_ms: 0.0,
            },
            tables: HashMap::new(),
            indexes: HashMap::new(),
            queries: QueryStatistics {
                recent_queries: Vec::new(),
                by_type: HashMap::new(),
                slow_queries: Vec::new(),
                cache_stats: CacheStats {
                    requests: 0,
                    hits: 0,
                    misses: 0,
                    hit_rate: 0.0,
                    size_bytes: 0,
                    item_count: 0,
                },
                error_stats: ErrorStats {
                    total_errors: 0,
                    by_type: HashMap::new(),
                    recent_errors: Vec::new(),
                },
            },
            system: SystemStatistics {
                cpu_usage: 0.0,
                memory_usage: 0,
                memory_total: 0,
                disk_usage: 0,
                disk_available: 0,
                network_io: NetworkStats {
                    bytes_received: 0,
                    bytes_sent: 0,
                    packets_received: 0,
                    packets_sent: 0,
                },
                disk_io: DiskStats {
                    bytes_read: 0,
                    bytes_written: 0,
                    read_ops: 0,
                    write_ops: 0,
                    avg_read_latency_ms: 0.0,
                    avg_write_latency_ms: 0.0,
                },
            },
            metadata: StatsMetadata {
                last_collected: SystemTime::now(),
                collection_duration_ms: 0,
                version: "1.0".to_string(),
                collection_method: CollectionMethod::Estimated,
                sample_size: None,
            },
        };

        Self {
            config,
            stats: Arc::new(RwLock::new(initial_stats)),
            query_history: Arc::new(RwLock::new(Vec::new())),
            last_collection: Arc::new(RwLock::new(SystemTime::now())),
        }
    }

    /// Collect table statistics
    pub fn collect_table_statistics(
        &self,
        table_name: &str,
        data: &[Value],
    ) -> Result<TableStatistics> {
        debug!("Collecting statistics for table '{}'", table_name);

        let start_time = std::time::Instant::now();
        let row_count = data.len();

        // Determine if we should sample
        let use_sampling = row_count > self.config.sample_threshold;
        let sample_data = if use_sampling {
            let sample_size = ((row_count as f64 * self.config.sample_percentage) / 100.0) as usize;
            self.sample_data(data, sample_size)
        } else {
            data.to_vec()
        };

        // Collect column statistics
        let mut column_stats = HashMap::new();
        if let Some(Value::Object(obj)) = sample_data.first() {
            for column_name in obj.keys() {
                let stats = self.collect_column_statistics(column_name, &sample_data)?;
                column_stats.insert(column_name.clone(), stats);
            }
        }

        let collection_time = start_time.elapsed();

        let column_count = column_stats.len();
        let table_stats = TableStatistics {
            table_name: table_name.to_string(),
            row_count,
            column_count,
            avg_row_size: if row_count > 0 {
                self.estimate_table_size(data) / row_count as u64
            } else {
                0
            } as usize,
            total_size_bytes: self.estimate_table_size(data),
            data_size_bytes: self.estimate_table_size(data),
            column_stats: column_stats.clone(),
            column_statistics: column_stats,
            index_stats: HashMap::new(),
            last_updated: SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            collection_method: if use_sampling {
                "SAMPLE".to_string()
            } else {
                "FULL".to_string()
            },
            collection_duration_ms: collection_time.as_millis() as u64,
        };

        // Update global statistics
        {
            let mut stats = self.stats.write();
            stats
                .tables
                .insert(table_name.to_string(), table_stats.clone());
            stats.global.table_count = stats.tables.len();
            stats.global.total_rows = stats.tables.values().map(|t| t.row_count as u64).sum();
            stats.global.total_size_bytes = stats.tables.values().map(|t| t.data_size_bytes).sum();
        }

        info!(
            "Collected statistics for table '{}': {} rows, {} columns",
            table_name, row_count, column_count
        );

        Ok(table_stats)
    }

    /// Collect column statistics
    fn collect_column_statistics(
        &self,
        column_name: &str,
        data: &[Value],
    ) -> Result<ColumnStatistics> {
        trace!("Collecting statistics for column '{}'", column_name);

        let mut distinct_values = std::collections::HashSet::new();
        let mut null_count = 0;
        let mut numeric_values = Vec::new();
        let mut string_lengths = Vec::new();

        // Analyze each value
        for row in data {
            if let Some(value) = row.get(column_name) {
                match value {
                    Value::Null => null_count += 1,
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            numeric_values.push(f);
                        }
                        distinct_values.insert(value.to_string());
                    }
                    Value::String(s) => {
                        string_lengths.push(s.len());
                        distinct_values.insert(s.clone());
                    }
                    _ => {
                        distinct_values.insert(value.to_string());
                    }
                }
            }
        }

        // Calculate statistics
        let distinct_count = distinct_values.len();
        let total_count = data.len() as u64;
        let _selectivity = if total_count > 0 {
            distinct_count as f64 / total_count as f64
        } else {
            0.0
        };

        // Calculate numeric statistics
        let (min_value, max_value, _avg_value) = if !numeric_values.is_empty() {
            let min = numeric_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
            let max = numeric_values
                .iter()
                .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let avg = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
            (Some(min), Some(max), Some(avg))
        } else {
            (None, None, None)
        };

        // Create histogram for numeric columns
        let histogram = if !numeric_values.is_empty() {
            Some(self.create_histogram(&numeric_values))
        } else {
            None
        };

        // Calculate average string length
        let _avg_length = if !string_lengths.is_empty() {
            Some(string_lengths.iter().sum::<usize>() as f64 / string_lengths.len() as f64)
        } else {
            None
        };

        Ok(ColumnStatistics {
            column_name: column_name.to_string(),
            distinct_values: distinct_count,
            null_count,
            min_value: min_value.map(|v| serde_json::json!(v)),
            max_value: max_value.map(|v| serde_json::json!(v)),
            histogram,
        })
    }

    /// Create histogram for numeric values
    fn create_histogram(&self, values: &[f64]) -> Histogram {
        if values.is_empty() {
            return Histogram {
                buckets: Vec::new(),
                bucket_count: 0,
            };
        }

        let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let range = max - min;

        if range == 0.0 {
            // All values are the same
            return Histogram {
                buckets: vec![HistogramBucket {
                    lower_bound: serde_json::json!(min),
                    upper_bound: serde_json::json!(max),
                    frequency: values.len(),
                    min_value: serde_json::json!(min),
                    max_value: serde_json::json!(max),
                    distinct_count: 1,
                }],
                bucket_count: 1,
            };
        }

        let bucket_width = range / self.config.histogram_buckets as f64;
        let mut buckets = vec![
            HistogramBucket {
                lower_bound: serde_json::json!(0.0),
                upper_bound: serde_json::json!(0.0),
                frequency: 0,
                min_value: serde_json::json!(0.0),
                max_value: serde_json::json!(0.0),
                distinct_count: 0,
            };
            self.config.histogram_buckets
        ];

        // Initialize bucket boundaries
        for (i, bucket) in buckets.iter_mut().enumerate() {
            let bucket_min = min + (i as f64 * bucket_width);
            let bucket_max = if i == self.config.histogram_buckets - 1 {
                max
            } else {
                min + ((i + 1) as f64 * bucket_width)
            };
            bucket.lower_bound = serde_json::json!(bucket_min);
            bucket.upper_bound = serde_json::json!(bucket_max);
            bucket.min_value = serde_json::json!(bucket_min);
            bucket.max_value = serde_json::json!(bucket_max);
        }

        // Count values in each bucket
        for &value in values {
            let bucket_index = if value == max {
                self.config.histogram_buckets - 1
            } else {
                ((value - min) / bucket_width) as usize
            };

            if bucket_index < buckets.len() {
                buckets[bucket_index].frequency += 1;
                // For distinct count, we'd need to track unique values per bucket
                buckets[bucket_index].distinct_count = buckets[bucket_index].frequency;
            }
        }

        Histogram {
            buckets,
            bucket_count: self.config.histogram_buckets,
        }
    }

    /// Record query execution
    pub fn record_query_execution(&self, execution: QueryExecution) {
        if !self.config.track_queries {
            return;
        }

        trace!("Recording query execution: {} ms", execution.duration_ms);

        // Add to history
        {
            let mut history = self.query_history.write();
            history.push(execution.clone());

            // Limit history size
            if history.len() > self.config.max_query_history {
                history.remove(0);
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write();

            // Update global stats
            stats.global.total_queries += 1;
            let total_time =
                stats.global.avg_query_time_ms * (stats.global.total_queries - 1) as f64;
            stats.global.avg_query_time_ms =
                (total_time + execution.duration_ms as f64) / stats.global.total_queries as f64;

            // Update query type stats
            let type_stats = stats
                .queries
                .by_type
                .entry(execution.query_type.clone())
                .or_insert(QueryTypeStats {
                    count: 0,
                    total_time_ms: 0,
                    avg_time_ms: 0.0,
                    min_time_ms: u64::MAX,
                    max_time_ms: 0,
                    total_rows: 0,
                    avg_rows: 0.0,
                });

            type_stats.count += 1;
            type_stats.total_time_ms += execution.duration_ms;
            type_stats.avg_time_ms = type_stats.total_time_ms as f64 / type_stats.count as f64;
            type_stats.min_time_ms = type_stats.min_time_ms.min(execution.duration_ms);
            type_stats.max_time_ms = type_stats.max_time_ms.max(execution.duration_ms);
            type_stats.total_rows += execution.rows_processed as u64;
            type_stats.avg_rows = type_stats.total_rows as f64 / type_stats.count as f64;

            // Check if it's a slow query (> 1 second)
            if execution.duration_ms > 1000 {
                let analysis = self.analyze_slow_query(&execution);
                stats.queries.slow_queries.push(SlowQuery {
                    execution: execution.clone(),
                    analysis,
                });

                // Limit slow query history
                if stats.queries.slow_queries.len() > 100 {
                    stats.queries.slow_queries.remove(0);
                }
            }

            // Update recent queries
            stats.queries.recent_queries.push(execution);
            if stats.queries.recent_queries.len() > 50 {
                stats.queries.recent_queries.remove(0);
            }
        }
    }

    /// Analyze why a query was slow
    fn analyze_slow_query(&self, execution: &QueryExecution) -> SlowQueryAnalysis {
        SlowQueryAnalysis {
            missing_indexes: Vec::new(), // TODO: Analyze index usage
            table_scans: execution.tables_accessed.clone(), // Simplified
            large_results: execution.rows_processed > 10000,
            inefficient_joins: execution.tables_accessed.len() > 1,
            suboptimal_filters: execution.query_text.contains("LIKE '%"),
        }
    }

    /// Sample data for large tables
    fn sample_data(&self, data: &[Value], sample_size: usize) -> Vec<Value> {
        if sample_size >= data.len() {
            return data.to_vec();
        }

        // Simple systematic sampling
        let step = data.len() / sample_size;
        let mut sample = Vec::with_capacity(sample_size);

        for i in (0..data.len()).step_by(step) {
            if sample.len() >= sample_size {
                break;
            }
            sample.push(data[i].clone());
        }

        sample
    }

    /// Estimate table size in bytes
    fn estimate_table_size(&self, data: &[Value]) -> u64 {
        if data.is_empty() {
            return 0;
        }

        // Estimate based on JSON serialization of a sample
        let sample_size = 100.min(data.len());
        let mut total_size = 0;

        for row in data.iter().take(sample_size) {
            total_size += row.to_string().len();
        }

        let avg_row_size = total_size / sample_size;
        (avg_row_size * data.len()) as u64
    }

    /// Infer data type from sample values
    #[allow(dead_code)]
    fn infer_data_type(&self, data: &[Value], column_name: &str) -> String {
        let mut has_number = false;
        let mut has_string = false;
        let mut has_bool = false;

        for row in data.iter().take(100) {
            if let Some(value) = row.get(column_name) {
                match value {
                    Value::Number(_) => has_number = true,
                    Value::String(_) => has_string = true,
                    Value::Bool(_) => has_bool = true,
                    _ => {}
                }
            }
        }

        if has_number && !has_string && !has_bool {
            "NUMERIC".to_string()
        } else if has_bool && !has_string && !has_number {
            "BOOLEAN".to_string()
        } else {
            "TEXT".to_string()
        }
    }

    /// Get current database statistics
    pub fn get_statistics(&self) -> DatabaseStatistics {
        self.stats.read().clone()
    }

    /// Update system resource statistics
    pub fn update_system_stats(&self, system_stats: SystemStatistics) {
        let mut stats = self.stats.write();
        stats.system = system_stats;
    }

    /// Check if automatic collection is due
    pub fn should_collect_stats(&self) -> bool {
        if !self.config.auto_collect {
            return false;
        }

        let last_collection = *self.last_collection.read();
        SystemTime::now()
            .duration_since(last_collection)
            .unwrap_or_default()
            > self.config.collection_interval
    }

    /// Mark statistics as collected
    pub fn mark_collection_complete(&self) {
        *self.last_collection.write() = SystemTime::now();
        let mut stats = self.stats.write();
        stats.metadata.last_collected = SystemTime::now();
    }

    /// Get configuration
    pub fn config(&self) -> &StatsConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: StatsConfig) {
        self.config = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_statistics_collection() {
        let config = StatsConfig::default();
        let manager = StatisticsManager::new(config);

        let data = vec![
            json!({"id": 1, "name": "Alice", "age": 30, "salary": 50000.0}),
            json!({"id": 2, "name": "Bob", "age": 25, "salary": 60000.0}),
            json!({"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0}),
        ];

        let stats = manager
            .collect_table_statistics("employees", &data)
            .unwrap();

        assert_eq!(stats.table_name, "employees");
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.column_count, 4);
        assert!(stats.column_statistics.contains_key("id"));
        assert!(stats.column_statistics.contains_key("name"));
        assert!(stats.column_statistics.contains_key("age"));
        assert!(stats.column_statistics.contains_key("salary"));
    }

    #[test]
    fn test_column_statistics() {
        let config = StatsConfig::default();
        let manager = StatisticsManager::new(config);

        let data = vec![
            json!({"score": 85}),
            json!({"score": 92}),
            json!({"score": 78}),
            json!({"score": 95}),
            json!({"score": 88}),
        ];

        let stats = manager.collect_column_statistics("score", &data).unwrap();

        assert_eq!(stats.column_name, "score");
        assert_eq!(stats.distinct_values, 5);
        assert_eq!(stats.null_count, 0);
        assert!(stats.min_value.is_some());
        assert!(stats.max_value.is_some());
    }

    #[test]
    fn test_query_execution_recording() {
        let config = StatsConfig::default();
        let manager = StatisticsManager::new(config);

        let execution = QueryExecution {
            query_text: "SELECT * FROM users".to_string(),
            start_time: SystemTime::now(),
            duration_ms: 150,
            rows_processed: 100,
            query_type: "SELECT".to_string(),
            tables_accessed: vec!["users".to_string()],
            used_indexes: vec![],
            memory_usage_bytes: Some(1024),
        };

        manager.record_query_execution(execution);

        let stats = manager.get_statistics();
        assert_eq!(stats.global.total_queries, 1);
        assert_eq!(stats.queries.recent_queries.len(), 1);
        assert!(stats.queries.by_type.contains_key("SELECT"));
    }

    #[test]
    fn test_histogram_creation() {
        let config = StatsConfig::default();
        let manager = StatisticsManager::new(config);

        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let histogram = manager.create_histogram(&values);

        assert!(histogram.bucket_count > 0);
        assert_eq!(histogram.buckets.len(), histogram.bucket_count);

        let total_frequency: usize = histogram.buckets.iter().map(|b| b.frequency).sum();
        assert_eq!(total_frequency, values.len());
    }
}
