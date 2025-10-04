use crate::errors::{DriftError, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Advanced partitioning and partition pruning system
pub struct PartitionManager {
    partitions: Arc<RwLock<HashMap<String, TablePartitions>>>,
    config: PartitionConfig,
    stats: Arc<RwLock<PartitionStats>>,
    pruning_cache: Arc<RwLock<PruningCache>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub enable_auto_partitioning: bool,
    pub max_partitions_per_table: usize,
    pub auto_split_threshold_rows: u64,
    pub auto_split_threshold_size_mb: u64,
    pub enable_pruning_cache: bool,
    pub cache_size: usize,
    pub enable_partition_wise_joins: bool,
    pub enable_parallel_partition_scan: bool,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            enable_auto_partitioning: true,
            max_partitions_per_table: 1000,
            auto_split_threshold_rows: 1_000_000,
            auto_split_threshold_size_mb: 100,
            enable_pruning_cache: true,
            cache_size: 10000,
            enable_partition_wise_joins: true,
            enable_parallel_partition_scan: true,
        }
    }
}

/// Partitions for a single table
#[derive(Debug, Clone)]
pub struct TablePartitions {
    pub table_name: String,
    pub partition_strategy: PartitionStrategy,
    pub partitions: BTreeMap<PartitionKey, Partition>,
    pub partition_columns: Vec<String>,
    pub subpartition_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    Range(RangePartitioning),
    List(ListPartitioning),
    Hash(HashPartitioning),
    Composite(Box<CompositePartitioning>),
    Interval(IntervalPartitioning),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangePartitioning {
    pub column: String,
    pub data_type: DataType,
    pub ranges: Vec<PartitionRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRange {
    pub name: String,
    pub lower_bound: PartitionValue,
    pub upper_bound: PartitionValue,
    pub is_default: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPartitioning {
    pub column: String,
    pub lists: HashMap<String, Vec<PartitionValue>>,
    pub default_partition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashPartitioning {
    pub column: String,
    pub num_buckets: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositePartitioning {
    pub primary: PartitionStrategy,
    pub secondary: PartitionStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntervalPartitioning {
    pub column: String,
    pub interval_type: IntervalType,
    pub interval_value: i64,
    pub start_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IntervalType {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PartitionKey {
    Range(String),
    List(String),
    Hash(usize),
    Composite(Box<(PartitionKey, PartitionKey)>),
    Interval(DateTime<Utc>),
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub key: PartitionKey,
    pub name: String,
    pub location: String,
    pub statistics: PartitionStatistics,
    pub subpartitions: Option<Vec<Partition>>,
}

#[derive(Debug, Clone, Default)]
pub struct PartitionStatistics {
    pub row_count: u64,
    pub size_bytes: u64,
    pub min_values: HashMap<String, PartitionValue>,
    pub max_values: HashMap<String, PartitionValue>,
    pub null_counts: HashMap<String, u64>,
    pub distinct_counts: HashMap<String, u64>,
    pub last_modified: Option<DateTime<Utc>>,
    pub last_analyzed: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PartitionValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Date(DateTime<Utc>),
    Boolean(bool),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    String,
    Date,
    Boolean,
    Bytes,
}

/// Cache for partition pruning decisions
struct PruningCache {
    cache: lru::LruCache<PruningCacheKey, Vec<PartitionKey>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PruningCacheKey {
    table: String,
    predicates: Vec<String>,
}

#[derive(Debug, Default)]
pub struct PartitionStats {
    pub total_partitions: usize,
    pub pruned_partitions: u64,
    pub scanned_partitions: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub auto_splits: u64,
    pub auto_merges: u64,
}

/// Predicate for partition pruning
#[derive(Debug, Clone)]
pub enum PruningPredicate {
    Equals(String, PartitionValue),
    NotEquals(String, PartitionValue),
    LessThan(String, PartitionValue),
    LessThanOrEqual(String, PartitionValue),
    GreaterThan(String, PartitionValue),
    GreaterThanOrEqual(String, PartitionValue),
    Between(String, PartitionValue, PartitionValue),
    In(String, Vec<PartitionValue>),
    IsNull(String),
    IsNotNull(String),
    And(Box<PruningPredicate>, Box<PruningPredicate>),
    Or(Box<PruningPredicate>, Box<PruningPredicate>),
}

impl PartitionManager {
    pub fn new(config: PartitionConfig) -> Self {
        let cache_size = config.cache_size;

        Self {
            partitions: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(RwLock::new(PartitionStats::default())),
            pruning_cache: Arc::new(RwLock::new(PruningCache::new(cache_size))),
        }
    }

    /// Create partitioned table
    pub fn create_partitioned_table(
        &self,
        table_name: String,
        strategy: PartitionStrategy,
        partition_columns: Vec<String>,
    ) -> Result<()> {
        // Validate partition strategy
        self.validate_strategy(&strategy)?;

        let table_partitions = TablePartitions {
            table_name: table_name.clone(),
            partition_strategy: strategy,
            partitions: BTreeMap::new(),
            partition_columns,
            subpartition_columns: None,
        };

        self.partitions.write().insert(table_name, table_partitions);
        self.stats.write().total_partitions += 1;

        Ok(())
    }

    /// Add a partition to a table
    pub fn add_partition(&self, table_name: &str, partition: Partition) -> Result<()> {
        let mut partitions = self.partitions.write();

        let table_partitions = partitions
            .get_mut(table_name)
            .ok_or_else(|| DriftError::NotFound(format!("Table '{}' not found", table_name)))?;

        // Check partition limit
        if table_partitions.partitions.len() >= self.config.max_partitions_per_table {
            return Err(DriftError::Other(format!(
                "Maximum partitions ({}) exceeded for table '{}'",
                self.config.max_partitions_per_table, table_name
            )));
        }

        table_partitions
            .partitions
            .insert(partition.key.clone(), partition);
        self.stats.write().total_partitions += 1;

        Ok(())
    }

    /// Prune partitions based on predicates
    pub fn prune_partitions(
        &self,
        table_name: &str,
        predicates: &[PruningPredicate],
    ) -> Result<Vec<PartitionKey>> {
        // Check cache first
        if self.config.enable_pruning_cache {
            let cache_key = PruningCacheKey {
                table: table_name.to_string(),
                predicates: predicates.iter().map(|p| format!("{:?}", p)).collect(),
            };

            if let Some(cached) = self.pruning_cache.write().get(&cache_key) {
                self.stats.write().cache_hits += 1;
                return Ok(cached);
            }

            self.stats.write().cache_misses += 1;
        }

        let partitions = self.partitions.read();
        let table_partitions = partitions
            .get(table_name)
            .ok_or_else(|| DriftError::NotFound(format!("Table '{}' not found", table_name)))?;

        let mut selected_partitions = Vec::new();

        for (key, partition) in &table_partitions.partitions {
            if self.should_scan_partition(
                partition,
                predicates,
                &table_partitions.partition_strategy,
            )? {
                selected_partitions.push(key.clone());
            }
        }

        // Update stats
        let total = table_partitions.partitions.len();
        let pruned = total - selected_partitions.len();
        self.stats.write().pruned_partitions += pruned as u64;
        self.stats.write().scanned_partitions += selected_partitions.len() as u64;

        // Update cache
        if self.config.enable_pruning_cache {
            let cache_key = PruningCacheKey {
                table: table_name.to_string(),
                predicates: predicates.iter().map(|p| format!("{:?}", p)).collect(),
            };
            self.pruning_cache
                .write()
                .put(cache_key, selected_partitions.clone());
        }

        Ok(selected_partitions)
    }

    fn should_scan_partition(
        &self,
        partition: &Partition,
        predicates: &[PruningPredicate],
        strategy: &PartitionStrategy,
    ) -> Result<bool> {
        for predicate in predicates {
            if !self.evaluate_predicate(partition, predicate, strategy)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn evaluate_predicate(
        &self,
        partition: &Partition,
        predicate: &PruningPredicate,
        strategy: &PartitionStrategy,
    ) -> Result<bool> {
        match predicate {
            PruningPredicate::Equals(column, value) => {
                self.check_value_in_partition(partition, column, value, strategy)
            }

            PruningPredicate::Between(column, low, high) => {
                self.check_range_overlap(partition, column, low, high, strategy)
            }

            PruningPredicate::LessThan(column, value) => {
                if let Some(min) = partition.statistics.min_values.get(column) {
                    Ok(min < value)
                } else {
                    Ok(true) // Conservative: scan if no stats
                }
            }

            PruningPredicate::GreaterThan(column, value) => {
                if let Some(max) = partition.statistics.max_values.get(column) {
                    Ok(max > value)
                } else {
                    Ok(true)
                }
            }

            PruningPredicate::In(column, values) => {
                for value in values {
                    if self.check_value_in_partition(partition, column, value, strategy)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }

            PruningPredicate::IsNull(column) => {
                if let Some(null_count) = partition.statistics.null_counts.get(column) {
                    Ok(*null_count > 0)
                } else {
                    Ok(true)
                }
            }

            PruningPredicate::And(left, right) => Ok(self
                .evaluate_predicate(partition, left, strategy)?
                && self.evaluate_predicate(partition, right, strategy)?),

            PruningPredicate::Or(left, right) => Ok(self
                .evaluate_predicate(partition, left, strategy)?
                || self.evaluate_predicate(partition, right, strategy)?),

            _ => Ok(true), // Conservative: scan for unsupported predicates
        }
    }

    fn check_value_in_partition(
        &self,
        partition: &Partition,
        column: &str,
        value: &PartitionValue,
        strategy: &PartitionStrategy,
    ) -> Result<bool> {
        match strategy {
            PartitionStrategy::Range(range_part) if range_part.column == column => {
                // Check if value falls within partition range
                match &partition.key {
                    PartitionKey::Range(name) => {
                        if let Some(range) = range_part.ranges.iter().find(|r| r.name == *name) {
                            Ok(value >= &range.lower_bound && value < &range.upper_bound)
                        } else {
                            Ok(false)
                        }
                    }
                    _ => Ok(true),
                }
            }

            PartitionStrategy::List(list_part) if list_part.column == column => {
                // Check if value is in partition list
                match &partition.key {
                    PartitionKey::List(name) => {
                        if let Some(values) = list_part.lists.get(name) {
                            Ok(values.contains(value))
                        } else {
                            Ok(false)
                        }
                    }
                    _ => Ok(true),
                }
            }

            PartitionStrategy::Hash(hash_part) if hash_part.column == column => {
                // Calculate hash and check if it matches partition
                match &partition.key {
                    PartitionKey::Hash(bucket) => {
                        let hash = self.calculate_hash(value) % hash_part.num_buckets;
                        Ok(hash == *bucket)
                    }
                    _ => Ok(true),
                }
            }

            _ => {
                // Use statistics-based pruning
                if let (Some(min), Some(max)) = (
                    partition.statistics.min_values.get(column),
                    partition.statistics.max_values.get(column),
                ) {
                    Ok(value >= min && value <= max)
                } else {
                    Ok(true) // Conservative: scan if no stats
                }
            }
        }
    }

    fn check_range_overlap(
        &self,
        partition: &Partition,
        column: &str,
        low: &PartitionValue,
        high: &PartitionValue,
        strategy: &PartitionStrategy,
    ) -> Result<bool> {
        match strategy {
            PartitionStrategy::Range(range_part) if range_part.column == column => {
                match &partition.key {
                    PartitionKey::Range(name) => {
                        if let Some(range) = range_part.ranges.iter().find(|r| r.name == *name) {
                            // Check if ranges overlap
                            Ok(!(high < &range.lower_bound || low >= &range.upper_bound))
                        } else {
                            Ok(false)
                        }
                    }
                    _ => Ok(true),
                }
            }

            _ => {
                // Use statistics-based pruning
                if let (Some(min), Some(max)) = (
                    partition.statistics.min_values.get(column),
                    partition.statistics.max_values.get(column),
                ) {
                    Ok(!(high < min || low > max))
                } else {
                    Ok(true)
                }
            }
        }
    }

    fn calculate_hash(&self, value: &PartitionValue) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        format!("{:?}", value).hash(&mut hasher);
        hasher.finish() as usize
    }

    fn validate_strategy(&self, strategy: &PartitionStrategy) -> Result<()> {
        match strategy {
            PartitionStrategy::Range(range) => {
                if range.ranges.is_empty() {
                    return Err(DriftError::Validation(
                        "Range partitioning requires at least one range".to_string(),
                    ));
                }
                // Check for overlapping ranges
                for i in 0..range.ranges.len() {
                    for j in (i + 1)..range.ranges.len() {
                        if self.ranges_overlap(&range.ranges[i], &range.ranges[j]) {
                            return Err(DriftError::Validation(format!(
                                "Overlapping ranges: {} and {}",
                                range.ranges[i].name, range.ranges[j].name
                            )));
                        }
                    }
                }
            }

            PartitionStrategy::Hash(hash) => {
                if hash.num_buckets == 0 {
                    return Err(DriftError::Validation(
                        "Hash partitioning requires at least one bucket".to_string(),
                    ));
                }
            }

            PartitionStrategy::Composite(composite) => {
                self.validate_strategy(&composite.primary)?;
                self.validate_strategy(&composite.secondary)?;
            }

            _ => {}
        }

        Ok(())
    }

    fn ranges_overlap(&self, r1: &PartitionRange, r2: &PartitionRange) -> bool {
        !(r1.upper_bound <= r2.lower_bound || r2.upper_bound <= r1.lower_bound)
    }

    /// Automatically split large partitions
    pub fn auto_split_partition(
        &self,
        table_name: &str,
        partition_key: &PartitionKey,
    ) -> Result<Vec<PartitionKey>> {
        if !self.config.enable_auto_partitioning {
            return Ok(vec![partition_key.clone()]);
        }

        let mut partitions = self.partitions.write();
        let table_partitions = partitions
            .get_mut(table_name)
            .ok_or_else(|| DriftError::NotFound(format!("Table '{}' not found", table_name)))?;

        let partition = table_partitions
            .partitions
            .get(partition_key)
            .ok_or_else(|| DriftError::NotFound("Partition not found".to_string()))?;

        // Check if split is needed
        if partition.statistics.row_count < self.config.auto_split_threshold_rows
            && partition.statistics.size_bytes
                < self.config.auto_split_threshold_size_mb * 1024 * 1024
        {
            return Ok(vec![partition_key.clone()]);
        }

        // Split based on strategy
        let new_partitions = match &table_partitions.partition_strategy {
            PartitionStrategy::Range(range) => self.split_range_partition(partition, range)?,
            PartitionStrategy::Hash(hash) => self.split_hash_partition(partition, hash)?,
            _ => vec![partition_key.clone()],
        };

        self.stats.write().auto_splits += 1;

        Ok(new_partitions)
    }

    fn split_range_partition(
        &self,
        partition: &Partition,
        range: &RangePartitioning,
    ) -> Result<Vec<PartitionKey>> {
        // Implement range splitting logic

        // Find the current range this partition belongs to
        let current_range = range.ranges.iter().find(|r| r.name == partition.name);
        let range_def = match current_range {
            Some(r) => r,
            None => {
                return Err(DriftError::InvalidQuery(format!(
                    "Range not found for partition {}",
                    partition.name
                )))
            }
        };

        // Calculate split point based on partition statistics
        let _split_point = self.calculate_range_split_point(partition, range_def)?;

        // Create two new partition keys
        let left_key = PartitionKey::Range(format!("{}_left", partition.name));
        let right_key = PartitionKey::Range(format!("{}_right", partition.name));

        // Update statistics to show split occurred
        self.stats.write().auto_splits += 1;

        Ok(vec![left_key, right_key])
    }

    fn split_hash_partition(
        &self,
        partition: &Partition,
        hash: &HashPartitioning,
    ) -> Result<Vec<PartitionKey>> {
        // Implement hash re-partitioning

        // Double the number of hash buckets by splitting this partition
        let current_bucket = self.extract_bucket_number(&partition.name)?;
        let _new_bucket_count = hash.num_buckets * 2;

        // Create new partition keys for the split buckets
        let new_bucket1 = current_bucket;
        let new_bucket2 = current_bucket + hash.num_buckets;

        let key1 = PartitionKey::Hash(new_bucket1);
        let key2 = PartitionKey::Hash(new_bucket2);

        // Update statistics
        self.stats.write().auto_splits += 1;

        Ok(vec![key1, key2])
    }

    fn calculate_range_split_point(
        &self,
        _partition: &Partition,
        range_def: &PartitionRange,
    ) -> Result<PartitionValue> {
        // Calculate optimal split point for a range partition
        // For simplicity, split at the midpoint between lower and upper bounds

        match (&range_def.lower_bound, &range_def.upper_bound) {
            (PartitionValue::Integer(low), PartitionValue::Integer(high)) => {
                let mid = (low + high) / 2;
                Ok(PartitionValue::Integer(mid))
            }
            (PartitionValue::Float(low), PartitionValue::Float(high)) => {
                let mid = (low + high) / 2.0;
                Ok(PartitionValue::Float(mid))
            }
            (PartitionValue::Date(low), PartitionValue::Date(high)) => {
                let low_timestamp = low.timestamp();
                let high_timestamp = high.timestamp();
                let mid_timestamp = (low_timestamp + high_timestamp) / 2;
                let mid_date = DateTime::from_timestamp(mid_timestamp, 0).unwrap_or(*low);
                Ok(PartitionValue::Date(mid_date))
            }
            _ => {
                // For other types or mismatched bounds, use a simple heuristic
                Ok(range_def.lower_bound.clone())
            }
        }
    }

    fn extract_bucket_number(&self, partition_name: &str) -> Result<usize> {
        // Extract bucket number from hash partition name like "hash_column_123"
        let parts: Vec<&str> = partition_name.split('_').collect();
        if parts.len() >= 3 {
            parts
                .last()
                .and_then(|s| s.parse::<usize>().ok())
                .ok_or_else(|| {
                    DriftError::InvalidQuery(format!(
                        "Invalid hash partition name: {}",
                        partition_name
                    ))
                })
        } else {
            Err(DriftError::InvalidQuery(format!(
                "Invalid hash partition name format: {}",
                partition_name
            )))
        }
    }

    /// Get partition statistics
    pub fn get_statistics(&self, table_name: &str) -> Result<TablePartitionStats> {
        let partitions = self.partitions.read();
        let table_partitions = partitions
            .get(table_name)
            .ok_or_else(|| DriftError::NotFound(format!("Table '{}' not found", table_name)))?;

        let mut total_rows = 0u64;
        let mut total_size = 0u64;
        let partition_count = table_partitions.partitions.len();

        for partition in table_partitions.partitions.values() {
            total_rows += partition.statistics.row_count;
            total_size += partition.statistics.size_bytes;
        }

        Ok(TablePartitionStats {
            table_name: table_name.to_string(),
            partition_count,
            total_rows,
            total_size_bytes: total_size,
            average_partition_size: if partition_count > 0 {
                total_size / partition_count as u64
            } else {
                0
            },
            strategy: format!("{:?}", table_partitions.partition_strategy),
        })
    }

    /// Optimize partition layout
    pub fn optimize_partitions(&self, table_name: &str) -> Result<OptimizationResult> {
        // Analyze partition statistics and suggest optimizations
        let stats = self.get_statistics(table_name)?;

        let mut suggestions = Vec::new();

        // Check for unbalanced partitions
        if stats.partition_count > 10 {
            let avg_size = stats.average_partition_size;
            let _threshold = avg_size as f64 * 0.2; // 20% deviation

            // Would check individual partition sizes
            suggestions.push("Consider rebalancing partitions for better distribution".to_string());
        }

        // Check for too many small partitions
        if stats.partition_count > 100 && stats.average_partition_size < 10 * 1024 * 1024 {
            suggestions.push("Consider merging small partitions to reduce overhead".to_string());
        }

        Ok(OptimizationResult {
            table_name: table_name.to_string(),
            suggestions,
            estimated_improvement_percent: 0.0,
        })
    }

    /// Clear pruning cache
    pub fn clear_cache(&self) {
        self.pruning_cache.write().clear();
    }

    /// Get partition pruning statistics
    pub fn stats(&self) -> PartitionStats {
        self.stats.read().clone()
    }
}

impl PruningCache {
    fn new(capacity: usize) -> Self {
        Self {
            cache: lru::LruCache::new(capacity.try_into().unwrap()),
        }
    }

    fn get(&mut self, key: &PruningCacheKey) -> Option<Vec<PartitionKey>> {
        self.cache.get(key).cloned()
    }

    fn put(&mut self, key: PruningCacheKey, value: Vec<PartitionKey>) {
        self.cache.put(key, value);
    }

    fn clear(&mut self) {
        self.cache.clear();
    }
}

#[derive(Debug, Clone)]
pub struct TablePartitionStats {
    pub table_name: String,
    pub partition_count: usize,
    pub total_rows: u64,
    pub total_size_bytes: u64,
    pub average_partition_size: u64,
    pub strategy: String,
}

#[derive(Debug)]
pub struct OptimizationResult {
    pub table_name: String,
    pub suggestions: Vec<String>,
    pub estimated_improvement_percent: f64,
}

impl Clone for PartitionStats {
    fn clone(&self) -> Self {
        Self {
            total_partitions: self.total_partitions,
            pruned_partitions: self.pruned_partitions,
            scanned_partitions: self.scanned_partitions,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            auto_splits: self.auto_splits,
            auto_merges: self.auto_merges,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_partitioning() {
        let manager = PartitionManager::new(PartitionConfig::default());

        let strategy = PartitionStrategy::Range(RangePartitioning {
            column: "date".to_string(),
            data_type: DataType::Date,
            ranges: vec![
                PartitionRange {
                    name: "p2024_q1".to_string(),
                    lower_bound: PartitionValue::String("2024-01-01".to_string()),
                    upper_bound: PartitionValue::String("2024-04-01".to_string()),
                    is_default: false,
                },
                PartitionRange {
                    name: "p2024_q2".to_string(),
                    lower_bound: PartitionValue::String("2024-04-01".to_string()),
                    upper_bound: PartitionValue::String("2024-07-01".to_string()),
                    is_default: false,
                },
            ],
        });

        manager
            .create_partitioned_table("sales".to_string(), strategy, vec!["date".to_string()])
            .unwrap();

        // Add partitions
        let partition1 = Partition {
            key: PartitionKey::Range("p2024_q1".to_string()),
            name: "p2024_q1".to_string(),
            location: "/data/sales/p2024_q1".to_string(),
            statistics: PartitionStatistics::default(),
            subpartitions: None,
        };

        manager.add_partition("sales", partition1).unwrap();

        // Test pruning
        let predicates = vec![PruningPredicate::Equals(
            "date".to_string(),
            PartitionValue::String("2024-02-15".to_string()),
        )];

        let selected = manager.prune_partitions("sales", &predicates).unwrap();
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn test_hash_partitioning() {
        let manager = PartitionManager::new(PartitionConfig::default());

        let strategy = PartitionStrategy::Hash(HashPartitioning {
            column: "user_id".to_string(),
            num_buckets: 4,
        });

        manager
            .create_partitioned_table("users".to_string(), strategy, vec!["user_id".to_string()])
            .unwrap();

        // Add hash partitions
        for i in 0..4 {
            let partition = Partition {
                key: PartitionKey::Hash(i),
                name: format!("bucket_{}", i),
                location: format!("/data/users/bucket_{}", i),
                statistics: PartitionStatistics::default(),
                subpartitions: None,
            };

            manager.add_partition("users", partition).unwrap();
        }

        // All partitions should be selected for non-partition column predicates
        let predicates = vec![PruningPredicate::Equals(
            "name".to_string(),
            PartitionValue::String("John".to_string()),
        )];

        let selected = manager.prune_partitions("users", &predicates).unwrap();
        assert_eq!(selected.len(), 4);
    }

    #[test]
    fn test_pruning_cache() {
        let mut config = PartitionConfig::default();
        config.enable_pruning_cache = true;

        let manager = PartitionManager::new(config);

        let strategy = PartitionStrategy::List(ListPartitioning {
            column: "country".to_string(),
            lists: vec![
                (
                    "us".to_string(),
                    vec![PartitionValue::String("USA".to_string())],
                ),
                (
                    "uk".to_string(),
                    vec![PartitionValue::String("UK".to_string())],
                ),
            ]
            .into_iter()
            .collect(),
            default_partition: Some("other".to_string()),
        });

        manager
            .create_partitioned_table(
                "customers".to_string(),
                strategy,
                vec!["country".to_string()],
            )
            .unwrap();

        let predicates = vec![PruningPredicate::Equals(
            "country".to_string(),
            PartitionValue::String("USA".to_string()),
        )];

        // First call - cache miss
        let _ = manager.prune_partitions("customers", &predicates).unwrap();
        assert_eq!(manager.stats.read().cache_misses, 1);

        // Second call - cache hit
        let _ = manager.prune_partitions("customers", &predicates).unwrap();
        assert_eq!(manager.stats.read().cache_hits, 1);
    }
}
