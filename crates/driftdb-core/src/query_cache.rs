use crate::errors::{DriftError, Result};
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Advanced query result caching system
pub struct QueryCacheManager {
    cache: Arc<RwLock<Cache>>,
    config: CacheConfig,
    stats: Arc<RwLock<CacheStats>>,
    invalidator: Arc<InvalidationManager>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub max_memory_mb: usize,
    pub ttl_seconds: u64,
    pub cache_strategy: CacheStrategy,
    pub enable_adaptive_caching: bool,
    pub enable_partial_results: bool,
    pub enable_compression: bool,
    pub invalidation_strategy: InvalidationStrategy,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10000,
            max_memory_mb: 512,
            ttl_seconds: 3600, // 1 hour
            cache_strategy: CacheStrategy::LRU,
            enable_adaptive_caching: true,
            enable_partial_results: true,
            enable_compression: true,
            invalidation_strategy: InvalidationStrategy::Immediate,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheStrategy {
    LRU,  // Least Recently Used
    LFU,  // Least Frequently Used
    FIFO, // First In First Out
    ARC,  // Adaptive Replacement Cache
    SLRU, // Segmented LRU
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvalidationStrategy {
    Immediate,  // Invalidate immediately on write
    Lazy,       // Mark stale, refresh on next access
    TimeToLive, // TTL-based expiration only
    Smart,      // Intelligent invalidation based on query patterns
}

/// Cache storage implementation
struct Cache {
    strategy: CacheStrategy,
    entries: LruCache<CacheKey, CacheEntry>,
    memory_usage: usize,
    max_memory: usize,
    frequency_map: HashMap<CacheKey, u64>,
    segment_hot: Option<LruCache<CacheKey, CacheEntry>>, // For SLRU
    segment_cold: Option<LruCache<CacheKey, CacheEntry>>, // For SLRU
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    query_hash: u64,
    query_text: String,
    parameters: Vec<String>,
    user_context: Option<String>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    #[allow(dead_code)]
    key: CacheKey,
    result: QueryResult,
    metadata: CacheMetadata,
    #[allow(dead_code)]
    compressed: bool,
    size_bytes: usize,
}

#[derive(Debug, Clone)]
struct CacheMetadata {
    created_at: Instant,
    last_accessed: Instant,
    access_count: u64,
    computation_time_ms: u64,
    #[allow(dead_code)]
    source_tables: Vec<String>,
    is_stale: bool,
    ttl: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<HashMap<String, serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    pub cached: bool,
}

/// Manages cache invalidation
struct InvalidationManager {
    #[allow(dead_code)]
    strategy: InvalidationStrategy,
    table_dependencies: Arc<RwLock<HashMap<String, HashSet<CacheKey>>>>,
    staleness_tracker: Arc<RwLock<HashMap<CacheKey, bool>>>,
    invalidation_queue: Arc<RwLock<Vec<InvalidationEvent>>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct InvalidationEvent {
    timestamp: SystemTime,
    event_type: InvalidationType,
    affected_tables: Vec<String>,
    affected_keys: Vec<CacheKey>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum InvalidationType {
    TableUpdate(String),
    TableDrop(String),
    SchemaChange(String),
    Manual(Vec<CacheKey>),
    Expiration,
}

#[derive(Debug, Default)]
pub struct CacheStats {
    pub total_requests: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hit_ratio: f64,
    pub evictions: u64,
    pub invalidations: u64,
    pub memory_usage_bytes: usize,
    pub avg_entry_size: usize,
    pub avg_computation_saved_ms: u64,
    pub total_computation_saved_ms: u64,
}

/// Adaptive caching predictor
#[allow(dead_code)]
struct AdaptiveCachePredictor {
    query_patterns: HashMap<String, QueryPattern>,
    benefit_scores: HashMap<CacheKey, f64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct QueryPattern {
    frequency: u64,
    avg_execution_time: u64,
    volatility_score: f64,
    last_seen: Instant,
}

impl QueryCacheManager {
    pub fn new(config: CacheConfig) -> Self {
        let cache = Cache::new(
            config.cache_strategy,
            config.max_entries,
            config.max_memory_mb * 1024 * 1024,
        );

        let invalidator = Arc::new(InvalidationManager::new(config.invalidation_strategy));

        Self {
            cache: Arc::new(RwLock::new(cache)),
            config,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            invalidator,
        }
    }

    /// Get cached query result
    pub fn get(
        &self,
        query: &str,
        parameters: &[String],
        user_context: Option<&str>,
    ) -> Option<QueryResult> {
        if !self.config.enabled {
            return None;
        }

        let key = self.create_cache_key(query, parameters, user_context);

        let mut cache = self.cache.write();
        let mut stats = self.stats.write();

        stats.total_requests += 1;

        if let Some(entry) = cache.get(&key) {
            // Check TTL
            if entry.metadata.created_at.elapsed() > entry.metadata.ttl {
                cache.remove(&key);
                stats.cache_misses += 1;
                stats.evictions += 1;
                return None;
            }

            // Check staleness
            if entry.metadata.is_stale
                && self.config.invalidation_strategy == InvalidationStrategy::Lazy
            {
                cache.remove(&key);
                stats.cache_misses += 1;
                stats.invalidations += 1;
                return None;
            }

            // Update metadata
            entry.metadata.last_accessed = Instant::now();
            entry.metadata.access_count += 1;

            // Update stats
            stats.cache_hits += 1;
            stats.hit_ratio = stats.cache_hits as f64 / stats.total_requests as f64;
            stats.total_computation_saved_ms += entry.metadata.computation_time_ms;

            let mut result = entry.result.clone();
            result.cached = true;

            Some(result)
        } else {
            stats.cache_misses += 1;
            stats.hit_ratio = stats.cache_hits as f64 / stats.total_requests as f64;
            None
        }
    }

    /// Store query result in cache
    pub fn put(
        &self,
        query: &str,
        parameters: &[String],
        user_context: Option<&str>,
        result: QueryResult,
        execution_time_ms: u64,
        source_tables: Vec<String>,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Check if query should be cached (adaptive caching)
        if self.config.enable_adaptive_caching && !self.should_cache(query, execution_time_ms) {
            return Ok(());
        }

        let key = self.create_cache_key(query, parameters, user_context);

        let metadata = CacheMetadata {
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 0,
            computation_time_ms: execution_time_ms,
            source_tables: source_tables.clone(),
            is_stale: false,
            ttl: Duration::from_secs(self.config.ttl_seconds),
        };

        let size_bytes = self.estimate_size(&result);

        let entry = CacheEntry {
            key: key.clone(),
            result,
            metadata,
            compressed: false,
            size_bytes,
        };

        // Store in cache
        let mut cache = self.cache.write();

        // Check memory limit
        if cache.memory_usage + size_bytes > cache.max_memory {
            self.evict_entries(&mut cache, size_bytes)?;
        }

        cache.put(key.clone(), entry)?;
        cache.memory_usage += size_bytes;

        // Update invalidation tracking
        self.invalidator.track_dependencies(key, source_tables)?;

        // Update stats
        let mut stats = self.stats.write();
        stats.memory_usage_bytes = cache.memory_usage;
        if cache.entry_count() > 0 {
            stats.avg_entry_size = cache.memory_usage / cache.entry_count();
        }

        Ok(())
    }

    /// Invalidate cache entries for a table
    pub fn invalidate_table(&self, table_name: &str) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let keys = self.invalidator.get_dependent_keys(table_name)?;

        match self.config.invalidation_strategy {
            InvalidationStrategy::Immediate => {
                let mut cache = self.cache.write();
                let mut stats = self.stats.write();

                for key in keys {
                    if let Some(entry) = cache.remove(&key) {
                        cache.memory_usage = cache.memory_usage.saturating_sub(entry.size_bytes);
                        stats.invalidations += 1;
                    }
                }

                stats.memory_usage_bytes = cache.memory_usage;
            }

            InvalidationStrategy::Lazy => {
                let mut cache = self.cache.write();

                for key in keys {
                    if let Some(entry) = cache.get(&key) {
                        entry.metadata.is_stale = true;
                    }
                }
            }

            _ => {}
        }

        Ok(())
    }

    /// Clear entire cache
    pub fn clear(&self) -> Result<()> {
        let mut cache = self.cache.write();
        cache.clear();

        let mut stats = self.stats.write();
        stats.invalidations += cache.entry_count() as u64;
        stats.memory_usage_bytes = 0;

        self.invalidator.clear()?;

        Ok(())
    }

    fn create_cache_key(
        &self,
        query: &str,
        parameters: &[String],
        user_context: Option<&str>,
    ) -> CacheKey {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        for param in parameters {
            param.hash(&mut hasher);
        }
        if let Some(ctx) = user_context {
            ctx.hash(&mut hasher);
        }

        CacheKey {
            query_hash: hasher.finish(),
            query_text: query.to_string(),
            parameters: parameters.to_vec(),
            user_context: user_context.map(|s| s.to_string()),
        }
    }

    fn should_cache(&self, query: &str, execution_time_ms: u64) -> bool {
        // Simple heuristic: cache queries that take more than 100ms
        // In production, would use more sophisticated prediction
        execution_time_ms > 100
            || query.to_lowercase().contains("group by")
            || query.to_lowercase().contains("join")
    }

    fn estimate_size(&self, result: &QueryResult) -> usize {
        // Estimate size based on number of rows and average row size
        let row_size = if !result.rows.is_empty() {
            let sample = &result.rows[0];
            sample.len() * 100 // Rough estimate: 100 bytes per field
        } else {
            0
        };

        result.row_count * row_size + std::mem::size_of::<QueryResult>()
    }

    fn evict_entries(&self, cache: &mut Cache, needed_space: usize) -> Result<()> {
        let mut freed_space = 0usize;
        let mut evicted = 0u64;

        while freed_space < needed_space && !cache.is_empty() {
            if let Some((_, entry)) = cache.pop_lru() {
                freed_space += entry.size_bytes;
                cache.memory_usage = cache.memory_usage.saturating_sub(entry.size_bytes);
                evicted += 1;
            } else {
                break;
            }
        }

        self.stats.write().evictions += evicted;

        if freed_space < needed_space {
            Err(DriftError::Other("Cache memory limit exceeded".to_string()))
        } else {
            Ok(())
        }
    }

    /// Warm up cache with common queries
    pub async fn warm_up(&self, _queries: Vec<(String, Vec<String>)>) -> Result<()> {
        // Would execute queries and cache results
        // This is a placeholder for the actual implementation
        Ok(())
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }

    /// Get cache utilization
    pub fn utilization(&self) -> CacheUtilization {
        let cache = self.cache.read();

        CacheUtilization {
            entry_count: cache.entry_count(),
            max_entries: self.config.max_entries,
            memory_usage_mb: cache.memory_usage / (1024 * 1024),
            max_memory_mb: self.config.max_memory_mb,
            utilization_percent: (cache.entry_count() as f64 / self.config.max_entries as f64)
                * 100.0,
        }
    }

    /// Analyze cache effectiveness
    pub fn analyze_effectiveness(&self) -> CacheAnalysis {
        let stats = self.stats.read();

        let effectiveness_score = if stats.total_requests > 0 {
            (stats.hit_ratio * 100.0)
                * (stats.total_computation_saved_ms as f64 / stats.total_requests as f64)
        } else {
            0.0
        };

        CacheAnalysis {
            hit_ratio: stats.hit_ratio,
            avg_computation_saved_ms: stats.avg_computation_saved_ms,
            total_computation_saved_ms: stats.total_computation_saved_ms,
            effectiveness_score,
            recommendations: self.generate_recommendations(&stats),
        }
    }

    fn generate_recommendations(&self, stats: &CacheStats) -> Vec<String> {
        let mut recommendations = Vec::new();

        if stats.hit_ratio < 0.3 {
            recommendations.push("Consider increasing cache size or TTL".to_string());
        }

        if stats.evictions > stats.cache_hits {
            recommendations.push("Cache thrashing detected - increase memory limit".to_string());
        }

        if stats.avg_entry_size > 1024 * 1024 {
            // 1MB
            recommendations.push("Large cache entries detected - consider compression".to_string());
        }

        recommendations
    }
}

impl Cache {
    fn new(strategy: CacheStrategy, max_entries: usize, max_memory: usize) -> Self {
        match strategy {
            CacheStrategy::SLRU => {
                // Segmented LRU with hot and cold segments
                let hot_size = max_entries / 3;
                let cold_size = max_entries - hot_size;

                Self {
                    strategy,
                    entries: LruCache::new(max_entries.try_into().unwrap()),
                    memory_usage: 0,
                    max_memory,
                    frequency_map: HashMap::new(),
                    segment_hot: Some(LruCache::new(hot_size.try_into().unwrap())),
                    segment_cold: Some(LruCache::new(cold_size.try_into().unwrap())),
                }
            }
            _ => Self {
                strategy,
                entries: LruCache::new(max_entries.try_into().unwrap()),
                memory_usage: 0,
                max_memory,
                frequency_map: HashMap::new(),
                segment_hot: None,
                segment_cold: None,
            },
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<&mut CacheEntry> {
        match self.strategy {
            CacheStrategy::LFU => {
                // Update frequency
                *self.frequency_map.entry(key.clone()).or_insert(0) += 1;
                self.entries.get_mut(key)
            }
            CacheStrategy::SLRU => {
                // Check hot segment first
                if let Some(ref mut hot) = self.segment_hot {
                    if let Some(entry) = hot.get_mut(key) {
                        return Some(unsafe { std::mem::transmute(entry) });
                    }
                }

                // Check cold segment
                if let Some(ref mut cold) = self.segment_cold {
                    if let Some(entry) = cold.pop(key) {
                        // Promote to hot segment
                        if let Some(ref mut hot) = self.segment_hot {
                            hot.put(key.clone(), entry);
                            return hot.get_mut(key).map(|e| unsafe { std::mem::transmute(e) });
                        }
                    }
                }

                None
            }
            _ => self.entries.get_mut(key),
        }
    }

    fn put(&mut self, key: CacheKey, entry: CacheEntry) -> Result<()> {
        match self.strategy {
            CacheStrategy::SLRU => {
                // New entries go to cold segment
                if let Some(ref mut cold) = self.segment_cold {
                    cold.put(key, entry);
                }
            }
            _ => {
                self.entries.put(key, entry);
            }
        }

        Ok(())
    }

    fn remove(&mut self, key: &CacheKey) -> Option<CacheEntry> {
        match self.strategy {
            CacheStrategy::SLRU => {
                if let Some(ref mut hot) = self.segment_hot {
                    if let Some(entry) = hot.pop(key) {
                        return Some(entry);
                    }
                }

                if let Some(ref mut cold) = self.segment_cold {
                    cold.pop(key)
                } else {
                    None
                }
            }
            _ => self.entries.pop(key),
        }
    }

    fn pop_lru(&mut self) -> Option<(CacheKey, CacheEntry)> {
        match self.strategy {
            CacheStrategy::LFU => {
                // Find least frequently used
                if self.frequency_map.is_empty() {
                    return self.entries.pop_lru();
                }

                let min_key = self
                    .frequency_map
                    .iter()
                    .min_by_key(|(_, freq)| *freq)
                    .map(|(k, _)| k.clone())?;

                self.frequency_map.remove(&min_key);
                self.entries.pop(&min_key).map(|e| (min_key, e))
            }
            CacheStrategy::SLRU => {
                // Evict from cold segment first
                if let Some(ref mut cold) = self.segment_cold {
                    if let Some((k, v)) = cold.pop_lru() {
                        return Some((k, v));
                    }
                }

                if let Some(ref mut hot) = self.segment_hot {
                    hot.pop_lru()
                } else {
                    None
                }
            }
            _ => self.entries.pop_lru(),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.frequency_map.clear();
        self.memory_usage = 0;

        if let Some(ref mut hot) = self.segment_hot {
            hot.clear();
        }
        if let Some(ref mut cold) = self.segment_cold {
            cold.clear();
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.segment_hot.as_ref().is_none_or(|h| h.is_empty())
            && self.segment_cold.as_ref().is_none_or(|c| c.is_empty())
    }

    fn entry_count(&self) -> usize {
        self.entries.len()
            + self.segment_hot.as_ref().map_or(0, |h| h.len())
            + self.segment_cold.as_ref().map_or(0, |c| c.len())
    }
}

impl InvalidationManager {
    fn new(strategy: InvalidationStrategy) -> Self {
        Self {
            strategy,
            table_dependencies: Arc::new(RwLock::new(HashMap::new())),
            staleness_tracker: Arc::new(RwLock::new(HashMap::new())),
            invalidation_queue: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn track_dependencies(&self, key: CacheKey, tables: Vec<String>) -> Result<()> {
        let mut deps = self.table_dependencies.write();

        for table in tables {
            deps.entry(table)
                .or_default()
                .insert(key.clone());
        }

        Ok(())
    }

    fn get_dependent_keys(&self, table: &str) -> Result<Vec<CacheKey>> {
        let deps = self.table_dependencies.read();

        Ok(deps
            .get(table)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default())
    }

    fn clear(&self) -> Result<()> {
        self.table_dependencies.write().clear();
        self.staleness_tracker.write().clear();
        self.invalidation_queue.write().clear();
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheUtilization {
    pub entry_count: usize,
    pub max_entries: usize,
    pub memory_usage_mb: usize,
    pub max_memory_mb: usize,
    pub utilization_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheAnalysis {
    pub hit_ratio: f64,
    pub avg_computation_saved_ms: u64,
    pub total_computation_saved_ms: u64,
    pub effectiveness_score: f64,
    pub recommendations: Vec<String>,
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        Self {
            total_requests: self.total_requests,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            hit_ratio: self.hit_ratio,
            evictions: self.evictions,
            invalidations: self.invalidations,
            memory_usage_bytes: self.memory_usage_bytes,
            avg_entry_size: self.avg_entry_size,
            avg_computation_saved_ms: self.avg_computation_saved_ms,
            total_computation_saved_ms: self.total_computation_saved_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_operations() {
        let manager = QueryCacheManager::new(CacheConfig::default());

        let result = QueryResult {
            rows: vec![
                vec![("id".to_string(), serde_json::Value::Number(1.into()))]
                    .into_iter()
                    .collect(),
            ],
            row_count: 1,
            execution_time_ms: 150,
            cached: false,
        };

        // Put in cache
        manager
            .put(
                "SELECT * FROM users WHERE id = 1",
                &["1".to_string()],
                None,
                result.clone(),
                150,
                vec!["users".to_string()],
            )
            .unwrap();

        // Get from cache
        let cached = manager.get("SELECT * FROM users WHERE id = 1", &["1".to_string()], None);

        assert!(cached.is_some());
        assert!(cached.unwrap().cached);
    }

    #[test]
    fn test_cache_invalidation() {
        let manager = QueryCacheManager::new(CacheConfig::default());

        let result = QueryResult {
            rows: vec![],
            row_count: 0,
            execution_time_ms: 100,
            cached: false,
        };

        // Cache a query
        manager
            .put(
                "SELECT COUNT(*) FROM orders",
                &[],
                None,
                result,
                100,
                vec!["orders".to_string()],
            )
            .unwrap();

        // Should be in cache
        assert!(manager
            .get("SELECT COUNT(*) FROM orders", &[], None)
            .is_some());

        // Invalidate table
        manager.invalidate_table("orders").unwrap();

        // Should not be in cache after invalidation
        assert!(manager
            .get("SELECT COUNT(*) FROM orders", &[], None)
            .is_none());
    }

    #[test]
    fn test_cache_ttl() {
        let mut config = CacheConfig::default();
        config.ttl_seconds = 0; // Immediate expiration

        let manager = QueryCacheManager::new(config);

        let result = QueryResult {
            rows: vec![],
            row_count: 0,
            execution_time_ms: 100,
            cached: false,
        };

        manager
            .put(
                "SELECT * FROM temp",
                &[],
                None,
                result,
                100,
                vec!["temp".to_string()],
            )
            .unwrap();

        // Should be expired immediately
        std::thread::sleep(Duration::from_millis(10));
        assert!(manager.get("SELECT * FROM temp", &[], None).is_none());
    }
}
