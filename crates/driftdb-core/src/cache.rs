//! Query Result Caching Module
//!
//! Provides efficient caching of query results to improve read performance.
//! Features:
//! - LRU eviction policy
//! - TTL-based expiration
//! - Query fingerprinting for cache keys
//! - Cache statistics and monitoring

use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, trace};

use crate::errors::Result;
use crate::query::QueryResult;

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of cached queries
    pub max_entries: usize,
    /// Default TTL for cached results
    pub default_ttl: Duration,
    /// Enable caching for temporal queries
    pub cache_temporal: bool,
    /// Enable caching for transactional queries
    pub cache_transactional: bool,
    /// Maximum result size to cache (in bytes)
    pub max_result_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            default_ttl: Duration::from_secs(300), // 5 minutes
            cache_temporal: false,                 // Don't cache temporal queries by default
            cache_transactional: false,            // Don't cache within transactions
            max_result_size: 10 * 1024 * 1024,     // 10MB
        }
    }
}

/// Cache entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    result: QueryResult,
    created_at: Instant,
    ttl: Duration,
    hit_count: u64,
    size_bytes: usize,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Query cache key
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    query_hash: String,
    database: String,
    user: Option<String>,
}

/// Query result cache
pub struct QueryCache {
    config: CacheConfig,
    cache: Arc<RwLock<LruCache<CacheKey, CacheEntry>>>,
    stats: Arc<RwLock<CacheStatistics>>,
}

/// Cache statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStatistics {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub expired_entries: u64,
    pub total_size_bytes: usize,
    pub avg_entry_size: usize,
}

impl CacheStatistics {
    pub fn hit_rate(&self) -> f64 {
        if self.total_queries == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.total_queries as f64
        }
    }
}

impl QueryCache {
    /// Create a new query cache
    pub fn new(config: CacheConfig) -> Self {
        use std::num::NonZeroUsize;
        let capacity = NonZeroUsize::new(config.max_entries)
            .unwrap_or_else(|| NonZeroUsize::new(1000).unwrap());
        let cache = LruCache::new(capacity);
        Self {
            config,
            cache: Arc::new(RwLock::new(cache)),
            stats: Arc::new(RwLock::new(CacheStatistics::default())),
        }
    }

    /// Generate a cache key from a query
    pub fn generate_key(&self, query: &str, database: &str, user: Option<&str>) -> CacheKey {
        // Create a fingerprint of the query
        let mut hasher = Sha256::new();
        hasher.update(query.as_bytes());
        let query_hash = format!("{:x}", hasher.finalize());

        CacheKey {
            query_hash,
            database: database.to_string(),
            user: user.map(|s| s.to_string()),
        }
    }

    /// Check if a query should be cached
    pub fn should_cache(&self, query: &str, in_transaction: bool) -> bool {
        // Don't cache writes
        let query_upper = query.trim().to_uppercase();
        if query_upper.starts_with("INSERT")
            || query_upper.starts_with("UPDATE")
            || query_upper.starts_with("DELETE")
            || query_upper.starts_with("CREATE")
            || query_upper.starts_with("DROP")
            || query_upper.starts_with("ALTER")
        {
            return false;
        }

        // Don't cache transaction control commands
        if query_upper.starts_with("BEGIN")
            || query_upper.starts_with("COMMIT")
            || query_upper.starts_with("ROLLBACK")
        {
            return false;
        }

        // Check transaction policy
        if in_transaction && !self.config.cache_transactional {
            return false;
        }

        // Check temporal query policy
        if query_upper.contains("AS OF") && !self.config.cache_temporal {
            return false;
        }

        true
    }

    /// Get a cached query result
    pub fn get(&self, key: &CacheKey) -> Option<QueryResult> {
        let mut cache = self.cache.write();
        let mut stats = self.stats.write();

        stats.total_queries += 1;

        // Check if entry exists and is not expired
        if let Some(entry) = cache.get_mut(key) {
            if entry.is_expired() {
                debug!("Cache entry expired for key: {:?}", key);
                cache.pop(key);
                stats.cache_misses += 1;
                stats.expired_entries += 1;
                return None;
            }

            entry.hit_count += 1;
            stats.cache_hits += 1;

            trace!("Cache hit for key: {:?} (hits: {})", key, entry.hit_count);
            Some(entry.result.clone())
        } else {
            stats.cache_misses += 1;
            trace!("Cache miss for key: {:?}", key);
            None
        }
    }

    /// Put a query result in the cache
    pub fn put(&self, key: CacheKey, result: QueryResult) -> Result<()> {
        self.put_with_ttl(key, result, self.config.default_ttl)
    }

    /// Put a query result with custom TTL
    pub fn put_with_ttl(&self, key: CacheKey, result: QueryResult, ttl: Duration) -> Result<()> {
        // Estimate result size
        let size_bytes = self.estimate_result_size(&result);

        // Check size limit
        if size_bytes > self.config.max_result_size {
            debug!("Result too large to cache: {} bytes", size_bytes);
            return Ok(());
        }

        let entry = CacheEntry {
            result,
            created_at: Instant::now(),
            ttl,
            hit_count: 0,
            size_bytes,
        };

        let mut cache = self.cache.write();
        let mut stats = self.stats.write();

        // Check if we're replacing an entry
        if let Some(old_entry) = cache.peek(&key) {
            stats.total_size_bytes = stats.total_size_bytes.saturating_sub(old_entry.size_bytes);
        }

        // Add new entry (may trigger eviction)
        if cache.put(key.clone(), entry).is_some() {
            stats.evictions += 1;
        }

        stats.total_size_bytes += size_bytes;
        stats.avg_entry_size = if cache.len() > 0 {
            stats.total_size_bytes / cache.len()
        } else {
            0
        };

        debug!(
            "Cached result with key: {:?}, size: {} bytes",
            key, size_bytes
        );
        Ok(())
    }

    /// Invalidate cache entries matching a pattern
    pub fn invalidate_pattern(&self, pattern: &str) {
        let mut cache = self.cache.write();
        let keys_to_remove: Vec<CacheKey> = cache
            .iter()
            .filter(|(k, _)| k.query_hash.contains(pattern))
            .map(|(k, _)| k.clone())
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            cache.pop(&key);
        }

        debug!(
            "Invalidated {} cache entries matching pattern: {}",
            count, pattern
        );
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        let mut stats = self.stats.write();

        cache.clear();
        stats.total_size_bytes = 0;
        stats.avg_entry_size = 0;

        debug!("Cache cleared");
    }

    /// Get cache statistics
    pub fn statistics(&self) -> CacheStatistics {
        self.stats.read().clone()
    }

    /// Estimate the size of a query result
    fn estimate_result_size(&self, result: &QueryResult) -> usize {
        // Simple estimation based on serialized size
        // In production, we'd use a more accurate method
        match result {
            QueryResult::Rows { data } => {
                data.len() * 100 // Rough estimate: 100 bytes per row
            }
            QueryResult::DriftHistory { events } => {
                events.len() * 100 // Rough estimate: 100 bytes per event
            }
            QueryResult::Success { message } => {
                message.len() + 50 // Message plus overhead
            }
            QueryResult::Error { message } => {
                message.len() + 50 // Message plus overhead
            }
            QueryResult::Plan { plan } => {
                // Query plans are relatively small
                plan.steps.len() * 50 + 100
            }
        }
    }

    /// Remove expired entries
    pub fn cleanup_expired(&self) {
        let mut cache = self.cache.write();
        let mut stats = self.stats.write();

        let expired_keys: Vec<CacheKey> = cache
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            if let Some(entry) = cache.pop(&key) {
                stats.expired_entries += 1;
                stats.total_size_bytes = stats.total_size_bytes.saturating_sub(entry.size_bytes);
            }
        }

        if cache.len() > 0 {
            stats.avg_entry_size = stats.total_size_bytes / cache.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cache_basic_operations() {
        let config = CacheConfig {
            max_entries: 10,
            default_ttl: Duration::from_secs(60),
            ..Default::default()
        };

        let cache = QueryCache::new(config);

        // Test key generation
        let key = cache.generate_key("SELECT * FROM users", "testdb", Some("user1"));

        // Test put and get
        let result = QueryResult::Rows {
            data: vec![json!({"id": 1, "name": "Alice"})],
        };

        cache.put(key.clone(), result.clone()).unwrap();

        let cached = cache.get(&key);
        assert!(cached.is_some());

        // Test statistics
        let stats = cache.statistics();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 0);
    }

    #[test]
    fn test_cache_expiration() {
        let config = CacheConfig {
            max_entries: 10,
            default_ttl: Duration::from_millis(1),
            ..Default::default()
        };

        let cache = QueryCache::new(config);
        let key = cache.generate_key("SELECT * FROM users", "testdb", None);

        let result = QueryResult::Rows { data: vec![] };

        cache.put(key.clone(), result).unwrap();

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(2));

        let cached = cache.get(&key);
        assert!(cached.is_none());

        let stats = cache.statistics();
        assert_eq!(stats.expired_entries, 1);
    }

    #[test]
    fn test_should_cache() {
        let cache = QueryCache::new(CacheConfig::default());

        // Should cache reads
        assert!(cache.should_cache("SELECT * FROM users", false));

        // Should not cache writes
        assert!(!cache.should_cache("INSERT INTO users VALUES (1)", false));
        assert!(!cache.should_cache("UPDATE users SET name='Bob'", false));
        assert!(!cache.should_cache("DELETE FROM users", false));

        // Should not cache transaction commands
        assert!(!cache.should_cache("BEGIN", false));
        assert!(!cache.should_cache("COMMIT", false));

        // Should not cache temporal queries by default
        assert!(!cache.should_cache("SELECT * FROM users AS OF @seq:1", false));
    }
}
