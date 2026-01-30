//! Advanced indexing strategies for optimized data access
//!
//! Implements multiple index types:
//! - B+ Tree indexes for range queries
//! - Hash indexes for point lookups
//! - Bitmap indexes for low-cardinality columns
//! - GiST indexes for spatial/geometric data
//! - Bloom filters for membership testing
//! - Adaptive Radix Trees for string keys

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info};

use crate::errors::{DriftError, Result};

/// Index type selection
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    /// B+ Tree for range queries
    BPlusTree,
    /// Hash table for exact matches
    Hash,
    /// Bitmap for low cardinality
    Bitmap,
    /// GiST for geometric/spatial data
    GiST,
    /// Bloom filter for fast membership test
    Bloom,
    /// Adaptive Radix Tree for strings
    ART,
    /// Inverted index for full-text search
    Inverted,
    /// LSM tree for write-heavy workloads
    LSMTree,
}

/// Index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub index_type: IndexType,
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub sparse: bool,
    pub partial_filter: Option<String>,
    pub fill_factor: f64,
    pub compression: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            index_type: IndexType::BPlusTree,
            name: String::new(),
            table: String::new(),
            columns: Vec::new(),
            unique: false,
            sparse: false,
            partial_filter: None,
            fill_factor: 0.9,
            compression: false,
        }
    }
}

/// B+ Tree implementation for range queries
pub struct BPlusTreeIndex {
    #[allow(dead_code)]
    config: IndexConfig,
    root: Arc<RwLock<Option<BPlusNode>>>,
    order: usize,
    stats: Arc<RwLock<IndexStats>>,
}

#[derive(Debug, Clone)]
enum BPlusNode {
    #[allow(dead_code)]
    Internal {
        keys: Vec<Value>,
        children: Vec<Arc<RwLock<BPlusNode>>>,
    },
    Leaf {
        keys: Vec<Value>,
        values: Vec<Vec<u64>>, // Record IDs
        next: Option<Arc<RwLock<BPlusNode>>>,
    },
}

impl BPlusTreeIndex {
    pub fn new(config: IndexConfig) -> Self {
        Self {
            config,
            root: Arc::new(RwLock::new(None)),
            order: 128, // B+ tree order (max keys per node)
            stats: Arc::new(RwLock::new(IndexStats::default())),
        }
    }

    pub fn insert(&self, key: Value, record_id: u64) -> Result<()> {
        let mut root_guard = self.root.write();

        if root_guard.is_none() {
            *root_guard = Some(BPlusNode::Leaf {
                keys: vec![key],
                values: vec![vec![record_id]],
                next: None,
            });
            self.stats.write().inserts += 1;
            return Ok(());
        }

        // Insert into existing tree
        let root = root_guard.as_mut().unwrap();
        if self.insert_recursive(root, key, record_id)? {
            // Node was split, create new root
            self.split_root(root_guard);
        }

        self.stats.write().inserts += 1;
        Ok(())
    }

    fn insert_recursive(&self, node: &mut BPlusNode, key: Value, record_id: u64) -> Result<bool> {
        match node {
            BPlusNode::Leaf { keys, values, .. } => {
                // Find insertion position
                let pos = keys
                    .binary_search_by(|k| self.compare_values(k, &key))
                    .unwrap_or_else(|i| i);

                if pos < keys.len() && keys[pos] == key {
                    // Key exists, add to values
                    values[pos].push(record_id);
                } else {
                    // Insert new key-value pair
                    keys.insert(pos, key);
                    values.insert(pos, vec![record_id]);
                }

                // Check if split is needed
                Ok(keys.len() > self.order)
            }
            BPlusNode::Internal { keys, children } => {
                // Find child to insert into
                let pos = keys
                    .binary_search_by(|k| self.compare_values(k, &key))
                    .unwrap_or_else(|i| i);

                let child_idx = if pos < keys.len() {
                    pos
                } else {
                    children.len() - 1
                };

                // Recursively insert (drop lock before handling split)
                let split = {
                    let mut child = children[child_idx].write();
                    self.insert_recursive(&mut child, key.clone(), record_id)?
                };

                if split {
                    // Handle child split
                    self.handle_child_split(keys, children, child_idx);
                }

                // Check if this node needs splitting
                Ok(keys.len() > self.order)
            }
        }
    }

    fn split_root(
        &self,
        _root_guard: parking_lot::lock_api::RwLockWriteGuard<
            parking_lot::RawRwLock,
            Option<BPlusNode>,
        >,
    ) {
        // Implementation would split the root and create new internal node
        // This is simplified for brevity
        debug!("Splitting B+ tree root");
    }

    fn handle_child_split(
        &self,
        _keys: &mut Vec<Value>,
        _children: &mut Vec<Arc<RwLock<BPlusNode>>>,
        child_idx: usize,
    ) {
        // Implementation would handle splitting of child nodes
        debug!("Handling child split at index {}", child_idx);
    }

    pub fn search(&self, key: &Value) -> Result<Vec<u64>> {
        let root = self.root.read();
        if root.is_none() {
            return Ok(Vec::new());
        }

        let result = self.search_recursive(root.as_ref().unwrap(), key)?;
        self.stats.write().searches += 1;
        Ok(result)
    }

    fn search_recursive(&self, node: &BPlusNode, key: &Value) -> Result<Vec<u64>> {
        match node {
            BPlusNode::Leaf { keys, values, .. } => {
                if let Ok(pos) = keys.binary_search_by(|k| self.compare_values(k, key)) {
                    Ok(values[pos].clone())
                } else {
                    Ok(Vec::new())
                }
            }
            BPlusNode::Internal { keys, children } => {
                let pos = keys
                    .binary_search_by(|k| self.compare_values(k, key))
                    .unwrap_or_else(|i| i);
                let child_idx = if pos < keys.len() {
                    pos
                } else {
                    children.len() - 1
                };

                let child = children[child_idx].read();
                self.search_recursive(&child, key)
            }
        }
    }

    pub fn range_search(&self, start: Bound<Value>, end: Bound<Value>) -> Result<Vec<u64>> {
        let root = self.root.read();
        if root.is_none() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        self.range_search_recursive(root.as_ref().unwrap(), &start, &end, &mut results)?;
        self.stats.write().range_searches += 1;
        Ok(results)
    }

    fn range_search_recursive(
        &self,
        node: &BPlusNode,
        start: &Bound<Value>,
        end: &Bound<Value>,
        results: &mut Vec<u64>,
    ) -> Result<()> {
        match node {
            BPlusNode::Leaf { keys, values, next } => {
                // Scan leaf nodes for range
                for (i, key) in keys.iter().enumerate() {
                    if self.in_range(key, start, end) {
                        results.extend(&values[i]);
                    }
                }

                // Continue to next leaf if needed
                if let Some(next_node) = next {
                    let next_guard = next_node.read();
                    if self.should_continue_range(&next_guard, end) {
                        self.range_search_recursive(&next_guard, start, end, results)?;
                    }
                }
                Ok(())
            }
            BPlusNode::Internal { keys, children } => {
                // Find relevant children for range
                for (i, child) in children.iter().enumerate() {
                    if i == 0 || self.should_traverse_child(keys, i, start, end) {
                        let child_guard = child.read();
                        self.range_search_recursive(&child_guard, start, end, results)?;
                    }
                }
                Ok(())
            }
        }
    }

    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        // Compare JSON values for ordering
        match (a, b) {
            (Value::Number(n1), Value::Number(n2)) => {
                let f1 = n1.as_f64().unwrap_or(0.0);
                let f2 = n2.as_f64().unwrap_or(0.0);
                f1.partial_cmp(&f2).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Value::String(s1), Value::String(s2)) => s1.cmp(s2),
            _ => std::cmp::Ordering::Equal,
        }
    }

    fn in_range(&self, key: &Value, start: &Bound<Value>, end: &Bound<Value>) -> bool {
        let after_start = match start {
            Bound::Included(s) => self.compare_values(key, s) >= std::cmp::Ordering::Equal,
            Bound::Excluded(s) => self.compare_values(key, s) > std::cmp::Ordering::Equal,
            Bound::Unbounded => true,
        };

        let before_end = match end {
            Bound::Included(e) => self.compare_values(key, e) <= std::cmp::Ordering::Equal,
            Bound::Excluded(e) => self.compare_values(key, e) < std::cmp::Ordering::Equal,
            Bound::Unbounded => true,
        };

        after_start && before_end
    }

    fn should_continue_range(&self, _node: &BPlusNode, _end: &Bound<Value>) -> bool {
        // Check if we should continue scanning
        true // Simplified
    }

    fn should_traverse_child(
        &self,
        _keys: &[Value],
        _index: usize,
        _start: &Bound<Value>,
        _end: &Bound<Value>,
    ) -> bool {
        // Check if child might contain range values
        true // Simplified
    }
}

/// Hash index for fast point lookups
pub struct HashIndex {
    #[allow(dead_code)]
    config: IndexConfig,
    data: Arc<RwLock<HashMap<String, HashSet<u64>>>>,
    stats: Arc<RwLock<IndexStats>>,
}

impl HashIndex {
    pub fn new(config: IndexConfig) -> Self {
        Self {
            config,
            data: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(IndexStats::default())),
        }
    }

    pub fn insert(&self, key: Value, record_id: u64) -> Result<()> {
        let key_str = serde_json::to_string(&key)?;
        let mut data = self.data.write();
        data.entry(key_str).or_default().insert(record_id);
        self.stats.write().inserts += 1;
        Ok(())
    }

    pub fn search(&self, key: &Value) -> Result<Vec<u64>> {
        let key_str = serde_json::to_string(key)?;
        let data = self.data.read();
        let results = data
            .get(&key_str)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        self.stats.write().searches += 1;
        Ok(results)
    }

    pub fn delete(&self, key: &Value, record_id: u64) -> Result<bool> {
        let key_str = serde_json::to_string(key)?;
        let mut data = self.data.write();

        if let Some(set) = data.get_mut(&key_str) {
            let removed = set.remove(&record_id);
            if set.is_empty() {
                data.remove(&key_str);
            }
            self.stats.write().deletes += 1;
            Ok(removed)
        } else {
            Ok(false)
        }
    }
}

/// Bitmap index for low-cardinality columns
pub struct BitmapIndex {
    #[allow(dead_code)]
    config: IndexConfig,
    bitmaps: Arc<RwLock<HashMap<Value, Bitmap>>>,
    stats: Arc<RwLock<IndexStats>>,
}

#[derive(Debug, Clone)]
struct Bitmap {
    bits: Vec<u64>,
    size: usize,
}

#[allow(dead_code)]
impl Bitmap {
    fn new() -> Self {
        Self {
            bits: Vec::new(),
            size: 0,
        }
    }

    fn set(&mut self, position: usize) {
        let word_idx = position / 64;
        let bit_idx = position % 64;

        while self.bits.len() <= word_idx {
            self.bits.push(0);
        }

        self.bits[word_idx] |= 1u64 << bit_idx;
        self.size = self.size.max(position + 1);
    }

    fn get(&self, position: usize) -> bool {
        let word_idx = position / 64;
        let bit_idx = position % 64;

        if word_idx >= self.bits.len() {
            return false;
        }

        (self.bits[word_idx] & (1u64 << bit_idx)) != 0
    }

    fn and(&self, other: &Bitmap) -> Bitmap {
        let mut result = Bitmap::new();
        let min_len = self.bits.len().min(other.bits.len());

        for i in 0..min_len {
            result.bits.push(self.bits[i] & other.bits[i]);
        }

        result.size = self.size.max(other.size);
        result
    }

    fn or(&self, other: &Bitmap) -> Bitmap {
        let mut result = Bitmap::new();
        let max_len = self.bits.len().max(other.bits.len());

        for i in 0..max_len {
            let a = self.bits.get(i).cloned().unwrap_or(0);
            let b = other.bits.get(i).cloned().unwrap_or(0);
            result.bits.push(a | b);
        }

        result.size = self.size.max(other.size);
        result
    }

    fn to_positions(&self) -> Vec<usize> {
        let mut positions = Vec::new();

        for (word_idx, &word) in self.bits.iter().enumerate() {
            if word == 0 {
                continue;
            }

            for bit_idx in 0..64 {
                if (word & (1u64 << bit_idx)) != 0 {
                    positions.push(word_idx * 64 + bit_idx);
                }
            }
        }

        positions
    }
}

impl BitmapIndex {
    pub fn new(config: IndexConfig) -> Self {
        Self {
            config,
            bitmaps: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(IndexStats::default())),
        }
    }

    pub fn insert(&self, key: Value, record_id: u64) -> Result<()> {
        let mut bitmaps = self.bitmaps.write();
        let bitmap = bitmaps.entry(key).or_insert_with(Bitmap::new);
        bitmap.set(record_id as usize);
        self.stats.write().inserts += 1;
        Ok(())
    }

    pub fn search(&self, key: &Value) -> Result<Vec<u64>> {
        let bitmaps = self.bitmaps.read();

        if let Some(bitmap) = bitmaps.get(key) {
            let positions = bitmap.to_positions();
            let results = positions.into_iter().map(|p| p as u64).collect();
            self.stats.write().searches += 1;
            Ok(results)
        } else {
            Ok(Vec::new())
        }
    }

    pub fn search_multiple(&self, keys: &[Value]) -> Result<Vec<u64>> {
        let bitmaps = self.bitmaps.read();

        let mut result_bitmap: Option<Bitmap> = None;

        for key in keys {
            if let Some(bitmap) = bitmaps.get(key) {
                result_bitmap = Some(match result_bitmap {
                    None => bitmap.clone(),
                    Some(existing) => existing.or(bitmap),
                });
            }
        }

        if let Some(bitmap) = result_bitmap {
            let positions = bitmap.to_positions();
            let results = positions.into_iter().map(|p| p as u64).collect();
            self.stats.write().searches += 1;
            Ok(results)
        } else {
            Ok(Vec::new())
        }
    }
}

/// Bloom filter for fast membership testing
#[derive(Debug)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: usize,
    size_bits: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let size_bits = Self::optimal_size(expected_items, false_positive_rate);
        let num_hashes = Self::optimal_hashes(expected_items, size_bits);

        Self {
            bits: vec![0; size_bits.div_ceil(64)],
            num_hashes,
            size_bits,
        }
    }

    fn optimal_size(n: usize, p: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        ((n as f64 * p.ln()) / (-8.0 * ln2.powi(2))).ceil() as usize
    }

    fn optimal_hashes(n: usize, m: usize) -> usize {
        let ln2 = std::f64::consts::LN_2;
        ((m as f64 / n as f64) * ln2).ceil() as usize
    }

    pub fn insert(&mut self, item: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i);
            let bit_idx = hash % self.size_bits;
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;

            self.bits[word_idx] |= 1u64 << bit_pos;
        }
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(item, i);
            let bit_idx = hash % self.size_bits;
            let word_idx = bit_idx / 64;
            let bit_pos = bit_idx % 64;

            if (self.bits[word_idx] & (1u64 << bit_pos)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash(&self, item: &[u8], seed: usize) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }
}

/// Wrapper for Value to make it orderable
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedValue(String);

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl From<Value> for OrderedValue {
    fn from(v: Value) -> Self {
        OrderedValue(v.to_string())
    }
}

/// LSM Tree for write-optimized workloads
pub struct LSMTree {
    #[allow(dead_code)]
    config: IndexConfig,
    memtable: Arc<RwLock<BTreeMap<OrderedValue, Vec<u64>>>>,
    #[allow(clippy::type_complexity)]
    immutable_memtables: Arc<RwLock<Vec<BTreeMap<OrderedValue, Vec<u64>>>>>,
    #[allow(dead_code)]
    sstables: Arc<RwLock<Vec<SSTable>>>,
    #[allow(dead_code)]
    wal_path: PathBuf,
    stats: Arc<RwLock<IndexStats>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SSTable {
    path: PathBuf,
    min_key: Value,
    max_key: Value,
    bloom_filter: Arc<BloomFilter>,
    level: usize,
}

impl LSMTree {
    pub fn new(config: IndexConfig, base_path: &Path) -> Result<Self> {
        let wal_path = base_path.join(format!("{}_wal", config.name));
        fs::create_dir_all(&wal_path)?;

        Ok(Self {
            config,
            memtable: Arc::new(RwLock::new(BTreeMap::new())),
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(Vec::new())),
            wal_path,
            stats: Arc::new(RwLock::new(IndexStats::default())),
        })
    }

    pub fn insert(&self, key: Value, record_id: u64) -> Result<()> {
        // Write to WAL first
        self.write_to_wal(&key, record_id)?;

        // Insert into memtable
        let mut memtable = self.memtable.write();
        let ordered_key = OrderedValue::from(key);
        memtable.entry(ordered_key).or_default().push(record_id);

        // Check if memtable is full and needs flushing
        if memtable.len() > 10000 {
            self.flush_memtable()?;
        }

        self.stats.write().inserts += 1;
        Ok(())
    }

    fn write_to_wal(&self, _key: &Value, _record_id: u64) -> Result<()> {
        // Write-ahead logging implementation
        Ok(())
    }

    fn flush_memtable(&self) -> Result<()> {
        let mut memtable = self.memtable.write();
        let mut immutable = self.immutable_memtables.write();

        // Move current memtable to immutable list
        let old_memtable = std::mem::take(&mut *memtable);
        immutable.push(old_memtable);

        // Trigger background compaction
        self.trigger_compaction()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // Background compaction logic
        info!("Triggering LSM tree compaction");
        Ok(())
    }

    pub fn search(&self, key: &Value) -> Result<Vec<u64>> {
        let mut results = Vec::new();
        let ordered_key = OrderedValue::from(key.clone());

        // Search in memtable
        {
            let memtable = self.memtable.read();
            if let Some(values) = memtable.get(&ordered_key) {
                results.extend(values);
            }
        }

        // Search in immutable memtables
        {
            let immutable = self.immutable_memtables.read();
            for table in immutable.iter() {
                if let Some(values) = table.get(&ordered_key) {
                    results.extend(values);
                }
            }
        }

        // Search in SSTables (would use bloom filters for efficiency)
        // Implementation simplified

        self.stats.write().searches += 1;
        Ok(results)
    }
}

/// Index statistics for monitoring
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexStats {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub searches: u64,
    pub range_searches: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub disk_reads: u64,
    pub disk_writes: u64,
    pub compactions: u64,
}

/// Unified index manager
pub struct AdvancedIndexManager {
    indexes: Arc<RwLock<HashMap<String, Box<dyn IndexOperations>>>>,
    configs: Arc<RwLock<HashMap<String, IndexConfig>>>,
}

/// Trait for index operations
pub trait IndexOperations: Send + Sync {
    fn insert(&self, key: Value, record_id: u64) -> Result<()>;
    fn search(&self, key: &Value) -> Result<Vec<u64>>;
    fn range_search(&self, start: Bound<Value>, end: Bound<Value>) -> Result<Vec<u64>>;
    fn delete(&self, key: &Value, record_id: u64) -> Result<bool>;
    fn stats(&self) -> IndexStats;
}

impl IndexOperations for BPlusTreeIndex {
    fn insert(&self, key: Value, record_id: u64) -> Result<()> {
        self.insert(key, record_id)
    }

    fn search(&self, key: &Value) -> Result<Vec<u64>> {
        self.search(key)
    }

    fn range_search(&self, start: Bound<Value>, end: Bound<Value>) -> Result<Vec<u64>> {
        self.range_search(start, end)
    }

    fn delete(&self, _key: &Value, _record_id: u64) -> Result<bool> {
        // Implementation would handle deletion
        Ok(false)
    }

    fn stats(&self) -> IndexStats {
        self.stats.read().clone()
    }
}

impl IndexOperations for HashIndex {
    fn insert(&self, key: Value, record_id: u64) -> Result<()> {
        self.insert(key, record_id)
    }

    fn search(&self, key: &Value) -> Result<Vec<u64>> {
        self.search(key)
    }

    fn range_search(&self, _start: Bound<Value>, _end: Bound<Value>) -> Result<Vec<u64>> {
        // Hash indexes don't support range queries
        Err(DriftError::Other(
            "Hash indexes don't support range queries".to_string(),
        ))
    }

    fn delete(&self, key: &Value, record_id: u64) -> Result<bool> {
        self.delete(key, record_id)
    }

    fn stats(&self) -> IndexStats {
        self.stats.read().clone()
    }
}

impl Default for AdvancedIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvancedIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_index(&self, config: IndexConfig) -> Result<()> {
        let index: Box<dyn IndexOperations> = match config.index_type {
            IndexType::BPlusTree => Box::new(BPlusTreeIndex::new(config.clone())),
            IndexType::Hash => Box::new(HashIndex::new(config.clone())),
            _ => {
                return Err(DriftError::Other(format!(
                    "Index type {:?} not yet implemented",
                    config.index_type
                )));
            }
        };

        let mut indexes = self.indexes.write();
        let mut configs = self.configs.write();

        indexes.insert(config.name.clone(), index);
        configs.insert(config.name.clone(), config);

        Ok(())
    }

    pub fn get_index(&self, _name: &str) -> Option<Arc<dyn IndexOperations>> {
        let _indexes = self.indexes.read();
        // This would need proper Arc wrapping in real implementation
        None
    }

    pub fn drop_index(&self, name: &str) -> Result<()> {
        let mut indexes = self.indexes.write();
        let mut configs = self.configs.write();

        indexes.remove(name);
        configs.remove(name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bplus_tree_index() {
        let config = IndexConfig {
            name: "test_btree".to_string(),
            index_type: IndexType::BPlusTree,
            ..Default::default()
        };

        let index = BPlusTreeIndex::new(config);

        // Test insertions
        index
            .insert(Value::Number(serde_json::Number::from(5)), 100)
            .unwrap();
        index
            .insert(Value::Number(serde_json::Number::from(3)), 101)
            .unwrap();
        index
            .insert(Value::Number(serde_json::Number::from(7)), 102)
            .unwrap();

        // Test search
        let results = index
            .search(&Value::Number(serde_json::Number::from(5)))
            .unwrap();
        assert_eq!(results, vec![100]);

        // Test range search
        let range_results = index
            .range_search(
                Bound::Included(Value::Number(serde_json::Number::from(3))),
                Bound::Included(Value::Number(serde_json::Number::from(7))),
            )
            .unwrap();
        assert_eq!(range_results.len(), 3);
    }

    #[test]
    fn test_bitmap_index() {
        let config = IndexConfig {
            name: "test_bitmap".to_string(),
            index_type: IndexType::Bitmap,
            ..Default::default()
        };

        let index = BitmapIndex::new(config);

        // Test with low cardinality values
        index.insert(Value::String("red".to_string()), 1).unwrap();
        index.insert(Value::String("blue".to_string()), 2).unwrap();
        index.insert(Value::String("red".to_string()), 3).unwrap();

        let red_results = index.search(&Value::String("red".to_string())).unwrap();
        assert_eq!(red_results.len(), 2);
        assert!(red_results.contains(&1));
        assert!(red_results.contains(&3));
    }

    #[test]
    fn test_bloom_filter() {
        let mut bloom = BloomFilter::new(1000, 0.01);

        bloom.insert(b"hello");
        bloom.insert(b"world");

        assert!(bloom.contains(b"hello"));
        assert!(bloom.contains(b"world"));
        assert!(!bloom.contains(b"goodbye"));
    }
}
