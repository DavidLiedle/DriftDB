//! Lock-free data structures for high-performance concurrent access
//!
//! Implements RCU (Read-Copy-Update) pattern for read-heavy workloads

use crossbeam_epoch::{self as epoch, Atomic, Owned};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::errors::Result;

/// A lock-free, concurrent hash map optimized for reads
/// Uses RCU pattern with epoch-based memory reclamation
pub struct LockFreeTable {
    /// Atomic pointer to the current version of the data
    data: Atomic<TableData>,
    /// Version counter for optimistic concurrency control
    version: AtomicU64,
    /// Write lock for serializing updates (reads are lock-free)
    write_lock: RwLock<()>,
}

struct TableData {
    map: HashMap<String, Value>,
    #[allow(dead_code)]
    version: u64,
}

impl LockFreeTable {
    pub fn new() -> Self {
        let initial_data = TableData {
            map: HashMap::new(),
            version: 0,
        };

        Self {
            data: Atomic::new(initial_data),
            version: AtomicU64::new(0),
            write_lock: RwLock::new(()),
        }
    }

    /// Lock-free read operation
    pub fn read(&self, key: &str) -> Option<Value> {
        let guard = &epoch::pin();
        let data = self.data.load(Ordering::Acquire, guard);

        // Safe because we're protected by the epoch guard
        unsafe { data.as_ref().and_then(|d| d.map.get(key).cloned()) }
    }

    /// Lock-free scan operation for range queries
    pub fn scan<F>(&self, predicate: F) -> Vec<(String, Value)>
    where
        F: Fn(&str, &Value) -> bool,
    {
        let guard = &epoch::pin();
        let data = self.data.load(Ordering::Acquire, guard);

        unsafe {
            data.as_ref()
                .map(|d| {
                    d.map
                        .iter()
                        .filter(|(k, v)| predicate(k, v))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    /// Write operation - requires brief lock but doesn't block reads
    pub fn write(&self, key: String, value: Value) -> Result<()> {
        // Acquire write lock to serialize updates
        let _lock = self.write_lock.write();

        let guard = &epoch::pin();
        let current = self.data.load(Ordering::Acquire, guard);

        // Create new version with the update
        let mut new_map = unsafe { current.as_ref().map(|d| d.map.clone()).unwrap_or_default() };

        new_map.insert(key, value);

        let new_version = self.version.fetch_add(1, Ordering::Release) + 1;
        let new_data = Owned::new(TableData {
            map: new_map,
            version: new_version,
        });

        // Atomically swap the pointer
        self.data.store(new_data, Ordering::Release);

        // Defer cleanup of old version
        unsafe {
            if !current.is_null() {
                guard.defer_destroy(current);
            }
        }

        Ok(())
    }

    /// Batch write operation for better throughput
    pub fn write_batch(&self, updates: Vec<(String, Value)>) -> Result<()> {
        let _lock = self.write_lock.write();

        let guard = &epoch::pin();
        let current = self.data.load(Ordering::Acquire, guard);

        let mut new_map = unsafe { current.as_ref().map(|d| d.map.clone()).unwrap_or_default() };

        for (key, value) in updates {
            new_map.insert(key, value);
        }

        let new_version = self.version.fetch_add(1, Ordering::Release) + 1;
        let new_data = Owned::new(TableData {
            map: new_map,
            version: new_version,
        });

        self.data.store(new_data, Ordering::Release);

        unsafe {
            if !current.is_null() {
                guard.defer_destroy(current);
            }
        }

        Ok(())
    }

    /// Get current version for optimistic concurrency control
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }
}

/// Lock-free index structure using B-tree with RCU
pub struct LockFreeIndex {
    root: Atomic<IndexNode>,
    version: AtomicU64,
    write_lock: RwLock<()>,
}

struct IndexNode {
    keys: Vec<String>,
    values: Vec<Vec<String>>, // Document IDs for each key
    children: Vec<Arc<IndexNode>>,
    is_leaf: bool,
}

impl LockFreeIndex {
    pub fn new() -> Self {
        let root = IndexNode {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
        };

        Self {
            root: Atomic::new(root),
            version: AtomicU64::new(0),
            write_lock: RwLock::new(()),
        }
    }

    /// Lock-free index lookup
    pub fn lookup(&self, key: &str) -> Vec<String> {
        let guard = &epoch::pin();
        let root = self.root.load(Ordering::Acquire, guard);

        unsafe {
            root.as_ref()
                .map(|node| self.lookup_in_node(node, key))
                .unwrap_or_default()
        }
    }

    fn lookup_in_node(&self, node: &IndexNode, key: &str) -> Vec<String> {
        match node.keys.binary_search_by(|k| k.as_str().cmp(key)) {
            Ok(idx) => node.values[idx].clone(),
            Err(idx) => {
                if !node.is_leaf && idx < node.children.len() {
                    self.lookup_in_node(&node.children[idx], key)
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// Insert into index (requires write lock)
    pub fn insert(&self, key: String, doc_id: String) -> Result<()> {
        let _lock = self.write_lock.write();

        // For simplicity, using a basic approach here
        // In production, would implement proper B-tree insertion with node splitting

        let guard = &epoch::pin();
        let current = self.root.load(Ordering::Acquire, guard);

        let mut new_node = unsafe {
            current
                .as_ref()
                .map(|n| self.clone_node(n))
                .unwrap_or_else(|| IndexNode {
                    keys: Vec::new(),
                    values: Vec::new(),
                    children: Vec::new(),
                    is_leaf: true,
                })
        };

        // Simple insertion for leaf nodes
        match new_node.keys.binary_search(&key) {
            Ok(idx) => {
                new_node.values[idx].push(doc_id);
            }
            Err(idx) => {
                new_node.keys.insert(idx, key);
                new_node.values.insert(idx, vec![doc_id]);
            }
        }

        let _new_version = self.version.fetch_add(1, Ordering::Release) + 1;
        self.root.store(Owned::new(new_node), Ordering::Release);

        unsafe {
            if !current.is_null() {
                guard.defer_destroy(current);
            }
        }

        Ok(())
    }

    fn clone_node(&self, node: &IndexNode) -> IndexNode {
        IndexNode {
            keys: node.keys.clone(),
            values: node.values.clone(),
            children: node.children.clone(),
            is_leaf: node.is_leaf,
        }
    }
}

/// Optimized read path using lock-free structures
pub struct OptimizedReadPath {
    tables: HashMap<String, Arc<LockFreeTable>>,
    indexes: HashMap<String, Arc<LockFreeIndex>>,
}

impl OptimizedReadPath {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, name: String) {
        self.tables.insert(name, Arc::new(LockFreeTable::new()));
    }

    pub fn add_index(&mut self, name: String) {
        self.indexes.insert(name, Arc::new(LockFreeIndex::new()));
    }

    /// Perform lock-free read
    pub fn read(&self, table: &str, key: &str) -> Option<Value> {
        self.tables.get(table).and_then(|t| t.read(key))
    }

    /// Perform lock-free index lookup
    pub fn lookup_index(&self, index: &str, key: &str) -> Vec<String> {
        self.indexes
            .get(index)
            .map(|idx| idx.lookup(key))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_lock_free_table() {
        let table = LockFreeTable::new();

        // Test write and read
        table
            .write("key1".to_string(), json!({"value": 1}))
            .unwrap();
        assert_eq!(table.read("key1"), Some(json!({"value": 1})));
        assert_eq!(table.read("key2"), None);

        // Test overwrite
        table
            .write("key1".to_string(), json!({"value": 2}))
            .unwrap();
        assert_eq!(table.read("key1"), Some(json!({"value": 2})));

        // Test batch write
        table
            .write_batch(vec![
                ("key2".to_string(), json!({"value": 20})),
                ("key3".to_string(), json!({"value": 30})),
            ])
            .unwrap();

        assert_eq!(table.read("key2"), Some(json!({"value": 20})));
        assert_eq!(table.read("key3"), Some(json!({"value": 30})));
    }

    #[test]
    fn test_lock_free_index() {
        let index = LockFreeIndex::new();

        // Test insert and lookup
        index
            .insert("alice".to_string(), "doc1".to_string())
            .unwrap();
        index.insert("bob".to_string(), "doc2".to_string()).unwrap();
        index
            .insert("alice".to_string(), "doc3".to_string())
            .unwrap();

        let alice_docs = index.lookup("alice");
        assert_eq!(alice_docs.len(), 2);
        assert!(alice_docs.contains(&"doc1".to_string()));
        assert!(alice_docs.contains(&"doc3".to_string()));

        let bob_docs = index.lookup("bob");
        assert_eq!(bob_docs, vec!["doc2".to_string()]);

        let charlie_docs = index.lookup("charlie");
        assert!(charlie_docs.is_empty());
    }
}
