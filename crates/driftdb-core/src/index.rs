use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::errors::{DriftError, Result};

/// A B-tree secondary index for efficient lookups on non-primary-key columns.
///
/// The `Index` struct maintains a mapping from column values to sets of primary keys,
/// enabling fast equality lookups without scanning the entire table.
///
/// # NULL Value Handling
///
/// NULL values are explicitly excluded from the index. When inserting a value,
/// if the JSON value is `null`, it is silently ignored and no entry is created.
/// This means queries using the index will never match rows with NULL values
/// in the indexed column.
///
/// # Thread Safety
///
/// The `Index` struct itself is not thread-safe. Thread safety is provided at
/// the `IndexManager` level through the engine's locking mechanisms. The engine
/// uses a process-global write lock for mutations while allowing concurrent
/// reads through snapshots.
///
/// # Storage
///
/// Indexes are persisted to disk using bincode serialization. Each index is
/// stored as a separate `.idx` file in the table's `indexes/` directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub column_name: String,
    pub entries: BTreeMap<String, HashSet<String>>,
}

impl Index {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            entries: BTreeMap::new(),
        }
    }

    /// Insert a value-to-primary-key mapping into the index.
    ///
    /// # NULL Handling
    ///
    /// If `value` is JSON `null`, this method does nothing and returns immediately.
    /// NULL values are intentionally excluded from the index.
    ///
    /// # Value Conversion
    ///
    /// - String values are stored directly
    /// - Other non-null values (numbers, booleans, objects, arrays) are converted
    ///   to their JSON string representation for storage
    pub fn insert(&mut self, value: &serde_json::Value, primary_key: &str) {
        if let Some(val_str) = value.as_str() {
            self.entries
                .entry(val_str.to_string())
                .or_default()
                .insert(primary_key.to_string());
        } else if !value.is_null() {
            let val_str = value.to_string();
            self.entries
                .entry(val_str)
                .or_default()
                .insert(primary_key.to_string());
        }
    }

    /// Remove a value-to-primary-key mapping from the index.
    ///
    /// # Cleanup Behavior
    ///
    /// When the last primary key is removed for a given value, the value entry
    /// itself is also removed from the index. This prevents memory leaks from
    /// accumulating empty entry sets.
    ///
    /// If the value or primary key does not exist in the index, this method
    /// does nothing (no error is raised).
    pub fn remove(&mut self, value: &serde_json::Value, primary_key: &str) {
        let val_str = if let Some(s) = value.as_str() {
            s.to_string()
        } else {
            value.to_string()
        };

        if let Some(keys) = self.entries.get_mut(&val_str) {
            keys.remove(primary_key);
            if keys.is_empty() {
                self.entries.remove(&val_str);
            }
        }
    }

    /// Find all primary keys associated with a given indexed value.
    ///
    /// # Return Value
    ///
    /// Returns `Some(&HashSet<String>)` containing the set of primary keys
    /// that have the specified value in the indexed column.
    ///
    /// Returns `None` if no rows have the specified value (or if all such
    /// rows have been deleted/removed).
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(keys) = index.find("active") {
    ///     for pk in keys {
    ///         // Process each primary key with status="active"
    ///     }
    /// }
    /// ```
    pub fn find(&self, value: &str) -> Option<&HashSet<String>> {
        self.entries.get(value)
    }

    /// Find all primary keys whose indexed value falls within the given
    /// JSON bounds. Either bound may be `None` (half-open range).
    ///
    /// # Why this iterates instead of using `BTreeMap::range`
    ///
    /// The underlying map is keyed by `String` (each row's JSON-stringified
    /// value), so its built-in range query gives lexicographic order — which
    /// matches semantic order for string columns but NOT for numeric columns
    /// (e.g. `"10" < "2"` lexicographically, but `10 > 2` numerically). To
    /// stay correct for both, we walk every distinct key and compare via
    /// `compare_json_values`, which numbers numerically and strings
    /// lexicographically.
    ///
    /// This is O(distinct_keys), not O(log n) — but still asymptotically
    /// better than a full table scan, since the index has at most one entry
    /// per distinct value while the table has one row per primary key. A
    /// future migration to a sort-preserving key encoding could promote
    /// this to true ordered iteration.
    pub fn range(
        &self,
        start: Option<(&serde_json::Value, /*inclusive:*/ bool)>,
        end: Option<(&serde_json::Value, /*inclusive:*/ bool)>,
    ) -> HashSet<String> {
        use std::cmp::Ordering;
        let mut result: HashSet<String> = HashSet::new();
        for (key_str, key_pks) in &self.entries {
            // Reconstruct a JSON value from the stored string key. Numeric
            // values round-trip through serde_json; strings (which were
            // stored without quotes) fall back to Value::String.
            let key_value = serde_json::from_str::<serde_json::Value>(key_str)
                .unwrap_or_else(|_| serde_json::Value::String(key_str.clone()));

            if let Some((lo, lo_incl)) = start {
                let ord = crate::query::predicate::compare_json_values(&key_value, lo);
                let inside = match ord {
                    Ordering::Greater => true,
                    Ordering::Equal => lo_incl,
                    Ordering::Less => false,
                };
                if !inside {
                    continue;
                }
            }
            if let Some((hi, hi_incl)) = end {
                let ord = crate::query::predicate::compare_json_values(&key_value, hi);
                let inside = match ord {
                    Ordering::Less => true,
                    Ordering::Equal => hi_incl,
                    Ordering::Greater => false,
                };
                if !inside {
                    continue;
                }
            }
            result.extend(key_pks.iter().cloned());
        }
        result
    }

    /// Get the number of unique indexed values
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, self)?;
        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(bincode::deserialize_from(reader)?)
    }
}

pub struct IndexManager {
    indexes_dir: PathBuf,
    indexes: BTreeMap<String, Index>,
}

impl IndexManager {
    pub fn new(table_path: &Path) -> Self {
        Self {
            indexes_dir: table_path.join("indexes"),
            indexes: BTreeMap::new(),
        }
    }

    pub fn load_indexes(&mut self, indexed_columns: &HashSet<String>) -> Result<()> {
        for column in indexed_columns {
            let index_path = self.indexes_dir.join(format!("{}.idx", column));
            if index_path.exists() {
                let index = Index::load_from_file(&index_path)?;
                self.indexes.insert(column.clone(), index);
            } else {
                self.indexes
                    .insert(column.clone(), Index::new(column.clone()));
            }
        }
        Ok(())
    }

    pub fn update_indexes(
        &mut self,
        event: &crate::events::Event,
        indexed_columns: &HashSet<String>,
    ) -> Result<()> {
        use crate::events::EventType;

        let pk_str = event.primary_key.to_string();

        match event.event_type {
            EventType::Insert => {
                if let serde_json::Value::Object(map) = &event.payload {
                    for column in indexed_columns {
                        if let Some(value) = map.get(column) {
                            if let Some(index) = self.indexes.get_mut(column) {
                                index.insert(value, &pk_str);
                            }
                        }
                    }
                }
            }
            EventType::Patch => {
                if let serde_json::Value::Object(map) = &event.payload {
                    for column in indexed_columns {
                        if let Some(value) = map.get(column) {
                            if let Some(index) = self.indexes.get_mut(column) {
                                index.insert(value, &pk_str);
                            }
                        }
                    }
                }
            }
            EventType::SoftDelete => {
                for index in self.indexes.values_mut() {
                    let keys_to_remove: Vec<String> = index
                        .entries
                        .iter()
                        .filter_map(|(val, keys)| {
                            if keys.contains(&pk_str) {
                                Some(val.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    for val in keys_to_remove {
                        index.remove(&serde_json::Value::String(val), &pk_str);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn save_all(&self) -> Result<()> {
        fs::create_dir_all(&self.indexes_dir)?;
        for (column, index) in &self.indexes {
            let path = self.indexes_dir.join(format!("{}.idx", column));
            index.save_to_file(path)?;
        }
        Ok(())
    }

    pub fn get_index(&self, column: &str) -> Option<&Index> {
        self.indexes.get(column)
    }

    /// Names of all columns this manager currently has indexes for.
    /// Used as the authoritative set when applying events, since the
    /// table schema's `index: bool` flag isn't updated by post-creation
    /// `CREATE INDEX` calls.
    pub fn indexed_column_names(&self) -> HashSet<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Add a new index for a column
    pub fn add_index(&mut self, column: &str) -> Result<()> {
        if self.indexes.contains_key(column) {
            return Err(DriftError::Other(format!(
                "Index already exists for column '{}'",
                column
            )));
        }

        let index = Index::new(column.to_string());
        self.indexes.insert(column.to_string(), index);
        Ok(())
    }

    /// Build index from existing data
    pub fn build_index_from_data(
        &mut self,
        column: &str,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        // First add the index if it doesn't exist
        if !self.indexes.contains_key(column) {
            self.add_index(column)?;
        }

        // Populate the index with existing data
        if let Some(index) = self.indexes.get_mut(column) {
            for (pk, row) in data {
                if let serde_json::Value::Object(map) = row {
                    if let Some(value) = map.get(column) {
                        index.insert(value, pk);
                    }
                }
            }
            // Save the index to disk
            let index_path = self.indexes_dir.join(format!("{}.idx", column));
            index.save_to_file(&index_path)?;
        }

        Ok(())
    }

    pub fn rebuild_from_state(
        &mut self,
        state: &HashMap<String, serde_json::Value>,
        indexed_columns: &HashSet<String>,
    ) -> Result<()> {
        self.indexes.clear();

        for column in indexed_columns {
            self.indexes
                .insert(column.clone(), Index::new(column.clone()));
        }

        for (pk, row) in state {
            if let serde_json::Value::Object(map) = row {
                for column in indexed_columns {
                    if let Some(value) = map.get(column) {
                        if let Some(index) = self.indexes.get_mut(column) {
                            index.insert(value, pk);
                        }
                    }
                }
            }
        }

        self.save_all()?;
        Ok(())
    }
}
