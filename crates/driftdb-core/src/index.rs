use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::errors::{DriftError, Result};

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

    pub fn find(&self, value: &str) -> Option<&HashSet<String>> {
        self.entries.get(value)
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
