use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

use crate::errors::{DriftError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    #[serde(default)]
    pub index: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub primary_key: String,
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(name: String, primary_key: String, columns: Vec<ColumnDef>) -> Self {
        Self {
            name,
            primary_key,
            columns,
        }
    }

    pub fn indexed_columns(&self) -> HashSet<String> {
        self.columns
            .iter()
            .filter(|c| c.index)
            .map(|c| c.name.clone())
            .collect()
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&content)?)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_yaml::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.primary_key.is_empty() {
            return Err(DriftError::Schema("Primary key cannot be empty".into()));
        }

        if !self.has_column(&self.primary_key) {
            return Err(DriftError::Schema(format!(
                "Primary key '{}' not found in columns",
                self.primary_key
            )));
        }

        let mut seen = HashSet::new();
        for col in &self.columns {
            if !seen.insert(&col.name) {
                return Err(DriftError::Schema(format!(
                    "Duplicate column name: {}",
                    col.name
                )));
            }
        }

        Ok(())
    }
}
