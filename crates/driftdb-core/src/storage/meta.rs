use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::errors::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub last_sequence: u64,
    pub last_snapshot_sequence: u64,
    pub segment_count: u64,
    pub snapshot_interval: u64,
    pub compact_threshold: u64,
}

impl Default for TableMeta {
    fn default() -> Self {
        Self {
            last_sequence: 0,
            last_snapshot_sequence: 0,
            segment_count: 1,
            snapshot_interval: 100_000,
            compact_threshold: 128 * 1024 * 1024,
        }
    }
}

impl TableMeta {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}
