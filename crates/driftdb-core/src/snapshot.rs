use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use crate::errors::Result;
use crate::storage::TableStorage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub row_count: usize,
    pub state: HashMap<String, String>, // Store as JSON strings to avoid bincode issues
}

impl Snapshot {
    pub fn create_from_storage(storage: &TableStorage, sequence: u64) -> Result<Self> {
        let state_raw = storage.reconstruct_state_at(Some(sequence))?;

        // Convert serde_json::Value to String for serialization
        let state: HashMap<String, String> = state_raw
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        Ok(Self {
            sequence,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_else(|_| {
                    // Fallback to a reasonable timestamp if system time is broken
                    tracing::error!("System time is before UNIX epoch, using fallback timestamp");
                    0
                }),
            row_count: state.len(),
            state,
        })
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let temp_path = PathBuf::from(format!("{}.tmp", path.as_ref().display()));

        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);
            let data = bincode::serialize(&self)?;
            let compressed = zstd::encode_all(&data[..], 3)?;
            std::io::Write::write_all(&mut writer, &compressed)?;
        }

        fs::rename(temp_path, path)?;
        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let compressed =
            std::io::Read::bytes(reader).collect::<std::result::Result<Vec<_>, _>>()?;
        let data = zstd::decode_all(&compressed[..])?;
        Ok(bincode::deserialize(&data)?)
    }
}

pub struct SnapshotManager {
    snapshots_dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(table_path: &Path) -> Self {
        Self {
            snapshots_dir: table_path.join("snapshots"),
        }
    }

    pub fn create_snapshot(&self, storage: &TableStorage, sequence: u64) -> Result<()> {
        let snapshot = Snapshot::create_from_storage(storage, sequence)?;
        let filename = format!("{:010}.snap", sequence);
        let path = self.snapshots_dir.join(filename);
        snapshot.save_to_file(path)?;
        Ok(())
    }

    pub fn find_latest_before(&self, sequence: u64) -> Result<Option<Snapshot>> {
        let mut best_snapshot = None;
        let mut best_sequence = 0;

        if !self.snapshots_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&self.snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(snap_seq) = name.parse::<u64>() {
                    if snap_seq <= sequence && snap_seq > best_sequence {
                        best_sequence = snap_seq;
                        best_snapshot = Some(path);
                    }
                }
            }
        }

        if let Some(path) = best_snapshot {
            Ok(Some(Snapshot::load_from_file(path)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_snapshots(&self) -> Result<Vec<u64>> {
        let mut sequences = Vec::new();

        if !self.snapshots_dir.exists() {
            return Ok(sequences);
        }

        for entry in fs::read_dir(&self.snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(seq) = name.parse::<u64>() {
                    sequences.push(seq);
                }
            }
        }

        sequences.sort();
        Ok(sequences)
    }
}
