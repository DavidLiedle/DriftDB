//! Write-Ahead Log (WAL) implementation for crash recovery
//!
//! The WAL ensures durability by logging all operations before they are applied to the main storage.
//! In case of a crash, the WAL can be replayed to recover uncommitted transactions.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::errors::{DriftError, Result};
use crate::events::Event;

/// WAL operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    /// Begin a new transaction
    Begin { txn_id: u64, timestamp_ms: u64 },
    /// Write an event
    Write { txn_id: u64, event: Event },
    /// Commit a transaction
    Commit { txn_id: u64 },
    /// Rollback a transaction
    Rollback { txn_id: u64 },
    /// Checkpoint - marks that all prior operations have been persisted
    Checkpoint { sequence: u64, timestamp_ms: u64 },
}

/// WAL entry with CRC for integrity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub operation: WalOperation,
    pub crc32: u32,
}

impl WalEntry {
    fn new(sequence: u64, operation: WalOperation) -> Self {
        let mut entry = Self {
            sequence,
            operation,
            crc32: 0,
        };
        entry.crc32 = entry.calculate_crc();
        entry
    }

    fn calculate_crc(&self) -> u32 {
        let data = rmp_serde::to_vec(&(&self.sequence, &self.operation))
            .unwrap_or_default();
        crc32fast::hash(&data)
    }

    fn verify(&self) -> bool {
        self.crc32 == self.calculate_crc()
    }
}

/// Write-Ahead Log manager
pub struct Wal {
    path: PathBuf,
    writer: Arc<Mutex<WalWriter>>,
    next_sequence: Arc<AtomicU64>,
    next_txn_id: Arc<AtomicU64>,
}

impl Wal {
    /// Create a new WAL in the specified directory
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let wal_dir = base_path.as_ref().join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let wal_path = wal_dir.join("wal.log");
        let writer = WalWriter::new(&wal_path)?;

        // Recover sequence number from existing WAL if present
        let next_sequence = Self::recover_sequence(&wal_path)?;

        Ok(Self {
            path: wal_path,
            writer: Arc::new(Mutex::new(writer)),
            next_sequence: Arc::new(AtomicU64::new(next_sequence)),
            next_txn_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<u64> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let operation = WalOperation::Begin { txn_id, timestamp_ms };
        self.write_operation(operation)?;
        Ok(txn_id)
    }

    /// Write an event to the WAL
    pub fn write_event(&self, txn_id: u64, event: Event) -> Result<u64> {
        let operation = WalOperation::Write { txn_id, event };
        self.write_operation(operation)
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, txn_id: u64) -> Result<()> {
        let operation = WalOperation::Commit { txn_id };
        self.write_operation(operation)?;
        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback_transaction(&self, txn_id: u64) -> Result<()> {
        let operation = WalOperation::Rollback { txn_id };
        self.write_operation(operation)?;
        Ok(())
    }

    /// Create a checkpoint
    pub fn checkpoint(&self, sequence: u64) -> Result<()> {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let operation = WalOperation::Checkpoint { sequence, timestamp_ms };
        self.write_operation(operation)?;

        // Optionally rotate the WAL file after checkpoint
        if self.should_rotate()? {
            self.rotate()?;
        }

        Ok(())
    }

    /// Write an operation to the WAL
    fn write_operation(&self, operation: WalOperation) -> Result<u64> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry::new(sequence, operation);

        let mut writer = self.writer.lock();
        writer.write_entry(&entry)?;
        writer.sync()?;

        Ok(sequence)
    }

    /// Check if WAL should be rotated
    fn should_rotate(&self) -> Result<bool> {
        let metadata = std::fs::metadata(&self.path)?;
        // Rotate if WAL is larger than 100MB
        Ok(metadata.len() > 100 * 1024 * 1024)
    }

    /// Rotate the WAL file
    fn rotate(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.rotate()?;
        Ok(())
    }

    /// Recover the next sequence number from existing WAL
    fn recover_sequence(path: &Path) -> Result<u64> {
        if !path.exists() {
            return Ok(1);
        }

        let mut max_sequence = 0u64;
        let reader = WalReader::new(path)?;

        for entry in reader {
            match entry {
                Ok(e) => max_sequence = max_sequence.max(e.sequence),
                Err(_) => break, // Stop on first corrupted entry
            }
        }

        Ok(max_sequence + 1)
    }

    /// Replay the WAL from a given checkpoint
    pub fn replay_from<F>(&self, checkpoint_seq: Option<u64>, mut callback: F) -> Result<()>
    where
        F: FnMut(WalOperation) -> Result<()>,
    {
        let reader = WalReader::new(&self.path)?;

        for entry_result in reader {
            let entry = entry_result?;

            // Skip entries before checkpoint
            if let Some(checkpoint) = checkpoint_seq {
                if entry.sequence <= checkpoint {
                    continue;
                }
            }

            // Verify integrity
            if !entry.verify() {
                return Err(DriftError::CorruptSegment(
                    format!("Corrupted WAL entry at sequence {}", entry.sequence)
                ));
            }

            callback(entry.operation)?;
        }

        Ok(())
    }

    /// Truncate the WAL at a specific sequence (for recovery)
    pub fn truncate_at(&self, sequence: u64) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.truncate_at_sequence(sequence)?;
        self.next_sequence.store(sequence + 1, Ordering::SeqCst);
        Ok(())
    }
}

/// WAL writer with buffering and sync
struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
}

impl WalWriter {
    fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            file: BufWriter::new(file),
            path: path.to_path_buf(),
        })
    }

    fn write_entry(&mut self, entry: &WalEntry) -> Result<()> {
        let data = rmp_serde::to_vec(entry)?;
        let len = data.len() as u32;

        // Write length prefix
        self.file.write_all(&len.to_le_bytes())?;
        // Write entry data
        self.file.write_all(&data)?;

        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        // Archive old WAL
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let archive_path = self.path.with_extension(format!("log.{}", timestamp));
        std::fs::rename(&self.path, archive_path)?;

        // Create new WAL file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.file = BufWriter::new(file);
        Ok(())
    }

    fn truncate_at_sequence(&mut self, target_sequence: u64) -> Result<()> {
        // This requires reading the WAL to find the position
        // For now, we'll implement a simple version that truncates the entire file
        // In production, we'd want to find the exact byte position

        self.file.flush()?;
        let file = self.file.get_mut();
        file.seek(SeekFrom::Start(0))?;

        // Read through entries to find truncation point
        let mut reader = BufReader::new(file.try_clone()?);
        let mut position = 0u64;

        loop {
            let mut len_bytes = [0u8; 4];
            if reader.read_exact(&mut len_bytes).is_err() {
                break;
            }

            let len = u32::from_le_bytes(len_bytes) as usize;
            let mut data = vec![0u8; len];

            if reader.read_exact(&mut data).is_err() {
                break;
            }

            if let Ok(entry) = rmp_serde::from_slice::<WalEntry>(&data) {
                if entry.sequence >= target_sequence {
                    break;
                }
                position = reader.stream_position()?;
            }
        }

        file.set_len(position)?;
        file.seek(SeekFrom::End(0))?;

        Ok(())
    }
}

/// WAL reader for replay
struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for WalReader {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        if self.reader.read_exact(&mut len_bytes).is_err() {
            return None;
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        if len == 0 || len > 10 * 1024 * 1024 { // Sanity check: max 10MB per entry
            return Some(Err(DriftError::CorruptSegment(
                "Invalid WAL entry length".into()
            )));
        }

        // Read entry data
        let mut data = vec![0u8; len];
        if let Err(e) = self.reader.read_exact(&mut data) {
            return Some(Err(e.into()));
        }

        // Deserialize entry
        match rmp_serde::from_slice::<WalEntry>(&data) {
            Ok(entry) => Some(Ok(entry)),
            Err(e) => Some(Err(DriftError::Deserialization(e.to_string()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::new(temp_dir.path()).unwrap();

        // Begin transaction
        let txn_id = wal.begin_transaction().unwrap();
        assert!(txn_id > 0);

        // Write event
        let event = Event::new_insert(
            "test_table".into(),
            serde_json::json!("key1"),
            serde_json::json!({"data": "value1"}),
        );
        let seq = wal.write_event(txn_id, event).unwrap();
        assert!(seq > 0);

        // Commit transaction
        wal.commit_transaction(txn_id).unwrap();

        // Create checkpoint
        wal.checkpoint(seq).unwrap();
    }

    #[test]
    fn test_wal_replay() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::new(temp_dir.path()).unwrap();

        // Write some operations
        let txn1 = wal.begin_transaction().unwrap();
        let event1 = Event::new_insert(
            "test".into(),
            serde_json::json!("k1"),
            serde_json::json!({"v": 1}),
        );
        wal.write_event(txn1, event1.clone()).unwrap();
        wal.commit_transaction(txn1).unwrap();

        // Replay and verify
        let mut replayed = Vec::new();
        wal.replay_from(None, |op| {
            replayed.push(op);
            Ok(())
        }).unwrap();

        assert_eq!(replayed.len(), 3); // Begin, Write, Commit
    }
}