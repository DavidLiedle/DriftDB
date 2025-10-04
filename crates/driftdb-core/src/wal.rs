//! Write-Ahead Logging (WAL) for DriftDB
//!
//! Provides durability guarantees by writing all changes to a WAL before
//! applying them to the main database. Critical for crash recovery.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::errors::{DriftError, Result};
// use crate::events::Event;

/// WAL entry representing a single logged operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Unique sequence number for this entry
    pub sequence: u64,
    /// Transaction ID (if part of a transaction)
    pub transaction_id: Option<u64>,
    /// The actual operation being logged
    pub operation: WalOperation,
    /// Timestamp when this entry was created
    pub timestamp: u64,
    /// CRC32 checksum for integrity verification
    pub checksum: u32,
}

/// Types of operations that can be logged to WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    /// Transaction begin
    TransactionBegin { transaction_id: u64 },
    /// Transaction commit
    TransactionCommit { transaction_id: u64 },
    /// Transaction abort/rollback
    TransactionAbort { transaction_id: u64 },
    /// Insert operation
    Insert {
        table: String,
        row_id: String,
        data: Value,
    },
    /// Update operation
    Update {
        table: String,
        row_id: String,
        old_data: Value,
        new_data: Value,
    },
    /// Delete operation
    Delete {
        table: String,
        row_id: String,
        data: Value,
    },
    /// Create table
    CreateTable { table: String, schema: Value },
    /// Drop table
    DropTable { table: String },
    /// Create index
    CreateIndex {
        table: String,
        index_name: String,
        columns: Vec<String>,
    },
    /// Drop index
    DropIndex { table: String, index_name: String },
    /// Checkpoint marker
    Checkpoint { sequence: u64 },
}

/// Write-Ahead Log manager
pub struct WalManager {
    /// Path to the WAL file
    wal_path: PathBuf,
    /// Current WAL file writer
    writer: Arc<Mutex<Option<BufWriter<File>>>>,
    /// Current sequence number
    sequence: Arc<Mutex<u64>>,
    /// WAL configuration
    config: WalConfig,
}

/// WAL configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum size of WAL file before rotation (bytes)
    pub max_file_size: u64,
    /// Force sync to disk on every write
    pub sync_on_write: bool,
    /// Checksum verification on read
    pub verify_checksums: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 100 * 1024 * 1024, // 100MB
            sync_on_write: true,              // Critical for durability
            verify_checksums: true,
        }
    }
}

impl WalManager {
    /// Create a new WAL manager
    pub fn new<P: AsRef<Path>>(wal_path: P, config: WalConfig) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();

        // Ensure WAL directory exists
        if let Some(parent) = wal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let manager = Self {
            wal_path,
            writer: Arc::new(Mutex::new(None)),
            sequence: Arc::new(Mutex::new(0)),
            config,
        };

        // Initialize WAL file
        manager.init_wal()?;

        Ok(manager)
    }

    /// Initialize WAL file and recover sequence number
    fn init_wal(&self) -> Result<()> {
        // If WAL file exists, read it to find the latest sequence number
        if self.wal_path.exists() {
            let last_sequence = self.find_last_sequence()?;
            *self.sequence.lock().unwrap() = last_sequence + 1;
        }

        // Open WAL file for writing
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;

        let writer = BufWriter::new(file);
        *self.writer.lock().unwrap() = Some(writer);

        Ok(())
    }

    /// Find the last sequence number in the WAL
    fn find_last_sequence(&self) -> Result<u64> {
        let file = File::open(&self.wal_path)?;
        let reader = BufReader::new(file);

        let mut last_sequence = 0;

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    if self.config.verify_checksums {
                        self.verify_entry_checksum(&entry)?;
                    }
                    last_sequence = entry.sequence;
                }
                Err(_) => {
                    // Corrupted entry - truncate WAL at this point
                    break;
                }
            }
        }

        Ok(last_sequence)
    }

    /// Write an operation to the WAL
    pub fn log_operation(&self, operation: WalOperation) -> Result<u64> {
        let sequence = {
            let mut seq = self.sequence.lock().unwrap();
            *seq += 1;
            *seq
        };

        let entry = WalEntry {
            sequence,
            transaction_id: None, // TODO: Get from current transaction context
            operation,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            checksum: 0, // Will be calculated below
        };

        let mut entry_with_checksum = entry;
        entry_with_checksum.checksum = self.calculate_checksum(&entry_with_checksum)?;

        // Serialize entry
        let serialized = serde_json::to_string(&entry_with_checksum)?;

        // Write to WAL
        {
            let mut writer_guard = self.writer.lock().unwrap();
            if let Some(ref mut writer) = *writer_guard {
                writeln!(writer, "{}", serialized)?;

                if self.config.sync_on_write {
                    writer.flush()?;
                    writer.get_ref().sync_all()?; // Force to disk
                }
            } else {
                return Err(DriftError::Internal(
                    "WAL writer not initialized".to_string(),
                ));
            }
        }

        Ok(sequence)
    }

    /// Calculate checksum for WAL entry
    fn calculate_checksum(&self, entry: &WalEntry) -> Result<u32> {
        // Create entry without checksum for calculation
        let entry_for_checksum = WalEntry {
            checksum: 0,
            ..entry.clone()
        };

        let serialized = serde_json::to_string(&entry_for_checksum)?;
        let mut hasher = Hasher::new();
        hasher.update(serialized.as_bytes());
        Ok(hasher.finalize())
    }

    /// Verify checksum of a WAL entry
    fn verify_entry_checksum(&self, entry: &WalEntry) -> Result<()> {
        let calculated = self.calculate_checksum(entry)?;
        if calculated != entry.checksum {
            return Err(DriftError::Corruption(format!(
                "WAL entry checksum mismatch: expected {}, got {}",
                entry.checksum, calculated
            )));
        }
        Ok(())
    }

    /// Replay WAL entries from a specific sequence number
    pub fn replay_from_sequence(&self, from_sequence: u64) -> Result<Vec<WalEntry>> {
        let file = File::open(&self.wal_path)?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => {
                    if self.config.verify_checksums {
                        self.verify_entry_checksum(&entry)?;
                    }

                    if entry.sequence >= from_sequence {
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    return Err(DriftError::Corruption(format!(
                        "Failed to parse WAL entry: {}",
                        e
                    )));
                }
            }
        }

        Ok(entries)
    }

    /// Create a checkpoint (truncate WAL up to this point)
    pub fn checkpoint(&self, up_to_sequence: u64) -> Result<()> {
        // Log the checkpoint operation first
        self.log_operation(WalOperation::Checkpoint {
            sequence: up_to_sequence,
        })?;

        // Read all entries after the checkpoint
        let entries_to_keep = self.replay_from_sequence(up_to_sequence + 1)?;

        // Rotate WAL file
        let backup_path = self.wal_path.with_extension("wal.old");
        std::fs::rename(&self.wal_path, backup_path)?;

        // Recreate WAL with only the entries after checkpoint
        self.init_wal()?;

        for entry in entries_to_keep {
            let serialized = serde_json::to_string(&entry)?;
            let mut writer_guard = self.writer.lock().unwrap();
            if let Some(ref mut writer) = *writer_guard {
                writeln!(writer, "{}", serialized)?;
            }
        }

        Ok(())
    }

    /// Force sync WAL to disk
    pub fn sync(&self) -> Result<()> {
        let mut writer_guard = self.writer.lock().unwrap();
        if let Some(ref mut writer) = *writer_guard {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> u64 {
        *self.sequence.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");

        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

        // Log some operations
        let seq1 = wal
            .log_operation(WalOperation::TransactionBegin { transaction_id: 1 })
            .unwrap();
        let seq2 = wal
            .log_operation(WalOperation::Insert {
                table: "users".to_string(),
                row_id: "1".to_string(),
                data: serde_json::json!({"name": "Alice", "age": 30}),
            })
            .unwrap();
        let seq3 = wal
            .log_operation(WalOperation::TransactionCommit { transaction_id: 1 })
            .unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);

        // Force sync
        wal.sync().unwrap();

        // Replay from beginning
        let entries = wal.replay_from_sequence(1).unwrap();
        assert_eq!(entries.len(), 3);

        // Verify operations
        match &entries[0].operation {
            WalOperation::TransactionBegin { transaction_id } => assert_eq!(*transaction_id, 1),
            _ => panic!("Expected TransactionBegin"),
        }

        match &entries[1].operation {
            WalOperation::Insert { table, row_id, .. } => {
                assert_eq!(table, "users");
                assert_eq!(row_id, "1");
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_wal_checksum_verification() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("test.wal");

        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

        // Log an operation
        wal.log_operation(WalOperation::Insert {
            table: "test".to_string(),
            row_id: "1".to_string(),
            data: serde_json::json!({"data": "test"}),
        })
        .unwrap();

        // Replay should succeed with valid checksums
        let entries = wal.replay_from_sequence(1).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
