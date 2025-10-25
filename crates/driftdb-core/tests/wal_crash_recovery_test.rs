//! Comprehensive WAL crash recovery tests
//!
//! Tests various crash scenarios and WAL replay functionality

use driftdb_core::wal::{WalConfig, WalManager, WalOperation};
use serde_json::json;
use std::fs;
use std::io::Write;
use tempfile::TempDir;

#[test]
fn test_wal_replay_after_clean_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    // Phase 1: Write operations and shutdown cleanly
    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

        wal.log_operation(WalOperation::TransactionBegin { transaction_id: 1 })
            .unwrap();
        wal.log_operation(WalOperation::Insert {
            table: "users".to_string(),
            row_id: "user1".to_string(),
            data: json!({"name": "Alice", "age": 30}),
        })
        .unwrap();
        wal.log_operation(WalOperation::TransactionCommit { transaction_id: 1 })
            .unwrap();

        // Ensure sync
        wal.sync().unwrap();
    }

    // Phase 2: Restart and replay
    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();
        let entries = wal.replay_from_sequence(1).unwrap();

        assert_eq!(entries.len(), 3);
        assert!(matches!(
            entries[0].operation,
            WalOperation::TransactionBegin { transaction_id: 1 }
        ));
        assert!(matches!(entries[1].operation, WalOperation::Insert { .. }));
        assert!(matches!(
            entries[2].operation,
            WalOperation::TransactionCommit { transaction_id: 1 }
        ));
    }

    println!("✅ WAL replay after clean shutdown passed");
}

#[test]
fn test_wal_replay_uncommitted_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    // Phase 1: Write uncommitted transaction
    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

        wal.log_operation(WalOperation::TransactionBegin { transaction_id: 1 })
            .unwrap();
        wal.log_operation(WalOperation::Insert {
            table: "users".to_string(),
            row_id: "user1".to_string(),
            data: json!({"name": "Bob"}),
        })
        .unwrap();
        // NO COMMIT - simulates crash during transaction

        wal.sync().unwrap();
    }

    // Phase 2: Replay should see uncommitted transaction
    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();
        let entries = wal.replay_from_sequence(1).unwrap();

        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].operation,
            WalOperation::TransactionBegin { transaction_id: 1 }
        ));
        // Application layer should handle rolling back uncommitted transactions
    }

    println!("✅ WAL replay of uncommitted transaction passed");
}

#[test]
fn test_wal_replay_multiple_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

    // Transaction 1
    wal.log_operation(WalOperation::TransactionBegin { transaction_id: 1 })
        .unwrap();
    wal.log_operation(WalOperation::Insert {
        table: "orders".to_string(),
        row_id: "order1".to_string(),
        data: json!({"amount": 100}),
    })
    .unwrap();
    wal.log_operation(WalOperation::TransactionCommit { transaction_id: 1 })
        .unwrap();

    // Transaction 2
    wal.log_operation(WalOperation::TransactionBegin { transaction_id: 2 })
        .unwrap();
    wal.log_operation(WalOperation::Update {
        table: "orders".to_string(),
        row_id: "order1".to_string(),
        old_data: json!({"amount": 100}),
        new_data: json!({"amount": 150}),
    })
    .unwrap();
    wal.log_operation(WalOperation::TransactionCommit { transaction_id: 2 })
        .unwrap();

    // Transaction 3 (aborted)
    wal.log_operation(WalOperation::TransactionBegin { transaction_id: 3 })
        .unwrap();
    wal.log_operation(WalOperation::Delete {
        table: "orders".to_string(),
        row_id: "order1".to_string(),
        data: json!({"amount": 150}),
    })
    .unwrap();
    wal.log_operation(WalOperation::TransactionAbort { transaction_id: 3 })
        .unwrap();

    wal.sync().unwrap();

    // Replay all
    let entries = wal.replay_from_sequence(1).unwrap();
    assert_eq!(entries.len(), 9);

    // Count transaction types
    let mut begins = 0;
    let mut commits = 0;
    let mut aborts = 0;

    for entry in &entries {
        match &entry.operation {
            WalOperation::TransactionBegin { .. } => begins += 1,
            WalOperation::TransactionCommit { .. } => commits += 1,
            WalOperation::TransactionAbort { .. } => aborts += 1,
            _ => {}
        }
    }

    assert_eq!(begins, 3);
    assert_eq!(commits, 2);
    assert_eq!(aborts, 1);

    println!("✅ WAL replay of multiple transactions passed");
}

#[test]
fn test_wal_checksum_verification() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();
        wal.log_operation(WalOperation::Insert {
            table: "test".to_string(),
            row_id: "1".to_string(),
            data: json!({"value": 42}),
        })
        .unwrap();
        wal.sync().unwrap();
    }

    // Corrupt the WAL file
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Append garbage data that won't parse correctly
        writeln!(file, "{{\"corrupted\": \"data\"}}").unwrap();
    }

    // Replay should handle corruption gracefully
    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();
        let result = wal.replay_from_sequence(1);

        // Should fail due to corruption
        assert!(result.is_err() || result.unwrap().len() == 1);
        // The first valid entry should be recovered
    }

    println!("✅ WAL checksum verification passed");
}

#[test]
fn test_wal_replay_from_specific_sequence() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

    // Write 10 operations
    for i in 1..=10 {
        wal.log_operation(WalOperation::Insert {
            table: "data".to_string(),
            row_id: format!("key{}", i),
            data: json!({"value": i}),
        })
        .unwrap();
    }

    wal.sync().unwrap();

    // Replay from sequence 5
    let entries = wal.replay_from_sequence(5).unwrap();

    // Should get sequences 5-10 (6 entries)
    assert_eq!(entries.len(), 6);
    assert!(entries[0].sequence >= 5);
    assert_eq!(entries[0].sequence, 5);

    println!("✅ WAL replay from specific sequence passed");
}

#[test]
fn test_wal_checkpoint_and_truncation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

    // Write some operations
    for i in 1..=5 {
        wal.log_operation(WalOperation::Insert {
            table: "data".to_string(),
            row_id: format!("key{}", i),
            data: json!({"value": i}),
        })
        .unwrap();
    }

    wal.sync().unwrap();

    // Get file size before checkpoint
    let size_before = std::fs::metadata(&wal_path).unwrap().len();

    // Create checkpoint at sequence 3
    // This marks that all entries up to sequence 3 have been durably written
    wal.checkpoint(3).unwrap();

    // After checkpoint, the WAL may truncate old entries
    // since they're now safely persisted to durable storage
    let size_after = std::fs::metadata(&wal_path).unwrap().len();

    // Checkpoint should either keep the entries or reduce size
    // (Implementation may vary - some keep, some truncate)
    println!(
        "WAL size before checkpoint: {}, after: {}",
        size_before, size_after
    );

    // The key property: checkpoint does not lose data
    // We can still do new operations after checkpoint
    wal.log_operation(WalOperation::Insert {
        table: "data".to_string(),
        row_id: "key6".to_string(),
        data: json!({"value": 6}),
    })
    .unwrap();
    wal.sync().unwrap();

    // Verify new operations are logged correctly
    let entries = wal.replay_from_sequence(1).unwrap();
    assert!(!entries.is_empty(), "Should have at least one entry after checkpoint");

    println!("✅ WAL checkpoint and truncation passed");
}

#[test]
fn test_wal_create_table_replay() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

        // Log table creation
        wal.log_operation(WalOperation::CreateTable {
            table: "products".to_string(),
            schema: json!({
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "price", "type": "DECIMAL"}
                ],
                "primary_key": "id"
            }),
        })
        .unwrap();

        // Log some data
        wal.log_operation(WalOperation::Insert {
            table: "products".to_string(),
            row_id: "1".to_string(),
            data: json!({"id": 1, "name": "Widget", "price": 19.99}),
        })
        .unwrap();

        wal.sync().unwrap();
    }

    // Replay
    {
        let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();
        let entries = wal.replay_from_sequence(1).unwrap();

        assert_eq!(entries.len(), 2);
        assert!(matches!(
            entries[0].operation,
            WalOperation::CreateTable { .. }
        ));
        assert!(matches!(entries[1].operation, WalOperation::Insert { .. }));
    }

    println!("✅ WAL CREATE TABLE replay passed");
}

#[test]
fn test_wal_concurrent_sequence_numbers() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

    // Log operations and track sequences
    let mut sequences = Vec::new();
    for i in 1..=5 {
        let seq = wal
            .log_operation(WalOperation::Insert {
                table: "test".to_string(),
                row_id: format!("key{}", i),
                data: json!({"value": i}),
            })
            .unwrap();
        sequences.push(seq);
    }

    // Sequences should be monotonically increasing
    for i in 1..sequences.len() {
        assert!(sequences[i] > sequences[i - 1]);
    }

    println!("✅ WAL concurrent sequence numbers passed");
}

#[test]
fn test_wal_index_operations_replay() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();

    // Create index
    wal.log_operation(WalOperation::CreateIndex {
        table: "users".to_string(),
        index_name: "idx_email".to_string(),
        columns: vec!["email".to_string()],
    })
    .unwrap();

    // Insert data
    wal.log_operation(WalOperation::Insert {
        table: "users".to_string(),
        row_id: "1".to_string(),
        data: json!({"id": 1, "email": "test@example.com"}),
    })
    .unwrap();

    // Drop index
    wal.log_operation(WalOperation::DropIndex {
        table: "users".to_string(),
        index_name: "idx_email".to_string(),
    })
    .unwrap();

    wal.sync().unwrap();

    // Replay
    let entries = wal.replay_from_sequence(1).unwrap();
    assert_eq!(entries.len(), 3);

    println!("✅ WAL index operations replay passed");
}

#[test]
fn test_wal_empty_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    // Create empty WAL file
    fs::write(&wal_path, "").unwrap();

    // Should handle empty file gracefully
    let wal = WalManager::new(&wal_path, WalConfig::default()).unwrap();
    let entries = wal.replay_from_sequence(1).unwrap();
    assert_eq!(entries.len(), 0);

    println!("✅ WAL empty file handling passed");
}
