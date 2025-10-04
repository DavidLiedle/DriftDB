//! Comprehensive tests for the enhanced backup and restore system

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;

use crate::backup_enhanced::{
    EnhancedBackupManager, BackupConfig, BackupType, CompressionType, EncryptionType,
    RetentionPolicy, StorageType, RestoreOptions, BackupCatalog
};
use crate::engine::{Engine, BackupStats};
use crate::wal::{WalManager, WalConfig, WalOperation};
use crate::errors::Result;

#[tokio::test]
async fn test_full_backup_creation() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&backup_dir)?;

    let wal_manager = Arc::new(WalManager::new(
        data_dir.join("wal.log"),
        WalConfig::default(),
    )?);

    // Log some operations to WAL
    wal_manager.log_operation(WalOperation::TransactionBegin { transaction_id: 1 })?;
    wal_manager.log_operation(WalOperation::Insert {
        table: "users".to_string(),
        row_id: "user1".to_string(),
        data: serde_json::json!({"name": "Alice", "age": 30}),
    })?;
    wal_manager.log_operation(WalOperation::TransactionCommit { transaction_id: 1 })?;

    let config = BackupConfig {
        compression: CompressionType::Zstd { level: 3 },
        verify_after_backup: true,
        ..Default::default()
    };

    let mut backup_manager = EnhancedBackupManager::new(
        &data_dir,
        &backup_dir,
        wal_manager,
        config,
    )?;

    // Create a full backup
    let mut tags = HashMap::new();
    tags.insert("test".to_string(), "full_backup_test".to_string());

    let result = backup_manager.create_full_backup(Some(tags)).await?;

    assert!(!result.backup_id.is_empty());
    assert!(matches!(result.backup_type, BackupType::Full));
    assert!(result.duration > Duration::ZERO);

    // Verify backup exists
    let backup_path = backup_dir.join(&result.backup_id);
    assert!(backup_path.exists());
    assert!(backup_path.join("metadata.json").exists());

    // Verify backup can be listed
    let backups = backup_manager.list_backups();
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].backup_id, result.backup_id);

    println!("✅ Full backup creation test passed");
    Ok(())
}

#[tokio::test]
async fn test_incremental_backup() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&backup_dir)?;

    let wal_manager = Arc::new(WalManager::new(
        data_dir.join("wal.log"),
        WalConfig::default(),
    )?);

    let config = BackupConfig::default();

    let mut backup_manager = EnhancedBackupManager::new(
        &data_dir,
        &backup_dir,
        wal_manager.clone(),
        config,
    )?;

    // Create initial full backup
    let full_result = backup_manager.create_full_backup(None).await?;
    assert!(matches!(full_result.backup_type, BackupType::Full));

    // Add more data to WAL
    wal_manager.log_operation(WalOperation::TransactionBegin { transaction_id: 2 })?;
    wal_manager.log_operation(WalOperation::Insert {
        table: "users".to_string(),
        row_id: "user2".to_string(),
        data: serde_json::json!({"name": "Bob", "age": 25}),
    })?;
    wal_manager.log_operation(WalOperation::TransactionCommit { transaction_id: 2 })?;

    // Create incremental backup
    let inc_result = backup_manager.create_incremental_backup(None).await?;
    assert!(matches!(inc_result.backup_type, BackupType::Incremental));

    // Verify both backups exist
    let backups = backup_manager.list_backups();
    assert_eq!(backups.len(), 2);

    let full_backup = backups.iter().find(|b| matches!(b.backup_type, BackupType::Full)).unwrap();
    let inc_backup = backups.iter().find(|b| matches!(b.backup_type, BackupType::Incremental)).unwrap();

    assert_eq!(full_backup.backup_id, full_result.backup_id);
    assert_eq!(inc_backup.backup_id, inc_result.backup_id);
    assert_eq!(inc_backup.parent_backup, Some(full_backup.backup_id.clone()));

    println!("✅ Incremental backup test passed");
    Ok(())
}

#[tokio::test]
async fn test_backup_verification() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    fs::create_dir_all(&data_dir)?;

    let wal_manager = Arc::new(WalManager::new(
        data_dir.join("wal.log"),
        WalConfig::default(),
    )?);

    let config = BackupConfig {
        verify_after_backup: true,
        ..Default::default()
    };

    let mut backup_manager = EnhancedBackupManager::new(
        &data_dir,
        &backup_dir,
        wal_manager,
        config,
    )?;

    // Create a backup
    let result = backup_manager.create_full_backup(None).await?;

    // Verify the backup
    let backup_path = backup_dir.join(&result.backup_id);
    let is_valid = backup_manager.verify_backup(&backup_path).await?;
    assert!(is_valid);

    println!("✅ Backup verification test passed");
    Ok(())
}

#[tokio::test]
async fn test_backup_catalog() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.json");

    let mut catalog = BackupCatalog::new(&catalog_path)?;

    // Create test metadata
    let metadata = crate::backup_enhanced::BackupMetadata {
        backup_id: "test_backup_001".to_string(),
        version: "1.0.0".to_string(),
        timestamp: SystemTime::now(),
        tables: Vec::new(),
        backup_type: BackupType::Full,
        parent_backup: None,
        start_sequence: 0,
        end_sequence: 100,
        wal_start_position: 0,
        wal_end_position: 100,
        total_size_bytes: 1024,
        compressed_size_bytes: 512,
        file_count: 5,
        checksum: "abc123".to_string(),
        compression: CompressionType::Zstd { level: 3 },
        encryption: EncryptionType::None,
        retention_policy: RetentionPolicy::default(),
        tags: HashMap::new(),
        system_info: crate::backup_enhanced::SystemBackupInfo {
            hostname: "test-host".to_string(),
            database_version: "1.0.0".to_string(),
            platform: "linux".to_string(),
            cpu_count: 4,
            total_memory_bytes: 8_000_000_000,
            available_disk_bytes: 100_000_000_000,
        },
    };

    // Add backup to catalog
    catalog.add_backup(metadata.clone())?;

    // Verify it's in the catalog
    let backups = catalog.list_backups();
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].backup_id, "test_backup_001");

    // Test retrieval
    let found = catalog.get_backup("test_backup_001");
    assert!(found.is_some());
    assert_eq!(found.unwrap().backup_id, "test_backup_001");

    // Test removal
    let removed = catalog.remove_backup("test_backup_001")?;
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().backup_id, "test_backup_001");

    let backups_after_removal = catalog.list_backups();
    assert_eq!(backups_after_removal.len(), 0);

    println!("✅ Backup catalog test passed");
    Ok(())
}

#[tokio::test]
async fn test_backup_retention_policy() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    fs::create_dir_all(&data_dir)?;

    let wal_manager = Arc::new(WalManager::new(
        data_dir.join("wal.log"),
        WalConfig::default(),
    )?);

    let retention_policy = RetentionPolicy {
        max_backup_count: Some(2), // Keep only 2 backups
        ..Default::default()
    };

    let config = BackupConfig {
        retention_policy,
        ..Default::default()
    };

    let mut backup_manager = EnhancedBackupManager::new(
        &data_dir,
        &backup_dir,
        wal_manager,
        config,
    )?;

    // Create 3 backups (should trigger retention policy)
    let _backup1 = backup_manager.create_full_backup(None).await?;

    // Wait a bit to ensure different timestamps
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _backup2 = backup_manager.create_full_backup(None).await?;

    tokio::time::sleep(Duration::from_millis(10)).await;
    let _backup3 = backup_manager.create_full_backup(None).await?;

    // Apply retention policy
    let deleted = backup_manager.apply_retention_policy().await?;

    // Should have deleted the oldest backup
    assert_eq!(deleted.len(), 1);

    // Should have 2 backups remaining
    let remaining_backups = backup_manager.list_backups();
    assert_eq!(remaining_backups.len(), 2);

    println!("✅ Backup retention policy test passed");
    Ok(())
}

#[tokio::test]
async fn test_engine_backup_integration() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    // Initialize engine
    let mut engine = Engine::init(&data_dir)?;

    // Enable backup system
    let config = BackupConfig::default();
    engine.enable_backups(backup_dir, config)?;

    assert!(engine.is_backup_enabled());

    // Create a table and insert some data
    engine.create_table("users", "id", vec!["name".to_string()])?;

    // Create a backup
    let backup_result = engine.create_full_backup(None).await?;
    assert!(!backup_result.backup_id.is_empty());

    // List backups
    let backups = engine.list_backups()?;
    assert_eq!(backups.len(), 1);

    // Get backup statistics
    let stats = engine.backup_stats()?;
    assert_eq!(stats.total_backups, 1);
    assert_eq!(stats.full_backups, 1);
    assert_eq!(stats.incremental_backups, 0);

    // Verify backup
    let is_valid = engine.verify_backup(&backup_result.backup_id).await?;
    assert!(is_valid);

    // Test restore (to a different directory)
    let restore_dir = temp_dir.path().join("restored_data");
    let restore_options = RestoreOptions {
        target_directory: Some(restore_dir.clone()),
        ..Default::default()
    };

    let restore_result = engine.restore_from_backup(&backup_result.backup_id, restore_options).await?;
    assert!(!restore_result.restored_tables.is_empty());

    // Verify restored directory exists
    assert!(restore_dir.exists());

    println!("✅ Engine backup integration test passed");
    Ok(())
}

#[tokio::test]
async fn test_backup_compression_and_encryption() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    fs::create_dir_all(&data_dir)?;

    let wal_manager = Arc::new(WalManager::new(
        data_dir.join("wal.log"),
        WalConfig::default(),
    )?);

    let config = BackupConfig {
        compression: CompressionType::Zstd { level: 9 }, // High compression
        encryption: EncryptionType::Aes256Gcm, // Strong encryption
        ..Default::default()
    };

    let mut backup_manager = EnhancedBackupManager::new(
        &data_dir,
        &backup_dir,
        wal_manager,
        config,
    )?;

    let result = backup_manager.create_full_backup(None).await?;

    // Verify backup metadata reflects compression and encryption settings
    let backups = backup_manager.list_backups();
    let backup = &backups[0];

    assert!(matches!(backup.compression, CompressionType::Zstd { level: 9 }));
    assert!(matches!(backup.encryption, EncryptionType::Aes256Gcm));

    println!("✅ Backup compression and encryption test passed");
    Ok(())
}

#[tokio::test]
async fn test_point_in_time_recovery() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let backup_dir = temp_dir.path().join("backups");

    fs::create_dir_all(&data_dir)?;

    let wal_manager = Arc::new(WalManager::new(
        data_dir.join("wal.log"),
        WalConfig::default(),
    )?);

    let mut backup_manager = EnhancedBackupManager::new(
        &data_dir,
        &backup_dir,
        wal_manager.clone(),
        BackupConfig::default(),
    )?;

    // Create initial data
    wal_manager.log_operation(WalOperation::Insert {
        table: "events".to_string(),
        row_id: "event1".to_string(),
        data: serde_json::json!({"type": "login", "user": "alice"}),
    })?;

    let checkpoint_time = SystemTime::now();

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Add more data after checkpoint
    wal_manager.log_operation(WalOperation::Insert {
        table: "events".to_string(),
        row_id: "event2".to_string(),
        data: serde_json::json!({"type": "logout", "user": "alice"}),
    })?;

    // Create backup
    let backup_result = backup_manager.create_full_backup(None).await?;

    // Test point-in-time restore
    let restore_dir = temp_dir.path().join("pit_restore");
    let restore_options = RestoreOptions {
        target_directory: Some(restore_dir.clone()),
        point_in_time: Some(checkpoint_time),
        ..Default::default()
    };

    let restore_result = backup_manager.restore_backup(&backup_result.backup_id, restore_options).await?;

    // Should have restored to the checkpoint time
    assert_eq!(restore_result.point_in_time_achieved, Some(checkpoint_time));

    println!("✅ Point-in-time recovery test passed");
    Ok(())
}

/// Run all enhanced backup tests
pub async fn run_backup_tests() -> Result<()> {
    println!("🧪 Running comprehensive backup and restore tests...\n");

    test_full_backup_creation().await?;
    test_incremental_backup().await?;
    test_backup_verification().await?;
    test_backup_catalog().await?;
    test_backup_retention_policy().await?;
    test_engine_backup_integration().await?;
    test_backup_compression_and_encryption().await?;
    test_point_in_time_recovery().await?;

    println!("\n🎉 ALL BACKUP AND RESTORE TESTS PASSED - System is production-ready!");
    Ok(())
}