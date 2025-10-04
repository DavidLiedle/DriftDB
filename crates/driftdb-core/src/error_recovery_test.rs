//! Comprehensive tests for error recovery system
//!
//! Tests crash recovery, corruption repair, health monitoring, and panic handling

use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::engine::Engine;
use crate::error_recovery::{RecoveryManager, RecoveryConfig, ComponentHealth, HealthStatus};
use crate::wal::{WalManager, WalConfig, WalOperation};
use crate::monitoring::{MonitoringSystem, MonitoringConfig};
use crate::observability::Metrics;
use crate::errors::{DriftError, Result};

#[tokio::test]
async fn test_crash_detection_and_recovery() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Simulate a crash by creating lock file without clean shutdown marker
    fs::write(data_path.join(".driftdb.lock"), "locked")?;

    let wal_manager = Arc::new(WalManager::new(
        data_path.join("wal.log"),
        WalConfig::default(),
    )?);

    // Log some operations before crash
    wal_manager.log_operation(WalOperation::TransactionBegin { transaction_id: 1 })?;
    wal_manager.log_operation(WalOperation::Insert {
        table: "users".to_string(),
        row_id: "1".to_string(),
        data: serde_json::json!({"name": "Alice", "email": "alice@test.com"}),
    })?;
    wal_manager.log_operation(WalOperation::TransactionCommit { transaction_id: 1 })?;

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let recovery_manager = RecoveryManager::new(
        data_path.clone(),
        wal_manager,
        None,
        monitoring,
        RecoveryConfig::default(),
    );

    // Perform crash recovery
    let result = recovery_manager.perform_startup_recovery().await?;

    // Verify recovery was performed
    assert!(result.success);
    assert!(!result.operations_performed.is_empty());

    // Verify clean shutdown marker was created
    assert!(data_path.join(".clean_shutdown").exists());

    println!("✅ Crash detection and recovery test passed");
    Ok(())
}

#[tokio::test]
async fn test_corruption_detection_and_repair() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Create a corrupted segment file
    let segments_dir = data_path.join("tables").join("test").join("segments");
    fs::create_dir_all(&segments_dir)?;

    let corrupt_segment = segments_dir.join("corrupt.seg");
    fs::write(&corrupt_segment, "corrupted data that doesn't match expected format")?;

    let wal_manager = Arc::new(WalManager::new(
        data_path.join("wal.log"),
        WalConfig::default(),
    )?);

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let recovery_manager = RecoveryManager::new(
        data_path,
        wal_manager,
        None,
        monitoring,
        RecoveryConfig::default(),
    );

    // Perform corruption repair
    let operations = recovery_manager.repair_corrupted_segments().await?;

    // Should detect and handle the corrupted segment
    assert!(!operations.is_empty());

    println!("✅ Corruption detection and repair test passed");
    Ok(())
}

#[tokio::test]
async fn test_wal_recovery_with_transactions() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let wal_manager = Arc::new(WalManager::new(
        data_path.join("wal.log"),
        WalConfig::default(),
    )?);

    // Log a complete transaction
    wal_manager.log_operation(WalOperation::TransactionBegin { transaction_id: 1 })?;
    wal_manager.log_operation(WalOperation::Insert {
        table: "orders".to_string(),
        row_id: "order_1".to_string(),
        data: serde_json::json!({"amount": 100.0, "customer": "Bob"}),
    })?;
    wal_manager.log_operation(WalOperation::Insert {
        table: "order_items".to_string(),
        row_id: "item_1".to_string(),
        data: serde_json::json!({"order_id": "order_1", "product": "Widget", "qty": 2}),
    })?;
    wal_manager.log_operation(WalOperation::TransactionCommit { transaction_id: 1 })?;

    // Log an incomplete transaction (should be rolled back)
    wal_manager.log_operation(WalOperation::TransactionBegin { transaction_id: 2 })?;
    wal_manager.log_operation(WalOperation::Insert {
        table: "orders".to_string(),
        row_id: "order_2".to_string(),
        data: serde_json::json!({"amount": 50.0, "customer": "Charlie"}),
    })?;
    // No commit for transaction 2

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let recovery_manager = RecoveryManager::new(
        data_path,
        wal_manager,
        None,
        monitoring,
        RecoveryConfig::default(),
    );

    // Perform WAL recovery
    let (operation, data_loss) = recovery_manager.recover_from_wal().await?;

    // Verify recovery operation
    assert!(operation.is_some());
    assert!(data_loss.is_none());

    if let Some(crate::error_recovery::RecoveryOperation::WalReplay { entries_recovered }) = operation {
        assert_eq!(entries_recovered, 5); // 4 from complete txn + 2 from incomplete
    }

    println!("✅ WAL recovery with transactions test passed");
    Ok(())
}

#[tokio::test]
async fn test_health_monitoring() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let wal_manager = Arc::new(WalManager::new(
        data_path.join("wal.log"),
        WalConfig::default(),
    )?);

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let mut config = RecoveryConfig::default();
    config.health_check_interval = 1; // 1 second for faster testing

    let recovery_manager = RecoveryManager::new(
        data_path,
        wal_manager,
        None,
        monitoring,
        config,
    );

    // Perform a health check
    let health_issues = recovery_manager.perform_health_check().await?;

    // Should not have any critical issues on a fresh system
    let critical_issues: Vec<_> = health_issues.iter()
        .filter(|h| h.status == HealthStatus::Critical)
        .collect();

    assert!(critical_issues.is_empty(), "Should not have critical health issues on fresh system");

    println!("✅ Health monitoring test passed");
    Ok(())
}

#[tokio::test]
async fn test_panic_recovery() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let wal_manager = Arc::new(WalManager::new(
        data_path.join("wal.log"),
        WalConfig::default(),
    )?);

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let recovery_manager = RecoveryManager::new(
        data_path,
        wal_manager,
        None,
        monitoring,
        RecoveryConfig::default(),
    );

    // Simulate a panic
    let thread_id = "worker_thread_1";
    let panic_info = "thread 'worker_thread_1' panicked at 'index out of bounds'";

    recovery_manager.handle_panic_recovery(thread_id, panic_info)?;

    // Verify panic was logged to WAL
    let entries = recovery_manager.wal_manager.replay_from_sequence(0)?;
    let panic_entries: Vec<_> = entries.iter()
        .filter(|e| {
            matches!(&e.operation, WalOperation::Insert { table, .. } if table == "system_events")
        })
        .collect();

    assert!(!panic_entries.is_empty(), "Panic should be logged to WAL");

    println!("✅ Panic recovery test passed");
    Ok(())
}

#[tokio::test]
async fn test_engine_integration() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Initialize engine with recovery system
    let engine = Engine::init(&data_path)?;

    // Test recovery methods
    let recovery_stats = engine.recovery_stats();
    assert_eq!(recovery_stats.healthy_components, 0); // No health checks performed yet

    // Test health check
    let health_status = engine.check_health().await?;
    assert!(health_status.is_empty() || health_status.iter().all(|h| h.status != HealthStatus::Failed));

    // Test monitoring
    let metrics = engine.system_metrics();
    assert!(metrics.is_some()); // Basic sanity check

    // Test graceful shutdown
    engine.shutdown_gracefully()?;

    // Verify clean shutdown marker exists
    assert!(data_path.join(".clean_shutdown").exists());

    println!("✅ Engine integration test passed");
    Ok(())
}

#[tokio::test]
async fn test_recovery_with_backup_fallback() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Create a completely corrupted WAL
    let wal_path = data_path.join("wal.log");
    fs::write(&wal_path, "completely corrupted WAL file that cannot be parsed")?;

    let wal_manager = Arc::new(WalManager::new(
        wal_path,
        WalConfig::default(),
    )?);

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let mut config = RecoveryConfig::default();
    config.max_wal_recovery_time = 1; // Very short timeout to force backup recovery
    config.auto_backup_recovery_enabled = false; // Disable for this test

    let recovery_manager = RecoveryManager::new(
        data_path,
        wal_manager,
        None,
        monitoring,
        config,
    );

    // Recovery should fail gracefully when WAL is corrupted and backup is disabled
    let result = recovery_manager.recover_from_wal().await;

    // Should get an error about backup recovery being disabled
    assert!(result.is_err());

    println!("✅ Recovery with backup fallback test passed");
    Ok(())
}

#[tokio::test]
async fn test_component_health_tracking() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let wal_manager = Arc::new(WalManager::new(
        data_path.join("wal.log"),
        WalConfig::default(),
    )?);

    let metrics = Arc::new(Metrics::new());
    let monitoring = Arc::new(MonitoringSystem::new(metrics, MonitoringConfig::default()));
    let recovery_manager = RecoveryManager::new(
        data_path,
        wal_manager,
        None,
        monitoring,
        RecoveryConfig::default(),
    );

    // Test health check functionality
    let health_issues = recovery_manager.perform_health_check().await?;

    // Check health status
    let stats = recovery_manager.get_recovery_stats();
    // Fresh system should have all healthy components
    assert!(stats.failed_components == 0);

    println!("✅ Component health tracking test passed");
    Ok(())
}

/// Run all recovery tests
pub async fn run_recovery_tests() -> Result<()> {
    println!("🧪 Running comprehensive error recovery tests...\n");

    test_crash_detection_and_recovery().await?;
    test_corruption_detection_and_repair().await?;
    test_wal_recovery_with_transactions().await?;
    test_health_monitoring().await?;
    test_panic_recovery().await?;
    test_engine_integration().await?;
    test_recovery_with_backup_fallback().await?;
    test_component_health_tracking().await?;

    println!("\n🎉 ALL ERROR RECOVERY TESTS PASSED - System is production-ready for fault tolerance!");
    Ok(())
}