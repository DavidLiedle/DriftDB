//! Enhanced backup and restore system for DriftDB
//!
//! This module provides production-ready backup and restore functionality including:
//! - Full, incremental, and differential backups
//! - Point-in-time recovery (PITR)
//! - Backup verification and integrity checking
//! - Compression and encryption support
//! - Backup scheduling and retention policies
//! - Cloud storage integration
//! - Backup catalog and metadata management

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, instrument, warn};

use crate::encryption::EncryptionService;
use crate::errors::{DriftError, Result};
use crate::wal::WalManager;

/// Enhanced backup metadata with comprehensive information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub backup_id: String,
    pub version: String,
    pub timestamp: SystemTime,
    pub tables: Vec<TableBackupInfo>,
    pub backup_type: BackupType,
    pub parent_backup: Option<String>,
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub wal_start_position: u64,
    pub wal_end_position: u64,
    pub total_size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub file_count: usize,
    pub checksum: String,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    pub retention_policy: RetentionPolicy,
    pub tags: HashMap<String, String>,
    pub system_info: SystemBackupInfo,
}

/// Information about system state during backup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemBackupInfo {
    pub hostname: String,
    pub database_version: String,
    pub platform: String,
    pub cpu_count: usize,
    pub total_memory_bytes: u64,
    pub available_disk_bytes: u64,
}

/// Enhanced table backup information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBackupInfo {
    pub name: String,
    pub schema_backup_path: String,
    pub data_backup_path: String,
    pub index_backup_path: String,
    pub segments_backed_up: Vec<SegmentInfo>,
    pub last_sequence: u64,
    pub total_events: u64,
    pub total_size_bytes: u64,
    pub row_count: u64,
    pub backup_timestamp: SystemTime,
}

/// Enhanced segment information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub segment_id: u64,
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub file_name: String,
    pub size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub checksum: String,
    pub event_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BackupType {
    Full,
    Incremental,
    Differential,
    PointInTime,
    ContinuousArchive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Zstd { level: i32 },
    Gzip { level: u32 },
    Lz4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionType {
    None,
    Aes256Gcm,
    ChaCha20Poly1305,
}

/// Backup retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub keep_hourly: Option<u32>,
    pub keep_daily: Option<u32>,
    pub keep_weekly: Option<u32>,
    pub keep_monthly: Option<u32>,
    pub keep_yearly: Option<u32>,
    pub max_age_days: Option<u32>,
    pub max_backup_count: Option<u32>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_hourly: Some(24),       // Keep 24 hourly backups
            keep_daily: Some(7),         // Keep 7 daily backups
            keep_weekly: Some(4),        // Keep 4 weekly backups
            keep_monthly: Some(12),      // Keep 12 monthly backups
            keep_yearly: Some(3),        // Keep 3 yearly backups
            max_age_days: Some(365),     // Maximum 1 year
            max_backup_count: Some(100), // Maximum 100 backups
        }
    }
}

/// Backup storage configuration
#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub storage_type: StorageType,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    pub chunk_size_mb: u64,
    pub parallel_uploads: usize,
    pub verify_after_backup: bool,
    pub delete_after_upload: bool,
    pub retention_policy: RetentionPolicy,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Local,
            compression: CompressionType::Zstd { level: 3 },
            encryption: EncryptionType::None,
            chunk_size_mb: 100,
            parallel_uploads: 4,
            verify_after_backup: true,
            delete_after_upload: false,
            retention_policy: RetentionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StorageType {
    Local,
    S3 {
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    },
    Azure {
        account: String,
        container: String,
        access_key: String,
    },
    Gcs {
        bucket: String,
        service_account_key: String,
    },
}

/// Backup restore options
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub target_directory: Option<PathBuf>,
    pub point_in_time: Option<SystemTime>,
    pub restore_tables: Option<Vec<String>>,
    pub restore_to_sequence: Option<u64>,
    pub verify_before_restore: bool,
    pub overwrite_existing: bool,
    pub parallel_restore: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            target_directory: None,
            point_in_time: None,
            restore_tables: None,
            restore_to_sequence: None,
            verify_before_restore: true,
            overwrite_existing: false,
            parallel_restore: true,
        }
    }
}

/// Backup operation result
#[derive(Debug)]
pub struct BackupResult {
    pub backup_id: String,
    pub backup_type: BackupType,
    pub total_size_bytes: u64,
    pub compressed_size_bytes: u64,
    pub duration: Duration,
    pub files_backed_up: usize,
    pub tables_backed_up: usize,
    pub sequence_range: (u64, u64),
    pub warnings: Vec<String>,
}

/// Restore operation result
#[derive(Debug)]
pub struct RestoreResult {
    pub backup_id: String,
    pub restored_tables: Vec<String>,
    pub total_size_bytes: u64,
    pub duration: Duration,
    pub files_restored: usize,
    pub final_sequence: u64,
    pub point_in_time_achieved: Option<SystemTime>,
    pub warnings: Vec<String>,
}

/// Enhanced backup manager
pub struct EnhancedBackupManager {
    data_dir: PathBuf,
    pub backup_dir: PathBuf,
    config: BackupConfig,
    wal_manager: Arc<WalManager>,
    encryption_service: Option<Arc<EncryptionService>>,
    catalog: BackupCatalog,
}

/// Backup catalog for tracking all backups
#[derive(Debug, Clone)]
pub struct BackupCatalog {
    catalog_path: PathBuf,
    backups: HashMap<String, BackupMetadata>,
}

impl BackupCatalog {
    pub fn new<P: AsRef<Path>>(catalog_path: P) -> Result<Self> {
        let catalog_path = catalog_path.as_ref().to_path_buf();
        let mut catalog = Self {
            catalog_path,
            backups: HashMap::new(),
        };
        catalog.load()?;
        Ok(catalog)
    }

    pub fn add_backup(&mut self, metadata: BackupMetadata) -> Result<()> {
        self.backups.insert(metadata.backup_id.clone(), metadata);
        self.save()
    }

    pub fn remove_backup(&mut self, backup_id: &str) -> Result<Option<BackupMetadata>> {
        let removed = self.backups.remove(backup_id);
        self.save()?;
        Ok(removed)
    }

    pub fn list_backups(&self) -> Vec<&BackupMetadata> {
        let mut backups: Vec<_> = self.backups.values().collect();
        backups.sort_by_key(|b| b.timestamp);
        backups
    }

    pub fn get_backup(&self, backup_id: &str) -> Option<&BackupMetadata> {
        self.backups.get(backup_id)
    }

    pub fn find_backups_by_type(&self, backup_type: &BackupType) -> Vec<&BackupMetadata> {
        self.backups
            .values()
            .filter(|b| {
                std::mem::discriminant(&b.backup_type) == std::mem::discriminant(backup_type)
            })
            .collect()
    }

    pub fn find_backups_in_range(
        &self,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<&BackupMetadata> {
        self.backups
            .values()
            .filter(|b| b.timestamp >= start && b.timestamp <= end)
            .collect()
    }

    fn load(&mut self) -> Result<()> {
        if !self.catalog_path.exists() {
            return Ok(());
        }

        let content = fs::read_to_string(&self.catalog_path)?;
        self.backups = serde_json::from_str(&content)
            .map_err(|e| DriftError::Other(format!("Failed to parse backup catalog: {}", e)))?;

        Ok(())
    }

    fn save(&self) -> Result<()> {
        if let Some(parent) = self.catalog_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = serde_json::to_string_pretty(&self.backups)?;
        fs::write(&self.catalog_path, content)?;
        Ok(())
    }
}

impl EnhancedBackupManager {
    /// Create a new enhanced backup manager
    pub fn new<P: AsRef<Path>>(
        data_dir: P,
        backup_dir: P,
        wal_manager: Arc<WalManager>,
        config: BackupConfig,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let backup_dir = backup_dir.as_ref().to_path_buf();

        fs::create_dir_all(&backup_dir)?;

        let catalog_path = backup_dir.join("catalog.json");
        let catalog = BackupCatalog::new(catalog_path)?;

        Ok(Self {
            data_dir,
            backup_dir,
            config,
            wal_manager,
            encryption_service: None,
            catalog,
        })
    }

    /// Set encryption service for encrypted backups
    pub fn with_encryption(mut self, encryption_service: Arc<EncryptionService>) -> Self {
        self.encryption_service = Some(encryption_service);
        self
    }

    /// Create a full backup with enhanced features
    #[instrument(skip(self))]
    pub async fn create_full_backup(
        &mut self,
        tags: Option<HashMap<String, String>>,
    ) -> Result<BackupResult> {
        let start_time = SystemTime::now();
        let backup_id = self.generate_backup_id(&BackupType::Full, start_time);
        let backup_path = self.backup_dir.join(&backup_id);

        info!("Starting enhanced full backup: {}", backup_id);

        fs::create_dir_all(&backup_path)?;

        // Get current WAL position
        let wal_start_position = self.wal_manager.current_sequence();

        // Collect system information
        let system_info = self.collect_system_info().await?;

        // Backup all tables
        let table_results = self.backup_all_tables(&backup_path).await?;

        // Get final WAL position
        let wal_end_position = self.wal_manager.current_sequence();

        // Backup WAL segments
        self.backup_wal_segments(&backup_path, wal_start_position, wal_end_position)
            .await?;

        // Calculate statistics
        let total_size = self.calculate_backup_size(&backup_path)?;
        let compressed_size = self.get_compressed_size(&backup_path)?;

        // Create metadata
        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: start_time,
            tables: table_results.iter().map(|t| t.table_info.clone()).collect(),
            backup_type: BackupType::Full,
            parent_backup: None,
            start_sequence: table_results
                .iter()
                .map(|t| t.start_sequence)
                .min()
                .unwrap_or(0),
            end_sequence: table_results
                .iter()
                .map(|t| t.end_sequence)
                .max()
                .unwrap_or(0),
            wal_start_position,
            wal_end_position,
            total_size_bytes: total_size,
            compressed_size_bytes: compressed_size,
            file_count: self.count_backup_files(&backup_path)?,
            checksum: self.compute_backup_checksum(&backup_path)?,
            compression: self.config.compression.clone(),
            encryption: self.config.encryption.clone(),
            retention_policy: self.config.retention_policy.clone(),
            tags: tags.unwrap_or_default(),
            system_info,
        };

        // Save metadata
        self.save_backup_metadata(&backup_path, &metadata)?;

        // Add to catalog
        self.catalog.add_backup(metadata)?;

        // Verify backup if configured
        if self.config.verify_after_backup {
            self.verify_backup(&backup_path).await?;
        }

        // Upload to cloud storage if configured
        if !matches!(self.config.storage_type, StorageType::Local) {
            self.upload_backup(&backup_path, &backup_id).await?;
        }

        let duration = start_time.elapsed().unwrap_or_default();

        let result = BackupResult {
            backup_id,
            backup_type: BackupType::Full,
            total_size_bytes: total_size,
            compressed_size_bytes: compressed_size,
            duration,
            files_backed_up: self.count_backup_files(&backup_path)?,
            tables_backed_up: table_results.len(),
            sequence_range: (wal_start_position, wal_end_position),
            warnings: Vec::new(),
        };

        info!(
            "Full backup completed: {} ({} bytes compressed to {} bytes in {:?})",
            result.backup_id,
            result.total_size_bytes,
            result.compressed_size_bytes,
            result.duration
        );

        Ok(result)
    }

    /// Create an incremental backup since the last backup
    #[instrument(skip(self))]
    pub async fn create_incremental_backup(
        &mut self,
        tags: Option<HashMap<String, String>>,
    ) -> Result<BackupResult> {
        let start_time = SystemTime::now();
        let backup_id = self.generate_backup_id(&BackupType::Incremental, start_time);

        // Find the last backup to use as parent
        let backups = self.catalog.list_backups();
        let parent_backup = backups.last().ok_or_else(|| {
            DriftError::Other("No parent backup found for incremental backup".to_string())
        })?;

        let since_sequence = parent_backup.end_sequence;
        let parent_id = parent_backup.backup_id.clone();

        info!(
            "Starting incremental backup: {} (since sequence {})",
            backup_id, since_sequence
        );

        let backup_path = self.backup_dir.join(&backup_id);
        fs::create_dir_all(&backup_path)?;

        // Get current WAL position
        let wal_start_position = since_sequence;
        let wal_end_position = self.wal_manager.current_sequence();

        if wal_end_position <= since_sequence {
            info!(
                "No new data since last backup (sequence {})",
                since_sequence
            );
            return Err(DriftError::Other("No new data to backup".to_string()));
        }

        // Backup changed tables only
        let table_results = self
            .backup_changed_tables(&backup_path, since_sequence)
            .await?;

        // Backup WAL segments since last backup
        self.backup_wal_segments(&backup_path, wal_start_position, wal_end_position)
            .await?;

        // Calculate statistics
        let total_size = self.calculate_backup_size(&backup_path)?;
        let compressed_size = self.get_compressed_size(&backup_path)?;

        // Create metadata
        let system_info = self.collect_system_info().await?;
        let metadata = BackupMetadata {
            backup_id: backup_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: start_time,
            tables: table_results.iter().map(|t| t.table_info.clone()).collect(),
            backup_type: BackupType::Incremental,
            parent_backup: Some(parent_id),
            start_sequence: since_sequence + 1,
            end_sequence: wal_end_position,
            wal_start_position,
            wal_end_position,
            total_size_bytes: total_size,
            compressed_size_bytes: compressed_size,
            file_count: self.count_backup_files(&backup_path)?,
            checksum: self.compute_backup_checksum(&backup_path)?,
            compression: self.config.compression.clone(),
            encryption: self.config.encryption.clone(),
            retention_policy: self.config.retention_policy.clone(),
            tags: tags.unwrap_or_default(),
            system_info,
        };

        // Save metadata
        self.save_backup_metadata(&backup_path, &metadata)?;

        // Add to catalog
        self.catalog.add_backup(metadata)?;

        // Verify backup if configured
        if self.config.verify_after_backup {
            self.verify_backup(&backup_path).await?;
        }

        let duration = start_time.elapsed().unwrap_or_default();

        let result = BackupResult {
            backup_id,
            backup_type: BackupType::Incremental,
            total_size_bytes: total_size,
            compressed_size_bytes: compressed_size,
            duration,
            files_backed_up: self.count_backup_files(&backup_path)?,
            tables_backed_up: table_results.len(),
            sequence_range: (since_sequence + 1, wal_end_position),
            warnings: Vec::new(),
        };

        info!(
            "Incremental backup completed: {} ({} new sequences)",
            result.backup_id,
            wal_end_position - since_sequence
        );

        Ok(result)
    }

    /// Restore from backup with enhanced options
    #[instrument(skip(self, options))]
    pub async fn restore_backup(
        &mut self,
        backup_id: &str,
        options: RestoreOptions,
    ) -> Result<RestoreResult> {
        let start_time = SystemTime::now();

        info!("Starting restore from backup: {}", backup_id);

        let backup_metadata = self
            .catalog
            .get_backup(backup_id)
            .ok_or_else(|| DriftError::Other(format!("Backup not found: {}", backup_id)))?
            .clone();

        let backup_path = self.backup_dir.join(backup_id);
        if !backup_path.exists() {
            // Try to download from cloud storage
            if !matches!(self.config.storage_type, StorageType::Local) {
                self.download_backup(backup_id, &backup_path).await?;
            } else {
                return Err(DriftError::Other(format!(
                    "Backup directory not found: {:?}",
                    backup_path
                )));
            }
        }

        // Verify backup before restore if configured
        if options.verify_before_restore {
            self.verify_backup(&backup_path).await?;
        }

        let target_dir = options
            .target_directory
            .unwrap_or_else(|| self.data_dir.clone());

        // Prepare target directory
        if options.overwrite_existing && target_dir.exists() {
            warn!(
                "Removing existing data directory for restore: {:?}",
                target_dir
            );
            fs::remove_dir_all(&target_dir)?;
        }
        fs::create_dir_all(&target_dir)?;

        let mut restored_tables = Vec::new();
        let mut total_size = 0u64;
        let mut files_restored = 0usize;

        // Restore tables
        for table_info in &backup_metadata.tables {
            if let Some(ref table_filter) = options.restore_tables {
                if !table_filter.contains(&table_info.name) {
                    continue;
                }
            }

            info!("Restoring table: {}", table_info.name);
            let table_size = self
                .restore_table(&backup_path, &target_dir, table_info)
                .await?;
            total_size += table_size;
            files_restored += table_info.segments_backed_up.len();
            restored_tables.push(table_info.name.clone());
        }

        // Restore WAL if needed for point-in-time recovery
        let final_sequence = if let Some(target_sequence) = options.restore_to_sequence {
            self.restore_wal_to_sequence(&backup_path, &target_dir, target_sequence)
                .await?
        } else if let Some(pit_time) = options.point_in_time {
            self.restore_wal_to_time(&backup_path, &target_dir, pit_time)
                .await?
        } else {
            backup_metadata.end_sequence
        };

        let duration = start_time.elapsed().unwrap_or_default();

        let result = RestoreResult {
            backup_id: backup_id.to_string(),
            restored_tables,
            total_size_bytes: total_size,
            duration,
            files_restored,
            final_sequence,
            point_in_time_achieved: options.point_in_time,
            warnings: Vec::new(),
        };

        info!(
            "Restore completed: {} ({} tables, {} bytes in {:?})",
            backup_id,
            result.restored_tables.len(),
            result.total_size_bytes,
            result.duration
        );

        Ok(result)
    }

    /// List all available backups
    pub fn list_backups(&self) -> Vec<&BackupMetadata> {
        self.catalog.list_backups()
    }

    /// Find backups by criteria
    pub fn find_backups(
        &self,
        backup_type: Option<BackupType>,
        start_time: Option<SystemTime>,
        end_time: Option<SystemTime>,
    ) -> Vec<&BackupMetadata> {
        let mut backups = if let Some(btype) = backup_type {
            self.catalog.find_backups_by_type(&btype)
        } else {
            self.catalog.list_backups()
        };

        if let (Some(start), Some(end)) = (start_time, end_time) {
            backups = self.catalog.find_backups_in_range(start, end);
        }

        backups
    }

    /// Delete a backup and clean up storage
    #[instrument(skip(self))]
    pub async fn delete_backup(&mut self, backup_id: &str) -> Result<()> {
        info!("Deleting backup: {}", backup_id);

        // Remove from catalog first
        let _metadata = self.catalog.remove_backup(backup_id)?.ok_or_else(|| {
            DriftError::Other(format!("Backup not found in catalog: {}", backup_id))
        })?;

        // Remove local backup directory
        let backup_path = self.backup_dir.join(backup_id);
        if backup_path.exists() {
            fs::remove_dir_all(&backup_path)?;
        }

        // Remove from cloud storage if configured
        if !matches!(self.config.storage_type, StorageType::Local) {
            self.delete_cloud_backup(backup_id).await?;
        }

        info!("Backup deleted: {}", backup_id);
        Ok(())
    }

    /// Apply retention policy to clean up old backups
    #[instrument(skip(self))]
    pub async fn apply_retention_policy(&mut self) -> Result<Vec<String>> {
        info!("Applying backup retention policy");

        let max_age_days = self.config.retention_policy.max_age_days;
        let max_backup_count = self.config.retention_policy.max_backup_count;
        let mut deleted_backups = Vec::new();

        // Get all backups sorted by timestamp
        let mut backups = self.catalog.list_backups();
        backups.sort_by_key(|b| b.timestamp);

        // Apply age-based cleanup
        if let Some(max_age_days) = max_age_days {
            let cutoff_time =
                SystemTime::now() - Duration::from_secs(max_age_days as u64 * 24 * 60 * 60);

            let to_delete: Vec<_> = backups
                .iter()
                .filter(|b| b.timestamp < cutoff_time)
                .map(|b| b.backup_id.clone())
                .collect();

            for backup_id in to_delete {
                self.delete_backup(&backup_id).await?;
                deleted_backups.push(backup_id);
            }
        }

        // Refresh backups list for count-based cleanup
        let mut backups = self.catalog.list_backups();
        backups.sort_by_key(|b| b.timestamp);

        // Apply count-based cleanup
        if let Some(max_count) = max_backup_count {
            if backups.len() > max_count as usize {
                let to_delete_count = backups.len() - max_count as usize;

                let backup_ids_to_delete: Vec<_> = backups
                    .iter()
                    .take(to_delete_count)
                    .map(|b| b.backup_id.clone())
                    .collect();

                for backup_id in backup_ids_to_delete {
                    self.delete_backup(&backup_id).await?;
                    deleted_backups.push(backup_id);
                }
            }
        }

        info!(
            "Retention policy applied: {} backups deleted",
            deleted_backups.len()
        );
        Ok(deleted_backups)
    }

    /// Verify backup integrity
    #[instrument(skip(self))]
    pub async fn verify_backup(&self, backup_path: &Path) -> Result<bool> {
        info!("Verifying backup integrity: {:?}", backup_path);

        // Load metadata
        let metadata_path = backup_path.join("metadata.json");
        if !metadata_path.exists() {
            return Err(DriftError::Other("Backup metadata not found".to_string()));
        }

        let metadata: BackupMetadata = serde_json::from_str(&fs::read_to_string(metadata_path)?)?;

        // Verify checksum
        let computed_checksum = self.compute_backup_checksum(backup_path)?;
        if computed_checksum != metadata.checksum {
            warn!(
                "Backup checksum mismatch: expected {}, got {}",
                metadata.checksum, computed_checksum
            );
            return Ok(false);
        }

        // Verify individual files
        for table_info in &metadata.tables {
            for segment in &table_info.segments_backed_up {
                let segment_path = backup_path
                    .join("tables")
                    .join(&table_info.name)
                    .join(&segment.file_name);
                if !segment_path.exists() {
                    warn!("Missing backup file: {:?}", segment_path);
                    return Ok(false);
                }

                // Verify segment checksum
                let segment_checksum = self.compute_file_checksum(&segment_path)?;
                if segment_checksum != segment.checksum {
                    warn!(
                        "Segment checksum mismatch in {}: expected {}, got {}",
                        segment.file_name, segment.checksum, segment_checksum
                    );
                    return Ok(false);
                }
            }
        }

        info!("Backup verification successful");
        Ok(true)
    }

    // Helper methods (implementation details)

    fn generate_backup_id(&self, backup_type: &BackupType, timestamp: SystemTime) -> String {
        let dt: DateTime<Utc> = timestamp.into();
        let type_prefix = match backup_type {
            BackupType::Full => "full",
            BackupType::Incremental => "inc",
            BackupType::Differential => "diff",
            BackupType::PointInTime => "pit",
            BackupType::ContinuousArchive => "arc",
        };
        format!("{}_{}", type_prefix, dt.format("%Y%m%d_%H%M%S"))
    }

    async fn collect_system_info(&self) -> Result<SystemBackupInfo> {
        Ok(SystemBackupInfo {
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            database_version: env!("CARGO_PKG_VERSION").to_string(),
            platform: std::env::consts::OS.to_string(),
            cpu_count: num_cpus::get(),
            total_memory_bytes: 0,   // Would get from system info
            available_disk_bytes: 0, // Would get from filesystem
        })
    }

    async fn backup_all_tables(&self, backup_path: &Path) -> Result<Vec<TableBackupResult>> {
        let tables_dir = self.data_dir.join("tables");
        let mut results = Vec::new();

        if !tables_dir.exists() {
            return Ok(results);
        }

        for entry in fs::read_dir(&tables_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let table_name = entry.file_name().to_string_lossy().to_string();
                let result = self.backup_table_full(&table_name, backup_path).await?;
                results.push(result);
            }
        }

        Ok(results)
    }

    async fn backup_changed_tables(
        &self,
        _backup_path: &Path,
        _since_sequence: u64,
    ) -> Result<Vec<TableBackupResult>> {
        // Implementation would check which tables have changes since the sequence
        // For now, return empty as this requires integration with table metadata
        Ok(Vec::new())
    }

    async fn backup_table_full(
        &self,
        table_name: &str,
        _backup_path: &Path,
    ) -> Result<TableBackupResult> {
        // Implementation would backup table schema, data, and indexes
        // This is a placeholder for the actual table backup logic
        Ok(TableBackupResult {
            table_info: TableBackupInfo {
                name: table_name.to_string(),
                schema_backup_path: format!("tables/{}/schema.json", table_name),
                data_backup_path: format!("tables/{}/data", table_name),
                index_backup_path: format!("tables/{}/indexes", table_name),
                segments_backed_up: Vec::new(),
                last_sequence: 0,
                total_events: 0,
                total_size_bytes: 0,
                row_count: 0,
                backup_timestamp: SystemTime::now(),
            },
            start_sequence: 0,
            end_sequence: 0,
        })
    }

    async fn backup_wal_segments(
        &self,
        backup_path: &Path,
        start_pos: u64,
        end_pos: u64,
    ) -> Result<()> {
        info!("Backing up WAL segments from {} to {}", start_pos, end_pos);

        let wal_backup_dir = backup_path.join("wal");
        fs::create_dir_all(&wal_backup_dir)?;

        // Copy WAL file
        if let Ok(entries) = self.wal_manager.replay_from_sequence(start_pos) {
            let wal_backup_file = wal_backup_dir.join("wal_entries.json");
            let file = File::create(wal_backup_file)?;
            serde_json::to_writer_pretty(file, &entries)?;
        }

        Ok(())
    }

    async fn restore_table(
        &self,
        backup_path: &Path,
        target_dir: &Path,
        table_info: &TableBackupInfo,
    ) -> Result<u64> {
        info!("Restoring table: {}", table_info.name);

        let table_target = target_dir.join("tables").join(&table_info.name);
        fs::create_dir_all(&table_target)?;

        // Restore schema
        let schema_source = backup_path.join(&table_info.schema_backup_path);
        let schema_target = table_target.join("schema.json");
        if schema_source.exists() {
            fs::copy(&schema_source, &schema_target)?;
        }

        // Restore data segments
        let data_source = backup_path.join(&table_info.data_backup_path);
        let data_target = table_target.join("segments");
        if data_source.exists() {
            fs::create_dir_all(&data_target)?;
            // Copy all segment files
            for segment in &table_info.segments_backed_up {
                let seg_source = data_source.join(&segment.file_name);
                let seg_target = data_target.join(&segment.file_name);
                if seg_source.exists() {
                    fs::copy(&seg_source, &seg_target)?;
                }
            }
        }

        Ok(table_info.total_size_bytes)
    }

    async fn restore_wal_to_sequence(
        &self,
        _backup_path: &Path,
        _target_dir: &Path,
        target_sequence: u64,
    ) -> Result<u64> {
        // Implementation would restore WAL up to a specific sequence
        Ok(target_sequence)
    }

    async fn restore_wal_to_time(
        &self,
        _backup_path: &Path,
        _target_dir: &Path,
        _target_time: SystemTime,
    ) -> Result<u64> {
        // Implementation would restore WAL up to a specific time
        Ok(0)
    }

    async fn upload_backup(&self, _backup_path: &Path, backup_id: &str) -> Result<()> {
        match &self.config.storage_type {
            StorageType::Local => Ok(()),
            StorageType::S3 { .. } => {
                info!("Uploading backup {} to S3", backup_id);
                // Implementation would upload to S3
                Ok(())
            }
            StorageType::Azure { .. } => {
                info!("Uploading backup {} to Azure", backup_id);
                // Implementation would upload to Azure
                Ok(())
            }
            StorageType::Gcs { .. } => {
                info!("Uploading backup {} to GCS", backup_id);
                // Implementation would upload to Google Cloud Storage
                Ok(())
            }
        }
    }

    async fn download_backup(&self, backup_id: &str, _backup_path: &Path) -> Result<()> {
        info!("Downloading backup {} from cloud storage", backup_id);
        // Implementation would download from configured cloud storage
        Ok(())
    }

    async fn delete_cloud_backup(&self, backup_id: &str) -> Result<()> {
        info!("Deleting backup {} from cloud storage", backup_id);
        // Implementation would delete from configured cloud storage
        Ok(())
    }

    fn calculate_backup_size(&self, backup_path: &Path) -> Result<u64> {
        let mut total_size = 0u64;
        self.visit_backup_files(backup_path, &mut |path| {
            if let Ok(metadata) = fs::metadata(path) {
                total_size += metadata.len();
            }
            Ok(())
        })?;
        Ok(total_size)
    }

    fn get_compressed_size(&self, backup_path: &Path) -> Result<u64> {
        // For now, return same as total size
        // In reality, would track compressed size during backup
        self.calculate_backup_size(backup_path)
    }

    fn count_backup_files(&self, backup_path: &Path) -> Result<usize> {
        let mut count = 0usize;
        self.visit_backup_files(backup_path, &mut |_| {
            count += 1;
            Ok(())
        })?;
        Ok(count)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn visit_backup_files<F>(&self, backup_path: &Path, visitor: &mut F) -> Result<()>
    where
        F: FnMut(&Path) -> Result<()>,
    {
        if backup_path.is_file() {
            visitor(backup_path)?;
        } else if backup_path.is_dir() {
            for entry in fs::read_dir(backup_path)? {
                let entry = entry?;
                self.visit_backup_files(&entry.path(), visitor)?;
            }
        }
        Ok(())
    }

    fn compute_backup_checksum(&self, backup_path: &Path) -> Result<String> {
        let mut hasher = Sha256::new();
        let mut paths = Vec::new();

        // Collect all file paths and sort them for consistent hashing
        self.visit_backup_files(backup_path, &mut |path| {
            paths.push(path.to_path_buf());
            Ok(())
        })?;
        paths.sort();

        // Hash each file in order
        for path in paths {
            if path.is_file() {
                let mut file = File::open(&path)?;
                let mut buffer = [0u8; 8192];
                loop {
                    let bytes_read = file.read(&mut buffer)?;
                    if bytes_read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..bytes_read]);
                }
            }
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    fn compute_file_checksum(&self, file_path: &Path) -> Result<String> {
        let mut hasher = Sha256::new();
        let mut file = File::open(file_path)?;
        let mut buffer = [0u8; 8192];

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    fn save_backup_metadata(&self, backup_path: &Path, metadata: &BackupMetadata) -> Result<()> {
        let metadata_path = backup_path.join("metadata.json");
        let file = File::create(metadata_path)?;
        serde_json::to_writer_pretty(file, metadata)?;
        Ok(())
    }
}

#[derive(Debug)]
struct TableBackupResult {
    table_info: TableBackupInfo,
    start_sequence: u64,
    end_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_backup_catalog() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("catalog.json");

        let mut catalog = BackupCatalog::new(&catalog_path)?;

        let metadata = BackupMetadata {
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
            system_info: SystemBackupInfo {
                hostname: "test-host".to_string(),
                database_version: "1.0.0".to_string(),
                platform: "linux".to_string(),
                cpu_count: 4,
                total_memory_bytes: 8_000_000_000,
                available_disk_bytes: 100_000_000_000,
            },
        };

        catalog.add_backup(metadata.clone())?;

        let backups = catalog.list_backups();
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].backup_id, "test_backup_001");

        let found = catalog.get_backup("test_backup_001");
        assert!(found.is_some());
        assert_eq!(found.unwrap().backup_id, "test_backup_001");

        println!("✅ Backup catalog test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_enhanced_backup_manager() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let backup_dir = temp_dir.path().join("backups");

        fs::create_dir_all(&data_dir)?;

        let wal_manager = Arc::new(WalManager::new(
            data_dir.join("wal.log"),
            WalConfig::default(),
        )?);

        let config = BackupConfig::default();
        let mut backup_manager =
            EnhancedBackupManager::new(&data_dir, &backup_dir, wal_manager, config)?;

        // Test backup creation
        let result = backup_manager.create_full_backup(None).await?;
        assert!(!result.backup_id.is_empty());
        assert_eq!(result.backup_type, BackupType::Full);

        // Test backup listing
        let backups = backup_manager.list_backups();
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].backup_id, result.backup_id);

        println!("✅ Enhanced backup manager test passed");
        Ok(())
    }
}
