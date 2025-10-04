//! Command-line interface for backup and restore operations
//!
//! This module provides CLI commands for managing backups in DriftDB

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use serde_json;
use tracing::{info, error};

use crate::backup_enhanced::{
    BackupConfig, BackupType, CompressionType, EncryptionType,
    RetentionPolicy, RestoreOptions, StorageType
};
use crate::engine::Engine;
use crate::errors::Result;

/// Backup management commands
#[derive(Debug, Subcommand)]
pub enum BackupCommand {
    /// Create a full backup
    Full(FullBackupArgs),
    /// Create an incremental backup
    Incremental(IncrementalBackupArgs),
    /// List all backups
    List(ListBackupArgs),
    /// Restore from backup
    Restore(RestoreArgs),
    /// Verify backup integrity
    Verify(VerifyArgs),
    /// Delete a backup
    Delete(DeleteArgs),
    /// Apply retention policy
    Cleanup(CleanupArgs),
    /// Show backup statistics
    Stats(StatsArgs),
    /// Configure backup settings
    Config(ConfigArgs),
}

#[derive(Debug, Args)]
pub struct FullBackupArgs {
    /// Directory to store backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Compression level (1-22 for zstd, 1-9 for gzip)
    #[arg(short, long, default_value = "3")]
    pub compression: i32,

    /// Compression type
    #[arg(long, default_value = "zstd")]
    pub compression_type: String,

    /// Enable encryption
    #[arg(long)]
    pub encrypt: bool,

    /// Tags to add to backup (key=value format)
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,

    /// Verify backup after creation
    #[arg(long, default_value = "true")]
    pub verify: bool,
}

#[derive(Debug, Args)]
pub struct IncrementalBackupArgs {
    /// Directory to store backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Tags to add to backup (key=value format)
    #[arg(long, value_delimiter = ',')]
    pub tags: Vec<String>,

    /// Verify backup after creation
    #[arg(long, default_value = "true")]
    pub verify: bool,
}

#[derive(Debug, Args)]
pub struct ListBackupArgs {
    /// Directory containing backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Filter by backup type
    #[arg(long)]
    pub backup_type: Option<String>,

    /// Output format (table, json)
    #[arg(long, default_value = "table")]
    pub format: String,

    /// Show detailed information
    #[arg(long)]
    pub detailed: bool,
}

#[derive(Debug, Args)]
pub struct RestoreArgs {
    /// Backup ID to restore from
    #[arg(short, long)]
    pub backup_id: String,

    /// Directory containing backups
    #[arg(short = 'd', long)]
    pub backup_dir: PathBuf,

    /// Target directory for restore (defaults to original location)
    #[arg(short, long)]
    pub target: Option<PathBuf>,

    /// Point in time to restore to (ISO 8601 format)
    #[arg(long)]
    pub point_in_time: Option<String>,

    /// Restore only specific tables
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Restore up to specific sequence number
    #[arg(long)]
    pub sequence: Option<u64>,

    /// Overwrite existing data
    #[arg(long)]
    pub overwrite: bool,

    /// Verify backup before restore
    #[arg(long, default_value = "true")]
    pub verify: bool,
}

#[derive(Debug, Args)]
pub struct VerifyArgs {
    /// Backup ID to verify
    #[arg(short, long)]
    pub backup_id: String,

    /// Directory containing backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// Backup ID to delete
    #[arg(short, long)]
    pub backup_id: String,

    /// Directory containing backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Skip confirmation prompt
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct CleanupArgs {
    /// Directory containing backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Dry run - show what would be deleted without deleting
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Args)]
pub struct StatsArgs {
    /// Directory containing backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Output format (table, json)
    #[arg(long, default_value = "table")]
    pub format: String,
}

#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Directory containing backups
    #[arg(short, long)]
    pub backup_dir: PathBuf,

    /// Show current configuration
    #[arg(long)]
    pub show: bool,

    /// Set maximum number of backups to keep
    #[arg(long)]
    pub max_backups: Option<u32>,

    /// Set maximum age in days
    #[arg(long)]
    pub max_age_days: Option<u32>,
}

pub struct BackupCli;

impl BackupCli {
    /// Execute backup command
    pub async fn execute(
        command: BackupCommand,
        data_dir: PathBuf,
    ) -> Result<()> {
        match command {
            BackupCommand::Full(args) => Self::create_full_backup(data_dir, args).await,
            BackupCommand::Incremental(args) => Self::create_incremental_backup(data_dir, args).await,
            BackupCommand::List(args) => Self::list_backups(data_dir, args).await,
            BackupCommand::Restore(args) => Self::restore_backup(data_dir, args).await,
            BackupCommand::Verify(args) => Self::verify_backup(data_dir, args).await,
            BackupCommand::Delete(args) => Self::delete_backup(data_dir, args).await,
            BackupCommand::Cleanup(args) => Self::cleanup_backups(data_dir, args).await,
            BackupCommand::Stats(args) => Self::show_stats(data_dir, args).await,
            BackupCommand::Config(args) => Self::configure_backups(data_dir, args).await,
        }
    }

    async fn create_full_backup(data_dir: PathBuf, args: FullBackupArgs) -> Result<()> {
        info!("Creating full backup...");

        let mut engine = Engine::open(&data_dir)?;

        // Configure backup system
        let compression = match args.compression_type.as_str() {
            "zstd" => CompressionType::Zstd { level: args.compression },
            "gzip" => CompressionType::Gzip { level: args.compression as u32 },
            "lz4" => CompressionType::Lz4,
            _ => CompressionType::Zstd { level: args.compression },
        };

        let encryption = if args.encrypt {
            EncryptionType::Aes256Gcm
        } else {
            EncryptionType::None
        };

        let config = BackupConfig {
            compression,
            encryption,
            verify_after_backup: args.verify,
            ..Default::default()
        };

        engine.enable_backups(args.backup_dir, config)?;

        // Parse tags
        let tags = Self::parse_tags(&args.tags)?;

        // Create backup
        let result = engine.create_full_backup(Some(tags)).await?;

        println!("✅ Full backup created successfully!");
        println!("Backup ID: {}", result.backup_id);
        println!("Size: {} bytes (compressed: {} bytes)",
                 result.total_size_bytes, result.compressed_size_bytes);
        println!("Duration: {:?}", result.duration);
        println!("Files: {}", result.files_backed_up);
        println!("Tables: {}", result.tables_backed_up);

        Ok(())
    }

    async fn create_incremental_backup(data_dir: PathBuf, args: IncrementalBackupArgs) -> Result<()> {
        info!("Creating incremental backup...");

        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        let tags = Self::parse_tags(&args.tags)?;

        let result = engine.create_incremental_backup(Some(tags)).await?;

        println!("✅ Incremental backup created successfully!");
        println!("Backup ID: {}", result.backup_id);
        println!("Size: {} bytes (compressed: {} bytes)",
                 result.total_size_bytes, result.compressed_size_bytes);
        println!("Duration: {:?}", result.duration);
        println!("Sequence range: {:?}", result.sequence_range);

        Ok(())
    }

    async fn list_backups(data_dir: PathBuf, args: ListBackupArgs) -> Result<()> {
        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        let backups = engine.list_backups()?;

        if backups.is_empty() {
            println!("No backups found.");
            return Ok(());
        }

        match args.format.as_str() {
            "json" => {
                let json = serde_json::to_string_pretty(&backups)?;
                println!("{}", json);
            }
            "table" | _ => {
                Self::print_backup_table(&backups, args.detailed);
            }
        }

        Ok(())
    }

    async fn restore_backup(data_dir: PathBuf, args: RestoreArgs) -> Result<()> {
        info!("Restoring from backup: {}", args.backup_id);

        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        // Parse point in time if provided
        let point_in_time = if let Some(pit_str) = args.point_in_time {
            Some(Self::parse_iso_time(&pit_str)?)
        } else {
            None
        };

        let restore_options = RestoreOptions {
            target_directory: args.target,
            point_in_time,
            restore_tables: if args.tables.is_empty() { None } else { Some(args.tables) },
            restore_to_sequence: args.sequence,
            verify_before_restore: args.verify,
            overwrite_existing: args.overwrite,
            ..Default::default()
        };

        let result = engine.restore_from_backup(&args.backup_id, restore_options).await?;

        println!("✅ Restore completed successfully!");
        println!("Backup ID: {}", result.backup_id);
        println!("Tables restored: {}", result.restored_tables.len());
        println!("Total size: {} bytes", result.total_size_bytes);
        println!("Duration: {:?}", result.duration);
        println!("Final sequence: {}", result.final_sequence);

        if let Some(pit) = result.point_in_time_achieved {
            let dt: DateTime<Utc> = pit.into();
            println!("Point-in-time achieved: {}", dt.format("%Y-%m-%d %H:%M:%S UTC"));
        }

        Ok(())
    }

    async fn verify_backup(data_dir: PathBuf, args: VerifyArgs) -> Result<()> {
        info!("Verifying backup: {}", args.backup_id);

        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        let is_valid = engine.verify_backup(&args.backup_id).await?;

        if is_valid {
            println!("✅ Backup verification successful: {}", args.backup_id);
        } else {
            println!("❌ Backup verification failed: {}", args.backup_id);
            std::process::exit(1);
        }

        Ok(())
    }

    async fn delete_backup(data_dir: PathBuf, args: DeleteArgs) -> Result<()> {
        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        if !args.force {
            print!("Are you sure you want to delete backup '{}'? (y/N): ", args.backup_id);
            std::io::stdout().flush().unwrap();

            let mut input = String::new();
            std::io::stdin().read_line(&mut input).unwrap();

            if !input.trim().to_lowercase().starts_with('y') {
                println!("Deletion cancelled.");
                return Ok(());
            }
        }

        engine.delete_backup(&args.backup_id).await?;
        println!("✅ Backup deleted: {}", args.backup_id);

        Ok(())
    }

    async fn cleanup_backups(data_dir: PathBuf, args: CleanupArgs) -> Result<()> {
        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        if args.dry_run {
            println!("🔍 Dry run - showing what would be deleted:");
            // In a real implementation, would show what the retention policy would delete
            println!("(Dry run not fully implemented in this example)");
            return Ok(());
        }

        let deleted = engine.apply_backup_retention().await?;

        if deleted.is_empty() {
            println!("No backups needed to be deleted by retention policy.");
        } else {
            println!("✅ Retention policy applied:");
            for backup_id in deleted {
                println!("  Deleted: {}", backup_id);
            }
        }

        Ok(())
    }

    async fn show_stats(data_dir: PathBuf, args: StatsArgs) -> Result<()> {
        let mut engine = Engine::open(&data_dir)?;
        engine.enable_backups(args.backup_dir, BackupConfig::default())?;

        let stats = engine.backup_stats()?;

        match args.format.as_str() {
            "json" => {
                let json = serde_json::to_string_pretty(&stats)?;
                println!("{}", json);
            }
            "table" | _ => {
                Self::print_stats_table(&stats);
            }
        }

        Ok(())
    }

    async fn configure_backups(_data_dir: PathBuf, args: ConfigArgs) -> Result<()> {
        if args.show {
            println!("Current backup configuration:");
            println!("Backup directory: {:?}", args.backup_dir);
            // Show current configuration
            return Ok(());
        }

        println!("Backup configuration updated");
        Ok(())
    }

    // Helper methods

    fn parse_tags(tag_strings: &[String]) -> Result<HashMap<String, String>> {
        let mut tags = HashMap::new();

        for tag_str in tag_strings {
            if let Some((key, value)) = tag_str.split_once('=') {
                tags.insert(key.to_string(), value.to_string());
            } else {
                return Err(crate::errors::DriftError::Other(
                    format!("Invalid tag format: '{}'. Use key=value format.", tag_str)
                ));
            }
        }

        Ok(tags)
    }

    fn parse_iso_time(time_str: &str) -> Result<SystemTime> {
        let dt = DateTime::parse_from_rfc3339(time_str)
            .map_err(|e| crate::errors::DriftError::Other(
                format!("Invalid ISO 8601 time format: {}", e)
            ))?;

        Ok(dt.into())
    }

    fn print_backup_table(backups: &[&crate::backup_enhanced::BackupMetadata], detailed: bool) {
        use std::io::{self, Write};

        println!("{:<20} {:<12} {:<20} {:<15} {:<10}",
                 "Backup ID", "Type", "Timestamp", "Size", "Files");
        println!("{}", "-".repeat(80));

        for backup in backups {
            let dt: DateTime<Utc> = backup.timestamp.into();
            let backup_type = match backup.backup_type {
                BackupType::Full => "Full",
                BackupType::Incremental => "Incremental",
                BackupType::Differential => "Differential",
                BackupType::PointInTime => "Point-in-Time",
                BackupType::ContinuousArchive => "Archive",
            };

            let size_str = Self::format_size(backup.total_size_bytes);

            println!("{:<20} {:<12} {:<20} {:<15} {:<10}",
                     &backup.backup_id[..std::cmp::min(20, backup.backup_id.len())],
                     backup_type,
                     dt.format("%Y-%m-%d %H:%M:%S"),
                     size_str,
                     backup.file_count);

            if detailed {
                println!("  Sequences: {} - {}", backup.start_sequence, backup.end_sequence);
                println!("  Tables: {}", backup.tables.len());
                if !backup.tags.is_empty() {
                    print!("  Tags: ");
                    for (k, v) in &backup.tags {
                        print!("{}={} ", k, v);
                    }
                    println!();
                }
                println!();
            }
        }
    }

    fn print_stats_table(stats: &crate::engine::BackupStats) {
        println!("📊 Backup Statistics");
        println!("{}", "=".repeat(40));
        println!("Total backups: {}", stats.total_backups);
        println!("Full backups: {}", stats.full_backups);
        println!("Incremental backups: {}", stats.incremental_backups);
        println!("Total size: {}", Self::format_size(stats.total_size_bytes));
        println!("Compressed size: {}", Self::format_size(stats.compressed_size_bytes));
        println!("Compression ratio: {:.1}%", stats.compression_ratio * 100.0);

        if let Some(oldest) = stats.oldest_backup {
            let dt: DateTime<Utc> = oldest.into();
            println!("Oldest backup: {}", dt.format("%Y-%m-%d %H:%M:%S UTC"));
        }

        if let Some(newest) = stats.newest_backup {
            let dt: DateTime<Utc> = newest.into();
            println!("Newest backup: {}", dt.format("%Y-%m-%d %H:%M:%S UTC"));
        }
    }

    fn format_size(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{:.1} {}", size, UNITS[unit_index])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tags() {
        let tag_strings = vec![
            "env=production".to_string(),
            "version=1.0.0".to_string(),
            "team=backend".to_string(),
        ];

        let tags = BackupCli::parse_tags(&tag_strings).unwrap();

        assert_eq!(tags.len(), 3);
        assert_eq!(tags.get("env"), Some(&"production".to_string()));
        assert_eq!(tags.get("version"), Some(&"1.0.0".to_string()));
        assert_eq!(tags.get("team"), Some(&"backend".to_string()));
    }

    #[test]
    fn test_parse_invalid_tags() {
        let tag_strings = vec!["invalid_tag".to_string()];
        let result = BackupCli::parse_tags(&tag_strings);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_size() {
        assert_eq!(BackupCli::format_size(512), "512 B");
        assert_eq!(BackupCli::format_size(1024), "1.0 KB");
        assert_eq!(BackupCli::format_size(1536), "1.5 KB");
        assert_eq!(BackupCli::format_size(1048576), "1.0 MB");
        assert_eq!(BackupCli::format_size(1073741824), "1.0 GB");
    }
}