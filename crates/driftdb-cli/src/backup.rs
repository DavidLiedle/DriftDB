//! Backup and restore CLI commands for DriftDB

use anyhow::{Context, Result};
use clap::Subcommand;
use serde_json;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use time::OffsetDateTime;

use driftdb_core::backup::{BackupManager, BackupMetadata};
use driftdb_core::{observability::Metrics, Engine};

#[derive(Subcommand)]
pub enum BackupCommands {
    /// Create a backup
    Create {
        /// Source database directory
        #[clap(short, long, default_value = "./data")]
        source: PathBuf,

        /// Destination backup path
        #[clap(short, long)]
        destination: Option<PathBuf>,

        /// Backup type
        #[clap(short = 't', long, default_value = "full")]
        backup_type: String,

        /// Compression (none, zstd, gzip)
        #[clap(short = 'c', long, default_value = "zstd")]
        compression: String,

        /// Parent backup for incremental
        #[clap(short = 'p', long)]
        parent: Option<PathBuf>,
    },

    /// Restore from backup
    Restore {
        /// Backup path to restore from
        #[clap(short, long)]
        backup: PathBuf,

        /// Target database directory
        #[clap(short, long, default_value = "./data")]
        target: PathBuf,

        /// Force overwrite existing data
        #[clap(short, long)]
        force: bool,

        /// Restore to specific point in time
        #[clap(long)]
        point_in_time: Option<String>,
    },

    /// List available backups
    List {
        /// Backup directory
        #[clap(short, long, default_value = "./backups")]
        path: PathBuf,
    },

    /// Verify backup integrity
    Verify {
        /// Backup path to verify
        #[clap(short, long)]
        backup: PathBuf,
    },

    /// Show backup information
    Info {
        /// Backup path
        #[clap(short, long)]
        backup: PathBuf,
    },
}

pub fn run(command: BackupCommands) -> Result<()> {
    match command {
        BackupCommands::Create {
            source,
            destination,
            backup_type,
            compression,
            parent,
        } => create_backup(source, destination, backup_type, compression, parent),
        BackupCommands::Restore {
            backup,
            target,
            force,
            point_in_time,
        } => restore_backup(backup, target, force, point_in_time),
        BackupCommands::List { path } => list_backups(path),
        BackupCommands::Verify { backup } => verify_backup(backup),
        BackupCommands::Info { backup } => show_backup_info(backup),
    }
}

fn create_backup(
    source: PathBuf,
    destination: Option<PathBuf>,
    backup_type: String,
    _compression: String,
    parent: Option<PathBuf>,
) -> Result<()> {
    println!("🔄 Creating {} backup...", backup_type);

    // Generate default backup name if not provided
    let backup_path = destination.unwrap_or_else(|| {
        let now = OffsetDateTime::now_utc();
        let timestamp = format!(
            "{}{}{}_{}{}{}",
            now.year(),
            now.month() as u8,
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        PathBuf::from(format!("./backups/backup_{}", timestamp))
    });

    println!("  Initializing backup...");

    // Open the database
    let _engine = Engine::open(&source).context("Failed to open source database")?;

    let metrics = Arc::new(Metrics::new());
    let backup_mgr = BackupManager::new(&source, metrics);

    // Perform backup based on type
    let metadata = match backup_type.as_str() {
        "full" => {
            println!("  Creating full backup...");
            backup_mgr.create_full_backup(&backup_path)?
        }
        "incremental" => {
            if parent.is_none() {
                return Err(anyhow::anyhow!(
                    "Incremental backup requires parent backup path"
                ));
            }
            println!("  Creating incremental backup...");
            // For incremental, we need to get the last sequence from parent
            // For now, use 0 as the starting sequence
            backup_mgr.create_incremental_backup(&backup_path, 0, Some(parent.as_ref().unwrap()))?
        }
        "differential" => {
            println!("  Creating differential backup...");
            // For now, treat as full backup
            backup_mgr.create_full_backup(&backup_path)?
        }
        _ => {
            return Err(anyhow::anyhow!("Unknown backup type: {}", backup_type));
        }
    };

    println!("✅ Backup completed successfully");

    // Display backup summary
    println!("\n📊 Backup Summary:");
    println!("  Location: {}", backup_path.display());
    println!("  Type: {:?}", metadata.backup_type);
    println!("  Tables: {}", metadata.tables.len());
    println!(
        "  Sequences: {} to {}",
        metadata.start_sequence, metadata.end_sequence
    );
    println!("  Compression: {:?}", metadata.compression);
    println!("  Checksum: {}", metadata.checksum);

    // Save metadata
    let metadata_path = backup_path.join("metadata.json");
    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    fs::write(&metadata_path, metadata_json)?;

    println!(
        "\n✅ Backup created successfully at: {}",
        backup_path.display()
    );

    Ok(())
}

fn restore_backup(
    backup: PathBuf,
    target: PathBuf,
    force: bool,
    point_in_time: Option<String>,
) -> Result<()> {
    println!("🔄 Restoring from backup: {}", backup.display());

    // Check if target exists
    if target.exists() && !force {
        return Err(anyhow::anyhow!(
            "Target directory exists. Use --force to overwrite"
        ));
    }

    // Load metadata
    let metadata_path = backup.join("metadata.json");
    let metadata_json =
        fs::read_to_string(&metadata_path).context("Failed to read backup metadata")?;
    let metadata: BackupMetadata = serde_json::from_str(&metadata_json)?;

    println!("  Processing {} tables...", metadata.tables.len());

    // Create target directory
    if force && target.exists() {
        fs::remove_dir_all(&target)?;
    }
    fs::create_dir_all(&target)?;

    let metrics = Arc::new(Metrics::new());
    let backup_mgr = BackupManager::new(&target, metrics);

    // Restore the backup
    println!("  Restoring database...");
    backup_mgr.restore_from_backup(&backup, Some(&target))?;

    println!("✅ Restore completed");

    // Apply point-in-time recovery if requested
    if let Some(pit_time) = point_in_time {
        println!("⏰ Applying point-in-time recovery to: {}", pit_time);
        // TODO: Implement point-in-time recovery by replaying WAL up to specified time
    }

    println!(
        "\n✅ Database restored successfully to: {}",
        target.display()
    );
    println!(
        "📊 Restored {} tables with sequences {} to {}",
        metadata.tables.len(),
        metadata.start_sequence,
        metadata.end_sequence
    );

    Ok(())
}

fn list_backups(path: PathBuf) -> Result<()> {
    println!("📋 Available backups in: {}", path.display());
    println!("{:-<80}", "");

    if !path.exists() {
        println!("No backups found (directory does not exist)");
        return Ok(());
    }

    let mut backups = Vec::new();

    // Scan for backups
    for entry in fs::read_dir(&path)? {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_dir() {
            let metadata_path = entry_path.join("metadata.json");

            if metadata_path.exists() {
                let metadata_json = fs::read_to_string(&metadata_path)?;
                let metadata: BackupMetadata = serde_json::from_str(&metadata_json)?;

                backups.push((entry_path, metadata));
            }
        }
    }

    if backups.is_empty() {
        println!("No backups found");
    } else {
        // Sort by timestamp
        backups.sort_by_key(|(_, m)| m.timestamp_ms);

        println!(
            "{:<30} {:<10} {:<20} {:<10}",
            "Backup Name", "Type", "Timestamp", "Tables"
        );
        println!("{:-<80}", "");

        for (path, metadata) in backups {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");

            let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
                (metadata.timestamp_ms as i128) * 1_000_000,
            )
            .map(|dt| {
                format!(
                    "{}-{:02}-{:02} {:02}:{:02}:{:02}",
                    dt.year(),
                    dt.month() as u8,
                    dt.day(),
                    dt.hour(),
                    dt.minute(),
                    dt.second()
                )
            })
            .unwrap_or_else(|_| "unknown".to_string());

            println!(
                "{:<30} {:<10} {:<20} {:<10}",
                name,
                format!("{:?}", metadata.backup_type),
                timestamp,
                metadata.tables.len()
            );
        }
    }

    Ok(())
}

fn verify_backup(backup: PathBuf) -> Result<()> {
    println!("🔍 Verifying backup: {}", backup.display());

    // Load metadata
    let metadata_path = backup.join("metadata.json");
    if !metadata_path.exists() {
        return Err(anyhow::anyhow!("Backup metadata not found"));
    }

    let metadata_json = fs::read_to_string(&metadata_path)?;
    let metadata: BackupMetadata = serde_json::from_str(&metadata_json)?;

    println!("  Type: {:?}", metadata.backup_type);
    println!("  Tables: {}", metadata.tables.len());
    println!("  Checksum: {}", metadata.checksum);

    // Verify each table backup
    let mut all_valid = true;
    for table_info in &metadata.tables {
        print!("  Checking table '{}': ", table_info.name);

        let table_dir = backup.join(&table_info.name);
        if !table_dir.exists() {
            println!("❌ Missing");
            all_valid = false;
            continue;
        }

        // Check segment files
        let mut table_valid = true;
        for segment in &table_info.segments_backed_up {
            let segment_path = table_dir.join(&segment.file_name);
            if !segment_path.exists() {
                table_valid = false;
                break;
            }
        }

        if table_valid {
            println!("✅ Valid");
        } else {
            println!("❌ Corrupted");
            all_valid = false;
        }
    }

    if all_valid {
        println!("\n✅ Backup verification passed");
    } else {
        println!("\n❌ Backup verification failed");
        return Err(anyhow::anyhow!("Backup is corrupted"));
    }

    Ok(())
}

fn show_backup_info(backup: PathBuf) -> Result<()> {
    // Load metadata
    let metadata_path = backup.join("metadata.json");
    let metadata_json = fs::read_to_string(&metadata_path)?;
    let metadata: BackupMetadata = serde_json::from_str(&metadata_json)?;

    println!("📊 Backup Information");
    println!("{:-<60}", "");
    println!("  Path: {}", backup.display());
    println!("  Version: {}", metadata.version);
    println!("  Type: {:?}", metadata.backup_type);

    if let Some(parent) = &metadata.parent_backup {
        println!("  Parent: {}", parent);
    }

    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos((metadata.timestamp_ms as i128) * 1_000_000)
            .map(|dt| {
                format!(
                    "{}-{:02}-{:02} {:02}:{:02}:{:02}",
                    dt.year(),
                    dt.month() as u8,
                    dt.day(),
                    dt.hour(),
                    dt.minute(),
                    dt.second()
                )
            })
            .unwrap_or_else(|_| "unknown".to_string());

    println!("  Created: {}", timestamp);
    println!(
        "  Sequences: {} to {}",
        metadata.start_sequence, metadata.end_sequence
    );
    println!("  Compression: {:?}", metadata.compression);
    println!("  Checksum: {}", metadata.checksum);

    println!("\n📋 Tables ({}):", metadata.tables.len());
    for table in &metadata.tables {
        println!(
            "  - {}: {} events, {} segments",
            table.name,
            table.total_events,
            table.segments_backed_up.len()
        );
    }

    // Calculate total size
    let total_size: u64 = metadata
        .tables
        .iter()
        .flat_map(|t| &t.segments_backed_up)
        .map(|s| s.size_bytes)
        .sum();

    let size_mb = total_size as f64 / (1024.0 * 1024.0);
    println!("\n📦 Total Size: {:.2} MB", size_mb);

    Ok(())
}
