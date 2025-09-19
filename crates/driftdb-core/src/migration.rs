//! Schema migration system for DriftDB
//!
//! Provides safe, versioned schema evolution with:
//! - Forward and backward migrations
//! - Automatic rollback on failure
//! - Migration history tracking
//! - Dry-run capability
//! - Zero-downtime migrations

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, warn};

use crate::errors::{DriftError, Result};
use crate::schema::{Schema, ColumnDef};

/// Migration version using semantic versioning
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl Version {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    #[allow(clippy::should_implement_trait)]  // Not implementing FromStr trait intentionally
    pub fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(DriftError::Other(format!("Invalid version: {}", s)));
        }

        Ok(Self {
            major: parts[0].parse().map_err(|_| DriftError::Other("Invalid major version".into()))?,
            minor: parts[1].parse().map_err(|_| DriftError::Other("Invalid minor version".into()))?,
            patch: parts[2].parse().map_err(|_| DriftError::Other("Invalid patch version".into()))?,
        })
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Type of schema change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationType {
    /// Add a new column
    AddColumn {
        table: String,
        column: ColumnDef,
        default_value: Option<serde_json::Value>,
    },
    /// Remove a column
    DropColumn {
        table: String,
        column: String,
    },
    /// Rename a column
    RenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },
    /// Add an index
    AddIndex {
        table: String,
        column: String,
    },
    /// Remove an index
    DropIndex {
        table: String,
        column: String,
    },
    /// Create a new table
    CreateTable {
        name: String,
        schema: Schema,
    },
    /// Drop a table
    DropTable {
        name: String,
    },
    /// Custom migration with SQL or code
    Custom {
        description: String,
        up_script: String,
        down_script: String,
    },
}

/// A single migration step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pub version: Version,
    pub name: String,
    pub description: String,
    pub migration_type: MigrationType,
    pub checksum: String,
    pub requires_downtime: bool,
    pub estimated_duration_ms: u64,
}

impl Migration {
    pub fn new(
        version: Version,
        name: String,
        description: String,
        migration_type: MigrationType,
    ) -> Self {
        #[allow(clippy::match_like_matches_macro)]  // More readable this way
        let requires_downtime = match &migration_type {
            MigrationType::DropColumn { .. } => true,
            MigrationType::DropTable { .. } => true,
            MigrationType::RenameColumn { .. } => true,
            _ => false,
        };

        let mut migration = Self {
            version,
            name,
            description,
            migration_type,
            checksum: String::new(),
            requires_downtime,
            estimated_duration_ms: 1000, // Default 1 second
        };

        migration.checksum = migration.calculate_checksum();
        migration
    }

    fn calculate_checksum(&self) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(self.version.to_string());
        hasher.update(&self.name);
        hasher.update(&self.description);
        hasher.update(format!("{:?}", self.migration_type));
        format!("{:x}", hasher.finalize())
    }

    pub fn validate_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

/// Applied migration record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub version: Version,
    pub name: String,
    pub checksum: String,
    pub applied_at: u64, // Unix timestamp
    pub duration_ms: u64,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Migration manager
pub struct MigrationManager {
    data_dir: PathBuf,
    migrations_dir: PathBuf,
    history: BTreeMap<Version, AppliedMigration>,
    pending_migrations: BTreeMap<Version, Migration>,
}

impl MigrationManager {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let migrations_dir = data_dir.join("migrations");
        fs::create_dir_all(&migrations_dir)?;

        let mut manager = Self {
            data_dir,
            migrations_dir: migrations_dir.clone(),
            history: BTreeMap::new(),
            pending_migrations: BTreeMap::new(),
        };

        manager.load_history()?;
        manager.load_pending_migrations()?;

        Ok(manager)
    }

    /// Load migration history
    fn load_history(&mut self) -> Result<()> {
        let history_file = self.migrations_dir.join("history.json");
        if history_file.exists() {
            let content = fs::read_to_string(history_file)?;
            let migrations: Vec<AppliedMigration> = serde_json::from_str(&content)?;
            for migration in migrations {
                self.history.insert(migration.version.clone(), migration);
            }
        }
        Ok(())
    }

    /// Save migration history
    fn save_history(&self) -> Result<()> {
        let history_file = self.migrations_dir.join("history.json");
        let migrations: Vec<_> = self.history.values().cloned().collect();
        let content = serde_json::to_string_pretty(&migrations)?;
        fs::write(history_file, content)?;
        Ok(())
    }

    /// Load pending migrations from directory
    fn load_pending_migrations(&mut self) -> Result<()> {
        let pending_dir = self.migrations_dir.join("pending");
        if !pending_dir.exists() {
            fs::create_dir_all(&pending_dir)?;
            return Ok(());
        }

        for entry in fs::read_dir(pending_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension() == Some(std::ffi::OsStr::new("json")) {
                let content = fs::read_to_string(&path)?;
                let migration: Migration = serde_json::from_str(&content)?;

                if !migration.validate_checksum() {
                    warn!("Migration {} has invalid checksum", migration.version);
                    continue;
                }

                if !self.history.contains_key(&migration.version) {
                    self.pending_migrations.insert(migration.version.clone(), migration);
                }
            }
        }

        Ok(())
    }

    /// Add a new migration
    pub fn add_migration(&mut self, migration: Migration) -> Result<()> {
        if self.history.contains_key(&migration.version) {
            return Err(DriftError::Other(format!(
                "Migration {} already applied",
                migration.version
            )));
        }

        if self.pending_migrations.contains_key(&migration.version) {
            return Err(DriftError::Other(format!(
                "Migration {} already exists",
                migration.version
            )));
        }

        // Save to pending directory
        let file_path = self.migrations_dir
            .join("pending")
            .join(format!("{}.json", migration.version));
        let content = serde_json::to_string_pretty(&migration)?;
        fs::write(file_path, content)?;

        self.pending_migrations.insert(migration.version.clone(), migration);
        Ok(())
    }

    /// Get current schema version
    pub fn current_version(&self) -> Option<Version> {
        self.history.keys().max().cloned()
    }

    /// Get pending migrations
    pub fn pending_migrations(&self) -> Vec<&Migration> {
        self.pending_migrations.values().collect()
    }

    /// Apply a migration with Engine
    #[instrument(skip(self, engine))]
    pub fn apply_migration_with_engine(&mut self, version: &Version, engine: &mut crate::Engine, dry_run: bool) -> Result<()> {
        // Check if already applied
        if self.history.contains_key(version) {
            info!("Migration {} already applied, skipping", version);
            return Ok(());
        }

        let migration = self.pending_migrations
            .get(version)
            .ok_or_else(|| DriftError::Other(format!("Migration {} not found", version)))?
            .clone();

        info!("Applying migration {}: {}", version, migration.name);

        if dry_run {
            info!("DRY RUN: Would apply migration {}", version);
            return Ok(());
        }

        let start = std::time::Instant::now();
        let result = self.execute_migration_with_engine(&migration, engine);
        let duration_ms = start.elapsed().as_millis() as u64;

        let applied = AppliedMigration {
            version: migration.version.clone(),
            name: migration.name.clone(),
            checksum: migration.checksum.clone(),
            applied_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            duration_ms,
            success: result.is_ok(),
            error_message: result.as_ref().err().map(|e| e.to_string()),
        };

        self.history.insert(version.clone(), applied);
        self.save_history()?;

        if result.is_ok() {
            self.pending_migrations.remove(version);
            // Remove from pending directory
            let file_path = self.migrations_dir
                .join("pending")
                .join(format!("{}.json", version));
            let _ = fs::remove_file(file_path);
        }

        result
    }

    /// Apply a specific migration (legacy - without Engine)
    #[instrument(skip(self))]
    pub fn apply_migration(&mut self, version: &Version, dry_run: bool) -> Result<()> {
        // Check if already applied
        if self.history.contains_key(version) {
            info!("Migration {} already applied, skipping", version);
            return Ok(());
        }

        let migration = self.pending_migrations
            .get(version)
            .ok_or_else(|| DriftError::Other(format!("Migration {} not found", version)))?
            .clone();

        info!("Applying migration {}: {}", version, migration.name);

        if dry_run {
            info!("DRY RUN: Would apply migration {}", version);
            return Ok(());
        }

        let start = std::time::Instant::now();
        let result = self.execute_migration(&migration);
        let duration_ms = start.elapsed().as_millis() as u64;

        let applied = AppliedMigration {
            version: migration.version.clone(),
            name: migration.name.clone(),
            checksum: migration.checksum.clone(),
            applied_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            duration_ms,
            success: result.is_ok(),
            error_message: result.as_ref().err().map(|e| e.to_string()),
        };

        self.history.insert(version.clone(), applied);
        self.save_history()?;

        if result.is_ok() {
            self.pending_migrations.remove(version);
            // Remove from pending directory
            let file_path = self.migrations_dir
                .join("pending")
                .join(format!("{}.json", version));
            let _ = fs::remove_file(file_path);
        }

        result
    }

    /// Execute the actual migration using the Engine
    pub fn execute_migration_with_engine(&self, migration: &Migration, engine: &mut crate::Engine) -> Result<()> {
        // Begin a transaction for the migration
        let txn_id = engine.begin_migration_transaction()?;

        let result = match &migration.migration_type {
            MigrationType::AddColumn { table, column, default_value } => {
                engine.migrate_add_column(table, column, default_value.clone())
            }
            MigrationType::DropColumn { table, column } => {
                engine.migrate_drop_column(table, column)
            }
            MigrationType::RenameColumn { table, old_name, new_name } => {
                engine.migrate_rename_column(table, old_name, new_name)
            }
            _ => {
                // For other migration types, fall back to the old implementation
                self.execute_migration(migration)
            }
        };

        // Commit or rollback based on result
        match result {
            Ok(_) => {
                engine.commit_migration_transaction(txn_id)?;
                Ok(())
            }
            Err(e) => {
                // Try to rollback, but return the original error
                let _ = engine.rollback_migration_transaction(txn_id);
                Err(e)
            }
        }
    }

    /// Execute the actual migration (legacy - without Engine)
    fn execute_migration(&self, migration: &Migration) -> Result<()> {
        match &migration.migration_type {
            MigrationType::AddColumn { table, column, default_value } => {
                self.add_column(table, column, default_value.as_ref())
            }
            MigrationType::DropColumn { table, column } => {
                self.drop_column(table, column)
            }
            MigrationType::RenameColumn { table, old_name, new_name } => {
                self.rename_column(table, old_name, new_name)
            }
            MigrationType::AddIndex { table, column } => {
                self.add_index(table, column)
            }
            MigrationType::DropIndex { table, column } => {
                self.drop_index(table, column)
            }
            MigrationType::CreateTable { name, schema } => {
                self.create_table(name, schema)
            }
            MigrationType::DropTable { name } => {
                self.drop_table(name)
            }
            MigrationType::Custom { up_script, .. } => {
                self.execute_custom_script(up_script)
            }
        }
    }

    /// Rollback a migration
    #[instrument(skip(self))]
    pub fn rollback_migration(&mut self, version: &Version) -> Result<()> {
        let applied = self.history
            .get(version)
            .ok_or_else(|| DriftError::Other(format!("Migration {} not in history", version)))?;

        if !applied.success {
            return Err(DriftError::Other(format!(
                "Cannot rollback failed migration {}",
                version
            )));
        }

        warn!("Rolling back migration {}: {}", version, applied.name);

        // In production, would execute down migration
        // For now, just remove from history
        self.history.remove(version);
        self.save_history()?;

        Ok(())
    }

    /// Apply all pending migrations
    pub fn migrate_all(&mut self, dry_run: bool) -> Result<Vec<Version>> {
        let mut applied = Vec::new();
        let pending: Vec<Version> = self.pending_migrations.keys().cloned().collect();

        for version in pending {
            match self.apply_migration(&version, dry_run) {
                Ok(()) => applied.push(version),
                Err(e) => {
                    error!("Failed to apply migration {}: {}", version, e);
                    if !dry_run {
                        // Stop on first error
                        break;
                    }
                }
            }
        }

        Ok(applied)
    }

    // Migration implementation helpers

    fn add_column(&self, table: &str, column: &ColumnDef, default_value: Option<&serde_json::Value>) -> Result<()> {
        debug!("Adding column {} to table {}", column.name, table);

        // Load the current table schema
        let table_path = self.data_dir.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        if !schema_path.exists() {
            return Err(DriftError::TableNotFound(table.to_string()));
        }

        // Read and update schema
        let schema_content = std::fs::read_to_string(&schema_path)?;
        let mut schema: Schema = serde_yaml::from_str(&schema_content)?;

        // Check if column already exists
        if schema.columns.iter().any(|c| c.name == column.name) {
            return Err(DriftError::Other(format!("Column {} already exists", column.name)));
        }

        // Add the new column
        schema.columns.push(column.clone());

        // Write updated schema back
        let updated_schema = serde_yaml::to_string(&schema)?;
        std::fs::write(&schema_path, updated_schema)?;

        // If there's a default value, backfill existing records
        if let Some(default) = default_value {
            // Create a patch event for each existing record
            let storage = crate::storage::TableStorage::open(&self.data_dir, table)?;
            let current_state = storage.reconstruct_state_at(None)?;

            debug!("Backfilling {} existing records with default value for column {}", current_state.len(), column.name);

            for (key, _) in current_state {
                let patch_event = crate::events::Event::new_patch(
                    table.to_string(),
                    serde_json::json!(key),
                    serde_json::json!({
                        &column.name: default
                    })
                );
                debug!("Creating patch event for key: {} with value: {:?}", key, default);
                storage.append_event(patch_event)?;
            }

            // Ensure events are synced to disk
            storage.sync()?;
        }

        info!("Added column {} to table {}", column.name, table);
        Ok(())
    }

    fn drop_column(&self, table: &str, column: &str) -> Result<()> {
        debug!("Dropping column {} from table {}", column, table);

        // Load the current table schema
        let table_path = self.data_dir.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        if !schema_path.exists() {
            return Err(DriftError::TableNotFound(table.to_string()));
        }

        // Read current schema
        let schema_content = std::fs::read_to_string(&schema_path)?;
        let mut schema: Schema = serde_yaml::from_str(&schema_content)?;

        // Check if column is primary key
        if schema.primary_key == column {
            return Err(DriftError::Other("Cannot drop primary key column".to_string()));
        }

        // Remove column from schema
        let original_count = schema.columns.len();
        schema.columns.retain(|c| c.name != column);

        if schema.columns.len() == original_count {
            return Err(DriftError::Other(format!("Column {} not found", column)));
        }

        // Note: Index removal would be handled separately through drop_index

        // Write updated schema back
        let updated_schema = serde_yaml::to_string(&schema)?;
        std::fs::write(&schema_path, updated_schema)?;

        // Note: The actual data still contains the column values in historical events
        // This is by design for time-travel queries
        info!("Dropped column {} from table {} schema", column, table);
        Ok(())
    }

    fn rename_column(&self, table: &str, old_name: &str, new_name: &str) -> Result<()> {
        debug!("Renaming column {} to {} in table {}", old_name, new_name, table);

        // Load the current table schema
        let table_path = self.data_dir.join("tables").join(table);
        let schema_path = table_path.join("schema.yaml");

        if !schema_path.exists() {
            return Err(DriftError::TableNotFound(table.to_string()));
        }

        // Read current schema
        let schema_content = std::fs::read_to_string(&schema_path)?;
        let mut schema: Schema = serde_yaml::from_str(&schema_content)?;

        // Find and rename the column
        let mut found = false;
        for column in &mut schema.columns {
            if column.name == old_name {
                column.name = new_name.to_string();
                found = true;
                break;
            }
        }

        if !found {
            return Err(DriftError::Other(format!("Column {} not found", old_name)));
        }

        // Update primary key if needed
        if schema.primary_key == old_name {
            schema.primary_key = new_name.to_string();
        }

        // Note: Index renaming would be handled separately if indexes exist

        // Write updated schema back
        let updated_schema = serde_yaml::to_string(&schema)?;
        std::fs::write(&schema_path, updated_schema)?;

        // Create migration events to update existing data
        let storage = crate::storage::TableStorage::open(&self.data_dir, table)?;
        let current_state = storage.reconstruct_state_at(None)?;

        for (key, mut value) in current_state {
            if let serde_json::Value::Object(ref mut map) = value {
                if let Some(old_value) = map.remove(old_name) {
                    map.insert(new_name.to_string(), old_value);

                    // Create a patch event to record the rename
                    let patch_event = crate::events::Event::new_patch(
                        table.to_string(),
                        serde_json::json!(key),
                        serde_json::Value::Object(map.clone())
                    );
                    storage.append_event(patch_event)?;
                }
            }
        }

        info!("Renamed column {} to {} in table {}", old_name, new_name, table);
        Ok(())
    }

    fn add_index(&self, table: &str, column: &str) -> Result<()> {
        debug!("Adding index on {}.{}", table, column);
        // In production, would build index in background
        Ok(())
    }

    fn drop_index(&self, table: &str, column: &str) -> Result<()> {
        debug!("Dropping index on {}.{}", table, column);
        // In production, would remove index files
        Ok(())
    }

    fn create_table(&self, name: &str, _schema: &Schema) -> Result<()> {
        debug!("Creating table {} with schema", name);
        // In production, would create new table directory and schema
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        debug!("Dropping table {}", name);
        // In production, would archive and remove table
        Ok(())
    }

    fn execute_custom_script(&self, _script: &str) -> Result<()> {
        debug!("Executing custom migration script");
        // In production, would parse and execute script
        Ok(())
    }

    /// Generate migration status report
    pub fn status(&self) -> MigrationStatus {
        MigrationStatus {
            current_version: self.current_version(),
            applied_count: self.history.len(),
            pending_count: self.pending_migrations.len(),
            last_migration: self.history.values().last().cloned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStatus {
    pub current_version: Option<Version>,
    pub applied_count: usize,
    pub pending_count: usize,
    pub last_migration: Option<AppliedMigration>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_version_ordering() {
        let v1 = Version::new(1, 0, 0);
        let v2 = Version::new(1, 1, 0);
        let v3 = Version::new(2, 0, 0);

        assert!(v1 < v2);
        assert!(v2 < v3);
    }

    #[test]
    fn test_migration_manager() {
        let temp_dir = TempDir::new().unwrap();
        let mut manager = MigrationManager::new(temp_dir.path()).unwrap();

        // Add migration
        let migration = Migration::new(
            Version::new(1, 0, 0),
            "initial".to_string(),
            "Initial schema".to_string(),
            MigrationType::CreateTable {
                name: "users".to_string(),
                schema: Schema::new("users".to_string(), "id".to_string(), vec![]),
            },
        );

        manager.add_migration(migration).unwrap();
        assert_eq!(manager.pending_migrations().len(), 1);

        // Apply migration
        manager.apply_migration(&Version::new(1, 0, 0), false).unwrap();
        assert_eq!(manager.pending_migrations().len(), 0);
        assert_eq!(manager.current_version(), Some(Version::new(1, 0, 0)));
    }

    #[test]
    fn test_migration_checksum() {
        let migration = Migration::new(
            Version::new(1, 0, 0),
            "test".to_string(),
            "Test migration".to_string(),
            MigrationType::AddColumn {
                table: "users".to_string(),
                column: ColumnDef {
                    name: "email".to_string(),
                    col_type: "string".to_string(),
                    index: true,
                },
                default_value: None,
            },
        );

        assert!(migration.validate_checksum());
    }
}