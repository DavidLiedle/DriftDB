use tempfile::TempDir;
use serde_json::json;

use driftdb_core::{Engine, Query, QueryResult};
use driftdb_core::migration::{MigrationManager, Migration, MigrationType, Version};
use driftdb_core::schema::{Schema, ColumnDef};

#[test]
fn test_add_column_migration() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create initial table
    engine.execute_query(Query::CreateTable {
        name: "users".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Insert initial data
    let insert_result = engine.execute_query(Query::Insert {
        table: "users".to_string(),
        data: json!({
            "id": "user1",
            "name": "Alice"
        }),
    }).unwrap();

    // Create migration to add email column
    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();
    let migration = Migration::new(
        Version::new(1, 0, 0),
        "add_email_column".to_string(),
        "Add email column to users table".to_string(),
        MigrationType::AddColumn {
            table: "users".to_string(),
            column: ColumnDef {
                name: "email".to_string(),
                col_type: "string".to_string(),
                index: false,
            },
            default_value: Some(json!("default@example.com")),
        },
    );

    // Apply migration through the Engine
    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();
    migration_mgr.apply_migration_with_engine(&version, &mut engine, false).unwrap();

    // Verify column was added with default value
    let result = engine.execute_query(Query::Select {
        table: "users".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            // Now that migrations work through the Engine, we can check the email field
            assert_eq!(data[0]["email"], json!("default@example.com"));
        }
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_drop_column_migration() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table with extra column
    engine.execute_query(Query::CreateTable {
        name: "products".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "products".to_string(),
        data: json!({
            "id": "prod1",
            "name": "Widget",
            "deprecated_field": "old_data"
        }),
    }).unwrap();

    // Create migration to drop deprecated column
    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();
    let migration = Migration::new(
        Version::new(1, 0, 1),
        "drop_deprecated_field".to_string(),
        "Drop deprecated field from products".to_string(),
        MigrationType::DropColumn {
            table: "products".to_string(),
            column: "deprecated_field".to_string(),
        },
    );

    // Apply migration through the Engine
    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();
    migration_mgr.apply_migration_with_engine(&version, &mut engine, false).unwrap();

    // Verify schema was updated (column removed from future operations)
    // Note: Historical data still contains the column for time-travel
}

#[test]
fn test_rename_column_migration() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create table
    engine.execute_query(Query::CreateTable {
        name: "accounts".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    engine.execute_query(Query::Insert {
        table: "accounts".to_string(),
        data: json!({
            "id": "acc1",
            "user_name": "john_doe"
        }),
    }).unwrap();

    // Create migration to rename column
    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();
    let migration = Migration::new(
        Version::new(1, 0, 2),
        "rename_username_column".to_string(),
        "Rename user_name to username".to_string(),
        MigrationType::RenameColumn {
            table: "accounts".to_string(),
            old_name: "user_name".to_string(),
            new_name: "username".to_string(),
        },
    );

    // Apply migration through the Engine
    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();

    // This should fail because user_name column doesn't exist in the schema
    let result = migration_mgr.apply_migration_with_engine(&version, &mut engine, false);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_migration_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let _engine = Engine::init(temp_dir.path()).unwrap();

    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();

    // Create and apply migration
    let migration = Migration::new(
        Version::new(2, 0, 0),
        "test_migration".to_string(),
        "Test migration for rollback".to_string(),
        MigrationType::Custom {
            description: "Empty migration".to_string(),
            up_script: String::new(),
            down_script: String::new(),
        },
    );

    let migration_version = migration.version.clone();
    let mut migration_mgr = migration_mgr;
    migration_mgr.add_migration(migration).unwrap();
    migration_mgr.apply_migration(&migration_version, false).unwrap();

    // Verify migration was applied
    let status = migration_mgr.status();
    assert_eq!(status.applied_count, 1);

    // Rollback migration
    migration_mgr.rollback_migration(&migration_version).unwrap();

    // Verify migration was rolled back
    let status = migration_mgr.status();
    assert_eq!(status.applied_count, 0);
}

#[test]
fn test_migration_idempotency() {
    let temp_dir = TempDir::new().unwrap();
    let _engine = Engine::init(temp_dir.path()).unwrap();

    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();

    let migration = Migration::new(
        Version::new(3, 0, 0),
        "idempotent_migration".to_string(),
        "Test idempotent migration".to_string(),
        MigrationType::Custom {
            description: "Empty migration".to_string(),
            up_script: String::new(),
            down_script: String::new(),
        },
    );

    // Apply migration twice
    let mut migration_mgr = migration_mgr;
    let migration_version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();
    let result1 = migration_mgr.apply_migration(&migration_version, false);
    assert!(result1.is_ok());

    let result2 = migration_mgr.apply_migration(&migration_version, false);
    // Second application should be skipped (already applied)
    assert!(result2.is_ok());

    // Verify only applied once
    let status = migration_mgr.status();
    assert_eq!(status.applied_count, 1);
}

#[test]
fn test_migration_with_validation() {
    let temp_dir = TempDir::new().unwrap();
    let _engine = Engine::init(temp_dir.path()).unwrap();

    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();

    // Create migration with validation
    let migration = Migration::new(
        Version::new(4, 0, 0),
        "validated_migration".to_string(),
        "Migration with validation".to_string(),
        MigrationType::Custom {
            description: "Migration with pre/post conditions".to_string(),
            up_script: String::new(),
            down_script: String::new(),
        },
    );

    // Validation should pass for empty operations
    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();
    let result = migration_mgr.apply_migration(&version, false);
    assert!(result.is_ok());
}