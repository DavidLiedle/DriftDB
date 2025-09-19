use tempfile::TempDir;
use serde_json::json;

use driftdb_core::{Engine, Query, QueryResult};
use driftdb_core::migration::{MigrationManager, Migration, MigrationType, Version};
use driftdb_core::schema::ColumnDef;

#[test]
fn test_migration_rollback_on_error() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create initial table
    engine.execute_query(Query::CreateTable {
        name: "users".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Insert initial data
    engine.execute_query(Query::Insert {
        table: "users".to_string(),
        data: json!({
            "id": "user1",
            "name": "Alice"
        }),
    }).unwrap();

    // Create a migration that will fail (trying to rename a non-existent column)
    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();
    let migration = Migration::new(
        Version::new(1, 0, 0),
        "rename_nonexistent_column".to_string(),
        "Try to rename a column that doesn't exist".to_string(),
        MigrationType::RenameColumn {
            table: "users".to_string(),
            old_name: "nonexistent_column".to_string(),
            new_name: "new_column".to_string(),
        },
    );

    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();

    // The migration should fail
    let result = migration_mgr.apply_migration_with_engine(&version, &mut engine, false);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));

    // Verify that the data is still intact after the failed migration
    let result = engine.execute_query(Query::Select {
        table: "users".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["id"], json!("user1"));
            assert_eq!(data[0]["name"], json!("Alice"));
        }
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_multiple_migration_steps_atomicity() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create initial table
    engine.execute_query(Query::CreateTable {
        name: "products".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Insert test data
    for i in 1..=5 {
        engine.execute_query(Query::Insert {
            table: "products".to_string(),
            data: json!({
                "id": format!("prod{}", i),
                "name": format!("Product {}", i),
                "price": i * 10
            }),
        }).unwrap();
    }

    // Create a migration that adds a column with default value
    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();
    let migration = Migration::new(
        Version::new(1, 0, 0),
        "add_discount_column".to_string(),
        "Add discount column to products table".to_string(),
        MigrationType::AddColumn {
            table: "products".to_string(),
            column: ColumnDef {
                name: "discount".to_string(),
                col_type: "number".to_string(),
                index: false,
            },
            default_value: Some(json!(0)),
        },
    );

    // Apply the migration
    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();
    let result = migration_mgr.apply_migration_with_engine(&version, &mut engine, false);
    assert!(result.is_ok());

    // Verify all records have the new column
    let result = engine.execute_query(Query::Select {
        table: "products".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 5);
            for record in data {
                assert_eq!(record["discount"], json!(0));
            }
        }
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_migration_transaction_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let mut engine = Engine::init(temp_dir.path()).unwrap();

    // Create initial table
    engine.execute_query(Query::CreateTable {
        name: "inventory".to_string(),
        primary_key: "id".to_string(),
        indexed_columns: vec![],
    }).unwrap();

    // Insert test data
    engine.execute_query(Query::Insert {
        table: "inventory".to_string(),
        data: json!({
            "id": "item1",
            "name": "Widget",
            "stock": 100
        }),
    }).unwrap();

    // Start a migration to add a column
    let migration_mgr = MigrationManager::new(temp_dir.path()).unwrap();
    let migration = Migration::new(
        Version::new(1, 0, 0),
        "add_location_column".to_string(),
        "Add location column to inventory table".to_string(),
        MigrationType::AddColumn {
            table: "inventory".to_string(),
            column: ColumnDef {
                name: "location".to_string(),
                col_type: "string".to_string(),
                index: false,
            },
            default_value: Some(json!("warehouse-1")),
        },
    );

    let mut migration_mgr = migration_mgr;
    let version = migration.version.clone();
    migration_mgr.add_migration(migration).unwrap();

    // Apply the migration
    let result = migration_mgr.apply_migration_with_engine(&version, &mut engine, false);
    assert!(result.is_ok());

    // Verify the migration was applied successfully
    let result = engine.execute_query(Query::Select {
        table: "inventory".to_string(),
        conditions: vec![],
        as_of: None,
        limit: None,
    }).unwrap();

    match result {
        QueryResult::Rows { data } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0]["location"], json!("warehouse-1"));
        }
        _ => panic!("Expected rows"),
    }
}