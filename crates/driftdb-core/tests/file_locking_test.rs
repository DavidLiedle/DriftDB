use tempfile::TempDir;
use driftdb_core::storage::TableStorage;
use driftdb_core::schema::Schema;

#[test]
fn test_exclusive_table_lock() {
    let temp_dir = TempDir::new().unwrap();

    // Create a table schema
    let schema = Schema {
        name: "test_table".to_string(),
        primary_key: "id".to_string(),
        columns: vec![],
    };

    // First TableStorage should acquire the lock successfully
    let _table1 = TableStorage::create(temp_dir.path(), schema.clone()).unwrap();

    // Second TableStorage should fail to acquire the lock
    let result = TableStorage::open(temp_dir.path(), "test_table");
    assert!(result.is_err());
    let error_msg = format!("{}", result.err().unwrap());
    assert!(error_msg.contains("Failed to acquire table lock"));
}

#[test]
fn test_lock_released_on_drop() {
    let temp_dir = TempDir::new().unwrap();

    // Create a table schema
    let schema = Schema {
        name: "test_table".to_string(),
        primary_key: "id".to_string(),
        columns: vec![],
    };

    // Create and drop first TableStorage
    {
        let _table1 = TableStorage::create(temp_dir.path(), schema.clone()).unwrap();
        // Lock is held here
    }
    // Lock should be released after drop

    // Second TableStorage should now acquire the lock successfully
    let _table2 = TableStorage::open(temp_dir.path(), "test_table").unwrap();
}