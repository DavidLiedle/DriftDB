#[cfg(test)]
mod tests {
    use crate::events::Event;
    use crate::schema::Schema;
    use crate::storage::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_table_storage_basic() {
        let temp_dir = TempDir::new().unwrap();

        let schema = Schema {
            name: "test_table".to_string(),
            primary_key: "id".to_string(),
            columns: vec![],
        };

        let _storage = TableStorage::create(temp_dir.path(), schema, None).unwrap();

        // Verify storage was created successfully (no panic)
    }

    #[test]
    fn test_event_creation() {
        let event = Event::new_insert(
            "test_table".to_string(),
            json!("key1"),
            json!({"id": 1, "data": "test"}),
        );

        assert_eq!(event.table_name, "test_table");
        assert_eq!(event.primary_key, json!("key1"));
        assert_eq!(event.payload, json!({"id": 1, "data": "test"}));
    }
}
