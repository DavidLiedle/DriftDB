#[cfg(test)]
mod tests {
    use crate::events::{Event, EventType};
    use crate::schema::Schema;
    use crate::storage::*;
    use serde_json::json;
    use std::time::SystemTime;
    use tempfile::TempDir;

    #[test]
    fn test_segment_basic() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("test.seg");

        let mut segment = Segment::create(&segment_path).unwrap();
        assert_eq!(segment.entry_count(), 0);

        let event = Event {
            sequence: 1,
            timestamp: SystemTime::now(),
            event_type: EventType::Insert,
            key: "key1".to_string(),
            payload: json!({"id": 1, "data": "test"}),
        };

        segment.append(&event).unwrap();
        assert_eq!(segment.entry_count(), 1);
    }

    #[test]
    fn test_table_storage_basic() {
        let temp_dir = TempDir::new().unwrap();

        let schema = Schema {
            name: "test_table".to_string(),
            primary_key: "id".to_string(),
            columns: vec![],
        };

        let mut storage = TableStorage::create(temp_dir.path(), "test_table", schema).unwrap();
        assert_eq!(storage.table_name(), "test_table");

        let event = Event {
            sequence: 1,
            timestamp: SystemTime::now(),
            event_type: EventType::Insert,
            key: "key1".to_string(),
            payload: json!({"id": 1, "data": "test"}),
        };

        storage.append(event).unwrap();
    }
}
