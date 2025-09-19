#[cfg(test)]
mod tests {
    use crate::errors::Result;
    use crate::events::{Event, EventType};
    use crate::schema::{ColumnDef, Schema};
    use crate::storage::frame::{Frame, FramedRecord};
    use crate::storage::segment::Segment;
    use serde_json::json;
    use tempfile::TempDir;
    use std::io::{Seek, Write};

    #[test]
    fn test_frame_crc_verification() -> Result<()> {
        let data = b"test data".to_vec();
        let frame = Frame::new(data.clone());

        assert!(frame.verify());
        assert_eq!(frame.data, data);

        let mut corrupted_frame = frame.clone();
        corrupted_frame.data[0] = b'x';
        assert!(!corrupted_frame.verify());

        Ok(())
    }

    #[test]
    fn test_frame_serialization() -> Result<()> {
        let event = Event::new_insert(
            "test_table".to_string(),
            json!("key1"),
            json!({"field": "value"}),
        );

        let record = FramedRecord::from_event(event.clone());
        let frame = record.to_frame()?;

        assert!(frame.verify());

        let restored_record = FramedRecord::from_frame(&frame)?;
        assert_eq!(restored_record.event.table_name, "test_table");
        assert_eq!(restored_record.event.primary_key, json!("key1"));

        Ok(())
    }

    #[test]
    fn test_segment_write_read() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let segment_path = temp_dir.path().join("test.seg");
        let segment = Segment::new(segment_path, 1);

        let mut writer = segment.create()?;

        let event1 = Event::new_insert(
            "orders".to_string(),
            json!("order1"),
            json!({"status": "pending", "amount": 100}),
        );

        let event2 = Event::new_patch(
            "orders".to_string(),
            json!("order1"),
            json!({"status": "paid"}),
        );

        writer.append_event(&event1)?;
        writer.append_event(&event2)?;
        writer.sync()?;
        drop(writer);

        let mut reader = segment.open_reader()?;
        let events = reader.read_all_events()?;

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].primary_key, json!("order1"));
        assert_eq!(events[0].event_type, EventType::Insert);
        assert_eq!(events[1].event_type, EventType::Patch);

        Ok(())
    }

    #[test]
    fn test_segment_corruption_detection() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let segment_path = temp_dir.path().join("corrupt.seg");
        let segment = Segment::new(segment_path.clone(), 1);

        let mut writer = segment.create()?;
        let event = Event::new_insert(
            "test".to_string(),
            json!("key1"),
            json!({"data": "valid"}),
        );
        writer.append_event(&event)?;
        writer.sync()?;
        drop(writer);

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&segment_path)?;
        use std::io::SeekFrom;
        file.seek(SeekFrom::End(-5))?;
        file.write_all(b"CORRUPT")?;
        drop(file);

        let mut reader = segment.open_reader()?;
        let corruption_pos = reader.verify_and_find_corruption()?;
        assert!(corruption_pos.is_some());

        Ok(())
    }

    #[test]
    fn test_schema_validation() -> Result<()> {
        let valid_schema = Schema::new(
            "users".to_string(),
            "id".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: "string".to_string(),
                    index: false,
                },
                ColumnDef {
                    name: "email".to_string(),
                    col_type: "string".to_string(),
                    index: true,
                },
            ],
        );

        assert!(valid_schema.validate().is_ok());
        assert!(valid_schema.has_column("id"));
        assert!(valid_schema.has_column("email"));
        assert!(!valid_schema.has_column("nonexistent"));

        let indexed = valid_schema.indexed_columns();
        assert!(indexed.contains("email"));
        assert!(!indexed.contains("id"));

        let invalid_schema = Schema::new(
            "invalid".to_string(),
            "missing_pk".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: "string".to_string(),
                    index: false,
                },
            ],
        );

        assert!(invalid_schema.validate().is_err());

        Ok(())
    }

    #[test]
    fn test_table_storage_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = Schema::new(
            "products".to_string(),
            "sku".to_string(),
            vec![
                ColumnDef {
                    name: "sku".to_string(),
                    col_type: "string".to_string(),
                    index: false,
                },
                ColumnDef {
                    name: "category".to_string(),
                    col_type: "string".to_string(),
                    index: true,
                },
                ColumnDef {
                    name: "price".to_string(),
                    col_type: "number".to_string(),
                    index: false,
                },
            ],
        );

        let storage = crate::storage::TableStorage::create(temp_dir.path(), schema)?;

        let event1 = Event::new_insert(
            "products".to_string(),
            json!("SKU001"),
            json!({"sku": "SKU001", "category": "electronics", "price": 299.99}),
        );

        let seq1 = storage.append_event(event1)?;
        assert_eq!(seq1, 1);

        let event2 = Event::new_patch(
            "products".to_string(),
            json!("SKU001"),
            json!({"price": 249.99}),
        );

        let seq2 = storage.append_event(event2)?;
        assert_eq!(seq2, 2);

        let event3 = Event::new_soft_delete(
            "products".to_string(),
            json!("SKU001"),
        );

        let seq3 = storage.append_event(event3)?;
        assert_eq!(seq3, 3);

        storage.sync()?;

        let events = storage.read_all_events()?;
        assert_eq!(events.len(), 3);

        let state_at_2 = storage.reconstruct_state_at(Some(2))?;
        assert!(state_at_2.contains_key("\"SKU001\""));
        if let Some(product) = state_at_2.get("\"SKU001\"") {
            assert_eq!(product["price"], json!(249.99));
        }

        let state_at_3 = storage.reconstruct_state_at(Some(3))?;
        assert!(!state_at_3.contains_key("\"SKU001\""));

        Ok(())
    }

    #[test]
    fn test_index_operations() -> Result<()> {
        use crate::index::Index;

        let mut index = Index::new("status".to_string());

        index.insert(&json!("pending"), "order1");
        index.insert(&json!("pending"), "order2");
        index.insert(&json!("paid"), "order3");

        let pending_orders = index.find("pending");
        assert!(pending_orders.is_some());
        assert_eq!(pending_orders.unwrap().len(), 2);

        let paid_orders = index.find("paid");
        assert!(paid_orders.is_some());
        assert_eq!(paid_orders.unwrap().len(), 1);

        index.remove(&json!("pending"), "order1");
        let pending_orders = index.find("pending");
        assert_eq!(pending_orders.unwrap().len(), 1);

        Ok(())
    }

    #[test]
    fn test_snapshot_creation_and_loading() -> Result<()> {
        use crate::snapshot::Snapshot;
        use std::collections::HashMap;

        let mut state = HashMap::new();
        state.insert("key1".to_string(), r#"{"field":"value1"}"#.to_string());
        state.insert("key2".to_string(), r#"{"field":"value2"}"#.to_string());

        let snapshot = Snapshot {
            sequence: 100,
            timestamp_ms: 1234567890,
            row_count: 2,
            state: state.clone(),
        };

        let temp_dir = TempDir::new()?;
        let snap_path = temp_dir.path().join("test.snap");

        snapshot.save_to_file(&snap_path)?;
        assert!(snap_path.exists());

        let loaded = Snapshot::load_from_file(&snap_path)?;
        assert_eq!(loaded.sequence, 100);
        assert_eq!(loaded.row_count, 2);
        assert_eq!(loaded.state.len(), 2);

        Ok(())
    }

    #[test]
    fn test_driftql_parser() -> Result<()> {
        use crate::query::{parse_driftql, Query};

        let create_query = parse_driftql("CREATE TABLE users (pk=id, INDEX(email, status))")?;
        if let Query::CreateTable { name, primary_key, indexed_columns } = create_query {
            assert_eq!(name, "users");
            assert_eq!(primary_key, "id");
            assert_eq!(indexed_columns, vec!["email", "status"]);
        } else {
            panic!("Expected CreateTable query");
        }

        let insert_query = parse_driftql(r#"INSERT INTO users {"id": "user1", "email": "test@example.com"}"#)?;
        if let Query::Insert { table, data } = insert_query {
            assert_eq!(table, "users");
            assert_eq!(data["id"], "user1");
        } else {
            panic!("Expected Insert query");
        }

        let select_query = parse_driftql("SELECT * FROM users WHERE status=\"active\" LIMIT 10")?;
        if let Query::Select { table, conditions, limit, .. } = select_query {
            assert_eq!(table, "users");
            assert_eq!(conditions.len(), 1);
            assert_eq!(conditions[0].column, "status");
            assert_eq!(limit, Some(10));
        } else {
            panic!("Expected Select query");
        }

        Ok(())
    }

    #[test]
    fn test_engine_end_to_end() -> Result<()> {
        use crate::Engine;

        let temp_dir = TempDir::new()?;
        let mut engine = Engine::init(temp_dir.path())?;

        engine.create_table("orders", "id", vec!["status".to_string()])?;

        let event1 = Event::new_insert(
            "orders".to_string(),
            json!("ORD001"),
            json!({"id": "ORD001", "status": "pending", "amount": 100}),
        );

        let seq1 = engine.apply_event(event1)?;
        assert_eq!(seq1, 1);

        let event2 = Event::new_patch(
            "orders".to_string(),
            json!("ORD001"),
            json!({"status": "paid"}),
        );

        let seq2 = engine.apply_event(event2)?;
        assert_eq!(seq2, 2);

        engine.create_snapshot("orders")?;

        let report = engine.doctor()?;
        assert!(!report.is_empty());

        Ok(())
    }
}


#[test]
fn test_frame_roundtrip() {
    use crate::storage::frame::Frame;
    use std::io::Cursor;

    let data = b"test data for frame".to_vec();
    let frame = Frame::new(data.clone());

    let mut buffer = Vec::new();
    frame.write_to(&mut buffer).unwrap();

    let mut cursor = Cursor::new(buffer);
    let restored = Frame::read_from(&mut cursor).unwrap().unwrap();

    assert_eq!(frame.data, restored.data);
    assert_eq!(frame.crc32, restored.crc32);
    assert!(restored.verify());
}