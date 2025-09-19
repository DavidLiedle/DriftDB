use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use fs2::FileExt;

use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::schema::Schema;
use crate::storage::{Segment, SegmentWriter, TableMeta};

pub struct TableStorage {
    path: PathBuf,
    schema: Schema,
    meta: Arc<RwLock<TableMeta>>,
    current_writer: Arc<RwLock<Option<SegmentWriter>>>,
    _lock_file: Option<fs::File>,
}

impl TableStorage {
    pub fn create<P: AsRef<Path>>(base_path: P, schema: Schema) -> Result<Self> {
        let path = base_path.as_ref().join("tables").join(&schema.name);
        fs::create_dir_all(&path)?;

        // Acquire exclusive lock on the table
        let lock_path = path.join(".lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file.try_lock_exclusive()
            .map_err(|e| DriftError::Other(format!("Failed to acquire table lock: {}", e)))?;

        let schema_path = path.join("schema.yaml");
        schema.save_to_file(&schema_path)?;

        let meta = TableMeta::default();
        let meta_path = path.join("meta.json");
        meta.save_to_file(&meta_path)?;

        fs::create_dir_all(path.join("segments"))?;
        fs::create_dir_all(path.join("snapshots"))?;
        fs::create_dir_all(path.join("indexes"))?;

        let segment = Segment::new(path.join("segments").join("00000001.seg"), 1);
        let writer = segment.create()?;

        Ok(Self {
            path,
            schema,
            meta: Arc::new(RwLock::new(meta)),
            current_writer: Arc::new(RwLock::new(Some(writer))),
            _lock_file: Some(lock_file),
        })
    }

    pub fn open<P: AsRef<Path>>(base_path: P, table_name: &str) -> Result<Self> {
        let path = base_path.as_ref().join("tables").join(table_name);

        if !path.exists() {
            return Err(DriftError::TableNotFound(table_name.to_string()));
        }

        // Acquire exclusive lock on the table
        let lock_path = path.join(".lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file.try_lock_exclusive()
            .map_err(|e| DriftError::Other(format!("Failed to acquire table lock: {}", e)))?;

        let schema = Schema::load_from_file(path.join("schema.yaml"))?;
        let meta = TableMeta::load_from_file(path.join("meta.json"))?;

        let segment_id = meta.segment_count;
        let segment_path = path.join("segments").join(format!("{:08}.seg", segment_id));
        let segment = Segment::new(segment_path, segment_id);

        let writer = if segment.exists() {
            segment.open_writer()?
        } else {
            segment.create()?
        };

        Ok(Self {
            path,
            schema,
            meta: Arc::new(RwLock::new(meta)),
            current_writer: Arc::new(RwLock::new(Some(writer))),
            _lock_file: Some(lock_file),
        })
    }

    pub fn append_event(&self, mut event: Event) -> Result<u64> {
        let mut meta = self.meta.write();
        let mut writer_guard = self.current_writer.write();

        meta.last_sequence += 1;
        event.sequence = meta.last_sequence;

        if let Some(writer) = writer_guard.as_mut() {
            let bytes_written = writer.append_event(&event)?;

            // Always sync after writing to ensure data is persisted
            writer.sync()?;

            if bytes_written > self.segment_rotation_threshold() {
                meta.segment_count += 1;
                let new_segment_path = self.path.join("segments")
                    .join(format!("{:08}.seg", meta.segment_count));
                let new_segment = Segment::new(new_segment_path, meta.segment_count);
                *writer_guard = Some(new_segment.create()?);
            }
        } else {
            return Err(DriftError::Other("No writer available".into()));
        }

        meta.save_to_file(self.path.join("meta.json"))?;
        Ok(event.sequence)
    }

    pub fn flush(&self) -> Result<()> {
        if let Some(writer) = self.current_writer.write().as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        if let Some(writer) = self.current_writer.write().as_mut() {
            writer.sync()?;
        }
        Ok(())
    }

    pub fn read_all_events(&self) -> Result<Vec<Event>> {
        let mut all_events = Vec::new();
        let segments_dir = self.path.join("segments");

        let mut segment_files: Vec<_> = fs::read_dir(&segments_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "seg")
                    .unwrap_or(false)
            })
            .collect();

        segment_files.sort_by_key(|entry| entry.path());

        for entry in segment_files {
            let segment = Segment::new(entry.path(), 0);
            let mut reader = segment.open_reader()?;
            all_events.extend(reader.read_all_events()?);
        }

        Ok(all_events)
    }

    pub fn reconstruct_state_at(&self, sequence: Option<u64>) -> Result<HashMap<String, serde_json::Value>> {
        let events = self.read_all_events()?;
        let mut state = HashMap::new();

        for event in events {
            if let Some(seq) = sequence {
                if event.sequence > seq {
                    break;
                }
            }

            match event.event_type {
                crate::events::EventType::Insert => {
                    state.insert(event.primary_key.to_string(), event.payload);
                }
                crate::events::EventType::Patch => {
                    if let Some(existing) = state.get_mut(&event.primary_key.to_string()) {
                        if let (serde_json::Value::Object(existing_map), serde_json::Value::Object(patch_map)) =
                            (existing, &event.payload) {
                            for (key, value) in patch_map {
                                existing_map.insert(key.clone(), value.clone());
                            }
                        }
                    }
                }
                crate::events::EventType::SoftDelete => {
                    state.remove(&event.primary_key.to_string());
                }
            }
        }

        Ok(state)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn segment_rotation_threshold(&self) -> u64 {
        10 * 1024 * 1024
    }
}

impl Drop for TableStorage {
    fn drop(&mut self) {
        // The lock file will be automatically unlocked when dropped
        // But we can be explicit about it
        if let Some(ref lock_file) = self._lock_file {
            let _ = fs2::FileExt::unlock(lock_file);
        }
    }
}