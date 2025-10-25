use fs2::FileExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::encryption::EncryptionService;
use crate::errors::{DriftError, Result};
use crate::events::Event;
use crate::schema::Schema;
use crate::storage::{Segment, SegmentWriter, TableMeta};

#[derive(Debug, Clone)]
pub struct TableStats {
    pub sequence_count: u64,
    pub segment_count: u64,
    pub snapshot_count: u64,
}

pub struct TableStorage {
    path: PathBuf,
    schema: Schema,
    meta: Arc<RwLock<TableMeta>>,
    current_writer: Arc<RwLock<Option<SegmentWriter>>>,
    encryption_service: Option<Arc<EncryptionService>>,
    _lock_file: Option<fs::File>,
}

impl TableStorage {
    pub fn create<P: AsRef<Path>>(
        base_path: P,
        schema: Schema,
        encryption_service: Option<Arc<EncryptionService>>,
    ) -> Result<Self> {
        let path = base_path.as_ref().join("tables").join(&schema.name);
        fs::create_dir_all(&path)?;

        // Acquire exclusive lock on the table
        let lock_path = path.join(".lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|e| DriftError::Other(format!("Failed to acquire table lock: {}", e)))?;

        let schema_path = path.join("schema.yaml");
        schema.save_to_file(&schema_path)?;

        let meta = TableMeta::default();
        let meta_path = path.join("meta.json");
        meta.save_to_file(&meta_path)?;

        fs::create_dir_all(path.join("segments"))?;
        fs::create_dir_all(path.join("snapshots"))?;
        fs::create_dir_all(path.join("indexes"))?;

        let segment = if let Some(ref encryption_service) = encryption_service {
            Segment::new_with_encryption(
                path.join("segments").join("00000001.seg"),
                1,
                encryption_service.clone(),
            )
        } else {
            Segment::new(path.join("segments").join("00000001.seg"), 1)
        };
        let writer = segment.create()?;

        Ok(Self {
            path,
            schema,
            meta: Arc::new(RwLock::new(meta)),
            current_writer: Arc::new(RwLock::new(Some(writer))),
            encryption_service,
            _lock_file: Some(lock_file),
        })
    }

    pub fn open<P: AsRef<Path>>(
        base_path: P,
        table_name: &str,
        encryption_service: Option<Arc<EncryptionService>>,
    ) -> Result<Self> {
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
        lock_file
            .try_lock_exclusive()
            .map_err(|e| DriftError::Other(format!("Failed to acquire table lock: {}", e)))?;

        let schema = Schema::load_from_file(path.join("schema.yaml"))?;
        let meta = TableMeta::load_from_file(path.join("meta.json"))?;

        let segment_id = meta.segment_count;
        let segment_path = path.join("segments").join(format!("{:08}.seg", segment_id));
        let segment = if let Some(ref encryption_service) = encryption_service {
            Segment::new_with_encryption(segment_path, segment_id, encryption_service.clone())
        } else {
            Segment::new(segment_path, segment_id)
        };

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
            encryption_service,
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
                let new_segment_path = self
                    .path
                    .join("segments")
                    .join(format!("{:08}.seg", meta.segment_count));
                let new_segment = if let Some(ref encryption_service) = self.encryption_service {
                    Segment::new_with_encryption(
                        new_segment_path,
                        meta.segment_count,
                        encryption_service.clone(),
                    )
                } else {
                    Segment::new(new_segment_path, meta.segment_count)
                };
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
        self.read_events_with_limit(None)
    }

    /// Read events with an optional limit to prevent unbounded memory usage
    /// This is the safe version that should be used in production
    pub fn read_events_with_limit(&self, max_events: Option<usize>) -> Result<Vec<Event>> {
        const DEFAULT_MAX_EVENTS: usize = 1_000_000; // 1M events max by default
        let limit = max_events.unwrap_or(DEFAULT_MAX_EVENTS);

        let mut all_events = Vec::new();
        let segments_dir = self.path.join("segments");

        let mut segment_files: Vec<_> = fs::read_dir(&segments_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "seg")
                    .unwrap_or(false)
            })
            .collect();

        segment_files.sort_by_key(|entry| entry.path());

        for entry in segment_files {
            if all_events.len() >= limit {
                tracing::warn!(
                    "Event limit reached ({} events). Consider using snapshots or pagination.",
                    limit
                );
                return Err(DriftError::Other(format!(
                    "Event limit exceeded: {} events. Use snapshots or reduce query scope.",
                    limit
                )));
            }

            let segment = if let Some(ref encryption_service) = self.encryption_service {
                Segment::new_with_encryption(entry.path(), 0, encryption_service.clone())
            } else {
                Segment::new(entry.path(), 0)
            };
            let mut reader = segment.open_reader()?;
            let segment_events = reader.read_all_events()?;

            // Check if adding these events would exceed the limit
            if all_events.len() + segment_events.len() > limit {
                let remaining = limit - all_events.len();
                all_events.extend(segment_events.into_iter().take(remaining));
                tracing::warn!(
                    "Event limit reached ({} events). Truncating results.",
                    limit
                );
                return Err(DriftError::Other(format!(
                    "Event limit exceeded: {} events. Use snapshots or reduce query scope.",
                    limit
                )));
            }

            all_events.extend(segment_events);
        }

        Ok(all_events)
    }

    pub fn reconstruct_state_at(
        &self,
        sequence: Option<u64>,
    ) -> Result<HashMap<String, serde_json::Value>> {
        let target_seq = sequence.unwrap_or(u64::MAX);

        // OPTIMIZATION: Try to use a snapshot first
        let snapshot_manager = crate::snapshot::SnapshotManager::new(&self.path);
        if let Ok(Some(snapshot)) = snapshot_manager.find_latest_before(target_seq) {
            // We have a snapshot before our target sequence!
            // Convert snapshot state from HashMap<String, String> to HashMap<String, serde_json::Value>
            let mut state: HashMap<String, serde_json::Value> = snapshot
                .state
                .into_iter()
                .filter_map(|(k, v)| serde_json::from_str(&v).ok().map(|json_val| (k, json_val)))
                .collect();

            // Only read events AFTER the snapshot
            let events = self.read_events_after_sequence(snapshot.sequence)?;

            for event in events {
                if event.sequence > target_seq {
                    break;
                }

                match event.event_type {
                    crate::events::EventType::Insert => {
                        state.insert(event.primary_key.to_string(), event.payload);
                    }
                    crate::events::EventType::Patch => {
                        if let Some(existing) = state.get_mut(&event.primary_key.to_string()) {
                            if let (
                                serde_json::Value::Object(existing_map),
                                serde_json::Value::Object(patch_map),
                            ) = (existing, &event.payload)
                            {
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

            return Ok(state);
        }

        // Fallback: No snapshot available, do full replay (existing logic)
        let events = self.read_all_events()?;
        let mut state = HashMap::new();

        for event in events {
            if event.sequence > target_seq {
                break;
            }

            match event.event_type {
                crate::events::EventType::Insert => {
                    state.insert(event.primary_key.to_string(), event.payload);
                }
                crate::events::EventType::Patch => {
                    if let Some(existing) = state.get_mut(&event.primary_key.to_string()) {
                        if let (
                            serde_json::Value::Object(existing_map),
                            serde_json::Value::Object(patch_map),
                        ) = (existing, &event.payload)
                        {
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

    /// Read only events after a specific sequence number
    pub fn read_events_after_sequence(&self, after_seq: u64) -> Result<Vec<Event>> {
        // For now, just filter from all events
        // TODO: Optimize this to only read relevant segments
        let all_events = self.read_all_events()?;

        let filtered_events: Vec<Event> = all_events
            .into_iter()
            .filter(|e| e.sequence > after_seq)
            .collect();

        Ok(filtered_events)
    }

    pub fn find_sequence_at_timestamp(
        &self,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<u64>> {
        // Find the sequence number that corresponds to a given timestamp
        let events = self.read_all_events()?;

        // Find the latest event before or at the timestamp
        let mut latest_seq = None;
        for event in events {
            // Convert event timestamp to chrono
            let event_ts = chrono::DateTime::from_timestamp(event.timestamp.unix_timestamp(), 0)
                .unwrap_or(chrono::Utc::now());

            if event_ts <= timestamp {
                latest_seq = Some(event.sequence);
            } else {
                break;
            }
        }

        Ok(latest_seq)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Calculate the total size of all table files in bytes
    pub fn calculate_size_bytes(&self) -> Result<u64> {
        let mut total_size = 0u64;

        // Calculate segments size
        let segments_dir = self.path.join("segments");
        if segments_dir.exists() {
            for entry in fs::read_dir(&segments_dir)? {
                let entry = entry?;
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        // Calculate snapshots size
        let snapshots_dir = self.path.join("snapshots");
        if snapshots_dir.exists() {
            for entry in fs::read_dir(&snapshots_dir)? {
                let entry = entry?;
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        // Calculate indexes size
        let indexes_dir = self.path.join("indexes");
        if indexes_dir.exists() {
            for entry in fs::read_dir(&indexes_dir)? {
                let entry = entry?;
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        // Add schema and meta files
        if let Ok(metadata) = fs::metadata(self.path.join("schema.yaml")) {
            total_size += metadata.len();
        }
        if let Ok(metadata) = fs::metadata(self.path.join("meta.json")) {
            total_size += metadata.len();
        }

        Ok(total_size)
    }

    /// Get breakdown of table storage by component
    pub fn get_storage_breakdown(&self) -> Result<HashMap<String, u64>> {
        let mut breakdown = HashMap::new();

        // Calculate segments size
        let segments_dir = self.path.join("segments");
        let mut segments_size = 0u64;
        if segments_dir.exists() {
            for entry in fs::read_dir(&segments_dir)? {
                let entry = entry?;
                if let Ok(metadata) = entry.metadata() {
                    segments_size += metadata.len();
                }
            }
        }
        breakdown.insert("segments".to_string(), segments_size);

        // Calculate snapshots size
        let snapshots_dir = self.path.join("snapshots");
        let mut snapshots_size = 0u64;
        if snapshots_dir.exists() {
            for entry in fs::read_dir(&snapshots_dir)? {
                let entry = entry?;
                if let Ok(metadata) = entry.metadata() {
                    snapshots_size += metadata.len();
                }
            }
        }
        breakdown.insert("snapshots".to_string(), snapshots_size);

        // Calculate indexes size
        let indexes_dir = self.path.join("indexes");
        let mut indexes_size = 0u64;
        if indexes_dir.exists() {
            for entry in fs::read_dir(&indexes_dir)? {
                let entry = entry?;
                if let Ok(metadata) = entry.metadata() {
                    indexes_size += metadata.len();
                }
            }
        }
        breakdown.insert("indexes".to_string(), indexes_size);

        Ok(breakdown)
    }

    /// Get metadata about the table
    pub fn get_table_stats(&self) -> TableStats {
        let meta = self.meta.read();

        // Count actual snapshot files
        let snapshots_dir = self.path.join("snapshots");
        let snapshot_count = if snapshots_dir.exists() {
            fs::read_dir(&snapshots_dir)
                .ok()
                .map(|entries| entries.filter_map(|e| e.ok()).count() as u64)
                .unwrap_or(0)
        } else {
            0
        };

        TableStats {
            sequence_count: meta.last_sequence,
            segment_count: meta.segment_count,
            snapshot_count,
        }
    }

    fn segment_rotation_threshold(&self) -> u64 {
        10 * 1024 * 1024
    }

    /// Count total number of records in the table
    pub fn count_records(&self) -> Result<usize> {
        let _events = self.read_all_events()?;

        // Count non-deleted records by reconstructing the current state
        let state = self.reconstruct_state_at(None)?;
        Ok(state.len())
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
