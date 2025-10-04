use std::fs;
use std::path::Path;

use crate::errors::Result;
use crate::events::{Event, EventType};
use crate::storage::Segment;

/// Configuration for memory-bounded operations
#[derive(Clone)]
pub struct StreamConfig {
    /// Maximum number of events to buffer in memory at once
    pub event_buffer_size: usize,
    /// Maximum memory usage in bytes for state reconstruction
    pub max_state_memory: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 10_000,           // 10k events max in memory
            max_state_memory: 512 * 1024 * 1024, // 512MB max for state
        }
    }
}

/// Iterator that streams events from segments without loading all into memory
pub struct EventStreamIterator {
    _segments_dir: std::path::PathBuf,
    segment_files: Vec<std::path::PathBuf>,
    current_segment_idx: usize,
    current_reader: Option<crate::storage::segment::SegmentReader>,
    sequence_limit: Option<u64>,
    events_read: usize,
    config: StreamConfig,
}

impl EventStreamIterator {
    pub fn new(table_path: &Path, sequence_limit: Option<u64>) -> Result<Self> {
        Self::with_config(table_path, sequence_limit, StreamConfig::default())
    }

    pub fn with_config(
        table_path: &Path,
        sequence_limit: Option<u64>,
        config: StreamConfig,
    ) -> Result<Self> {
        let segments_dir = table_path.join("segments");

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
            .map(|entry| entry.path())
            .collect();

        segment_files.sort();

        Ok(Self {
            _segments_dir: segments_dir,
            segment_files,
            current_segment_idx: 0,
            current_reader: None,
            sequence_limit,
            events_read: 0,
            config,
        })
    }

    fn open_next_segment(&mut self) -> Result<bool> {
        if self.current_segment_idx >= self.segment_files.len() {
            return Ok(false);
        }

        let segment_path = &self.segment_files[self.current_segment_idx];
        let segment = Segment::new(segment_path.clone(), 0);
        self.current_reader = Some(segment.open_reader()?);
        self.current_segment_idx += 1;
        Ok(true)
    }
}

impl Iterator for EventStreamIterator {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we've hit the configured limit
            if self.events_read >= self.config.event_buffer_size {
                return None;
            }

            // Try to read from current segment
            if let Some(ref mut reader) = self.current_reader {
                match reader.read_next_event() {
                    Ok(Some(event)) => {
                        // Check sequence limit
                        if let Some(limit) = self.sequence_limit {
                            if event.sequence > limit {
                                return None;
                            }
                        }
                        self.events_read += 1;
                        return Some(Ok(event));
                    }
                    Ok(None) => {
                        // Current segment exhausted, try next
                        self.current_reader = None;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Open next segment
            match self.open_next_segment() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Memory-bounded state reconstruction
pub fn reconstruct_state_streaming(
    table_path: &Path,
    sequence: Option<u64>,
    config: StreamConfig,
) -> Result<std::collections::HashMap<String, serde_json::Value>> {
    let mut state = std::collections::HashMap::new();
    let mut estimated_memory = 0usize;

    let stream = EventStreamIterator::with_config(table_path, sequence, config.clone())?;

    for event_result in stream {
        let event = event_result?;

        // Rough memory estimation (conservative)
        let event_size = estimate_event_memory(&event);

        // Check memory limit
        if estimated_memory + event_size > config.max_state_memory {
            tracing::warn!(
                "State reconstruction approaching memory limit ({} bytes), stopping at sequence {}",
                estimated_memory,
                event.sequence
            );
            break;
        }

        match event.event_type {
            EventType::Insert => {
                let key = event.primary_key.to_string();
                let old_size = state.get(&key).map(estimate_value_memory).unwrap_or(0);
                state.insert(key, event.payload.clone());
                estimated_memory = estimated_memory - old_size + event_size;
            }
            EventType::Patch => {
                if let Some(existing) = state.get_mut(&event.primary_key.to_string()) {
                    let old_size = estimate_value_memory(existing);
                    if let serde_json::Value::Object(ref mut existing_map) = existing {
                        if let serde_json::Value::Object(patch_map) = &event.payload {
                            for (key, value) in patch_map {
                                existing_map.insert(key.clone(), value.clone());
                            }
                        }
                    }
                    let new_size = estimate_value_memory(existing);
                    estimated_memory = estimated_memory - old_size + new_size;
                }
            }
            EventType::SoftDelete => {
                if let Some(removed) = state.remove(&event.primary_key.to_string()) {
                    estimated_memory -= estimate_value_memory(&removed);
                }
            }
        }
    }

    Ok(state)
}

fn estimate_event_memory(event: &Event) -> usize {
    // Conservative estimation: JSON string representation * 2 for overhead
    let json_size = event.payload.to_string().len();
    let key_size = event.primary_key.to_string().len();
    (json_size + key_size) * 2 + 128 // Add fixed overhead for Event struct
}

fn estimate_value_memory(value: &serde_json::Value) -> usize {
    value.to_string().len() * 2 + 64
}
