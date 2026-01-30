use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::errors::Result;

/// Metadata about a single segment's sequence range
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SegmentBounds {
    /// Minimum sequence number in this segment
    pub min_sequence: u64,
    /// Maximum sequence number in this segment
    pub max_sequence: u64,
    /// Number of events in this segment
    pub event_count: u64,
}

impl SegmentBounds {
    pub fn new(min_sequence: u64, max_sequence: u64, event_count: u64) -> Self {
        Self {
            min_sequence,
            max_sequence,
            event_count,
        }
    }

    /// Check if this segment could contain events after a given sequence
    pub fn contains_events_after(&self, after_seq: u64) -> bool {
        self.max_sequence > after_seq
    }

    /// Check if this segment could contain events before or at a given sequence
    pub fn contains_events_at_or_before(&self, target_seq: u64) -> bool {
        self.min_sequence <= target_seq
    }
}

/// Index of all segments with their sequence ranges for fast lookups
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SegmentIndex {
    /// Map from segment ID to its bounds (using BTreeMap for sorted iteration)
    pub segments: BTreeMap<u64, SegmentBounds>,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            segments: BTreeMap::new(),
        }
    }

    /// Add or update bounds for a segment
    pub fn update_segment(&mut self, segment_id: u64, bounds: SegmentBounds) {
        self.segments.insert(segment_id, bounds);
    }

    /// Find the first segment that might contain events after a given sequence
    /// Returns the segment ID using binary search
    pub fn find_first_segment_after(&self, after_seq: u64) -> Option<u64> {
        // Find the first segment whose max_sequence > after_seq
        for (&segment_id, bounds) in &self.segments {
            if bounds.contains_events_after(after_seq) {
                return Some(segment_id);
            }
        }
        None
    }

    /// Find the last segment that contains events at or before a given sequence
    pub fn find_last_segment_at_or_before(&self, target_seq: u64) -> Option<u64> {
        let mut result = None;
        for (&segment_id, bounds) in &self.segments {
            if bounds.contains_events_at_or_before(target_seq) {
                result = Some(segment_id);
            }
            // Once we pass segments that start after target, we can stop
            if bounds.min_sequence > target_seq {
                break;
            }
        }
        result
    }

    /// Get segments that could contain events in a sequence range
    pub fn segments_in_range(&self, start_seq: u64, end_seq: u64) -> Vec<u64> {
        self.segments
            .iter()
            .filter(|(_, bounds)| {
                // Segment overlaps with [start_seq, end_seq]
                bounds.max_sequence >= start_seq && bounds.min_sequence <= end_seq
            })
            .map(|(&id, _)| id)
            .collect()
    }

    /// Check if the index needs rebuilding (empty or stale)
    pub fn needs_rebuild(&self, expected_segment_count: u64) -> bool {
        self.segments.is_empty() || self.segments.len() != expected_segment_count as usize
    }

    /// Load segment index from file
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }

    /// Save segment index to file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub last_sequence: u64,
    pub last_snapshot_sequence: u64,
    pub segment_count: u64,
    pub snapshot_interval: u64,
    pub compact_threshold: u64,
    /// Index of segment sequence ranges for optimized reads
    #[serde(default)]
    pub segment_index: SegmentIndex,
}

impl Default for TableMeta {
    fn default() -> Self {
        Self {
            last_sequence: 0,
            last_snapshot_sequence: 0,
            segment_count: 1,
            snapshot_interval: 100_000,
            compact_threshold: 128 * 1024 * 1024,
            segment_index: SegmentIndex::new(),
        }
    }
}

impl TableMeta {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_bounds_contains_events_after() {
        let bounds = SegmentBounds::new(100, 200, 50);

        // Should contain events after 50 (max 200 > 50)
        assert!(bounds.contains_events_after(50));

        // Should contain events after 150 (max 200 > 150)
        assert!(bounds.contains_events_after(150));

        // Should NOT contain events after 200 (max 200 is not > 200)
        assert!(!bounds.contains_events_after(200));

        // Should NOT contain events after 250
        assert!(!bounds.contains_events_after(250));
    }

    #[test]
    fn test_segment_bounds_contains_events_at_or_before() {
        let bounds = SegmentBounds::new(100, 200, 50);

        // Should contain events at or before 200
        assert!(bounds.contains_events_at_or_before(200));

        // Should contain events at or before 100
        assert!(bounds.contains_events_at_or_before(100));

        // Should contain events at or before 150
        assert!(bounds.contains_events_at_or_before(150));

        // Should NOT contain events at or before 50 (min is 100)
        assert!(!bounds.contains_events_at_or_before(50));
    }

    #[test]
    fn test_segment_index_find_first_segment_after() {
        let mut index = SegmentIndex::new();

        // Add segments: 1 (1-100), 2 (101-200), 3 (201-300)
        index.update_segment(1, SegmentBounds::new(1, 100, 100));
        index.update_segment(2, SegmentBounds::new(101, 200, 100));
        index.update_segment(3, SegmentBounds::new(201, 300, 100));

        // Looking for events after 0, should find segment 1
        assert_eq!(index.find_first_segment_after(0), Some(1));

        // Looking for events after 50, should find segment 1 (has events up to 100)
        assert_eq!(index.find_first_segment_after(50), Some(1));

        // Looking for events after 100, should find segment 2 (segment 1's max is 100, not > 100)
        assert_eq!(index.find_first_segment_after(100), Some(2));

        // Looking for events after 150, should find segment 2
        assert_eq!(index.find_first_segment_after(150), Some(2));

        // Looking for events after 250, should find segment 3
        assert_eq!(index.find_first_segment_after(250), Some(3));

        // Looking for events after 300, should find none
        assert_eq!(index.find_first_segment_after(300), None);
    }

    #[test]
    fn test_segment_index_segments_in_range() {
        let mut index = SegmentIndex::new();

        index.update_segment(1, SegmentBounds::new(1, 100, 100));
        index.update_segment(2, SegmentBounds::new(101, 200, 100));
        index.update_segment(3, SegmentBounds::new(201, 300, 100));

        // Range that covers all segments
        let segments = index.segments_in_range(1, 300);
        assert_eq!(segments, vec![1, 2, 3]);

        // Range that covers only middle segment
        let segments = index.segments_in_range(150, 175);
        assert_eq!(segments, vec![2]);

        // Range that spans two segments
        let segments = index.segments_in_range(50, 150);
        assert_eq!(segments, vec![1, 2]);

        // Range outside all segments
        let segments = index.segments_in_range(400, 500);
        assert!(segments.is_empty());
    }

    #[test]
    fn test_segment_index_needs_rebuild() {
        let mut index = SegmentIndex::new();

        // Empty index needs rebuild
        assert!(index.needs_rebuild(3));

        // Add segments
        index.update_segment(1, SegmentBounds::new(1, 100, 100));
        index.update_segment(2, SegmentBounds::new(101, 200, 100));

        // Wrong count needs rebuild
        assert!(index.needs_rebuild(3));

        // Correct count doesn't need rebuild
        assert!(!index.needs_rebuild(2));
    }

    #[test]
    fn test_table_meta_with_segment_index() {
        let mut meta = TableMeta::default();

        // Add segment bounds
        meta.segment_index
            .update_segment(1, SegmentBounds::new(1, 100, 100));

        // Serialize and deserialize
        let json = serde_json::to_string(&meta).unwrap();
        let restored: TableMeta = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.segment_index.segments.len(), 1);
        assert!(restored.segment_index.segments.contains_key(&1));
    }

    #[test]
    fn test_table_meta_backward_compatibility() {
        // Old meta format without segment_index
        let old_json = r#"{
            "last_sequence": 100,
            "last_snapshot_sequence": 50,
            "segment_count": 2,
            "snapshot_interval": 100000,
            "compact_threshold": 134217728
        }"#;

        // Should deserialize with default segment_index
        let meta: TableMeta = serde_json::from_str(old_json).unwrap();
        assert_eq!(meta.last_sequence, 100);
        assert!(meta.segment_index.segments.is_empty());
    }
}
