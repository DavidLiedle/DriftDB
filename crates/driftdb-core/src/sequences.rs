use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Sequence definition with configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sequence {
    pub name: String,
    pub current_value: i64,
    pub increment_by: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub start_value: i64,
    pub cache_size: i64,
    pub cycle: bool,
    pub owned_by: Option<TableColumn>, // For auto-increment columns
}

/// Reference to a table column that owns this sequence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumn {
    pub table_name: String,
    pub column_name: String,
}

/// Cached sequence values for performance
#[derive(Debug, Clone)]
struct SequenceCache {
    next_value: i64,
    last_value: i64,
    exhausted: bool,
}

/// Manages all sequences in the database
pub struct SequenceManager {
    sequences: Arc<RwLock<HashMap<String, Sequence>>>,
    caches: Arc<RwLock<HashMap<String, SequenceCache>>>,
    auto_increment_map: Arc<RwLock<HashMap<String, String>>>, // table.column -> sequence_name
}

impl SequenceManager {
    pub fn new() -> Self {
        Self {
            sequences: Arc::new(RwLock::new(HashMap::new())),
            caches: Arc::new(RwLock::new(HashMap::new())),
            auto_increment_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new sequence
    pub fn create_sequence(&self, mut sequence: Sequence) -> Result<()> {
        // Validate sequence parameters
        self.validate_sequence(&sequence)?;

        // Initialize current value to start_value - increment_by
        // (so first nextval returns start_value)
        sequence.current_value = sequence.start_value - sequence.increment_by;

        // If this is for an auto-increment column, register it
        if let Some(ref owned_by) = sequence.owned_by {
            let key = format!("{}.{}", owned_by.table_name, owned_by.column_name);
            self.auto_increment_map
                .write()
                .insert(key, sequence.name.clone());
        }

        // Store sequence
        let mut sequences = self.sequences.write();
        if sequences.contains_key(&sequence.name) {
            return Err(anyhow!("Sequence '{}' already exists", sequence.name));
        }
        sequences.insert(sequence.name.clone(), sequence);

        Ok(())
    }

    /// Create an auto-increment sequence for a table column
    pub fn create_auto_increment(
        &self,
        table_name: &str,
        column_name: &str,
        start_value: i64,
    ) -> Result<String> {
        let sequence_name = format!("{}_{}_seq", table_name, column_name);

        let sequence = Sequence {
            name: sequence_name.clone(),
            current_value: start_value - 1,
            increment_by: 1,
            min_value: Some(1),
            max_value: Some(i64::MAX),
            start_value,
            cache_size: 10,
            cycle: false,
            owned_by: Some(TableColumn {
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
            }),
        };

        self.create_sequence(sequence)?;
        Ok(sequence_name)
    }

    /// Get the next value from a sequence
    pub fn next_value(&self, sequence_name: &str) -> Result<i64> {
        // Check cache first
        {
            let mut caches = self.caches.write();
            if let Some(cache) = caches.get_mut(sequence_name) {
                if !cache.exhausted && cache.next_value <= cache.last_value {
                    let value = cache.next_value;
                    cache.next_value += self.get_increment(sequence_name)?;
                    return Ok(value);
                }
            }
        }

        // Cache miss or exhausted - refill cache
        self.refill_cache(sequence_name)
    }

    /// Get next value for an auto-increment column
    pub fn next_auto_increment(&self, table_name: &str, column_name: &str) -> Result<i64> {
        let key = format!("{}.{}", table_name, column_name);
        let sequence_name = self
            .auto_increment_map
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "No auto-increment sequence for {}.{}",
                    table_name,
                    column_name
                )
            })?;

        self.next_value(&sequence_name)
    }

    /// Get the current value without incrementing
    pub fn current_value(&self, sequence_name: &str) -> Result<i64> {
        let sequences = self.sequences.read();
        let sequence = sequences
            .get(sequence_name)
            .ok_or_else(|| anyhow!("Sequence '{}' not found", sequence_name))?;
        Ok(sequence.current_value)
    }

    /// Set the value of a sequence
    pub fn set_value(&self, sequence_name: &str, value: i64) -> Result<()> {
        let mut sequences = self.sequences.write();
        let sequence = sequences
            .get_mut(sequence_name)
            .ok_or_else(|| anyhow!("Sequence '{}' not found", sequence_name))?;

        // Validate new value
        if let Some(min) = sequence.min_value {
            if value < min {
                return Err(anyhow!("Value {} is below minimum {}", value, min));
            }
        }
        if let Some(max) = sequence.max_value {
            if value > max {
                return Err(anyhow!("Value {} exceeds maximum {}", value, max));
            }
        }

        sequence.current_value = value;

        // Invalidate cache
        self.caches.write().remove(sequence_name);

        Ok(())
    }

    /// Restart a sequence from its start value
    pub fn restart_sequence(&self, sequence_name: &str) -> Result<()> {
        let mut sequences = self.sequences.write();
        let sequence = sequences
            .get_mut(sequence_name)
            .ok_or_else(|| anyhow!("Sequence '{}' not found", sequence_name))?;

        sequence.current_value = sequence.start_value - sequence.increment_by;

        // Invalidate cache
        self.caches.write().remove(sequence_name);

        Ok(())
    }

    /// Drop a sequence
    pub fn drop_sequence(&self, sequence_name: &str) -> Result<()> {
        let mut sequences = self.sequences.write();
        let sequence = sequences
            .remove(sequence_name)
            .ok_or_else(|| anyhow!("Sequence '{}' not found", sequence_name))?;

        // Remove from auto-increment map if applicable
        if let Some(owned_by) = sequence.owned_by {
            let key = format!("{}.{}", owned_by.table_name, owned_by.column_name);
            self.auto_increment_map.write().remove(&key);
        }

        // Remove cache
        self.caches.write().remove(sequence_name);

        Ok(())
    }

    /// List all sequences
    pub fn list_sequences(&self) -> Vec<Sequence> {
        self.sequences.read().values().cloned().collect()
    }

    /// Get sequence info
    pub fn get_sequence(&self, sequence_name: &str) -> Option<Sequence> {
        self.sequences.read().get(sequence_name).cloned()
    }

    /// Refill the cache for a sequence
    fn refill_cache(&self, sequence_name: &str) -> Result<i64> {
        let mut sequences = self.sequences.write();
        let sequence = sequences
            .get_mut(sequence_name)
            .ok_or_else(|| anyhow!("Sequence '{}' not found", sequence_name))?;

        // Calculate next batch of values
        let mut next_value = sequence.current_value + sequence.increment_by;

        // Check bounds
        if let Some(max) = sequence.max_value {
            if next_value > max {
                if sequence.cycle {
                    next_value = sequence.min_value.unwrap_or(1);
                } else {
                    return Err(anyhow!(
                        "Sequence '{}' exceeded maximum value",
                        sequence_name
                    ));
                }
            }
        }
        if let Some(min) = sequence.min_value {
            if next_value < min {
                if sequence.cycle {
                    next_value = sequence.max_value.unwrap_or(i64::MAX);
                } else {
                    return Err(anyhow!(
                        "Sequence '{}' fell below minimum value",
                        sequence_name
                    ));
                }
            }
        }

        // Update sequence current value to end of cache range
        let cache_end = next_value + (sequence.increment_by * (sequence.cache_size - 1));
        sequence.current_value = cache_end;

        // Update cache
        let cache = SequenceCache {
            next_value: next_value + sequence.increment_by, // Next call will return this
            last_value: cache_end,
            exhausted: false,
        };
        self.caches.write().insert(sequence_name.to_string(), cache);

        Ok(next_value)
    }

    /// Get the increment value for a sequence
    fn get_increment(&self, sequence_name: &str) -> Result<i64> {
        let sequences = self.sequences.read();
        let sequence = sequences
            .get(sequence_name)
            .ok_or_else(|| anyhow!("Sequence '{}' not found", sequence_name))?;
        Ok(sequence.increment_by)
    }

    /// Validate sequence parameters
    fn validate_sequence(&self, sequence: &Sequence) -> Result<()> {
        if sequence.increment_by == 0 {
            return Err(anyhow!("INCREMENT BY cannot be zero"));
        }

        if let (Some(min), Some(max)) = (sequence.min_value, sequence.max_value) {
            if min >= max {
                return Err(anyhow!("MINVALUE must be less than MAXVALUE"));
            }
        }

        if sequence.cache_size < 1 {
            return Err(anyhow!("CACHE must be at least 1"));
        }

        Ok(())
    }

    /// Persist sequences to storage
    pub fn save_sequences(&self) -> Result<HashMap<String, Sequence>> {
        Ok(self.sequences.read().clone())
    }

    /// Load sequences from storage
    pub fn load_sequences(&self, sequences: HashMap<String, Sequence>) -> Result<()> {
        let mut seq_write = self.sequences.write();
        let mut auto_inc_write = self.auto_increment_map.write();

        for (name, sequence) in sequences {
            // Register auto-increment mapping if applicable
            if let Some(ref owned_by) = sequence.owned_by {
                let key = format!("{}.{}", owned_by.table_name, owned_by.column_name);
                auto_inc_write.insert(key, name.clone());
            }
            seq_write.insert(name, sequence);
        }

        Ok(())
    }
}

impl Default for SequenceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_sequence() {
        let mgr = SequenceManager::new();

        // Create sequence
        let seq = Sequence {
            name: "test_seq".to_string(),
            current_value: 0,
            increment_by: 1,
            min_value: Some(1),
            max_value: Some(100),
            start_value: 1,
            cache_size: 10,
            cycle: false,
            owned_by: None,
        };
        mgr.create_sequence(seq).unwrap();

        // Get next values
        assert_eq!(mgr.next_value("test_seq").unwrap(), 1);
        assert_eq!(mgr.next_value("test_seq").unwrap(), 2);
        assert_eq!(mgr.next_value("test_seq").unwrap(), 3);

        // Current value should be ahead due to caching
        assert!(mgr.current_value("test_seq").unwrap() >= 3);
    }

    #[test]
    fn test_auto_increment() {
        let mgr = SequenceManager::new();

        // Create auto-increment
        mgr.create_auto_increment("users", "id", 1).unwrap();

        // Get next values
        assert_eq!(mgr.next_auto_increment("users", "id").unwrap(), 1);
        assert_eq!(mgr.next_auto_increment("users", "id").unwrap(), 2);
        assert_eq!(mgr.next_auto_increment("users", "id").unwrap(), 3);
    }

    #[test]
    fn test_sequence_with_increment() {
        let mgr = SequenceManager::new();

        // Create sequence with increment of 5
        let seq = Sequence {
            name: "by_five".to_string(),
            current_value: 0,
            increment_by: 5,
            min_value: None,
            max_value: None,
            start_value: 10,
            cache_size: 3,
            cycle: false,
            owned_by: None,
        };
        mgr.create_sequence(seq).unwrap();

        // Values should increment by 5
        assert_eq!(mgr.next_value("by_five").unwrap(), 10);
        assert_eq!(mgr.next_value("by_five").unwrap(), 15);
        assert_eq!(mgr.next_value("by_five").unwrap(), 20);
    }

    #[test]
    fn test_sequence_cycle() {
        let mgr = SequenceManager::new();

        // Create cycling sequence with small range
        let seq = Sequence {
            name: "cycle_seq".to_string(),
            current_value: 0,
            increment_by: 1,
            min_value: Some(1),
            max_value: Some(3),
            start_value: 1,
            cache_size: 1, // Small cache to test cycling
            cycle: true,
            owned_by: None,
        };
        mgr.create_sequence(seq).unwrap();

        // Should cycle back to min after max
        assert_eq!(mgr.next_value("cycle_seq").unwrap(), 1);
        assert_eq!(mgr.next_value("cycle_seq").unwrap(), 2);
        assert_eq!(mgr.next_value("cycle_seq").unwrap(), 3);
        assert_eq!(mgr.next_value("cycle_seq").unwrap(), 1); // Cycle back
    }

    #[test]
    fn test_set_sequence_value() {
        let mgr = SequenceManager::new();

        // Create sequence
        let seq = Sequence {
            name: "reset_seq".to_string(),
            current_value: 0,
            increment_by: 1,
            min_value: Some(1),
            max_value: Some(1000),
            start_value: 1,
            cache_size: 10,
            cycle: false,
            owned_by: None,
        };
        mgr.create_sequence(seq).unwrap();

        // Use sequence
        mgr.next_value("reset_seq").unwrap();
        mgr.next_value("reset_seq").unwrap();

        // Set to specific value
        mgr.set_value("reset_seq", 100).unwrap();

        // Next value should be 101
        assert_eq!(mgr.next_value("reset_seq").unwrap(), 101);
    }
}
