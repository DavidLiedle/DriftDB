//! Bloom Filter Implementation
//!
//! Provides space-efficient probabilistic data structures for membership testing.
//! Bloom filters can quickly determine if an element is definitely NOT in a set,
//! or POSSIBLY in a set (with a configurable false positive rate).
//!
//! Use cases:
//! - Index existence checks (avoid disk I/O for non-existent keys)
//! - Join optimization (filter non-matching rows early)
//! - Cache membership testing
//! - Duplicate detection in streams

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Bloom filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomConfig {
    /// Expected number of elements
    pub expected_elements: usize,
    /// Target false positive rate (0.0 to 1.0)
    pub false_positive_rate: f64,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            expected_elements: 10000,
            false_positive_rate: 0.01, // 1% false positive rate
        }
    }
}

impl BloomConfig {
    /// Calculate optimal bit array size
    pub fn optimal_bits(&self) -> usize {
        let n = self.expected_elements as f64;
        let p = self.false_positive_rate;
        let m = -(n * p.ln()) / (2.0_f64.ln().powi(2));
        m.ceil() as usize
    }

    /// Calculate optimal number of hash functions
    pub fn optimal_hashes(&self) -> usize {
        let m = self.optimal_bits() as f64;
        let n = self.expected_elements as f64;
        let k = (m / n) * 2.0_f64.ln();
        k.ceil().max(1.0) as usize
    }
}

/// Bloom filter for membership testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of bits in the filter
    num_bits: usize,
    /// Number of hash functions to use
    num_hashes: usize,
    /// Number of elements added
    element_count: usize,
    /// Target false positive rate
    false_positive_rate: f64,
}

impl BloomFilter {
    /// Create a new bloom filter with configuration
    pub fn new(config: BloomConfig) -> Self {
        let num_bits = config.optimal_bits();
        let num_hashes = config.optimal_hashes();
        let num_u64s = (num_bits + 63) / 64; // Round up to nearest u64

        Self {
            bits: vec![0u64; num_u64s],
            num_bits,
            num_hashes,
            element_count: 0,
            false_positive_rate: config.false_positive_rate,
        }
    }

    /// Create a bloom filter with explicit parameters
    pub fn with_params(num_bits: usize, num_hashes: usize) -> Self {
        let num_u64s = (num_bits + 63) / 64;
        Self {
            bits: vec![0u64; num_u64s],
            num_bits,
            num_hashes,
            element_count: 0,
            false_positive_rate: 0.01,
        }
    }

    /// Add an element to the bloom filter
    pub fn add<T: Hash>(&mut self, item: &T) {
        let hashes = self.compute_hashes(item);
        for hash in hashes {
            let bit_index = (hash % self.num_bits as u64) as usize;
            self.set_bit(bit_index);
        }
        self.element_count += 1;
    }

    /// Check if an element might be in the set
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let hashes = self.compute_hashes(item);
        for hash in hashes {
            let bit_index = (hash % self.num_bits as u64) as usize;
            if !self.get_bit(bit_index) {
                return false; // Definitely not in the set
            }
        }
        true // Possibly in the set
    }

    /// Clear all bits in the filter
    pub fn clear(&mut self) {
        for chunk in &mut self.bits {
            *chunk = 0;
        }
        self.element_count = 0;
    }

    /// Get the number of elements added
    pub fn len(&self) -> usize {
        self.element_count
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.element_count == 0
    }

    /// Get the current false positive probability
    pub fn current_false_positive_rate(&self) -> f64 {
        if self.element_count == 0 {
            return 0.0;
        }

        let k = self.num_hashes as f64;
        let m = self.num_bits as f64;
        let n = self.element_count as f64;

        // False positive probability: (1 - e^(-kn/m))^k
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Check if the filter should be rebuilt due to saturation
    pub fn is_saturated(&self) -> bool {
        self.current_false_positive_rate() > self.false_positive_rate * 2.0
    }

    /// Merge another bloom filter into this one
    /// Both filters must have the same size and hash count
    pub fn merge(&mut self, other: &BloomFilter) -> Result<(), String> {
        if self.num_bits != other.num_bits {
            return Err(format!(
                "Bloom filter size mismatch: {} vs {}",
                self.num_bits, other.num_bits
            ));
        }
        if self.num_hashes != other.num_hashes {
            return Err(format!(
                "Hash function count mismatch: {} vs {}",
                self.num_hashes, other.num_hashes
            ));
        }

        for (i, chunk) in other.bits.iter().enumerate() {
            self.bits[i] |= chunk;
        }
        self.element_count += other.element_count;

        Ok(())
    }

    /// Get statistics about the bloom filter
    pub fn statistics(&self) -> BloomStatistics {
        let total_bits = self.num_bits;
        let set_bits = self.count_set_bits();
        let fill_ratio = set_bits as f64 / total_bits as f64;

        BloomStatistics {
            num_bits: total_bits,
            num_hashes: self.num_hashes,
            element_count: self.element_count,
            set_bits,
            fill_ratio,
            false_positive_rate: self.current_false_positive_rate(),
            memory_bytes: self.bits.len() * 8,
        }
    }

    /// Compute k hash values for an item
    fn compute_hashes<T: Hash>(&self, item: &T) -> Vec<u64> {
        let mut hashes = Vec::with_capacity(self.num_hashes);

        // Use two hash functions to generate k hashes (double hashing technique)
        let hash1 = self.hash_item(item, 0);
        let hash2 = self.hash_item(item, 1);

        for i in 0..self.num_hashes {
            // Combine using: hash_i = hash1 + i * hash2
            let combined = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            hashes.push(combined);
        }

        hashes
    }

    /// Hash an item with a seed
    fn hash_item<T: Hash>(&self, item: &T, seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        item.hash(&mut hasher);
        hasher.finish()
    }

    /// Set a bit in the bit array
    fn set_bit(&mut self, index: usize) {
        let chunk_index = index / 64;
        let bit_index = index % 64;
        self.bits[chunk_index] |= 1u64 << bit_index;
    }

    /// Get a bit from the bit array
    fn get_bit(&self, index: usize) -> bool {
        let chunk_index = index / 64;
        let bit_index = index % 64;
        (self.bits[chunk_index] & (1u64 << bit_index)) != 0
    }

    /// Count the number of set bits
    fn count_set_bits(&self) -> usize {
        self.bits.iter().map(|chunk| chunk.count_ones() as usize).sum()
    }
}

/// Bloom filter statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomStatistics {
    pub num_bits: usize,
    pub num_hashes: usize,
    pub element_count: usize,
    pub set_bits: usize,
    pub fill_ratio: f64,
    pub false_positive_rate: f64,
    pub memory_bytes: usize,
}

/// Scalable bloom filter that grows automatically
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalableBloomFilter {
    /// List of bloom filters
    filters: Vec<BloomFilter>,
    /// Configuration for new filters
    config: BloomConfig,
    /// Growth factor for new filters
    growth_factor: usize,
}

impl ScalableBloomFilter {
    /// Create a new scalable bloom filter
    pub fn new(config: BloomConfig) -> Self {
        let initial_filter = BloomFilter::new(config.clone());
        Self {
            filters: vec![initial_filter],
            config,
            growth_factor: 2,
        }
    }

    /// Add an element to the filter
    pub fn add<T: Hash>(&mut self, item: &T) {
        // Check if current filter is saturated
        if let Some(current) = self.filters.last_mut() {
            if current.is_saturated() {
                // Create a new filter with larger capacity
                let new_capacity = self.config.expected_elements * self.growth_factor;
                let new_config = BloomConfig {
                    expected_elements: new_capacity,
                    false_positive_rate: self.config.false_positive_rate,
                };
                self.filters.push(BloomFilter::new(new_config));
            }
        }

        // Add to the most recent filter
        if let Some(current) = self.filters.last_mut() {
            current.add(item);
        }
    }

    /// Check if an element might be in the set
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        // Check all filters (element could be in any of them)
        self.filters.iter().any(|filter| filter.contains(item))
    }

    /// Clear all filters
    pub fn clear(&mut self) {
        self.filters.clear();
        self.filters.push(BloomFilter::new(self.config.clone()));
    }

    /// Get total element count
    pub fn len(&self) -> usize {
        self.filters.iter().map(|f| f.len()).sum()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.filters.iter().all(|f| f.is_empty())
    }

    /// Get statistics for all filters
    pub fn statistics(&self) -> Vec<BloomStatistics> {
        self.filters.iter().map(|f| f.statistics()).collect()
    }

    /// Get total memory usage
    pub fn memory_bytes(&self) -> usize {
        self.filters.iter().map(|f| f.statistics().memory_bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_config_calculations() {
        let config = BloomConfig {
            expected_elements: 1000,
            false_positive_rate: 0.01,
        };

        let bits = config.optimal_bits();
        let hashes = config.optimal_hashes();

        assert!(bits > 0);
        assert!(hashes > 0);
        assert!(hashes <= 20); // Reasonable number of hashes
    }

    #[test]
    fn test_basic_operations() {
        let config = BloomConfig {
            expected_elements: 100,
            false_positive_rate: 0.01,
        };
        let mut filter = BloomFilter::new(config);

        // Add elements
        filter.add(&"apple");
        filter.add(&"banana");
        filter.add(&"cherry");

        // Check membership
        assert!(filter.contains(&"apple"));
        assert!(filter.contains(&"banana"));
        assert!(filter.contains(&"cherry"));
        assert!(!filter.contains(&"dragon_fruit")); // Should be false

        assert_eq!(filter.len(), 3);
    }

    #[test]
    fn test_false_negatives_impossible() {
        let config = BloomConfig::default();
        let mut filter = BloomFilter::new(config);

        let items = vec!["item1", "item2", "item3", "item4", "item5"];

        for item in &items {
            filter.add(item);
        }

        // All added items MUST return true (no false negatives)
        for item in &items {
            assert!(filter.contains(item), "False negative for {}", item);
        }
    }

    #[test]
    fn test_false_positive_rate() {
        let config = BloomConfig {
            expected_elements: 1000,
            false_positive_rate: 0.01,
        };
        let mut filter = BloomFilter::new(config);

        // Add 1000 items
        for i in 0..1000 {
            filter.add(&format!("item_{}", i));
        }

        // Test 1000 items that were NOT added
        let mut false_positives = 0;
        for i in 1000..2000 {
            if filter.contains(&format!("item_{}", i)) {
                false_positives += 1;
            }
        }

        let actual_fp_rate = false_positives as f64 / 1000.0;

        // False positive rate should be close to target (within 3x)
        assert!(actual_fp_rate <= 0.03, "FP rate too high: {}", actual_fp_rate);
    }

    #[test]
    fn test_clear() {
        let config = BloomConfig::default();
        let mut filter = BloomFilter::new(config);

        filter.add(&"test");
        assert!(filter.contains(&"test"));
        assert_eq!(filter.len(), 1);

        filter.clear();
        assert_eq!(filter.len(), 0);
        // After clearing, may still have false positives
    }

    #[test]
    fn test_merge() {
        let config = BloomConfig {
            expected_elements: 100,
            false_positive_rate: 0.01,
        };

        let mut filter1 = BloomFilter::new(config.clone());
        filter1.add(&"apple");
        filter1.add(&"banana");

        let mut filter2 = BloomFilter::new(config);
        filter2.add(&"cherry");
        filter2.add(&"date");

        filter1.merge(&filter2).unwrap();

        // Merged filter should contain all elements
        assert!(filter1.contains(&"apple"));
        assert!(filter1.contains(&"banana"));
        assert!(filter1.contains(&"cherry"));
        assert!(filter1.contains(&"date"));
        assert_eq!(filter1.len(), 4);
    }

    #[test]
    fn test_merge_incompatible() {
        let config1 = BloomConfig {
            expected_elements: 100,
            false_positive_rate: 0.01,
        };
        let config2 = BloomConfig {
            expected_elements: 1000,
            false_positive_rate: 0.01,
        };

        let mut filter1 = BloomFilter::new(config1);
        let filter2 = BloomFilter::new(config2);

        // Should fail due to size mismatch
        assert!(filter1.merge(&filter2).is_err());
    }

    #[test]
    fn test_statistics() {
        let config = BloomConfig {
            expected_elements: 100,
            false_positive_rate: 0.01,
        };
        let mut filter = BloomFilter::new(config);

        for i in 0..50 {
            filter.add(&i);
        }

        let stats = filter.statistics();
        assert_eq!(stats.element_count, 50);
        assert!(stats.fill_ratio > 0.0 && stats.fill_ratio < 1.0);
        assert!(stats.memory_bytes > 0);
    }

    #[test]
    fn test_saturation_detection() {
        let config = BloomConfig {
            expected_elements: 10,
            false_positive_rate: 0.01,
        };
        let mut filter = BloomFilter::new(config);

        // Add more elements than expected
        for i in 0..100 {
            filter.add(&i);
        }

        // Should detect saturation
        assert!(filter.is_saturated());
    }

    #[test]
    fn test_scalable_bloom_filter() {
        let config = BloomConfig {
            expected_elements: 10,
            false_positive_rate: 0.01,
        };
        let mut filter = ScalableBloomFilter::new(config);

        // Add many elements (more than initial capacity)
        for i in 0..100 {
            filter.add(&i);
        }

        // Should have multiple filters
        assert!(filter.filters.len() > 1);

        // All elements should be found
        for i in 0..100 {
            assert!(filter.contains(&i), "Missing element {}", i);
        }

        assert_eq!(filter.len(), 100);
    }

    #[test]
    fn test_different_types() {
        let config = BloomConfig::default();
        let mut filter = BloomFilter::new(config);

        filter.add(&42i32);
        filter.add(&"string");
        filter.add(&3.14f64.to_bits());
        filter.add(&vec![1u8, 2, 3]);

        assert!(filter.contains(&42i32));
        assert!(filter.contains(&"string"));
        assert!(filter.contains(&3.14f64.to_bits()));
        assert!(filter.contains(&vec![1u8, 2, 3]));
    }

    #[test]
    fn test_empty_filter() {
        let config = BloomConfig::default();
        let filter = BloomFilter::new(config);

        assert!(filter.is_empty());
        assert_eq!(filter.len(), 0);

        // Empty filter should return false for any query
        // (though false positives are possible with very low probability)
        assert!(!filter.contains(&"anything"));
    }
}
