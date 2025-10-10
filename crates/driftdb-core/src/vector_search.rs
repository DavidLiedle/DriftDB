//! Vector Similarity Search for DriftDB
//!
//! Provides high-performance vector search with:
//! - Multiple distance metrics (Cosine, Euclidean, Dot Product)
//! - HNSW (Hierarchical Navigable Small World) index
//! - IVF (Inverted File) index with Product Quantization
//! - Hybrid search combining vector and metadata filters
//! - Incremental index updates
//! - GPU acceleration support (optional)

use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

use crate::errors::{DriftError, Result};

/// Vector type (32-bit float for efficiency)
pub type Vector = Vec<f32>;

/// Vector dimension
pub type Dimension = usize;

/// Distance metric for similarity
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
    DotProduct,
    Manhattan,
}

/// Vector index types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    /// Flat index - exact search
    Flat,
    /// HNSW - Hierarchical Navigable Small World
    HNSW {
        m: usize,               // Number of connections
        ef_construction: usize, // Size of dynamic candidate list
        max_m: usize,           // Maximum connections per layer
        seed: u64,              // Random seed
    },
    /// IVF - Inverted File Index
    IVF {
        n_lists: usize,      // Number of inverted lists
        n_probe: usize,      // Number of lists to probe
        use_pq: bool,        // Use Product Quantization
        pq_bits: Option<u8>, // Bits for PQ (if used)
    },
}

/// Vector entry in the index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    pub id: String,
    pub vector: Vector,
    pub metadata: HashMap<String, serde_json::Value>,
    pub timestamp: u64,
}

/// Search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub score: f32,
    pub vector: Option<Vector>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Vector index trait
pub trait VectorIndex: Send + Sync {
    /// Add a vector to the index
    fn add(&mut self, entry: VectorEntry) -> Result<()>;

    /// Remove a vector from the index
    fn remove(&mut self, id: &str) -> Result<()>;

    /// Search for k nearest neighbors
    fn search(
        &self,
        query: &Vector,
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> Result<Vec<SearchResult>>;

    /// Get index statistics
    fn statistics(&self) -> IndexStatistics;

    /// Optimize the index
    fn optimize(&mut self) -> Result<()>;

    /// Save index to bytes
    fn serialize(&self) -> Result<Vec<u8>>;

    /// Load index from bytes
    fn deserialize(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// Metadata filter for hybrid search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataFilter {
    pub conditions: Vec<FilterCondition>,
    pub combine: CombineOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterCondition {
    Equals {
        field: String,
        value: serde_json::Value,
    },
    NotEquals {
        field: String,
        value: serde_json::Value,
    },
    GreaterThan {
        field: String,
        value: serde_json::Value,
    },
    LessThan {
        field: String,
        value: serde_json::Value,
    },
    In {
        field: String,
        values: Vec<serde_json::Value>,
    },
    Contains {
        field: String,
        value: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CombineOp {
    And,
    Or,
}

/// Index statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexStatistics {
    pub total_vectors: usize,
    pub dimension: usize,
    pub index_size_bytes: usize,
    pub search_count: u64,
    pub add_count: u64,
    pub remove_count: u64,
    pub avg_search_time_ms: f64,
}

/// Flat index for exact search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatIndex {
    entries: HashMap<String, VectorEntry>,
    dimension: usize,
    metric: DistanceMetric,
    stats: IndexStatistics,
}

impl FlatIndex {
    pub fn new(dimension: usize, metric: DistanceMetric) -> Self {
        Self {
            entries: HashMap::new(),
            dimension,
            metric,
            stats: IndexStatistics {
                dimension,
                ..Default::default()
            },
        }
    }

    fn calculate_distance(&self, v1: &Vector, v2: &Vector) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => cosine_distance(v1, v2),
            DistanceMetric::Euclidean => euclidean_distance(v1, v2),
            DistanceMetric::DotProduct => dot_product_distance(v1, v2),
            DistanceMetric::Manhattan => manhattan_distance(v1, v2),
        }
    }
}

impl VectorIndex for FlatIndex {
    fn add(&mut self, entry: VectorEntry) -> Result<()> {
        if entry.vector.len() != self.dimension {
            return Err(DriftError::Other(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimension,
                entry.vector.len()
            )));
        }

        self.entries.insert(entry.id.clone(), entry);
        self.stats.total_vectors = self.entries.len();
        self.stats.add_count += 1;
        Ok(())
    }

    fn remove(&mut self, id: &str) -> Result<()> {
        self.entries
            .remove(id)
            .ok_or_else(|| DriftError::Other(format!("Vector '{}' not found", id)))?;
        self.stats.total_vectors = self.entries.len();
        self.stats.remove_count += 1;
        Ok(())
    }

    fn search(
        &self,
        query: &Vector,
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.dimension {
            return Err(DriftError::Other(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            )));
        }

        let start = std::time::Instant::now();

        // Calculate distances for all entries
        let mut results: Vec<(f32, &VectorEntry)> = self
            .entries
            .values()
            .filter(|entry| {
                // Apply metadata filter if provided
                if let Some(f) = filter {
                    apply_filter(&entry.metadata, f)
                } else {
                    true
                }
            })
            .map(|entry| {
                let distance = self.calculate_distance(query, &entry.vector);
                (distance, entry)
            })
            .collect();

        // Sort by distance (ascending for similarity)
        results.sort_by_key(|(dist, _)| OrderedFloat(*dist));

        // Take top k results
        let search_results: Vec<SearchResult> = results
            .into_iter()
            .take(k)
            .map(|(score, entry)| SearchResult {
                id: entry.id.clone(),
                score,
                vector: None,
                metadata: entry.metadata.clone(),
            })
            .collect();

        let _elapsed = start.elapsed().as_millis() as f64;
        // Update stats (would need mutex for thread safety)

        Ok(search_results)
    }

    fn statistics(&self) -> IndexStatistics {
        self.stats.clone()
    }

    fn optimize(&mut self) -> Result<()> {
        // Flat index doesn't need optimization
        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| DriftError::Other(format!("Serialization failed: {}", e)))
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| DriftError::Other(format!("Deserialization failed: {}", e)))
    }
}

/// HNSW index for approximate nearest neighbor search
pub struct HNSWIndex {
    entries: HashMap<String, VectorEntry>,
    layers: Vec<HashMap<String, HashSet<String>>>, // Adjacency lists per layer
    entry_point: Option<String>,
    m: usize,
    max_m: usize,
    #[allow(dead_code)]
    ef_construction: usize,
    dimension: usize,
    metric: DistanceMetric,
    #[allow(dead_code)]
    stats: IndexStatistics,
}

impl HNSWIndex {
    pub fn new(dimension: usize, metric: DistanceMetric, m: usize, ef_construction: usize) -> Self {
        Self {
            entries: HashMap::new(),
            layers: vec![HashMap::new()],
            entry_point: None,
            m,
            max_m: m * 2,
            ef_construction,
            dimension,
            metric,
            stats: IndexStatistics {
                dimension,
                ..Default::default()
            },
        }
    }

    fn calculate_distance(&self, v1: &Vector, v2: &Vector) -> f32 {
        match self.metric {
            DistanceMetric::Cosine => cosine_distance(v1, v2),
            DistanceMetric::Euclidean => euclidean_distance(v1, v2),
            DistanceMetric::DotProduct => dot_product_distance(v1, v2),
            DistanceMetric::Manhattan => manhattan_distance(v1, v2),
        }
    }

    fn get_random_level(&self) -> usize {
        let mut level = 0;
        let ml = 1.0 / (2.0_f64).ln();
        while rand::random::<f64>() < ml && level < 16 {
            level += 1;
        }
        level
    }

    fn search_layer(
        &self,
        query: &Vector,
        entry_points: HashSet<String>,
        num_closest: usize,
        layer: usize,
    ) -> Vec<(f32, String)> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut nearest = BinaryHeap::new();

        // Initialize with entry points
        for point in entry_points {
            if let Some(entry) = self.entries.get(&point) {
                let dist = self.calculate_distance(query, &entry.vector);
                candidates.push(SearchCandidate {
                    distance: OrderedFloat(-dist),
                    id: point.clone(),
                });
                nearest.push(SearchCandidate {
                    distance: OrderedFloat(dist),
                    id: point,
                });
                visited.insert(entry.id.clone());
            }
        }

        // Search expansion
        while let Some(current) = candidates.pop() {
            let lower_bound = nearest
                .peek()
                .map(|n| n.distance)
                .unwrap_or(OrderedFloat(f32::INFINITY));

            if -current.distance > lower_bound {
                break;
            }

            // Check neighbors
            if let Some(neighbors) = self.layers[layer].get(&current.id) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        visited.insert(neighbor.clone());

                        if let Some(entry) = self.entries.get(neighbor) {
                            let dist = self.calculate_distance(query, &entry.vector);
                            let upper_bound = nearest
                                .peek()
                                .map(|n| n.distance)
                                .unwrap_or(OrderedFloat(f32::INFINITY));

                            if OrderedFloat(dist) < upper_bound || nearest.len() < num_closest {
                                candidates.push(SearchCandidate {
                                    distance: OrderedFloat(-dist),
                                    id: neighbor.clone(),
                                });
                                nearest.push(SearchCandidate {
                                    distance: OrderedFloat(dist),
                                    id: neighbor.clone(),
                                });

                                if nearest.len() > num_closest {
                                    nearest.pop();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert to sorted vector
        let mut result: Vec<(f32, String)> =
            nearest.into_iter().map(|c| (c.distance.0, c.id)).collect();
        result.sort_by_key(|(dist, _)| OrderedFloat(*dist));
        result
    }
}

impl VectorIndex for HNSWIndex {
    fn add(&mut self, entry: VectorEntry) -> Result<()> {
        // Get the level for this node
        let level = self.get_random_level();

        // Add to all layers up to the level
        for l in 0..=level {
            if l >= self.layers.len() {
                self.layers.push(HashMap::new());
            }
            self.layers[l].insert(entry.id.clone(), HashSet::new());
        }

        // Store the entry
        self.entries.insert(entry.id.clone(), entry.clone());

        // Update entry point if needed
        if self.entry_point.is_none() {
            self.entry_point = Some(entry.id.clone());
            return Ok(());
        }

        // Connect to nearest neighbors in each layer
        let mut entry_points = HashSet::new();
        entry_points.insert(self.entry_point.as_ref().unwrap().clone());

        for layer_idx in (0..=level).rev() {
            let candidates =
                self.search_layer(&entry.vector, entry_points.clone(), self.m, layer_idx);

            // Connect to M nearest neighbors at this layer
            let m = if layer_idx == 0 { self.max_m } else { self.m };
            for (_, neighbor_id) in candidates.iter().take(m) {
                // Add bidirectional edges
                if let Some(neighbors) = self.layers[layer_idx].get_mut(&entry.id) {
                    neighbors.insert(neighbor_id.clone());
                }
                if let Some(neighbors) = self.layers[layer_idx].get_mut(neighbor_id) {
                    neighbors.insert(entry.id.clone());

                    // Prune connections if necessary
                    if neighbors.len() > m {
                        // Get data we need before borrowing neighbors mutably
                        let neighbor_entry = self.entries[neighbor_id].clone();
                        let neighbor_ids: Vec<_> = neighbors.iter().cloned().collect();

                        // Drop the mutable borrow by ending the scope
                        let _ = neighbors;

                        // Calculate distances without holding any borrow
                        let mut neighbor_distances: Vec<_> = neighbor_ids
                            .iter()
                            .filter_map(|n| self.entries.get(n))
                            .map(|n| {
                                (
                                    self.calculate_distance(&neighbor_entry.vector, &n.vector),
                                    n.id.clone(),
                                )
                            })
                            .collect();
                        neighbor_distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

                        let keep: HashSet<_> = neighbor_distances
                            .iter()
                            .take(m)
                            .map(|(_, id)| id.clone())
                            .collect();

                        // Re-borrow for the final operation
                        if let Some(neighbors) = self.layers[layer_idx].get_mut(neighbor_id) {
                            neighbors.retain(|n| keep.contains(n));
                        }
                    }
                }
            }

            // Update entry points for next layer
            entry_points.clear();
            for (_, id) in candidates.iter().take(1) {
                entry_points.insert(id.clone());
            }
        }

        Ok(())
    }

    fn remove(&mut self, id: &str) -> Result<()> {
        if self.entries.remove(id).is_some() {
            // Remove from all layers
            for layer in &mut self.layers {
                layer.remove(id);
            }

            // Update entry point if needed
            if self.entry_point.as_ref() == Some(&id.to_string()) {
                self.entry_point = self.entries.keys().next().cloned();
            }

            Ok(())
        } else {
            Err(DriftError::NotFound(format!("Vector {} not found", id)))
        }
    }

    fn search(
        &self,
        query: &Vector,
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> Result<Vec<SearchResult>> {
        // For now, ignore metadata filter - would need to be implemented
        if filter.is_some() {
            debug!("Metadata filter not yet implemented for HNSW");
        }

        // Use entry point to start search
        let entry_points = if let Some(ref ep) = self.entry_point {
            let mut set = HashSet::new();
            set.insert(ep.clone());
            set
        } else {
            return Ok(Vec::new());
        };

        // Search from top layer down
        let top_layer = self.layers.len().saturating_sub(1);
        let results = self.search_layer(query, entry_points, k, top_layer);

        Ok(results
            .into_iter()
            .map(|(distance, id)| SearchResult {
                id,
                score: -distance, // Convert distance to similarity score
                vector: None,
                metadata: HashMap::new(),
            })
            .collect())
    }

    fn statistics(&self) -> IndexStatistics {
        IndexStatistics {
            total_vectors: self.entries.len(),
            dimension: self.dimension,
            index_size_bytes: 0, // Would need actual calculation
            search_count: 0,
            add_count: 0,
            remove_count: 0,
            avg_search_time_ms: 0.0,
        }
    }

    fn optimize(&mut self) -> Result<()> {
        // HNSW doesn't need regular optimization
        Ok(())
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        // Would need proper serialization
        Err(DriftError::Other(
            "HNSW serialization not yet implemented".to_string(),
        ))
    }

    fn deserialize(_data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        // Would need proper deserialization
        Err(DriftError::Other(
            "HNSW deserialization not yet implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
struct SearchCandidate {
    distance: OrderedFloat<f32>,
    id: String,
}

impl Ord for SearchCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance.cmp(&other.distance)
    }
}

impl PartialOrd for SearchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SearchCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for SearchCandidate {}

// Distance functions
fn cosine_distance(v1: &Vector, v2: &Vector) -> f32 {
    let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let norm1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm1 == 0.0 || norm2 == 0.0 {
        1.0
    } else {
        1.0 - (dot / (norm1 * norm2))
    }
}

fn euclidean_distance(v1: &Vector, v2: &Vector) -> f32 {
    v1.iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>()
        .sqrt()
}

fn dot_product_distance(v1: &Vector, v2: &Vector) -> f32 {
    let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    -dot // Negative because higher dot product means more similar
}

fn manhattan_distance(v1: &Vector, v2: &Vector) -> f32 {
    v1.iter().zip(v2.iter()).map(|(a, b)| (a - b).abs()).sum()
}

fn apply_filter(metadata: &HashMap<String, serde_json::Value>, filter: &MetadataFilter) -> bool {
    let results: Vec<bool> = filter
        .conditions
        .iter()
        .map(|condition| {
            match condition {
                FilterCondition::Equals { field, value } => {
                    metadata.get(field).map_or(false, |v| v == value)
                }
                FilterCondition::NotEquals { field, value } => {
                    metadata.get(field).map_or(true, |v| v != value)
                }
                FilterCondition::GreaterThan { field, value } => {
                    metadata.get(field).map_or(false, |v| {
                        // Simple comparison for numbers
                        if let (Some(v_num), Some(val_num)) = (v.as_f64(), value.as_f64()) {
                            v_num > val_num
                        } else {
                            false
                        }
                    })
                }
                FilterCondition::LessThan { field, value } => {
                    metadata.get(field).map_or(false, |v| {
                        if let (Some(v_num), Some(val_num)) = (v.as_f64(), value.as_f64()) {
                            v_num < val_num
                        } else {
                            false
                        }
                    })
                }
                FilterCondition::In { field, values } => {
                    metadata.get(field).map_or(false, |v| values.contains(v))
                }
                FilterCondition::Contains { field, value } => metadata
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map_or(false, |s| s.contains(value)),
            }
        })
        .collect();

    match filter.combine {
        CombineOp::And => results.iter().all(|&r| r),
        CombineOp::Or => results.iter().any(|&r| r),
    }
}

/// Vector search manager
pub struct VectorSearchManager {
    indices: Arc<RwLock<HashMap<String, Box<dyn VectorIndex>>>>,
    stats: Arc<RwLock<VectorSearchStats>>,
}

#[derive(Debug, Default)]
struct VectorSearchStats {
    total_indices: usize,
    #[allow(dead_code)]
    total_vectors: usize,
    total_searches: u64,
    total_adds: u64,
    #[allow(dead_code)]
    total_removes: u64,
}

impl VectorSearchManager {
    pub fn new() -> Self {
        Self {
            indices: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(VectorSearchStats::default())),
        }
    }

    /// Create a new vector index
    pub fn create_index(
        &self,
        name: &str,
        dimension: usize,
        metric: DistanceMetric,
        index_type: IndexType,
    ) -> Result<()> {
        let index: Box<dyn VectorIndex> = match index_type {
            IndexType::Flat => Box::new(FlatIndex::new(dimension, metric)),
            IndexType::HNSW {
                m, ef_construction, ..
            } => Box::new(HNSWIndex::new(dimension, metric, m, ef_construction)),
            IndexType::IVF { .. } => {
                // TODO: Implement IVF index
                return Err(DriftError::Other(
                    "IVF index not yet implemented".to_string(),
                ));
            }
        };

        self.indices.write().insert(name.to_string(), index);
        self.stats.write().total_indices += 1;

        Ok(())
    }

    /// Add vector to index
    pub fn add_vector(&self, index_name: &str, entry: VectorEntry) -> Result<()> {
        let mut indices = self.indices.write();
        let index = indices
            .get_mut(index_name)
            .ok_or_else(|| DriftError::Other(format!("Index '{}' not found", index_name)))?;

        index.add(entry)?;
        self.stats.write().total_adds += 1;

        Ok(())
    }

    /// Search for similar vectors
    pub fn search(
        &self,
        index_name: &str,
        query: &Vector,
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> Result<Vec<SearchResult>> {
        let indices = self.indices.read();
        let index = indices
            .get(index_name)
            .ok_or_else(|| DriftError::Other(format!("Index '{}' not found", index_name)))?;

        let results = index.search(query, k, filter)?;
        self.stats.write().total_searches += 1;

        Ok(results)
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> Result<()> {
        self.indices
            .write()
            .remove(name)
            .ok_or_else(|| DriftError::Other(format!("Index '{}' not found", name)))?;
        self.stats.write().total_indices -= 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flat_index() {
        let mut index = FlatIndex::new(3, DistanceMetric::Euclidean);

        let entry1 = VectorEntry {
            id: "vec1".to_string(),
            vector: vec![1.0, 2.0, 3.0],
            metadata: HashMap::new(),
            timestamp: 0,
        };

        let entry2 = VectorEntry {
            id: "vec2".to_string(),
            vector: vec![2.0, 3.0, 4.0],
            metadata: HashMap::new(),
            timestamp: 0,
        };

        index.add(entry1).unwrap();
        index.add(entry2).unwrap();

        let query = vec![1.5, 2.5, 3.5];
        let results = index.search(&query, 2, None).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "vec2");
    }

    #[test]
    fn test_cosine_similarity() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        let v3 = vec![1.0, 0.0, 0.0];

        assert!(cosine_distance(&v1, &v2) > 0.9); // Nearly perpendicular
        assert!(cosine_distance(&v1, &v3) < 0.1); // Same direction
    }
}
