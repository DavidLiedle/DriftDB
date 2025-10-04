use crate::errors::{DriftError, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Compression algorithms supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Zstandard compression
    Zstd { level: i32 },
    /// Snappy compression (fast)
    Snappy,
    /// LZ4 compression (very fast)
    Lz4 { level: u32 },
    /// Gzip compression
    Gzip { level: u32 },
    /// Brotli compression (high ratio)
    Brotli { quality: u32 },
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::Zstd { level: 3 }
    }
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Default compression algorithm
    pub default_algorithm: CompressionAlgorithm,
    /// Minimum size threshold for compression (bytes)
    pub min_size_threshold: usize,
    /// Maximum compression attempts before giving up
    pub max_attempts: usize,
    /// Whether to compress in background threads
    pub async_compression: bool,
    /// Table-specific compression settings
    pub table_configs: HashMap<String, TableCompressionConfig>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            default_algorithm: CompressionAlgorithm::default(),
            min_size_threshold: 1024, // 1KB
            max_attempts: 3,
            async_compression: true,
            table_configs: HashMap::new(),
        }
    }
}

/// Table-specific compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCompressionConfig {
    /// Algorithm for this table
    pub algorithm: CompressionAlgorithm,
    /// Column-specific compression
    pub column_compression: HashMap<String, CompressionAlgorithm>,
    /// Whether to compress indexes
    pub compress_indexes: bool,
    /// Whether to compress WAL entries
    pub compress_wal: bool,
}

/// Compression statistics
#[derive(Debug, Default)]
pub struct CompressionStats {
    pub bytes_compressed: u64,
    pub bytes_decompressed: u64,
    pub compression_ratio: f64,
    pub compression_time_ms: u64,
    pub decompression_time_ms: u64,
    pub failed_compressions: u64,
}

/// Main compression manager
pub struct CompressionManager {
    config: CompressionConfig,
    stats: Arc<RwLock<CompressionStats>>,
    cache: Arc<RwLock<CompressionCache>>,
}

/// Cache for frequently accessed compressed data
struct CompressionCache {
    entries: lru::LruCache<Vec<u8>, Vec<u8>>,
    hit_rate: f64,
    hits: u64,
    misses: u64,
}

impl CompressionCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: lru::LruCache::new(capacity.try_into().unwrap()),
            hit_rate: 0.0,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let result = self.entries.get(key).cloned();
        if result.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        self.update_hit_rate();
        result
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.entries.put(key, value);
    }

    fn update_hit_rate(&mut self) {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hit_rate = self.hits as f64 / total as f64;
        }
    }
}

impl CompressionManager {
    pub fn new(config: CompressionConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(CompressionStats::default())),
            cache: Arc::new(RwLock::new(CompressionCache::new(1000))),
        }
    }

    /// Compress data using the specified algorithm
    pub fn compress(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        // Check minimum size threshold
        if data.len() < self.config.min_size_threshold {
            return Ok(data.to_vec());
        }

        let start = std::time::Instant::now();

        let result = match algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),

            CompressionAlgorithm::Zstd { level } => zstd::encode_all(data, level)
                .map_err(|e| DriftError::Internal(format!("Zstd compression failed: {}", e))),

            CompressionAlgorithm::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder
                    .compress_vec(data)
                    .map_err(|e| DriftError::Internal(format!("Snappy compression failed: {}", e)))
            }

            CompressionAlgorithm::Lz4 { level } => {
                use std::io::Write;

                let mut compressed = Vec::new();
                lz4::EncoderBuilder::new()
                    .level(level)
                    .build(&mut compressed)
                    .and_then(|mut encoder| {
                        encoder.write_all(data)?;
                        encoder.finish().1
                    })
                    .map_err(|e| DriftError::Internal(format!("LZ4 compression failed: {}", e)))?;
                Ok(compressed)
            }

            CompressionAlgorithm::Gzip { level } => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;

                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
                encoder
                    .write_all(data)
                    .and_then(|_| encoder.finish())
                    .map_err(|e| DriftError::Internal(format!("Gzip compression failed: {}", e)))
            }

            CompressionAlgorithm::Brotli { quality } => {
                let mut compressed = Vec::new();
                brotli::BrotliCompress(
                    &mut std::io::Cursor::new(data),
                    &mut compressed,
                    &brotli::enc::BrotliEncoderParams {
                        quality: quality as i32,
                        ..Default::default()
                    },
                )
                .map_err(|e| DriftError::Internal(format!("Brotli compression failed: {}", e)))?;
                Ok(compressed)
            }
        };

        // Update statistics
        if let Ok(ref compressed) = result {
            let elapsed = start.elapsed();
            let mut stats = self.stats.write();
            stats.bytes_compressed += data.len() as u64;
            stats.compression_time_ms += elapsed.as_millis() as u64;
            stats.compression_ratio = compressed.len() as f64 / data.len() as f64;
        } else {
            let mut stats = self.stats.write();
            stats.failed_compressions += 1;
        }

        result
    }

    /// Decompress data using the specified algorithm
    pub fn decompress(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        // Check cache first
        {
            let mut cache = self.cache.write();
            if let Some(decompressed) = cache.get(data) {
                return Ok(decompressed);
            }
        }

        let start = std::time::Instant::now();

        let result = match algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),

            CompressionAlgorithm::Zstd { .. } => zstd::decode_all(data)
                .map_err(|e| DriftError::Internal(format!("Zstd decompression failed: {}", e))),

            CompressionAlgorithm::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                decoder.decompress_vec(data).map_err(|e| {
                    DriftError::Internal(format!("Snappy decompression failed: {}", e))
                })
            }

            CompressionAlgorithm::Lz4 { .. } => {
                let mut decompressed = Vec::new();
                lz4::Decoder::new(std::io::Cursor::new(data))
                    .and_then(|mut decoder| {
                        std::io::copy(&mut decoder, &mut decompressed)?;
                        Ok(decompressed)
                    })
                    .map_err(|e| DriftError::Internal(format!("LZ4 decompression failed: {}", e)))
            }

            CompressionAlgorithm::Gzip { .. } => {
                use flate2::read::GzDecoder;
                use std::io::Read;

                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    DriftError::Internal(format!("Gzip decompression failed: {}", e))
                })?;
                Ok(decompressed)
            }

            CompressionAlgorithm::Brotli { .. } => {
                let mut decompressed = Vec::new();
                brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)
                    .map_err(|e| {
                        DriftError::Internal(format!("Brotli decompression failed: {}", e))
                    })?;
                Ok(decompressed)
            }
        };

        // Update statistics and cache
        if let Ok(ref decompressed) = result {
            let elapsed = start.elapsed();
            let mut stats = self.stats.write();
            stats.bytes_decompressed += decompressed.len() as u64;
            stats.decompression_time_ms += elapsed.as_millis() as u64;

            // Add to cache
            let mut cache = self.cache.write();
            cache.put(data.to_vec(), decompressed.clone());
        }

        result
    }

    /// Choose optimal compression algorithm based on data characteristics
    pub fn choose_algorithm(&self, data: &[u8], table: Option<&str>) -> CompressionAlgorithm {
        // Check table-specific configuration
        if let Some(table_name) = table {
            if let Some(table_config) = self.config.table_configs.get(table_name) {
                return table_config.algorithm;
            }
        }

        // Analyze data characteristics
        let entropy = self.estimate_entropy(data);
        let size = data.len();

        // Choose algorithm based on heuristics
        if size < 100 {
            CompressionAlgorithm::None
        } else if entropy < 3.0 {
            // Low entropy - highly compressible
            CompressionAlgorithm::Zstd { level: 5 }
        } else if entropy < 5.0 {
            // Medium entropy
            CompressionAlgorithm::Snappy
        } else if size > 1_000_000 {
            // Large data with high entropy
            CompressionAlgorithm::Lz4 { level: 1 }
        } else {
            self.config.default_algorithm
        }
    }

    /// Estimate entropy of data (simplified Shannon entropy)
    fn estimate_entropy(&self, data: &[u8]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }

        let mut frequencies = [0u64; 256];
        for &byte in data {
            frequencies[byte as usize] += 1;
        }

        let len = data.len() as f64;
        let mut entropy = 0.0;

        for &freq in &frequencies {
            if freq > 0 {
                let p = freq as f64 / len;
                entropy -= p * p.log2();
            }
        }

        entropy
    }

    /// Compress data in parallel chunks
    pub async fn compress_parallel(
        &self,
        data: Vec<u8>,
        algorithm: CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        if data.len() < 1_000_000 {
            // For small data, use regular compression
            return self.compress(&data, algorithm);
        }

        use rayon::prelude::*;

        let chunk_size = 256 * 1024; // 256KB chunks
        let chunks: Vec<_> = data.chunks(chunk_size).collect();

        let compressed_chunks: Result<Vec<_>> = chunks
            .par_iter()
            .map(|chunk| self.compress(chunk, algorithm))
            .collect();

        let compressed_chunks = compressed_chunks?;

        // Combine chunks with metadata
        let mut result = Vec::new();

        // Write header
        result.extend_from_slice(&(compressed_chunks.len() as u32).to_le_bytes());

        // Write each chunk with its size
        for chunk in compressed_chunks {
            result.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            result.extend_from_slice(&chunk);
        }

        Ok(result)
    }

    /// Get compression statistics
    pub fn stats(&self) -> CompressionStats {
        let stats = self.stats.read();
        CompressionStats {
            bytes_compressed: stats.bytes_compressed,
            bytes_decompressed: stats.bytes_decompressed,
            compression_ratio: stats.compression_ratio,
            compression_time_ms: stats.compression_time_ms,
            decompression_time_ms: stats.decompression_time_ms,
            failed_compressions: stats.failed_compressions,
        }
    }

    /// Configure table-specific compression
    pub fn configure_table(&mut self, table: String, config: TableCompressionConfig) {
        self.config.table_configs.insert(table, config);
    }

    /// Benchmark compression algorithms on given data
    pub fn benchmark(&self, data: &[u8]) -> HashMap<String, BenchmarkResult> {
        let algorithms = vec![
            ("None", CompressionAlgorithm::None),
            ("Zstd-3", CompressionAlgorithm::Zstd { level: 3 }),
            ("Zstd-9", CompressionAlgorithm::Zstd { level: 9 }),
            ("Snappy", CompressionAlgorithm::Snappy),
            ("LZ4", CompressionAlgorithm::Lz4 { level: 1 }),
            ("Gzip-6", CompressionAlgorithm::Gzip { level: 6 }),
            ("Brotli-5", CompressionAlgorithm::Brotli { quality: 5 }),
        ];

        let mut results = HashMap::new();

        for (name, algo) in algorithms {
            let start = std::time::Instant::now();
            let compressed = self.compress(data, algo);
            let compress_time = start.elapsed();

            if let Ok(compressed_data) = compressed {
                let start = std::time::Instant::now();
                let _ = self.decompress(&compressed_data, algo);
                let decompress_time = start.elapsed();

                results.insert(
                    name.to_string(),
                    BenchmarkResult {
                        compressed_size: compressed_data.len(),
                        compression_ratio: compressed_data.len() as f64 / data.len() as f64,
                        compress_time_ms: compress_time.as_millis() as u64,
                        decompress_time_ms: decompress_time.as_millis() as u64,
                    },
                );
            }
        }

        results
    }
}

#[derive(Debug)]
pub struct BenchmarkResult {
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub compress_time_ms: u64,
    pub decompress_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_algorithms() {
        let manager = CompressionManager::new(CompressionConfig::default());
        let data = b"Hello, World! This is a test of compression algorithms. ".repeat(100);

        // Test each algorithm
        let algorithms = vec![
            CompressionAlgorithm::None,
            CompressionAlgorithm::Zstd { level: 3 },
            CompressionAlgorithm::Snappy,
            CompressionAlgorithm::Lz4 { level: 1 },
            CompressionAlgorithm::Gzip { level: 6 },
            CompressionAlgorithm::Brotli { quality: 5 },
        ];

        for algo in algorithms {
            let compressed = manager.compress(&data, algo).unwrap();
            let decompressed = manager.decompress(&compressed, algo).unwrap();
            assert_eq!(data, decompressed.as_slice());
        }
    }

    #[test]
    fn test_entropy_estimation() {
        let manager = CompressionManager::new(CompressionConfig::default());

        // Low entropy (repetitive)
        let low_entropy = vec![0u8; 1000];
        assert!(manager.estimate_entropy(&low_entropy) < 1.0);

        // High entropy (random)
        let high_entropy: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        assert!(manager.estimate_entropy(&high_entropy) > 5.0);
    }

    #[test]
    fn test_algorithm_selection() {
        let manager = CompressionManager::new(CompressionConfig::default());

        // Small data - no compression
        let small = vec![1u8; 50];
        assert_eq!(
            manager.choose_algorithm(&small, None),
            CompressionAlgorithm::None
        );

        // Highly compressible - Zstd
        let repetitive = vec![0u8; 10000];
        match manager.choose_algorithm(&repetitive, None) {
            CompressionAlgorithm::Zstd { .. } => {}
            _ => panic!("Expected Zstd for repetitive data"),
        }
    }
}
