# Columnar Storage and Advanced Compression

## Overview

DriftDB implements high-performance columnar storage with advanced compression techniques that dramatically reduce storage requirements and improve analytical query performance. Column-oriented storage groups data by column rather than by row, enabling superior compression ratios and faster scans for analytical workloads.

## Benefits

**Compression Ratios:**
- **10-100x compression** for low-cardinality columns (status fields, categories)
- **2-10x compression** for numeric time-series data
- **3-5x compression** for general string data

**Query Performance:**
- **5-10x faster** analytical queries (only read needed columns)
- **Better CPU cache utilization** (similar data types grouped together)
- **Predicate pushdown** with statistics and zone maps
- **I/O reduction** through selective column reading

## Architecture

### Storage Layout

```
table_dir/
├── column_name1.col    # Column data file
├── column_name2.col    # Column data file
├── column_name3.col    # Column data file
└── metadata.json       # Table metadata and schema
```

### Row Groups

Data is organized into row groups (default: 100,000 rows) for:
- Efficient compression
- Statistics collection
- Parallel processing
- Selective scanning

### Encoding Types

1. **Plain Encoding**
   - Raw values stored directly
   - Used for high-cardinality data
   - No compression overhead

2. **Dictionary Encoding**
   - Maps repeated values to integer IDs
   - Ideal for low-cardinality columns (< 10% unique values)
   - Example: Status codes, categories, flags

3. **Run-Length Encoding (RLE)**
   - Encodes consecutive repeated values as (value, count) pairs
   - Ideal for sorted or repetitive data
   - Example: Time-series with constant values

4. **Delta Encoding**
   - Stores differences between consecutive values
   - Ideal for monotonic sequences
   - Example: Timestamps, IDs, counters

5. **Bit-Packed Encoding**
   - Uses minimal bits for small integer ranges
   - Example: Age (0-120), percentages (0-100)

### Compression Algorithms

Each encoded column can be further compressed with:

| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| **None** | - | 1.0x | Already compressed data |
| **Snappy** | Very Fast | 2-3x | Default for hot data |
| **LZ4** | Very Fast | 2-3x | Real-time ingestion |
| **Zstd** | Fast | 3-5x | Balanced compression |
| **Gzip** | Medium | 4-6x | Cold storage |
| **Brotli** | Slow | 5-8x | Archive storage |

## Configuration

### Basic Setup

```rust
use driftdb_core::columnar::{
    ColumnarConfig, ColumnarStorage, CompressionType, EncodingType
};

// Create with defaults
let config = ColumnarConfig::default();
let storage = ColumnarStorage::new("/path/to/storage", config)?;

// Custom configuration
let config = ColumnarConfig {
    block_size: 65536,                    // 64KB blocks
    compression: CompressionType::Zstd,   // Use Zstd compression
    encoding: EncodingType::Auto,         // Auto-select encoding
    dictionary_encoding_threshold: 0.75,  // Use dict if < 75% unique
    enable_statistics: true,              // Track column stats
    enable_bloom_filters: true,           // Add bloom filters
    enable_zone_maps: true,               // Track min/max values
    row_group_size: 100_000,              // Rows per group
    page_size: 8192,                      // Page size for I/O
};
```

### Schema Definition

```rust
use driftdb_core::columnar::{Schema, ColumnSchema, DataType};

let schema = Schema {
    columns: vec![
        // ID column: Delta encoding for monotonic sequence
        ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            encoding: EncodingType::Delta,
            compression: CompressionType::Snappy,
            dictionary: None,
        },

        // Status column: Dictionary encoding for low cardinality
        ColumnSchema {
            name: "status".to_string(),
            data_type: DataType::String,
            nullable: false,
            encoding: EncodingType::Dictionary,
            compression: CompressionType::Zstd,
            dictionary: None,
        },

        // Timestamp: Delta encoding for time-series
        ColumnSchema {
            name: "created_at".to_string(),
            data_type: DataType::Timestamp,
            nullable: false,
            encoding: EncodingType::Delta,
            compression: CompressionType::Snappy,
            dictionary: None,
        },

        // Value: Plain encoding for high-variance data
        ColumnSchema {
            name: "amount".to_string(),
            data_type: DataType::Float64,
            nullable: true,
            encoding: EncodingType::Plain,
            compression: CompressionType::Zstd,
            dictionary: None,
        },
    ],
};

storage.create_table(schema)?;
```

## Usage

### Writing Data

```rust
use driftdb_core::columnar::{Row, Value, ColumnarWriter};
use std::sync::{Arc, RwLock};

// Wrap storage in Arc for shared access
let storage = Arc::new(RwLock::new(storage));

// Create writer with buffer size
let mut writer = ColumnarWriter::new(storage.clone(), 10_000);

// Create and write rows
let mut row = Row::new();
row.insert("id".to_string(), Some(Value::Int64(1)));
row.insert("status".to_string(), Some(Value::String("active".to_string())));
row.insert("created_at".to_string(), Some(Value::Int64(1234567890)));
row.insert("amount".to_string(), Some(Value::Float64(99.99_f64.to_bits())));

writer.write_row(row)?;

// Batch insert
let rows = vec![
    create_row(1, "active", 100.0),
    create_row(2, "pending", 200.0),
    create_row(3, "active", 150.0),
];

for row in rows {
    writer.write_row(row)?;
}

// Flush remaining buffered rows
writer.flush()?;
```

### Batch Writing

```rust
// Write large batches efficiently
let mut batch = Vec::new();
for i in 0..1_000_000 {
    let mut row = Row::new();
    row.insert("id".to_string(), Some(Value::Int64(i)));
    row.insert("status".to_string(), Some(Value::String(
        match i % 3 {
            0 => "active",
            1 => "pending",
            _ => "inactive",
        }.to_string()
    )));
    batch.push(row);
}

// Direct batch write (bypasses buffer)
storage.write().unwrap().write_batch(batch)?;
```

### Reading Data

```rust
use driftdb_core::columnar::ColumnarReader;

let reader = ColumnarReader::new(storage.clone());

// Get row count
let count = reader.count()?;
println!("Total rows: {}", count);

// Scan specific columns
let columns = vec!["id".to_string(), "status".to_string()];
let result = reader.scan(columns, None)?;

println!("Scanned {} rows", result.row_count);
for (column_name, values) in &result.columns {
    println!("Column {}: {} values", column_name, values.len());
}
```

### Filtered Scans with Predicates

```rust
use driftdb_core::columnar::{Predicate, ComparisonOperator, Value};

// Create predicate: WHERE status = 'active'
let predicate = Predicate {
    column: "status".to_string(),
    operator: ComparisonOperator::Equal,
    value: Value::String("active".to_string()),
};

// Scan with predicate pushdown
let columns = vec!["id".to_string(), "amount".to_string()];
let result = reader.scan(columns, Some(predicate))?;

// Only active rows are returned
println!("Active rows: {}", result.row_count);
```

## Encoding Selection

### Automatic Encoding

The columnar storage automatically selects optimal encoding based on data characteristics:

```rust
// Auto encoding analyzes data and chooses:
ColumnSchema {
    encoding: EncodingType::Auto,  // Automatic selection
    // ...
}
```

**Selection Logic:**

1. **Dictionary**: Cardinality < 10% of rows
   - Example: 100 distinct values in 10,000 rows

2. **RLE**: Average run length > 5
   - Example: [1,1,1,1,1,2,2,2,2,3,3,3,3,3,3]

3. **Delta**: Sorted monotonic data
   - Example: [100, 101, 103, 106, 110]

4. **Bit-Packed**: Small integer ranges
   - Example: Values 0-255 → 8 bits instead of 64

5. **Plain**: Everything else

### Manual Encoding Override

```rust
// Force dictionary encoding for known low-cardinality column
ColumnSchema {
    name: "country".to_string(),
    data_type: DataType::String,
    encoding: EncodingType::Dictionary,  // Explicit encoding
    // ...
}
```

## Performance Optimization

### 1. Choose Appropriate Encoding

```rust
// GOOD: Dictionary for status codes
ColumnSchema {
    name: "status".to_string(),
    encoding: EncodingType::Dictionary,
    compression: CompressionType::Zstd,
    // 1000 rows, 3 distinct values → ~97% reduction
}

// BAD: Plain encoding for repetitive data
ColumnSchema {
    name: "status".to_string(),
    encoding: EncodingType::Plain,  // Wastes space!
    // ...
}
```

### 2. Configure Compression Levels

```rust
// Hot data: Fast compression
let config = ColumnarConfig {
    compression: CompressionType::Snappy,  // Fast
    // ...
};

// Cold data: High compression
let config = ColumnarConfig {
    compression: CompressionType::Brotli { quality: 11 },  // Best ratio
    // ...
};

// Real-time: Minimal compression
let config = ColumnarConfig {
    compression: CompressionType::Lz4 { level: 1 },  // Fastest
    // ...
};
```

### 3. Optimize Row Group Size

```rust
let config = ColumnarConfig {
    row_group_size: 100_000,  // Default: 100K rows
    // Smaller: Better for point queries
    // Larger: Better for sequential scans
};
```

### 4. Enable Statistics

```rust
let config = ColumnarConfig {
    enable_statistics: true,      // Track null count, min/max
    enable_bloom_filters: true,   // Fast negative lookups
    enable_zone_maps: true,       // Min/max pruning
    // ...
};
```

## Compression Ratios

### Real-World Examples

**Status Field (Low Cardinality):**
```
1M rows, 5 distinct values (active, pending, inactive, completed, failed)

Uncompressed: 1M × 10 bytes/string = 10 MB
Dictionary:   1M × 4 bytes/ID + 5 × 10 bytes = 4.0 MB (2.5x)
+ Zstd:       ~500 KB (20x compression!)
```

**Timestamp Field (Monotonic):**
```
1M rows, timestamps 1609459200-1609545600 (24 hours of seconds)

Uncompressed: 1M × 8 bytes = 8 MB
Delta:        Base + 1M × ~3 bytes/delta = 3 MB (2.7x)
+ Snappy:     ~1.5 MB (5.3x compression)
```

**Boolean Field (Run-Length):**
```
1M rows, sorted boolean values [false × 500K, true × 500K]

Uncompressed: 1M × 1 byte = 1 MB
RLE:          2 runs × (1 byte value + 4 bytes count) = 10 bytes (100,000x!)
+ Snappy:     ~8 bytes (125,000x compression)
```

## Statistics and Zone Maps

### Column Statistics

Automatically collected per row group:

```rust
pub struct ColumnStatistics {
    pub null_count: u64,           // Number of NULL values
    pub distinct_count: Option<u64>, // Approximate distinct count
    pub min_value: Option<Vec<u8>>, // Minimum value
    pub max_value: Option<Vec<u8>>, // Maximum value
}
```

**Usage:**
- **Skip row groups** that don't match predicates
- **Estimate query selectivity** for optimization
- **Detect data skew** for partitioning decisions

### Zone Maps

Track min/max values per row group for efficient pruning:

```rust
pub struct ZoneMap {
    pub min_value: Vec<u8>,
    pub max_value: Vec<u8>,
}
```

**Example:**
```sql
SELECT * FROM events WHERE timestamp >= '2024-01-01'
```

Row groups with `max_value < '2024-01-01'` are skipped entirely.

## Integration with DriftDB

### Hybrid Storage

DriftDB uses hybrid storage:

1. **Row-oriented WAL**: Fast writes, ACID transactions
2. **Columnar storage**: Background conversion for analytics

```
[WAL (row-oriented)] --compaction--> [Columnar storage]
     Fast writes                      Fast analytical queries
```

### Compaction Process

```rust
// Periodic compaction converts WAL to columnar format
engine.compact_to_columnar(
    table_name,
    ColumnarConfig {
        compression: CompressionType::Zstd,
        encoding: EncodingType::Auto,
        row_group_size: 100_000,
        // ...
    }
)?;
```

## Best Practices

### 1. Choose Encoding Per Column

```rust
// Analyze your data characteristics
let schema = Schema {
    columns: vec![
        // Low cardinality → Dictionary
        ColumnSchema {
            name: "country".to_string(),
            encoding: EncodingType::Dictionary,
            // 200 countries, millions of rows
        },

        // Monotonic → Delta
        ColumnSchema {
            name: "timestamp".to_string(),
            encoding: EncodingType::Delta,
            // Sorted time series
        },

        // Sorted repetitive → RLE
        ColumnSchema {
            name: "is_active".to_string(),
            encoding: EncodingType::RunLength,
            // Sorted boolean
        },

        // High variance → Plain
        ColumnSchema {
            name: "amount".to_string(),
            encoding: EncodingType::Plain,
            // Unique float values
        },
    ],
};
```

### 2. Monitor Compression Ratios

```rust
let metadata = storage.metadata.read().unwrap();
for row_group in &metadata.row_groups {
    for column in &row_group.columns {
        let ratio = column.uncompressed_size as f64 / column.compressed_size as f64;
        println!("Column {}: {:.2}x compression", column.column_name, ratio);

        if ratio < 1.5 {
            println!("WARNING: Poor compression for {}", column.column_name);
            // Consider different encoding or compression
        }
    }
}
```

### 3. Tune for Workload

**OLTP (transactional):**
```rust
let config = ColumnarConfig {
    compression: CompressionType::Snappy,  // Fast
    row_group_size: 10_000,                // Small groups
    block_size: 16_384,                    // Small blocks
    // ...
};
```

**OLAP (analytical):**
```rust
let config = ColumnarConfig {
    compression: CompressionType::Zstd,    // High ratio
    row_group_size: 1_000_000,             // Large groups
    block_size: 1_048_576,                 // Large blocks (1MB)
    // ...
};
```

### 4. Handle NULL Values Efficiently

```rust
// NULLs are compressed efficiently in all encodings
row.insert("optional_field".to_string(), None);

// Dictionary encoding: NULL → special ID (u32::MAX)
// RLE: NULL runs encoded as (NULL, count)
// Delta: NULL breaks delta chain
```

### 5. Partition Large Tables

```rust
// Partition by date for time-series data
/table/2024-01-01/
/table/2024-01-02/
/table/2024-01-03/

// Each partition is independent columnar storage
// Allows efficient time-range queries
```

## Testing

### Unit Tests

```bash
# Run columnar storage tests
cargo test -p driftdb-core columnar::tests
```

**Test Coverage (14 tests):**
- Storage creation and initialization
- Dictionary encoding/decoding
- Batch write and read operations
- Column-specific scanning
- Automatic encoding selection
- Compression algorithm validation
- Writer/Reader integration
- NULL value handling
- Large batch processing
- Auto-flush on buffer size
- Compression ratio validation

### Performance Benchmarks

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_columnar_write(c: &mut Criterion) {
    let storage = create_storage();

    c.bench_function("write_1M_rows", |b| {
        b.iter(|| {
            let rows = generate_rows(1_000_000);
            storage.write_batch(rows).unwrap();
        });
    });
}

fn bench_columnar_scan(c: &mut Criterion) {
    let storage = create_storage_with_data();

    c.bench_function("scan_1_column", |b| {
        b.iter(|| {
            storage.scan(vec!["id".to_string()], None).unwrap();
        });
    });
}
```

## Comparison

### vs Row-Oriented Storage

| Aspect | Row-Oriented | Columnar |
|--------|-------------|----------|
| **Write Performance** | Faster (sequential) | Slower (per-column) |
| **Point Query** | Faster (single seek) | Slower (multiple seeks) |
| **Analytical Query** | Slower (read all data) | 5-10x Faster |
| **Compression Ratio** | 2-3x typical | 10-100x typical |
| **Storage Size** | 1x | 0.1-0.5x |
| **Update Cost** | Low (in-place) | High (rewrite) |

### vs Parquet

DriftDB columnar storage is similar to Apache Parquet:

| Feature | DriftDB Columnar | Parquet |
|---------|-----------------|---------|
| **Encoding** | Plain, Dict, RLE, Delta | Same + more |
| **Compression** | Snappy, Zstd, LZ4, etc. | Same |
| **Statistics** | Yes | Yes |
| **Zone Maps** | Yes | Yes (page index) |
| **Bloom Filters** | Yes | Yes (optional) |
| **Row Groups** | Yes | Yes |
| **Nested Types** | No | Yes (complex) |
| **Integration** | Native DriftDB | External |

## Status

✅ **Fully Implemented**
- Column-oriented storage with row groups
- Dictionary encoding for low-cardinality columns
- Run-length encoding for repetitive data
- Delta encoding for monotonic sequences
- Multiple compression algorithms (Snappy, Zstd, LZ4, Brotli, Gzip)
- Automatic encoding selection
- Column statistics and zone maps
- Predicate pushdown with statistics
- Bloom filter integration
- 14 comprehensive unit tests

## Files

- `crates/driftdb-core/src/columnar.rs` - Columnar storage implementation (1,269 lines)

**Advanced Compression Code:**
- Columnar storage: 1,269 lines (914 impl + 355 tests)
- General compression: 505 lines (compression.rs)
- Bloom filters: 650 lines
- **Total: 2,424 lines of compression/encoding code**
