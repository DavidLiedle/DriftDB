# Query Optimization in DriftDB

## Overview

DriftDB includes a comprehensive cost-based query optimization system with table statistics collection, join optimization, and intelligent query planning.

## Architecture

### 1. **Statistics Collection** (`stats.rs` - 788 lines)

**Comprehensive Statistics:**
- **Table Statistics**: Row count, column count, data size, null counts
- **Column Statistics**: Cardinality, histograms, min/max values, null percentages
- **Index Statistics**: Height, pages, leaf pages, distinct values
- **Query Statistics**: Execution patterns, slow queries, error rates
- **System Statistics**: Memory usage, CPU utilization, disk I/O

**Features:**
- Automatic statistics collection (configurable interval, default 1 hour)
- Sampling for large tables (10% sample for tables > 10,000 rows)
- Histogram generation (50 buckets for numeric columns)
- Query execution tracking
- Resource monitoring

**Configuration:**
```rust
StatsConfig {
    auto_collect: true,
    collection_interval: Duration::from_secs(3600),
    histogram_buckets: 50,
    sample_percentage: 10.0,
    sample_threshold: 10000,
    track_queries: true,
    max_query_history: 1000,
    monitor_resources: true,
}
```

### 2. **Query Optimizer** (`optimizer.rs` - 898 lines)

**Optimization Techniques:**
- Predicate pushdown (move filters closer to data source)
- Projection pushdown (select only needed columns early)
- Join reordering (optimize join order based on cardinality)
- Snapshot selection (choose optimal snapshot for time-travel queries)
- Plan caching (cache optimized plans for repeated queries)

**Cost Model:**
- Sequential scan cost
- Index scan cost
- Join cost estimation
- Aggregation cost
- Sort cost

**Integration:**
- Maintains statistics cache
- Updates statistics via `update_statistics(table, stats)`
- Provides cost-based plan selection

### 3. **Cost-Based Optimizer** (`cost_optimizer.rs` - 855 lines)

**Advanced Features:**
- **Join Algorithms:**
  - Nested loop join
  - Hash join (with build side selection)
  - Sort-merge join

- **Plan Nodes:**
  - TableScan with cost
  - IndexScan with cost
  - Filter, Project, Sort, Aggregate
  - Limit with offset
  - Materialize (force materialization points)

- **Cost Calculation:**
  - I/O cost (page reads)
  - CPU cost (tuple processing)
  - Memory requirements
  - Network cost (for distributed)
  - Estimated row count
  - Estimated data size

**Cost Formula:**
```rust
total_cost = io_cost + (cpu_cost * 0.01) + (network_cost * 2.0)
```

**Dynamic Programming for Join Order:**
- Considers all join permutations
- Selects minimum-cost plan
- Handles complex multi-table joins

### 4. **Advanced Query Optimizer** (`query_optimizer.rs` - 1059 lines)

**Sophisticated Optimizations:**
- **Subquery Optimization:**
  - Subquery flattening
  - Subquery decorrelation
  - Materialization of expensive subqueries

- **Parallel Execution Planning:**
  - Parallel table scans
  - Parallel aggregations
  - Parallel joins

- **Index Selection:**
  - Multi-column index matching
  - Covering index detection
  - Index-only scans

- **Predicate Analysis:**
  - Range predicate detection
  - Equality predicate extraction
  - Index-compatible predicate identification

## Usage

### Collecting Statistics

**Manual Collection:**
```rust
// Collect statistics for a specific table
let stats = engine.collect_table_statistics("users")?;
optimizer.update_statistics("users", stats);

// Collect statistics for all tables
engine.analyze_all_tables(&optimizer)?;
```

**Automatic Collection:**
```rust
// Enable automatic statistics collection
let config = StatsConfig {
    auto_collect: true,
    collection_interval: Duration::from_secs(3600),
    ..Default::default()
};

// Automatic collection runs in background
engine.auto_collect_statistics()?;
```

### Query Optimization

**Optimizer automatically optimizes queries:**
```rust
// Create optimizer
let optimizer = QueryOptimizer::new();

// Update with statistics
optimizer.update_statistics("users", stats);

// Optimize a query
let optimized_plan = optimizer.optimize(query)?;
```

### Cost Estimation

```rust
// Estimate sequential scan cost
let cost = Cost::seq_scan(pages, rows);

// Estimate index scan cost
let cost = Cost::index_scan(index_pages, data_pages, rows);

// Total cost
let total = cost.total();
```

## Statistics Schema

### TableStatistics
```rust
{
    table_name: String,
    row_count: usize,
    column_count: usize,
    avg_row_size: usize,
    data_size_bytes: u64,
    page_count: usize,
    null_count: usize,
    last_updated: u64,
    columns: HashMap<String, ColumnStatistics>,
    indexes: HashMap<String, IndexStatistics>,
}
```

### ColumnStatistics
```rust
{
    column_name: String,
    data_type: String,
    null_count: usize,
    distinct_count: usize,
    min_value: Option<Value>,
    max_value: Option<Value>,
    avg_size: usize,
    histogram: Option<Histogram>,
}
```

### Histogram
```rust
{
    buckets: Vec<HistogramBucket>,
    total_count: usize,
}

HistogramBucket {
    lower_bound: Value,
    upper_bound: Value,
    count: usize,
    distinct_count: usize,
}
```

## Performance Impact

**Statistics Collection:**
- Minimal overhead (runs in background)
- Sampling reduces cost for large tables
- Incremental updates avoid full scans
- Default 1-hour interval balances freshness vs. cost

**Query Optimization:**
- Plan cache eliminates repeated optimization
- Cost-based selection improves query performance
- Join reordering can provide 10-100x speedups
- Index selection reduces I/O by orders of magnitude

## Best Practices

1. **Run ANALYZE regularly** for accurate statistics
2. **Update statistics after bulk data changes**
3. **Monitor statistics collection overhead**
4. **Adjust sample percentage for very large tables**
5. **Use EXPLAIN to verify optimizer choices**
6. **Monitor slow query log for optimization opportunities**

## Integration with Other Features

- **MVCC**: Statistics respect transaction isolation
- **Snapshots**: Optimizer selects optimal snapshots
- **Indexes**: Statistics guide index selection
- **Slow Query Log**: Identifies optimization opportunities
- **Metrics**: Statistics exported to Prometheus
- **Monitoring**: Query performance tracked via statistics

## Status

✅ **Fully Implemented**
- Cost-based query optimizer
- Comprehensive table statistics collection
- Histogram generation for selectivity estimation
- Automatic statistics maintenance
- Join order optimization
- Predicate pushdown
- Index selection
- Plan caching

🔄 **Enhancements Possible (Future):**
- Multi-column statistics (column correlation)
- Query feedback loop (learn from execution)
- Adaptive query optimization
- Machine learning-based cost estimation
- Distributed query planning

## EXPLAIN and EXPLAIN ANALYZE

The system provides comprehensive query plan visualization:

### Features
- **Multiple Output Formats**: Text (tree view), JSON, YAML
- **Cost Estimates**: I/O cost, CPU cost, memory, network cost per plan node
- **Execution Metrics**: Actual row counts, execution time, planning time
- **Accuracy Metrics**: Compare estimated vs. actual rows
- **Verbose Mode**: Show predicates, join conditions, column projections

### Usage

```rust
use driftdb_core::{ExplainExecutor, ExplainOptions, ExplainFormat};
use std::time::Duration;

// EXPLAIN without execution
let plan = optimizer.optimize(query)?;
let explain = ExplainExecutor::explain(plan, Duration::from_millis(5));

// Display as text
println!("{}", explain.format_text(&ExplainOptions::default()));

// EXPLAIN ANALYZE with execution
let explain = ExplainExecutor::explain_analyze(
    plan,
    Duration::from_millis(5),
    || {
        // Execute query and return row count
        let result = engine.execute(query)?;
        Ok(result.row_count)
    }
)?;

// Display as JSON
println!("{}", explain.format_json()?);
```

### Output Example

```
Query Plan
============================================================
└─ Table Scan on users (cost=100.10, rows=10000)

============================================================
Planning Time: 5.000 ms
Total Cost: 100.10
Estimated Rows: 10000
Actual Rows: 9876 (accuracy: 98.76%)
Execution Time: 23.456 ms
```

### Options

```rust
ExplainOptions {
    format: ExplainFormat::Text,  // Text, Json, Yaml, Tree
    verbose: false,                // Show detailed info
    costs: true,                   // Show cost estimates
    buffers: false,                // Show buffer usage
    timing: true,                  // Show timing info
    analyze: false,                // Execute and show actual metrics
}
```

## Files

- `crates/driftdb-core/src/stats.rs` - Statistics collection (788 lines)
- `crates/driftdb-core/src/optimizer.rs` - Query optimizer (898 lines)
- `crates/driftdb-core/src/cost_optimizer.rs` - Cost-based optimizer (855 lines)
- `crates/driftdb-core/src/query_optimizer.rs` - Advanced optimizer (1059 lines)
- `crates/driftdb-core/src/explain.rs` - EXPLAIN implementation (546 lines)

**Total: 4,146 lines of query optimization code**
