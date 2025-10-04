# Changelog

## [0.2.0] - 2024-12-20

### Added
- **Advanced Indexing Strategies**
  - B+ Tree indexes for efficient range queries
  - Hash indexes for O(1) point lookups
  - Bitmap indexes for low-cardinality columns
  - Bloom filters for membership testing
  - LSM tree storage engine for write-optimized workloads

- **Full MVCC Transaction Support**
  - Multi-version concurrency control implementation
  - Support for all standard isolation levels
  - Snapshot isolation for consistent reads
  - Distributed transaction coordination
  - Automatic garbage collection of old versions

- **Query Plan Visualization and Analysis**
  - Visual representation of query execution plans
  - Multiple output formats (Text, JSON, DOT, HTML)
  - Cost estimation and optimization hints
  - Runtime statistics tracking
  - Performance bottleneck identification

- **Enterprise-Grade Replication**
  - Advanced Raft consensus implementation
  - Pre-vote optimization for stability
  - Support for learner and witness nodes
  - Pipelined replication for performance
  - Automatic leader election and failover

- **Columnar Storage Engine**
  - Column-oriented data storage
  - Multiple compression algorithms (Snappy, Zstd, LZ4)
  - Adaptive encoding strategies
  - Dictionary and run-length encoding
  - Zone maps for partition pruning

- **Distributed Query Processing**
  - Consistent hashing for data distribution
  - Multi-node query coordination
  - Partition-aware query routing
  - Cross-node join optimization
  - Automatic rebalancing

- **Additional Enterprise Features**
  - Query result caching with LRU eviction
  - Parallel query execution
  - Database views (regular and materialized)
  - Triggers and stored procedures
  - Full-text search with TF-IDF
  - Window functions
  - Adaptive connection pooling

### Fixed
- Compilation errors in transaction module
- Move/borrow issues in consensus implementation
- Type mismatches in statistics collection
- Circuit breaker state management
- Serialization issues with timestamps

### Changed
- Reorganized repository structure
  - Moved test files to `client_test/` folder
  - Moved documentation to `docs/` folder
  - Moved scripts to `bin/` folder
- Enhanced query optimizer with more optimization rules
- Improved connection pool with adaptive sizing

## [0.1.0] - 2024-12-19

### Initial Release
- Core append-only storage engine
- Basic time-travel queries with AS OF clause
- DriftQL parser and executor
- B-tree secondary indexes
- Snapshot management with compression
- CRC32 data integrity checking
- Process-level file locking
- Basic backup and restore
- CLI interface
- Initial test suite