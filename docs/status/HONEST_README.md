# DriftDB - Honest Status Report

## What DriftDB Actually Is

DriftDB is an **experimental** append-only database with time-travel capabilities, written in Rust. It's a learning project that explores temporal database concepts and provides a foundation for understanding event-sourced architectures.

## Current Status: ~70% Complete

**⚠️ NOT PRODUCTION READY** - This is experimental software with significant limitations.

## What Actually Works ✅

### Core Database Features
- **Append-only storage** with CRC32 verification
- **Basic time-travel queries** using sequence numbers (`AS OF @seq:N`)
- **Event sourcing** with INSERT, PATCH, SOFT_DELETE
- **B-tree indexes** for secondary columns
- **Snapshot management** with zstd compression
- **Basic SQL support** for simple queries
- **ACID transactions** with BEGIN/COMMIT/ROLLBACK
- **PostgreSQL wire protocol** (partial compatibility)

### Recently Implemented
- **SQL Constraints**: FOREIGN KEY, CHECK, UNIQUE, NOT NULL, DEFAULT values
- **Sequences**: AUTO_INCREMENT and SEQUENCE support
- **Transaction isolation levels**: READ COMMITTED, SERIALIZABLE, etc.
- **Savepoints**: Nested transaction support
- **Real health checks**: Actual disk space monitoring (not fake data)

## What Doesn't Work ❌

### Critical Limitations
1. **SQL Compatibility**: Not fully PostgreSQL compatible despite claims
2. **Temporal Queries**: Timestamp-based queries (`AS OF '2024-01-01'`) fall back to latest data
3. **Replication**: Framework exists but no real consensus or failover
4. **Query Optimizer**: Returns placeholder plans, not real cost-based optimization
5. **Monitoring**: Most metrics show hardcoded values
6. **Backup**: Incremental backups not implemented, Gzip compression missing

### Missing SQL Features
- Complex JOINs (temporal JOINs explicitly unsupported)
- Views, Triggers, Stored Procedures
- Window functions
- Common Table Expressions (CTEs)
- Full text search
- JSON operators

### Enterprise Features (Not Ready)
- High availability (replication is non-functional)
- Real-time monitoring (shows fake metrics)
- Production-grade backup/restore
- User authentication and RBAC
- Encryption key rotation
- Audit logging

## Honest Performance Expectations

- **Single-threaded** query execution
- **No query result caching**
- **Limited connection pooling** (not wired to query engine)
- **No parallel query execution**
- Suitable for **small to medium datasets** (< 1GB)
- Best for **write-once, read-many** workloads

## Getting Started (What Actually Works)

### Installation
```bash
# Clone THIS repository (not the non-existent github.com/driftdb/driftdb)
git clone [actual-repo-url]
cd driftdb

# Build (requires Rust 1.70+)
cargo build --release

# Run server
./target/release/driftdb-server --data-path ./data
```

### Basic Usage
```sql
-- These commands actually work
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT);
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
SELECT * FROM users;
SELECT * FROM users AS OF @seq:1;  -- Time travel by sequence

-- These are implemented but may have issues
BEGIN;
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
COMMIT;

-- These won't work as expected
SELECT * FROM users AS OF '2024-01-01';  -- Falls back to latest
SELECT * FROM users JOIN orders ON ...;   -- Complex joins fail
```

## Architecture (Reality Check)

```
┌─────────────────┐
│  PostgreSQL     │ ← Partial compatibility only
│  Wire Protocol  │
└────────┬────────┘
         │
┌────────▼────────┐
│  SQL Executor   │ ← Handles basic SQL, not all features
├─────────────────┤
│  Query Engine   │ ← Simple execution, no real optimization
├─────────────────┤
│  Storage Layer  │ ← This actually works well!
│  - Event Log    │
│  - Snapshots    │
│  - Indexes      │
└─────────────────┘
```

## Known Issues

### Data Safety
- Transaction durability depends on fsync (may lose recent commits on crash)
- No point-in-time recovery beyond snapshots
- Replication provides no real redundancy

### Compatibility
- Many PostgreSQL clients will encounter issues
- SQL:2011 temporal syntax not supported despite documentation
- DriftQL vs SQL confusion in various components

### Performance
- No actual query optimization
- Connection pooling not integrated
- Metrics system returns fake data

## Contributing

This is a learning project. Contributions are welcome, but please understand:
- Many "features" are partially implemented
- The codebase mixes production-style code with experiments
- Documentation may not match implementation
- Tests may not cover all edge cases

## Roadmap to Production

To make DriftDB production-ready would require:

1. **Fix critical issues** (3-6 months)
   - Complete PostgreSQL compatibility
   - Implement real replication with consensus
   - Fix temporal queries with timestamps
   - Wire up all monitoring and metrics

2. **Add missing features** (6-9 months)
   - Full SQL support (views, CTEs, etc.)
   - Authentication and authorization
   - Production backup/restore
   - Query optimization

3. **Production hardening** (3+ months)
   - Comprehensive testing
   - Performance optimization
   - Security audit
   - Documentation update

**Total estimate**: 12-18 months for true production readiness

## License

MIT - Use at your own risk. This is experimental software.

## Acknowledgments

DriftDB is a learning project inspired by:
- Event sourcing patterns
- Temporal database concepts
- PostgreSQL's architecture
- Rust's safety guarantees

It should not be used for production workloads in its current state.