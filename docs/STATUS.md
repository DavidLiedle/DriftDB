# DriftDB — Project Status

**⚠️ Alpha Software — Not Production Ready**

DriftDB is an experimental append-only database with time-travel capabilities. The core
storage engine and SQL interface work; several higher-level features are partially
implemented or aspirational.

---

## What Works ✅

### Core Engine
- Append-only storage with CRC32 verification on every frame
- Time-travel queries by sequence number: `FOR SYSTEM_TIME AS OF @SEQ:N`
- Time-travel queries by timestamp: `FOR SYSTEM_TIME AS OF 'timestamp'`
- Full drift history: `SELECT * FROM t FOR SYSTEM_TIME ALL WHERE id = 'pk'`
- Event sourcing — every INSERT/UPDATE/DELETE is stored as an immutable event
- B-tree secondary indexes
- Snapshot management with zstd compression
- Basic ACID transactions with BEGIN/COMMIT/ROLLBACK

### SQL Interface (CLI + PostgreSQL server)
- `CREATE TABLE t (pk=id, INDEX(col))` — table creation with indexes
- `INSERT INTO t {"id": ..., "col": ...}` — JSON document insert
- `SELECT` with WHERE, GROUP BY, ORDER BY, LIMIT, JOINs, subqueries, CTEs
- `UPDATE ... SET ... WHERE` — partial updates
- `DELETE FROM ... WHERE` — soft deletes (history preserved)
- `VACUUM t` — compact old event segments
- `CHECKPOINT TABLE t` — materialize a snapshot

### Recently Added
- FOREIGN KEY, CHECK, UNIQUE, NOT NULL, DEFAULT constraints
- Correlated subqueries, CASE WHEN expressions
- Row-Level Security wiring (policy enforcement hooks)
- Real disk-space health checks

---

## What Doesn't Work ❌

### SQL Compatibility
- Table creation uses `pk=id` syntax instead of standard `PRIMARY KEY (id)`
- Temporal JOINs not supported
- Views, triggers, stored procedures not implemented
- Full-text search not implemented

### PostgreSQL Server
- Partial protocol compatibility — basic psql works, many clients will hit edge cases
- Query optimizer returns placeholder plans (no real cost estimation)

### Enterprise Features
- Replication framework exists but has no consensus or split-brain prevention
- Monitoring metrics are mostly hardcoded/fake
- Incremental backups not implemented
- Encryption key rotation not implemented
- Grafana dashboards don't exist

---

## Known Issues

### Critical (Data Safety)

| # | Issue | Impact | Priority |
|---|-------|--------|----------|
| 1 | No fsync after WAL writes — recent commits may be lost on crash | HIGH | P0 |
| 2 | `TransactionManager` hardcodes WAL path to `/tmp/wal` | HIGH | P0 |
| 3 | Multiple `.unwrap()` panics on unexpected input | HIGH | P0 |

### Major

| # | Issue | Impact |
|---|-------|--------|
| 4 | Transaction isolation levels stubbed — no real serializable isolation | HIGH |
| 5 | Replication: no split-brain prevention, no consistency verification | HIGH |
| 6 | Metrics structs exist but no data is ever collected | MEDIUM |

### Performance

| # | Issue | Impact |
|---|-------|--------|
| 7 | All operations go through global RwLocks — poor concurrent write throughput | MEDIUM |
| 8 | Unbounded in-memory caches — OOM risk under sustained load | MEDIUM |

### Operational

| # | Issue | Impact |
|---|-------|--------|
| 9 | Admin tool shows hardcoded values (e.g. always "1,234 QPS") | MEDIUM |
| 10 | Schema migration rollbacks may corrupt data | MEDIUM |
| 11 | No authentication on admin endpoints | HIGH |

---

## Quick Reference — Commands That Work

```bash
# Build
cargo build --release

# Initialize a database
./target/release/driftdb init test_data

# Create a table
./target/release/driftdb sql --data test_data \
  -e 'CREATE TABLE users (pk=id, INDEX(name, email))'

# Insert a row (JSON document)
./target/release/driftdb sql --data test_data \
  -e 'INSERT INTO users {"id": "u1", "name": "Alice", "age": 30}'

# Query
./target/release/driftdb sql --data test_data -e 'SELECT * FROM users'

# Update
./target/release/driftdb sql --data test_data \
  -e "UPDATE users SET age = 31 WHERE id = 'u1'"

# Time-travel by sequence
./target/release/driftdb sql --data test_data \
  -e 'SELECT * FROM users FOR SYSTEM_TIME AS OF @SEQ:2'

# Full history for a row
./target/release/driftdb sql --data test_data \
  -e "SELECT * FROM users FOR SYSTEM_TIME ALL WHERE id = 'u1'"

# Snapshot / compact
./target/release/driftdb sql --data test_data -e 'CHECKPOINT TABLE users'
./target/release/driftdb sql --data test_data -e 'VACUUM users'

# Start PostgreSQL-protocol server (port 5433)
./target/release/driftdb-server --data-path test_data
```

---

## Reporting Issues

Open an issue at <https://github.com/davidliedle/DriftDB/issues> and include:
1. Steps to reproduce
2. Expected vs. actual behaviour
3. Impact assessment
