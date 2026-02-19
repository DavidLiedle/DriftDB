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
- fsync on segment boundaries — data durability on crash
- WAL path is configurable (defaults to `<data-dir>/wal.log`)

### SQL Interface (CLI + PostgreSQL server)
- Standard `CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR)` syntax
- `CREATE INDEX ON users (name)` — post-creation index building
- `INSERT INTO t {"id": ..., "col": ...}` — JSON document insert
- `SELECT` with WHERE, GROUP BY, ORDER BY, LIMIT, JOINs, subqueries, CTEs
- `UPDATE ... SET ... WHERE` — partial updates
- `DELETE FROM ... WHERE` — soft deletes (history preserved)
- `VACUUM t` — compact old event segments
- `CHECKPOINT TABLE t` — materialize a snapshot

### Security
- `--admin-token` / `DRIFTDB_ADMIN_TOKEN` — Bearer token auth on metrics, alerts, and performance HTTP endpoints
- Health endpoints (`/health/live`, `/health/ready`) remain public

### Recently Fixed
- FOREIGN KEY, CHECK, UNIQUE, NOT NULL, DEFAULT constraints
- Correlated subqueries, CASE WHEN expressions
- Row-Level Security wiring (policy enforcement hooks)
- Real disk-space health checks
- Multiple `.unwrap()` panics on unexpected input — all 9 hot-path panics resolved

---

## What Doesn't Work ❌

### SQL Compatibility
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
| 8 | Table/index/snapshot maps bounded at 1,000 tables — OOM risk above that limit | LOW |

### Operational

| # | Issue | Impact |
|---|-------|--------|
| 9 | Admin tool shows hardcoded values (e.g. always "1,234 QPS") | MEDIUM |
| 10 | Schema migration rollbacks may corrupt data | MEDIUM |

---

## Quick Reference — Commands That Work

```bash
# Build
cargo build --release

# Initialize a database
./target/release/driftdb init test_data

# Create a table (standard SQL)
./target/release/driftdb sql --data test_data \
  -e 'CREATE TABLE users (id VARCHAR PRIMARY KEY, name VARCHAR, email VARCHAR)'

# Create an index
./target/release/driftdb sql --data test_data \
  -e 'CREATE INDEX ON users (name)'

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

# Start with admin endpoint authentication
./target/release/driftdb-server --data-path test_data --admin-token mysecrettoken
# or: DRIFTDB_ADMIN_TOKEN=mysecrettoken ./target/release/driftdb-server ...

# Health check (always public)
curl http://127.0.0.1:8080/health/live

# Metrics (requires token if --admin-token set)
curl -H "Authorization: Bearer mysecrettoken" http://127.0.0.1:8080/metrics
```

---

## Reporting Issues

Open an issue at <https://github.com/davidliedle/DriftDB/issues> and include:
1. Steps to reproduce
2. Expected vs. actual behaviour
3. Impact assessment
