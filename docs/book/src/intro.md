# DriftDB

**An append-only database with time-travel capabilities**

DriftDB is an experimental database written in Rust that combines append-only storage with
SQL:2011 temporal queries. It is designed for workloads where full audit history matters —
audit logs, event sourcing, debugging, and analytics over historical snapshots.

> **Status: Alpha.** The core engine and SQL interface work. Several higher-level features
> are partially implemented. See [STATUS.md](../../STATUS.md) for details.

---

## Key Concepts

### Time-Travel Queries

Query your data at any past point in time using SQL:2011 temporal syntax:

```sql
-- As of a specific sequence number (reliable — sequences are immutable)
SELECT * FROM orders FOR SYSTEM_TIME AS OF @SEQ:1000;

-- As of a timestamp
SELECT * FROM users FOR SYSTEM_TIME AS OF '2025-10-24 12:00:00';

-- Full history for a row
SELECT * FROM users FOR SYSTEM_TIME ALL WHERE id = 'u1';
```

### Append-Only Architecture

Every INSERT, UPDATE, and DELETE is stored as an immutable event. Nothing is ever
overwritten. This gives you a built-in audit trail with zero extra configuration.

### PostgreSQL Wire Protocol

DriftDB speaks the PostgreSQL wire protocol on port 5433, so standard PostgreSQL
clients (`psql`, JDBC, etc.) can connect to it directly.

---

## What Actually Works

- Basic SQL: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`
- Time-travel queries by sequence number and timestamp
- Full drift history per row (`FOR SYSTEM_TIME ALL`)
- Subqueries, CTEs, JOINs, `GROUP BY`, `ORDER BY`
- `VACUUM` (compact) and `CHECKPOINT TABLE` (snapshot)
- ACID transactions with `BEGIN` / `COMMIT` / `ROLLBACK`
- B-tree secondary indexes

## Known Limitations

- Table creation uses `pk=id` syntax instead of standard `PRIMARY KEY (id)`
- No fsync after WAL writes — recent commits may be lost on crash
- Replication framework exists but has no real consensus or failover
- Most monitoring metrics are hardcoded placeholder values
- Temporal JOINs not supported

---

## Quick Start

```bash
# Build from source (requires Rust 1.70+)
cargo build --release

# Start the server
./target/release/driftdb-server --data-path ./data

# Or use the CLI directly
./target/release/driftdb init ./data
./target/release/driftdb sql --data ./data \
  -e 'CREATE TABLE users (pk=id, INDEX(name))'
```

[Continue to the Quick Start guide →](./getting-started/quick-start.md)

---

## Links

- **GitHub**: [github.com/davidliedle/DriftDB](https://github.com/davidliedle/DriftDB)
- **License**: MIT
