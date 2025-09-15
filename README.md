# DriftDB

An experimental append-only database with time-travel queries.

## Features

- **Append-only storage**: All changes are immutable drift events
- **Time travel**: Query any historical state with `AS OF` clauses
- **CRC-verified segments**: Data integrity with checksummed records
- **Secondary indexes**: Fast lookups on indexed columns
- **Snapshots & compaction**: Optimize query performance
- **Crash-safe**: Atomic writes with fsync on segment boundaries
- **Simple query language**: DriftQL for easy interaction

## Quick Start

### Build from source

If you don't have rust on your system, you can run the following commands in a docker container using the official rust image: 

```bash
docker run -it rust:1.89 /bin/bash
```

Building the app:

```bash
# Clone and build
git clone https://github.com/DavidLiedle/DriftDB.git
cd driftdb
make build
```

### 60-second demo

```bash
# Run the full demo (creates sample data and runs queries)
make demo
```

### Manual usage

```bash
# Initialize a database
driftdb init ./mydata

# Create a table
driftdb sql -d ./mydata -e 'CREATE TABLE users (pk=id, INDEX(email, status))'

# Insert data
driftdb sql -d ./mydata -e 'INSERT INTO users {"id": "user1", "email": "alice@example.com", "status": "active"}'

# Query data
driftdb select -d ./mydata --table users --where 'status="active"'

# Time travel query
driftdb select -d ./mydata --table users --as-of "@seq:100"

# Show row history
driftdb drift -d ./mydata --table users --key "user1"

# Create snapshot for faster queries
driftdb snapshot -d ./mydata --table users

# Compact storage
driftdb compact -d ./mydata --table users
```

## DriftQL Syntax

```sql
-- Create table with indexes
CREATE TABLE orders (pk=id, INDEX(status, customer_id))

-- Insert full document
INSERT INTO orders {"id": "order1", "status": "pending", "amount": 100}

-- Partial update
PATCH orders KEY "order1" SET {"status": "paid"}

-- Soft delete (data remains for audit)
SOFT DELETE FROM orders KEY "order1"

-- Query with time travel
SELECT * FROM orders WHERE status="paid" AS OF "2025-01-01T00:00:00Z"
SELECT * FROM orders WHERE customer_id="cust1" AS OF "@seq:1000"

-- Show drift history
SHOW DRIFT orders KEY "order1"

-- Maintenance
SNAPSHOT orders
COMPACT orders
```

## Architecture

### Storage Layout

```
data/
  tables/<table>/
    schema.yaml           # Table schema definition
    segments/            # Append-only event logs
      00000001.seg
      00000002.seg
    snapshots/           # Materialized states
      00000100.snap
    indexes/             # Secondary B-tree indexes
      status.idx
      customer_id.idx
    meta.json           # Table metadata
```

### Event Types

- **INSERT**: Add new row with full document
- **PATCH**: Partial update by primary key
- **SOFT_DELETE**: Mark row as deleted (audit trail preserved)

### Segment Format

```
[u32 length][u32 crc32][varint seq][u64 unix_ms][u8 event_type][msgpack payload]
```

## Safety & Reliability

- **Single writer**: Process-global lock prevents concurrent writes
- **Multi-reader**: Readers never block, use snapshots + replay
- **Crash recovery**: CRC verification + truncate corrupt tail
- **Atomic operations**: fsync on segment rotation and snapshots

## Development

```bash
# Run tests
make test

# Run benchmarks
make bench

# Format code
make fmt

# Run linter
make clippy

# Full CI checks
make ci
```

## Performance

- Snapshot every 100k events or 128MB (configurable)
- Compaction rewrites minimal segments with latest state
- Indexed columns use B-tree for O(log n) lookups
- Zstd compression for snapshots
- MessagePack for efficient event serialization

## License

MIT

## Status

Experimental MVP - suitable for development and testing, not production use.
