# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Building
```bash
# Build the project (release mode)
cargo build --release

# Build for development
cargo build
```

### Testing
```bash
# Run all tests
cargo test --all

# Run tests for a specific crate
cargo test -p driftdb-core
cargo test -p driftdb-cli

# Run a single test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

### Development Workflow
```bash
# Format code
cargo fmt --all

# Run linter (must pass with no warnings)
cargo clippy --all -- -D warnings

# Full CI check (format, clippy, tests)
make ci

# Run benchmarks
cargo bench --all

# Run the demo scenario
make demo
```

## Architecture

DriftDB is an append-only database with time-travel capabilities, structured as a Rust workspace with two main crates:

### Crate Structure
- `crates/driftdb-core/`: Core database engine implementation
  - Storage layer with CRC-verified segments
  - Event-sourcing with INSERT, PATCH, SOFT_DELETE events
  - B-tree secondary indexes
  - Snapshot management with zstd compression
  - Query engine with time-travel support
- `crates/driftdb-cli/`: Command-line interface
  - Binary name: `driftdb`
  - DriftQL parser and executor
  - Table management commands

### Core Components

**Storage Architecture:**
- `TableStorage`: Manages table-level storage with schema and segments
- `Segment`: Append-only event log with CRC32 verification per record
- `Frame`: Individual event records with format: `[length][crc32][seq][timestamp][event_type][msgpack_payload]`
- `SnapshotManager`: Creates compressed materialized states for faster queries
- `IndexManager`: Manages B-tree indexes for secondary columns

**Engine (`src/engine.rs`):**
- Central coordinator managing tables, indexes, and snapshots
- Thread-safe with process-global write lock (single writer)
- Multi-reader support via snapshots + event replay

**Event System:**
- All changes are immutable drift events with sequence numbers
- Events carry full MessagePack-encoded payloads
- Soft deletes preserve data for audit trails

### Time Travel Implementation
Queries can specify `AS OF` clauses to query historical states:
- `@seq:N`: Query as of sequence number N
- ISO timestamp: Query as of specific time
- Implemented by loading nearest snapshot + replaying events

### Safety Features
- CRC32 verification on every frame
- Atomic writes with fsync on segment boundaries
- Crash recovery via tail truncation of corrupt segments
- Process-level file locking prevents concurrent writes

## DriftQL Language

The CLI implements a SQL-like query language for interacting with DriftDB. The parser is in `crates/driftdb-cli/src/main.rs` and uses pattern matching to execute queries against the core engine.

Supported operations:
- `CREATE TABLE` with primary key and indexes
- `INSERT INTO` with JSON documents
- `PATCH` for partial updates
- `SOFT DELETE` for audit-preserving deletes
- `SELECT` with WHERE conditions and time travel
- `SNAPSHOT` and `COMPACT` for maintenance

## Testing Approach

Tests are co-located with source files using `#[cfg(test)]` modules. The project uses:
- Unit tests for individual components
- Integration tests with temporary databases
- Property-based testing with `proptest` for invariant validation
- Benchmarks using `criterion` for performance tracking

## Key Dependencies

- `rmp-serde`: MessagePack serialization for events
- `zstd`: Compression for snapshots
- `crc32fast`: Data integrity checking
- `parking_lot`: Faster synchronization primitives
- `fs2`: Cross-platform file locking
- `nom`: Parser combinators (used for DriftQL parsing)