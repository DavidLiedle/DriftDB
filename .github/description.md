# DriftDB

**An experimental append-only database with built-in time travel capabilities**

DriftDB is a lightweight, embeddable database that treats data as an immutable stream of events. Every change is preserved forever, enabling powerful time-travel queries to any point in history.

## 🎯 Key Features

- **🔒 Append-only architecture** - No in-place updates or deletes, all changes are immutable events
- **⏰ Time travel queries** - Query data as it existed at any timestamp or sequence number
- **✅ CRC-verified storage** - Every record is checksummed for data integrity
- **🔍 Secondary indexes** - Fast lookups on indexed columns with B-tree indexes
- **📸 Snapshots & compaction** - Periodic materialized views for query performance
- **🛡️ Crash-safe** - Automatic recovery with segment verification and repair
- **📝 SQL-like query language** - Familiar SQL syntax with custom time-travel extensions

## 🚀 Use Cases

- **Audit logging** - Complete history of all changes with who, what, when
- **Event sourcing** - Natural fit for event-driven architectures
- **Debugging** - Travel back in time to understand system state during incidents
- **Compliance** - Immutable audit trail for regulatory requirements
- **Analytics** - Analyze how data evolved over time
- **Testing** - Reproducible database states for testing

## 🏗️ Architecture Highlights

- **Single-writer, multi-reader** - Lock-free reads with consistent snapshots
- **Segmented storage** - Append-only segments with automatic rotation
- **MessagePack serialization** - Efficient, schema-flexible encoding
- **Zstd compression** - Compressed snapshots reduce storage footprint
- **Rust implementation** - Memory-safe, zero-cost abstractions, blazing fast

## 📦 Installation

```bash
# Build from source
cargo build --release

# Or download pre-built binaries from releases
```

## 🎮 Quick Demo

```bash
# Initialize and run the demo
make demo

# Or try it yourself
driftdb init ./mydb
driftdb sql -d ./mydb -e 'CREATE TABLE events (pk=id, INDEX(type, status))'
driftdb sql -d ./mydb -e 'INSERT INTO events {"id": "evt1", "type": "order", "status": "pending"}'
driftdb sql -d ./mydb -e 'SELECT * FROM events WHERE type="order" AS OF "@seq:1"'
```

## 🔬 Project Status

This is an experimental MVP demonstrating append-only database concepts. While fully functional with comprehensive tests, it's intended for learning and experimentation rather than production use.

## 📚 Learn More

- [Architecture Guide](docs/DESIGN.md)
- [SQL Reference](docs/SQL.md)
- [Storage Format](docs/STORAGE.md)

## 📄 License

MIT