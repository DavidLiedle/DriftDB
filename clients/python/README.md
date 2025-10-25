# DriftDB Python Client

Official Python client for [DriftDB](https://github.com/davidliedle/DriftDB) - an append-only database with time-travel queries.

## Features

- ✨ **Async/await support** with asyncio
- 🏊 **Connection pooling** for high performance
- 🕐 **Time-travel queries** - query historical data
- 💪 **Type hints** for better IDE support
- 🔒 **Transactions** with context managers
- 🚀 **Fluent query builder** for programmatic queries
- 📦 **Pythonic API** - feels natural to Python developers

## Installation

```bash
pip install driftdb
```

## Quick Start

```python
import asyncio
from driftdb import Client

async def main():
    # Connect to DriftDB
    client = await Client.connect("localhost:5432")

    # Execute a query
    results = await client.query("SELECT * FROM users WHERE age > ?", [18])

    # Iterate over results
    for row in results:
        print(f"User: {row['name']}, Age: {row['age']}")

    # Close connection
    await client.close()

asyncio.run(main())
```

## Time-Travel Queries

Query your data as it existed at any point in time!

```python
# Query at specific sequence number
historical = await client.query_at_seq(
    "SELECT * FROM orders",
    sequence=1000
)

# Query at specific timestamp
from datetime import datetime
past_data = await client.query_at_time(
    "SELECT * FROM inventory",
    timestamp=datetime(2025, 10, 1, 12, 0, 0)
)

# Or use ISO string
past_data = await client.query_at_time(
    "SELECT * FROM inventory",
    timestamp="2025-10-01T12:00:00"
)
```

## Transactions

```python
# Using context manager (recommended)
async with client.transaction() as tx:
    await tx.execute("INSERT INTO users VALUES (?, ?)", [1, "Alice"])
    await tx.execute("UPDATE accounts SET balance = balance - 100 WHERE user_id = ?", [1])
    # Automatically commits on success, rolls back on exception

# Manual control
tx = client.transaction()
await tx.__aenter__()
try:
    await tx.execute("INSERT INTO logs VALUES (?, ?)", ["event", "data"])
    await tx.commit()
except Exception:
    await tx.rollback()
finally:
    await tx.__aexit__(None, None, None)
```

## Query Builder

Build queries programmatically with a fluent API:

```python
from driftdb import QueryBuilder

# Build a complex query
query = (QueryBuilder("products")
    .select("id", "name", "price")
    .where("category", "=", "electronics")
    .where("price", "<", 1000)
    .order_by("price", "DESC")
    .limit(10))

# Execute the query
results = await client.query(query.build(), query.params())
```

## Connection Pooling

The client automatically uses connection pooling for optimal performance:

```python
# Configure pool size
client = await Client.connect(
    "localhost:5432",
    min_connections=2,    # Minimum pool size
    max_connections=20,   # Maximum pool size
    timeout=10.0         # Connection timeout in seconds
)

# Pool is managed automatically
# Connections are reused across requests
```

## Advanced Usage

### Using as Context Manager

```python
async with await Client.connect("localhost:5432") as client:
    results = await client.query("SELECT * FROM users")
    # Client is automatically closed when exiting context
```

### Custom Connection Pool

```python
from driftdb import ConnectionPool

pool = ConnectionPool("localhost", 5432, min_size=5, max_size=50)
await pool.initialize()

# Acquire connection manually
conn = await pool.acquire()
try:
    result = await conn.execute("SELECT * FROM users")
finally:
    await pool.release(conn)
```

### Error Handling

```python
from driftdb import DriftDBError, ConnectionError, QueryError, TimeoutError

try:
    results = await client.query("INVALID SQL")
except QueryError as e:
    print(f"Query failed: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except TimeoutError as e:
    print(f"Operation timed out: {e}")
except DriftDBError as e:
    print(f"DriftDB error: {e}")
```

### Working with Results

```python
results = await client.query("SELECT * FROM users")

# Length
print(f"Got {len(results)} rows")

# Index access
first_user = results[0]

# Iteration
for row in results:
    print(row['name'])

# Get first row (or None)
first = results.first()

# Convert to list of dicts
dicts = results.to_dict_list()

# Check execution time
print(f"Query took {results.execution_time_ms}ms")
```

## API Reference

### Client

```python
# Connect to DriftDB
client = await Client.connect(
    address: str,
    min_connections: int = 2,
    max_connections: int = 10,
    timeout: float = 10.0
) -> Client

# Execute query
results = await client.query(
    sql: str,
    params: Optional[List[Any]] = None,
    timeout: Optional[float] = None
) -> QueryResult

# Time-travel queries
results = await client.query_at_seq(
    sql: str,
    sequence: int,
    params: Optional[List[Any]] = None
) -> QueryResult

results = await client.query_at_time(
    sql: str,
    timestamp: Union[str, datetime],
    params: Optional[List[Any]] = None
) -> QueryResult

# Execute non-query (INSERT, UPDATE, DELETE)
affected_rows = await client.execute(
    sql: str,
    params: Optional[List[Any]] = None
) -> int

# Start transaction
tx = client.transaction() -> Transaction

# Close all connections
await client.close()
```

### QueryResult

```python
result = await client.query("SELECT * FROM users")

len(result)              # Number of rows
result[0]                # Get row by index
result.first()           # Get first row or None
result.all()             # Get all rows as list
result.to_dict_list()    # Convert to list of dicts
result.columns           # List of column names
result.execution_time_ms # Query execution time
```

### Row

```python
row = results[0]

row['name']              # Get column value
row.get('age', 0)        # Get with default
'email' in row           # Check if column exists
row.keys()               # Get all column names
row.values()             # Get all values
row.items()              # Get (key, value) pairs
row.to_dict()            # Convert to dict
```

## Examples

### Simple CRUD Operations

```python
# Create
await client.execute(
    "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
    [1, "Alice", "alice@example.com"]
)

# Read
users = await client.query("SELECT * FROM users WHERE id = ?", [1])
user = users.first()

# Update
await client.execute(
    "UPDATE users SET email = ? WHERE id = ?",
    ["newemail@example.com", 1]
)

# Delete (soft delete preserves history)
await client.execute("SOFT DELETE FROM users WHERE id = ?", [1])

# Query including deleted records
all_users = await client.query("SELECT * FROM users INCLUDING DELETED")
```

### Audit Log / Time Travel

```python
# Insert some data
await client.execute("INSERT INTO inventory VALUES (?, ?, ?)", [1, "Widget", 100])
await client.execute("UPDATE inventory SET quantity = 90 WHERE id = ?", [1])
await client.execute("UPDATE inventory SET quantity = 80 WHERE id = ?", [1])

# Check current state
current = await client.query("SELECT * FROM inventory WHERE id = ?", [1])
print(f"Current quantity: {current[0]['quantity']}")  # 80

# Check historical state
past = await client.query_at_seq("SELECT * FROM inventory WHERE id = ?", sequence=1, params=[1])
print(f"Original quantity: {past[0]['quantity']}")  # 100
```

### Bulk Operations

```python
# Batch insert in transaction
async with client.transaction() as tx:
    for i in range(1000):
        await tx.execute(
            "INSERT INTO logs VALUES (?, ?)",
            [i, f"Event {i}"]
        )
```

## Development

### Setup

```bash
cd clients/python
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"
```

### Testing

```bash
pytest tests/
```

### Code Quality

```bash
# Format code
black driftdb/

# Type checking
mypy driftdb/

# Linting
ruff check driftdb/
```

## Requirements

- Python 3.8+
- asyncio support
- DriftDB server running

## License

MIT License - see [LICENSE](../../LICENSE) for details.

## Links

- **GitHub**: https://github.com/davidliedle/DriftDB
- **Documentation**: https://driftdb.io/docs
- **PyPI**: https://pypi.org/project/driftdb/
- **Issues**: https://github.com/davidliedle/DriftDB/issues

## Support

- 📖 Read the [documentation](https://driftdb.io/docs)
- 💬 Ask questions in [GitHub Discussions](https://github.com/davidliedle/DriftDB/discussions)
- 🐛 Report bugs on [GitHub Issues](https://github.com/davidliedle/DriftDB/issues)
