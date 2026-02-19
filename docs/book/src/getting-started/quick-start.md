# Quick Start

Get up and running with DriftDB in under 5 minutes!

## Installation

### Option 1: Using Cargo (Recommended)

```bash
cargo install driftdb
```

### Option 2: From Source

```bash
git clone https://github.com/davidliedle/DriftDB
cd DriftDB
cargo build --release
./target/release/driftdb --help
```

### Option 3: Docker

```bash
docker pull driftdb/driftdb:latest
docker run -d -p 5432:5432 -v $(pwd)/data:/data driftdb/driftdb
```

## Starting the Server

```bash
# Start with default settings
driftdb --data-dir ./mydata

# With custom port and logging
driftdb --data-dir ./mydata --port 5433 --log-level debug
```

The server will start and listen on `localhost:5432` by default.

## Your First Database

### 1. Connect to DriftDB

Using the CLI client:

```bash
# In another terminal
driftdb-cli
```

### 2. Create a Table

```sql
-- Standard SQL (recommended)
CREATE TABLE products (
    id TEXT PRIMARY KEY,
    name TEXT,
    price DECIMAL,
    created_at TIMESTAMP
);

-- Add an index after creation
CREATE INDEX ON products (name);
```

### 3. Insert Some Data

```sql
INSERT INTO products VALUES
    ('p1', 'Laptop', 999.99, '2025-10-25 10:00:00'),
    ('p2', 'Mouse', 29.99, '2025-10-25 10:05:00'),
    ('p3', 'Keyboard', 79.99, '2025-10-25 10:10:00');
```

### 4. Query Your Data

```sql
-- Simple query
SELECT * FROM products;

-- With filtering
SELECT * FROM products WHERE price < 100;

-- Aggregation
SELECT COUNT(*), AVG(price) FROM products;
```

## Time-Travel Queries 🕐

This is where DriftDB shines! Query historical data effortlessly.

### Query by Sequence Number

Every operation has a sequence number. Query at any point:

```sql
-- Get current sequence
SELECT MAX(__sequence) FROM products;

-- Query as of sequence 2 (after first 2 inserts)
SELECT * FROM products FOR SYSTEM_TIME AS OF @SEQ:2;
```

### Query by Timestamp

```sql
-- See data as it was at 10:07 (only 2 products existed)
SELECT * FROM products FOR SYSTEM_TIME AS OF '2025-10-25 10:07:00';
```

### Track Changes Over Time

```sql
-- Insert an update
UPDATE products SET price = 899.99 WHERE id = 'p1';

-- Query before the update
SELECT * FROM products FOR SYSTEM_TIME AS OF @SEQ:3;

-- Query after the update
SELECT * FROM products;
```

## Using Transactions

```sql
BEGIN TRANSACTION;

INSERT INTO products VALUES ('p4', 'Monitor', 299.99, '2025-10-25 11:00:00');
UPDATE products SET price = price * 0.9 WHERE id = 'p4'; -- 10% discount

COMMIT;
```

## Creating Indexes

Speed up your queries with indexes:

```sql
-- Create an index (with or without an explicit name)
CREATE INDEX ON products (price);
CREATE INDEX idx_price ON products (price);  -- named form

-- Queries using price will now be faster
SELECT * FROM products WHERE price > 50;
```

## Using the Rust Client

```rust
use driftdb::client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to DriftDB
    let client = Client::connect("localhost:5432").await?;

    // Execute a query
    let products = client.query("SELECT * FROM products WHERE price < ?", &[&100.0]).await?;

    for row in products {
        println!("Product: {} - ${}", row.get::<String>("name"), row.get::<f64>("price"));
    }

    Ok(())
}
```

## Common Operations

### Delete (Preserves History)

```sql
-- Delete a row (data still preserved in audit log)
DELETE FROM products WHERE id = 'p1';
```

### Create a Snapshot

Snapshots speed up time-travel queries:

```sql
-- Create a snapshot at current state (PostgreSQL convention)
CHECKPOINT TABLE products;
```

### Compact Old Data

```sql
-- Remove old event segments to reclaim space
VACUUM products;
```

## Configuration

Common configuration options:

```bash
driftdb \
  --data-dir ./data \
  --port 5432 \
  --max-connections 100 \
  --enable-metrics \
  --log-level info
```

See [Configuration](../operations/configuration.md) for all options.

## Next Steps

Now that you're up and running:

1. **Learn more SQL**: See [SQL Reference](../guide/sql-reference.md)
2. **Time-Travel Deep Dive**: [Time-Travel Queries](../guide/time-travel.md)
3. **Deploy to Production**: [Deployment Guide](../operations/deployment.md)
4. **Monitor Your Database**: [Monitoring](../operations/monitoring.md)

## Getting Help

- 📖 Read the [User Guide](../guide/sql-reference.md)
- 💬 Ask questions in [GitHub Discussions](https://github.com/davidliedle/DriftDB/discussions)
- 🐛 Report bugs on [GitHub Issues](https://github.com/davidliedle/DriftDB/issues)

---

**Having trouble?** Check out the [Troubleshooting Guide](../troubleshooting/common-issues.md)
