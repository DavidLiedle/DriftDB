# DriftDB JavaScript/TypeScript Client

Official JavaScript/TypeScript client for [DriftDB](https://github.com/davidliedle/DriftDB) - an append-only database with time-travel queries.

## Features

- 🚀 **Modern async/await** API
- 📘 **Full TypeScript support** with type definitions
- 🏊 **Connection pooling** for high performance
- 🕐 **Time-travel queries** - query historical data
- 🔒 **Transactions** with commit/rollback
- 🌐 **Node.js and Browser** support (coming soon)
- 📦 **Zero dependencies** (uses built-in `net` module)

## Installation

```bash
npm install driftdb
# or
yarn add driftdb
# or
pnpm add driftdb
```

## Quick Start

### JavaScript

```javascript
const { Client } = require('driftdb');

async function main() {
  // Connect to DriftDB
  const client = await Client.connect('localhost:5432');

  // Execute a query
  const results = await client.query('SELECT * FROM users WHERE age > ?', [18]);

  // Iterate over results
  for (const row of results.rows) {
    console.log(`User: ${row.name}, Age: ${row.age}`);
  }

  // Close connection
  await client.close();
}

main();
```

### TypeScript

```typescript
import { Client, QueryResult } from 'driftdb';

async function main(): Promise<void> {
  const client = await Client.connect('localhost:5432');

  const results: QueryResult = await client.query(
    'SELECT * FROM users WHERE age > ?',
    [18]
  );

  for (const row of results.rows) {
    console.log(`User: ${row.name}, Age: ${row.age}`);
  }

  await client.close();
}

main();
```

## Time-Travel Queries

Query your data as it existed at any point in time!

```typescript
// Query at specific sequence number
const historical = await client.queryAtSeq(
  'SELECT * FROM orders',
  1000
);

// Query at specific timestamp
const pastData = await client.queryAtTime(
  'SELECT * FROM inventory',
  new Date('2025-10-01T12:00:00')
);

// Or use ISO string
const pastData2 = await client.queryAtTime(
  'SELECT * FROM inventory',
  '2025-10-01T12:00:00'
);
```

## Transactions

```typescript
// Start transaction
const tx = client.transaction();
await tx.begin();

try {
  await tx.execute('INSERT INTO users VALUES (?, ?)', [1, 'Alice']);
  await tx.execute('UPDATE accounts SET balance = balance - 100 WHERE user_id = ?', [1]);
  await tx.commit();
} catch (err) {
  await tx.rollback();
  throw err;
}
```

## Connection Pooling

The client automatically uses connection pooling for optimal performance:

```typescript
const client = await Client.connect('localhost:5432', {
  minConnections: 2,    // Minimum pool size
  maxConnections: 20,   // Maximum pool size
  timeout: 10000       // Connection timeout in ms
});

// Pool is managed automatically
// Connections are reused across requests
```

## API Reference

### Client

#### `Client.connect(address, config?)`

Connect to DriftDB server.

```typescript
const client = await Client.connect('localhost:5432', {
  minConnections: 2,
  maxConnections: 10,
  timeout: 10000
});
```

**Parameters:**
- `address`: Server address in format `"host:port"`
- `config`: Optional configuration object

**Returns:** Promise<Client>

---

#### `client.query(sql, params?)`

Execute a SQL query.

```typescript
const results = await client.query(
  'SELECT * FROM users WHERE age > ?',
  [18]
);
```

**Parameters:**
- `sql`: SQL query string
- `params`: Optional array of parameters

**Returns:** Promise<QueryResult>

---

#### `client.queryAtSeq(sql, sequence, params?)`

Execute a time-travel query at a specific sequence number.

```typescript
const historical = await client.queryAtSeq(
  'SELECT * FROM orders',
  1000
);
```

**Parameters:**
- `sql`: SQL query string
- `sequence`: Sequence number to query at
- `params`: Optional array of parameters

**Returns:** Promise<QueryResult>

---

#### `client.queryAtTime(sql, timestamp, params?)`

Execute a time-travel query at a specific timestamp.

```typescript
const pastData = await client.queryAtTime(
  'SELECT * FROM inventory',
  new Date('2025-10-01T12:00:00')
);
```

**Parameters:**
- `sql`: SQL query string
- `timestamp`: ISO timestamp string or Date object
- `params`: Optional array of parameters

**Returns:** Promise<QueryResult>

---

#### `client.execute(sql, params?)`

Execute a non-query SQL statement (INSERT, UPDATE, DELETE).

```typescript
const affectedRows = await client.execute(
  'INSERT INTO users VALUES (?, ?)',
  [1, 'Alice']
);
```

**Parameters:**
- `sql`: SQL statement
- `params`: Optional array of parameters

**Returns:** Promise<number> - Number of affected rows

---

#### `client.transaction()`

Start a new transaction.

```typescript
const tx = client.transaction();
```

**Returns:** Transaction

---

#### `client.close()`

Close all connections.

```typescript
await client.close();
```

**Returns:** Promise<void>

---

### QueryResult

```typescript
interface QueryResult {
  rows: Row[];
  rowCount: number;
  columns: string[];
  executionTimeMs?: number;
}
```

**Properties:**
- `rows`: Array of row objects
- `rowCount`: Number of rows returned
- `columns`: Array of column names
- `executionTimeMs`: Optional query execution time in milliseconds

---

### Row

```typescript
interface Row {
  [key: string]: any;
  get<T>(column: string): T;
  get<T>(column: string, defaultValue: T): T;
}
```

**Methods:**
- `row.get<T>(column)`: Get typed column value
- `row.get<T>(column, defaultValue)`: Get with default value
- `row[column]`: Direct property access

---

### Transaction

#### `tx.begin()`

Start the transaction.

```typescript
await tx.begin();
```

**Returns:** Promise<void>

---

#### `tx.execute(sql, params?)`

Execute a query within the transaction.

```typescript
await tx.execute('INSERT INTO users VALUES (?, ?)', [1, 'Alice']);
```

**Returns:** Promise<QueryResult>

---

#### `tx.commit()`

Commit the transaction.

```typescript
await tx.commit();
```

**Returns:** Promise<void>

---

#### `tx.rollback()`

Rollback the transaction.

```typescript
await tx.rollback();
```

**Returns:** Promise<void>

---

## Examples

### Simple CRUD Operations

```typescript
// Create
await client.execute(
  'INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
  [1, 'Alice', 'alice@example.com']
);

// Read
const users = await client.query('SELECT * FROM users WHERE id = ?', [1]);
const user = users.rows[0];

// Update
await client.execute(
  'UPDATE users SET email = ? WHERE id = ?',
  ['newemail@example.com', 1]
);

// Delete (soft delete preserves history)
await client.execute('SOFT DELETE FROM users WHERE id = ?', [1]);

// Query including deleted records
const allUsers = await client.query('SELECT * FROM users INCLUDING DELETED');
```

### Audit Log / Time Travel

```typescript
// Insert some data
await client.execute('INSERT INTO inventory VALUES (?, ?, ?)', [1, 'Widget', 100]);
await client.execute('UPDATE inventory SET quantity = 90 WHERE id = ?', [1]);
await client.execute('UPDATE inventory SET quantity = 80 WHERE id = ?', [1]);

// Check current state
const current = await client.query('SELECT * FROM inventory WHERE id = ?', [1]);
console.log(`Current quantity: ${current.rows[0].quantity}`);  // 80

// Check historical state
const past = await client.queryAtSeq('SELECT * FROM inventory WHERE id = ?', 1, [1]);
console.log(`Original quantity: ${past.rows[0].quantity}`);  // 100
```

### Bulk Operations

```typescript
// Batch insert in transaction
const tx = client.transaction();
await tx.begin();

try {
  for (let i = 0; i < 1000; i++) {
    await tx.execute('INSERT INTO logs VALUES (?, ?)', [i, `Event ${i}`]);
  }
  await tx.commit();
} catch (err) {
  await tx.rollback();
  throw err;
}
```

### Error Handling

```typescript
import {
  DriftDBError,
  ConnectionError,
  QueryError,
  TimeoutError
} from 'driftdb';

try {
  const results = await client.query('INVALID SQL');
} catch (err) {
  if (err instanceof QueryError) {
    console.error('Query failed:', err.message);
  } else if (err instanceof ConnectionError) {
    console.error('Connection failed:', err.message);
  } else if (err instanceof TimeoutError) {
    console.error('Operation timed out:', err.message);
  } else if (err instanceof DriftDBError) {
    console.error('DriftDB error:', err.message);
  }
}
```

### Working with Results

```typescript
const results = await client.query('SELECT * FROM users');

// Length
console.log(`Got ${results.rowCount} rows`);

// Index access
const firstUser = results.rows[0];

// Iteration
for (const row of results.rows) {
  console.log(row.name);
}

// Typed access
const name = results.rows[0].get<string>('name');
const age = results.rows[0].get<number>('age', 0); // with default

// Check execution time
console.log(`Query took ${results.executionTimeMs}ms`);
```

## TypeScript Support

Full TypeScript support with type definitions:

```typescript
import { Client, QueryResult, Row, DataType } from 'driftdb';

// Client is fully typed
const client: Client = await Client.connect('localhost:5432');

// Results are typed
const results: QueryResult = await client.query('SELECT * FROM users');

// Rows have type-safe getters
const row: Row = results.rows[0];
const name: string = row.get<string>('name');
const age: number = row.get<number>('age', 0);
```

## Development

### Setup

```bash
cd clients/javascript
npm install
```

### Build

```bash
npm run build
```

### Test

```bash
npm test
```

### Lint & Format

```bash
npm run lint
npm run format
```

## Requirements

- Node.js 14.0+
- DriftDB server running

## License

MIT License - see [LICENSE](../../LICENSE) for details.

## Links

- **GitHub**: https://github.com/davidliedle/DriftDB
- **Documentation**: https://driftdb.io/docs
- **npm**: https://www.npmjs.com/package/driftdb
- **Issues**: https://github.com/davidliedle/DriftDB/issues

## Support

- 📖 Read the [documentation](https://driftdb.io/docs)
- 💬 Ask questions in [GitHub Discussions](https://github.com/davidliedle/DriftDB/discussions)
- 🐛 Report bugs on [GitHub Issues](https://github.com/davidliedle/DriftDB/issues)
