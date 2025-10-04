# Client Tests

This directory contains all client-side integration and unit tests for DriftDB.

## Test Organization

### `/sql/`
SQL functionality tests including:
- Basic SQL operations (SELECT, INSERT, UPDATE, DELETE)
- Advanced SQL features (DISTINCT, COUNT, SET operations)
- WHERE clause tests
- EXPLAIN PLAN tests

### `/postgres/`
PostgreSQL wire protocol and compatibility tests:
- Connection tests
- Prepared statements
- psql client compatibility
- PostgreSQL protocol compliance

### `/engine/`
Core engine and storage tests:
- WAL (Write-Ahead Log) tests
- WAL recovery tests
- Backup and restore operations
- Engine-WAL integration

### `/integration/`
End-to-end integration tests:
- Transactions
- Time travel queries
- Temporal features
- New feature validation

### `/performance/`
Performance and stress tests (if any)

## Running Tests

To run all tests:
```bash
python -m pytest client_tests/
```

To run a specific category:
```bash
python -m pytest client_tests/sql/
```

To run a specific test file:
```bash
python -m pytest client_tests/sql/test_basic_sql.py
```

## Test Requirements

All tests require:
- Python 3.8+
- psycopg2 or psycopg3
- pytest
- A running DriftDB server instance

See requirements.txt for full dependencies.
