# DriftDB Python Test Suite

Organized test suite for DriftDB PostgreSQL wire protocol server.

## Test Categories

### 📁 `unit/`
Unit tests for specific features and components:
- `test_column_ordering.py` - Column order consistency tests
- `test_column_order_detailed.py` - Detailed column ordering validation
- `test_update_columns.py` - Column update operations
- `test_simple.py` - Basic connectivity tests
- `test_improvements.py` - Feature improvement validation

### 📁 `integration/`
Integration tests for SQL functionality:
- `test_basic_sql.py` - Basic SQL operations
- `test_full_sql.py` - Comprehensive SQL feature tests
- `test_sql.py` - SQL query execution tests
- `test_create_index.py` - Index creation and management
- `test_drop_table.py` - Table dropping operations
- `test_transactions.py` - Transaction handling (BEGIN, COMMIT)
- `test_rollback_detailed.py` - ROLLBACK functionality tests
- `test_transaction_debug.py` - Transaction debugging helpers

### 📁 `performance/`
Performance and benchmark tests:
- `test_basic_performance.py` - Basic performance benchmarks
- `test_performance.py` - Comprehensive performance testing

### 📁 `security/`
Security validation tests:
- `test_sql_injection.py` - SQL injection prevention
- `test_validator.py` - Input validation tests

## Running Tests

### Run All Tests
```bash
python tests/python/run_all_tests.py
```

### Run Specific Category
```bash
# Unit tests
python -m pytest tests/python/unit/

# Integration tests
python -m pytest tests/python/integration/

# Performance tests
python -m pytest tests/python/performance/

# Security tests
python -m pytest tests/python/security/
```

### Run Individual Test
```bash
python tests/python/integration/test_transactions.py
```

## Prerequisites

1. DriftDB server running on `localhost:5433`
2. Python 3.8+
3. Required packages:
   ```bash
   pip install psycopg2-binary pytest
   ```

## Test Configuration

Default connection settings:
- Host: `localhost`
- Port: `5433`
- Database: `driftdb`
- User: `driftdb`
- Password: `driftdb`

## Notes

- Some tests may create/drop tables and data
- Performance tests may take longer to complete
- Security tests attempt various attack vectors (safe in test environment)