# DriftDB WAL Testing Guide

This directory contains comprehensive tests for DriftDB's Write-Ahead Log (WAL) crash recovery functionality.

## Test Files

### 1. `test_wal_recovery.py`
Comprehensive WAL crash recovery test that simulates various crash scenarios:

- **Basic WAL functionality and recovery**
- **Crash during INSERT operations** - Tests uncommitted vs committed transaction recovery
- **Crash during UPDATE operations** - Verifies rollback and recovery of updates
- **Crash during DELETE operations** - Ensures proper deletion recovery
- **Crash during transaction commit** - Tests atomicity during commit process
- **Multiple concurrent transactions with crash** - Mixed committed/uncommitted scenarios
- **WAL file corruption detection** - Tests CRC32 verification and corruption handling
- **WAL rotation functionality** - Tests large data handling and file rotation
- **Checkpoint and replay functionality** - Verifies WAL replay from checkpoints

### 2. `test_engine_wal_integration.py`
Tests integration between the Engine and WAL through the TransactionManager:

- **Basic Engine-WAL integration** - Verifies WAL files are created during operations
- **WAL recovery after restart** - Tests data recovery after graceful restart
- **Transaction boundaries in WAL** - Ensures proper transaction commit/rollback
- **Data consistency after crash** - Verifies data integrity after crash recovery
- **WAL integration with table operations** - Tests various SQL operations

### 3. `run_wal_tests.py`
Test runner that executes both test suites and provides a comprehensive summary.

## Prerequisites

### Dependencies
```bash
pip install psycopg2-binary
```

### Build DriftDB
```bash
cd /Users/david/vcs/git/github/davidliedle/DriftDB/driftdb
cargo build --release
```

## Running Tests

### Run All Tests
```bash
python3 run_wal_tests.py
```

### Run Specific Test Suites
```bash
# Run only crash recovery tests
python3 run_wal_tests.py --test recovery

# Run only integration tests
python3 run_wal_tests.py --test integration

# Enable verbose output
python3 run_wal_tests.py --verbose
```

### Run Individual Test Files
```bash
# Comprehensive crash recovery tests
python3 test_wal_recovery.py

# Engine-WAL integration tests
python3 test_engine_wal_integration.py
```

## Test Architecture

### Crash Simulation
The tests simulate crashes using `SIGKILL` to ensure:
- No graceful shutdown procedures
- WAL entries are not artificially flushed
- Recovery must handle incomplete transactions

### Data Verification
Tests verify:
- **Atomicity**: Transactions are either fully committed or fully rolled back
- **Consistency**: Data integrity is maintained across crashes
- **Isolation**: Concurrent transactions don't interfere with recovery
- **Durability**: Committed data survives crashes

### WAL Analysis
Tests include utilities to:
- Read and analyze WAL file structure
- Corrupt WAL files for corruption testing
- Monitor WAL file creation and rotation

## Test Scenarios Covered

### 1. Basic Recovery Scenarios
- Server restart with uncommitted transactions
- Server restart with committed transactions
- Mixed committed/uncommitted transaction recovery

### 2. Crash Timing Scenarios
- Crash during SQL INSERT execution
- Crash during SQL UPDATE execution
- Crash during SQL DELETE execution
- Crash during transaction COMMIT
- Crash during concurrent transactions

### 3. WAL File Scenarios
- WAL file corruption (middle, CRC, truncation)
- WAL file rotation and recovery
- Multiple WAL file recovery
- Checkpoint-based recovery

### 4. Data Consistency Scenarios
- Large transaction recovery
- Partial update recovery
- Foreign key constraint recovery (if implemented)
- Index consistency after recovery

## Expected Results

### Successful Test Run
- All committed transactions are recovered
- All uncommitted transactions are rolled back
- Data integrity is maintained
- WAL corruption is detected and handled gracefully
- Server can restart and function normally after any crash

### Test Output
Each test provides:
- **Pass/Fail status** for each scenario
- **Detailed logs** of operations and recovery
- **Data verification** results
- **Performance metrics** where applicable

## Implementation Notes

### WAL Integration Points
The tests verify these integration points:

1. **TransactionManager ↔ WAL**
   - `begin_transaction()` → WAL Begin entry
   - `write_event()` → WAL Write entry
   - `commit_transaction()` → WAL Commit entry
   - `rollback_transaction()` → WAL Rollback entry

2. **Engine ↔ TransactionManager**
   - `apply_event()` → Transaction context
   - Table operations → WAL logging
   - Recovery → WAL replay

3. **Server ↔ WAL System**
   - Startup → WAL recovery
   - Shutdown → WAL sync
   - Crash → WAL integrity

### Recovery Process
The tests verify this recovery process:

1. **Server Startup**
   - Detect existing WAL files
   - Find last checkpoint (if any)
   - Replay WAL entries from checkpoint

2. **Transaction Recovery**
   - Identify committed transactions
   - Roll back uncommitted transactions
   - Restore data to consistent state

3. **Corruption Handling**
   - Detect corrupted WAL entries
   - Truncate at corruption point
   - Continue with valid entries

## Troubleshooting

### Common Issues

**Server fails to start**
- Check that DriftDB builds successfully
- Verify no other instance is running on port 5433/5434
- Check file permissions in test directory

**Tests fail with connection errors**
- Ensure server startup timeout is sufficient
- Check firewall/network configuration
- Verify PostgreSQL wire protocol compatibility

**WAL files not found**
- Check that `DRIFTDB_DATA_PATH` is set correctly
- Verify WAL directory creation
- Check write permissions

**Recovery tests fail**
- Verify crash simulation works (server process killed)
- Check WAL file integrity
- Ensure sufficient disk space

### Debug Mode
Run tests with verbose logging:
```bash
python3 run_wal_tests.py --verbose
```

This provides detailed information about:
- Server startup/shutdown
- SQL operations
- WAL file operations
- Recovery process
- Data verification steps

## Contributing

When adding new WAL tests:

1. **Follow the existing pattern**
   - Setup → Test → Verify → Cleanup
   - Proper error handling and logging
   - Clear test descriptions

2. **Test realistic scenarios**
   - Real-world crash points
   - Edge cases and error conditions
   - Performance under load

3. **Verify all ACID properties**
   - Atomicity of transactions
   - Consistency of data
   - Isolation between transactions
   - Durability after crashes

4. **Update documentation**
   - Add test description to this file
   - Document any new test utilities
   - Update troubleshooting section if needed