# Known Issues - DriftDB Alpha

## Critical Issues (Data Loss Risk)

### 1. WAL Durability
- **Issue**: No fsync after WAL writes - data can be lost on crash
- **Impact**: HIGH - Recent transactions may be lost
- **Workaround**: None
- **Fix Priority**: P0

### 2. Hardcoded /tmp/wal Path
- **Issue**: TransactionManager creates WAL at `/tmp/wal` (hardcoded)
- **Impact**: HIGH - Will fail in production, /tmp may be cleared
- **Workaround**: None
- **Fix Priority**: P0

### 3. Panic Points Throughout Code
- **Issue**: Multiple `.unwrap()` calls that will panic in production
- **Impact**: HIGH - Database crashes on unexpected input
- **Workaround**: None
- **Fix Priority**: P0

## Major Issues

### 4. Transaction Isolation Not Real
- **Issue**: ACID claims are not accurate, isolation levels not properly implemented
- **Impact**: HIGH - Data consistency issues under concurrent load
- **Status**: Needs complete rewrite

### 5. Replication is Experimental
- **Issue**: No split-brain prevention, no data consistency verification
- **Impact**: HIGH - Data divergence in replicated setup
- **Status**: Do not use in production

### 6. No Real Metrics Collection
- **Issue**: Metrics structs exist but data is never collected
- **Impact**: MEDIUM - Cannot monitor production systems
- **Status**: Admin tool shows fake data

## Performance Issues

### 7. Global Lock Contention
- **Issue**: All operations go through global RwLocks
- **Impact**: MEDIUM - Poor concurrent performance
- **Status**: Needs architecture change

### 8. Unbounded Memory Growth
- **Issue**: Multiple unbounded queues and caches
- **Impact**: MEDIUM - OOM under load
- **Status**: Need backpressure implementation

## Security Issues

### 9. No Authentication
- **Issue**: Admin tools have no authentication mechanism
- **Impact**: HIGH - Anyone can manage database
- **Status**: Not implemented

### 10. Weak Encryption
- **Issue**: No key rotation, no KDF, accepts any TLS cert
- **Impact**: HIGH - Security vulnerabilities
- **Status**: Needs security audit

## Operational Issues

### 11. Mock Admin Tool Data
- **Issue**: Admin tool returns hardcoded values for metrics
- **Impact**: MEDIUM - Cannot actually monitor system
- **Example**: Always shows "1,234" QPS

### 12. Backup/Restore Incomplete
- **Issue**: Incremental backups not actually implemented
- **Impact**: MEDIUM - Full backups only
- **Status**: Needs implementation

### 13. Migrations Cannot Rollback Safely
- **Issue**: Schema migrations may corrupt data on rollback
- **Impact**: MEDIUM - Stuck with bad migrations
- **Status**: Needs redesign

## Testing Gaps

### 14. Integration Tests Simplified
- **Issue**: Tests don't actually test the real APIs
- **Impact**: LOW - False confidence in code
- **Status**: Need real integration tests

### 15. No Failure Testing
- **Issue**: No chaos testing, no failure injection
- **Impact**: MEDIUM - Unknown failure modes
- **Status**: Need comprehensive test suite

## API Inconsistencies

### 16. Mixed Mutability Requirements
- **Issue**: Some methods need &mut self, others &self inconsistently
- **Impact**: LOW - Poor API ergonomics
- **Status**: Needs refactoring

### 17. Duplicate Method Names
- **Issue**: `begin` vs `simple_begin`, `commit` vs `simple_commit`
- **Impact**: LOW - Confusing API
- **Status**: Needs cleanup

## Documentation Issues

### 18. Overstated Capabilities
- **Issue**: README claims "production ready" but code is alpha
- **Impact**: HIGH - Misleading users
- **Status**: Being fixed

## How to Report New Issues

Please report issues to: https://github.com/driftdb/driftdb/issues

Include:
1. Steps to reproduce
2. Expected behavior
3. Actual behavior
4. Impact assessment
5. Suggested fix if known

## Contributing Fixes

We welcome contributions! Priority order:
1. Fix critical data loss issues (P0)
2. Add comprehensive tests
3. Fix security issues
4. Improve performance
5. Add missing features

See CONTRIBUTING.md for guidelines.