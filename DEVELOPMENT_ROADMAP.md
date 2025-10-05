# DriftDB Development Roadmap

**Last Updated**: 2025-10-04
**Current Version**: v0.7.3-alpha

This roadmap outlines the path from alpha to production-ready v1.0, organized by priority and dependencies.

---

## 🎯 Priority Matrix

### P0 - Critical for Correctness
Features that affect data integrity and correctness. Must be fixed before beta.

### P1 - Required for Production
Features needed for production deployments. Target for v1.0.

### P2 - Performance & Optimization
Features that improve performance but aren't blocking.

### P3 - Nice to Have
Advanced features for future releases.

---

## Phase 1: Correctness & Stability (v0.8.0 - Beta)
**Target**: 6-8 weeks
**Goal**: Fix all correctness issues, comprehensive testing

### P0-1: Complete ROLLBACK Implementation
**Status**: 70% complete (code added, needs testing)
**Estimated Effort**: 1-2 weeks

**Tasks**:
- [ ] Verify DELETE buffering works correctly
  - Test file created: `tests/test_rollback_fix.py`
  - Code added in `executor.rs:2115-2253`
  - Needs debugging of transaction detection
- [ ] Implement UPDATE buffering in transactions
  - Similar pattern to DELETE buffering
  - Store old data for rollback
- [ ] Add comprehensive ROLLBACK tests
  - Test all DML operations (INSERT, UPDATE, DELETE)
  - Test savepoint ROLLBACK
  - Test nested transactions
- [ ] Performance testing with transactions
  - Ensure buffering doesn't degrade performance
  - Test large transactions (1000+ operations)

**Acceptance Criteria**:
- All DML operations properly buffered
- ROLLBACK restores database to transaction start state
- Savepoints work correctly
- No data loss or corruption

---

### P0-2: MVCC Transaction Isolation
**Status**: 40% complete (framework exists)
**Estimated Effort**: 3-4 weeks

**Tasks**:
- [ ] Implement snapshot isolation properly
  - Read transactions see consistent snapshot
  - Write transactions detect conflicts
- [ ] Add read-write conflict detection
  - Track read sets and write sets
  - Implement optimistic concurrency control
- [ ] Implement all isolation levels
  - READ UNCOMMITTED (allow dirty reads)
  - READ COMMITTED (no dirty reads)
  - REPEATABLE READ (snapshot isolation)
  - SERIALIZABLE (full serializability)
- [ ] Add deadlock detection and resolution
  - Wait-for graph construction
  - Cycle detection algorithm
  - Automatic deadlock resolution (abort youngest transaction)
- [ ] Comprehensive concurrency testing
  - Test concurrent readers and writers
  - Test all isolation levels
  - Stress test with high concurrency

**Files to Modify**:
- `crates/driftdb-core/src/mvcc.rs` (expand existing implementation)
- `crates/driftdb-server/src/transaction.rs` (integrate MVCC)
- `crates/driftdb-core/src/engine.rs` (snapshot reads)

**Acceptance Criteria**:
- All isolation levels work correctly
- No phantom reads, dirty reads, or non-repeatable reads (per isolation level)
- Deadlocks detected and resolved automatically
- Performance acceptable under high concurrency

---

### P0-3: Comprehensive Testing Suite
**Status**: Basic tests exist
**Estimated Effort**: 2-3 weeks

**Tasks**:
- [ ] Expand integration test coverage
  - Currently 12 tests in `comprehensive_sql_test.py`
  - Add 50+ more tests covering edge cases
- [ ] Add concurrency tests
  - Multiple clients executing queries simultaneously
  - Test transaction conflicts
  - Test deadlock scenarios
- [ ] Add crash recovery tests
  - Kill server mid-transaction
  - Verify WAL replay works correctly
  - Test data integrity after crash
- [ ] Add performance regression tests
  - Benchmark all major operations
  - Track performance over time
  - Alert on regressions
- [ ] Add fuzzing tests
  - Random SQL generation
  - Random data generation
  - Stress test with invalid inputs

**Test Coverage Goals**:
- Unit tests: 80%+ coverage
- Integration tests: All major features
- Performance tests: All CRUD operations
- Crash/recovery tests: WAL scenarios

---

## Phase 2: Production Features (v0.9.0 - RC)
**Target**: 8-10 weeks after Phase 1
**Goal**: Add essential production features

### P1-1: Native TLS/SSL Support
**Status**: 10% complete (flags exist)
**Estimated Effort**: 2-3 weeks

**Tasks**:
- [ ] Implement TLS handshake
  - Use `tokio-rustls` (already dependency)
  - Support TLS 1.2 and 1.3
- [ ] Add certificate loading
  - Support PEM format certificates
  - Validate certificate chain
- [ ] Implement certificate validation
  - Check expiration
  - Verify hostname
  - Support self-signed certs for dev
- [ ] Add STARTTLS support
  - PostgreSQL protocol STARTTLS flow
- [ ] Update connection handling
  - Detect TLS vs plain connections
  - Support mixed-mode (optional TLS)
- [ ] Comprehensive TLS testing
  - Test with various clients
  - Test certificate validation
  - Test encrypted data transmission

**Files to Create/Modify**:
- `crates/driftdb-server/src/tls.rs` (new file)
- `crates/driftdb-server/src/session/mod.rs` (integrate TLS)
- Configuration flags already exist in `main.rs`

**Acceptance Criteria**:
- TLS 1.2+ working with psql and drivers
- Certificate validation working
- No performance degradation with TLS
- Passes security audit

---

### P1-2: Streaming Replication
**Status**: 30% complete (framework exists)
**Estimated Effort**: 4-5 weeks

**Tasks**:
- [ ] Implement WAL streaming protocol
  - Stream WAL changes to replicas
  - Support multiple replicas
  - Handle network interruptions
- [ ] Add replica management
  - Track replica lag
  - Monitor replica health
  - Automatic reconnection
- [ ] Implement synchronous and asynchronous replication
  - Synchronous: Wait for replica acknowledgment
  - Asynchronous: Fire-and-forget
- [ ] Add failover support
  - Automatic failover to replica
  - Manual failover commands
  - Prevent split-brain
- [ ] Testing
  - Test with multiple replicas
  - Test network partition scenarios
  - Test failover scenarios

**Files to Modify**:
- `crates/driftdb-core/src/replication.rs` (expand)
- `crates/driftdb-server/src/replication/` (new module)
- WAL changes in `crates/driftdb-core/src/wal.rs`

**Dependencies**: WAL improvements

**Acceptance Criteria**:
- Streaming replication working
- Automatic failover functional
- Replica lag < 100ms for async
- Zero data loss for sync replication

---

### P1-3: Production Monitoring & Alerting
**Status**: 70% complete (basic metrics exist)
**Estimated Effort**: 2 weeks

**Tasks**:
- [ ] Expand Prometheus metrics
  - Currently has basic metrics
  - Add query latency percentiles (p50, p95, p99)
  - Add connection pool stats
  - Add replication lag metrics
- [ ] Add alerting rules
  - High error rate
  - Replication lag too high
  - Connection pool exhaustion
  - Disk space low
- [ ] Add structured logging improvements
  - Add request IDs for tracing
  - Add slow query logging
  - Add detailed error logging
- [ ] Create Grafana dashboards
  - System overview dashboard
  - Query performance dashboard
  - Replication dashboard
- [ ] Add health check improvements
  - Deep health checks (test queries)
  - Dependency health checks
  - Startup/shutdown health state

**Files to Modify**:
- `crates/driftdb-server/src/metrics.rs` (expand)
- Add `grafana/` directory with dashboards
- Add `prometheus/` directory with alert rules

**Acceptance Criteria**:
- Comprehensive metrics for all operations
- Grafana dashboards working
- Alert rules tested
- Slow query log functional

---

### P1-4: Security Hardening
**Status**: 60% complete (basic security working)
**Estimated Effort**: 3 weeks

**Tasks**:
- [ ] Complete RBAC implementation
  - Role creation and management
  - Permission assignment
  - Permission checking on all operations
- [ ] Add row-level security
  - Policy definitions
  - Policy enforcement
- [ ] Implement connection encryption verification
  - Require TLS for remote connections
  - Disallow plaintext passwords over non-TLS
- [ ] Add security audit logging
  - Log all authentication events
  - Log all permission changes
  - Log all failed access attempts
- [ ] Conduct security audit
  - Penetration testing
  - Code review for security issues
  - SQL injection testing (already 7/7 pass)
- [ ] Add security documentation
  - Security best practices
  - Hardening guide
  - Incident response plan

**Files to Modify**:
- `crates/driftdb-server/src/rbac.rs` (expand)
- `crates/driftdb-server/src/security/` (expand)
- `crates/driftdb-server/src/audit.rs` (expand)

**Acceptance Criteria**:
- Full RBAC working
- Security audit passed
- All security docs complete
- No critical vulnerabilities

---

## Phase 3: Performance & Optimization (v1.0)
**Target**: 4-6 weeks after Phase 2
**Goal**: Optimize for production workloads

### P2-1: Query Optimizer Improvements
**Status**: 35% complete (basic planner works)
**Estimated Effort**: 4-5 weeks

**Tasks**:
- [ ] Implement cost-based optimization
  - Collect table statistics (row counts, cardinalities)
  - Estimate query costs
  - Choose optimal execution plan
- [ ] Add join strategy optimization
  - Nested loop join (current)
  - Hash join
  - Merge join
  - Choose based on cost
- [ ] Implement subquery optimization
  - Subquery flattening
  - Subquery decorrelation
  - Push down predicates
- [ ] Add index selection improvements
  - Multi-column index usage
  - Covering indexes
  - Index-only scans
- [ ] Implement query plan caching
  - Cache parsed and optimized plans
  - Invalidate on schema changes
- [ ] Add `EXPLAIN` and `EXPLAIN ANALYZE`
  - Show query plans
  - Show actual execution stats

**Files to Modify**:
- `crates/driftdb-core/src/cost_optimizer.rs` (expand)
- `crates/driftdb-server/src/executor.rs` (integrate optimizer)
- `crates/driftdb-core/src/query_planner.rs` (new file)

**Acceptance Criteria**:
- Queries automatically use optimal plans
- `EXPLAIN` shows detailed plans
- TPC-H benchmarks show improvement
- No query performance regressions

---

### P2-2: Parallel Query Execution
**Status**: 10% complete (design exists)
**Estimated Effort**: 5-6 weeks

**Tasks**:
- [ ] Implement parallel scan
  - Split table scans across workers
  - Merge results
- [ ] Add parallel aggregation
  - Partial aggregation per worker
  - Final aggregation
- [ ] Implement parallel join
  - Partition data across workers
  - Join partitions in parallel
- [ ] Add query coordinator
  - Schedule work across workers
  - Handle worker failures
- [ ] Testing
  - Test with varying worker counts
  - Test load balancing
  - Performance benchmarking

**Dependencies**: Query optimizer improvements

**Acceptance Criteria**:
- Parallel execution working for large queries
- Linear speedup up to 4-8 cores
- No deadlocks or race conditions

---

### P2-3: Storage Optimizations
**Status**: Storage engine functional, optimizations possible
**Estimated Effort**: 3-4 weeks

**Tasks**:
- [ ] Implement better compression
  - Currently uses MessagePack + Zstd for snapshots
  - Add column-oriented compression
  - Dictionary encoding for strings
- [ ] Add snapshot tuning
  - Configurable snapshot intervals
  - Adaptive snapshot timing based on write volume
- [ ] Implement vacuum/compaction improvements
  - Automatic compaction scheduling
  - Incremental compaction
  - Online compaction (no downtime)
- [ ] Add bloom filters
  - Skip reading segments that don't contain data
  - Faster negative lookups
- [ ] Storage engine benchmarking
  - Compare against PostgreSQL
  - Identify bottlenecks
  - Optimize hot paths

**Acceptance Criteria**:
- Storage size reduced 20-30%
- Read performance improved 10-20%
- Compaction doesn't impact query latency

---

## Phase 4: Advanced Features (v1.1+)
**Target**: Post v1.0
**Goal**: Advanced/differentiating features

### P3-1: Materialized Views
**Status**: 5% complete (design only)
**Estimated Effort**: 4-5 weeks

**Tasks**:
- [ ] CREATE MATERIALIZED VIEW
- [ ] REFRESH MATERIALIZED VIEW
- [ ] Automatic refresh on data changes
- [ ] Incremental view maintenance
- [ ] View query rewriting (use view instead of base tables)

---

### P3-2: Advanced Backup & Recovery
**Status**: 20% complete (basic design)
**Estimated Effort**: 3-4 weeks

**Tasks**:
- [ ] Fix compilation errors in backup module
- [ ] Implement incremental backups
- [ ] Point-in-time recovery (PITR)
- [ ] Backup encryption
- [ ] Backup compression
- [ ] Backup to cloud storage (S3, GCS, Azure)

---

### P3-3: Distributed Consensus (Raft)
**Status**: 25% complete (framework with bugs)
**Estimated Effort**: 6-8 weeks

**Tasks**:
- [ ] Fix leader election bugs
- [ ] Complete log replication
- [ ] Implement cluster formation
- [ ] Add member addition/removal
- [ ] Testing with Jepsen or similar

**Note**: This is complex and may be deferred to v2.0

---

### P3-4: Cloud-Native Features
**Estimated Effort**: 4-6 weeks

**Tasks**:
- [ ] Kubernetes operator
- [ ] Helm charts
- [ ] Auto-scaling based on metrics
- [ ] Multi-region support
- [ ] Service mesh integration

---

## Testing Strategy

### Continuous Testing
- Run all tests on every commit
- Performance benchmarks on release branches
- Nightly stress tests

### Pre-Release Testing
- Full regression suite
- Performance comparison against previous version
- Upgrade testing (v0.x → v0.y)
- Compatibility testing with all client drivers

### Production Readiness Criteria
- [ ] All P0 and P1 features complete
- [ ] 90%+ test coverage
- [ ] No known data corruption bugs
- [ ] Performance meets benchmarks
- [ ] Security audit passed
- [ ] Documentation complete
- [ ] Production deployments running successfully for 30+ days

---

## Version Timeline

### v0.8.0-beta (3 months)
- Complete ROLLBACK
- MVCC implementation
- Comprehensive tests
- Bug fixes

### v0.9.0-rc (5 months)
- Native TLS
- Streaming replication
- Production monitoring
- Security hardening

### v1.0.0 (7 months)
- Query optimizer
- Parallel execution
- Storage optimizations
- Production validation

### v1.1.0+ (ongoing)
- Materialized views
- Advanced backup
- Cloud features
- Distributed consensus

---

## Resource Requirements

### Engineering Team (Estimated)
- 2-3 engineers for Phase 1 (Correctness)
- 3-4 engineers for Phase 2 (Production)
- 2-3 engineers for Phase 3 (Performance)
- 1-2 engineers for Phase 4 (Advanced)

### Infrastructure
- CI/CD for automated testing
- Performance testing environment
- Staging environment for pre-release testing
- Production monitoring (for early adopters)

---

## Success Metrics

### Technical Metrics
- TPC-H benchmark performance
- Query latency (p50, p95, p99)
- Throughput (queries/sec)
- Replication lag
- MTBF (Mean Time Between Failures)

### Adoption Metrics
- GitHub stars
- Production deployments
- Community contributions
- Issue resolution time

---

*This roadmap is a living document and will be updated as priorities change and features are completed.*
