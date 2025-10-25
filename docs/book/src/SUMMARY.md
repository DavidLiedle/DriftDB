# Summary

[Introduction](./intro.md)

---

# Getting Started

- [Quick Start](./getting-started/quick-start.md)
- [Installation](./getting-started/installation.md)
- [First Steps](./getting-started/first-steps.md)
- [Basic Concepts](./getting-started/concepts.md)

---

# User Guide

- [SQL Reference](./guide/sql-reference.md)
  - [Data Types](./guide/data-types.md)
  - [Queries](./guide/queries.md)
  - [Time-Travel Queries](./guide/time-travel.md)
  - [Transactions](./guide/transactions.md)
  - [Indexes](./guide/indexes.md)

- [Client Libraries](./guide/clients.md)
  - [Rust Client](./guide/clients/rust.md)
  - [Python Client](./guide/clients/python.md)
  - [JavaScript/TypeScript](./guide/clients/javascript.md)

- [Performance Tuning](./guide/performance.md)
  - [Query Optimization](./guide/performance/query-optimization.md)
  - [Indexing Strategies](./guide/performance/indexing.md)
  - [Snapshots & Compaction](./guide/performance/snapshots.md)
  - [Benchmarks](./guide/performance/benchmarks.md)

---

# Operations

- [Deployment](./operations/deployment.md)
  - [Docker](./operations/deployment/docker.md)
  - [Kubernetes](./operations/deployment/kubernetes.md)
  - [Bare Metal / systemd](./operations/deployment/systemd.md)
  - [Cloud Providers](./operations/deployment/cloud.md)

- [Configuration](./operations/configuration.md)
  - [Server Options](./operations/config/server.md)
  - [Storage Settings](./operations/config/storage.md)
  - [Security Settings](./operations/config/security.md)
  - [Performance Tuning](./operations/config/performance.md)

- [Monitoring & Observability](./operations/monitoring.md)
  - [Metrics (Prometheus)](./operations/monitoring/prometheus.md)
  - [Logging](./operations/monitoring/logging.md)
  - [Health Checks](./operations/monitoring/health.md)
  - [Grafana Dashboards](./operations/monitoring/grafana.md)

- [Backup & Recovery](./operations/backup.md)
  - [Backup Strategies](./operations/backup/strategies.md)
  - [Point-in-Time Recovery](./operations/backup/pitr.md)
  - [Disaster Recovery](./operations/backup/disaster-recovery.md)

- [Replication](./operations/replication.md)
  - [Setup & Configuration](./operations/replication/setup.md)
  - [Failover](./operations/replication/failover.md)
  - [Conflict Resolution](./operations/replication/conflicts.md)

---

# Security

- [Authentication](./security/authentication.md)
  - [MD5 Authentication](./security/auth/md5.md)
  - [SCRAM-SHA-256](./security/auth/scram.md)
  - [TLS/SSL](./security/auth/tls.md)

- [Authorization & RBAC](./security/authorization.md)
  - [Role-Based Access Control](./security/rbac.md)
  - [Row-Level Security](./security/rls.md)

- [Encryption](./security/encryption.md)
  - [Encryption at Rest](./security/encryption/at-rest.md)
  - [Key Management](./security/encryption/keys.md)
  - [TLS in Transit](./security/encryption/in-transit.md)

- [Security Best Practices](./security/best-practices.md)
- [Security Audit Report](./security/audit.md)

---

# Architecture

- [Overview](./architecture/overview.md)
- [Storage Engine](./architecture/storage.md)
  - [Append-Only Log](./architecture/storage/append-only.md)
  - [Segments & Frames](./architecture/storage/segments.md)
  - [Snapshots](./architecture/storage/snapshots.md)
  - [Compaction](./architecture/storage/compaction.md)
  - [Columnar Storage](./architecture/storage/columnar.md)

- [Query Engine](./architecture/query-engine.md)
  - [Parser](./architecture/query/parser.md)
  - [Planner](./architecture/query/planner.md)
  - [Executor](./architecture/query/executor.md)
  - [Time-Travel](./architecture/query/time-travel.md)

- [Transaction System](./architecture/transactions.md)
  - [MVCC](./architecture/transactions/mvcc.md)
  - [Isolation Levels](./architecture/transactions/isolation.md)
  - [WAL (Write-Ahead Log)](./architecture/transactions/wal.md)

- [Indexing](./architecture/indexing.md)
  - [B-Tree Indexes](./architecture/indexes/btree.md)
  - [Bloom Filters](./architecture/indexes/bloom.md)

- [Replication](./architecture/replication.md)
- [Connection Pooling](./architecture/connection-pool.md)
- [Rate Limiting](./architecture/rate-limiting.md)

---

# API Reference

- [Rust API](./api/rust.md)
  - [Engine](./api/rust/engine.md)
  - [Query Types](./api/rust/query.md)
  - [Transactions](./api/rust/transactions.md)
  - [Client](./api/rust/client.md)

- [Python API](./api/python.md)
- [JavaScript API](./api/javascript.md)

---

# Tutorials

- [Building a Blog with DriftDB](./tutorials/blog.md)
- [Time-Travel Analytics](./tutorials/analytics.md)
- [Audit Log System](./tutorials/audit-log.md)
- [Real-Time Dashboard](./tutorials/dashboard.md)

---

# Troubleshooting

- [Common Issues](./troubleshooting/common-issues.md)
- [Performance Problems](./troubleshooting/performance.md)
- [Error Messages](./troubleshooting/errors.md)
- [Debugging Guide](./troubleshooting/debugging.md)

---

# Contributing

- [Development Setup](./contributing/setup.md)
- [Code Style](./contributing/style.md)
- [Testing](./contributing/testing.md)
- [Pull Request Process](./contributing/pr-process.md)

---

# Appendix

- [Glossary](./appendix/glossary.md)
- [Comparison with Other Databases](./appendix/comparison.md)
- [Roadmap](./appendix/roadmap.md)
- [FAQ](./appendix/faq.md)
- [License](./appendix/license.md)
