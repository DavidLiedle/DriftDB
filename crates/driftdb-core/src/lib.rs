pub mod audit;
pub mod auth;
pub mod backup;
pub mod backup_enhanced;
pub mod bloom_filter;
pub mod cache;
pub mod connection;
pub mod consensus;
pub mod constraints;
pub mod distributed_coordinator;
pub mod encryption;
pub mod engine;
pub mod error_recovery;
pub mod errors;
pub mod events;
pub mod explain;
pub mod failover;
pub mod fk;
pub mod fulltext;
pub mod index;
pub mod index_strategies;
pub mod migration;
pub mod monitoring;
pub mod mvcc;
pub mod observability;
pub mod optimizer;
pub mod parallel;
pub mod procedures;
pub mod query;
pub mod query_cancellation;
pub mod query_performance;
pub mod raft;
pub mod rate_limit;
pub mod replication;
pub mod row_level_security;
pub mod schema;
pub mod security_monitor;
pub mod sequences;
pub mod snapshot;
pub mod sql;
pub mod sql_bridge;
pub mod sql_views;
pub mod stats;
pub mod storage;
pub mod transaction;
pub mod transaction_coordinator;
pub mod triggers;
pub mod views;
pub mod wal;
pub mod window;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod storage_test;

pub use audit::{AuditAction, AuditConfig, AuditEvent, AuditEventType, AuditSystem};
pub use auth::{AuthConfig, AuthContext, AuthManager, Permission, Role, Session, User};
pub use bloom_filter::{BloomConfig, BloomFilter, BloomStatistics, ScalableBloomFilter};
pub use connection::{EngineGuard, EnginePool, EnginePoolStats, PoolConfig, PoolStats};
pub use engine::Engine;
pub use errors::{DriftError, Result};
pub use events::{Event, EventType};
pub use explain::{ExplainExecutor, ExplainFormat, ExplainOptions, ExplainPlan};
pub use failover::{
    FailoverConfig, FailoverEvent, FailoverManager, FencingToken, HealthStatus, NodeHealth,
    NodeRole,
};
pub use query::{Query, QueryResult};
pub use query_performance::{OptimizationConfig, OptimizationStats, QueryPerformanceOptimizer};
pub use rate_limit::{QueryCost, RateLimitConfig, RateLimitManager, RateLimitStats};
pub use row_level_security::{
    Policy, PolicyAction, PolicyCheck, PolicyResult, RlsManager, RlsStatistics, SecurityContext,
};
pub use schema::Schema;
pub use security_monitor::{
    AlertType, SecurityConfig, SecurityMonitor, SecurityStats, ThreatEvent, ThreatType,
};
pub use snapshot::{
    AdaptiveSnapshotManager, Snapshot, SnapshotManager, SnapshotPolicy, SnapshotStatistics,
};
