//! DriftDB Server with PostgreSQL Wire Protocol
//!
//! This server allows DriftDB to be accessed using any PostgreSQL client,
//! including psql, pgAdmin, DBeaver, and all PostgreSQL drivers.

mod advanced_pool;
mod advanced_pool_routes;
mod alert_routes;
mod alerting;
mod errors;
mod executor;
mod health;
mod metrics;
mod monitoring;
mod optimized_executor;
mod ordered_columns;
mod performance;
mod performance_routes;
mod protocol;
mod replication;
mod security;
mod security_audit;
mod session;
mod slow_query_log;
mod tls;
mod transaction;
mod transaction_buffer;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use driftdb_core::{Engine, EnginePool, PoolConfig, RateLimitConfig, RateLimitManager};
use parking_lot::RwLock as SyncRwLock;
use performance::{PerformanceMonitor, QueryOptimizer, ConnectionPoolOptimizer};
use security_audit::{AuditConfig, SecurityAuditLogger};
use session::SessionManager;
use slow_query_log::{SlowQueryConfig, SlowQueryLogger};
use tls::{TlsConfig, TlsManager};

#[derive(Parser, Debug)]
#[command(name = "driftdb-server")]
#[command(about = "DriftDB Server with PostgreSQL wire protocol")]
struct Args {
    /// Database directory
    #[arg(short, long, env = "DRIFTDB_DATA_PATH", default_value = "./data")]
    data_path: PathBuf,

    /// Listen address for PostgreSQL wire protocol
    #[arg(short, long, env = "DRIFTDB_LISTEN", default_value = "127.0.0.1:5433")]
    listen: SocketAddr,

    /// HTTP server listen address for health checks and metrics
    #[arg(long, env = "DRIFTDB_HTTP_LISTEN", default_value = "127.0.0.1:8080")]
    http_listen: SocketAddr,

    /// Maximum connections
    #[arg(
        short = 'c',
        long,
        env = "DRIFTDB_MAX_CONNECTIONS",
        default_value = "100"
    )]
    max_connections: usize,

    /// Minimum idle connections in pool
    #[arg(long, env = "DRIFTDB_MIN_IDLE_CONNECTIONS", default_value = "10")]
    min_idle_connections: usize,

    /// Connection timeout in seconds
    #[arg(long, env = "DRIFTDB_CONNECTION_TIMEOUT", default_value = "30")]
    connection_timeout: u64,

    /// Idle timeout in seconds
    #[arg(long, env = "DRIFTDB_IDLE_TIMEOUT", default_value = "600")]
    idle_timeout: u64,

    /// Enable SQL:2011 temporal extensions
    #[arg(long, env = "DRIFTDB_TEMPORAL", default_value = "true")]
    enable_temporal: bool,

    /// Enable metrics collection
    #[arg(long, env = "DRIFTDB_METRICS", default_value = "true")]
    enable_metrics: bool,

    /// Authentication method (trust, md5, scram-sha-256)
    #[arg(long, env = "DRIFTDB_AUTH_METHOD", default_value = "md5")]
    auth_method: String,

    /// Require authentication (disable for development)
    #[arg(long, env = "DRIFTDB_REQUIRE_AUTH", default_value = "true")]
    require_auth: bool,

    /// Maximum failed authentication attempts before lockout
    #[arg(long, env = "DRIFTDB_MAX_AUTH_ATTEMPTS", default_value = "3")]
    max_auth_attempts: u32,

    /// Lockout duration in seconds after max failed attempts
    #[arg(long, env = "DRIFTDB_AUTH_LOCKOUT_DURATION", default_value = "300")]
    auth_lockout_duration: u64,

    /// Rate limit: connections per minute per client
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_CONNECTIONS", default_value = "30")]
    rate_limit_connections: Option<u32>,

    /// Rate limit: queries per second per client
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_QUERIES", default_value = "100")]
    rate_limit_queries: Option<u32>,

    /// Rate limit: token bucket burst size
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_BURST_SIZE", default_value = "1000")]
    rate_limit_burst_size: u32,

    /// Rate limit: global queries per second limit
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_GLOBAL", default_value = "10000")]
    rate_limit_global: Option<u32>,

    /// Rate limit: comma-separated list of exempt IP addresses
    #[arg(
        long,
        env = "DRIFTDB_RATE_LIMIT_EXEMPT_IPS",
        default_value = "127.0.0.1,::1"
    )]
    rate_limit_exempt_ips: String,

    /// Enable adaptive rate limiting based on server load
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_ADAPTIVE", default_value = "true")]
    rate_limit_adaptive: bool,

    /// Enable TLS/SSL support
    #[arg(long, env = "DRIFTDB_TLS_ENABLED", default_value = "false")]
    tls_enabled: bool,

    /// Path to TLS certificate file (PEM format)
    #[arg(long, env = "DRIFTDB_TLS_CERT_PATH")]
    tls_cert_path: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    #[arg(long, env = "DRIFTDB_TLS_KEY_PATH")]
    tls_key_path: Option<PathBuf>,

    /// Require TLS for all connections
    #[arg(long, env = "DRIFTDB_TLS_REQUIRED", default_value = "false")]
    tls_required: bool,

    /// Generate self-signed certificate for development/testing (if cert files don't exist)
    #[arg(long, env = "DRIFTDB_TLS_GENERATE_SELF_SIGNED", default_value = "false")]
    tls_generate_self_signed: bool,

    /// Enable performance monitoring and optimization
    #[arg(long, env = "DRIFTDB_PERFORMANCE_MONITORING", default_value = "true")]
    enable_performance_monitoring: bool,

    /// Maximum concurrent requests for performance limiting
    #[arg(long, env = "DRIFTDB_MAX_CONCURRENT_REQUESTS", default_value = "10000")]
    max_concurrent_requests: usize,

    /// Query execution plan cache size
    #[arg(long, env = "DRIFTDB_QUERY_CACHE_SIZE", default_value = "1000")]
    query_cache_size: usize,

    /// Slow query threshold in milliseconds
    #[arg(long, env = "DRIFTDB_SLOW_QUERY_THRESHOLD", default_value = "1000")]
    slow_query_threshold: u64,

    /// Maximum number of slow queries to keep in memory
    #[arg(long, env = "DRIFTDB_SLOW_QUERY_MAX_STORED", default_value = "1000")]
    slow_query_max_stored: usize,

    /// Enable slow query logging to stdout
    #[arg(long, env = "DRIFTDB_SLOW_QUERY_STDOUT", default_value = "false")]
    slow_query_log_stdout: bool,

    /// Path to slow query log file
    #[arg(long, env = "DRIFTDB_SLOW_QUERY_LOG_PATH", default_value = "./logs/slow_queries.log")]
    slow_query_log_path: String,

    /// Enable security audit logging
    #[arg(long, env = "DRIFTDB_AUDIT_ENABLED", default_value = "true")]
    audit_enabled: bool,

    /// Maximum number of audit entries to keep in memory
    #[arg(long, env = "DRIFTDB_AUDIT_MAX_ENTRIES", default_value = "10000")]
    audit_max_entries: usize,

    /// Path to security audit log file
    #[arg(long, env = "DRIFTDB_AUDIT_LOG_PATH", default_value = "./logs/security_audit.log")]
    audit_log_path: String,

    /// Enable detection of suspicious activity patterns
    #[arg(long, env = "DRIFTDB_AUDIT_SUSPICIOUS_DETECTION", default_value = "true")]
    audit_suspicious_detection: bool,

    /// Threshold for suspicious failed login attempts
    #[arg(long, env = "DRIFTDB_AUDIT_LOGIN_THRESHOLD", default_value = "5")]
    audit_login_threshold: u32,

    /// Enable alerting system
    #[arg(long, env = "DRIFTDB_ALERTING_ENABLED", default_value = "true")]
    alerting_enabled: bool,

    /// Alert evaluation interval in seconds
    #[arg(long, env = "DRIFTDB_ALERT_EVAL_INTERVAL", default_value = "30")]
    alert_eval_interval: u64,

    /// Alert resolution timeout in seconds
    #[arg(long, env = "DRIFTDB_ALERT_RESOLUTION_TIMEOUT", default_value = "300")]
    alert_resolution_timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("driftdb_server=info".parse()?),
        )
        .init();

    let args = Args::parse();

    info!(
        "Starting DriftDB Server v{} on {}",
        env!("CARGO_PKG_VERSION"),
        args.listen
    );

    // Initialize metrics if enabled
    if args.enable_metrics {
        metrics::init_metrics()?;
        info!("Metrics collection enabled");
    }

    // Initialize or open the database
    let engine = if args.data_path.exists() {
        info!("Opening existing database at {:?}", args.data_path);
        Engine::open(&args.data_path)?
    } else {
        info!("Initializing new database at {:?}", args.data_path);
        Engine::init(&args.data_path)?
    };

    let engine = Arc::new(SyncRwLock::new(engine));

    // Create metrics for the pool
    let pool_metrics = Arc::new(driftdb_core::observability::Metrics::new());

    // Configure connection pool
    let pool_config = PoolConfig {
        min_connections: args.min_idle_connections,
        max_connections: args.max_connections,
        connection_timeout: std::time::Duration::from_secs(args.connection_timeout),
        idle_timeout: std::time::Duration::from_secs(args.idle_timeout),
        ..Default::default()
    };

    info!(
        "Creating connection pool with {} max connections",
        args.max_connections
    );
    let engine_pool = EnginePool::new(engine.clone(), pool_config, pool_metrics.clone())?;

    // Parse authentication method
    let auth_method = args
        .auth_method
        .parse::<protocol::auth::AuthMethod>()
        .unwrap_or_else(|e| {
            eprintln!(
                "Invalid authentication method '{}': {}",
                args.auth_method, e
            );
            std::process::exit(1);
        });

    // Create authentication configuration
    let auth_config = protocol::auth::AuthConfig {
        method: auth_method.clone(),
        require_auth: args.require_auth,
        max_failed_attempts: args.max_auth_attempts,
        lockout_duration_seconds: args.auth_lockout_duration,
    };

    info!(
        "Authentication: method={}, require_auth={}, max_attempts={}",
        auth_method, args.require_auth, args.max_auth_attempts
    );

    // Parse exempt IP addresses for rate limiting
    let exempt_ips: Vec<IpAddr> = args
        .rate_limit_exempt_ips
        .split(',')
        .filter_map(|ip_str| ip_str.trim().parse().ok())
        .collect();

    // Create rate limiting configuration
    let rate_limit_config = RateLimitConfig {
        connections_per_minute: args.rate_limit_connections,
        queries_per_second: args.rate_limit_queries,
        burst_size: args.rate_limit_burst_size,
        global_queries_per_second: args.rate_limit_global,
        exempt_ips,
        adaptive_limiting: args.rate_limit_adaptive,
        cost_multiplier: 1.0,
        auth_multiplier: 2.0,
        superuser_multiplier: 5.0,
    };

    info!(
        "Rate limiting: connections_per_min={:?}, queries_per_sec={:?}, adaptive={}",
        rate_limit_config.connections_per_minute,
        rate_limit_config.queries_per_second,
        rate_limit_config.adaptive_limiting
    );

    // Create rate limit manager
    let rate_limit_manager = Arc::new(RateLimitManager::new(
        rate_limit_config,
        pool_metrics.clone(),
    ));

    // Initialize slow query logger
    let slow_query_config = SlowQueryConfig {
        slow_threshold_ms: args.slow_query_threshold,
        max_stored_queries: args.slow_query_max_stored,
        log_to_file: true,
        log_to_stdout: args.slow_query_log_stdout,
        log_file_path: args.slow_query_log_path.clone(),
    };
    let slow_query_logger = Arc::new(SlowQueryLogger::new(slow_query_config));
    info!(
        "Slow query logging enabled: threshold={}ms, log_path={}, stdout={}",
        args.slow_query_threshold, args.slow_query_log_path, args.slow_query_log_stdout
    );

    // Initialize security audit logger
    let audit_config = AuditConfig {
        enabled: args.audit_enabled,
        max_stored_entries: args.audit_max_entries,
        log_to_file: true,
        log_file_path: args.audit_log_path.clone(),
        log_suspicious_patterns: args.audit_suspicious_detection,
        suspicious_login_threshold: args.audit_login_threshold,
    };
    let audit_logger = Arc::new(SecurityAuditLogger::new(audit_config));
    info!(
        "Security audit logging enabled: log_path={}, suspicious_detection={}, threshold={}",
        args.audit_log_path, args.audit_suspicious_detection, args.audit_login_threshold
    );

    // Initialize alerting system
    let alert_manager = if args.alerting_enabled {
        let alert_config = alerting::AlertManagerConfig {
            enabled: true,
            evaluation_interval: std::time::Duration::from_secs(args.alert_eval_interval),
            resolution_timeout: std::time::Duration::from_secs(args.alert_resolution_timeout),
        };
        let manager = Arc::new(alerting::AlertManager::new(alert_config));
        info!(
            "Alerting system enabled: eval_interval={}s, resolution_timeout={}s",
            args.alert_eval_interval, args.alert_resolution_timeout
        );
        Some(manager)
    } else {
        info!("Alerting system disabled");
        None
    };

    // Initialize RBAC (Role-Based Access Control) manager
    let rbac_manager = Arc::new(security::RbacManager::new());
    info!("RBAC manager initialized with 4 system roles (superuser, admin, user, readonly)");

    // Grant superuser role to default 'driftdb' user
    if let Err(e) = rbac_manager.grant_role("driftdb", "superuser") {
        warn!("Failed to grant superuser role to default user: {}", e);
    }

    // Create session manager with authentication and rate limiting
    let session_manager = Arc::new(SessionManager::new(
        engine_pool.clone(),
        auth_config,
        rate_limit_manager.clone(),
        slow_query_logger.clone(),
        audit_logger.clone(),
        rbac_manager.clone(),
    ));

    // Initialize TLS if enabled
    let tls_manager = if args.tls_enabled {
        if let (Some(cert_path), Some(key_path)) = (&args.tls_cert_path, &args.tls_key_path) {
            // Generate self-signed certificate if requested and files don't exist
            if args.tls_generate_self_signed && (!cert_path.exists() || !key_path.exists()) {
                info!("Generating self-signed certificate for development/testing");
                if let Err(e) = tls::generate_self_signed_cert(cert_path, key_path) {
                    error!("Failed to generate self-signed certificate: {}", e);
                    if args.tls_required {
                        return Err(e);
                    } else {
                        warn!("Continuing without TLS support");
                        None
                    }
                } else {
                    info!("Self-signed certificate generated successfully");
                    // Continue with TLS initialization
                    let tls_config = TlsConfig::new(cert_path, key_path)
                        .require_tls(args.tls_required);

                    match TlsManager::new(tls_config).await {
                        Ok(manager) => {
                            info!(
                                "TLS initialized with generated certificate: cert={:?}, key={:?}, required={}",
                                cert_path, key_path, args.tls_required
                            );
                            Some(Arc::new(manager))
                        }
                        Err(e) => {
                            error!("Failed to initialize TLS: {}", e);
                            if args.tls_required {
                                return Err(e);
                            } else {
                                warn!("Continuing without TLS support");
                                None
                            }
                        }
                    }
                }
            } else {
                // Use existing certificates
                let tls_config = TlsConfig::new(cert_path, key_path)
                    .require_tls(args.tls_required);

                match TlsManager::new(tls_config).await {
                    Ok(manager) => {
                        info!(
                            "TLS initialized: cert={:?}, key={:?}, required={}",
                            cert_path, key_path, args.tls_required
                        );
                        Some(Arc::new(manager))
                    }
                    Err(e) => {
                        error!("Failed to initialize TLS: {}", e);
                        if args.tls_required {
                            return Err(e);
                        } else {
                            warn!("Continuing without TLS support");
                            None
                        }
                    }
                }
            }
        } else {
            error!("TLS enabled but certificate/key paths not provided");
            if args.tls_required {
                return Err(anyhow::anyhow!("TLS certificate/key paths required when TLS is enabled"));
            } else {
                warn!("Continuing without TLS support");
                None
            }
        }
    } else {
        info!("TLS disabled");
        None
    };

    // Initialize performance monitoring if enabled
    let (performance_monitor, query_optimizer, pool_optimizer) = if args.enable_performance_monitoring {
        let perf_monitor = Arc::new(PerformanceMonitor::new(args.max_concurrent_requests));
        let query_opt = Arc::new(QueryOptimizer::new());
        let pool_opt = Arc::new(ConnectionPoolOptimizer::new());

        info!(
            "Performance monitoring enabled: max_concurrent_requests={}, query_cache_size={}",
            args.max_concurrent_requests, args.query_cache_size
        );

        (Some(perf_monitor), Some(query_opt), Some(pool_opt))
    } else {
        info!("Performance monitoring disabled");
        (None, None, None)
    };

    // Start pool health checks, metrics updates, and rate limit cleanup
    let pool_tasks = {
        let pool_clone = engine_pool.clone();
        let rate_limit_clone = rate_limit_manager.clone();
        let performance_monitor_clone = performance_monitor.clone();
        let enable_metrics = args.enable_metrics;
        tokio::spawn(async move {
            let health_check_future = pool_clone.run_health_checks();

            let metrics_update_future = async {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    let stats = pool_clone.stats();
                    if enable_metrics {
                        metrics::update_pool_size(
                            stats.connection_stats.total_connections,
                            stats.connection_stats.available_connections,
                            stats.connection_stats.active_connections,
                        );
                        // Also update additional pool metrics
                        metrics::POOL_CONNECTIONS_CREATED
                            .set(stats.connection_stats.total_created as f64);
                        // Could add more metrics here for transactions and requests
                    }
                }
            };

            let rate_limit_cleanup_future = async {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 minutes
                loop {
                    interval.tick().await;
                    rate_limit_clone.cleanup_expired();
                }
            };

            let performance_update_future = async {
                if let Some(monitor) = &performance_monitor_clone {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60)); // 1 minute
                    loop {
                        interval.tick().await;
                        monitor.update_memory_stats();
                        debug!("Updated performance metrics");
                    }
                } else {
                    // If no performance monitoring, just sleep forever
                    std::future::pending::<()>().await;
                }
            };

            tokio::select! {
                _ = health_check_future => {},
                _ = metrics_update_future => {},
                _ = rate_limit_cleanup_future => {},
                _ = performance_update_future => {},
            }
        })
    };

    // Start alert evaluation task if enabled
    let alert_task = if let Some(ref manager) = alert_manager {
        let manager_clone = manager.clone();
        let eval_interval = std::time::Duration::from_secs(args.alert_eval_interval);

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(eval_interval);
            info!("Starting alert evaluation loop with interval: {:?}", eval_interval);

            loop {
                interval.tick().await;
                manager_clone.evaluate_rules();
            }
        }))
    } else {
        None
    };

    // Start HTTP server for health checks and metrics
    let http_server = {
        let engine_clone = engine.clone();
        let session_manager_clone = session_manager.clone();
        let pool_clone = engine_pool.clone();
        let http_addr = args.http_listen;

        let perf_monitor_clone = performance_monitor.clone();
        let query_opt_clone = query_optimizer.clone();
        let pool_opt_clone = pool_optimizer.clone();
        let alert_manager_clone = alert_manager.clone();

        tokio::spawn(async move {
            let result = start_http_server(
                http_addr,
                engine_clone,
                session_manager_clone,
                pool_clone,
                args.enable_metrics,
                perf_monitor_clone,
                query_opt_clone,
                pool_opt_clone,
                alert_manager_clone,
            )
            .await;

            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        })
    };

    // Start PostgreSQL protocol server
    let pg_server = {
        let session_manager_clone = session_manager.clone();
        let tls_manager_clone = tls_manager.clone();
        let pg_addr = args.listen;

        tokio::spawn(async move {
            let result = start_postgres_server(pg_addr, session_manager_clone, tls_manager_clone).await;

            if let Err(e) = result {
                error!("PostgreSQL server error: {}", e);
            }
        })
    };

    info!("DriftDB PostgreSQL server listening on {}", args.listen);
    info!("DriftDB HTTP server listening on {}", args.http_listen);
    info!(
        "Connect with: psql -h {} -p {} -d driftdb",
        args.listen.ip(),
        args.listen.port()
    );
    info!(
        "Health check: http://{}:{}/health/live",
        args.http_listen.ip(),
        args.http_listen.port()
    );

    // Set up graceful shutdown handling
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C signal");
        info!("Shutdown signal received, initiating graceful shutdown...");
    };

    // Wait for shutdown signal or server failures
    tokio::select! {
        _ = shutdown_signal => {
            info!("Shutting down servers...");
        }
        result = pg_server => {
            if let Err(e) = result {
                error!("PostgreSQL server task failed: {}", e);
            }
        }
        result = http_server => {
            if let Err(e) = result {
                error!("HTTP server task failed: {}", e);
            }
        }
        result = pool_tasks => {
            if let Err(e) = result {
                error!("Pool management task failed: {}", e);
            }
        }
        result = async {
            if let Some(task) = alert_task {
                task.await
            } else {
                std::future::pending().await
            }
        } => {
            if let Err(e) = result {
                error!("Alert evaluation task failed: {}", e);
            }
        }
    }

    // Graceful shutdown of connection pool
    info!("Shutting down connection pool...");
    engine_pool.shutdown().await;
    info!("Connection pool shutdown complete");

    Ok(())
}

/// Start the HTTP server for health checks and metrics
#[allow(clippy::too_many_arguments)]
async fn start_http_server(
    addr: SocketAddr,
    engine: Arc<SyncRwLock<Engine>>,
    session_manager: Arc<SessionManager>,
    _engine_pool: EnginePool,
    enable_metrics: bool,
    performance_monitor: Option<Arc<PerformanceMonitor>>,
    query_optimizer: Option<Arc<QueryOptimizer>>,
    pool_optimizer: Option<Arc<ConnectionPoolOptimizer>>,
    alert_manager: Option<Arc<alerting::AlertManager>>,
) -> Result<()> {
    use axum::Router;
    use tower_http::trace::TraceLayer;

    // Create health check router
    let health_state = health::HealthState::new(engine.clone(), session_manager.clone());
    let health_router = health::create_health_router(health_state);

    // Create base router
    let mut app = Router::new()
        .merge(health_router)
        .layer(TraceLayer::new_for_http());

    // Add metrics router if enabled
    if enable_metrics {
        let metrics_state = metrics::MetricsState::new(engine, session_manager);
        let metrics_router = metrics::create_metrics_router(metrics_state);
        app = app.merge(metrics_router);
    }

    // Add performance monitoring routes if enabled
    if performance_monitor.is_some() || query_optimizer.is_some() || pool_optimizer.is_some() {
        let performance_state = performance_routes::PerformanceState::new(
            performance_monitor,
            query_optimizer,
            pool_optimizer,
        );
        let performance_router = performance_routes::create_performance_routes(performance_state);
        app = app.merge(performance_router);
        info!("Performance monitoring routes enabled");
    }

    // Add alerting routes if enabled
    if let Some(ref manager) = alert_manager {
        let alert_router = alert_routes::create_router(manager.clone());
        app = app.merge(alert_router);
        info!("Alerting routes enabled");
    }

    // Start the server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server bound to {}", addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("HTTP server failed: {}", e))
}

/// Start the PostgreSQL protocol server
async fn start_postgres_server(
    addr: SocketAddr,
    session_manager: Arc<SessionManager>,
    tls_manager: Option<Arc<TlsManager>>,
) -> Result<()> {
    // Bind to address
    let listener = TcpListener::bind(addr).await?;
    info!("PostgreSQL server bound to {}", addr);

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((tcp_stream, client_addr)) => {
                info!("New connection from {}", client_addr);

                if !metrics::REGISTRY.gather().is_empty() {
                    metrics::record_connection();
                }

                let session_mgr = session_manager.clone();
                let tls_mgr = tls_manager.clone();
                tokio::spawn(async move {
                    // Handle TLS negotiation if enabled
                    let secure_stream = match &tls_mgr {
                        Some(tls) => {
                            match tls.accept_connection(tcp_stream).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    error!("TLS handshake failed for {}: {}", client_addr, e);
                                    if !metrics::REGISTRY.gather().is_empty() {
                                        metrics::record_error("tls", "handshake");
                                    }
                                    return;
                                }
                            }
                        }
                        None => {
                            // No TLS configured - handle as plain connection
                            use crate::tls::SecureStream;
                            SecureStream::Plain(tcp_stream)
                        }
                    };

                    let result = session_mgr.handle_secure_connection(secure_stream, client_addr).await;

                    if !metrics::REGISTRY.gather().is_empty() {
                        metrics::record_connection_closed();
                    }

                    if let Err(e) = result {
                        error!("Connection error from {}: {}", client_addr, e);
                        if !metrics::REGISTRY.gather().is_empty() {
                            metrics::record_error("connection", "handle_connection");
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
                if !metrics::REGISTRY.gather().is_empty() {
                    metrics::record_error("connection", "accept");
                }
            }
        }
    }
}
