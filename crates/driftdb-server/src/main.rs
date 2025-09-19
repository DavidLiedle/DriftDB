//! DriftDB Server with PostgreSQL Wire Protocol
//!
//! This server allows DriftDB to be accessed using any PostgreSQL client,
//! including psql, pgAdmin, DBeaver, and all PostgreSQL drivers.

mod protocol;
mod session;
mod executor;
mod health;
mod metrics;

use std::net::{SocketAddr, IpAddr};
use std::sync::Arc;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};

use driftdb_core::{Engine, EnginePool, PoolConfig, RateLimitConfig, RateLimitManager};
use session::SessionManager;

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
    #[arg(short = 'c', long, env = "DRIFTDB_MAX_CONNECTIONS", default_value = "100")]
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
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_EXEMPT_IPS", default_value = "127.0.0.1,::1")]
    rate_limit_exempt_ips: String,

    /// Enable adaptive rate limiting based on server load
    #[arg(long, env = "DRIFTDB_RATE_LIMIT_ADAPTIVE", default_value = "true")]
    rate_limit_adaptive: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("driftdb_server=info".parse()?)
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

    let engine = Arc::new(tokio::sync::RwLock::new(engine));

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

    info!("Creating connection pool with {} max connections", args.max_connections);
    let engine_pool = EnginePool::new(engine.clone(), pool_config, pool_metrics.clone())?;

    // Parse authentication method
    let auth_method = args.auth_method.parse::<protocol::auth::AuthMethod>()
        .unwrap_or_else(|e| {
            eprintln!("Invalid authentication method '{}': {}", args.auth_method, e);
            std::process::exit(1);
        });

    // Create authentication configuration
    let auth_config = protocol::auth::AuthConfig {
        method: auth_method.clone(),
        require_auth: args.require_auth,
        max_failed_attempts: args.max_auth_attempts,
        lockout_duration_seconds: args.auth_lockout_duration,
    };

    info!("Authentication: method={}, require_auth={}, max_attempts={}",
          auth_method, args.require_auth, args.max_auth_attempts);

    // Parse exempt IP addresses for rate limiting
    let exempt_ips: Vec<IpAddr> = args.rate_limit_exempt_ips
        .split(',')
        .filter_map(|ip_str| {
            ip_str.trim().parse().ok()
        })
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

    info!("Rate limiting: connections_per_min={:?}, queries_per_sec={:?}, adaptive={}",
          rate_limit_config.connections_per_minute,
          rate_limit_config.queries_per_second,
          rate_limit_config.adaptive_limiting);

    // Create rate limit manager
    let rate_limit_manager = Arc::new(RateLimitManager::new(rate_limit_config, pool_metrics.clone()));

    // Create session manager with authentication and rate limiting
    let session_manager = Arc::new(SessionManager::new(engine_pool.clone(), auth_config, rate_limit_manager.clone()));

    // Start pool health checks, metrics updates, and rate limit cleanup
    let pool_tasks = {
        let pool_clone = engine_pool.clone();
        let rate_limit_clone = rate_limit_manager.clone();
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
                            stats.connection_stats.total_connections - stats.connection_stats.available_connections
                        );
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

            tokio::select! {
                _ = health_check_future => {},
                _ = metrics_update_future => {},
                _ = rate_limit_cleanup_future => {},
            }
        })
    };

    // Start HTTP server for health checks and metrics
    let http_server = {
        let engine_clone = engine.clone();
        let session_manager_clone = session_manager.clone();
        let pool_clone = engine_pool.clone();
        let http_addr = args.http_listen;

        tokio::spawn(async move {
            let result = start_http_server(
                http_addr,
                engine_clone,
                session_manager_clone,
                pool_clone,
                args.enable_metrics,
            ).await;

            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        })
    };

    // Start PostgreSQL protocol server
    let pg_server = {
        let session_manager_clone = session_manager.clone();
        let pg_addr = args.listen;

        tokio::spawn(async move {
            let result = start_postgres_server(pg_addr, session_manager_clone).await;

            if let Err(e) = result {
                error!("PostgreSQL server error: {}", e);
            }
        })
    };

    info!("DriftDB PostgreSQL server listening on {}", args.listen);
    info!("DriftDB HTTP server listening on {}", args.http_listen);
    info!("Connect with: psql -h {} -p {} -d driftdb",
          args.listen.ip(),
          args.listen.port());
    info!("Health check: http://{}:{}/health/live",
          args.http_listen.ip(),
          args.http_listen.port());

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
    }

    // Graceful shutdown of connection pool
    info!("Shutting down connection pool...");
    engine_pool.shutdown().await;
    info!("Connection pool shutdown complete");

    Ok(())
}

/// Start the HTTP server for health checks and metrics
async fn start_http_server(
    addr: SocketAddr,
    engine: Arc<tokio::sync::RwLock<Engine>>,
    session_manager: Arc<SessionManager>,
    engine_pool: EnginePool,
    enable_metrics: bool,
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

    // Start the server
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server bound to {}", addr);

    axum::serve(listener, app).await.map_err(|e| {
        anyhow::anyhow!("HTTP server failed: {}", e)
    })
}

/// Start the PostgreSQL protocol server
async fn start_postgres_server(
    addr: SocketAddr,
    session_manager: Arc<SessionManager>,
) -> Result<()> {
    // Bind to address
    let listener = TcpListener::bind(addr).await?;
    info!("PostgreSQL server bound to {}", addr);

    // Accept connections
    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                info!("New connection from {}", client_addr);

                if metrics::REGISTRY.gather().len() > 0 {
                    metrics::record_connection();
                }

                let session_mgr = session_manager.clone();
                tokio::spawn(async move {
                    let result = session_mgr.handle_connection(stream, client_addr).await;

                    if metrics::REGISTRY.gather().len() > 0 {
                        metrics::record_connection_closed();
                    }

                    if let Err(e) = result {
                        error!("Connection error from {}: {}", client_addr, e);
                        if metrics::REGISTRY.gather().len() > 0 {
                            metrics::record_error("connection", "handle_connection");
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
                if metrics::REGISTRY.gather().len() > 0 {
                    metrics::record_error("connection", "accept");
                }
            }
        }
    }
}