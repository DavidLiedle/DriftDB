//! PostgreSQL Session Management

#![allow(dead_code)]

mod prepared;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use self::prepared::PreparedStatementManager;
use crate::executor::QueryExecutor;
use crate::protocol::{self, Message, TransactionStatus};
use crate::security::{SqlValidator, RbacManager};
use crate::security_audit::SecurityAuditLogger;
use crate::slow_query_log::SlowQueryLogger;
use crate::tls::SecureStream;
use crate::transaction::TransactionManager;
use driftdb_core::{EngineGuard, EnginePool, RateLimitManager};

pub struct SessionManager {
    engine_pool: EnginePool,
    next_process_id: AtomicU32,
    auth_db: Arc<protocol::auth::UserDb>,
    rate_limit_manager: Arc<RateLimitManager>,
    slow_query_logger: Arc<SlowQueryLogger>,
    audit_logger: Arc<SecurityAuditLogger>,
    rbac_manager: Arc<RbacManager>,
}

impl SessionManager {
    pub fn new(
        engine_pool: EnginePool,
        auth_config: protocol::auth::AuthConfig,
        rate_limit_manager: Arc<RateLimitManager>,
        slow_query_logger: Arc<SlowQueryLogger>,
        audit_logger: Arc<SecurityAuditLogger>,
        rbac_manager: Arc<RbacManager>,
    ) -> Self {
        Self {
            engine_pool,
            next_process_id: AtomicU32::new(1000),
            auth_db: Arc::new(protocol::auth::UserDb::new(auth_config)),
            rate_limit_manager,
            slow_query_logger,
            audit_logger,
            rbac_manager,
        }
    }

    pub fn rbac_manager(&self) -> &Arc<RbacManager> {
        &self.rbac_manager
    }

    #[allow(dead_code)]
    pub fn auth_db(&self) -> &Arc<protocol::auth::UserDb> {
        &self.auth_db
    }

    pub fn rate_limit_manager(&self) -> &Arc<RateLimitManager> {
        &self.rate_limit_manager
    }

    pub async fn handle_secure_connection(
        self: Arc<Self>,
        stream: SecureStream,
        addr: SocketAddr,
    ) -> Result<()> {
        // Get peer address
        let peer_addr = stream.peer_addr().unwrap_or(addr);

        // Check if this is a TLS connection and log/audit accordingly
        let is_encrypted = stream.is_tls();

        // Record connection encryption status in metrics
        crate::metrics::record_connection_encryption(is_encrypted);

        if is_encrypted {
            info!("Secure TLS connection established with {}", peer_addr);

            // Log successful TLS connection to audit log
            use crate::security_audit::{AuditEventType, AuditSeverity, AuditOutcome};
            self.audit_logger.log_event(
                AuditEventType::LoginSuccess,  // Reusing LoginSuccess for connection events
                None,
                peer_addr,
                AuditSeverity::Info,
                "Encrypted TLS connection established".to_string(),
                serde_json::json!({
                    "connection_type": "TLS",
                    "encrypted": true
                }),
                AuditOutcome::Success,
                None,
            );
        } else {
            warn!("Unencrypted connection from {} (TLS not requested by client)", peer_addr);

            // Log unencrypted connection to audit log as warning
            use crate::security_audit::{AuditEventType, AuditSeverity, AuditOutcome};
            self.audit_logger.log_event(
                AuditEventType::SuspiciousActivity,
                None,
                peer_addr,
                AuditSeverity::Warning,
                "Unencrypted connection established (TLS available but not used)".to_string(),
                serde_json::json!({
                    "connection_type": "Plain",
                    "encrypted": false,
                    "tls_available": true
                }),
                AuditOutcome::Success,
                None,
            );
        }

        // Handle the same way as regular connections but with SecureStream
        self.handle_connection_internal(stream, peer_addr, is_encrypted).await
    }

    pub async fn handle_connection(
        self: Arc<Self>,
        stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        // Wrap TcpStream in SecureStream::Plain for unified handling
        let secure_stream = SecureStream::Plain(stream);
        self.handle_connection_internal(secure_stream, addr, false).await
    }

    async fn handle_connection_internal(
        self: Arc<Self>,
        mut stream: SecureStream,
        addr: SocketAddr,
        is_encrypted: bool,
    ) -> Result<()> {
        // Check rate limiting first
        if !self.rate_limit_manager.allow_connection(addr) {
            warn!(
                "Connection rate limit exceeded for {}, dropping connection",
                addr
            );
            // Don't send any response - just drop the connection
            return Ok(());
        }

        // Try to acquire a connection from the pool
        let engine_guard = match self.engine_pool.acquire(addr).await {
            Ok(guard) => guard,
            Err(e) => {
                warn!(
                    "Connection limit reached or pool error, rejecting {}: {}",
                    addr, e
                );
                // Release the rate limit connection since we're not using it
                self.rate_limit_manager.release_connection(addr);
                // Send an error response to the client before closing
                let _ = stream.write_all(b"N").await; // SSL not supported response
                return Ok(());
            }
        };

        // Create session
        let process_id = self.next_process_id.fetch_add(1, Ordering::SeqCst) as i32;
        let secret_key = rand::random::<i32>();

        // Create a shared transaction manager for this session
        let engine_for_txn = engine_guard.get_engine_ref();
        let transaction_manager = Arc::new(TransactionManager::new(engine_for_txn));

        let session = Session {
            process_id,
            secret_key,
            addr,
            username: None,
            database: "driftdb".to_string(),
            transaction_status: TransactionStatus::Idle,
            engine_guard,
            auth_db: self.auth_db.clone(),
            rate_limit_manager: self.rate_limit_manager.clone(),
            authenticated: false,
            auth_challenge: None,
            prepared_statements: PreparedStatementManager::new(),
            sql_validator: SqlValidator::new(),
            transaction_manager,
            slow_query_logger: self.slow_query_logger.clone(),
            audit_logger: self.audit_logger.clone(),
            is_encrypted,
        };

        // Handle session
        let result = session.run(&mut stream).await;

        // Clean up rate limiting state
        self.rate_limit_manager.release_connection(addr);

        if let Err(e) = result {
            error!("Session error from {}: {}", addr, e);
        } else {
            info!("Session closed for {}", addr);
        }

        Ok(())
    }
}

struct Session {
    process_id: i32,
    secret_key: i32,
    addr: SocketAddr,
    username: Option<String>,
    database: String,
    transaction_status: TransactionStatus,
    engine_guard: EngineGuard,
    auth_db: Arc<protocol::auth::UserDb>,
    rate_limit_manager: Arc<RateLimitManager>,
    authenticated: bool,
    auth_challenge: Option<Vec<u8>>,
    prepared_statements: PreparedStatementManager,
    sql_validator: SqlValidator,
    transaction_manager: Arc<TransactionManager>,
    slow_query_logger: Arc<SlowQueryLogger>,
    audit_logger: Arc<SecurityAuditLogger>,
    is_encrypted: bool,
}

impl Session {
    async fn run(mut self, stream: &mut SecureStream) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(8192);
        let mut startup_done = false;

        loop {
            // Read from stream
            info!(
                "Waiting for data from {}, startup_done={}",
                self.addr, startup_done
            );
            let n = stream.read_buf(&mut buffer).await?;
            if n == 0 {
                debug!("Connection closed by client {}", self.addr);
                break;
            }
            info!("Read {} bytes from {}", n, self.addr);

            // Decode messages
            while let Some(msg) = protocol::codec::decode_message(&mut buffer, startup_done)? {
                info!("Received message from {}: {:?}", self.addr, msg);

                match msg {
                    Message::SSLRequest => {
                        // We don't support SSL yet
                        stream.write_all(b"N").await?;
                    }

                    Message::StartupMessage { parameters, .. } => {
                        self.handle_startup(stream, parameters).await?;
                        // Only mark startup done if we're not waiting for authentication
                        if self.authenticated || !self.auth_db.config().require_auth {
                            startup_done = true;
                        }
                    }

                    Message::PasswordMessage { password } => {
                        if self.handle_password(stream, password).await? {
                            startup_done = true; // Authentication complete
                                                 // ReadyForQuery already sent by send_startup_complete
                        } else {
                            break;
                        }
                    }

                    Message::Query { sql } => {
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_query(stream, &sql).await?;
                            self.send_ready_for_query(stream).await?;
                        }
                    }

                    Message::Parse {
                        statement_name,
                        query,
                        parameter_types,
                    } => {
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_parse(stream, statement_name, query, parameter_types)
                                .await?;
                        }
                    }

                    Message::Bind {
                        portal_name,
                        statement_name,
                        parameter_formats,
                        parameters,
                        result_formats,
                    } => {
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_bind(
                                stream,
                                portal_name,
                                statement_name,
                                parameter_formats,
                                parameters,
                                result_formats,
                            )
                            .await?;
                        }
                    }

                    Message::Execute {
                        portal_name,
                        max_rows,
                    } => {
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_execute(stream, portal_name, max_rows).await?;
                        }
                    }

                    Message::Describe { typ, name } => {
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_describe(stream, typ, name).await?;
                        }
                    }

                    Message::Close { typ, name } => {
                        if !self.authenticated && self.auth_db.config().require_auth {
                            let error = Message::error(
                                protocol::error_codes::INVALID_AUTHORIZATION,
                                "Authentication required",
                            );
                            self.send_message(stream, &error).await?;
                        } else {
                            self.handle_close(stream, typ, name).await?;
                        }
                    }

                    Message::Sync => {
                        // Sync message completes the extended query protocol sequence
                        self.send_ready_for_query(stream).await?;
                    }

                    Message::Terminate => {
                        debug!("Client requested termination");
                        break;
                    }

                    _ => {
                        warn!("Unhandled message type: {:?}", msg);
                        let error = Message::error(
                            protocol::error_codes::FEATURE_NOT_SUPPORTED,
                            "Message type not supported",
                        );
                        self.send_message(stream, &error).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_startup(
        &mut self,
        stream: &mut SecureStream,
        parameters: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        // Extract connection parameters
        self.username = parameters.get("user").cloned();
        if let Some(db) = parameters.get("database") {
            self.database = db.clone();
        }

        let username = self.username.as_deref().unwrap_or("anonymous");
        info!("Startup: user={}, database={}", username, self.database);

        // Check authentication requirements
        let auth_config = self.auth_db.config();

        if !auth_config.require_auth || auth_config.method == protocol::auth::AuthMethod::Trust {
            // Trust authentication or auth disabled
            self.authenticated = true;
            let is_superuser = self
                .username
                .as_ref()
                .map(|u| self.auth_db.is_superuser(u))
                .unwrap_or(false);
            self.rate_limit_manager
                .set_client_auth(self.addr, true, is_superuser);

            // Log successful trust authentication
            self.audit_logger.log_login_success(
                username.to_string(),
                self.addr,
                format!("session_{}", self.process_id),
            );

            self.send_message(stream, &Message::AuthenticationOk)
                .await?;
            self.send_startup_complete(stream).await?;
        } else {
            // Require authentication
            match auth_config.method {
                protocol::auth::AuthMethod::MD5 => {
                    // Generate MD5 challenge
                    let salt = protocol::auth::generate_md5_challenge();
                    self.auth_challenge = Some(salt.to_vec());

                    let auth_msg = Message::AuthenticationMD5Password { salt };
                    info!("Sending MD5 auth challenge to {}", username);
                    self.send_message(stream, &auth_msg).await?;
                    stream.flush().await?; // Ensure the message is sent
                    info!("MD5 auth challenge sent, waiting for password");
                    // Don't return here - let the main loop continue
                }
                protocol::auth::AuthMethod::ScramSha256 => {
                    // Generate SCRAM-SHA-256 challenge
                    if let Some(challenge) =
                        protocol::auth::generate_auth_challenge(&auth_config.method)
                    {
                        self.auth_challenge = Some(challenge.clone());

                        let auth_msg = Message::AuthenticationSASL {
                            mechanisms: vec!["SCRAM-SHA-256".to_string()],
                        };
                        self.send_message(stream, &auth_msg).await?;
                        // Return here to wait for SASL response
                        return Ok(());
                    }
                }
                protocol::auth::AuthMethod::Trust => {
                    // Already handled above
                    unreachable!()
                }
            }
        }

        Ok(())
    }

    async fn send_startup_complete(&self, stream: &mut SecureStream) -> Result<()> {
        // Send backend key data
        let key_data = Message::BackendKeyData {
            process_id: self.process_id,
            secret_key: self.secret_key,
        };
        self.send_message(stream, &key_data).await?;

        // Send parameter status messages
        self.send_parameter_status(stream, "server_version", "14.0 (DriftDB 0.2.0)")
            .await?;
        self.send_parameter_status(stream, "server_encoding", "UTF8")
            .await?;
        self.send_parameter_status(stream, "client_encoding", "UTF8")
            .await?;
        self.send_parameter_status(stream, "DateStyle", "ISO, MDY")
            .await?;
        self.send_parameter_status(stream, "application_name", "")
            .await?;

        // Send ready for query
        self.send_ready_for_query(stream).await?;

        Ok(())
    }

    async fn handle_password(&mut self, stream: &mut SecureStream, password: String) -> Result<bool> {
        let username = self
            .username
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No username provided"))?;

        let client_addr = self.addr.to_string();

        // Prepare challenge salt for MD5
        let challenge_salt = if self.auth_db.config().method == protocol::auth::AuthMethod::MD5 {
            self.auth_challenge.as_ref().and_then(|challenge| {
                if challenge.len() >= 4 {
                    let mut salt = [0u8; 4];
                    salt.copy_from_slice(&challenge[0..4]);
                    Some(salt)
                } else {
                    None
                }
            })
        } else {
            None
        };

        match self
            .auth_db
            .authenticate(username, &password, &client_addr, challenge_salt.as_ref())
        {
            Ok(true) => {
                self.authenticated = true;
                let is_superuser = self.auth_db.is_superuser(username);
                self.rate_limit_manager
                    .set_client_auth(self.addr, true, is_superuser);
                info!(
                    "Authentication successful for {} from {}",
                    username, client_addr
                );

                // Log successful authentication
                self.audit_logger.log_login_success(
                    username.to_string(),
                    self.addr,
                    format!("session_{}", self.process_id),
                );

                // Send authentication OK
                self.send_message(stream, &Message::AuthenticationOk)
                    .await?;

                // Send startup completion
                self.send_startup_complete(stream).await?;

                Ok(true)
            }
            Ok(false) | Err(_) => {
                warn!(
                    "Authentication failed for {} from {}",
                    username, client_addr
                );

                let error_msg = if self
                    .auth_db
                    .get_user_info(username)
                    .map_or(false, |user| user.is_locked())
                {
                    "User account is temporarily locked due to failed login attempts"
                } else {
                    "Authentication failed"
                };

                // Log failed authentication attempt
                self.audit_logger.log_login_failure(
                    username.to_string(),
                    self.addr,
                    error_msg.to_string(),
                );

                let error = Message::error(protocol::error_codes::INVALID_AUTHORIZATION, error_msg);
                self.send_message(stream, &error).await?;
                Ok(false)
            }
        }
    }

    async fn handle_query(&mut self, stream: &mut SecureStream, sql: &str) -> Result<()> {
        info!("Query from {}: {}", self.addr, sql);

        // Check rate limiting for this query
        if !self.rate_limit_manager.allow_query(self.addr, sql) {
            warn!("Query rate limit exceeded for {}: {}", self.addr, sql);

            // Log rate limit exceeded audit event
            use crate::security_audit::{AuditEventType, AuditSeverity, AuditOutcome};
            self.audit_logger.log_event(
                AuditEventType::SuspiciousActivity,
                self.username.clone(),
                self.addr,
                AuditSeverity::Warning,
                "Query rate limit exceeded".to_string(),
                serde_json::json!({
                    "query": sql.chars().take(200).collect::<String>(),
                }),
                AuditOutcome::Blocked,
                Some(format!("session_{}", self.process_id)),
            );

            let error = Message::error(
                protocol::error_codes::TOO_MANY_CONNECTIONS,
                "Rate limit exceeded. Please slow down your requests.",
            );
            self.send_message(stream, &error).await?;
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        // Determine query type for metrics
        let query_type = determine_query_type(sql);

        // Check for user management commands first
        if let Some(result) = self.handle_user_management_query(sql).await? {
            self.send_query_result(stream, result).await?;
            return Ok(());
        }

        // Validate SQL before execution
        if let Err(validation_error) = self.sql_validator.validate_query(sql) {
            warn!(
                "SQL validation failed for query from {}: {}",
                self.addr, validation_error
            );
            let error = Message::error(
                protocol::error_codes::SYNTAX_ERROR,
                &format!("SQL validation failed: {}", validation_error),
            );
            self.send_message(stream, &error).await?;
            return Ok(());
        }

        // Execute query using our SQL executor with shared transaction manager
        let session_id = format!("session_{}", self.process_id);
        let executor = QueryExecutor::new_with_guard_and_transaction_manager(
            &self.engine_guard,
            self.transaction_manager.clone(),
            session_id,
        );
        match executor.execute(sql).await {
            Ok(result) => {
                let duration = start_time.elapsed();
                let duration_secs = duration.as_secs_f64();

                // Update transaction status based on the command
                let sql_upper = sql.trim().to_uppercase();
                if sql_upper.starts_with("BEGIN") {
                    self.transaction_status = TransactionStatus::InTransaction;
                } else if sql_upper.starts_with("COMMIT") || sql_upper.starts_with("ROLLBACK") {
                    self.transaction_status = TransactionStatus::Idle;
                }

                // Record successful query metrics if registry is available
                if crate::metrics::REGISTRY.gather().len() > 0 {
                    crate::metrics::record_query(&query_type, "success", duration_secs);
                }

                // Log slow query if it exceeds threshold
                let rows_affected = match &result {
                    crate::executor::QueryResult::Select { rows, .. } => Some(rows.len() as u64),
                    crate::executor::QueryResult::Insert { count } => Some(*count as u64),
                    crate::executor::QueryResult::Update { count } => Some(*count as u64),
                    crate::executor::QueryResult::Delete { count } => Some(*count as u64),
                    _ => None,
                };

                self.slow_query_logger.log_query(
                    sql.to_string(),
                    duration,
                    self.addr.to_string(),
                    self.username.clone().unwrap_or_else(|| "anonymous".to_string()),
                    self.database.clone(),
                    rows_affected,
                    None,
                );

                self.send_query_result(stream, result).await?;
            }
            Err(e) => {
                let duration = start_time.elapsed();
                let duration_secs = duration.as_secs_f64();

                error!("Query error: {}", e);

                // Record failed query metrics if registry is available
                if crate::metrics::REGISTRY.gather().len() > 0 {
                    crate::metrics::record_query(&query_type, "error", duration_secs);
                    crate::metrics::record_error("query", &query_type);
                }

                // Log slow query even if it failed
                self.slow_query_logger.log_query(
                    sql.to_string(),
                    duration,
                    self.addr.to_string(),
                    self.username.clone().unwrap_or_else(|| "anonymous".to_string()),
                    self.database.clone(),
                    None,
                    Some(format!("error: {}", e)),
                );

                let error = Message::error(
                    protocol::error_codes::SYNTAX_ERROR,
                    &format!("Query error: {}", e),
                );
                self.send_message(stream, &error).await?;
            }
        }

        Ok(())
    }

    async fn handle_user_management_query(
        &self,
        sql: &str,
    ) -> Result<Option<crate::executor::QueryResult>> {
        let sql_upper = sql.trim().to_uppercase();

        // CREATE USER command
        if sql_upper.starts_with("CREATE USER") {
            return self.handle_create_user(sql).await;
        }

        // DROP USER command
        if sql_upper.starts_with("DROP USER") {
            return self.handle_drop_user(sql).await;
        }

        // ALTER USER command (password change)
        if sql_upper.starts_with("ALTER USER") && sql_upper.contains("PASSWORD") {
            return self.handle_alter_user_password(sql).await;
        }

        // SHOW USERS command
        if sql_upper == "SHOW USERS" || sql_upper == "SELECT * FROM PG_USER" {
            return self.handle_show_users().await;
        }

        // SHOW AUTH_ATTEMPTS command
        if sql_upper == "SHOW AUTH_ATTEMPTS" {
            return self.handle_show_auth_attempts().await;
        }

        Ok(None)
    }

    async fn handle_create_user(&self, sql: &str) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser
        let current_username = if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!(
                    "Permission denied: only superusers can create users"
                ));
            }
            username.clone()
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        };

        // Parse CREATE USER statement (simplified)
        // Example: CREATE USER 'testuser' WITH PASSWORD 'testpass' SUPERUSER;
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 6 {
            return Err(anyhow!("Invalid CREATE USER syntax"));
        }

        let new_username = parts[2].trim_matches('\'').trim_matches('"');

        // Find password
        let password_pos = parts.iter().position(|&x| x.to_uppercase() == "PASSWORD");
        let password_idx = match password_pos {
            Some(idx) if idx + 1 < parts.len() => idx + 1,
            _ => return Err(anyhow!("Password required for CREATE USER")),
        };

        let password = parts[password_idx].trim_matches('\'').trim_matches('"');

        // Check for SUPERUSER flag
        let is_superuser = sql.to_uppercase().contains("SUPERUSER");

        // Validate username and password
        protocol::auth::validate_username(new_username)?;
        protocol::auth::validate_password(password)?;

        // Create user
        self.auth_db
            .create_user(new_username.to_string(), password, is_superuser)?;

        // Log user creation audit event
        use crate::security_audit::{AuditEventType, AuditSeverity, AuditOutcome};
        self.audit_logger.log_event(
            AuditEventType::UserCreated,
            Some(current_username.clone()),
            self.addr,
            AuditSeverity::Info,
            format!("User '{}' created with superuser={}", new_username, is_superuser),
            serde_json::json!({
                "new_user": new_username,
                "is_superuser": is_superuser,
                "created_by": current_username
            }),
            AuditOutcome::Success,
            Some(format!("session_{}", self.process_id)),
        );

        Ok(Some(crate::executor::QueryResult::CreateTable))
    }

    async fn handle_drop_user(&self, sql: &str) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser
        let current_username = if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!("Permission denied: only superusers can drop users"));
            }
            username.clone()
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        };

        // Parse DROP USER statement
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(anyhow!("Invalid DROP USER syntax"));
        }

        let target_username = parts[2].trim_matches('\'').trim_matches('"');
        self.auth_db.drop_user(target_username)?;

        // Log user deletion audit event
        use crate::security_audit::{AuditEventType, AuditSeverity, AuditOutcome};
        self.audit_logger.log_event(
            AuditEventType::UserDeleted,
            Some(current_username.clone()),
            self.addr,
            AuditSeverity::Warning,
            format!("User '{}' deleted", target_username),
            serde_json::json!({
                "deleted_user": target_username,
                "deleted_by": current_username
            }),
            AuditOutcome::Success,
            Some(format!("session_{}", self.process_id)),
        );

        Ok(Some(crate::executor::QueryResult::Delete { count: 1 }))
    }

    async fn handle_alter_user_password(
        &self,
        sql: &str,
    ) -> Result<Option<crate::executor::QueryResult>> {
        // Users can change their own password, or superusers can change any password
        let current_user = self
            .username
            .as_ref()
            .ok_or_else(|| anyhow!("Authentication required"))?;

        // Parse ALTER USER statement
        // Example: ALTER USER 'username' PASSWORD 'newpassword';
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 5 {
            return Err(anyhow!("Invalid ALTER USER syntax"));
        }

        let target_username = parts[2].trim_matches('\'').trim_matches('"');

        // Check permissions
        if target_username != current_user && !self.auth_db.is_superuser(current_user) {
            return Err(anyhow!("Permission denied: can only change own password"));
        }

        let password_pos = parts.iter().position(|&x| x.to_uppercase() == "PASSWORD");
        let password_idx = match password_pos {
            Some(idx) if idx + 1 < parts.len() => idx + 1,
            _ => return Err(anyhow!("Password required")),
        };

        let new_password = parts[password_idx].trim_matches('\'').trim_matches('"');
        protocol::auth::validate_password(new_password)?;

        self.auth_db
            .change_password(target_username, new_password)?;

        // Log password change audit event
        use crate::security_audit::{AuditEventType, AuditSeverity, AuditOutcome};
        self.audit_logger.log_event(
            AuditEventType::PasswordChanged,
            Some(current_user.to_string()),
            self.addr,
            AuditSeverity::Warning,
            format!("Password changed for user '{}'", target_username),
            serde_json::json!({
                "target_user": target_username,
                "changed_by": current_user,
                "self_change": target_username == current_user
            }),
            AuditOutcome::Success,
            Some(format!("session_{}", self.process_id)),
        );

        Ok(Some(crate::executor::QueryResult::Update { count: 1 }))
    }

    async fn handle_show_users(&self) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser or authenticated
        if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!(
                    "Permission denied: only superusers can view user list"
                ));
            }
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        }

        let usernames = self.auth_db.list_users();
        let mut rows = Vec::new();

        for username in usernames {
            if let Some(user) = self.auth_db.get_user_info(&username) {
                let is_superuser = user.roles.contains(&"superuser".to_string());
                let row = vec![
                    Value::String(user.username),
                    Value::Bool(is_superuser),
                    Value::String(user.auth_method.to_string()),
                    Value::Number(serde_json::Number::from(user.created_at)),
                    match user.last_login {
                        Some(ts) => Value::Number(serde_json::Number::from(ts)),
                        None => Value::Null,
                    },
                    Value::Number(serde_json::Number::from(user.failed_attempts)),
                    match user.locked_until {
                        Some(ts) => Value::Number(serde_json::Number::from(ts)),
                        None => Value::Null,
                    },
                ];
                rows.push(row);
            }
        }

        Ok(Some(crate::executor::QueryResult::Select {
            columns: vec![
                "username".to_string(),
                "is_superuser".to_string(),
                "auth_method".to_string(),
                "created_at".to_string(),
                "last_login".to_string(),
                "failed_attempts".to_string(),
                "locked_until".to_string(),
            ],
            rows,
        }))
    }

    async fn handle_show_auth_attempts(&self) -> Result<Option<crate::executor::QueryResult>> {
        // Check if current user is superuser
        if let Some(username) = &self.username {
            if !self.auth_db.is_superuser(username) {
                return Err(anyhow!(
                    "Permission denied: only superusers can view auth attempts"
                ));
            }
        } else {
            return Err(anyhow!("Permission denied: authentication required"));
        }

        let attempts = self.auth_db.get_recent_auth_attempts(100);
        let mut rows = Vec::new();

        for attempt in attempts {
            let row = vec![
                Value::String(attempt.username),
                Value::Number(serde_json::Number::from(attempt.timestamp)),
                Value::Bool(attempt.success),
                Value::String(attempt.client_addr),
            ];
            rows.push(row);
        }

        Ok(Some(crate::executor::QueryResult::Select {
            columns: vec![
                "username".to_string(),
                "timestamp".to_string(),
                "success".to_string(),
                "client_addr".to_string(),
            ],
            rows,
        }))
    }

    async fn send_query_result(
        &mut self,
        stream: &mut SecureStream,
        result: crate::executor::QueryResult,
    ) -> Result<()> {
        use crate::executor::QueryResult;

        match result {
            QueryResult::Select { columns, rows } => {
                // Infer proper PostgreSQL data types from the actual data
                let column_types = Self::infer_column_types(&columns, &rows);

                // Send row description with proper data types
                let fields = columns
                    .iter()
                    .zip(column_types.iter())
                    .map(|(col, &data_type)| {
                        protocol::FieldDescription::new(col.clone(), data_type)
                    })
                    .collect();

                let row_desc = Message::RowDescription { fields };
                self.send_message(stream, &row_desc).await?;

                // Store row count before iteration
                let row_count = rows.len();

                // Send data rows
                for row in rows {
                    let values = row
                        .iter()
                        .map(|val| protocol::value_to_postgres_text(val).map(|s| s.into_bytes()))
                        .collect();

                    let data_row = Message::DataRow { values };
                    self.send_message(stream, &data_row).await?;
                }

                // Send command complete
                let complete = Message::CommandComplete {
                    tag: format!("SELECT {}", row_count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Insert { count } => {
                let complete = Message::CommandComplete {
                    tag: format!("INSERT 0 {}", count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Update { count } => {
                let complete = Message::CommandComplete {
                    tag: format!("UPDATE {}", count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Delete { count } => {
                let complete = Message::CommandComplete {
                    tag: format!("DELETE {}", count),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::CreateTable => {
                let complete = Message::CommandComplete {
                    tag: "CREATE TABLE".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::DropTable => {
                let complete = Message::CommandComplete {
                    tag: "DROP TABLE".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::CreateIndex => {
                let complete = Message::CommandComplete {
                    tag: "CREATE INDEX".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Begin => {
                let complete = Message::CommandComplete {
                    tag: "BEGIN".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Commit => {
                let complete = Message::CommandComplete {
                    tag: "COMMIT".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Rollback => {
                let complete = Message::CommandComplete {
                    tag: "ROLLBACK".to_string(),
                };
                self.send_message(stream, &complete).await?;
            }

            QueryResult::Empty => {
                self.send_message(stream, &Message::EmptyQueryResponse)
                    .await?;
            }
        }

        Ok(())
    }

    async fn send_ready_for_query(&self, stream: &mut SecureStream) -> Result<()> {
        let msg = Message::ReadyForQuery {
            status: self.transaction_status.to_byte(),
        };
        self.send_message(stream, &msg).await
    }

    async fn send_parameter_status(
        &self,
        stream: &mut SecureStream,
        name: &str,
        value: &str,
    ) -> Result<()> {
        let msg = Message::ParameterStatus {
            name: name.to_string(),
            value: value.to_string(),
        };
        self.send_message(stream, &msg).await
    }

    async fn send_message(&self, stream: &mut SecureStream, msg: &Message) -> Result<()> {
        let bytes = protocol::codec::encode_message(msg);
        stream.write_all(&bytes).await?;
        stream.flush().await?;
        Ok(())
    }
}

/// Determine the query type from SQL for metrics classification
fn determine_query_type(sql: &str) -> String {
    let sql_upper = sql.trim().to_uppercase();

    if sql_upper.starts_with("SELECT") {
        "SELECT"
    } else if sql_upper.starts_with("INSERT") {
        "INSERT"
    } else if sql_upper.starts_with("UPDATE") {
        "UPDATE"
    } else if sql_upper.starts_with("DELETE") {
        "DELETE"
    } else if sql_upper.starts_with("CREATE") {
        "CREATE"
    } else if sql_upper.starts_with("DROP") {
        "DROP"
    } else if sql_upper.starts_with("ALTER") {
        "ALTER"
    } else if sql_upper.starts_with("PATCH") {
        "PATCH"
    } else if sql_upper.starts_with("SOFT DELETE") {
        "SOFT_DELETE"
    } else {
        "OTHER"
    }
    .to_string()
}

impl Session {
    async fn handle_parse(
        &mut self,
        stream: &mut SecureStream,
        name: String,
        sql: String,
        param_types: Vec<i32>,
    ) -> Result<()> {
        info!(
            "Parse: name='{}', sql='{}', param_types={:?}",
            name, sql, param_types
        );

        match self
            .prepared_statements
            .parse_statement(name, sql, param_types)
        {
            Ok(_) => {
                // Send ParseComplete message
                self.send_message(stream, &Message::ParseComplete).await?;
            }
            Err(e) => {
                error!("Parse error: {}", e);
                let error = Message::error(
                    protocol::error_codes::SYNTAX_ERROR,
                    &format!("Parse error: {}", e),
                );
                self.send_message(stream, &error).await?;
            }
        }

        Ok(())
    }

    async fn handle_bind(
        &mut self,
        stream: &mut SecureStream,
        portal_name: String,
        statement_name: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<()> {
        info!(
            "Bind: portal='{}', statement='{}', params={}",
            portal_name,
            statement_name,
            params.len()
        );

        match self.prepared_statements.bind_portal(
            portal_name,
            statement_name,
            params,
            param_formats,
            result_formats,
        ) {
            Ok(_) => {
                // Send BindComplete message
                self.send_message(stream, &Message::BindComplete).await?;
            }
            Err(e) => {
                error!("Bind error: {}", e);
                let error = Message::error(
                    protocol::error_codes::INVALID_SQL_STATEMENT_NAME,
                    &format!("Bind error: {}", e),
                );
                self.send_message(stream, &error).await?;
            }
        }

        Ok(())
    }

    async fn handle_execute(
        &mut self,
        stream: &mut SecureStream,
        portal_name: String,
        max_rows: i32,
    ) -> Result<()> {
        info!("Execute: portal='{}', max_rows={}", portal_name, max_rows);

        // Check rate limiting
        if !self.rate_limit_manager.allow_query(self.addr, "EXECUTE") {
            warn!("Query rate limit exceeded for {}: EXECUTE", self.addr);
            let error = Message::error(
                protocol::error_codes::TOO_MANY_CONNECTIONS,
                "Rate limit exceeded. Please slow down your requests.",
            );
            self.send_message(stream, &error).await?;
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        // Get the SQL with parameters substituted
        match self
            .prepared_statements
            .execute_portal(&portal_name, max_rows)
        {
            Ok(sql) => {
                info!("Executing prepared statement: {}", sql);

                // Validate SQL before execution
                if let Err(validation_error) = self.sql_validator.validate_query(&sql) {
                    warn!(
                        "SQL validation failed for prepared statement from {}: {}",
                        self.addr, validation_error
                    );
                    let error = Message::error(
                        protocol::error_codes::SYNTAX_ERROR,
                        &format!("SQL validation failed: {}", validation_error),
                    );
                    self.send_message(stream, &error).await?;
                    return Ok(());
                }

                // Execute the query using our SQL executor with shared transaction manager
                let session_id = format!("session_{}", self.process_id);
                let executor = QueryExecutor::new_with_guard_and_transaction_manager(
                    &self.engine_guard,
                    self.transaction_manager.clone(),
                    session_id,
                );
                match executor.execute(&sql).await {
                    Ok(result) => {
                        let duration = start_time.elapsed();
                        let duration_secs = duration.as_secs_f64();

                        // Update transaction status based on the command
                        let sql_upper = sql.trim().to_uppercase();
                        if sql_upper.starts_with("BEGIN") {
                            self.transaction_status = TransactionStatus::InTransaction;
                        } else if sql_upper.starts_with("COMMIT")
                            || sql_upper.starts_with("ROLLBACK")
                        {
                            self.transaction_status = TransactionStatus::Idle;
                        }

                        // Record successful query metrics if registry is available
                        if crate::metrics::REGISTRY.gather().len() > 0 {
                            let query_type = determine_query_type(&sql);
                            crate::metrics::record_query(&query_type, "success", duration_secs);
                        }

                        // Log slow query if it exceeds threshold
                        let rows_affected = match &result {
                            crate::executor::QueryResult::Select { rows, .. } => Some(rows.len() as u64),
                            crate::executor::QueryResult::Insert { count } => Some(*count as u64),
                            crate::executor::QueryResult::Update { count } => Some(*count as u64),
                            crate::executor::QueryResult::Delete { count } => Some(*count as u64),
                            _ => None,
                        };

                        self.slow_query_logger.log_query(
                            sql.to_string(),
                            duration,
                            self.addr.to_string(),
                            self.username.clone().unwrap_or_else(|| "anonymous".to_string()),
                            self.database.clone(),
                            rows_affected,
                            Some(format!("prepared_statement={}", portal_name)),
                        );

                        self.send_query_result(stream, result).await?;
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        let duration_secs = duration.as_secs_f64();
                        error!("Execute error: {}", e);

                        // Record failed query metrics if registry is available
                        if crate::metrics::REGISTRY.gather().len() > 0 {
                            let query_type = determine_query_type(&sql);
                            crate::metrics::record_query(&query_type, "error", duration_secs);
                            crate::metrics::record_error("query", &query_type);
                        }

                        // Log slow query even if it failed
                        self.slow_query_logger.log_query(
                            sql.to_string(),
                            duration,
                            self.addr.to_string(),
                            self.username.clone().unwrap_or_else(|| "anonymous".to_string()),
                            self.database.clone(),
                            None,
                            Some(format!("prepared_statement={}, error: {}", portal_name, e)),
                        );

                        let error = Message::error(
                            protocol::error_codes::SYNTAX_ERROR,
                            &format!("Execute error: {}", e),
                        );
                        self.send_message(stream, &error).await?;
                    }
                }
            }
            Err(e) => {
                error!("Portal execution error: {}", e);
                let error = Message::error(
                    protocol::error_codes::INVALID_CURSOR_NAME,
                    &format!("Portal not found: {}", e),
                );
                self.send_message(stream, &error).await?;
            }
        }

        Ok(())
    }

    async fn handle_describe(
        &mut self,
        stream: &mut SecureStream,
        object_type: u8,
        name: String,
    ) -> Result<()> {
        info!(
            "Describe: type={}, name='{}'",
            if object_type == b'S' {
                "Statement"
            } else {
                "Portal"
            },
            name
        );

        // For now, send a simple response indicating no parameters and text results
        // A full implementation would analyze the query and return proper column info
        if object_type == b'S' {
            // Describe statement
            match self.prepared_statements.describe_statement(&name) {
                Ok(stmt) => {
                    // Send ParameterDescription
                    let param_desc = Message::ParameterDescription {
                        types: stmt.param_types.iter().map(|t| t.unwrap_or(0)).collect(),
                    };
                    self.send_message(stream, &param_desc).await?;

                    // For now, send NoData as we can't determine result columns without execution
                    self.send_message(stream, &Message::NoData).await?;
                }
                Err(e) => {
                    error!("Describe statement error: {}", e);
                    let error = Message::error(
                        protocol::error_codes::INVALID_SQL_STATEMENT_NAME,
                        &format!("Statement not found: {}", e),
                    );
                    self.send_message(stream, &error).await?;
                }
            }
        } else {
            // Describe portal
            match self.prepared_statements.describe_portal(&name) {
                Ok(_portal) => {
                    // For now, send NoData as we can't determine result columns without execution
                    self.send_message(stream, &Message::NoData).await?;
                }
                Err(e) => {
                    error!("Describe portal error: {}", e);
                    let error = Message::error(
                        protocol::error_codes::INVALID_CURSOR_NAME,
                        &format!("Portal not found: {}", e),
                    );
                    self.send_message(stream, &error).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_close(
        &mut self,
        stream: &mut SecureStream,
        object_type: u8,
        name: String,
    ) -> Result<()> {
        info!(
            "Close: type={}, name='{}'",
            if object_type == b'S' {
                "Statement"
            } else {
                "Portal"
            },
            name
        );

        let result = if object_type == b'S' {
            // Close statement
            self.prepared_statements.close_statement(&name)
        } else {
            // Close portal
            self.prepared_statements.close_portal(&name)
        };

        match result {
            Ok(_) => {
                // Send CloseComplete message
                self.send_message(stream, &Message::CloseComplete).await?;
            }
            Err(e) => {
                // Log but still send CloseComplete (PostgreSQL behavior)
                debug!("Close error (ignored): {}", e);
                self.send_message(stream, &Message::CloseComplete).await?;
            }
        }

        Ok(())
    }

    /// Infer PostgreSQL data type from a sample value
    fn infer_postgres_type(value: &Value) -> protocol::DataType {
        match value {
            Value::Bool(_) => protocol::DataType::Bool,
            Value::Number(n) => {
                if n.is_i64() {
                    let val = n.as_i64().unwrap();
                    if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                        protocol::DataType::Int4  // 32-bit integer
                    } else {
                        protocol::DataType::Int8  // 64-bit integer
                    }
                } else if n.is_f64() {
                    protocol::DataType::Float8  // Double precision
                } else {
                    protocol::DataType::Text  // Fallback
                }
            }
            Value::String(_) => protocol::DataType::Text,
            Value::Array(_) | Value::Object(_) => protocol::DataType::Json,
            Value::Null => protocol::DataType::Text,  // Default for NULL values
        }
    }

    /// Infer PostgreSQL data types for columns from sample data
    fn infer_column_types(columns: &[String], rows: &[Vec<Value>]) -> Vec<protocol::DataType> {
        if rows.is_empty() {
            // No data, default to Text for all columns
            return vec![protocol::DataType::Text; columns.len()];
        }

        columns
            .iter()
            .enumerate()
            .map(|(col_idx, _)| {
                // Sample the first few non-null values to infer type
                for row in rows.iter().take(5) {  // Sample up to 5 rows
                    if col_idx < row.len() && !row[col_idx].is_null() {
                        return Self::infer_postgres_type(&row[col_idx]);
                    }
                }
                // If all sampled values are null, default to Text
                protocol::DataType::Text
            })
            .collect()
    }
}
