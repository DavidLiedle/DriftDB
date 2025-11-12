//! Security audit logging for DriftDB
//!
//! Tracks security-relevant events for compliance and incident response
//! Provides tamper-evident logging with cryptographic checksums

#![allow(dead_code)]

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use uuid::Uuid;

/// Configuration for security audit logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,
    /// Maximum number of audit entries to keep in memory
    pub max_stored_entries: usize,
    /// Log to file
    pub log_to_file: bool,
    /// Path to audit log file
    pub log_file_path: String,
    /// Log suspicious activity patterns
    pub log_suspicious_patterns: bool,
    /// Threshold for suspicious failed login attempts
    pub suspicious_login_threshold: u32,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_stored_entries: 10000,
            log_to_file: true,
            log_file_path: "./logs/security_audit.log".to_string(),
            log_suspicious_patterns: true,
            suspicious_login_threshold: 5,
        }
    }
}

/// Types of security events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    // Authentication events
    LoginSuccess,
    LoginFailure,
    Logout,
    SessionExpired,

    // Authorization events
    AccessDenied,
    PermissionDenied,

    // User management events
    UserCreated,
    UserDeleted,
    PasswordChanged,
    UserLocked,
    UserUnlocked,

    // Role/permission events
    RoleGranted,
    RoleRevoked,
    PermissionGranted,
    PermissionRevoked,

    // Configuration events
    ConfigChanged,
    SecurityPolicyChanged,

    // Suspicious activity
    SuspiciousActivity,
    BruteForceAttempt,
    UnauthorizedAccessAttempt,

    // Data access events
    SensitiveDataAccess,
    DataExport,
    MassDataDeletion,
}

/// Security audit event entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unique event ID
    pub event_id: String,
    /// Timestamp when event occurred
    pub timestamp: u64,
    /// Type of security event
    pub event_type: AuditEventType,
    /// Username involved (if applicable)
    pub username: Option<String>,
    /// Client address
    pub client_addr: String,
    /// Event severity (info, warning, critical)
    pub severity: AuditSeverity,
    /// Detailed event description
    pub description: String,
    /// Additional context data
    pub metadata: serde_json::Value,
    /// Outcome of the event (success, failure, blocked)
    pub outcome: AuditOutcome,
    /// Session ID (if applicable)
    pub session_id: Option<String>,
    /// Cryptographic checksum for tamper detection
    pub checksum: String,
}

/// Severity level of audit event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuditSeverity {
    Info,
    Warning,
    Critical,
}

/// Outcome of audited event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuditOutcome {
    Success,
    Failure,
    Blocked,
    Denied,
}

impl AuditEntry {
    /// Calculate checksum for tamper detection
    fn calculate_checksum(&self) -> String {
        use sha2::{Digest, Sha256};

        let data = format!(
            "{}|{}|{:?}|{}|{}|{:?}|{}",
            self.event_id,
            self.timestamp,
            self.event_type,
            self.username.as_deref().unwrap_or(""),
            self.client_addr,
            self.outcome,
            self.description
        );

        let hash = Sha256::digest(data.as_bytes());
        format!("{:x}", hash)
    }

    /// Verify checksum to detect tampering
    pub fn verify_checksum(&self) -> bool {
        let expected = self.calculate_checksum();
        expected == self.checksum
    }
}

/// Security audit logger
pub struct SecurityAuditLogger {
    config: Arc<RwLock<AuditConfig>>,
    entries: Arc<RwLock<VecDeque<AuditEntry>>>,
    failed_login_tracker: Arc<RwLock<std::collections::HashMap<String, u32>>>,
}

impl SecurityAuditLogger {
    /// Create a new security audit logger
    pub fn new(config: AuditConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            entries: Arc::new(RwLock::new(VecDeque::new())),
            failed_login_tracker: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Log a security audit event
    #[allow(clippy::too_many_arguments)]
    pub fn log_event(
        &self,
        event_type: AuditEventType,
        username: Option<String>,
        client_addr: SocketAddr,
        severity: AuditSeverity,
        description: String,
        metadata: serde_json::Value,
        outcome: AuditOutcome,
        session_id: Option<String>,
    ) {
        let config = self.config.read();
        if !config.enabled {
            return;
        }

        let event_id = Uuid::new_v4().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();

        let mut entry = AuditEntry {
            event_id: event_id.clone(),
            timestamp,
            event_type: event_type.clone(),
            username: username.clone(),
            client_addr: client_addr.to_string(),
            severity: severity.clone(),
            description: description.clone(),
            metadata,
            outcome: outcome.clone(),
            session_id,
            checksum: String::new(),
        };

        // Calculate checksum for tamper detection
        entry.checksum = entry.calculate_checksum();

        // Track failed login attempts for brute force detection
        if event_type == AuditEventType::LoginFailure {
            if let Some(ref user) = username {
                let mut tracker = self.failed_login_tracker.write();
                let count = tracker.entry(user.clone()).or_insert(0);
                *count += 1;

                if config.log_suspicious_patterns && *count >= config.suspicious_login_threshold {
                    // Log suspicious brute force attempt
                    self.log_suspicious_activity(
                        user.clone(),
                        client_addr,
                        format!("{} failed login attempts detected", count),
                    );
                }
            }
        } else if event_type == AuditEventType::LoginSuccess {
            // Reset failed login counter on successful login
            if let Some(ref user) = username {
                self.failed_login_tracker.write().remove(user);
            }
        }

        // Store in memory
        {
            let mut entries = self.entries.write();
            entries.push_back(entry.clone());

            // Keep only most recent entries
            while entries.len() > config.max_stored_entries {
                entries.pop_front();
            }
        }

        // Log to stdout for critical events
        if severity == AuditSeverity::Critical {
            warn!(
                "SECURITY AUDIT [CRITICAL] event={:?} user={} client={} outcome={:?} desc={}",
                event_type,
                username.as_deref().unwrap_or("unknown"),
                client_addr,
                outcome,
                description
            );
        }

        // Log to file if enabled
        if config.log_to_file {
            self.log_to_file(&entry);
        }
    }

    /// Log authentication success
    pub fn log_login_success(
        &self,
        username: String,
        client_addr: SocketAddr,
        session_id: String,
    ) {
        self.log_event(
            AuditEventType::LoginSuccess,
            Some(username.clone()),
            client_addr,
            AuditSeverity::Info,
            format!("User {} logged in successfully", username),
            serde_json::json!({"session_id": session_id}),
            AuditOutcome::Success,
            Some(session_id),
        );
    }

    /// Log authentication failure
    pub fn log_login_failure(
        &self,
        username: String,
        client_addr: SocketAddr,
        reason: String,
    ) {
        self.log_event(
            AuditEventType::LoginFailure,
            Some(username.clone()),
            client_addr,
            AuditSeverity::Warning,
            format!("Failed login attempt for user {}: {}", username, reason),
            serde_json::json!({"reason": reason}),
            AuditOutcome::Failure,
            None,
        );
    }

    /// Log access denied event
    pub fn log_access_denied(
        &self,
        username: Option<String>,
        client_addr: SocketAddr,
        resource: String,
        reason: String,
    ) {
        self.log_event(
            AuditEventType::AccessDenied,
            username.clone(),
            client_addr,
            AuditSeverity::Warning,
            format!(
                "Access denied to {} for user {}: {}",
                resource,
                username.as_deref().unwrap_or("unknown"),
                reason
            ),
            serde_json::json!({"resource": resource, "reason": reason}),
            AuditOutcome::Denied,
            None,
        );
    }

    /// Log user creation
    pub fn log_user_created(
        &self,
        created_by: String,
        new_user: String,
        is_superuser: bool,
        client_addr: SocketAddr,
    ) {
        self.log_event(
            AuditEventType::UserCreated,
            Some(created_by.clone()),
            client_addr,
            AuditSeverity::Info,
            format!("User {} created by {}", new_user, created_by),
            serde_json::json!({"new_user": new_user, "is_superuser": is_superuser}),
            AuditOutcome::Success,
            None,
        );
    }

    /// Log user deletion
    pub fn log_user_deleted(
        &self,
        deleted_by: String,
        deleted_user: String,
        client_addr: SocketAddr,
    ) {
        self.log_event(
            AuditEventType::UserDeleted,
            Some(deleted_by.clone()),
            client_addr,
            AuditSeverity::Warning,
            format!("User {} deleted by {}", deleted_user, deleted_by),
            serde_json::json!({"deleted_user": deleted_user}),
            AuditOutcome::Success,
            None,
        );
    }

    /// Log password change
    pub fn log_password_changed(
        &self,
        username: String,
        changed_by: String,
        client_addr: SocketAddr,
    ) {
        self.log_event(
            AuditEventType::PasswordChanged,
            Some(username.clone()),
            client_addr,
            AuditSeverity::Info,
            format!("Password changed for user {} by {}", username, changed_by),
            serde_json::json!({"changed_by": changed_by}),
            AuditOutcome::Success,
            None,
        );
    }

    /// Log suspicious activity
    fn log_suspicious_activity(
        &self,
        username: String,
        client_addr: SocketAddr,
        description: String,
    ) {
        self.log_event(
            AuditEventType::SuspiciousActivity,
            Some(username),
            client_addr,
            AuditSeverity::Critical,
            description,
            serde_json::json!({}),
            AuditOutcome::Blocked,
            None,
        );
    }

    /// Write audit entry to log file
    fn log_to_file(&self, entry: &AuditEntry) {
        let config = self.config.read();
        let log_path = &config.log_file_path;

        // Ensure log directory exists
        if let Some(parent) = std::path::Path::new(log_path).parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Failed to create audit log directory: {}", e);
                return;
            }
        }

        // Format log entry as JSON with newline
        let log_line = match serde_json::to_string(entry) {
            Ok(json) => format!("{}\n", json),
            Err(e) => {
                warn!("Failed to serialize audit entry: {}", e);
                return;
            }
        };

        // Append to log file
        use std::fs::OpenOptions;
        use std::io::Write;

        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            Ok(mut file) => {
                if let Err(e) = file.write_all(log_line.as_bytes()) {
                    warn!("Failed to write to audit log: {}", e);
                }
            }
            Err(e) => {
                warn!("Failed to open audit log file: {}", e);
            }
        }
    }

    /// Get recent audit entries
    pub fn get_recent_entries(&self, limit: usize) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get audit entries within a time range
    pub fn get_entries_in_range(
        &self,
        start_timestamp: u64,
        end_timestamp: u64,
    ) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.timestamp >= start_timestamp && e.timestamp <= end_timestamp)
            .cloned()
            .collect()
    }

    /// Get audit entries by event type
    pub fn get_entries_by_type(&self, event_type: AuditEventType) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.event_type == event_type)
            .cloned()
            .collect()
    }

    /// Get audit entries by username
    pub fn get_entries_by_user(&self, username: &str) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.username.as_deref() == Some(username))
            .cloned()
            .collect()
    }

    /// Get audit entries by severity
    pub fn get_entries_by_severity(&self, severity: AuditSeverity) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.severity == severity)
            .cloned()
            .collect()
    }

    /// Get statistics about audit events
    pub fn get_statistics(&self) -> AuditStatistics {
        let entries = self.entries.read();

        if entries.is_empty() {
            return AuditStatistics::default();
        }

        let total_events = entries.len();
        let critical_events = entries.iter().filter(|e| e.severity == AuditSeverity::Critical).count();
        let warning_events = entries.iter().filter(|e| e.severity == AuditSeverity::Warning).count();
        let failed_logins = entries.iter().filter(|e| e.event_type == AuditEventType::LoginFailure).count();
        let successful_logins = entries.iter().filter(|e| e.event_type == AuditEventType::LoginSuccess).count();
        let access_denied_events = entries.iter().filter(|e| e.event_type == AuditEventType::AccessDenied).count();
        let suspicious_events = entries.iter().filter(|e| e.event_type == AuditEventType::SuspiciousActivity).count();

        // Get unique users
        let unique_users: std::collections::HashSet<_> = entries
            .iter()
            .filter_map(|e| e.username.as_ref())
            .collect();

        AuditStatistics {
            total_events,
            critical_events,
            warning_events,
            failed_logins,
            successful_logins,
            access_denied_events,
            suspicious_events,
            unique_users: unique_users.len(),
        }
    }

    /// Verify integrity of audit log (check all checksums)
    pub fn verify_integrity(&self) -> AuditIntegrityReport {
        let entries = self.entries.read();
        let total_entries = entries.len();
        let mut tampered_entries = Vec::new();

        for entry in entries.iter() {
            if !entry.verify_checksum() {
                tampered_entries.push(entry.event_id.clone());
            }
        }

        let is_intact = tampered_entries.is_empty();
        AuditIntegrityReport {
            total_entries,
            valid_entries: total_entries - tampered_entries.len(),
            tampered_entries,
            integrity_verified: is_intact,
        }
    }

    /// Clear audit log (admin only)
    pub fn clear(&self) {
        self.entries.write().clear();
        self.failed_login_tracker.write().clear();
        info!("Security audit log cleared");
    }

    /// Update configuration
    pub fn update_config(&self, config: AuditConfig) {
        *self.config.write() = config;
        info!("Security audit configuration updated");
    }

    /// Get current configuration
    pub fn get_config(&self) -> AuditConfig {
        self.config.read().clone()
    }
}

/// Statistics about audit events
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuditStatistics {
    pub total_events: usize,
    pub critical_events: usize,
    pub warning_events: usize,
    pub failed_logins: usize,
    pub successful_logins: usize,
    pub access_denied_events: usize,
    pub suspicious_events: usize,
    pub unique_users: usize,
}

/// Audit log integrity verification report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditIntegrityReport {
    pub total_entries: usize,
    pub valid_entries: usize,
    pub tampered_entries: Vec<String>,
    pub integrity_verified: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5433)
    }

    #[test]
    fn test_audit_logging() {
        let config = AuditConfig {
            enabled: true,
            max_stored_entries: 100,
            log_to_file: false,
            log_file_path: "/tmp/test_audit.log".to_string(),
            log_suspicious_patterns: true,
            suspicious_login_threshold: 3,
        };

        let logger = SecurityAuditLogger::new(config);

        // Log successful login
        logger.log_login_success(
            "testuser".to_string(),
            test_addr(),
            "session_123".to_string(),
        );

        // Log failed login
        logger.log_login_failure(
            "testuser".to_string(),
            test_addr(),
            "invalid password".to_string(),
        );

        let recent = logger.get_recent_entries(10);
        assert_eq!(recent.len(), 2);

        // Verify checksums
        for entry in &recent {
            assert!(entry.verify_checksum(), "Checksum should be valid");
        }
    }

    #[test]
    fn test_brute_force_detection() {
        let config = AuditConfig {
            enabled: true,
            max_stored_entries: 100,
            log_to_file: false,
            log_file_path: "/tmp/test_audit.log".to_string(),
            log_suspicious_patterns: true,
            suspicious_login_threshold: 3,
        };

        let logger = SecurityAuditLogger::new(config);

        // Simulate failed login attempts
        for _ in 0..5 {
            logger.log_login_failure(
                "testuser".to_string(),
                test_addr(),
                "invalid password".to_string(),
            );
        }

        let recent = logger.get_recent_entries(10);

        // Should have logged suspicious activity
        let suspicious = recent.iter().any(|e| e.event_type == AuditEventType::SuspiciousActivity);
        assert!(suspicious, "Should detect suspicious activity after threshold");
    }

    #[test]
    fn test_audit_statistics() {
        let config = AuditConfig::default();
        let logger = SecurityAuditLogger::new(config);

        // Log various events
        logger.log_login_success("user1".to_string(), test_addr(), "session1".to_string());
        logger.log_login_failure("user2".to_string(), test_addr(), "wrong password".to_string());
        logger.log_access_denied(Some("user3".to_string()), test_addr(), "table1".to_string(), "no permission".to_string());

        let stats = logger.get_statistics();
        assert_eq!(stats.total_events, 3);
        assert_eq!(stats.successful_logins, 1);
        assert_eq!(stats.failed_logins, 1);
        assert_eq!(stats.access_denied_events, 1);
    }

    #[test]
    fn test_integrity_verification() {
        let config = AuditConfig::default();
        let logger = SecurityAuditLogger::new(config);

        logger.log_login_success("user1".to_string(), test_addr(), "session1".to_string());

        let report = logger.verify_integrity();
        assert!(report.integrity_verified);
        assert_eq!(report.valid_entries, 1);
        assert_eq!(report.tampered_entries.len(), 0);
    }
}
