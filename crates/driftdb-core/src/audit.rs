use crate::auth::AuthContext;
use crate::errors::{DriftError, Result};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Comprehensive audit logging system
pub struct AuditSystem {
    config: AuditConfig,
    logger: Arc<Mutex<AuditLogger>>,
    storage: Arc<RwLock<AuditStorage>>,
    filters: Arc<RwLock<Vec<Box<dyn AuditFilter>>>>,
    processors: Arc<RwLock<Vec<Box<dyn AuditProcessor>>>>,
    stats: Arc<RwLock<AuditStats>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    pub enabled: bool,
    pub log_file_path: PathBuf,
    pub rotation_size_mb: u64,
    pub retention_days: u32,
    pub buffer_size: usize,
    pub async_logging: bool,
    pub include_query_results: bool,
    pub include_sensitive_data: bool,
    pub compression_enabled: bool,
    pub encryption_enabled: bool,
    pub event_filters: Vec<EventFilter>,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_file_path: PathBuf::from("./audit/audit.log"),
            rotation_size_mb: 100,
            retention_days: 90,
            buffer_size: 1000,
            async_logging: true,
            include_query_results: false,
            include_sensitive_data: false,
            compression_enabled: true,
            encryption_enabled: false,
            event_filters: Vec::new(),
        }
    }
}

/// Audit event that captures all database activity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: Uuid,
    pub timestamp: SystemTime,
    pub event_type: AuditEventType,
    pub user: Option<UserInfo>,
    pub session_id: Option<String>,
    pub client_address: Option<String>,
    pub database: Option<String>,
    pub table: Option<String>,
    pub action: AuditAction,
    pub query: Option<String>,
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    pub affected_rows: Option<u64>,
    pub execution_time_ms: Option<u64>,
    pub success: bool,
    pub error_message: Option<String>,
    pub metadata: HashMap<String, String>,
    pub risk_score: RiskScore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_id: Uuid,
    pub username: String,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditEventType {
    Authentication,
    Authorization,
    DataAccess,
    DataModification,
    SchemaChange,
    SecurityEvent,
    SystemEvent,
    Administrative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditAction {
    // Authentication actions
    Login,
    Logout,
    LoginFailed,
    PasswordChange,
    SessionCreated,
    SessionExpired,

    // Data access actions
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    Copy,
    Export,
    Import,

    // Schema actions
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    CreateView,
    DropView,
    CreateProcedure,
    DropProcedure,

    // Administrative actions
    CreateUser,
    DropUser,
    GrantPermission,
    RevokePermission,
    CreateRole,
    DropRole,
    Backup,
    Restore,
    Configuration,

    // Security events
    PermissionDenied,
    SuspiciousActivity,
    PolicyViolation,
    DataExfiltration,
    SqlInjectionAttempt,
    BruteForceAttempt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RiskScore {
    pub level: RiskLevel,
    pub score: u8, // 0-100
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RiskLevel {
    None,
    Low,
    Medium,
    High,
    Critical,
}

/// Filter for audit events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    pub name: String,
    pub include_types: Option<Vec<AuditEventType>>,
    pub exclude_types: Option<Vec<AuditEventType>>,
    pub include_actions: Option<Vec<AuditAction>>,
    pub exclude_actions: Option<Vec<AuditAction>>,
    pub include_users: Option<Vec<String>>,
    pub exclude_users: Option<Vec<String>>,
    pub include_tables: Option<Vec<String>>,
    pub exclude_tables: Option<Vec<String>>,
    pub min_risk_score: Option<u8>,
}

/// Trait for custom audit event processors
pub trait AuditProcessor: Send + Sync {
    fn name(&self) -> &str;
    fn process(&mut self, event: &AuditEvent) -> Result<()>;
}

/// Filter trait for audit events
pub trait AuditFilter: Send + Sync {
    fn should_log(&self, event: &AuditEvent) -> bool;
}

/// Logger that writes audit events
struct AuditLogger {
    current_file: Option<BufWriter<File>>,
    current_file_path: PathBuf,
    current_size: u64,
    rotation_size: u64,
    base_path: PathBuf,
    rotation_counter: u32,
}

/// Storage for audit logs
struct AuditStorage {
    memory_buffer: VecDeque<AuditEvent>,
    max_buffer_size: usize,
    indexed_events: HashMap<Uuid, AuditEvent>,
    user_index: HashMap<String, Vec<Uuid>>,
    table_index: HashMap<String, Vec<Uuid>>,
    time_index: Vec<(SystemTime, Uuid)>,
}

/// Statistics for audit logging
#[derive(Debug, Default)]
pub struct AuditStats {
    pub events_logged: u64,
    pub events_filtered: u64,
    pub events_failed: u64,
    pub bytes_written: u64,
    pub files_rotated: u32,
    pub avg_event_size: u64,
    pub high_risk_events: u64,
    pub security_violations: u64,
}

impl AuditSystem {
    pub fn new(config: AuditConfig) -> Result<Self> {
        // Create audit directory if it doesn't exist
        if let Some(parent) = config.log_file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let logger = AuditLogger::new(
            config.log_file_path.clone(),
            config.rotation_size_mb * 1024 * 1024,
        )?;

        let storage = AuditStorage::new(config.buffer_size);

        Ok(Self {
            config,
            logger: Arc::new(Mutex::new(logger)),
            storage: Arc::new(RwLock::new(storage)),
            filters: Arc::new(RwLock::new(Vec::new())),
            processors: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(AuditStats::default())),
        })
    }

    /// Log an audit event
    pub fn log_event(&self, mut event: AuditEvent) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Apply filters
        if !self.should_log(&event) {
            self.stats.write().events_filtered += 1;
            return Ok(());
        }

        // Calculate risk score if not set
        if event.risk_score.score == 0 {
            event.risk_score = self.calculate_risk_score(&event);
        }

        // Process event through custom processors
        for processor in self.processors.write().iter_mut() {
            if let Err(e) = processor.process(&event) {
                tracing::error!("Audit processor {} failed: {}", processor.name(), e);
            }
        }

        // Store in memory buffer
        self.storage.write().add_event(event.clone());

        // Write to file
        if let Err(e) = self.write_event(&event) {
            self.stats.write().events_failed += 1;
            return Err(e);
        }

        // Update statistics
        let mut stats = self.stats.write();
        stats.events_logged += 1;
        if event.risk_score.level as u8 >= RiskLevel::High as u8 {
            stats.high_risk_events += 1;
        }
        if matches!(event.event_type, AuditEventType::SecurityEvent) {
            stats.security_violations += 1;
        }

        Ok(())
    }

    /// Log a security event with high priority
    pub fn log_security_event(
        &self,
        action: AuditAction,
        user: Option<&AuthContext>,
        message: &str,
        risk_level: RiskLevel,
    ) -> Result<()> {
        let event = AuditEvent {
            id: Uuid::new_v4(),
            timestamp: SystemTime::now(),
            event_type: AuditEventType::SecurityEvent,
            user: user.map(|ctx| UserInfo {
                user_id: ctx.user.id,
                username: ctx.user.username.clone(),
                roles: ctx.user.roles.iter().cloned().collect(),
            }),
            session_id: user.map(|ctx| ctx.session.id.to_string()),
            client_address: user.and_then(|ctx| ctx.session.ip_address.clone()),
            database: None,
            table: None,
            action,
            query: None,
            parameters: None,
            affected_rows: None,
            execution_time_ms: None,
            success: false,
            error_message: Some(message.to_string()),
            metadata: HashMap::new(),
            risk_score: RiskScore {
                level: risk_level,
                score: match risk_level {
                    RiskLevel::None => 0,
                    RiskLevel::Low => 25,
                    RiskLevel::Medium => 50,
                    RiskLevel::High => 75,
                    RiskLevel::Critical => 100,
                },
            },
        };

        self.log_event(event)
    }

    fn should_log(&self, event: &AuditEvent) -> bool {
        // Check event filters
        for filter in &self.config.event_filters {
            if !Self::matches_filter(event, filter) {
                return false;
            }
        }

        // Check custom filters
        for filter in self.filters.read().iter() {
            if !filter.should_log(event) {
                return false;
            }
        }

        true
    }

    fn matches_filter(event: &AuditEvent, filter: &EventFilter) -> bool {
        // Check event type
        if let Some(ref include) = filter.include_types {
            if !include.contains(&event.event_type) {
                return false;
            }
        }
        if let Some(ref exclude) = filter.exclude_types {
            if exclude.contains(&event.event_type) {
                return false;
            }
        }

        // Check action
        if let Some(ref include) = filter.include_actions {
            if !include.contains(&event.action) {
                return false;
            }
        }
        if let Some(ref exclude) = filter.exclude_actions {
            if exclude.contains(&event.action) {
                return false;
            }
        }

        // Check user
        if let Some(ref user) = event.user {
            if let Some(ref include) = filter.include_users {
                if !include.contains(&user.username) {
                    return false;
                }
            }
            if let Some(ref exclude) = filter.exclude_users {
                if exclude.contains(&user.username) {
                    return false;
                }
            }
        }

        // Check table
        if let Some(ref table) = event.table {
            if let Some(ref include) = filter.include_tables {
                if !include.contains(table) {
                    return false;
                }
            }
            if let Some(ref exclude) = filter.exclude_tables {
                if exclude.contains(table) {
                    return false;
                }
            }
        }

        // Check risk score
        if let Some(min_score) = filter.min_risk_score {
            if event.risk_score.score < min_score {
                return false;
            }
        }

        true
    }

    fn calculate_risk_score(&self, event: &AuditEvent) -> RiskScore {
        let mut score = 0u8;

        // Base score by action
        score += match event.action {
            AuditAction::DropTable | AuditAction::Truncate => 60,
            AuditAction::Delete | AuditAction::DropUser => 50,
            AuditAction::GrantPermission | AuditAction::CreateUser => 40,
            AuditAction::Update | AuditAction::AlterTable => 30,
            AuditAction::Insert | AuditAction::CreateTable => 20,
            AuditAction::Select => 10,
            _ => 5,
        };

        // Increase for failed operations
        if !event.success {
            score += 20;
        }

        // Increase for security events
        if event.event_type == AuditEventType::SecurityEvent {
            score += 30;
        }

        // Check for suspicious patterns
        if let Some(ref query) = event.query {
            if Self::is_suspicious_query(query) {
                score += 40;
            }
        }

        // Check for large data operations
        if let Some(rows) = event.affected_rows {
            if rows > 10000 {
                score += 20;
            }
        }

        // Cap at 100
        score = score.min(100);

        let level = match score {
            0..=20 => RiskLevel::None,
            21..=40 => RiskLevel::Low,
            41..=59 => RiskLevel::Medium,
            60..=80 => RiskLevel::High,
            _ => RiskLevel::Critical,
        };

        RiskScore { level, score }
    }

    fn is_suspicious_query(query: &str) -> bool {
        let query_lower = query.to_lowercase();
        let suspicious_patterns = [
            " or '1'='1",
            "'; drop table",
            "'; delete from",
            "union select",
            "information_schema",
            "sys.tables",
            "xp_cmdshell",
            "exec sp_",
        ];

        suspicious_patterns
            .iter()
            .any(|pattern| query_lower.contains(pattern))
    }

    fn write_event(&self, event: &AuditEvent) -> Result<()> {
        let mut logger = self.logger.lock();

        // Serialize event
        let json = serde_json::to_string(event)?;
        let data = format!("{}\n", json);

        // Write to file
        logger.write(data.as_bytes())?;

        // Update stats
        self.stats.write().bytes_written += data.len() as u64;

        Ok(())
    }

    /// Query audit logs
    pub fn query_logs(&self, criteria: &QueryCriteria) -> Vec<AuditEvent> {
        self.storage.read().query(criteria)
    }

    /// Register a custom audit processor
    pub fn register_processor(&self, processor: Box<dyn AuditProcessor>) {
        self.processors.write().push(processor);
    }

    /// Add a custom filter
    pub fn add_filter(&self, filter: Box<dyn AuditFilter>) {
        self.filters.write().push(filter);
    }

    /// Clean up old audit logs
    pub async fn cleanup_old_logs(&self) -> Result<()> {
        let retention_period = Duration::from_secs(self.config.retention_days as u64 * 24 * 3600);

        let cutoff = SystemTime::now() - retention_period;

        // Find and remove old log files
        let base_dir = self
            .config
            .log_file_path
            .parent()
            .ok_or_else(|| DriftError::Internal("Invalid log path".to_string()))?;

        for entry in std::fs::read_dir(base_dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_file() {
                if let Ok(modified) = metadata.modified() {
                    if modified < cutoff {
                        std::fs::remove_file(entry.path())?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get audit statistics
    pub fn stats(&self) -> AuditStats {
        self.stats.read().clone()
    }

    /// Export audit logs
    pub fn export_logs(&self, format: ExportFormat, output: &Path) -> Result<()> {
        let events = self.storage.read().all_events();

        match format {
            ExportFormat::Json => {
                let file = File::create(output)?;
                serde_json::to_writer_pretty(file, &events)?;
            }
            ExportFormat::Csv => {
                let mut wtr = csv::Writer::from_path(output)
                    .map_err(|e| DriftError::Internal(e.to_string()))?;

                for event in events {
                    wtr.serialize(&event)
                        .map_err(|e| DriftError::Internal(e.to_string()))?;
                }
                wtr.flush()?;
            }
            ExportFormat::Syslog => {
                // Syslog format export not yet implemented
                return Err(DriftError::InvalidQuery(
                    "Syslog export format is not yet implemented. Use JSON or CSV format instead.".to_string()
                ));
            }
        }

        Ok(())
    }
}

impl AuditLogger {
    fn new(base_path: PathBuf, rotation_size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&base_path)?;

        Ok(Self {
            current_file: Some(BufWriter::new(file)),
            current_file_path: base_path.clone(),
            current_size: std::fs::metadata(&base_path).map(|m| m.len()).unwrap_or(0),
            rotation_size,
            base_path,
            rotation_counter: 0,
        })
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        // Check if rotation is needed
        if self.current_size + data.len() as u64 > self.rotation_size {
            self.rotate()?;
        }

        // Write data
        if let Some(ref mut file) = self.current_file {
            file.write_all(data)?;
            file.flush()?;
            self.current_size += data.len() as u64;
        }

        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        // Close current file
        if let Some(mut file) = self.current_file.take() {
            file.flush()?;
        }

        // Generate new filename with timestamp
        self.rotation_counter += 1;
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let new_name = format!(
            "{}.{}.{}",
            self.base_path.display(),
            timestamp,
            self.rotation_counter
        );

        // Rename current file
        std::fs::rename(&self.current_file_path, &new_name)?;

        // Create new file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.base_path)?;

        self.current_file = Some(BufWriter::new(file));
        self.current_size = 0;

        Ok(())
    }
}

impl AuditStorage {
    fn new(max_buffer_size: usize) -> Self {
        Self {
            memory_buffer: VecDeque::with_capacity(max_buffer_size),
            max_buffer_size,
            indexed_events: HashMap::new(),
            user_index: HashMap::new(),
            table_index: HashMap::new(),
            time_index: Vec::new(),
        }
    }

    fn add_event(&mut self, event: AuditEvent) {
        // Add to buffer
        if self.memory_buffer.len() >= self.max_buffer_size {
            if let Some(old_event) = self.memory_buffer.pop_front() {
                // Remove from indexes
                self.indexed_events.remove(&old_event.id);
                if let Some(user) = &old_event.user {
                    if let Some(events) = self.user_index.get_mut(&user.username) {
                        events.retain(|id| *id != old_event.id);
                    }
                }
                if let Some(table) = &old_event.table {
                    if let Some(events) = self.table_index.get_mut(table) {
                        events.retain(|id| *id != old_event.id);
                    }
                }
            }
        }

        // Add to indexes
        self.indexed_events.insert(event.id, event.clone());

        if let Some(user) = &event.user {
            self.user_index
                .entry(user.username.clone())
                .or_default()
                .push(event.id);
        }

        if let Some(table) = &event.table {
            self.table_index
                .entry(table.clone())
                .or_default()
                .push(event.id);
        }

        self.time_index.push((event.timestamp, event.id));
        self.memory_buffer.push_back(event);
    }

    fn query(&self, criteria: &QueryCriteria) -> Vec<AuditEvent> {
        let mut results = Vec::new();

        for event in &self.memory_buffer {
            if Self::matches_criteria(event, criteria) {
                results.push(event.clone());
            }
        }

        // Apply limit
        if let Some(limit) = criteria.limit {
            results.truncate(limit);
        }

        results
    }

    fn matches_criteria(event: &AuditEvent, criteria: &QueryCriteria) -> bool {
        // Check time range
        if let Some(ref start) = criteria.start_time {
            if event.timestamp < *start {
                return false;
            }
        }
        if let Some(ref end) = criteria.end_time {
            if event.timestamp > *end {
                return false;
            }
        }

        // Check user
        if let Some(ref user) = criteria.user {
            if let Some(ref event_user) = event.user {
                if event_user.username != *user {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check table
        if let Some(ref table) = criteria.table {
            if event.table.as_ref() != Some(table) {
                return false;
            }
        }

        // Check event type
        if let Some(ref event_type) = criteria.event_type {
            if event.event_type != *event_type {
                return false;
            }
        }

        // Check action
        if let Some(ref action) = criteria.action {
            if event.action != *action {
                return false;
            }
        }

        true
    }

    fn all_events(&self) -> Vec<AuditEvent> {
        self.memory_buffer.iter().cloned().collect()
    }
}

/// Criteria for querying audit logs
#[derive(Debug, Clone)]
pub struct QueryCriteria {
    pub start_time: Option<SystemTime>,
    pub end_time: Option<SystemTime>,
    pub user: Option<String>,
    pub table: Option<String>,
    pub event_type: Option<AuditEventType>,
    pub action: Option<AuditAction>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Json,
    Csv,
    Syslog,
}

impl Clone for AuditStats {
    fn clone(&self) -> Self {
        Self {
            events_logged: self.events_logged,
            events_filtered: self.events_filtered,
            events_failed: self.events_failed,
            bytes_written: self.bytes_written,
            files_rotated: self.files_rotated,
            avg_event_size: self.avg_event_size,
            high_risk_events: self.high_risk_events,
            security_violations: self.security_violations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_score_calculation() {
        let system = AuditSystem::new(AuditConfig::default()).unwrap();

        let event = AuditEvent {
            id: Uuid::new_v4(),
            timestamp: SystemTime::now(),
            event_type: AuditEventType::DataModification,
            user: None,
            session_id: None,
            client_address: None,
            database: Some("test".to_string()),
            table: Some("users".to_string()),
            action: AuditAction::DropTable,
            query: None,
            parameters: None,
            affected_rows: None,
            execution_time_ms: None,
            success: true,
            error_message: None,
            metadata: HashMap::new(),
            risk_score: RiskScore {
                level: RiskLevel::None,
                score: 0,
            },
        };

        let risk = system.calculate_risk_score(&event);
        assert!(risk.score >= 60); // DropTable should have high risk
        assert_eq!(risk.level, RiskLevel::High);
    }

    #[test]
    fn test_suspicious_query_detection() {
        assert!(AuditSystem::is_suspicious_query(
            "SELECT * FROM users WHERE id = 1 OR '1'='1'"
        ));
        assert!(AuditSystem::is_suspicious_query("'; DROP TABLE users; --"));
        assert!(!AuditSystem::is_suspicious_query(
            "SELECT * FROM users WHERE id = 1"
        ));
    }
}
