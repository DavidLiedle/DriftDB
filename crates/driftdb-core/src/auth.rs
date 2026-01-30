use crate::errors::{DriftError, Result};
use argon2::password_hash::{rand_core::OsRng, SaltString};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    pub email: Option<String>,
    pub password_hash: String,
    pub roles: HashSet<String>,
    pub created_at: SystemTime,
    pub last_login: Option<SystemTime>,
    pub is_active: bool,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
    pub description: Option<String>,
    pub is_system: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    // Database-level permissions
    CreateDatabase,
    DropDatabase,
    AlterDatabase,

    // Table-level permissions
    CreateTable {
        database: Option<String>,
    },
    DropTable {
        database: Option<String>,
        table: Option<String>,
    },
    AlterTable {
        database: Option<String>,
        table: Option<String>,
    },

    // Data manipulation permissions
    Select {
        database: Option<String>,
        table: Option<String>,
        columns: Option<Vec<String>>,
    },
    Insert {
        database: Option<String>,
        table: Option<String>,
    },
    Update {
        database: Option<String>,
        table: Option<String>,
        columns: Option<Vec<String>>,
    },
    Delete {
        database: Option<String>,
        table: Option<String>,
    },

    // Index permissions
    CreateIndex {
        database: Option<String>,
        table: Option<String>,
    },
    DropIndex {
        database: Option<String>,
        table: Option<String>,
    },

    // View permissions
    CreateView {
        database: Option<String>,
    },
    DropView {
        database: Option<String>,
    },

    // Procedure permissions
    CreateProcedure {
        database: Option<String>,
    },
    DropProcedure {
        database: Option<String>,
    },
    ExecuteProcedure {
        database: Option<String>,
        procedure: Option<String>,
    },

    // User management permissions
    CreateUser,
    DropUser,
    AlterUser,
    GrantRole,
    RevokeRole,

    // System permissions
    ViewSystemTables,
    ManageBackup,
    ManageReplication,
    ViewMetrics,
    ManageCache,

    // Special permissions
    SuperUser,
    ReadOnly,
    WriteOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: Uuid,
    pub user_id: Uuid,
    pub token: String,
    pub created_at: SystemTime,
    pub expires_at: SystemTime,
    pub last_activity: SystemTime,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub session_timeout: Duration,
    pub max_sessions_per_user: usize,
    pub password_min_length: usize,
    pub password_require_uppercase: bool,
    pub password_require_lowercase: bool,
    pub password_require_digit: bool,
    pub password_require_special: bool,
    pub max_failed_attempts: usize,
    pub lockout_duration: Duration,
    pub token_length: usize,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_secs(3600 * 24), // 24 hours
            max_sessions_per_user: 10,
            password_min_length: 8,
            password_require_uppercase: true,
            password_require_lowercase: true,
            password_require_digit: true,
            password_require_special: false,
            max_failed_attempts: 5,
            lockout_duration: Duration::from_secs(900), // 15 minutes
            token_length: 32,
        }
    }
}

pub struct AuthManager {
    users: Arc<RwLock<HashMap<Uuid, User>>>,
    username_index: Arc<RwLock<HashMap<String, Uuid>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    user_sessions: Arc<RwLock<HashMap<Uuid, HashSet<String>>>>,
    failed_attempts: Arc<RwLock<HashMap<String, (usize, SystemTime)>>>,
    config: AuthConfig,
    argon2: Argon2<'static>,
}

impl AuthManager {
    pub fn new(config: AuthConfig) -> Self {
        let mut manager = Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            username_index: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            user_sessions: Arc::new(RwLock::new(HashMap::new())),
            failed_attempts: Arc::new(RwLock::new(HashMap::new())),
            config,
            argon2: Argon2::default(),
        };

        manager.initialize_default_roles();
        manager.create_default_admin().ok();
        manager
    }

    fn initialize_default_roles(&mut self) {
        let mut roles = self.roles.write().unwrap();

        // SuperAdmin role
        roles.insert(
            "superadmin".to_string(),
            Role {
                name: "superadmin".to_string(),
                permissions: vec![Permission::SuperUser].into_iter().collect(),
                description: Some("Full system access".to_string()),
                is_system: true,
            },
        );

        // Admin role
        let admin_permissions = vec![
            Permission::CreateDatabase,
            Permission::DropDatabase,
            Permission::AlterDatabase,
            Permission::CreateTable { database: None },
            Permission::DropTable {
                database: None,
                table: None,
            },
            Permission::AlterTable {
                database: None,
                table: None,
            },
            Permission::CreateUser,
            Permission::DropUser,
            Permission::AlterUser,
            Permission::GrantRole,
            Permission::RevokeRole,
            Permission::ManageBackup,
            Permission::ManageReplication,
        ];
        roles.insert(
            "admin".to_string(),
            Role {
                name: "admin".to_string(),
                permissions: admin_permissions.into_iter().collect(),
                description: Some("Database administration".to_string()),
                is_system: true,
            },
        );

        // Developer role
        let dev_permissions = vec![
            Permission::CreateTable { database: None },
            Permission::AlterTable {
                database: None,
                table: None,
            },
            Permission::Select {
                database: None,
                table: None,
                columns: None,
            },
            Permission::Insert {
                database: None,
                table: None,
            },
            Permission::Update {
                database: None,
                table: None,
                columns: None,
            },
            Permission::Delete {
                database: None,
                table: None,
            },
            Permission::CreateIndex {
                database: None,
                table: None,
            },
            Permission::CreateView { database: None },
            Permission::CreateProcedure { database: None },
            Permission::ExecuteProcedure {
                database: None,
                procedure: None,
            },
        ];
        roles.insert(
            "developer".to_string(),
            Role {
                name: "developer".to_string(),
                permissions: dev_permissions.into_iter().collect(),
                description: Some("Development access".to_string()),
                is_system: true,
            },
        );

        // Read-only role
        roles.insert(
            "readonly".to_string(),
            Role {
                name: "readonly".to_string(),
                permissions: vec![
                    Permission::Select {
                        database: None,
                        table: None,
                        columns: None,
                    },
                    Permission::ViewSystemTables,
                ]
                .into_iter()
                .collect(),
                description: Some("Read-only access".to_string()),
                is_system: true,
            },
        );

        // Write-only role
        roles.insert(
            "writeonly".to_string(),
            Role {
                name: "writeonly".to_string(),
                permissions: vec![
                    Permission::Insert {
                        database: None,
                        table: None,
                    },
                    Permission::Update {
                        database: None,
                        table: None,
                        columns: None,
                    },
                    Permission::Delete {
                        database: None,
                        table: None,
                    },
                ]
                .into_iter()
                .collect(),
                description: Some("Write-only access".to_string()),
                is_system: true,
            },
        );
    }

    fn create_default_admin(&mut self) -> Result<()> {
        // Generate a secure random password for the default admin
        use rand::distributions::Alphanumeric;
        use rand::{thread_rng, Rng};

        let random_password: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        eprintln!("===========================================");
        eprintln!("IMPORTANT: DEFAULT ADMIN CREDENTIALS");
        eprintln!("Username: admin");
        eprintln!("Password: {}", random_password);
        eprintln!("PLEASE CHANGE THIS PASSWORD IMMEDIATELY!");
        eprintln!("===========================================");

        let admin_user = User {
            id: Uuid::new_v4(),
            username: "admin".to_string(),
            email: Some("admin@driftdb.local".to_string()),
            password_hash: self.hash_password(&random_password)?,
            roles: vec!["superadmin".to_string()].into_iter().collect(),
            created_at: SystemTime::now(),
            last_login: None,
            is_active: true,
            metadata: HashMap::new(),
        };

        let mut users = self.users.write().unwrap();
        let mut username_index = self.username_index.write().unwrap();

        users.insert(admin_user.id, admin_user.clone());
        username_index.insert(admin_user.username.clone(), admin_user.id);

        Ok(())
    }

    pub fn hash_password(&self, password: &str) -> Result<String> {
        self.validate_password_strength(password)?;

        let salt = SaltString::generate(&mut OsRng);
        let password_hash = self
            .argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| DriftError::Internal(format!("Failed to hash password: {}", e)))?;

        Ok(password_hash.to_string())
    }

    fn validate_password_strength(&self, password: &str) -> Result<()> {
        if password.len() < self.config.password_min_length {
            return Err(DriftError::Validation(format!(
                "Password must be at least {} characters long",
                self.config.password_min_length
            )));
        }

        if self.config.password_require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            return Err(DriftError::Validation(
                "Password must contain at least one uppercase letter".to_string(),
            ));
        }

        if self.config.password_require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            return Err(DriftError::Validation(
                "Password must contain at least one lowercase letter".to_string(),
            ));
        }

        if self.config.password_require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            return Err(DriftError::Validation(
                "Password must contain at least one digit".to_string(),
            ));
        }

        if self.config.password_require_special && !password.chars().any(|c| !c.is_alphanumeric()) {
            return Err(DriftError::Validation(
                "Password must contain at least one special character".to_string(),
            ));
        }

        Ok(())
    }

    pub fn verify_password(&self, password: &str, hash: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| DriftError::Internal(format!("Invalid password hash: {}", e)))?;

        Ok(self
            .argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }

    pub fn create_user(
        &mut self,
        username: String,
        password: String,
        email: Option<String>,
        roles: HashSet<String>,
    ) -> Result<Uuid> {
        // Check if username already exists
        {
            let username_index = self.username_index.read().unwrap();
            if username_index.contains_key(&username) {
                return Err(DriftError::Conflict(format!(
                    "User '{}' already exists",
                    username
                )));
            }
        }

        // Validate roles exist
        {
            let role_map = self.roles.read().unwrap();
            for role in &roles {
                if !role_map.contains_key(role) {
                    return Err(DriftError::NotFound(format!(
                        "Role '{}' does not exist",
                        role
                    )));
                }
            }
        }

        let user = User {
            id: Uuid::new_v4(),
            username: username.clone(),
            email,
            password_hash: self.hash_password(&password)?,
            roles,
            created_at: SystemTime::now(),
            last_login: None,
            is_active: true,
            metadata: HashMap::new(),
        };

        let mut users = self.users.write().unwrap();
        let mut username_index = self.username_index.write().unwrap();

        users.insert(user.id, user.clone());
        username_index.insert(username, user.id);

        Ok(user.id)
    }

    pub fn authenticate(&mut self, username: &str, password: &str) -> Result<String> {
        // Check for account lockout
        {
            let failed_attempts = self.failed_attempts.read().unwrap();
            if let Some((attempts, last_attempt)) = failed_attempts.get(username) {
                if *attempts >= self.config.max_failed_attempts {
                    let elapsed = SystemTime::now()
                        .duration_since(*last_attempt)
                        .unwrap_or(Duration::ZERO);
                    if elapsed < self.config.lockout_duration {
                        return Err(DriftError::Unauthorized(format!(
                            "Account locked. Try again in {} seconds",
                            (self.config.lockout_duration - elapsed).as_secs()
                        )));
                    }
                }
            }
        }

        // Find user
        let user_id = {
            let username_index = self.username_index.read().unwrap();
            username_index
                .get(username)
                .copied()
                .ok_or_else(|| DriftError::Unauthorized("Invalid credentials".to_string()))?
        };

        let user = {
            let users = self.users.read().unwrap();
            users
                .get(&user_id)
                .cloned()
                .ok_or_else(|| DriftError::Unauthorized("Invalid credentials".to_string()))?
        };

        if !user.is_active {
            return Err(DriftError::Unauthorized("Account is disabled".to_string()));
        }

        // Verify password
        if !self.verify_password(password, &user.password_hash)? {
            // Record failed attempt
            let mut failed_attempts = self.failed_attempts.write().unwrap();
            let entry = failed_attempts
                .entry(username.to_string())
                .or_insert((0, SystemTime::now()));
            entry.0 += 1;
            entry.1 = SystemTime::now();

            return Err(DriftError::Unauthorized("Invalid credentials".to_string()));
        }

        // Clear failed attempts
        {
            let mut failed_attempts = self.failed_attempts.write().unwrap();
            failed_attempts.remove(username);
        }

        // Update last login
        {
            let mut users = self.users.write().unwrap();
            if let Some(user) = users.get_mut(&user_id) {
                user.last_login = Some(SystemTime::now());
            }
        }

        // Create session
        let session = self.create_session(user_id, None, None)?;

        Ok(session.token)
    }

    pub fn create_session(
        &mut self,
        user_id: Uuid,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> Result<Session> {
        // Check max sessions
        {
            let user_sessions = self.user_sessions.read().unwrap();
            if let Some(sessions) = user_sessions.get(&user_id) {
                if sessions.len() >= self.config.max_sessions_per_user {
                    return Err(DriftError::Validation(format!(
                        "Maximum sessions ({}) reached for user",
                        self.config.max_sessions_per_user
                    )));
                }
            }
        }

        let token = self.generate_token();
        let now = SystemTime::now();

        let session = Session {
            id: Uuid::new_v4(),
            user_id,
            token: token.clone(),
            created_at: now,
            expires_at: now + self.config.session_timeout,
            last_activity: now,
            ip_address,
            user_agent,
        };

        let mut sessions = self.sessions.write().unwrap();
        let mut user_sessions = self.user_sessions.write().unwrap();

        sessions.insert(token.clone(), session.clone());
        user_sessions.entry(user_id).or_default().insert(token);

        Ok(session)
    }

    fn generate_token(&self) -> String {
        use rand::distributions::Alphanumeric;
        use rand::{thread_rng, Rng};

        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(self.config.token_length)
            .map(char::from)
            .collect()
    }

    pub fn validate_session(&mut self, token: &str) -> Result<Session> {
        let mut sessions = self.sessions.write().unwrap();

        let session = sessions
            .get_mut(token)
            .ok_or_else(|| DriftError::Unauthorized("Invalid or expired session".to_string()))?;

        let now = SystemTime::now();
        if now > session.expires_at {
            // Remove expired session
            let user_id = session.user_id;
            sessions.remove(token);

            let mut user_sessions = self.user_sessions.write().unwrap();
            if let Some(user_session_set) = user_sessions.get_mut(&user_id) {
                user_session_set.remove(token);
            }

            return Err(DriftError::Unauthorized("Session expired".to_string()));
        }

        // Update last activity
        session.last_activity = now;

        Ok(session.clone())
    }

    pub fn logout(&mut self, token: &str) -> Result<()> {
        let mut sessions = self.sessions.write().unwrap();

        if let Some(session) = sessions.remove(token) {
            let mut user_sessions = self.user_sessions.write().unwrap();
            if let Some(user_session_set) = user_sessions.get_mut(&session.user_id) {
                user_session_set.remove(token);
            }
        }

        Ok(())
    }

    pub fn check_permission(&self, user_id: Uuid, permission: &Permission) -> Result<bool> {
        let users = self.users.read().unwrap();
        let user = users
            .get(&user_id)
            .ok_or_else(|| DriftError::NotFound(format!("User {} not found", user_id)))?;

        if !user.is_active {
            return Ok(false);
        }

        let roles = self.roles.read().unwrap();

        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                // SuperUser has all permissions
                if role.permissions.contains(&Permission::SuperUser) {
                    return Ok(true);
                }

                // Check specific permission
                if role.permissions.contains(permission) {
                    return Ok(true);
                }

                // Check wildcard permissions
                if self.check_wildcard_permission(&role.permissions, permission) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn check_wildcard_permission(
        &self,
        role_permissions: &HashSet<Permission>,
        requested: &Permission,
    ) -> bool {
        for perm in role_permissions {
            match (perm, requested) {
                // Table-level wildcards
                (
                    Permission::Select {
                        database: None,
                        table: None,
                        columns: None,
                    },
                    Permission::Select { .. },
                ) => return true,

                (
                    Permission::Insert {
                        database: None,
                        table: None,
                    },
                    Permission::Insert { .. },
                ) => return true,

                (
                    Permission::Update {
                        database: None,
                        table: None,
                        columns: None,
                    },
                    Permission::Update { .. },
                ) => return true,

                (
                    Permission::Delete {
                        database: None,
                        table: None,
                    },
                    Permission::Delete { .. },
                ) => return true,

                // Database-specific wildcards
                (
                    Permission::Select {
                        database: Some(d1),
                        table: None,
                        columns: None,
                    },
                    Permission::Select {
                        database: Some(d2), ..
                    },
                ) if d1 == d2 => return true,

                (
                    Permission::Insert {
                        database: Some(d1),
                        table: None,
                    },
                    Permission::Insert {
                        database: Some(d2), ..
                    },
                ) if d1 == d2 => return true,

                (
                    Permission::Update {
                        database: Some(d1),
                        table: None,
                        columns: None,
                    },
                    Permission::Update {
                        database: Some(d2), ..
                    },
                ) if d1 == d2 => return true,

                (
                    Permission::Delete {
                        database: Some(d1),
                        table: None,
                    },
                    Permission::Delete {
                        database: Some(d2), ..
                    },
                ) if d1 == d2 => return true,

                _ => {}
            }
        }

        false
    }

    pub fn grant_role(&mut self, user_id: Uuid, role_name: &str) -> Result<()> {
        // Check role exists
        {
            let roles = self.roles.read().unwrap();
            if !roles.contains_key(role_name) {
                return Err(DriftError::NotFound(format!(
                    "Role '{}' does not exist",
                    role_name
                )));
            }
        }

        let mut users = self.users.write().unwrap();
        let user = users
            .get_mut(&user_id)
            .ok_or_else(|| DriftError::NotFound(format!("User {} not found", user_id)))?;

        user.roles.insert(role_name.to_string());

        Ok(())
    }

    pub fn revoke_role(&mut self, user_id: Uuid, role_name: &str) -> Result<()> {
        let mut users = self.users.write().unwrap();
        let user = users
            .get_mut(&user_id)
            .ok_or_else(|| DriftError::NotFound(format!("User {} not found", user_id)))?;

        if !user.roles.remove(role_name) {
            return Err(DriftError::NotFound(format!(
                "User does not have role '{}'",
                role_name
            )));
        }

        Ok(())
    }

    pub fn create_custom_role(
        &mut self,
        name: String,
        permissions: HashSet<Permission>,
        description: Option<String>,
    ) -> Result<()> {
        let mut roles = self.roles.write().unwrap();

        if roles.contains_key(&name) {
            return Err(DriftError::Conflict(format!(
                "Role '{}' already exists",
                name
            )));
        }

        roles.insert(
            name.clone(),
            Role {
                name,
                permissions,
                description,
                is_system: false,
            },
        );

        Ok(())
    }

    pub fn delete_role(&mut self, name: &str) -> Result<()> {
        let mut roles = self.roles.write().unwrap();

        let role = roles
            .get(name)
            .ok_or_else(|| DriftError::NotFound(format!("Role '{}' not found", name)))?;

        if role.is_system {
            return Err(DriftError::Validation(
                "Cannot delete system role".to_string(),
            ));
        }

        // Remove role from all users
        {
            let mut users = self.users.write().unwrap();
            for user in users.values_mut() {
                user.roles.remove(name);
            }
        }

        roles.remove(name);

        Ok(())
    }

    pub fn change_password(
        &mut self,
        user_id: Uuid,
        old_password: &str,
        new_password: &str,
    ) -> Result<()> {
        let mut users = self.users.write().unwrap();
        let user = users
            .get_mut(&user_id)
            .ok_or_else(|| DriftError::NotFound(format!("User {} not found", user_id)))?;

        // Verify old password
        if !self.verify_password(old_password, &user.password_hash)? {
            return Err(DriftError::Unauthorized(
                "Invalid current password".to_string(),
            ));
        }

        // Set new password
        user.password_hash = self.hash_password(new_password)?;

        // Invalidate all sessions for this user
        {
            let mut sessions = self.sessions.write().unwrap();
            let mut user_sessions = self.user_sessions.write().unwrap();

            if let Some(session_tokens) = user_sessions.remove(&user_id) {
                for token in session_tokens {
                    sessions.remove(&token);
                }
            }
        }

        Ok(())
    }

    pub fn reset_password(&mut self, user_id: Uuid, new_password: &str) -> Result<()> {
        let mut users = self.users.write().unwrap();
        let user = users
            .get_mut(&user_id)
            .ok_or_else(|| DriftError::NotFound(format!("User {} not found", user_id)))?;

        user.password_hash = self.hash_password(new_password)?;

        // Invalidate all sessions
        {
            let mut sessions = self.sessions.write().unwrap();
            let mut user_sessions = self.user_sessions.write().unwrap();

            if let Some(session_tokens) = user_sessions.remove(&user_id) {
                for token in session_tokens {
                    sessions.remove(&token);
                }
            }
        }

        Ok(())
    }

    pub fn cleanup_expired_sessions(&mut self) {
        let now = SystemTime::now();
        let mut sessions = self.sessions.write().unwrap();
        let mut user_sessions = self.user_sessions.write().unwrap();

        let expired_tokens: Vec<String> = sessions
            .iter()
            .filter(|(_, session)| now > session.expires_at)
            .map(|(token, _)| token.clone())
            .collect();

        for token in expired_tokens {
            if let Some(session) = sessions.remove(&token) {
                if let Some(user_session_set) = user_sessions.get_mut(&session.user_id) {
                    user_session_set.remove(&token);
                }
            }
        }
    }

    pub fn get_user_by_token(&self, token: &str) -> Result<User> {
        let sessions = self.sessions.read().unwrap();
        let session = sessions
            .get(token)
            .ok_or_else(|| DriftError::Unauthorized("Invalid session".to_string()))?;

        let users = self.users.read().unwrap();
        users
            .get(&session.user_id)
            .cloned()
            .ok_or_else(|| DriftError::NotFound("User not found".to_string()))
    }
}

#[derive(Debug)]
pub struct AuthContext {
    pub user: User,
    pub session: Session,
    pub effective_permissions: HashSet<Permission>,
}

impl AuthContext {
    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.effective_permissions.contains(&Permission::SuperUser)
            || self.effective_permissions.contains(permission)
    }

    pub fn require_permission(&self, permission: &Permission) -> Result<()> {
        if !self.has_permission(permission) {
            return Err(DriftError::Unauthorized(format!(
                "Insufficient permissions. Required: {:?}",
                permission
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hashing() {
        let config = AuthConfig::default();
        let auth = AuthManager::new(config);

        let password = "TestPass123!";
        let hash = auth.hash_password(password).unwrap();

        assert!(auth.verify_password(password, &hash).unwrap());
        assert!(!auth.verify_password("WrongPass", &hash).unwrap());
    }

    #[test]
    fn test_user_creation_and_auth() {
        let config = AuthConfig::default();
        let mut auth = AuthManager::new(config);

        let user_id = auth
            .create_user(
                "testuser".to_string(),
                "TestPass123!".to_string(),
                Some("test@example.com".to_string()),
                vec!["readonly".to_string()].into_iter().collect(),
            )
            .unwrap();

        let token = auth.authenticate("testuser", "TestPass123!").unwrap();
        let session = auth.validate_session(&token).unwrap();

        assert_eq!(session.user_id, user_id);
    }

    #[test]
    fn test_permission_checking() {
        let config = AuthConfig::default();
        let mut auth = AuthManager::new(config);

        let user_id = auth
            .create_user(
                "devuser".to_string(),
                "DevPass123!".to_string(),
                None,
                vec!["developer".to_string()].into_iter().collect(),
            )
            .unwrap();

        assert!(auth
            .check_permission(user_id, &Permission::CreateTable { database: None })
            .unwrap());
        assert!(auth
            .check_permission(
                user_id,
                &Permission::Select {
                    database: None,
                    table: None,
                    columns: None
                }
            )
            .unwrap());
        assert!(!auth
            .check_permission(user_id, &Permission::CreateUser)
            .unwrap());
    }

    #[test]
    fn test_account_lockout() {
        let mut config = AuthConfig::default();
        config.max_failed_attempts = 3;
        let mut auth = AuthManager::new(config);

        auth.create_user(
            "locktest".to_string(),
            "CorrectPass123!".to_string(),
            None,
            HashSet::new(),
        )
        .unwrap();

        // Try wrong password multiple times
        for _ in 0..3 {
            assert!(auth.authenticate("locktest", "WrongPass").is_err());
        }

        // Should be locked out now
        let result = auth.authenticate("locktest", "CorrectPass123!");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("locked"));
    }
}
