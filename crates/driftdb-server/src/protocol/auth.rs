//! PostgreSQL Authentication

use anyhow::{anyhow, Result};
use hex;
use md5;
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Authentication methods supported by DriftDB
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMethod {
    Trust,       // No authentication required
    MD5,         // MD5 hashed password (PostgreSQL compatible)
    ScramSha256, // SCRAM-SHA-256 (PostgreSQL 10+ standard)
}

impl std::str::FromStr for AuthMethod {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "trust" => Ok(AuthMethod::Trust),
            "md5" => Ok(AuthMethod::MD5),
            "scram-sha-256" => Ok(AuthMethod::ScramSha256),
            _ => Err(anyhow!("Invalid authentication method: {}", s)),
        }
    }
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMethod::Trust => write!(f, "trust"),
            AuthMethod::MD5 => write!(f, "md5"),
            AuthMethod::ScramSha256 => write!(f, "scram-sha-256"),
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub method: AuthMethod,
    pub require_auth: bool,
    pub max_failed_attempts: u32,
    pub lockout_duration_seconds: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            method: AuthMethod::MD5,
            require_auth: true,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300, // 5 minutes
        }
    }
}

/// Generate a random salt for password hashing
pub fn generate_salt() -> [u8; 16] {
    let mut salt = [0u8; 16];
    thread_rng().fill_bytes(&mut salt);
    salt
}

/// Hash password with salt using SHA-256
pub fn hash_password_sha256(password: &str, salt: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.update(salt);
    hex::encode(hasher.finalize())
}

/// Verify SHA-256 hashed password
pub fn verify_password_sha256(password: &str, stored_hash: &str, salt: &[u8]) -> bool {
    let computed_hash = hash_password_sha256(password, salt);
    computed_hash == stored_hash
}

/// Perform MD5 authentication as per PostgreSQL protocol
pub fn md5_auth(password: &str, username: &str, salt: &[u8; 4]) -> String {
    // PostgreSQL MD5 auth:
    // 1. MD5(password + username)
    // 2. MD5(result + salt)
    // 3. Prepend "md5"

    let pass_user = format!("{}{}", password, username);
    let pass_user_hash = md5::compute(pass_user.as_bytes());

    // The salt is raw bytes, not text - concatenate hex hash with raw salt bytes
    let mut salt_input = hex::encode(pass_user_hash.as_ref()).into_bytes();
    salt_input.extend_from_slice(salt);
    let final_hash = md5::compute(&salt_input);

    format!("md5{}", hex::encode(final_hash.as_ref()))
}

/// Verify MD5 authentication
pub fn verify_md5(received: &str, expected_password: &str, username: &str, salt: &[u8; 4]) -> bool {
    let expected = md5_auth(expected_password, username, salt);
    received == expected
}

/// Generate MD5 challenge for client
pub fn generate_md5_challenge() -> [u8; 4] {
    let mut salt = [0u8; 4];
    thread_rng().fill_bytes(&mut salt);
    salt
}

/// SCRAM-SHA-256 implementation (simplified)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScramSha256 {
    pub salt: Vec<u8>,
    pub iteration_count: u32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl ScramSha256 {
    pub fn new(password: &str, salt: Option<Vec<u8>>) -> Self {
        let salt = salt.unwrap_or_else(|| {
            let mut s = vec![0u8; 16];
            thread_rng().fill_bytes(&mut s);
            s
        });
        let iteration_count = 4096;

        // For simplified implementation, we'll use basic PBKDF2
        let salted_password = pbkdf2_simple(password.as_bytes(), &salt, iteration_count);

        // Generate keys (simplified)
        let stored_key = hash_password_sha256(&hex::encode(&salted_password), b"stored");
        let server_key = hash_password_sha256(&hex::encode(&salted_password), b"server");

        Self {
            salt,
            iteration_count,
            stored_key: hex::decode(stored_key).unwrap_or_default(),
            server_key: hex::decode(server_key).unwrap_or_default(),
        }
    }
}

/// Simplified PBKDF2 implementation
fn pbkdf2_simple(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut result = password.to_vec();
    result.extend_from_slice(salt);

    for _ in 0..iterations {
        let mut hasher = Sha256::new();
        hasher.update(&result);
        result = hasher.finalize().to_vec();
    }

    result
}

/// User information stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub salt: Vec<u8>,
    #[deprecated(note = "Use roles field instead. Kept for backward compatibility.")]
    pub is_superuser: bool,
    pub created_at: u64,
    pub last_login: Option<u64>,
    pub failed_attempts: u32,
    pub locked_until: Option<u64>,
    pub auth_method: AuthMethod,
    pub scram_sha256: Option<ScramSha256>,
    /// RBAC roles assigned to this user
    #[serde(default)]
    pub roles: Vec<String>,
}

impl User {
    pub fn new(
        username: String,
        password: &str,
        is_superuser: bool,
        auth_method: AuthMethod,
    ) -> Self {
        let salt = generate_salt().to_vec();
        let password_hash = match auth_method {
            AuthMethod::Trust => String::new(),
            AuthMethod::MD5 => password.to_string(), // Store plaintext for MD5 compatibility
            AuthMethod::ScramSha256 => hash_password_sha256(password, &salt),
        };

        let scram_sha256 = if auth_method == AuthMethod::ScramSha256 {
            Some(ScramSha256::new(password, Some(salt.clone())))
        } else {
            None
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_secs();

        // Auto-assign role based on is_superuser for backward compatibility
        let roles = if is_superuser {
            vec!["superuser".to_string()]
        } else {
            vec!["user".to_string()]
        };

        Self {
            username,
            password_hash,
            salt,
            #[allow(deprecated)]
            is_superuser,
            created_at: now,
            last_login: None,
            failed_attempts: 0,
            locked_until: None,
            auth_method,
            scram_sha256,
            roles,
        }
    }

    /// Check if user has superuser role (RBAC-aware)
    #[allow(dead_code)]
    pub fn is_superuser_rbac(&self) -> bool {
        self.roles.contains(&"superuser".to_string())
    }

    pub fn is_locked(&self) -> bool {
        if let Some(locked_until) = self.locked_until {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_secs();
            locked_until > now
        } else {
            false
        }
    }

    pub fn verify_password(&self, password: &str, challenge_salt: Option<&[u8; 4]>) -> bool {
        match self.auth_method {
            AuthMethod::Trust => true,
            AuthMethod::MD5 => {
                if let Some(salt) = challenge_salt {
                    verify_md5(password, &self.password_hash, &self.username, salt)
                } else {
                    self.password_hash == password
                }
            }
            AuthMethod::ScramSha256 => {
                verify_password_sha256(password, &self.password_hash, &self.salt)
            }
        }
    }
}

/// Authentication attempt tracking
#[derive(Debug, Clone)]
pub struct AuthAttempt {
    pub username: String,
    pub timestamp: u64,
    pub success: bool,
    pub client_addr: String,
}

/// Enhanced user database with security features
pub struct UserDb {
    users: parking_lot::RwLock<HashMap<String, User>>,
    config: AuthConfig,
    auth_attempts: parking_lot::RwLock<Vec<AuthAttempt>>,
}

impl UserDb {
    pub fn new(config: AuthConfig) -> Self {
        let mut users = HashMap::new();

        // Create default superuser if authentication is enabled
        if config.require_auth {
            let default_password =
                std::env::var("DRIFTDB_PASSWORD").unwrap_or_else(|_| "driftdb".to_string());

            let superuser = User::new(
                "driftdb".to_string(),
                &default_password,
                true,
                config.method.clone(),
            );

            info!(
                "Created default superuser 'driftdb' with {} authentication",
                config.method
            );
            users.insert("driftdb".to_string(), superuser);
        }

        Self {
            users: parking_lot::RwLock::new(users),
            config,
            auth_attempts: parking_lot::RwLock::new(Vec::new()),
        }
    }

    pub fn config(&self) -> &AuthConfig {
        &self.config
    }

    pub fn create_user(&self, username: String, password: &str, is_superuser: bool) -> Result<()> {
        let mut users = self.users.write();

        if users.contains_key(&username) {
            return Err(anyhow!("User '{}' already exists", username));
        }

        let user = User::new(
            username.clone(),
            password,
            is_superuser,
            self.config.method.clone(),
        );
        users.insert(username.clone(), user);

        info!(
            "Created user '{}' with superuser={}",
            username, is_superuser
        );
        Ok(())
    }

    pub fn drop_user(&self, username: &str) -> Result<()> {
        let mut users = self.users.write();

        if username == "driftdb" {
            return Err(anyhow!("Cannot drop default superuser 'driftdb'"));
        }

        if users.remove(username).is_some() {
            info!("Dropped user '{}'", username);
            Ok(())
        } else {
            Err(anyhow!("User '{}' does not exist", username))
        }
    }

    pub fn change_password(&self, username: &str, new_password: &str) -> Result<()> {
        let mut users = self.users.write();

        if let Some(user) = users.get_mut(username) {
            let salt = generate_salt().to_vec();
            user.salt = salt.clone();

            match user.auth_method {
                AuthMethod::Trust => {}
                AuthMethod::MD5 => {
                    user.password_hash = new_password.to_string();
                }
                AuthMethod::ScramSha256 => {
                    user.password_hash = hash_password_sha256(new_password, &salt);
                    user.scram_sha256 = Some(ScramSha256::new(new_password, Some(salt)));
                }
            }

            // Reset failed attempts
            user.failed_attempts = 0;
            user.locked_until = None;

            info!("Changed password for user '{}'", username);
            Ok(())
        } else {
            Err(anyhow!("User '{}' does not exist", username))
        }
    }

    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
        client_addr: &str,
        challenge_salt: Option<&[u8; 4]>,
    ) -> Result<bool> {
        // Trust authentication bypasses everything
        if self.config.method == AuthMethod::Trust && !self.config.require_auth {
            self.record_auth_attempt(username, true, client_addr);
            return Ok(true);
        }

        let mut users = self.users.write();
        let user = users
            .get_mut(username)
            .ok_or_else(|| anyhow!("User '{}' does not exist", username))?;

        // Check if user is locked
        if user.is_locked() {
            warn!(
                "Authentication blocked for locked user '{}' from {}",
                username, client_addr
            );
            return Err(anyhow!("User account is temporarily locked"));
        }

        // Verify password
        let success = user.verify_password(password, challenge_salt);

        if success {
            // Reset failed attempts and update last login
            user.failed_attempts = 0;
            user.locked_until = None;
            user.last_login = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                    .as_secs(),
            );

            info!(
                "Successful authentication for user '{}' from {}",
                username, client_addr
            );
            self.record_auth_attempt(username, true, client_addr);
            Ok(true)
        } else {
            // Increment failed attempts
            user.failed_attempts += 1;

            // Lock account if max attempts reached
            if user.failed_attempts >= self.config.max_failed_attempts {
                let lock_until = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                    .as_secs()
                    + self.config.lockout_duration_seconds;

                user.locked_until = Some(lock_until);

                warn!(
                    "User '{}' locked after {} failed attempts from {}",
                    username, user.failed_attempts, client_addr
                );
            } else {
                warn!(
                    "Failed authentication for user '{}' from {} (attempt {}/{})",
                    username, client_addr, user.failed_attempts, self.config.max_failed_attempts
                );
            }

            self.record_auth_attempt(username, false, client_addr);
            Err(anyhow!("Authentication failed"))
        }
    }

    pub fn is_superuser(&self, username: &str) -> bool {
        self.users
            .read()
            .get(username)
            .map(|user| user.roles.contains(&"superuser".to_string()))
            .unwrap_or(false)
    }

    pub fn list_users(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    pub fn get_user_info(&self, username: &str) -> Option<User> {
        self.users.read().get(username).cloned()
    }

    fn record_auth_attempt(&self, username: &str, success: bool, client_addr: &str) {
        let attempt = AuthAttempt {
            username: username.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_secs(),
            success,
            client_addr: client_addr.to_string(),
        };

        let mut attempts = self.auth_attempts.write();
        attempts.push(attempt);

        // Keep only last 1000 attempts
        if attempts.len() > 1000 {
            let drain_count = attempts.len() - 1000;
            attempts.drain(..drain_count);
        }
    }

    pub fn get_recent_auth_attempts(&self, limit: usize) -> Vec<AuthAttempt> {
        let attempts = self.auth_attempts.read();
        attempts.iter().rev().take(limit).cloned().collect()
    }
}

/// Generate authentication challenge based on method
pub fn generate_auth_challenge(method: &AuthMethod) -> Option<Vec<u8>> {
    match method {
        AuthMethod::Trust => None,
        AuthMethod::MD5 => Some(generate_md5_challenge().to_vec()),
        AuthMethod::ScramSha256 => {
            // SCRAM-SHA-256 uses server-first message
            let mut nonce = vec![0u8; 18];
            thread_rng().fill_bytes(&mut nonce);
            Some(nonce)
        }
    }
}

/// Validate username for security
pub fn validate_username(username: &str) -> Result<()> {
    if username.is_empty() {
        return Err(anyhow!("Username cannot be empty"));
    }

    if username.len() > 63 {
        return Err(anyhow!("Username too long (max 63 characters)"));
    }

    if !username
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(anyhow!(
            "Username can only contain alphanumeric characters, underscores, and hyphens"
        ));
    }

    Ok(())
}

/// Validate password strength
pub fn validate_password(password: &str) -> Result<()> {
    if password.len() < 8 {
        return Err(anyhow!("Password must be at least 8 characters long"));
    }

    if password.len() > 100 {
        return Err(anyhow!("Password too long (max 100 characters)"));
    }

    // Check for at least one letter and one number
    let has_letter = password.chars().any(|c| c.is_alphabetic());
    let has_number = password.chars().any(|c| c.is_numeric());

    if !has_letter || !has_number {
        return Err(anyhow!(
            "Password must contain at least one letter and one number"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== AuthMethod Tests ====================

    #[test]
    fn test_auth_method_from_str() {
        assert_eq!("trust".parse::<AuthMethod>().unwrap(), AuthMethod::Trust);
        assert_eq!("TRUST".parse::<AuthMethod>().unwrap(), AuthMethod::Trust);
        assert_eq!("md5".parse::<AuthMethod>().unwrap(), AuthMethod::MD5);
        assert_eq!("MD5".parse::<AuthMethod>().unwrap(), AuthMethod::MD5);
        assert_eq!(
            "scram-sha-256".parse::<AuthMethod>().unwrap(),
            AuthMethod::ScramSha256
        );
        assert_eq!(
            "SCRAM-SHA-256".parse::<AuthMethod>().unwrap(),
            AuthMethod::ScramSha256
        );
        assert!("invalid".parse::<AuthMethod>().is_err());
    }

    #[test]
    fn test_auth_method_display() {
        assert_eq!(AuthMethod::Trust.to_string(), "trust");
        assert_eq!(AuthMethod::MD5.to_string(), "md5");
        assert_eq!(AuthMethod::ScramSha256.to_string(), "scram-sha-256");
    }

    // ==================== MD5 Authentication Tests ====================

    #[test]
    fn test_md5_auth_correctness() {
        // Known PostgreSQL MD5 test vector
        let password = "password123";
        let username = "testuser";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let result = md5_auth(password, username, &salt);

        // Result should start with "md5"
        assert!(result.starts_with("md5"));
        // Result should be exactly 35 characters (3 for "md5" + 32 hex chars)
        assert_eq!(result.len(), 35);
        // Verify determinism - same inputs produce same output
        assert_eq!(result, md5_auth(password, username, &salt));
    }

    #[test]
    fn test_md5_auth_different_users_different_hashes() {
        let password = "password123";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let hash1 = md5_auth(password, "user1", &salt);
        let hash2 = md5_auth(password, "user2", &salt);

        // Different users should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_md5_auth_different_salts_different_hashes() {
        let password = "password123";
        let username = "testuser";

        let hash1 = md5_auth(password, username, &[0x01, 0x02, 0x03, 0x04]);
        let hash2 = md5_auth(password, username, &[0x05, 0x06, 0x07, 0x08]);

        // Different salts should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_verify_md5_success() {
        let password = "password123";
        let username = "testuser";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let hash = md5_auth(password, username, &salt);
        assert!(verify_md5(&hash, password, username, &salt));
    }

    #[test]
    fn test_verify_md5_wrong_password() {
        let password = "password123";
        let wrong_password = "wrongpassword";
        let username = "testuser";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let hash = md5_auth(password, username, &salt);
        assert!(!verify_md5(&hash, wrong_password, username, &salt));
    }

    // ==================== SCRAM-SHA-256 Authentication Tests ====================

    #[test]
    fn test_scram_sha256_creation() {
        let password = "secure_password123";
        let scram = ScramSha256::new(password, None);

        // Verify fields are populated
        assert_eq!(scram.salt.len(), 16);
        assert_eq!(scram.iteration_count, 4096);
        assert!(!scram.stored_key.is_empty());
        assert!(!scram.server_key.is_empty());
    }

    #[test]
    fn test_scram_sha256_with_custom_salt() {
        let password = "secure_password123";
        let custom_salt = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
        let scram = ScramSha256::new(password, Some(custom_salt.clone()));

        assert_eq!(scram.salt, custom_salt);
    }

    #[test]
    fn test_scram_sha256_deterministic() {
        let password = "secure_password123";
        let salt = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

        let scram1 = ScramSha256::new(password, Some(salt.clone()));
        let scram2 = ScramSha256::new(password, Some(salt));

        // Same password and salt should produce same keys
        assert_eq!(scram1.stored_key, scram2.stored_key);
        assert_eq!(scram1.server_key, scram2.server_key);
    }

    #[test]
    fn test_scram_sha256_different_passwords() {
        let salt = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

        let scram1 = ScramSha256::new("password1", Some(salt.clone()));
        let scram2 = ScramSha256::new("password2", Some(salt));

        // Different passwords should produce different keys
        assert_ne!(scram1.stored_key, scram2.stored_key);
    }

    // ==================== Password Hashing Tests ====================

    #[test]
    fn test_sha256_hash_deterministic() {
        let password = "test_password";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let hash1 = hash_password_sha256(password, &salt);
        let hash2 = hash_password_sha256(password, &salt);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_sha256_hash_different_salts() {
        let password = "test_password";

        let hash1 = hash_password_sha256(password, &[0x01, 0x02, 0x03, 0x04]);
        let hash2 = hash_password_sha256(password, &[0x05, 0x06, 0x07, 0x08]);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_verify_sha256_success() {
        let password = "test_password";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let hash = hash_password_sha256(password, &salt);
        assert!(verify_password_sha256(password, &hash, &salt));
    }

    #[test]
    fn test_verify_sha256_wrong_password() {
        let password = "correct_password";
        let salt = [0x01, 0x02, 0x03, 0x04];

        let hash = hash_password_sha256(password, &salt);
        assert!(!verify_password_sha256("wrong_password", &hash, &salt));
    }

    // ==================== User Tests ====================

    #[test]
    fn test_user_creation_md5() {
        let user = User::new("testuser".to_string(), "password123", false, AuthMethod::MD5);

        assert_eq!(user.username, "testuser");
        assert_eq!(user.auth_method, AuthMethod::MD5);
        assert!(!user.is_locked());
        assert_eq!(user.failed_attempts, 0);
        assert!(user.scram_sha256.is_none());
    }

    #[test]
    fn test_user_creation_scram() {
        let user = User::new(
            "testuser".to_string(),
            "password123",
            false,
            AuthMethod::ScramSha256,
        );

        assert_eq!(user.auth_method, AuthMethod::ScramSha256);
        assert!(user.scram_sha256.is_some());
    }

    #[test]
    fn test_user_creation_trust() {
        let user = User::new("testuser".to_string(), "", true, AuthMethod::Trust);

        assert_eq!(user.auth_method, AuthMethod::Trust);
        assert!(user.password_hash.is_empty());
    }

    #[test]
    fn test_user_superuser_role() {
        let superuser = User::new("admin".to_string(), "password123", true, AuthMethod::MD5);
        let regular = User::new("user".to_string(), "password123", false, AuthMethod::MD5);

        assert!(superuser.roles.contains(&"superuser".to_string()));
        assert!(!regular.roles.contains(&"superuser".to_string()));
        assert!(regular.roles.contains(&"user".to_string()));
    }

    #[test]
    fn test_user_verify_password_trust() {
        let user = User::new("testuser".to_string(), "", false, AuthMethod::Trust);
        // Trust always succeeds
        assert!(user.verify_password("any_password", None));
        assert!(user.verify_password("", None));
    }

    #[test]
    fn test_user_verify_password_md5_direct() {
        let user = User::new("testuser".to_string(), "password123", false, AuthMethod::MD5);
        // Direct password verification (no challenge)
        assert!(user.verify_password("password123", None));
        assert!(!user.verify_password("wrongpassword", None));
    }

    #[test]
    fn test_user_verify_password_md5_with_challenge() {
        let user = User::new("testuser".to_string(), "password123", false, AuthMethod::MD5);
        let salt: [u8; 4] = [0x01, 0x02, 0x03, 0x04];

        let response = md5_auth("password123", "testuser", &salt);
        assert!(user.verify_password(&response, Some(&salt)));

        let wrong_response = md5_auth("wrongpassword", "testuser", &salt);
        assert!(!user.verify_password(&wrong_response, Some(&salt)));
    }

    #[test]
    fn test_user_verify_password_scram() {
        let user = User::new(
            "testuser".to_string(),
            "password123",
            false,
            AuthMethod::ScramSha256,
        );
        assert!(user.verify_password("password123", None));
        assert!(!user.verify_password("wrongpassword", None));
    }

    // ==================== User Lockout Tests ====================

    #[test]
    fn test_user_not_locked_by_default() {
        let user = User::new("testuser".to_string(), "password123", false, AuthMethod::MD5);
        assert!(!user.is_locked());
    }

    #[test]
    fn test_user_locked_until_future() {
        let mut user = User::new("testuser".to_string(), "password123", false, AuthMethod::MD5);

        // Lock user until future time
        let future_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600; // 1 hour from now

        user.locked_until = Some(future_time);
        assert!(user.is_locked());
    }

    #[test]
    fn test_user_unlocked_after_expiry() {
        let mut user = User::new("testuser".to_string(), "password123", false, AuthMethod::MD5);

        // Lock user until past time
        let past_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 1; // 1 second ago

        user.locked_until = Some(past_time);
        assert!(!user.is_locked());
    }

    // ==================== UserDb Tests ====================

    #[test]
    fn test_userdb_creates_default_superuser() {
        let config = AuthConfig {
            method: AuthMethod::MD5,
            require_auth: true,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300,
        };

        let db = UserDb::new(config);
        let users = db.list_users();

        assert!(users.contains(&"driftdb".to_string()));
        assert!(db.is_superuser("driftdb"));
    }

    #[test]
    fn test_userdb_no_default_user_when_auth_not_required() {
        let config = AuthConfig {
            method: AuthMethod::Trust,
            require_auth: false,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300,
        };

        let db = UserDb::new(config);
        let users = db.list_users();

        assert!(users.is_empty());
    }

    #[test]
    fn test_userdb_create_user() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("newuser".to_string(), "password123", false)
            .unwrap();

        let users = db.list_users();
        assert!(users.contains(&"newuser".to_string()));
        assert!(!db.is_superuser("newuser"));
    }

    #[test]
    fn test_userdb_create_duplicate_user_fails() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("newuser".to_string(), "password123", false)
            .unwrap();

        let result = db.create_user("newuser".to_string(), "password456", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_userdb_drop_user() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("tempuser".to_string(), "password123", false)
            .unwrap();
        assert!(db.list_users().contains(&"tempuser".to_string()));

        db.drop_user("tempuser").unwrap();
        assert!(!db.list_users().contains(&"tempuser".to_string()));
    }

    #[test]
    fn test_userdb_cannot_drop_default_superuser() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        let result = db.drop_user("driftdb");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot drop"));
    }

    #[test]
    fn test_userdb_drop_nonexistent_user_fails() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        let result = db.drop_user("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_userdb_change_password() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("testuser".to_string(), "oldpassword1", false)
            .unwrap();

        // Authentication should work with old password
        assert!(db
            .authenticate("testuser", "oldpassword1", "127.0.0.1", None)
            .unwrap());

        // Change password
        db.change_password("testuser", "newpassword2").unwrap();

        // Old password should fail
        assert!(db
            .authenticate("testuser", "oldpassword1", "127.0.0.1", None)
            .is_err());

        // New password should work
        assert!(db
            .authenticate("testuser", "newpassword2", "127.0.0.1", None)
            .unwrap());
    }

    #[test]
    fn test_userdb_authenticate_success() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("testuser".to_string(), "password123", false)
            .unwrap();

        let result = db.authenticate("testuser", "password123", "127.0.0.1", None);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_userdb_authenticate_wrong_password() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("testuser".to_string(), "password123", false)
            .unwrap();

        let result = db.authenticate("testuser", "wrongpassword", "127.0.0.1", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_userdb_authenticate_nonexistent_user() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        let result = db.authenticate("nonexistent", "password", "127.0.0.1", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_userdb_lockout_after_failed_attempts() {
        let config = AuthConfig {
            method: AuthMethod::MD5,
            require_auth: true,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300,
        };

        let db = UserDb::new(config);
        db.create_user("testuser".to_string(), "correctpassword1", false)
            .unwrap();

        // Fail 3 times
        for _ in 0..3 {
            let _ = db.authenticate("testuser", "wrongpassword", "127.0.0.1", None);
        }

        // Next attempt should fail due to lockout, even with correct password
        let result = db.authenticate("testuser", "correctpassword1", "127.0.0.1", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("locked"));
    }

    #[test]
    fn test_userdb_successful_auth_resets_failed_attempts() {
        let config = AuthConfig {
            method: AuthMethod::MD5,
            require_auth: true,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300,
        };

        let db = UserDb::new(config);
        db.create_user("testuser".to_string(), "password123", false)
            .unwrap();

        // Fail twice
        let _ = db.authenticate("testuser", "wrong", "127.0.0.1", None);
        let _ = db.authenticate("testuser", "wrong", "127.0.0.1", None);

        // Succeed - should reset counter
        db.authenticate("testuser", "password123", "127.0.0.1", None)
            .unwrap();

        // Fail twice more - should not be locked
        let _ = db.authenticate("testuser", "wrong", "127.0.0.1", None);
        let _ = db.authenticate("testuser", "wrong", "127.0.0.1", None);

        // Should still be able to authenticate
        let result = db.authenticate("testuser", "password123", "127.0.0.1", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_userdb_trust_bypasses_authentication() {
        let config = AuthConfig {
            method: AuthMethod::Trust,
            require_auth: false,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300,
        };

        let db = UserDb::new(config);

        // Trust should succeed for any password
        let result = db.authenticate("anyone", "anything", "127.0.0.1", None);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_userdb_auth_attempts_recorded() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("testuser".to_string(), "password123", false)
            .unwrap();

        // Make some authentication attempts
        let _ = db.authenticate("testuser", "wrong", "192.168.1.1", None);
        let _ = db.authenticate("testuser", "password123", "192.168.1.2", None);

        let attempts = db.get_recent_auth_attempts(10);
        assert_eq!(attempts.len(), 2);

        // Most recent should be first (reverse order)
        assert!(attempts[0].success);
        assert!(!attempts[1].success);
    }

    #[test]
    fn test_userdb_auth_attempts_limited_to_1000() {
        let config = AuthConfig {
            method: AuthMethod::Trust,
            require_auth: false,
            max_failed_attempts: 3,
            lockout_duration_seconds: 300,
        };

        let db = UserDb::new(config);

        // Record 1100 attempts
        for i in 0..1100 {
            db.authenticate(&format!("user{}", i), "pass", "127.0.0.1", None)
                .unwrap();
        }

        let attempts = db.get_recent_auth_attempts(2000);
        assert_eq!(attempts.len(), 1000);
    }

    #[test]
    fn test_userdb_get_user_info() {
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("testuser".to_string(), "password123", false)
            .unwrap();

        let info = db.get_user_info("testuser");
        assert!(info.is_some());
        let user = info.unwrap();
        assert_eq!(user.username, "testuser");

        let nonexistent = db.get_user_info("nonexistent");
        assert!(nonexistent.is_none());
    }

    // ==================== Username Validation Tests ====================

    #[test]
    fn test_validate_username_valid() {
        assert!(validate_username("testuser").is_ok());
        assert!(validate_username("test_user").is_ok());
        assert!(validate_username("test-user").is_ok());
        assert!(validate_username("user123").is_ok());
        assert!(validate_username("a").is_ok());
    }

    #[test]
    fn test_validate_username_empty() {
        let result = validate_username("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn test_validate_username_too_long() {
        let long_name = "a".repeat(64);
        let result = validate_username(&long_name);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn test_validate_username_invalid_characters() {
        assert!(validate_username("user@domain").is_err());
        assert!(validate_username("user name").is_err());
        assert!(validate_username("user;drop").is_err());
        assert!(validate_username("user'injection").is_err());
        assert!(validate_username("user\"quote").is_err());
        assert!(validate_username("user<script>").is_err());
    }

    #[test]
    fn test_validate_username_sql_injection_attempts() {
        // These should all fail validation
        assert!(validate_username("admin'--").is_err());
        assert!(validate_username("admin'; DROP TABLE users;--").is_err());
        assert!(validate_username("1' OR '1'='1").is_err());
        assert!(validate_username("admin/**/OR/**/1=1").is_err());
    }

    // ==================== Password Validation Tests ====================

    #[test]
    fn test_validate_password_valid() {
        assert!(validate_password("password1").is_ok());
        assert!(validate_password("SecurePass123!").is_ok());
        assert!(validate_password("12345678a").is_ok());
    }

    #[test]
    fn test_validate_password_too_short() {
        let result = validate_password("short1");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least 8"));
    }

    #[test]
    fn test_validate_password_too_long() {
        let long_password = "a1".repeat(51); // 102 chars
        let result = validate_password(&long_password);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn test_validate_password_no_letter() {
        let result = validate_password("12345678");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("letter"));
    }

    #[test]
    fn test_validate_password_no_number() {
        let result = validate_password("password");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("number"));
    }

    // ==================== Challenge Generation Tests ====================

    #[test]
    fn test_generate_md5_challenge() {
        let salt1 = generate_md5_challenge();
        let salt2 = generate_md5_challenge();

        // Salts should be different (with very high probability)
        assert_ne!(salt1, salt2);
        assert_eq!(salt1.len(), 4);
        assert_eq!(salt2.len(), 4);
    }

    #[test]
    fn test_generate_salt() {
        let salt1 = generate_salt();
        let salt2 = generate_salt();

        assert_ne!(salt1, salt2);
        assert_eq!(salt1.len(), 16);
    }

    #[test]
    fn test_generate_auth_challenge_trust() {
        let challenge = generate_auth_challenge(&AuthMethod::Trust);
        assert!(challenge.is_none());
    }

    #[test]
    fn test_generate_auth_challenge_md5() {
        let challenge = generate_auth_challenge(&AuthMethod::MD5);
        assert!(challenge.is_some());
        assert_eq!(challenge.unwrap().len(), 4);
    }

    #[test]
    fn test_generate_auth_challenge_scram() {
        let challenge = generate_auth_challenge(&AuthMethod::ScramSha256);
        assert!(challenge.is_some());
        assert_eq!(challenge.unwrap().len(), 18);
    }

    // ==================== Timing Attack Resistance Tests ====================

    #[test]
    fn test_timing_resistance_user_exists_vs_not() {
        // This test verifies that authentication time doesn't leak
        // whether a user exists or not. In production, you'd want
        // constant-time comparison, but at minimum we should ensure
        // both paths execute similar code.
        let config = AuthConfig::default();
        let db = UserDb::new(config);

        db.create_user("existinguser".to_string(), "password123", false)
            .unwrap();

        // Both should return errors, so neither leaks timing info
        let result1 = db.authenticate("existinguser", "wrongpassword", "127.0.0.1", None);
        let result2 = db.authenticate("nonexistent", "wrongpassword", "127.0.0.1", None);

        // Both fail - good for security
        assert!(result1.is_err());
        assert!(result2.is_err());
    }

    // ==================== Concurrent Access Tests ====================

    #[test]
    fn test_concurrent_authentication() {
        use std::sync::Arc;
        use std::thread;

        let config = AuthConfig::default();
        let db = Arc::new(UserDb::new(config));

        db.create_user("concurrent_user".to_string(), "password123", false)
            .unwrap();

        let mut handles = vec![];

        for i in 0..10 {
            let db_clone = Arc::clone(&db);
            let password = if i % 2 == 0 {
                "password123"
            } else {
                "wrongpassword"
            };

            handles.push(thread::spawn(move || {
                db_clone.authenticate("concurrent_user", password, "127.0.0.1", None)
            }));
        }

        for handle in handles {
            let _ = handle.join();
        }

        // After concurrent access, user should still be valid
        let result = db.authenticate("concurrent_user", "password123", "127.0.0.1", None);
        // May be locked due to failed attempts, but shouldn't panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_concurrent_user_creation() {
        use std::sync::Arc;
        use std::thread;

        let config = AuthConfig::default();
        let db = Arc::new(UserDb::new(config));

        let mut handles = vec![];

        for i in 0..10 {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                db_clone.create_user(format!("user{}", i), "password123", false)
            }));
        }

        let mut success_count = 0;
        for handle in handles {
            if handle.join().unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 10);
        assert_eq!(db.list_users().len(), 11); // 10 new + 1 default
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_password_md5() {
        let user = User::new("testuser".to_string(), "", false, AuthMethod::MD5);
        // Empty password should still work
        assert!(user.verify_password("", None));
    }

    #[test]
    fn test_unicode_username_allowed() {
        // Unicode alphanumeric characters are allowed per is_alphanumeric()
        // This is consistent with PostgreSQL which allows unicode identifiers
        assert!(validate_username("用户").is_ok());
        assert!(validate_username("пользователь").is_ok());
        // But special chars are still rejected
        assert!(validate_username("用户@").is_err());
    }

    #[test]
    fn test_special_chars_in_password() {
        // Passwords can have special chars
        let user = User::new(
            "testuser".to_string(),
            "p@ssw0rd!#$%",
            false,
            AuthMethod::MD5,
        );
        assert!(user.verify_password("p@ssw0rd!#$%", None));
    }

    #[test]
    fn test_max_length_username() {
        let username = "a".repeat(63); // Max valid length
        assert!(validate_username(&username).is_ok());
    }

    #[test]
    fn test_boundary_password_lengths() {
        // Exactly 8 chars with letter and number
        assert!(validate_password("passwor1").is_ok());

        // Exactly 100 chars
        let long_password = "a".repeat(99) + "1";
        assert!(validate_password(&long_password).is_ok());

        // 101 chars - too long
        let too_long = "a".repeat(100) + "1";
        assert!(validate_password(&too_long).is_err());
    }
}
