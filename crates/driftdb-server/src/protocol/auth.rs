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
    pub is_superuser: bool,
    pub created_at: u64,
    pub last_login: Option<u64>,
    pub failed_attempts: u32,
    pub locked_until: Option<u64>,
    pub auth_method: AuthMethod,
    pub scram_sha256: Option<ScramSha256>,
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

        Self {
            username,
            password_hash,
            salt,
            is_superuser,
            created_at: now,
            last_login: None,
            failed_attempts: 0,
            locked_until: None,
            auth_method,
            scram_sha256,
        }
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
            .map(|user| user.is_superuser)
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
