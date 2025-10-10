//! Role-Based Access Control (RBAC) for DriftDB
//!
//! This module implements a comprehensive RBAC system with:
//! - Predefined roles (Superuser, Admin, User, ReadOnly)
//! - Fine-grained permissions for all database operations
//! - Role-permission mappings
//! - User-role assignments
//! - Permission enforcement at query execution time
//! - Security audit integration

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{debug, warn, info};

/// All possible permissions in the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    // Table operations
    CreateTable,
    DropTable,
    AlterTable,
    TruncateTable,

    // Data operations
    Select,
    Insert,
    Update,
    Delete,

    // Index operations
    CreateIndex,
    DropIndex,

    // Transaction operations
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,

    // User management
    CreateUser,
    DropUser,
    AlterUser,
    ViewUsers,

    // Role management
    GrantRole,
    RevokeRole,
    ViewRoles,

    // Permission management
    GrantPermission,
    RevokePermission,

    // Database management
    CreateDatabase,
    DropDatabase,
    ViewDatabases,

    // System operations
    ViewSystemInfo,
    ModifySystemSettings,
    ViewMetrics,
    ViewAuditLog,

    // Replication operations
    ViewReplicationStatus,
    ManageReplication,

    // Snapshot and maintenance
    CreateSnapshot,
    RestoreSnapshot,
    CompactDatabase,

    // Security operations
    ViewSecuritySettings,
    ModifySecuritySettings,
}

impl Permission {
    /// Get human-readable description of permission
    pub fn description(&self) -> &'static str {
        match self {
            Permission::CreateTable => "Create new tables",
            Permission::DropTable => "Drop existing tables",
            Permission::AlterTable => "Modify table structure",
            Permission::TruncateTable => "Remove all data from tables",
            Permission::Select => "Read data from tables",
            Permission::Insert => "Insert data into tables",
            Permission::Update => "Update existing data",
            Permission::Delete => "Delete data from tables",
            Permission::CreateIndex => "Create indexes on tables",
            Permission::DropIndex => "Drop existing indexes",
            Permission::BeginTransaction => "Start transactions",
            Permission::CommitTransaction => "Commit transactions",
            Permission::RollbackTransaction => "Rollback transactions",
            Permission::CreateUser => "Create new users",
            Permission::DropUser => "Drop existing users",
            Permission::AlterUser => "Modify user accounts",
            Permission::ViewUsers => "View user list",
            Permission::GrantRole => "Grant roles to users",
            Permission::RevokeRole => "Revoke roles from users",
            Permission::ViewRoles => "View role information",
            Permission::GrantPermission => "Grant permissions to roles",
            Permission::RevokePermission => "Revoke permissions from roles",
            Permission::CreateDatabase => "Create new databases",
            Permission::DropDatabase => "Drop existing databases",
            Permission::ViewDatabases => "View database list",
            Permission::ViewSystemInfo => "View system information",
            Permission::ModifySystemSettings => "Modify system settings",
            Permission::ViewMetrics => "View performance metrics",
            Permission::ViewAuditLog => "View security audit log",
            Permission::ViewReplicationStatus => "View replication status",
            Permission::ManageReplication => "Manage replication settings",
            Permission::CreateSnapshot => "Create database snapshots",
            Permission::RestoreSnapshot => "Restore from snapshots",
            Permission::CompactDatabase => "Compact database files",
            Permission::ViewSecuritySettings => "View security settings",
            Permission::ModifySecuritySettings => "Modify security settings",
        }
    }
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Predefined system roles
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RoleName {
    /// Superuser with all permissions
    Superuser,
    /// Admin with most permissions except some system-level operations
    Admin,
    /// Regular user with read/write access
    User,
    /// Read-only user
    ReadOnly,
    /// Custom role
    Custom(String),
}

impl RoleName {
    /// Get all predefined roles
    pub fn predefined_roles() -> Vec<RoleName> {
        vec![
            RoleName::Superuser,
            RoleName::Admin,
            RoleName::User,
            RoleName::ReadOnly,
        ]
    }
}

impl std::fmt::Display for RoleName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoleName::Superuser => write!(f, "superuser"),
            RoleName::Admin => write!(f, "admin"),
            RoleName::User => write!(f, "user"),
            RoleName::ReadOnly => write!(f, "readonly"),
            RoleName::Custom(name) => write!(f, "{}", name),
        }
    }
}

impl std::str::FromStr for RoleName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "superuser" => Ok(RoleName::Superuser),
            "admin" => Ok(RoleName::Admin),
            "user" => Ok(RoleName::User),
            "readonly" => Ok(RoleName::ReadOnly),
            name => Ok(RoleName::Custom(name.to_string())),
        }
    }
}

/// Role definition with permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: RoleName,
    pub permissions: HashSet<Permission>,
    pub description: String,
    pub created_at: u64,
    pub is_system_role: bool,
}

impl Role {
    /// Create superuser role with all permissions
    pub fn superuser() -> Self {
        let mut permissions = HashSet::new();
        // Superuser has ALL permissions
        permissions.insert(Permission::CreateTable);
        permissions.insert(Permission::DropTable);
        permissions.insert(Permission::AlterTable);
        permissions.insert(Permission::TruncateTable);
        permissions.insert(Permission::Select);
        permissions.insert(Permission::Insert);
        permissions.insert(Permission::Update);
        permissions.insert(Permission::Delete);
        permissions.insert(Permission::CreateIndex);
        permissions.insert(Permission::DropIndex);
        permissions.insert(Permission::BeginTransaction);
        permissions.insert(Permission::CommitTransaction);
        permissions.insert(Permission::RollbackTransaction);
        permissions.insert(Permission::CreateUser);
        permissions.insert(Permission::DropUser);
        permissions.insert(Permission::AlterUser);
        permissions.insert(Permission::ViewUsers);
        permissions.insert(Permission::GrantRole);
        permissions.insert(Permission::RevokeRole);
        permissions.insert(Permission::ViewRoles);
        permissions.insert(Permission::GrantPermission);
        permissions.insert(Permission::RevokePermission);
        permissions.insert(Permission::CreateDatabase);
        permissions.insert(Permission::DropDatabase);
        permissions.insert(Permission::ViewDatabases);
        permissions.insert(Permission::ViewSystemInfo);
        permissions.insert(Permission::ModifySystemSettings);
        permissions.insert(Permission::ViewMetrics);
        permissions.insert(Permission::ViewAuditLog);
        permissions.insert(Permission::ViewReplicationStatus);
        permissions.insert(Permission::ManageReplication);
        permissions.insert(Permission::CreateSnapshot);
        permissions.insert(Permission::RestoreSnapshot);
        permissions.insert(Permission::CompactDatabase);
        permissions.insert(Permission::ViewSecuritySettings);
        permissions.insert(Permission::ModifySecuritySettings);

        Self {
            name: RoleName::Superuser,
            permissions,
            description: "Superuser with all system permissions".to_string(),
            created_at: current_timestamp(),
            is_system_role: true,
        }
    }

    /// Create admin role (most permissions except critical system operations)
    pub fn admin() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert(Permission::CreateTable);
        permissions.insert(Permission::DropTable);
        permissions.insert(Permission::AlterTable);
        permissions.insert(Permission::TruncateTable);
        permissions.insert(Permission::Select);
        permissions.insert(Permission::Insert);
        permissions.insert(Permission::Update);
        permissions.insert(Permission::Delete);
        permissions.insert(Permission::CreateIndex);
        permissions.insert(Permission::DropIndex);
        permissions.insert(Permission::BeginTransaction);
        permissions.insert(Permission::CommitTransaction);
        permissions.insert(Permission::RollbackTransaction);
        permissions.insert(Permission::CreateUser);
        permissions.insert(Permission::AlterUser);
        permissions.insert(Permission::ViewUsers);
        permissions.insert(Permission::GrantRole);
        permissions.insert(Permission::ViewRoles);
        permissions.insert(Permission::CreateDatabase);
        permissions.insert(Permission::ViewDatabases);
        permissions.insert(Permission::ViewSystemInfo);
        permissions.insert(Permission::ViewMetrics);
        permissions.insert(Permission::ViewAuditLog);
        permissions.insert(Permission::ViewReplicationStatus);
        permissions.insert(Permission::CreateSnapshot);
        permissions.insert(Permission::CompactDatabase);
        permissions.insert(Permission::ViewSecuritySettings);

        Self {
            name: RoleName::Admin,
            permissions,
            description: "Administrator with most permissions".to_string(),
            created_at: current_timestamp(),
            is_system_role: true,
        }
    }

    /// Create regular user role (read/write access)
    pub fn user() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert(Permission::CreateTable);
        permissions.insert(Permission::Select);
        permissions.insert(Permission::Insert);
        permissions.insert(Permission::Update);
        permissions.insert(Permission::Delete);
        permissions.insert(Permission::CreateIndex);
        permissions.insert(Permission::BeginTransaction);
        permissions.insert(Permission::CommitTransaction);
        permissions.insert(Permission::RollbackTransaction);
        permissions.insert(Permission::ViewDatabases);
        permissions.insert(Permission::CreateSnapshot);

        Self {
            name: RoleName::User,
            permissions,
            description: "Regular user with read/write access".to_string(),
            created_at: current_timestamp(),
            is_system_role: true,
        }
    }

    /// Create read-only role
    pub fn readonly() -> Self {
        let mut permissions = HashSet::new();
        permissions.insert(Permission::Select);
        permissions.insert(Permission::BeginTransaction);
        permissions.insert(Permission::CommitTransaction);
        permissions.insert(Permission::RollbackTransaction);
        permissions.insert(Permission::ViewDatabases);
        permissions.insert(Permission::ViewMetrics);

        Self {
            name: RoleName::ReadOnly,
            permissions,
            description: "Read-only user with SELECT permission".to_string(),
            created_at: current_timestamp(),
            is_system_role: true,
        }
    }

    /// Create custom role
    pub fn custom(name: String, permissions: HashSet<Permission>, description: String) -> Self {
        Self {
            name: RoleName::Custom(name),
            permissions,
            description,
            created_at: current_timestamp(),
            is_system_role: false,
        }
    }

    /// Check if role has a specific permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.permissions.contains(&permission)
    }
}

/// RBAC Manager for managing roles and permissions
pub struct RbacManager {
    roles: Arc<RwLock<HashMap<String, Role>>>,
    user_roles: Arc<RwLock<HashMap<String, HashSet<String>>>>, // username -> role names
}

impl RbacManager {
    /// Create new RBAC manager with predefined roles
    pub fn new() -> Self {
        let mut roles = HashMap::new();

        // Register predefined system roles
        let superuser = Role::superuser();
        let admin = Role::admin();
        let user = Role::user();
        let readonly = Role::readonly();

        roles.insert(superuser.name.to_string(), superuser);
        roles.insert(admin.name.to_string(), admin);
        roles.insert(user.name.to_string(), user);
        roles.insert(readonly.name.to_string(), readonly);

        info!("RBAC manager initialized with 4 system roles");

        Self {
            roles: Arc::new(RwLock::new(roles)),
            user_roles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Grant a role to a user
    pub fn grant_role(&self, username: &str, role_name: &str) -> Result<()> {
        // Verify role exists
        {
            let roles = self.roles.read();
            if !roles.contains_key(role_name) {
                return Err(anyhow!("Role '{}' does not exist", role_name));
            }
        }

        let mut user_roles = self.user_roles.write();
        user_roles
            .entry(username.to_string())
            .or_insert_with(HashSet::new)
            .insert(role_name.to_string());

        info!("Granted role '{}' to user '{}'", role_name, username);
        Ok(())
    }

    /// Revoke a role from a user
    pub fn revoke_role(&self, username: &str, role_name: &str) -> Result<()> {
        let mut user_roles = self.user_roles.write();
        if let Some(roles) = user_roles.get_mut(username) {
            if roles.remove(role_name) {
                info!("Revoked role '{}' from user '{}'", role_name, username);
                return Ok(());
            }
        }

        Err(anyhow!("User '{}' does not have role '{}'", username, role_name))
    }

    /// Get all roles assigned to a user
    pub fn get_user_roles(&self, username: &str) -> Vec<Role> {
        let user_roles = self.user_roles.read();
        let roles = self.roles.read();

        if let Some(role_names) = user_roles.get(username) {
            role_names
                .iter()
                .filter_map(|name| roles.get(name).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Check if user has a specific permission
    pub fn has_permission(&self, username: &str, permission: Permission) -> bool {
        let user_roles = self.get_user_roles(username);

        for role in user_roles {
            if role.has_permission(permission) {
                debug!("User '{}' has permission {:?} via role '{}'",
                    username, permission, role.name);
                return true;
            }
        }

        debug!("User '{}' does NOT have permission {:?}", username, permission);
        false
    }

    /// Check if user has permission, returning error if not
    pub fn require_permission(&self, username: &str, permission: Permission) -> Result<()> {
        if self.has_permission(username, permission) {
            Ok(())
        } else {
            warn!("Permission denied for user '{}': missing {:?}", username, permission);
            Err(anyhow!(
                "Permission denied: user '{}' does not have '{}' permission",
                username,
                permission
            ))
        }
    }

    /// Create a custom role
    pub fn create_custom_role(
        &self,
        name: String,
        permissions: HashSet<Permission>,
        description: String,
    ) -> Result<()> {
        let mut roles = self.roles.write();

        if roles.contains_key(&name) {
            return Err(anyhow!("Role '{}' already exists", name));
        }

        let role = Role::custom(name.clone(), permissions, description);
        roles.insert(name.clone(), role);

        info!("Created custom role '{}'", name);
        Ok(())
    }

    /// Delete a custom role (cannot delete system roles)
    pub fn delete_custom_role(&self, name: &str) -> Result<()> {
        let mut roles = self.roles.write();

        if let Some(role) = roles.get(name) {
            if role.is_system_role {
                return Err(anyhow!("Cannot delete system role '{}'", name));
            }
        } else {
            return Err(anyhow!("Role '{}' does not exist", name));
        }

        roles.remove(name);

        // Remove role from all users
        let mut user_roles = self.user_roles.write();
        for roles_set in user_roles.values_mut() {
            roles_set.remove(name);
        }

        info!("Deleted custom role '{}'", name);
        Ok(())
    }

    /// Get all roles in the system
    pub fn get_all_roles(&self) -> Vec<Role> {
        let roles = self.roles.read();
        roles.values().cloned().collect()
    }

    /// Get a specific role by name
    pub fn get_role(&self, name: &str) -> Option<Role> {
        let roles = self.roles.read();
        roles.get(name).cloned()
    }

    /// Add permission to a custom role
    pub fn add_permission_to_role(&self, role_name: &str, permission: Permission) -> Result<()> {
        let mut roles = self.roles.write();

        if let Some(role) = roles.get_mut(role_name) {
            if role.is_system_role {
                return Err(anyhow!("Cannot modify system role '{}'", role_name));
            }

            role.permissions.insert(permission);
            info!("Added permission {:?} to role '{}'", permission, role_name);
            Ok(())
        } else {
            Err(anyhow!("Role '{}' does not exist", role_name))
        }
    }

    /// Remove permission from a custom role
    pub fn remove_permission_from_role(&self, role_name: &str, permission: Permission) -> Result<()> {
        let mut roles = self.roles.write();

        if let Some(role) = roles.get_mut(role_name) {
            if role.is_system_role {
                return Err(anyhow!("Cannot modify system role '{}'", role_name));
            }

            role.permissions.remove(&permission);
            info!("Removed permission {:?} from role '{}'", permission, role_name);
            Ok(())
        } else {
            Err(anyhow!("Role '{}' does not exist", role_name))
        }
    }

    /// Get all permissions for a user (aggregated from all roles)
    pub fn get_user_permissions(&self, username: &str) -> HashSet<Permission> {
        let user_roles = self.get_user_roles(username);
        let mut all_permissions = HashSet::new();

        for role in user_roles {
            all_permissions.extend(role.permissions);
        }

        all_permissions
    }
}

impl Default for RbacManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to get current timestamp
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_superuser_has_all_permissions() {
        let role = Role::superuser();
        assert!(role.has_permission(Permission::CreateTable));
        assert!(role.has_permission(Permission::DropTable));
        assert!(role.has_permission(Permission::CreateUser));
        assert!(role.has_permission(Permission::ModifySystemSettings));
    }

    #[test]
    fn test_admin_lacks_some_permissions() {
        let role = Role::admin();
        assert!(role.has_permission(Permission::CreateTable));
        assert!(role.has_permission(Permission::CreateUser));
        assert!(!role.has_permission(Permission::ModifySystemSettings));
        assert!(!role.has_permission(Permission::DropUser));
    }

    #[test]
    fn test_readonly_only_select() {
        let role = Role::readonly();
        assert!(role.has_permission(Permission::Select));
        assert!(!role.has_permission(Permission::Insert));
        assert!(!role.has_permission(Permission::Update));
        assert!(!role.has_permission(Permission::Delete));
        assert!(!role.has_permission(Permission::CreateTable));
    }

    #[test]
    fn test_grant_and_revoke_role() {
        let rbac = RbacManager::new();

        rbac.grant_role("alice", "user").unwrap();
        assert!(rbac.has_permission("alice", Permission::Select));
        assert!(rbac.has_permission("alice", Permission::Insert));

        rbac.revoke_role("alice", "user").unwrap();
        assert!(!rbac.has_permission("alice", Permission::Select));
    }

    #[test]
    fn test_multiple_roles() {
        let rbac = RbacManager::new();

        rbac.grant_role("bob", "readonly").unwrap();
        rbac.grant_role("bob", "user").unwrap();

        // Should have permissions from both roles
        assert!(rbac.has_permission("bob", Permission::Select)); // from readonly
        assert!(rbac.has_permission("bob", Permission::Insert)); // from user
    }

    #[test]
    fn test_custom_role_creation() {
        let rbac = RbacManager::new();

        let mut perms = HashSet::new();
        perms.insert(Permission::Select);
        perms.insert(Permission::ViewMetrics);

        rbac.create_custom_role(
            "analyst".to_string(),
            perms,
            "Data analyst role".to_string(),
        ).unwrap();

        rbac.grant_role("charlie", "analyst").unwrap();
        assert!(rbac.has_permission("charlie", Permission::Select));
        assert!(rbac.has_permission("charlie", Permission::ViewMetrics));
        assert!(!rbac.has_permission("charlie", Permission::Insert));
    }

    #[test]
    fn test_cannot_delete_system_role() {
        let rbac = RbacManager::new();

        let result = rbac.delete_custom_role("superuser");
        assert!(result.is_err());
    }

    #[test]
    fn test_require_permission() {
        let rbac = RbacManager::new();

        rbac.grant_role("dave", "user").unwrap();

        // Should succeed
        assert!(rbac.require_permission("dave", Permission::Select).is_ok());

        // Should fail
        assert!(rbac.require_permission("dave", Permission::DropUser).is_err());
    }

    #[test]
    fn test_get_user_permissions() {
        let rbac = RbacManager::new();

        rbac.grant_role("eve", "user").unwrap();

        let perms = rbac.get_user_permissions("eve");
        assert!(perms.contains(&Permission::Select));
        assert!(perms.contains(&Permission::Insert));
        assert!(!perms.contains(&Permission::DropTable));
    }

    #[test]
    fn test_add_remove_permission_from_custom_role() {
        let rbac = RbacManager::new();

        let perms = HashSet::new();
        rbac.create_custom_role(
            "tester".to_string(),
            perms,
            "Test role".to_string(),
        ).unwrap();

        rbac.add_permission_to_role("tester", Permission::Select).unwrap();
        rbac.grant_role("frank", "tester").unwrap();

        assert!(rbac.has_permission("frank", Permission::Select));

        rbac.remove_permission_from_role("tester", Permission::Select).unwrap();
        assert!(!rbac.has_permission("frank", Permission::Select));
    }

    #[test]
    fn test_cannot_modify_system_role_permissions() {
        let rbac = RbacManager::new();

        let result = rbac.add_permission_to_role("readonly", Permission::Insert);
        assert!(result.is_err());
    }
}
