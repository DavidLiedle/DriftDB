//! RBAC Permission Enforcement
//!
//! This module provides permission checking functions that integrate
//! RBAC with query execution

use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::net::SocketAddr;
use tracing::{debug, warn};

use super::{RbacManager, Permission};
use crate::security_audit::{SecurityAuditLogger, AuditEventType, AuditSeverity, AuditOutcome};

/// Check if user has permission to execute a query type
pub fn check_query_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
    query_type: &str,
    audit_logger: Option<&Arc<SecurityAuditLogger>>,
    client_addr: &str,
) -> Result<()> {
    let permission = match query_type.to_uppercase().as_str() {
        "SELECT" => Permission::Select,
        "INSERT" => Permission::Insert,
        "UPDATE" => Permission::Update,
        "DELETE" => Permission::Delete,
        "CREATE TABLE" | "CREATE_TABLE" => Permission::CreateTable,
        "DROP TABLE" | "DROP_TABLE" => Permission::DropTable,
        "ALTER TABLE" | "ALTER_TABLE" => Permission::AlterTable,
        "TRUNCATE TABLE" | "TRUNCATE_TABLE" => Permission::TruncateTable,
        "CREATE INDEX" | "CREATE_INDEX" => Permission::CreateIndex,
        "DROP INDEX" | "DROP_INDEX" => Permission::DropIndex,
        "CREATE USER" | "CREATE_USER" => Permission::CreateUser,
        "DROP USER" | "DROP_USER" => Permission::DropUser,
        "ALTER USER" | "ALTER_USER" => Permission::AlterUser,
        "BEGIN" | "START TRANSACTION" => Permission::BeginTransaction,
        "COMMIT" => Permission::CommitTransaction,
        "ROLLBACK" => Permission::RollbackTransaction,
        "CREATE DATABASE" | "CREATE_DATABASE" => Permission::CreateDatabase,
        "DROP DATABASE" | "DROP_DATABASE" => Permission::DropDatabase,
        "CREATE SNAPSHOT" | "CREATE_SNAPSHOT" => Permission::CreateSnapshot,
        "RESTORE SNAPSHOT" | "RESTORE_SNAPSHOT" => Permission::RestoreSnapshot,
        "COMPACT" => Permission::CompactDatabase,
        _ => {
            // Unknown query type - log and allow (fail open for compatibility)
            debug!("Unknown query type for RBAC: {}, allowing by default", query_type);
            return Ok(());
        }
    };

    debug!("Checking permission {:?} for user '{}'", permission, username);

    match rbac_manager.require_permission(username, permission) {
        Ok(_) => {
            debug!("Permission granted: {:?} for user '{}'", permission, username);
            Ok(())
        }
        Err(e) => {
            warn!(
                "Permission denied: {:?} for user '{}' - {}",
                permission, username, e
            );

            // Log to audit logger if provided
            if let Some(logger) = audit_logger {
                let addr: SocketAddr = client_addr.parse().unwrap_or_else(|_| {
                    "127.0.0.1:0".parse().unwrap()
                });
                logger.log_event(
                    AuditEventType::PermissionDenied,
                    Some(username.to_string()),
                    addr,
                    AuditSeverity::Warning,
                    format!("Permission denied: {} for {} operation", permission, query_type),
                    serde_json::json!({
                        "permission": format!("{:?}", permission),
                        "operation": query_type,
                    }),
                    AuditOutcome::Failure,
                    None,
                );
            }

            Err(anyhow!(
                "Permission denied: user '{}' does not have '{}' permission for {} operation",
                username,
                permission,
                query_type
            ))
        }
    }
}

/// Check if user has permission to view users list
pub fn check_view_users_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
) -> Result<()> {
    rbac_manager.require_permission(username, Permission::ViewUsers)
}

/// Check if user has permission to view system information
pub fn check_view_system_info_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
) -> Result<()> {
    rbac_manager.require_permission(username, Permission::ViewSystemInfo)
}

/// Check if user has permission to view metrics
pub fn check_view_metrics_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
) -> Result<()> {
    rbac_manager.require_permission(username, Permission::ViewMetrics)
}

/// Check if user has permission to view audit log
pub fn check_view_audit_log_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
) -> Result<()> {
    rbac_manager.require_permission(username, Permission::ViewAuditLog)
}

/// Check if user has permission to grant roles
pub fn check_grant_role_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
) -> Result<()> {
    rbac_manager.require_permission(username, Permission::GrantRole)
}

/// Check if user has permission to revoke roles
pub fn check_revoke_role_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
) -> Result<()> {
    rbac_manager.require_permission(username, Permission::RevokeRole)
}

/// Check if user has permission for multiple operations (requires ALL)
pub fn check_multiple_permissions(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
    permissions: &[Permission],
) -> Result<()> {
    for permission in permissions {
        rbac_manager.require_permission(username, *permission)?;
    }
    Ok(())
}

/// Check if user has ANY of the listed permissions (requires at least ONE)
pub fn check_any_permission(
    rbac_manager: &Arc<RbacManager>,
    username: &str,
    permissions: &[Permission],
) -> Result<()> {
    for permission in permissions {
        if rbac_manager.has_permission(username, *permission) {
            return Ok(());
        }
    }

    Err(anyhow!(
        "Permission denied: user '{}' does not have any of the required permissions",
        username
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_select_permission() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("alice", "user").unwrap();

        let result = check_query_permission(&rbac, "alice", "SELECT", None, "127.0.0.1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_denied_permission() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("bob", "readonly").unwrap();

        let result = check_query_permission(&rbac, "bob", "INSERT", None, "127.0.0.1");
        assert!(result.is_err());
    }

    #[test]
    fn test_check_create_table_permission() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("charlie", "user").unwrap();

        let result = check_query_permission(&rbac, "charlie", "CREATE TABLE", None, "127.0.0.1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_drop_user_permission_denied() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("dave", "admin").unwrap();

        // Admin doesn't have DropUser permission
        let result = check_query_permission(&rbac, "dave", "DROP USER", None, "127.0.0.1");
        assert!(result.is_err());
    }

    #[test]
    fn test_check_drop_user_permission_allowed() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("eve", "superuser").unwrap();

        // Superuser has DropUser permission
        let result = check_query_permission(&rbac, "eve", "DROP USER", None, "127.0.0.1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_multiple_permissions() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("frank", "user").unwrap();

        let permissions = vec![Permission::Select, Permission::Insert];
        let result = check_multiple_permissions(&rbac, "frank", &permissions);
        assert!(result.is_ok());

        let permissions_with_denied = vec![Permission::Select, Permission::DropTable];
        let result = check_multiple_permissions(&rbac, "frank", &permissions_with_denied);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_any_permission() {
        let rbac = Arc::new(RbacManager::new());
        rbac.grant_role("grace", "readonly").unwrap();

        // Has Select, not Insert
        let permissions = vec![Permission::Select, Permission::Insert];
        let result = check_any_permission(&rbac, "grace", &permissions);
        assert!(result.is_ok());

        // Has neither DropTable nor CreateTable
        let permissions_all_denied = vec![Permission::DropTable, Permission::CreateTable];
        let result = check_any_permission(&rbac, "grace", &permissions_all_denied);
        assert!(result.is_err());
    }
}
