//! Security module for DriftDB server
//!
//! This module provides security features including:
//! - SQL injection protection and validation
//! - Input sanitization
//! - Query pattern analysis
//! - Security logging and monitoring
//! - Role-Based Access Control (RBAC)
//! - RBAC permission enforcement

pub mod sql_validator;
pub mod rbac;
pub mod rbac_enforcement;

pub use sql_validator::SqlValidator;
pub use rbac::{RbacManager, Permission, RoleName, Role};
pub use rbac_enforcement::{
    check_query_permission,
    check_view_users_permission,
    check_view_system_info_permission,
    check_view_metrics_permission,
    check_view_audit_log_permission,
    check_grant_role_permission,
    check_revoke_role_permission,
    check_multiple_permissions,
    check_any_permission,
};
