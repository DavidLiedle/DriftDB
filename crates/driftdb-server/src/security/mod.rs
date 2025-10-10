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
pub use rbac::{RbacManager, Permission};
