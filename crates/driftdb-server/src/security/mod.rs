//! Security module for DriftDB server
//!
//! This module provides security features including:
//! - SQL injection protection and validation
//! - Input sanitization
//! - Query pattern analysis
//! - Security logging and monitoring

pub mod sql_validator;

pub use sql_validator::SqlValidator;
