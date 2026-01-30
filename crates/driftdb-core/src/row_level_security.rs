//! Row-Level Security (RLS) Implementation
//!
//! Provides fine-grained access control at the row level through declarative
//! security policies. Policies determine which rows users can see, insert,
//! update, or delete based on security expressions.
//!
//! Features:
//! - Per-table security policies
//! - Policy types: SELECT, INSERT, UPDATE, DELETE
//! - Expression-based filtering with user context
//! - Integration with RBAC for user roles
//! - Policy caching for performance
//! - Bypass for superusers and table owners

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::errors::{DriftError, Result};

/// Policy action type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PolicyAction {
    /// Policy applies to SELECT queries
    Select,
    /// Policy applies to INSERT statements
    Insert,
    /// Policy applies to UPDATE statements
    Update,
    /// Policy applies to DELETE statements
    Delete,
    /// Policy applies to all operations
    All,
}

impl PolicyAction {
    /// Check if this action matches a specific operation
    pub fn matches(&self, action: PolicyAction) -> bool {
        *self == PolicyAction::All || *self == action
    }
}

/// Policy check type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyCheck {
    /// Permissive policy (OR with other policies)
    Permissive,
    /// Restrictive policy (AND with other policies)
    Restrictive,
}

/// Row-level security policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Policy name
    pub name: String,
    /// Table this policy applies to
    pub table_name: String,
    /// Action(s) this policy applies to
    pub action: PolicyAction,
    /// Check type (permissive or restrictive)
    pub check_type: PolicyCheck,
    /// Roles this policy applies to (empty = all roles)
    pub roles: Vec<String>,
    /// USING expression (for SELECT, UPDATE, DELETE)
    pub using_expr: Option<String>,
    /// WITH CHECK expression (for INSERT, UPDATE)
    pub with_check_expr: Option<String>,
    /// Whether this policy is enabled
    pub enabled: bool,
}

impl Policy {
    /// Create a new policy
    pub fn new(
        name: String,
        table_name: String,
        action: PolicyAction,
        check_type: PolicyCheck,
    ) -> Self {
        Self {
            name,
            table_name,
            action,
            check_type,
            roles: Vec::new(),
            using_expr: None,
            with_check_expr: None,
            enabled: true,
        }
    }

    /// Set the roles this policy applies to
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    /// Set the USING expression
    pub fn with_using(mut self, expr: String) -> Self {
        self.using_expr = Some(expr);
        self
    }

    /// Set the WITH CHECK expression
    pub fn with_check(mut self, expr: String) -> Self {
        self.with_check_expr = Some(expr);
        self
    }

    /// Check if this policy applies to a user role
    pub fn applies_to_role(&self, user_roles: &[String]) -> bool {
        if self.roles.is_empty() {
            return true; // Applies to all roles
        }

        user_roles.iter().any(|r| self.roles.contains(r))
    }

    /// Check if this policy applies to an action
    pub fn applies_to_action(&self, action: PolicyAction) -> bool {
        self.action.matches(action)
    }
}

/// Security context for policy evaluation
#[derive(Debug, Clone)]
pub struct SecurityContext {
    /// Current user
    pub username: String,
    /// User's roles
    pub roles: Vec<String>,
    /// Is superuser (bypasses all policies)
    pub is_superuser: bool,
    /// Current session ID
    pub session_id: Option<String>,
    /// Additional context variables for policy expressions
    pub variables: HashMap<String, String>,
}

impl SecurityContext {
    /// Create a new security context
    pub fn new(username: String, roles: Vec<String>, is_superuser: bool) -> Self {
        Self {
            username,
            roles,
            is_superuser,
            session_id: None,
            variables: HashMap::new(),
        }
    }

    /// Add a context variable
    pub fn with_variable(mut self, key: String, value: String) -> Self {
        self.variables.insert(key, value);
        self
    }
}

/// Policy evaluation result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyResult {
    /// Access allowed
    Allow,
    /// Access denied
    Deny,
    /// Conditional access with filter expression
    Filter(String),
}

/// Row-level security manager
pub struct RlsManager {
    /// Table policies: table_name -> policies
    policies: Arc<RwLock<HashMap<String, Vec<Policy>>>>,
    /// Tables with RLS enabled
    enabled_tables: Arc<RwLock<HashMap<String, bool>>>,
    /// Policy evaluation cache: (table, user, action) -> result
    #[allow(clippy::type_complexity)]
    cache: Arc<RwLock<HashMap<(String, String, PolicyAction), PolicyResult>>>,
}

impl RlsManager {
    /// Create a new RLS manager
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
            enabled_tables: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Enable RLS for a table
    pub fn enable_rls(&self, table_name: &str) -> Result<()> {
        info!("Enabling RLS for table: {}", table_name);
        self.enabled_tables
            .write()
            .insert(table_name.to_string(), true);
        self.clear_cache_for_table(table_name);
        Ok(())
    }

    /// Disable RLS for a table
    pub fn disable_rls(&self, table_name: &str) -> Result<()> {
        info!("Disabling RLS for table: {}", table_name);
        self.enabled_tables
            .write()
            .insert(table_name.to_string(), false);
        self.clear_cache_for_table(table_name);
        Ok(())
    }

    /// Check if RLS is enabled for a table
    pub fn is_rls_enabled(&self, table_name: &str) -> bool {
        self.enabled_tables
            .read()
            .get(table_name)
            .copied()
            .unwrap_or(false)
    }

    /// Create a new policy
    pub fn create_policy(&self, policy: Policy) -> Result<()> {
        let table_name = policy.table_name.clone();
        info!(
            "Creating policy '{}' for table '{}'",
            policy.name, table_name
        );

        let mut policies = self.policies.write();
        let table_policies = policies.entry(table_name.clone()).or_default();

        // Check for duplicate policy names
        if table_policies.iter().any(|p| p.name == policy.name) {
            return Err(DriftError::Other(format!(
                "Policy '{}' already exists for table '{}'",
                policy.name, table_name
            )));
        }

        table_policies.push(policy);
        drop(policies);

        self.clear_cache_for_table(&table_name);
        Ok(())
    }

    /// Drop a policy
    pub fn drop_policy(&self, table_name: &str, policy_name: &str) -> Result<()> {
        info!(
            "Dropping policy '{}' from table '{}'",
            policy_name, table_name
        );

        let mut policies = self.policies.write();
        if let Some(table_policies) = policies.get_mut(table_name) {
            let initial_len = table_policies.len();
            table_policies.retain(|p| p.name != policy_name);

            if table_policies.len() == initial_len {
                return Err(DriftError::Other(format!(
                    "Policy '{}' not found for table '{}'",
                    policy_name, table_name
                )));
            }
        } else {
            return Err(DriftError::Other(format!(
                "No policies found for table '{}'",
                table_name
            )));
        }

        drop(policies);
        self.clear_cache_for_table(table_name);
        Ok(())
    }

    /// Get all policies for a table
    pub fn get_policies(&self, table_name: &str) -> Vec<Policy> {
        self.policies
            .read()
            .get(table_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Evaluate policies for an action
    pub fn check_access(
        &self,
        table_name: &str,
        action: PolicyAction,
        context: &SecurityContext,
    ) -> Result<PolicyResult> {
        // Superusers bypass all RLS
        if context.is_superuser {
            debug!("Superuser {} bypasses RLS", context.username);
            return Ok(PolicyResult::Allow);
        }

        // If RLS is not enabled for this table, allow access
        if !self.is_rls_enabled(table_name) {
            debug!("RLS not enabled for table {}", table_name);
            return Ok(PolicyResult::Allow);
        }

        // Check cache
        let cache_key = (table_name.to_string(), context.username.clone(), action);
        if let Some(result) = self.cache.read().get(&cache_key) {
            debug!("Cache hit for RLS check");
            return Ok(result.clone());
        }

        // Get applicable policies
        let policies = self.get_applicable_policies(table_name, action, &context.roles);

        if policies.is_empty() {
            // No policies defined = deny by default when RLS is enabled
            warn!(
                "No policies for table {} action {:?}, denying access",
                table_name, action
            );
            let result = PolicyResult::Deny;
            self.cache.write().insert(cache_key, result.clone());
            return Ok(result);
        }

        // Evaluate policies
        let result = self.evaluate_policies(&policies, action, context)?;

        // Cache the result
        self.cache.write().insert(cache_key, result.clone());

        Ok(result)
    }

    /// Get applicable policies for a table, action, and roles
    fn get_applicable_policies(
        &self,
        table_name: &str,
        action: PolicyAction,
        user_roles: &[String],
    ) -> Vec<Policy> {
        let policies = self.policies.read();
        let table_policies = match policies.get(table_name) {
            Some(p) => p,
            None => return Vec::new(),
        };

        table_policies
            .iter()
            .filter(|p| p.enabled && p.applies_to_action(action) && p.applies_to_role(user_roles))
            .cloned()
            .collect()
    }

    /// Evaluate a set of policies
    fn evaluate_policies(
        &self,
        policies: &[Policy],
        action: PolicyAction,
        context: &SecurityContext,
    ) -> Result<PolicyResult> {
        let mut permissive_filters = Vec::new();
        let mut restrictive_filters = Vec::new();

        for policy in policies {
            let expr = match action {
                PolicyAction::Select | PolicyAction::Delete => &policy.using_expr,
                PolicyAction::Insert => &policy.with_check_expr,
                PolicyAction::Update => {
                    // UPDATE uses both USING and WITH CHECK
                    &policy.using_expr
                }
                PolicyAction::All => &policy.using_expr,
            };

            if let Some(expr) = expr {
                let evaluated = self.evaluate_expression(expr, context)?;

                match policy.check_type {
                    PolicyCheck::Permissive => permissive_filters.push(evaluated),
                    PolicyCheck::Restrictive => restrictive_filters.push(evaluated),
                }
            }
        }

        // Combine filters:
        // - Permissive policies are OR'd together
        // - Restrictive policies are AND'd together
        // - Final result is: (permissive_1 OR permissive_2) AND (restrictive_1 AND restrictive_2)

        let mut filter_parts = Vec::new();

        if !permissive_filters.is_empty() {
            let permissive = permissive_filters.join(" OR ");
            filter_parts.push(format!("({})", permissive));
        }

        if !restrictive_filters.is_empty() {
            for restrictive in restrictive_filters {
                filter_parts.push(format!("({})", restrictive));
            }
        }

        if filter_parts.is_empty() {
            Ok(PolicyResult::Allow)
        } else {
            Ok(PolicyResult::Filter(filter_parts.join(" AND ")))
        }
    }

    /// Evaluate a policy expression with context
    fn evaluate_expression(&self, expr: &str, context: &SecurityContext) -> Result<String> {
        // Replace context variables in expression
        let mut result = expr.to_string();

        // Replace $user with current username
        result = result.replace("$user", &format!("'{}'", context.username));

        // Replace $session_id if present
        if let Some(session_id) = &context.session_id {
            result = result.replace("$session_id", &format!("'{}'", session_id));
        }

        // Replace custom variables
        for (key, value) in &context.variables {
            result = result.replace(&format!("${}", key), &format!("'{}'", value));
        }

        Ok(result)
    }

    /// Clear policy cache for a table
    fn clear_cache_for_table(&self, table_name: &str) {
        let mut cache = self.cache.write();
        cache.retain(|(t, _, _), _| t != table_name);
        debug!("Cleared RLS cache for table {}", table_name);
    }

    /// Clear entire policy cache
    pub fn clear_cache(&self) {
        self.cache.write().clear();
        debug!("Cleared entire RLS cache");
    }

    /// Get statistics about policies
    pub fn get_statistics(&self) -> RlsStatistics {
        let policies = self.policies.read();
        let enabled_tables = self.enabled_tables.read();

        let total_policies: usize = policies.values().map(|v| v.len()).sum();
        let enabled_policies: usize = policies
            .values()
            .map(|v| v.iter().filter(|p| p.enabled).count())
            .sum();

        RlsStatistics {
            total_policies,
            enabled_policies,
            tables_with_rls: enabled_tables.values().filter(|&&v| v).count(),
            cache_entries: self.cache.read().len(),
        }
    }
}

impl Default for RlsManager {
    fn default() -> Self {
        Self::new()
    }
}

/// RLS statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsStatistics {
    pub total_policies: usize,
    pub enabled_policies: usize,
    pub tables_with_rls: usize,
    pub cache_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_policy() {
        let manager = RlsManager::new();

        let policy = Policy::new(
            "users_select_policy".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        )
        .with_using("user_id = $user".to_string());

        assert!(manager.create_policy(policy).is_ok());

        let policies = manager.get_policies("users");
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].name, "users_select_policy");
    }

    #[test]
    fn test_duplicate_policy_name() {
        let manager = RlsManager::new();

        let policy1 = Policy::new(
            "test_policy".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        );

        let policy2 = Policy::new(
            "test_policy".to_string(),
            "users".to_string(),
            PolicyAction::Update,
            PolicyCheck::Permissive,
        );

        assert!(manager.create_policy(policy1).is_ok());
        assert!(manager.create_policy(policy2).is_err());
    }

    #[test]
    fn test_enable_disable_rls() {
        let manager = RlsManager::new();

        assert!(!manager.is_rls_enabled("users"));

        manager.enable_rls("users").unwrap();
        assert!(manager.is_rls_enabled("users"));

        manager.disable_rls("users").unwrap();
        assert!(!manager.is_rls_enabled("users"));
    }

    #[test]
    fn test_superuser_bypass() {
        let manager = RlsManager::new();

        manager.enable_rls("users").unwrap();

        let policy = Policy::new(
            "restrictive_policy".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        )
        .with_using("false".to_string());

        manager.create_policy(policy).unwrap();

        let context = SecurityContext::new("admin".to_string(), vec![], true);

        let result = manager.check_access("users", PolicyAction::Select, &context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PolicyResult::Allow);
    }

    #[test]
    fn test_no_policies_deny() {
        let manager = RlsManager::new();

        manager.enable_rls("users").unwrap();

        let context = SecurityContext::new("alice".to_string(), vec!["user".to_string()], false);

        let result = manager.check_access("users", PolicyAction::Select, &context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PolicyResult::Deny);
    }

    #[test]
    fn test_policy_action_matching() {
        assert!(PolicyAction::All.matches(PolicyAction::Select));
        assert!(PolicyAction::All.matches(PolicyAction::Insert));
        assert!(PolicyAction::Select.matches(PolicyAction::Select));
        assert!(!PolicyAction::Select.matches(PolicyAction::Insert));
    }

    #[test]
    fn test_policy_role_filtering() {
        let policy = Policy::new(
            "test".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        )
        .with_roles(vec!["admin".to_string(), "user".to_string()]);

        assert!(policy.applies_to_role(&["admin".to_string()]));
        assert!(policy.applies_to_role(&["user".to_string()]));
        assert!(!policy.applies_to_role(&["guest".to_string()]));
    }

    #[test]
    fn test_expression_substitution() {
        let manager = RlsManager::new();

        let context = SecurityContext::new("alice".to_string(), vec![], false)
            .with_variable("tenant_id".to_string(), "123".to_string());

        let expr = "user_id = $user AND tenant_id = $tenant_id";
        let result = manager.evaluate_expression(expr, &context).unwrap();

        assert_eq!(result, "user_id = 'alice' AND tenant_id = '123'");
    }

    #[test]
    fn test_permissive_policies_or() {
        let manager = RlsManager::new();

        manager.enable_rls("users").unwrap();

        let policy1 = Policy::new(
            "own_rows".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        )
        .with_using("user_id = $user".to_string());

        let policy2 = Policy::new(
            "public_rows".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        )
        .with_using("is_public = true".to_string());

        manager.create_policy(policy1).unwrap();
        manager.create_policy(policy2).unwrap();

        let context = SecurityContext::new("alice".to_string(), vec!["user".to_string()], false);

        let result = manager.check_access("users", PolicyAction::Select, &context);
        assert!(result.is_ok());

        if let PolicyResult::Filter(filter) = result.unwrap() {
            // Should be OR'd together
            assert!(filter.contains("user_id = 'alice'"));
            assert!(filter.contains("is_public = true"));
            assert!(filter.contains("OR"));
        } else {
            panic!("Expected Filter result");
        }
    }

    #[test]
    fn test_drop_policy() {
        let manager = RlsManager::new();

        let policy = Policy::new(
            "test_policy".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        );

        manager.create_policy(policy).unwrap();
        assert_eq!(manager.get_policies("users").len(), 1);

        manager.drop_policy("users", "test_policy").unwrap();
        assert_eq!(manager.get_policies("users").len(), 0);
    }

    #[test]
    fn test_statistics() {
        let manager = RlsManager::new();

        manager.enable_rls("users").unwrap();
        manager.enable_rls("posts").unwrap();

        let policy1 = Policy::new(
            "p1".to_string(),
            "users".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        );

        let policy2 = Policy::new(
            "p2".to_string(),
            "posts".to_string(),
            PolicyAction::Select,
            PolicyCheck::Permissive,
        );

        manager.create_policy(policy1).unwrap();
        manager.create_policy(policy2).unwrap();

        let stats = manager.get_statistics();
        assert_eq!(stats.total_policies, 2);
        assert_eq!(stats.enabled_policies, 2);
        assert_eq!(stats.tables_with_rls, 2);
    }
}
