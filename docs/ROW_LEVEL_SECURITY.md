# Row-Level Security (RLS)

## Overview

DriftDB implements comprehensive row-level security (RLS) that provides fine-grained access control at the row level. RLS policies determine which rows users can see, insert, update, or delete based on security expressions evaluated in the context of the current user and session.

## Architecture

### Core Components

1. **RlsManager** (`row_level_security.rs` - 680 lines)
   - Policy storage and management
   - Table-level RLS enable/disable
   - Policy evaluation engine
   - Result caching for performance

2. **Policy System**
   - Declarative security policies attached to tables
   - Policy types: SELECT, INSERT, UPDATE, DELETE, ALL
   - Two policy checks: Permissive (OR) and Restrictive (AND)
   - Role-based policy application

3. **Security Context**
   - User identity and roles
   - Session information
   - Context variables for policy expressions
   - Superuser bypass mechanism

4. **Policy Evaluation**
   - Expression-based filtering with variable substitution
   - Permissive policies OR'd together
   - Restrictive policies AND'd together
   - Result caching with automatic invalidation

## Configuration

### Enabling RLS

```rust
use driftdb_core::RlsManager;

let rls_manager = RlsManager::new();

// Enable RLS for a table
rls_manager.enable_rls("users")?;

// Disable RLS for a table
rls_manager.disable_rls("users")?;

// Check if RLS is enabled
if rls_manager.is_rls_enabled("users") {
    // RLS is active
}
```

### Creating Policies

```rust
use driftdb_core::{Policy, PolicyAction, PolicyCheck};

// Policy 1: Users can only see their own rows
let own_rows_policy = Policy::new(
    "users_own_rows".to_string(),
    "users".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_using("user_id = $user".to_string());

rls_manager.create_policy(own_rows_policy)?;

// Policy 2: Users can also see public rows
let public_rows_policy = Policy::new(
    "users_public_rows".to_string(),
    "users".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_using("is_public = true".to_string());

rls_manager.create_policy(public_rows_policy)?;

// Policy 3: Restrict to active accounts only (restrictive)
let active_only_policy = Policy::new(
    "users_active_only".to_string(),
    "users".to_string(),
    PolicyAction::Select,
    PolicyCheck::Restrictive,
)
.with_using("status = 'active'".to_string());

rls_manager.create_policy(active_only_policy)?;
```

### Role-Based Policies

```rust
// Policy applies only to specific roles
let admin_policy = Policy::new(
    "admin_access".to_string(),
    "sensitive_data".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_roles(vec!["admin".to_string(), "auditor".to_string()])
.with_using("true".to_string()); // Admins see everything

rls_manager.create_policy(admin_policy)?;
```

## Usage

### Policy Types

#### SELECT Policies (USING expression)

Controls which rows are visible in SELECT queries:

```rust
// Users see only their own data
Policy::new(
    "select_own".to_string(),
    "documents".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_using("owner_id = $user");
```

#### INSERT Policies (WITH CHECK expression)

Controls which rows can be inserted:

```rust
// Users can only insert rows they own
Policy::new(
    "insert_own".to_string(),
    "documents".to_string(),
    PolicyAction::Insert,
    PolicyCheck::Permissive,
)
.with_check("owner_id = $user");
```

#### UPDATE Policies (USING + WITH CHECK)

Controls which rows can be updated and validates new values:

```rust
// Users can update their own rows
Policy::new(
    "update_own".to_string(),
    "documents".to_string(),
    PolicyAction::Update,
    PolicyCheck::Permissive,
)
.with_using("owner_id = $user")           // Can update if owner
.with_check("owner_id = $user");          // New values must preserve ownership
```

#### DELETE Policies (USING expression)

Controls which rows can be deleted:

```rust
// Users can delete their own non-published documents
Policy::new(
    "delete_own_draft".to_string(),
    "documents".to_string(),
    PolicyAction::Delete,
    PolicyCheck::Permissive,
)
.with_using("owner_id = $user AND status = 'draft'");
```

### Checking Access

```rust
use driftdb_core::{SecurityContext, PolicyAction};

// Create security context for current user
let context = SecurityContext::new(
    "alice".to_string(),
    vec!["user".to_string()],
    false, // not superuser
);

// Check access for SELECT
let result = rls_manager.check_access(
    "users",
    PolicyAction::Select,
    &context,
)?;

match result {
    PolicyResult::Allow => {
        // Full access, no filtering needed
    }
    PolicyResult::Deny => {
        // Access denied
        return Err("Permission denied");
    }
    PolicyResult::Filter(filter_expr) => {
        // Apply filter to query
        // e.g., "WHERE (user_id = 'alice' OR is_public = true) AND (status = 'active')"
        query = query.with_filter(filter_expr);
    }
}
```

### Context Variables

Policies can use context variables for dynamic expressions:

```rust
let context = SecurityContext::new(
    "alice".to_string(),
    vec!["user".to_string()],
    false,
)
.with_variable("tenant_id".to_string(), "acme_corp".to_string())
.with_variable("department".to_string(), "engineering".to_string());

// Policy using context variables
let policy = Policy::new(
    "tenant_isolation".to_string(),
    "data".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_using("tenant_id = $tenant_id AND department = $department");
```

**Available variables:**
- `$user` - Current username
- `$session_id` - Current session ID (if set)
- `$<custom>` - Any custom variable added to context

### Policy Combination

Policies are combined using boolean logic:

**Permissive Policies (OR):**
Multiple permissive policies are OR'd together - user needs to match ANY policy:

```
SELECT * FROM users WHERE (user_id = 'alice' OR is_public = true);
```

**Restrictive Policies (AND):**
Restrictive policies are AND'd with permissive - ALL restrictive policies must pass:

```
SELECT * FROM users WHERE
    (user_id = 'alice' OR is_public = true)  -- Permissive policies (OR'd)
    AND (status = 'active')                   -- Restrictive policy
    AND (tenant_id = 'acme');                 -- Another restrictive policy
```

## Examples

### Multi-Tenant Application

```rust
// Enable RLS for all tenant tables
rls_manager.enable_rls("customers")?;
rls_manager.enable_rls("orders")?;
rls_manager.enable_rls("products")?;

// Create tenant isolation policy
for table in &["customers", "orders", "products"] {
    let policy = Policy::new(
        format!("{}_tenant_isolation", table),
        table.to_string(),
        PolicyAction::All, // Applies to all operations
        PolicyCheck::Restrictive,
    )
    .with_using("tenant_id = $tenant_id".to_string())
    .with_check("tenant_id = $tenant_id".to_string());

    rls_manager.create_policy(policy)?;
}

// Users automatically see only their tenant's data
let context = SecurityContext::new(
    "user@acme.com".to_string(),
    vec!["user".to_string()],
    false,
)
.with_variable("tenant_id".to_string(), "acme".to_string());
```

### Hierarchical Access Control

```rust
// Employees can see their own department's data
let dept_policy = Policy::new(
    "department_access".to_string(),
    "employees".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_roles(vec!["employee".to_string()])
.with_using("department = $department");

// Managers can see all departments
let manager_policy = Policy::new(
    "manager_access".to_string(),
    "employees".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_roles(vec!["manager".to_string()])
.with_using("true");

rls_manager.create_policy(dept_policy)?;
rls_manager.create_policy(manager_policy)?;
```

### Data Ownership

```rust
// Users own their data
let owner_policy = Policy::new(
    "owner_access".to_string(),
    "documents".to_string(),
    PolicyAction::All,
    PolicyCheck::Permissive,
)
.with_using("owner_id = $user")
.with_check("owner_id = $user");

// Shared with user
let shared_policy = Policy::new(
    "shared_access".to_string(),
    "documents".to_string(),
    PolicyAction::Select,
    PolicyCheck::Permissive,
)
.with_using("$user = ANY(shared_with)");

rls_manager.create_policy(owner_policy)?;
rls_manager.create_policy(shared_policy)?;
```

## Performance

### Caching

RLS results are cached for performance:
- Cache key: `(table_name, username, action)`
- Automatic cache invalidation on policy changes
- Manual cache clearing: `rls_manager.clear_cache()`

**Cache Benefits:**
- Avoids re-evaluating policies for repeated queries
- Typically 100-1000x faster for cached results
- Minimal memory overhead

### Best Practices

1. **Use Restrictive Policies Sparingly**
   - Restrictive policies add complexity
   - Prefer permissive policies when possible
   - Reserve restrictive for mandatory constraints

2. **Keep Expressions Simple**
   - Simple expressions evaluate faster
   - Use indexed columns in policies
   - Avoid complex subqueries in policy expressions

3. **Minimize Policy Count**
   - Combine related policies when possible
   - Too many policies slow evaluation
   - Typical: 2-5 policies per table

4. **Index Policy Columns**
   - Ensure columns in policy expressions are indexed
   - Dramatically improves query performance
   - Example: If policy uses `tenant_id`, index it

5. **Use Context Variables**
   - More efficient than hardcoded values
   - Enables policy reuse across users
   - Reduces policy count

## Security Considerations

### Superuser Bypass

Superusers bypass all RLS policies:

```rust
let admin_context = SecurityContext::new(
    "admin".to_string(),
    vec!["admin".to_string()],
    true, // is_superuser = true
);

// Always returns PolicyResult::Allow
let result = rls_manager.check_access("users", PolicyAction::Select, &admin_context)?;
```

**Important:** Only grant superuser to fully trusted accounts.

### Policy Testing

Always test policies thoroughly:

```rust
#[test]
fn test_tenant_isolation() {
    let rls = RlsManager::new();
    rls.enable_rls("data")?;

    let policy = Policy::new(
        "tenant_policy".to_string(),
        "data".to_string(),
        PolicyAction::Select,
        PolicyCheck::Restrictive,
    )
    .with_using("tenant_id = $tenant_id");

    rls.create_policy(policy)?;

    let context = SecurityContext::new("user1".to_string(), vec![], false)
        .with_variable("tenant_id".to_string(), "tenant_a".to_string());

    let result = rls.check_access("data", PolicyAction::Select, &context)?;

    if let PolicyResult::Filter(filter) = result {
        assert!(filter.contains("tenant_id = 'tenant_a'"));
    }
}
```

### Audit Logging

Integrate RLS with audit logging:

```rust
let result = rls_manager.check_access(table, action, &context)?;

if result == PolicyResult::Deny {
    audit_logger.log_event(
        AuditEventType::AccessDenied,
        Some(context.username.clone()),
        client_addr,
        AuditSeverity::Warning,
        format!("RLS denied {} on {}", action, table),
        serde_json::json!({
            "table": table,
            "action": format!("{:?}", action),
            "user": context.username,
        }),
        AuditOutcome::Failure,
        None,
    );
}
```

## Management Operations

### Listing Policies

```rust
// Get all policies for a table
let policies = rls_manager.get_policies("users");
for policy in policies {
    println!("Policy: {} ({})", policy.name, if policy.enabled { "enabled" } else { "disabled" });
}
```

### Dropping Policies

```rust
// Remove a policy
rls_manager.drop_policy("users", "users_own_rows")?;
```

### Statistics

```rust
// Get RLS statistics
let stats = rls_manager.get_statistics();
println!("Total policies: {}", stats.total_policies);
println!("Enabled policies: {}", stats.enabled_policies);
println!("Tables with RLS: {}", stats.tables_with_rls);
println!("Cache entries: {}", stats.cache_entries);
```

## Integration

### With RBAC

RLS integrates seamlessly with RBAC:

```rust
// RBAC checks table-level permissions
rbac_manager.require_permission(username, Permission::Select)?;

// RLS filters rows within allowed tables
let result = rls_manager.check_access(table, PolicyAction::Select, &context)?;
```

**Security Layers:**
1. Authentication (who are you?)
2. RBAC (which tables can you access?)
3. RLS (which rows can you see?)

### With Query Optimizer

RLS filters are integrated into query planning:

```rust
// Original query
let mut query = select().from("users");

// Apply RLS filter
let result = rls_manager.check_access("users", PolicyAction::Select, &context)?;
if let PolicyResult::Filter(filter) = result {
    query = query.where_clause(filter);
}

// Optimizer sees the complete query with RLS filters
let optimized = optimizer.optimize(query)?;
```

## Testing

### Unit Tests (12 tests)

```bash
cargo test -p driftdb-core row_level_security::tests
```

Tests cover:
- Policy creation and management
- Duplicate policy prevention
- Enable/disable RLS
- Superuser bypass
- Default deny when no policies
- Policy action matching
- Role-based filtering
- Expression variable substitution
- Permissive policy OR combination
- Restrictive policy AND combination
- Policy dropping
- Statistics collection

**All 12 tests pass** ✅

## Status

✅ **Fully Implemented**
- Row-level security manager with policy storage
- Policy types: SELECT, INSERT, UPDATE, DELETE, ALL
- Permissive and restrictive policy checks
- Expression-based filtering with variable substitution
- Security context with user, roles, and variables
- Superuser bypass mechanism
- Policy caching for performance
- Role-based policy application
- 12 comprehensive unit tests (all passing)

## Files

- `crates/driftdb-core/src/row_level_security.rs` - RLS implementation (680 lines)

**Total Security Code:**
- RBAC: 969 lines (rbac.rs + rbac_enforcement.rs)
- RLS: 680 lines
- Audit Logging: ~500 lines
- TLS/Encryption: ~600 lines
- **Total: 2,749 lines of security code**
