# SQL Bridge Integration Solution

## Problem
The PostgreSQL server cannot execute SQL queries through the comprehensive sql_bridge module due to an async/sync deadlock:
- The server uses `tokio::sync::RwLock` (async locks)
- The sql_bridge `execute_sql()` function is synchronous
- Holding an async lock while calling sync code causes deadlock

## Root Cause
```rust
// This causes deadlock:
let mut engine = self.engine_write().await?;  // Async lock
driftdb_core::sql_bridge::execute_sql(&mut *engine, sql);  // Sync call
```

The async runtime blocks waiting for the sync function, but the sync function can't complete because the async runtime is blocked.

## Solution Options

### Option 1: Complete parking_lot Refactor (Attempted)
Replace all `tokio::sync::RwLock` with `parking_lot::RwLock`:
- ✅ Pros: Fixes the fundamental issue, allows sync SQL execution
- ❌ Cons: Requires refactoring entire server, breaks EnginePool compatibility

### Option 2: Make sql_bridge Async
Refactor sql_bridge to be async:
- ✅ Pros: Clean solution, maintains async consistency
- ❌ Cons: Requires refactoring core library, complex changes

### Option 3: Use spawn_blocking (Recommended)
Run SQL in blocking thread pool:
```rust
let sql_owned = sql.to_string();
let engine_clone = self.engine.clone();

let result = tokio::task::spawn_blocking(move || {
    let mut engine = engine_clone.blocking_write();
    driftdb_core::sql_bridge::execute_sql(&mut *engine, &sql_owned)
}).await?;
```
- ✅ Pros: Minimal changes, works with existing code
- ✅ Cons: None significant

### Option 4: Hybrid Approach
Use parking_lot for Engine, tokio for everything else:
- ✅ Pros: Best of both worlds
- ❌ Cons: Complex type management

## Implementation Steps

1. **Short-term (Quick Fix)**:
   - Use spawn_blocking for SQL execution
   - Keep existing tokio locks
   - Document the workaround

2. **Long-term (Proper Solution)**:
   - Gradually migrate to parking_lot
   - Or make sql_bridge async
   - Comprehensive testing

## Current Status
- WAL path issue: ✅ Fixed
- SQL bridge integration: ✅ Code added
- Deadlock resolution: 🔧 In progress
- Testing: ⏳ Blocked by deadlock

## Files Modified
- `crates/driftdb-server/src/executor.rs` - SQL execution logic
- `crates/driftdb-core/src/transaction.rs` - WAL path fix
- Various server files for lock refactoring (stashed)

## Next Actions
1. Apply spawn_blocking solution
2. Test SQL execution with psycopg2
3. Verify performance impact
4. Plan long-term refactor if needed