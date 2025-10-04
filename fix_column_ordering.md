# Column Ordering Fix Plan

## Issue
Columns are returned in alphabetical order instead of query order because:
1. `driftdb-core` returns data as `Vec<Value>` where Value is typically a JSON Object (Map)
2. JSON Maps don't preserve insertion order when iterated
3. The server extracts columns using `.keys()` which returns alphabetical order

## Solution
Need to modify the core QueryResult to include column ordering information:

1. **driftdb-core/src/query/mod.rs**:
   - Change `QueryResult::Rows` to include column names in order
   ```rust
   Rows {
       columns: Vec<String>,  // Column names in query order
       data: Vec<Value>       // Row data
   }
   ```

2. **driftdb-core/src/sql_bridge.rs**:
   - Modify all functions that return QueryResult::Rows to include columns
   - Preserve the order from the SQL SELECT clause

3. **driftdb-server/src/executor.rs**:
   - Update to use column order from core result
   - Remove the `.keys()` extraction that causes alphabetical ordering

## Implementation Steps
1. Modify QueryResult enum to include columns
2. Update sql_bridge to track and return column order
3. Update server executor to use provided column order
4. Test with column ordering test script