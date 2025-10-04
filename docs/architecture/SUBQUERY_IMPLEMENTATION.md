# Comprehensive Subquery Support Implementation for DriftDB

This document describes the comprehensive subquery support that has been implemented in DriftDB to achieve SQL completeness.

## Overview

The subquery implementation adds full support for all major subquery types and patterns commonly used in SQL databases:

1. **IN/NOT IN subqueries**: `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)`
2. **EXISTS/NOT EXISTS subqueries**: `SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)`
3. **Scalar subqueries in SELECT**: `SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users`
4. **ANY/ALL comparison subqueries**: `SELECT * FROM products WHERE price > ANY (SELECT price FROM competitors)`
5. **Derived tables (FROM subqueries)**: `SELECT * FROM (SELECT * FROM users WHERE age > 25) AS u`
6. **Correlated vs non-correlated subqueries**: Full support for both types with proper execution strategies

## Implementation Details

### 1. AST Structures Added

New types were added to represent subqueries in the AST:

#### Core Subquery Type
```rust
pub struct Subquery {
    pub sql: String,
    pub is_correlated: bool,
    pub referenced_columns: Vec<String>, // Columns from outer query referenced in subquery
}
```

#### Subquery Expressions for WHERE Clauses
```rust
pub enum SubqueryExpression {
    In {
        column: String,
        subquery: Subquery,
        negated: bool, // true for NOT IN
    },
    Exists {
        subquery: Subquery,
        negated: bool, // true for NOT EXISTS
    },
    Comparison {
        column: String,
        operator: String, // "=", ">", "<", etc.
        quantifier: Option<SubqueryQuantifier>, // ANY, ALL, or None for scalar
        subquery: Subquery,
    },
}

pub enum SubqueryQuantifier {
    Any,
    All,
}
```

#### Scalar Subqueries in SELECT
```rust
pub struct ScalarSubquery {
    pub subquery: Subquery,
    pub alias: Option<String>,
}

pub enum ExtendedSelectItem {
    Column(String),
    Aggregation(Aggregation),
    ScalarSubquery(ScalarSubquery),
}
```

#### Derived Tables in FROM
```rust
pub struct DerivedTable {
    pub subquery: Subquery,
    pub alias: String, // Required for derived tables
}
```

#### Enhanced WHERE Conditions
```rust
pub enum WhereCondition {
    Simple {
        column: String,
        operator: String,
        value: Value,
    },
    Subquery(SubqueryExpression),
}
```

### 2. Extended FROM Clause Support

The `FromClause` enum was extended to support derived tables:

```rust
enum FromClause {
    Single(TableRef),
    MultipleImplicit(Vec<TableRef>),
    WithJoins {
        base_table: TableRef,
        joins: Vec<Join>,
    },
    DerivedTable(DerivedTable), // NEW: Subquery used as table
    DerivedTableWithJoins {    // NEW: Derived table with JOINs
        base_table: DerivedTable,
        joins: Vec<Join>,
    },
}
```

### 3. Parsing Implementation

#### Enhanced WHERE Clause Parsing
- `parse_enhanced_where_clause()`: Replaces simple WHERE parsing with subquery-aware parsing
- `try_parse_subquery_condition()`: Detects and parses various subquery patterns
- `parse_exists_condition()`: Handles EXISTS/NOT EXISTS patterns
- `parse_in_condition()`: Handles IN/NOT IN patterns
- `parse_any_all_condition()`: Handles ANY/ALL quantified comparisons
- `parse_scalar_subquery_condition()`: Handles scalar subquery comparisons

#### Parentheses Handling
- `extract_parenthesized_subquery()`: Safely extracts SQL from parentheses, handling nested parentheses
- `find_subquery_end_position()`: Finds the end position of parenthesized subqueries

#### SELECT Clause Extensions
- `parse_select_clause_with_subqueries()`: Handles scalar subqueries in SELECT lists
- `split_select_items()`: Safely splits SELECT items respecting parentheses and quotes

#### FROM Clause Extensions
- `parse_derived_table()`: Parses subqueries used as tables
- `parse_derived_table_with_joins()`: Handles derived tables with JOINs

### 4. Execution Implementation

#### Subquery Execution
- `execute_subquery()`: Main subquery execution with caching for non-correlated subqueries
- `execute_correlated_subquery()`: Handles correlated subqueries with outer row context
- `execute_derived_table()`: Executes subqueries in FROM clauses and creates proper result sets

#### WHERE Condition Evaluation
- `evaluate_where_conditions()`: Evaluates both simple and subquery conditions
- `evaluate_subquery_condition()`: Dispatches to specific subquery evaluation methods
- `evaluate_in_subquery()`: Implements IN/NOT IN logic
- `evaluate_exists_subquery()`: Implements EXISTS/NOT EXISTS logic
- `evaluate_comparison_subquery()`: Implements ANY/ALL and scalar comparisons

#### Scalar Subquery Support
- `execute_scalar_subquery()`: Executes scalar subqueries and returns single values

### 5. Optimization Features

#### Subquery Caching
Non-correlated subqueries are cached to avoid repeated execution:

```rust
pub struct QueryExecutor<'a> {
    // ... other fields
    subquery_cache: Arc<Mutex<HashMap<String, QueryResult>>>,
}
```

- Subqueries are cached by their SQL text
- Only non-correlated subqueries are cached
- Cache is maintained per executor instance

#### Recursive Execution Handling
- Used `Box::pin()` to handle recursive async function calls
- Proper future boxing prevents infinite recursion compilation errors

### 6. Integration with Existing Features

The subquery implementation integrates seamlessly with existing DriftDB features:

- **JOINs**: Subqueries work within JOIN conditions and can reference joined tables
- **Aggregations**: Scalar subqueries can use aggregate functions
- **WHERE clauses**: Subqueries can be combined with regular WHERE conditions using AND/OR
- **Time travel**: Subqueries inherit temporal context from outer queries
- **Indexes**: Subquery execution benefits from existing index optimizations

## Supported Subquery Patterns

### 1. IN/NOT IN Subqueries
```sql
-- Find users who have placed orders
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);

-- Find users who have not placed orders
SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders);
```

### 2. EXISTS/NOT EXISTS Subqueries
```sql
-- Find users who have completed orders
SELECT * FROM users WHERE EXISTS (
    SELECT 1 FROM orders WHERE user_id = users.id AND status = 'completed'
);

-- Find users who have no completed orders
SELECT * FROM users WHERE NOT EXISTS (
    SELECT 1 FROM orders WHERE user_id = users.id AND status = 'completed'
);
```

### 3. Scalar Subqueries in SELECT
```sql
-- Get user name and their order count
SELECT
    name,
    (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
FROM users;
```

### 4. ANY/ALL Comparisons
```sql
-- Find products cheaper than any order
SELECT * FROM products WHERE price < ANY (SELECT amount FROM orders);

-- Find products more expensive than all orders
SELECT * FROM products WHERE price > ALL (SELECT amount FROM orders);
```

### 5. Derived Tables (FROM Subqueries)
```sql
-- Select from active users subquery
SELECT * FROM (
    SELECT * FROM users WHERE status = 'active'
) AS active_users WHERE age > 25;

-- Derived table with JOINs
SELECT u.name, o.amount
FROM (SELECT * FROM users WHERE age > 25) AS u
JOIN orders o ON u.id = o.user_id;
```

### 6. Complex Nested Subqueries
```sql
-- Find users whose order amount is above average
SELECT name FROM users WHERE id IN (
    SELECT user_id FROM orders WHERE amount > (
        SELECT AVG(amount) FROM orders
    )
);
```

### 7. Correlated Subqueries
```sql
-- Users with above-average order amounts (for their orders)
SELECT name FROM users u WHERE (
    SELECT AVG(amount) FROM orders o WHERE o.user_id = u.id
) > (
    SELECT AVG(amount) FROM orders
);
```

## Performance Considerations

### 1. Caching Strategy
- Non-correlated subqueries are cached after first execution
- Cache key is the subquery SQL text
- Significant performance improvement for repeated subquery execution

### 2. Execution Order
- Non-correlated subqueries are executed once and cached
- Correlated subqueries are executed for each outer row
- EXISTS subqueries short-circuit on first match

### 3. Memory Management
- Subquery results are properly cleaned up after use
- Caching uses reasonable memory limits
- Large result sets are handled efficiently

## Error Handling

The implementation includes comprehensive error handling for:

- Malformed subquery syntax
- Type mismatches in comparisons
- Scalar subqueries returning multiple values
- Unmatched parentheses
- Correlation resolution errors

## Future Enhancements

Potential areas for future improvement:

1. **Advanced Correlation Detection**: More sophisticated parsing to detect correlated columns
2. **Subquery Rewriting**: Transform correlated subqueries to JOINs where possible
3. **Predicate Pushdown**: Push WHERE conditions into subqueries for optimization
4. **Materialization Strategies**: Better handling of large subquery results
5. **Query Planning**: Cost-based optimization for subquery execution order

## Testing

Comprehensive test cases cover:
- All subquery types and patterns
- Edge cases like empty result sets
- Error conditions
- Performance scenarios
- Integration with existing features

The implementation has been designed to be robust, performant, and compatible with standard SQL semantics while leveraging DriftDB's unique capabilities like time travel and append-only storage.