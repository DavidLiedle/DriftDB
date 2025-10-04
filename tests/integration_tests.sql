-- DriftDB Integration Test Suite
-- This SQL file tests core database functionality

-- Test 1: Basic table operations
CREATE TABLE test_users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO test_users (id, username, email) VALUES
    (1, 'alice', 'alice@example.com'),
    (2, 'bob', 'bob@example.com'),
    (3, 'charlie', 'charlie@example.com');

SELECT COUNT(*) AS user_count FROM test_users;

-- Test 2: Transaction support
BEGIN;
INSERT INTO test_users (id, username, email) VALUES (4, 'dave', 'dave@example.com');
UPDATE test_users SET email = 'alice_new@example.com' WHERE username = 'alice';
COMMIT;

SELECT * FROM test_users WHERE username IN ('alice', 'dave');

-- Test 3: Rollback functionality
BEGIN;
DELETE FROM test_users WHERE id = 2;
ROLLBACK;

SELECT * FROM test_users WHERE id = 2;

-- Test 4: JOINs
CREATE TABLE test_orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product VARCHAR(100),
    amount DECIMAL(10,2),
    order_date DATE
);

INSERT INTO test_orders VALUES
    (1, 1, 'Laptop', 999.99, '2024-01-15'),
    (2, 1, 'Mouse', 29.99, '2024-01-20'),
    (3, 2, 'Keyboard', 79.99, '2024-02-01'),
    (4, 3, 'Monitor', 399.99, '2024-02-15');

SELECT
    u.username,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_spent
FROM test_users u
LEFT JOIN test_orders o ON u.id = o.user_id
GROUP BY u.username
ORDER BY total_spent DESC;

-- Test 5: Subqueries
SELECT username
FROM test_users
WHERE id IN (
    SELECT user_id
    FROM test_orders
    WHERE amount > 100
);

-- Test 6: CTEs (Common Table Expressions)
WITH high_value_customers AS (
    SELECT user_id, SUM(amount) AS total
    FROM test_orders
    GROUP BY user_id
    HAVING SUM(amount) > 500
)
SELECT u.username, h.total
FROM test_users u
JOIN high_value_customers h ON u.id = h.user_id;

-- Test 7: Window functions
SELECT
    username,
    email,
    ROW_NUMBER() OVER (ORDER BY username) AS row_num,
    COUNT(*) OVER () AS total_users
FROM test_users;

-- Test 8: UNION operations
SELECT username AS name FROM test_users WHERE id <= 2
UNION
SELECT username AS name FROM test_users WHERE id >= 2
ORDER BY name;

-- Test 9: Complex aggregations
SELECT
    CASE
        WHEN COUNT(*) > 2 THEN 'Many orders'
        WHEN COUNT(*) = 2 THEN 'Some orders'
        ELSE 'Few orders'
    END AS order_category,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM test_orders
GROUP BY user_id;

-- Test 10: Time-travel queries (DriftDB specific)
SELECT * FROM test_users AS OF @seq:1;

-- Cleanup
DROP TABLE test_orders;
DROP TABLE test_users;