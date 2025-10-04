-- Test subquery support in DriftDB
-- This file contains comprehensive subquery test cases

-- Test setup: Create tables for testing
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    status VARCHAR(20)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    price DECIMAL(10,2)
);

-- Insert test data
INSERT INTO users VALUES (1, 'Alice', 25, 'active');
INSERT INTO users VALUES (2, 'Bob', 30, 'active');
INSERT INTO users VALUES (3, 'Charlie', 35, 'inactive');

INSERT INTO orders VALUES (1, 1, 100.00, 'completed');
INSERT INTO orders VALUES (2, 1, 150.00, 'pending');
INSERT INTO orders VALUES (3, 2, 200.00, 'completed');

INSERT INTO products VALUES (1, 'Widget A', 50.00);
INSERT INTO products VALUES (2, 'Widget B', 75.00);
INSERT INTO products VALUES (3, 'Widget C', 100.00);

-- Test 1: IN subquery
-- Find users who have placed orders
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders);

-- Test 2: NOT IN subquery
-- Find users who have not placed orders
SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders);

-- Test 3: EXISTS subquery
-- Find users who have completed orders
SELECT name FROM users WHERE EXISTS (
    SELECT 1 FROM orders WHERE user_id = users.id AND status = 'completed'
);

-- Test 4: NOT EXISTS subquery
-- Find users who have no completed orders
SELECT name FROM users WHERE NOT EXISTS (
    SELECT 1 FROM orders WHERE user_id = users.id AND status = 'completed'
);

-- Test 5: Scalar subquery in SELECT
-- Get user name and their order count
SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
FROM users;

-- Test 6: ANY comparison
-- Find products cheaper than any order
SELECT name FROM products WHERE price < ANY (SELECT amount FROM orders);

-- Test 7: ALL comparison
-- Find products more expensive than all orders
SELECT name FROM products WHERE price > ALL (SELECT amount FROM orders);

-- Test 8: Derived table (subquery in FROM)
-- Select from active users subquery
SELECT * FROM (SELECT * FROM users WHERE status = 'active') AS active_users;

-- Test 9: Complex nested subquery
-- Find users whose order amount is above average
SELECT name FROM users WHERE id IN (
    SELECT user_id FROM orders WHERE amount > (
        SELECT AVG(amount) FROM orders
    )
);

-- Test 10: Correlated subquery
-- Users with above-average order amounts (for their orders)
SELECT name FROM users u WHERE (
    SELECT AVG(amount) FROM orders o WHERE o.user_id = u.id
) > (
    SELECT AVG(amount) FROM orders
);