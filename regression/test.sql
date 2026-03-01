-- join_optimizer test script
-- Run with: psql -d testdb -f test.sql

\echo 'Testing join_optimizer extension...'

-- Create extension
DROP EXTENSION IF EXISTS join_optimizer CASCADE;
CREATE EXTENSION join_optimizer;

\echo 'Extension created successfully'

-- Test table creation
SELECT COUNT(*) AS stats_tables 
FROM information_schema.tables 
WHERE table_schema = 'join_optimizer';

-- Create test tables
DROP TABLE IF EXISTS test_orders CASCADE;
DROP TABLE IF EXISTS test_customers CASCADE;
DROP TABLE IF EXISTS test_products CASCADE;
DROP TABLE IF EXISTS test_order_items CASCADE;

CREATE TABLE test_customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE test_products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    category VARCHAR(50)
);

CREATE TABLE test_orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES test_customers(id),
    order_date DATE DEFAULT CURRENT_DATE,
    total DECIMAL(10,2)
);

CREATE TABLE test_order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES test_orders(id),
    product_id INTEGER REFERENCES test_products(id),
    quantity INTEGER,
    price DECIMAL(10,2)
);

\echo 'Test tables created'

-- Insert test data
INSERT INTO test_customers (name, email)
SELECT 
    'Customer ' || i,
    'customer' || i || '@test.com'
FROM generate_series(1, 1000) i;

INSERT INTO test_products (name, price, category)
SELECT 
    'Product ' || i,
    (random() * 100)::DECIMAL(10,2),
    CASE (i % 5) 
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        ELSE 'Other'
    END
FROM generate_series(1, 500) i;

INSERT INTO test_orders (customer_id, order_date, total)
SELECT 
    (random() * 999 + 1)::INTEGER,
    CURRENT_DATE - (random() * 365)::INTEGER,
    (random() * 500)::DECIMAL(10,2)
FROM generate_series(1, 5000) i;

INSERT INTO test_order_items (order_id, product_id, quantity, price)
SELECT 
    (random() * 4999 + 1)::INTEGER,
    (random() * 499 + 1)::INTEGER,
    (random() * 10 + 1)::INTEGER,
    (random() * 50)::DECIMAL(10,2)
FROM generate_series(1, 20000) i;

\echo 'Test data inserted'

-- Analyze tables
ANALYZE test_customers;
ANALYZE test_products;
ANALYZE test_orders;
ANALYZE test_order_items;

-- Refresh table stats
SELECT join_optimizer.refresh_table_stats('test_customers'::regclass);
SELECT join_optimizer.refresh_table_stats('test_products'::regclass);
SELECT join_optimizer.refresh_table_stats('test_orders'::regclass);
SELECT join_optimizer.refresh_table_stats('test_order_items'::regclass);

\echo 'Statistics refreshed'

-- Test GUC settings
SET join_optimizer.enabled = on;
SET join_optimizer.debug = on;
SET join_optimizer.min_tables = 3;

-- Run a multi-table join query
EXPLAIN ANALYZE
SELECT 
    c.name as customer_name,
    o.order_date,
    p.name as product_name,
    oi.quantity,
    oi.price
FROM test_customers c
JOIN test_orders o ON c.id = o.customer_id
JOIN test_order_items oi ON o.id = oi.order_id
JOIN test_products p ON oi.product_id = p.id
WHERE c.id < 100
LIMIT 100;

-- Update join statistics manually
SELECT join_optimizer.update_join_stats(
    'test_customers'::regclass,
    'test_orders'::regclass,
    'id',
    'customer_id',
    500,
    450,
    25.5
);

SELECT join_optimizer.update_join_stats(
    'test_orders'::regclass,
    'test_order_items'::regclass,
    'id',
    'order_id',
    2000,
    1800,
    45.0
);

-- Check stats
\echo 'Join statistics:'
SELECT * FROM join_optimizer.stats_summary;

-- Test join order suggestion
\echo 'Suggested join order:'
SELECT * FROM join_optimizer.suggest_join_order(
    ARRAY['test_customers', 'test_orders', 'test_products', 'test_order_items']::regclass[]
);

-- Check table stats
\echo 'Table statistics:'
SELECT * FROM join_optimizer.table_stats;

-- Disable debug and run query again
SET join_optimizer.debug = off;

EXPLAIN ANALYZE
SELECT 
    c.name as customer_name,
    COUNT(DISTINCT o.id) as order_count,
    SUM(oi.quantity * oi.price) as total_spent
FROM test_customers c
JOIN test_orders o ON c.id = o.customer_id
JOIN test_order_items oi ON o.id = oi.order_id
JOIN test_products p ON oi.product_id = p.id
GROUP BY c.id, c.name
ORDER BY total_spent DESC
LIMIT 10;

-- Cleanup
DROP TABLE IF EXISTS test_order_items CASCADE;
DROP TABLE IF EXISTS test_orders CASCADE;
DROP TABLE IF EXISTS test_products CASCADE;
DROP TABLE IF EXISTS test_customers CASCADE;

\echo 'Test completed successfully!'
