-- Safe Benchmark: Timing only (no EXPLAIN ANALYZE to avoid stats collector issues)
-- =========================================================================

\echo '============================================================='
\echo 'SAFE BENCHMARK - TIMING COMPARISON'
\echo '============================================================='

\timing on

-- =========================================================================
-- PART 1: WITHOUT JOIN OPTIMIZER
-- =========================================================================
\echo ''
\echo '>>> RUNNING WITHOUT JOIN OPTIMIZER <<<<'
SET join_optimizer.enabled = off;
SHOW join_optimizer.enabled;

\echo ''
\echo '--- Query 1: 2-table join (no optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, o.order_date, o.total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.city = 'New York'
) sub;

\echo '--- Query 2: 3-table join (no optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, o.order_date, p.product_name
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE c.country = 'USA'
) sub;

\echo '--- Query 3: 4-table aggregation (no optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT cat.category_name, 
           SUM(oi.quantity * oi.unit_price) as total_revenue
    FROM categories cat
    JOIN products p ON cat.category_id = p.category_id
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    GROUP BY cat.category_name
) sub;

\echo '--- Query 4: 5-table full chain (no optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, c.city, o.order_date, p.product_name, cat.category_name
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories cat ON p.category_id = cat.category_id
    WHERE o.status = 'delivered'
) sub;

\echo '--- Query 5: Complex aggregation (no optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, c.country, sub.order_count
    FROM customers c
    JOIN (
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) >= 3
    ) sub ON c.customer_id = sub.customer_id
) sub2;

-- =========================================================================
-- PART 2: WITH JOIN OPTIMIZER
-- =========================================================================
\echo ''
\echo '>>> RUNNING WITH JOIN OPTIMIZER <<<<'
SET join_optimizer.enabled = on;
SHOW join_optimizer.enabled;

\echo ''
\echo '--- Query 1: 2-table join (with optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, o.order_date, o.total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.city = 'New York'
) sub;

\echo '--- Query 2: 3-table join (with optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, o.order_date, p.product_name
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE c.country = 'USA'
) sub;

\echo '--- Query 3: 4-table aggregation (with optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT cat.category_name, 
           SUM(oi.quantity * oi.unit_price) as total_revenue
    FROM categories cat
    JOIN products p ON cat.category_id = p.category_id
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    GROUP BY cat.category_name
) sub;

\echo '--- Query 4: 5-table full chain (with optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, c.city, o.order_date, p.product_name, cat.category_name
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories cat ON p.category_id = cat.category_id
    WHERE o.status = 'delivered'
) sub;

\echo '--- Query 5: Complex aggregation (with optimizer) ---'
SELECT COUNT(*) FROM (
    SELECT c.name, c.country, sub.order_count
    FROM customers c
    JOIN (
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) >= 3
    ) sub ON c.customer_id = sub.customer_id
) sub2;

\timing off

\echo ''
\echo '============================================================='
\echo 'BENCHMARK COMPLETE'
\echo '============================================================='
