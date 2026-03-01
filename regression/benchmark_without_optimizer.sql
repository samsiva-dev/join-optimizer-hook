-- Benchmark Queries: Without Join Optimizer
-- =========================================================================
-- Run these queries with join_optimizer.enabled = off

\echo '============================================================='
\echo 'BENCHMARK WITHOUT JOIN OPTIMIZER (join_optimizer.enabled = off)'
\echo '============================================================='

SET join_optimizer.enabled = off;
SHOW join_optimizer.enabled;

-- Clear any existing stats
TRUNCATE join_optimizer.join_stats;

\echo ''
\echo '--- Query 1: Simple 2-table join ---'
EXPLAIN ANALYZE
SELECT c.name, o.order_date, o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.city = 'New York'
LIMIT 100;

\echo ''
\echo '--- Query 2: 3-table join ---'
EXPLAIN ANALYZE
SELECT c.name, o.order_date, p.product_name, oi.quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE c.country = 'USA'
LIMIT 100;

\echo ''
\echo '--- Query 3: 4-table join with aggregation ---'
EXPLAIN ANALYZE
SELECT cat.category_name, 
       COUNT(*) as order_count,
       SUM(oi.quantity * oi.unit_price) as total_revenue
FROM categories cat
JOIN products p ON cat.category_id = p.category_id
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
GROUP BY cat.category_name
ORDER BY total_revenue DESC
LIMIT 10;

\echo ''
\echo '--- Query 4: 5-table join (full chain) ---'
EXPLAIN ANALYZE
SELECT c.name, c.city, o.order_date, p.product_name, cat.category_name, oi.quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id
WHERE o.status = 'delivered'
  AND cat.category_name LIKE 'Category_1%'
LIMIT 100;

\echo ''
\echo '--- Query 5: Complex join with subquery ---'
EXPLAIN ANALYZE
SELECT c.name, c.country, sub.order_count, sub.total_spent
FROM customers c
JOIN (
    SELECT customer_id, 
           COUNT(*) as order_count, 
           SUM(total_amount) as total_spent
    FROM orders
    GROUP BY customer_id
    HAVING COUNT(*) >= 3
) sub ON c.customer_id = sub.customer_id
ORDER BY sub.total_spent DESC
LIMIT 20;

\echo ''
\echo '--- Query 6: Self-referential pattern (orders in same month) ---'
EXPLAIN ANALYZE
SELECT o1.order_id, o2.order_id, o1.customer_id
FROM orders o1
JOIN orders o2 ON o1.customer_id = o2.customer_id
               AND o1.order_id < o2.order_id
               AND DATE_TRUNC('month', o1.order_date) = DATE_TRUNC('month', o2.order_date)
LIMIT 100;

\echo ''
\echo '--- Timing Summary Query (run 3 times for average) ---'

\timing on

-- Run Q4 three times for timing
\echo 'Run 1:'
SELECT COUNT(*) FROM (
SELECT c.name, c.city, o.order_date, p.product_name, cat.category_name, oi.quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id
WHERE o.status = 'delivered'
) sub;

\echo 'Run 2:'
SELECT COUNT(*) FROM (
SELECT c.name, c.city, o.order_date, p.product_name, cat.category_name, oi.quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id
WHERE o.status = 'delivered'
) sub;

\echo 'Run 3:'
SELECT COUNT(*) FROM (
SELECT c.name, c.city, o.order_date, p.product_name, cat.category_name, oi.quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id
WHERE o.status = 'delivered'
) sub;

\timing off
