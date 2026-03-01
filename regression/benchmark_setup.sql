-- Test Setup: Create tables and load data for join optimizer benchmarking
-- =========================================================================

-- Ensure extension is loaded
CREATE EXTENSION IF NOT EXISTS join_optimizer;

-- Drop existing test tables if they exist
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

-- Create test tables with various sizes
-- =========================================================================

-- Small table: categories (100 rows)
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100),
    description TEXT
);

-- Medium table: customers (10,000 rows)
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Medium table: products (5,000 rows)
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category_id INTEGER REFERENCES categories(category_id),
    price NUMERIC(10,2),
    stock_quantity INTEGER
);

-- Large table: orders (50,000 rows)
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE,
    total_amount NUMERIC(12,2),
    status VARCHAR(20)
);

-- Large table: order_items (200,000 rows)
CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER,
    unit_price NUMERIC(10,2)
);

-- Load test data
-- =========================================================================

-- Categories (100 rows)
INSERT INTO categories (category_name, description)
SELECT 
    'Category_' || i,
    'Description for category ' || i
FROM generate_series(1, 100) AS i;

-- Customers (10,000 rows)
INSERT INTO customers (name, email, city, country)
SELECT 
    'Customer_' || i,
    'customer' || i || '@example.com',
    (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin'])[1 + (i % 10)],
    (ARRAY['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'Mexico'])[1 + (i % 10)]
FROM generate_series(1, 10000) AS i;

-- Products (5,000 rows)
INSERT INTO products (product_name, category_id, price, stock_quantity)
SELECT 
    'Product_' || i,
    1 + (i % 100),
    (random() * 1000)::NUMERIC(10,2),
    (random() * 1000)::INTEGER
FROM generate_series(1, 5000) AS i;

-- Orders (50,000 rows)
INSERT INTO orders (customer_id, order_date, total_amount, status)
SELECT 
    1 + (i % 10000),
    CURRENT_DATE - (random() * 365)::INTEGER,
    (random() * 5000)::NUMERIC(12,2),
    (ARRAY['pending', 'processing', 'shipped', 'delivered', 'cancelled'])[1 + (i % 5)]
FROM generate_series(1, 50000) AS i;

-- Order items (200,000 rows)
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT 
    1 + (i % 50000),
    1 + (i % 5000),
    1 + (random() * 10)::INTEGER,
    (random() * 500)::NUMERIC(10,2)
FROM generate_series(1, 200000) AS i;

-- Create indexes for realistic scenario
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_customers_city ON customers(city);

-- Analyze tables for accurate statistics
ANALYZE categories;
ANALYZE customers;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;

-- Show table sizes
SELECT 
    relname as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables 
WHERE schemaname = 'public'
ORDER BY n_live_tup DESC;
