-- join_optimizer regression test
-- Verifies extension functionality

-- Create extension
DROP EXTENSION IF EXISTS join_optimizer CASCADE;
CREATE EXTENSION join_optimizer;

-- Verify schema exists
SELECT nspname FROM pg_namespace WHERE nspname = 'join_optimizer';

-- Verify tables exist
SELECT COUNT(*) >= 1 AS has_stats_table
FROM information_schema.tables 
WHERE table_schema = 'join_optimizer' AND table_name = 'join_stats';

-- Test GUC variables exist
SHOW join_optimizer.enabled;

-- Create test tables
CREATE TABLE regtest_t1 (id INT PRIMARY KEY, val TEXT);
CREATE TABLE regtest_t2 (id INT PRIMARY KEY, t1_id INT REFERENCES regtest_t1(id), data TEXT);
CREATE TABLE regtest_t3 (id INT PRIMARY KEY, t2_id INT REFERENCES regtest_t2(id), info TEXT);

-- Insert minimal test data
INSERT INTO regtest_t1 SELECT i, 'val_' || i FROM generate_series(1, 100) i;
INSERT INTO regtest_t2 SELECT i, (i % 100) + 1, 'data_' || i FROM generate_series(1, 500) i;
INSERT INTO regtest_t3 SELECT i, (i % 500) + 1, 'info_' || i FROM generate_series(1, 1000) i;

-- Analyze
ANALYZE regtest_t1;
ANALYZE regtest_t2;
ANALYZE regtest_t3;

-- Test enable/disable functions
SELECT join_optimizer.enable();
SELECT join_optimizer.disable();
SELECT join_optimizer.enable();

-- Test with optimizer enabled
SET join_optimizer.enabled = on;

-- Run a 3-table join (should trigger optimizer)
SELECT COUNT(*) AS join_count
FROM regtest_t1 t1
JOIN regtest_t2 t2 ON t1.id = t2.t1_id
JOIN regtest_t3 t3 ON t2.id = t3.t2_id;

-- Test with optimizer disabled
SET join_optimizer.enabled = off;

SELECT COUNT(*) AS join_count_disabled
FROM regtest_t1 t1
JOIN regtest_t2 t2 ON t1.id = t2.t1_id
JOIN regtest_t3 t3 ON t2.id = t3.t2_id;

-- Check clear_stats function
SELECT join_optimizer.clear_stats();

-- Cleanup
DROP TABLE regtest_t3 CASCADE;
DROP TABLE regtest_t2 CASCADE;
DROP TABLE regtest_t1 CASCADE;

-- Verify cleanup
SELECT COUNT(*) AS remaining_test_tables
FROM information_schema.tables 
WHERE table_name LIKE 'regtest_%';
