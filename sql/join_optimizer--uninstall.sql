-- join_optimizer--1.0--uninstall.sql  
-- Uninstall script for join_optimizer extension

-- Drop all views
DROP VIEW IF EXISTS join_optimizer.stats_summary;

-- Drop all functions
DROP FUNCTION IF EXISTS join_optimizer.update_join_stats(regclass, regclass, name, name, bigint, bigint, double precision);
DROP FUNCTION IF EXISTS join_optimizer.refresh_table_stats(regclass);
DROP FUNCTION IF EXISTS join_optimizer.suggest_join_order(regclass[]);
DROP FUNCTION IF EXISTS join_optimizer.clear_stats();
DROP FUNCTION IF EXISTS join_optimizer.enable();
DROP FUNCTION IF EXISTS join_optimizer.disable();

-- Drop all tables
DROP TABLE IF EXISTS join_optimizer.query_history;
DROP TABLE IF EXISTS join_optimizer.table_stats;
DROP TABLE IF EXISTS join_optimizer.join_stats;

-- Drop schema
DROP SCHEMA IF EXISTS join_optimizer CASCADE;
