-- join_optimizer--1.0.sql
-- Install script for join_optimizer extension

-- Create schema for our optimizer statistics
CREATE SCHEMA IF NOT EXISTS join_optimizer;

-- Table to store join statistics
CREATE TABLE IF NOT EXISTS join_optimizer.join_stats (
    id SERIAL PRIMARY KEY,
    left_table regclass NOT NULL,
    right_table regclass NOT NULL,
    join_column_left name NOT NULL,
    join_column_right name NOT NULL,
    estimated_rows bigint DEFAULT 0,
    actual_rows bigint DEFAULT 0,
    selectivity double precision DEFAULT 1.0,
    join_cost double precision DEFAULT 0.0,
    execution_count bigint DEFAULT 0,
    last_updated timestamp with time zone DEFAULT now(),
    UNIQUE(left_table, right_table, join_column_left, join_column_right)
);

-- Table to store table statistics
CREATE TABLE IF NOT EXISTS join_optimizer.table_stats (
    id SERIAL PRIMARY KEY,
    table_oid regclass NOT NULL UNIQUE,
    row_count bigint DEFAULT 0,
    avg_row_width integer DEFAULT 0,
    n_distinct_values jsonb DEFAULT '{}',
    most_common_values jsonb DEFAULT '{}',
    last_analyzed timestamp with time zone DEFAULT now()
);

-- Table to store query execution history
CREATE TABLE IF NOT EXISTS join_optimizer.query_history (
    id SERIAL PRIMARY KEY,
    query_hash bigint NOT NULL,
    query_text text,
    join_order text[],
    total_cost double precision,
    actual_time_ms double precision,
    executed_at timestamp with time zone DEFAULT now()
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_join_stats_tables 
    ON join_optimizer.join_stats(left_table, right_table);
CREATE INDEX IF NOT EXISTS idx_query_history_hash 
    ON join_optimizer.query_history(query_hash);

-- Function to update join statistics after query execution
CREATE OR REPLACE FUNCTION join_optimizer.update_join_stats(
    p_left_table regclass,
    p_right_table regclass,
    p_join_col_left name,
    p_join_col_right name,
    p_estimated_rows bigint,
    p_actual_rows bigint,
    p_join_cost double precision
) RETURNS void AS $$
DECLARE
    v_selectivity double precision;
BEGIN
    -- Calculate selectivity
    IF p_estimated_rows > 0 THEN
        v_selectivity := p_actual_rows::double precision / p_estimated_rows::double precision;
    ELSE
        v_selectivity := 1.0;
    END IF;

    INSERT INTO join_optimizer.join_stats 
        (left_table, right_table, join_column_left, join_column_right, 
         estimated_rows, actual_rows, selectivity, join_cost, execution_count)
    VALUES 
        (p_left_table, p_right_table, p_join_col_left, p_join_col_right,
         p_estimated_rows, p_actual_rows, v_selectivity, p_join_cost, 1)
    ON CONFLICT (left_table, right_table, join_column_left, join_column_right)
    DO UPDATE SET
        estimated_rows = p_estimated_rows,
        actual_rows = (join_optimizer.join_stats.actual_rows + p_actual_rows) / 2,
        selectivity = (join_optimizer.join_stats.selectivity + v_selectivity) / 2,
        join_cost = (join_optimizer.join_stats.join_cost + p_join_cost) / 2,
        execution_count = join_optimizer.join_stats.execution_count + 1,
        last_updated = now();
END;
$$ LANGUAGE plpgsql;

-- Function to refresh table statistics
CREATE OR REPLACE FUNCTION join_optimizer.refresh_table_stats(p_table regclass)
RETURNS void AS $$
DECLARE
    v_row_count bigint;
    v_avg_width integer;
BEGIN
    -- Get row count
    EXECUTE format('SELECT count(*) FROM %s', p_table) INTO v_row_count;
    
    -- Get average row width from pg_class
    SELECT avg_width INTO v_avg_width
    FROM pg_stats
    WHERE schemaname = (SELECT nspname FROM pg_namespace 
                        WHERE oid = (SELECT relnamespace FROM pg_class WHERE oid = p_table))
      AND tablename = (SELECT relname FROM pg_class WHERE oid = p_table)
    LIMIT 1;

    INSERT INTO join_optimizer.table_stats (table_oid, row_count, avg_row_width, last_analyzed)
    VALUES (p_table, v_row_count, COALESCE(v_avg_width, 0), now())
    ON CONFLICT (table_oid)
    DO UPDATE SET
        row_count = v_row_count,
        avg_row_width = COALESCE(v_avg_width, join_optimizer.table_stats.avg_row_width),
        last_analyzed = now();
END;
$$ LANGUAGE plpgsql;

-- Function to get optimal join order suggestion
CREATE OR REPLACE FUNCTION join_optimizer.suggest_join_order(p_tables regclass[])
RETURNS TABLE(join_position integer, table_name regclass, estimated_cost double precision) AS $$
DECLARE
    v_table regclass;
    v_pos integer := 1;
    v_remaining regclass[];
    v_result regclass[];
    v_min_cost double precision;
    v_best_table regclass;
    v_current_cost double precision;
BEGIN
    v_remaining := p_tables;
    
    -- Simple greedy algorithm: pick table with smallest estimated join cost
    WHILE array_length(v_remaining, 1) > 0 LOOP
        v_min_cost := 'infinity'::double precision;
        v_best_table := v_remaining[1];
        
        FOREACH v_table IN ARRAY v_remaining LOOP
            -- Get estimated cost from stats or use table size
            SELECT COALESCE(
                (SELECT AVG(join_cost) FROM join_optimizer.join_stats 
                 WHERE left_table = v_table OR right_table = v_table),
                (SELECT row_count FROM join_optimizer.table_stats WHERE table_oid = v_table),
                1000000.0
            ) INTO v_current_cost;
            
            IF v_current_cost < v_min_cost THEN
                v_min_cost := v_current_cost;
                v_best_table := v_table;
            END IF;
        END LOOP;
        
        join_position := v_pos;
        table_name := v_best_table;
        estimated_cost := v_min_cost;
        RETURN NEXT;
        
        v_remaining := array_remove(v_remaining, v_best_table);
        v_pos := v_pos + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to clear all statistics
CREATE OR REPLACE FUNCTION join_optimizer.clear_stats()
RETURNS void AS $$
BEGIN
    TRUNCATE join_optimizer.join_stats;
    TRUNCATE join_optimizer.table_stats;
    TRUNCATE join_optimizer.query_history;
END;
$$ LANGUAGE plpgsql;

-- Function to enable/disable the optimizer hook
CREATE OR REPLACE FUNCTION join_optimizer.enable()
RETURNS void AS $$
BEGIN
    PERFORM set_config('join_optimizer.enabled', 'on', false);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION join_optimizer.disable()
RETURNS void AS $$
BEGIN
    PERFORM set_config('join_optimizer.enabled', 'off', false);
END;
$$ LANGUAGE plpgsql;

-- View for monitoring join statistics
CREATE OR REPLACE VIEW join_optimizer.stats_summary AS
SELECT 
    js.left_table::text AS left_table,
    js.right_table::text AS right_table,
    js.join_column_left,
    js.join_column_right,
    js.estimated_rows,
    js.actual_rows,
    js.selectivity,
    js.join_cost,
    js.execution_count,
    js.last_updated,
    CASE 
        WHEN js.estimated_rows > 0 
        THEN round(((js.actual_rows - js.estimated_rows)::numeric / js.estimated_rows * 100), 2)
        ELSE 0
    END AS estimation_error_pct
FROM join_optimizer.join_stats js
ORDER BY js.execution_count DESC;

-- C functions exposed to SQL
CREATE OR REPLACE FUNCTION join_optimizer.status()
RETURNS text
AS 'MODULE_PATHNAME', 'join_optimizer_status'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION join_optimizer.refresh_stats_cache()
RETURNS void
AS 'MODULE_PATHNAME', 'join_optimizer_refresh_stats'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION join_optimizer.get_cached_stats_count()
RETURNS integer
AS 'MODULE_PATHNAME', 'join_optimizer_get_stats_count'
LANGUAGE C STRICT;

-- Grant permissions
GRANT USAGE ON SCHEMA join_optimizer TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA join_optimizer TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA join_optimizer TO PUBLIC;
