# Join Optimizer Hook - PostgreSQL Extension

A PostgreSQL extension that hooks into the query planner to provide statistics-based join order optimization. It collects runtime statistics and uses them to make better join ordering decisions.

## Features

- **Join Search Hook**: Intercepts PostgreSQL's join planning to apply custom optimization
- **Automatic Statistics Collection**: ExecutorEnd hook automatically captures join statistics from EXPLAIN ANALYZE queries
- **Adaptive Learning**: Improves join order decisions over time based on actual execution results
- **Multiple Algorithms**: Supports both greedy and dynamic programming approaches
- **Distributed System Support**: Handles CustomScan nodes for distributed database systems (e.g., Citus)
- **Configurable**: Extensive GUC parameters to tune behavior

## Project Structure

```
join-optimizer-hook/
├── include/
│   └── join_optimizer.h      # Main header with type definitions
├── src/
│   ├── join_optimizer_main.c # Module init, hooks, GUC setup
│   ├── stats.c               # Statistics loading/saving
│   ├── stats_collector.c     # ExecutorEnd hook for auto stats collection
│   ├── algorithms.c          # Greedy and DP algorithms
│   ├── cost.c                # Cost estimation functions
│   ├── paths.c               # Path generation
│   └── utils.c               # Utility functions
├── sql/
│   ├── join_optimizer--1.0.sql    # Installation SQL
│   └── join_optimizer--uninstall.sql
├── regression/
│   ├── sql/                  # Regression test SQL files
│   │   └── join_optimizer_regtest.sql
│   ├── expected/             # Expected test outputs
│   │   └── join_optimizer_regtest.out
│   ├── results/              # Actual test outputs (generated)
│   ├── regress_schedule      # Test schedule file
│   ├── benchmark_setup.sql   # Benchmark data setup
│   └── benchmark_safe.sql    # Benchmark timing script
├── Makefile
├── join_optimizer.control
└── README.md
```

## How Statistics Loading Works

The extension maintains join statistics in shared memory for fast access during query planning:

### 1. Storage Layer (SQL Tables)

Statistics are persisted in the `join_optimizer` schema:
- `join_stats` - Join pair statistics (selectivity, row counts, costs)
- `table_stats` - Per-table statistics
- `query_history` - Execution history for analysis

### 2. Loading Process (`jo_load_stats_from_table`)

When the join search hook is invoked:

```
┌─────────────────────────────────────────────────────────────┐
│                  Statistics Loading Flow                     │
├─────────────────────────────────────────────────────────────┤
│  1. Hook triggered (join_optimizer_hook)                    │
│         │                                                   │
│         ▼                                                   │
│  2. SPI_connect() - Connect to database                     │
│         │                                                   │
│         ▼                                                   │
│  3. Execute SQL query on join_optimizer.join_stats          │
│     - Ordered by execution_count (most used first)          │
│     - Limited to 1000 entries                               │
│         │                                                   │
│         ▼                                                   │
│  4. Acquire exclusive lock on shared memory                 │
│         │                                                   │
│         ▼                                                   │
│  5. Populate jo_shmem->stats[] array                        │
│     - left_relid, right_relid (table OIDs)                  │
│     - selectivity (actual/estimated ratio)                  │
│     - execution_count (usage frequency)                     │
│         │                                                   │
│         ▼                                                   │
│  6. Release lock, SPI_finish()                              │
└─────────────────────────────────────────────────────────────┘
```

### 3. Statistics Lookup (`jo_get_join_stats`)

During cost estimation, the optimizer queries shared memory:

```c
JoinPairStats *stats = jo_get_join_stats(relid1, relid2);
if (stats != NULL && stats->execution_count > 0) {
    selectivity = stats->selectivity;  // Use learned value
}
```

The lookup is symmetric: `(A,B)` matches statistics stored as `(B,A)`.

### 4. Statistics Updates

Statistics are updated via SQL functions after query execution:
```sql
SELECT join_optimizer.update_join_stats(
    'orders'::regclass, 'customers'::regclass,
    'customer_id', 'id',
    estimated_rows, actual_rows, cost
);
```

The `update_join_stats` function uses upsert logic with running averages to smooth out variance.

## Requirements

- PostgreSQL 14 - 17 (tested on PostgreSQL 17.2)
- PostgreSQL development headers and PGXS build system
- C compiler (gcc or clang)
- `pg_config` in your PATH (or specify via `PG_CONFIG` variable)

## Building

```bash
# Using PGXS (default)
make
make install

# With custom PostgreSQL installation
make PG_CONFIG=/path/to/your/pg_config
make PG_CONFIG=/path/to/your/pg_config install

# Example with PostgreSQL 17.2 built from source
make PG_CONFIG=/usr/local/pgsql/bin/pg_config
make PG_CONFIG=/usr/local/pgsql/bin/pg_config install

# Clean build
make clean && make
```

## Installation

### Step 1: Configure shared_preload_libraries

The extension requires shared memory, so it must be loaded at server startup.

Edit your `postgresql.conf`:

```ini
# Add to postgresql.conf
shared_preload_libraries = 'join_optimizer'

# Optional: pre-configure settings
join_optimizer.enabled = on
join_optimizer.debug = off
```

### Step 2: Restart PostgreSQL

```bash
# Using pg_ctl
pg_ctl restart -D /path/to/data/directory

# Or using systemctl (Linux)
sudo systemctl restart postgresql

# Or on macOS with Homebrew
brew services restart postgresql
```

### Step 3: Create the Extension

```sql
-- Connect to your database
psql -d your_database

-- Create the extension
CREATE EXTENSION join_optimizer;

-- Verify installation
\dx join_optimizer

-- Enable the optimizer (if not auto-enabled)
SELECT join_optimizer.enable();
```

## Configuration

The extension provides several GUC parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `join_optimizer.enabled` | `on` | Enable/disable the optimizer hook |
| `join_optimizer.min_tables` | `3` | Minimum tables required to trigger optimization |
| `join_optimizer.cost_threshold` | `1000.0` | Cost threshold for using learned statistics |
| `join_optimizer.collect_stats` | `on` | Enable statistics collection |
| `join_optimizer.use_dp` | `on` | Use dynamic programming algorithm |
| `join_optimizer.dp_limit` | `12` | Maximum tables for DP algorithm |
| `join_optimizer.debug` | `off` | Enable debug output |

### Setting Parameters

```sql
-- Enable debug mode
SET join_optimizer.debug = on;

-- Change minimum tables threshold
SET join_optimizer.min_tables = 4;

-- Use greedy algorithm only
SET join_optimizer.use_dp = off;
```

## Statistics Tables

The extension creates the following tables in the `join_optimizer` schema:

### join_optimizer.join_stats

Stores statistics about join operations:

| Column | Type | Description |
|--------|------|-------------|
| `left_table` | `regclass` | Left table in the join |
| `right_table` | `regclass` | Right table in the join |
| `join_column_left` | `name` | Join column from left table |
| `join_column_right` | `name` | Join column from right table |
| `estimated_rows` | `bigint` | Planner's row estimate |
| `actual_rows` | `bigint` | Actual rows returned |
| `selectivity` | `double precision` | Calculated join selectivity |
| `join_cost` | `double precision` | Execution cost |
| `execution_count` | `bigint` | Number of executions |

### join_optimizer.table_stats

Stores per-table statistics:

| Column | Type | Description |
|--------|------|-------------|
| `table_oid` | `regclass` | Table OID |
| `row_count` | `bigint` | Number of rows |
| `avg_row_width` | `integer` | Average row width |
| `n_distinct_values` | `jsonb` | Distinct values per column |

### join_optimizer.query_history

Stores query execution history:

| Column | Type | Description |
|--------|------|-------------|
| `query_hash` | `bigint` | Hash of the query |
| `query_text` | `text` | Original query text |
| `join_order` | `text[]` | Join order used |
| `total_cost` | `double precision` | Total execution cost |
| `actual_time_ms` | `double precision` | Actual execution time |

## Usage Examples

### Monitoring Statistics

```sql
-- View join statistics summary
SELECT * FROM join_optimizer.stats_summary;

-- Check estimation accuracy
SELECT 
    left_table,
    right_table,
    estimated_rows,
    actual_rows,
    estimation_error_pct
FROM join_optimizer.stats_summary
WHERE estimation_error_pct > 50
ORDER BY execution_count DESC;
```

### Updating Statistics

```sql
-- Manually update join statistics
SELECT join_optimizer.update_join_stats(
    'orders'::regclass,
    'customers'::regclass,
    'customer_id',
    'id',
    1000,    -- estimated rows
    1234,    -- actual rows
    150.5    -- join cost
);

-- Refresh table statistics
SELECT join_optimizer.refresh_table_stats('orders'::regclass);
```

### Get Join Order Suggestions

```sql
-- Get suggested join order for a set of tables
SELECT * FROM join_optimizer.suggest_join_order(
    ARRAY['orders', 'customers', 'products', 'order_items']::regclass[]
);
```

### Managing the Extension

```sql
-- Enable optimizer
SELECT join_optimizer.enable();

-- Disable optimizer  
SELECT join_optimizer.disable();

-- Clear all statistics
SELECT join_optimizer.clear_stats();
```

## How It Works

### Join Search Hook

The extension installs a `join_search_hook` that intercepts PostgreSQL's join planning phase:

1. **Statistics Loading**: When planning starts, the hook loads cached statistics from shared memory
2. **Algorithm Selection**: Based on the number of tables, it selects either DP or greedy algorithm
3. **Cost Estimation**: Uses learned selectivity values to estimate join costs
4. **Order Generation**: Produces an optimized join order
5. **Path Generation**: Hands off to the standard planner for path generation

### Dynamic Programming Algorithm

For queries with fewer than `dp_limit` tables (default 12):

- Builds optimal join order bottom-up
- Considers all possible ways to partition tables
- Uses learned statistics for cost estimation
- Time complexity: O(3^n) where n is number of tables

### Greedy Algorithm

For larger queries:

- Starts with the smallest table
- Iteratively adds the table with lowest join cost
- Falls back to standard planner if no viable joins found
- Time complexity: O(n^2)

### Statistics Collection

The extension collects statistics automatically through the ExecutorEnd hook:

1. **ExecutorEnd Hook**: Intercepts query execution completion and walks the plan tree
2. **Join Node Detection**: Identifies HashJoin, MergeJoin, and NestLoop nodes
3. **CustomScan Support**: Handles distributed system CustomScan nodes with Bitmapset relation IDs
4. **Instrumentation Extraction**: Captures actual rows, execution time, and loops from plan instrumentation
5. **Persistent Storage**: Stores statistics in `join_optimizer.join_stats` via SPI

```
┌─────────────────────────────────────────────────────────────┐
│           Automatic Statistics Collection Flow               │
├─────────────────────────────────────────────────────────────┤
│  1. Query executes with EXPLAIN ANALYZE                     │
│         │                                                   │
│         ▼                                                   │
│  2. ExecutorEnd_hook triggered (jo_executor_end_hook)       │
│         │                                                   │
│         ▼                                                   │
│  3. Walk plan tree (jo_walk_plan_tree)                      │
│     - Identify join nodes (Hash/Merge/NestLoop)             │
│     - Identify CustomScan nodes (distributed systems)       │
│         │                                                   │
│         ▼                                                   │
│  4. Extract instrumentation data                            │
│     - actual_rows = ntuples * nloops                        │
│     - actual_time_ms = total_time                           │
│         │                                                   │
│         ▼                                                   │
│  5. Record statistics (jo_record_join_stats)                │
│     - SPI_connect() → INSERT/UPDATE → SPI_finish()          │
└─────────────────────────────────────────────────────────────┘
```

## Distributed System Support (CustomScan)

The extension supports distributed database systems that use CustomScan nodes (e.g., Citus, pg_shard):

- **CustomScan Detection**: Identifies CustomScan nodes in the plan tree
- **Bitmapset Iteration**: Extracts relation IDs from `custom_relids` using `bms_next_member()`
- **Multi-relation Joins**: Records statistics when CustomScan involves 2+ relations

This enables the optimizer to learn from distributed query execution patterns.

## Performance Considerations

- Enable `join_optimizer.debug` to monitor optimization decisions
- Adjust `min_tables` to avoid overhead on small queries
- Use `dp_limit` to balance optimization quality vs planning time
- Statistics are cached in shared memory for fast access
- Run representative queries initially to warm up the statistics cache

## Troubleshooting

### Extension not activating

```sql
-- Check if extension is loaded in shared_preload_libraries
SHOW shared_preload_libraries;
-- Should show: join_optimizer

-- Verify GUC settings
SHOW join_optimizer.enabled;
```

If `shared_preload_libraries` doesn't show `join_optimizer`, you need to:
1. Edit `postgresql.conf` and add `shared_preload_libraries = 'join_optimizer'`
2. Restart PostgreSQL (reload is not sufficient for shared libraries)

### Extension fails to load on startup

Check the PostgreSQL log for errors:

```bash
# View recent logs
tail -100 /path/to/postgresql/log/postgresql.log
```

Common issues:
- **Library not found**: Ensure `make install` was run with the correct `PG_CONFIG`
- **Symbol errors**: Version mismatch between extension and PostgreSQL

### No statistics being collected

```sql
-- Verify collection is enabled
SHOW join_optimizer.collect_stats;

-- Check if stats table exists and has data
SELECT count(*) FROM join_optimizer.join_stats;

-- View collected statistics
SELECT * FROM join_optimizer.stats_summary;
```

### Optimizer makes poor decisions

```sql
-- Enable debug mode to see decisions
SET join_optimizer.debug = on;

-- Run your query to see optimization trace
EXPLAIN (ANALYZE, VERBOSE) SELECT ...;

-- Clear old statistics and let it relearn
SELECT join_optimizer.clear_stats();
```

### Build errors

**Missing pg_config**: Ensure PostgreSQL is installed and `pg_config` is in your PATH, or specify it explicitly:
```bash
make PG_CONFIG=/path/to/pg_config
```

**Missing headers**: Install PostgreSQL development packages:
```bash
# Debian/Ubuntu
sudo apt-get install postgresql-server-dev-17

# RHEL/CentOS
sudo yum install postgresql17-devel

# macOS with Homebrew
brew install postgresql@17
```

## Uninstallation

### Step 1: Drop the Extension

```sql
-- Remove the extension from database
DROP EXTENSION join_optimizer CASCADE;
```

### Step 2: Remove from shared_preload_libraries

Edit `postgresql.conf` and remove `join_optimizer` from `shared_preload_libraries`.

### Step 3: Restart PostgreSQL

```bash
pg_ctl restart -D /path/to/data/directory
```

### Step 4: (Optional) Remove Library Files

```bash
# Find and remove installed files
rm $(pg_config --libdir)/join_optimizer.*
rm $(pg_config --sharedir)/extension/join_optimizer*
```

## Benchmark Results

Benchmark performed with test dataset:
- 200,000 order_items, 50,000 orders, 10,000 customers, 5,000 products, 100 categories

| Query | Tables | Without Optimizer | With Optimizer | Improvement |
|-------|--------|-------------------|----------------|-------------|
| Q1 | 2 | 15.7 ms | 1.1 ms | **92.7%** |
| Q2 | 4 | 28.9 ms | 14.9 ms | **48.5%** |
| Q3 | 4 | 42.0 ms | 35.0 ms | **16.7%** |
| Q4 | 5 | 19.7 ms | 17.9 ms | **9.2%** |
| Q5 | 3 | 7.1 ms | 6.7 ms | **5.7%** |

**Total: 113.5 ms → 75.6 ms (33.4% faster)**

See [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) for detailed analysis.

## PostgreSQL Version Compatibility

| PostgreSQL Version | Status | Notes |
|-------------------|--------|-------|
| 17.x | ✅ Supported | Uses `shmem_request_hook` |
| 16.x | ✅ Supported | Uses `shmem_request_hook` |
| 15.x | ⚠️ May require changes | Check shmem hook API |
| 14.x | ⚠️ May require changes | Check shmem hook API |
| < 14 | ❌ Not supported | Missing required APIs |

## License

This extension is released under the PostgreSQL License.

## Contributing

Contributions are welcome! Please submit issues and pull requests on the project repository.