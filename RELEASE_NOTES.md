# Release Notes - Join Optimizer Hook v1.0

**Release Date:** March 1, 2026  
**PostgreSQL Compatibility:** 17.x

---

## Overview

Initial release of the Join Optimizer Hook extension for PostgreSQL. This extension hooks into the query planner to provide statistics-based join order optimization, automatically learning from query execution to improve future join ordering decisions.

---

## Features

### Core Functionality

- **Join Search Hook** - Intercepts PostgreSQL's native join planning to apply custom optimization strategies
- **Automatic Statistics Collection** - ExecutorEnd hook captures join statistics from `EXPLAIN ANALYZE` queries without manual intervention
- **Adaptive Learning** - Improves join order decisions over time based on actual execution results
- **Shared Memory Cache** - Cross-connection statistics sharing for consistent optimization across sessions

### Optimization Algorithms

- **Dynamic Programming** - Optimal join ordering for queries with up to 12 tables (configurable via `dp_threshold`)
- **Greedy Algorithm** - Fast heuristic-based ordering for larger queries up to 20 tables (configurable via `greedy_threshold`)
- **Auto Mode** - Automatically selects the best algorithm based on query complexity

### Distributed System Support

- Handles CustomScan nodes for distributed database systems (e.g., Citus)
- Compatible with distributed query execution patterns

### SQL Functions

| Function | Description |
|----------|-------------|
| `join_optimizer.update_join_stats()` | Update join statistics after query execution |
| `join_optimizer.refresh_table_stats()` | Refresh per-table statistics |
| `join_optimizer.suggest_join_order()` | Get optimal join order suggestion for a set of tables |
| `join_optimizer.clear_stats()` | Clear all collected statistics |
| `join_optimizer.get_stats_summary()` | View current statistics summary |

### GUC Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `join_optimizer.enabled` | `on` | Enable/disable the optimizer |
| `join_optimizer.algorithm` | `auto` | Algorithm selection (`auto`, `dp`, `greedy`) |
| `join_optimizer.dp_threshold` | `12` | Max tables for dynamic programming |
| `join_optimizer.greedy_threshold` | `20` | Max tables for greedy algorithm |

---

## Performance

Benchmarks show significant query improvements with cached statistics:

| Query Type | Improvement |
|------------|-------------|
| 2-table join | **92.7%** faster |
| 3-table join | **48.5%** faster |
| 4-table aggregation | **16.7%** faster |
| 5-table chain | **9.2%** faster |
| Complex aggregation | **5.7%** faster |

*Results vary based on data distribution and query patterns. Greatest improvements observed after statistics have been collected from prior executions.*

---

## Installation

```bash
# Build and install
make
make install

# Enable in PostgreSQL
psql -c "CREATE EXTENSION join_optimizer;"
```

---

## Schema Objects Created

- **Schema:** `join_optimizer`
- **Tables:**
  - `join_optimizer.join_stats` - Join pair statistics
  - `join_optimizer.table_stats` - Per-table statistics  
  - `join_optimizer.query_history` - Execution history

---

## Known Limitations

- Statistics are best collected via `EXPLAIN ANALYZE` queries
- Initial queries before statistics collection use PostgreSQL's default planner
- Maximum of 1000 statistics entries cached in shared memory

---

## Testing

The extension includes a PGXS-compatible regression test framework:

```bash
make installcheck
```

---

## Contributors

Initial release developed for PostgreSQL 17.

---

## License

See LICENSE file for details.
