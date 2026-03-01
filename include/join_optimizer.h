/*-------------------------------------------------------------------------
 *
 * join_optimizer.h
 *      Header file for join optimizer hook extension
 *
 * This extension hooks into PostgreSQL's query planner to provide
 * statistics-based join ordering optimization.
 *
 *-------------------------------------------------------------------------
 */

#ifndef JOIN_OPTIMIZER_H
#define JOIN_OPTIMIZER_H

#include "postgres.h"
#include "fmgr.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/geqo.h"
#include "optimizer/cost.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "catalog/pg_class.h"
#include "access/htup_details.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "utils/guc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "miscadmin.h"
#include "pgstat.h"

/* Configuration parameters */
typedef struct JoinOptimizerConfig
{
    bool        enabled;            /* Is the optimizer hook enabled? */
    int         min_tables;         /* Minimum tables to trigger optimization */
    double      cost_threshold;     /* Cost threshold for using stats */
    bool        collect_stats;      /* Should we collect execution stats? */
    bool        use_dp;             /* Use dynamic programming algorithm */
    int         dp_table_limit;     /* Max tables for DP algorithm */
    bool        debug_mode;         /* Enable debug output */
} JoinOptimizerConfig;

/* Statistics for a single join pair */
typedef struct JoinPairStats
{
    Oid         left_relid;         /* OID of left relation */
    Oid         right_relid;        /* OID of right relation */
    char        left_col[NAMEDATALEN];
    char        right_col[NAMEDATALEN];
    int64       estimated_rows;     /* Estimated result rows */
    int64       actual_rows;        /* Actual result rows */
    double      selectivity;        /* Join selectivity */
    double      avg_cost;           /* Average execution cost */
    int64       execution_count;    /* Number of executions */
} JoinPairStats;

/* Statistics for a table */
typedef struct TableStats
{
    Oid         relid;              /* Relation OID */
    int64       row_count;          /* Number of rows */
    int         avg_width;          /* Average tuple width */
    double      seq_scan_cost;      /* Sequential scan cost */
    double      idx_scan_cost;      /* Index scan cost (if available) */
} TableStats;

/* Join order candidate */
typedef struct JoinOrderCandidate
{
    List       *tables;             /* List of relation OIDs in order */
    double      total_cost;         /* Estimated total cost */
    double      total_rows;         /* Estimated total rows */
} JoinOrderCandidate;

/* Shared memory structure for stats cache */
#define JOIN_OPTIMIZER_STATS_SIZE 1024

typedef struct JoinOptimizerShmemStruct
{
    LWLock     *lock;
    int         num_entries;
    JoinPairStats stats[JOIN_OPTIMIZER_STATS_SIZE];
} JoinOptimizerShmemStruct;

/* Function declarations */

/* Initialization */
extern void _PG_init(void);
extern void _PG_fini(void);

/* Hook functions */
extern RelOptInfo *join_optimizer_hook(PlannerInfo *root,
                                       int levels_needed,
                                       List *initial_rels);

extern void join_optimizer_set_rel_pathlist_hook(PlannerInfo *root,
                                                  RelOptInfo *rel,
                                                  Index rti,
                                                  RangeTblEntry *rte);

/* Statistics functions */
extern void jo_load_stats_from_table(void);
extern void jo_save_stats_to_table(void);
extern JoinPairStats *jo_get_join_stats(Oid left_rel, Oid right_rel);
extern TableStats *jo_get_table_stats(Oid relid);
extern void jo_update_join_stats(Oid left_rel, Oid right_rel,
                                  const char *left_col, const char *right_col,
                                  int64 estimated, int64 actual, double cost);

/* Join ordering algorithms */
extern List *jo_optimize_join_order_greedy(PlannerInfo *root, List *joinlist);
extern List *jo_optimize_join_order_dp(PlannerInfo *root, List *joinlist);
extern double jo_estimate_join_cost(PlannerInfo *root, 
                                     RelOptInfo *rel1, 
                                     RelOptInfo *rel2);

/* Path generation */
extern void jo_create_join_paths(PlannerInfo *root,
                                  RelOptInfo *joinrel,
                                  RelOptInfo *outerrel,
                                  RelOptInfo *innerrel,
                                  JoinType jointype);

/* Utility functions */
extern double jo_get_selectivity(Oid left_rel, Oid right_rel,
                                  const char *left_col, const char *right_col);
extern int64 jo_estimate_join_rows(RelOptInfo *rel1, RelOptInfo *rel2,
                                    double selectivity);
extern bool jo_tables_can_join(RelOptInfo *rel1, RelOptInfo *rel2,
                                List *joininfos);

/* GUC variables */
extern bool join_optimizer_enabled;
extern int join_optimizer_min_tables;
extern double join_optimizer_cost_threshold;
extern bool join_optimizer_collect_stats;
extern bool join_optimizer_use_dp;
extern int join_optimizer_dp_limit;
extern bool join_optimizer_debug;

/* Shared memory pointer */
extern JoinOptimizerShmemStruct *jo_shmem;

/* Executor hook functions (stats_collector.c) */
extern void jo_install_executor_hook(void);
extern void jo_uninstall_executor_hook(void);

/* Shared memory initialization */
extern void jo_ensure_shmem_init(void);

#endif /* JOIN_OPTIMIZER_H */
