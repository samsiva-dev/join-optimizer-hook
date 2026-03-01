/*-------------------------------------------------------------------------
 *
 * join_optimizer_main.c
 *      Main module initialization and hook installation
 *
 * This file contains the module entry points (_PG_init, _PG_fini),
 * GUC variable definitions, shared memory setup, and hook wrappers.
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"

PG_MODULE_MAGIC;

/*-------------------------------------------------------------------------
 * GUC variables
 *-------------------------------------------------------------------------
 */

bool        join_optimizer_enabled = true;
int         join_optimizer_min_tables = 3;
double      join_optimizer_cost_threshold = 1000.0;
bool        join_optimizer_collect_stats = true;
bool        join_optimizer_use_dp = true;
int         join_optimizer_dp_limit = 12;
bool        join_optimizer_debug = false;

/*-------------------------------------------------------------------------
 * Saved hook values
 *-------------------------------------------------------------------------
 */

static join_search_hook_type prev_join_search_hook = NULL;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/*-------------------------------------------------------------------------
 * Shared memory
 *-------------------------------------------------------------------------
 */

JoinOptimizerShmemStruct *jo_shmem = NULL;

/*-------------------------------------------------------------------------
 * Local declarations
 *-------------------------------------------------------------------------
 */

static void jo_shmem_request(void);
static void jo_shmem_init(void);
static Size jo_shmem_size(void);
static void jo_init_guc(void);
static PlannedStmt *jo_planner_hook(Query *parse, const char *query_string,
                                     int cursorOptions, ParamListInfo boundParams);

/*-------------------------------------------------------------------------
 * _PG_init
 *      Module initialization function
 *-------------------------------------------------------------------------
 */
void
_PG_init(void)
{
    /* Initialize GUC variables */
    jo_init_guc();

    /* Install hooks */
    prev_join_search_hook = join_search_hook;
    join_search_hook = join_optimizer_hook;

    prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = join_optimizer_set_rel_pathlist_hook;

    prev_planner_hook = planner_hook;
    planner_hook = jo_planner_hook;

    /* Register shared memory request hook (PG15+) */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = jo_shmem_request;

    /* Install executor hook for automatic stats collection */
    jo_install_executor_hook();

    elog(LOG, "join_optimizer: extension loaded");
}

/*-------------------------------------------------------------------------
 * _PG_fini
 *      Module unload function
 *-------------------------------------------------------------------------
 */
void
_PG_fini(void)
{
    /* Restore hooks */
    join_search_hook = prev_join_search_hook;
    set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
    planner_hook = prev_planner_hook;
    shmem_request_hook = prev_shmem_request_hook;
    jo_uninstall_executor_hook();

    elog(LOG, "join_optimizer: extension unloaded");
}

/*-------------------------------------------------------------------------
 * jo_init_guc
 *      Initialize GUC variables
 *-------------------------------------------------------------------------
 */
static void
jo_init_guc(void)
{
    DefineCustomBoolVariable("join_optimizer.enabled",
                             "Enable the join optimizer hook",
                             NULL,
                             &join_optimizer_enabled,
                             true,
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("join_optimizer.min_tables",
                            "Minimum number of tables to trigger optimization",
                            NULL,
                            &join_optimizer_min_tables,
                            3,
                            2, 100,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomRealVariable("join_optimizer.cost_threshold",
                             "Cost threshold for using learned statistics",
                             NULL,
                             &join_optimizer_cost_threshold,
                             1000.0,
                             0.0, 1e12,
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomBoolVariable("join_optimizer.collect_stats",
                             "Collect execution statistics",
                             NULL,
                             &join_optimizer_collect_stats,
                             true,
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomBoolVariable("join_optimizer.use_dp",
                             "Use dynamic programming for join ordering",
                             NULL,
                             &join_optimizer_use_dp,
                             true,
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("join_optimizer.dp_limit",
                            "Maximum tables for DP algorithm",
                            NULL,
                            &join_optimizer_dp_limit,
                            12,
                            2, 20,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("join_optimizer.debug",
                             "Enable debug output",
                             NULL,
                             &join_optimizer_debug,
                             false,
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);

    MarkGUCPrefixReserved("join_optimizer");
}

/*-------------------------------------------------------------------------
 * Shared memory functions
 *-------------------------------------------------------------------------
 */

static Size
jo_shmem_size(void)
{
    return sizeof(JoinOptimizerShmemStruct);
}

/*
 * jo_shmem_request - Request shared memory space
 * Called during postmaster startup via shmem_request_hook
 */
static void
jo_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(jo_shmem_size());
    RequestNamedLWLockTranche("join_optimizer", 1);
}

/*
 * jo_shmem_init - Initialize shared memory segment
 * Called on first access after shared memory is available
 */
static void
jo_shmem_init(void)
{
    bool        found;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    jo_shmem = ShmemInitStruct("join_optimizer",
                               jo_shmem_size(),
                               &found);

    if (!found)
    {
        /* First time, initialize the structure */
        memset(jo_shmem, 0, jo_shmem_size());
        jo_shmem->lock = &(GetNamedLWLockTranche("join_optimizer"))->lock;
        jo_shmem->num_entries = 0;
    }

    LWLockRelease(AddinShmemInitLock);
}

/*-------------------------------------------------------------------------
 * join_optimizer_hook
 *      Main join search hook - intercepts join planning
 *-------------------------------------------------------------------------
 */
RelOptInfo *
join_optimizer_hook(PlannerInfo *root,
                    int levels_needed,
                    List *initial_rels)
{
    int         num_rels = list_length(initial_rels);
    List       *optimal_order;

    /* Check if optimization is enabled */
    if (!join_optimizer_enabled)
    {
        if (prev_join_search_hook)
            return prev_join_search_hook(root, levels_needed, initial_rels);
        else
            return standard_join_search(root, levels_needed, initial_rels);
    }

    /* Check minimum tables threshold */
    if (num_rels < join_optimizer_min_tables)
    {
        if (prev_join_search_hook)
            return prev_join_search_hook(root, levels_needed, initial_rels);
        else
            return standard_join_search(root, levels_needed, initial_rels);
    }

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: optimizing join for %d tables", num_rels);

    /* Load statistics from tables if available */
    jo_load_stats_from_table();

    /* Choose optimization algorithm based on number of tables */
    if (join_optimizer_use_dp && num_rels <= join_optimizer_dp_limit)
    {
        /* Use dynamic programming for smaller joins */
        optimal_order = jo_optimize_join_order_dp(root, initial_rels);
    }
    else
    {
        /* Use greedy algorithm for larger joins */
        optimal_order = jo_optimize_join_order_greedy(root, initial_rels);
    }

    if (join_optimizer_debug && optimal_order != NIL)
        elog(NOTICE, "join_optimizer: computed optimal order with %d tables", 
             list_length(optimal_order));

    /* Fall back to standard planning - let it use our hints */
    if (prev_join_search_hook)
        return prev_join_search_hook(root, levels_needed, initial_rels);
    else
        return standard_join_search(root, levels_needed, initial_rels);
}

/*-------------------------------------------------------------------------
 * join_optimizer_set_rel_pathlist_hook
 *      Set pathlist hook - can add additional paths
 *-------------------------------------------------------------------------
 */
void
join_optimizer_set_rel_pathlist_hook(PlannerInfo *root,
                                      RelOptInfo *rel,
                                      Index rti,
                                      RangeTblEntry *rte)
{
    /* Call previous hook first */
    if (prev_set_rel_pathlist_hook)
        prev_set_rel_pathlist_hook(root, rel, rti, rte);

    /* We could add additional path generation here if needed */
}

/*-------------------------------------------------------------------------
 * jo_planner_hook
 *      Planner hook - wraps the entire planning process
 *-------------------------------------------------------------------------
 */
static PlannedStmt *
jo_planner_hook(Query *parse, const char *query_string,
                int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt *result;
    instr_time  start_time;
    instr_time  end_time;
    double      planning_time;

    INSTR_TIME_SET_CURRENT(start_time);

    /* Call the standard planner (or previous hook) */
    if (prev_planner_hook)
        result = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    else
        result = standard_planner(parse, query_string, cursorOptions, boundParams);

    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    planning_time = INSTR_TIME_GET_MILLISEC(end_time);

    if (join_optimizer_debug && planning_time > 10.0)
        elog(NOTICE, "join_optimizer: planning took %.2f ms", planning_time);

    return result;
}

/*-------------------------------------------------------------------------
 * SQL-callable functions
 *-------------------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(join_optimizer_status);
PG_FUNCTION_INFO_V1(join_optimizer_refresh_stats);
PG_FUNCTION_INFO_V1(join_optimizer_get_stats_count);

Datum
join_optimizer_status(PG_FUNCTION_ARGS)
{
    char       *status;
    
    status = psprintf("Join Optimizer Hook\n"
                      "Enabled: %s\n"
                      "Min Tables: %d\n"
                      "Cost Threshold: %.2f\n"
                      "Collect Stats: %s\n"
                      "Use DP: %s\n"
                      "DP Limit: %d\n"
                      "Debug: %s\n"
                      "Cached Stats: %d",
                      join_optimizer_enabled ? "yes" : "no",
                      join_optimizer_min_tables,
                      join_optimizer_cost_threshold,
                      join_optimizer_collect_stats ? "yes" : "no",
                      join_optimizer_use_dp ? "yes" : "no",
                      join_optimizer_dp_limit,
                      join_optimizer_debug ? "yes" : "no",
                      jo_shmem ? jo_shmem->num_entries : 0);

    PG_RETURN_TEXT_P(cstring_to_text(status));
}

Datum
join_optimizer_refresh_stats(PG_FUNCTION_ARGS)
{
    jo_load_stats_from_table();
    PG_RETURN_VOID();
}

Datum
join_optimizer_get_stats_count(PG_FUNCTION_ARGS)
{
    if (jo_shmem)
        PG_RETURN_INT32(jo_shmem->num_entries);
    else
        PG_RETURN_INT32(0);
}
