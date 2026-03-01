/*-------------------------------------------------------------------------
 *
 * paths.c
 *      Path generation functions for join optimizer
 *
 * This file handles creating access paths using the optimizer's
 * learned statistics and preferences.
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "parser/parsetree.h"

/*-------------------------------------------------------------------------
 * jo_create_join_paths
 *      Create join paths using learned statistics
 *
 * This function can add custom paths to a join relation based on
 * the statistics we've collected. Currently, it relies on PostgreSQL's
 * standard path generation but could be extended to:
 * - Force specific join methods based on learned performance
 * - Adjust path costs based on actual execution times
 * - Add parameterized paths for known-good access patterns
 *-------------------------------------------------------------------------
 */
void
jo_create_join_paths(PlannerInfo *root,
                      RelOptInfo *joinrel,
                      RelOptInfo *outerrel,
                      RelOptInfo *innerrel,
                      JoinType jointype)
{
    Oid         outer_relid = 0;
    Oid         inner_relid = 0;
    JoinPairStats *stats;

    /* Get relation OIDs for stats lookup */
    if (outerrel->reloptkind == RELOPT_BASEREL && outerrel->relid > 0)
    {
        RangeTblEntry *rte = planner_rt_fetch(outerrel->relid, root);
        if (rte->rtekind == RTE_RELATION)
            outer_relid = rte->relid;
    }
    
    if (innerrel->reloptkind == RELOPT_BASEREL && innerrel->relid > 0)
    {
        RangeTblEntry *rte = planner_rt_fetch(innerrel->relid, root);
        if (rte->rtekind == RTE_RELATION)
            inner_relid = rte->relid;
    }

    /* Look up statistics for this join pair */
    if (outer_relid != 0 && inner_relid != 0)
    {
        stats = jo_get_join_stats(outer_relid, inner_relid);
        
        if (stats != NULL && stats->execution_count > 0)
        {
            /*
             * We have statistics for this join. We could use them to:
             * 
             * 1. Adjust row estimates on existing paths
             *    - Loop through joinrel->pathlist and adjust rows
             *    
             * 2. Prefer certain join methods based on past performance
             *    - If hash join was fast, ensure it's generated
             *    - If merge join was better, add merge join path
             *
             * 3. Create parameterized paths
             *    - If inner is small, add nested loop with index
             *
             * For now, we just log that we have stats available.
             */
            if (join_optimizer_debug)
            {
                elog(NOTICE, "join_optimizer: stats available for %u-%u "
                     "(selectivity=%.6f, avg_cost=%.2f, executions=%ld)",
                     outer_relid, inner_relid,
                     stats->selectivity, stats->avg_cost,
                     (long)stats->execution_count);
            }
        }
    }

    /*
     * The standard path creation is handled by PostgreSQL's normal
     * path generation process. This hook is called after paths are
     * already created, so we can modify existing paths or add new ones.
     */
}

/*-------------------------------------------------------------------------
 * jo_adjust_path_rows
 *      Adjust row estimate on a path based on learned statistics
 *
 * Internal helper to update path->rows based on actual execution data.
 *-------------------------------------------------------------------------
 */
static void
jo_adjust_path_rows(Path *path, double actual_selectivity)
{
    /*
     * This would adjust the path's row estimate based on learned
     * selectivity. Care must be taken not to invalidate PostgreSQL's
     * internal assumptions about path costs.
     *
     * For now, we leave this as a placeholder.
     */
}

/*-------------------------------------------------------------------------
 * jo_should_use_hash_join
 *      Determine if hash join is preferred based on statistics
 *
 * Returns true if hash join was historically faster for this pair.
 *-------------------------------------------------------------------------
 */
static bool
jo_should_use_hash_join(Oid outer_relid, Oid inner_relid, 
                         double outer_rows, double inner_rows)
{
    JoinPairStats *stats = jo_get_join_stats(outer_relid, inner_relid);
    
    if (stats == NULL)
        return true;  /* Default to hash join */
    
    /*
     * Heuristics for hash join preference:
     * - Inner relation fits in work_mem
     * - High selectivity (many matches per probe)
     * - No useful sort order needed
     */
    if (inner_rows * 100 < work_mem * 1024L)
        return true;  /* Inner fits in memory */
    
    return true;  /* Default */
}

/*-------------------------------------------------------------------------
 * jo_should_use_merge_join
 *      Determine if merge join is preferred based on statistics
 *
 * Returns true if merge join would be beneficial.
 *-------------------------------------------------------------------------
 */
static bool
jo_should_use_merge_join(Oid outer_relid, Oid inner_relid,
                          bool outer_sorted, bool inner_sorted)
{
    /*
     * Prefer merge join when:
     * - Both inputs are already sorted on join key
     * - Output needs to be sorted
     * - Very large inputs where hash table would spill
     */
    if (outer_sorted && inner_sorted)
        return true;
    
    return false;
}

/*-------------------------------------------------------------------------
 * jo_should_use_nestloop
 *      Determine if nested loop is preferred based on statistics
 *
 * Returns true if nested loop would be beneficial.
 *-------------------------------------------------------------------------
 */
static bool
jo_should_use_nestloop(Oid outer_relid, Oid inner_relid,
                        double outer_rows, bool has_inner_index)
{
    /*
     * Prefer nested loop when:
     * - Outer is very small
     * - Inner has an index on the join key
     * - LIMIT is present (parameterized path)
     */
    if (outer_rows < 100 && has_inner_index)
        return true;
    
    return false;
}
