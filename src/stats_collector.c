/*-------------------------------------------------------------------------
 *
 * stats_collector.c
 *      Automatic statistics collection via ExecutorEnd hook
 *
 * This file implements automatic collection of join execution statistics
 * by hooking into the executor and capturing actual vs estimated row counts.
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

/* Saved hook value */
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;

/* Forward declarations */
static void jo_executor_end_hook(QueryDesc *queryDesc);
static void jo_collect_join_stats(QueryDesc *queryDesc);
static void jo_walk_plan_tree(PlanState *planstate, QueryDesc *queryDesc);
static void jo_record_join_stats(PlanState *planstate, QueryDesc *queryDesc);
static void jo_record_custom_scan_stats(PlanState *planstate, QueryDesc *queryDesc);
static Oid jo_get_relid_from_scan(Plan *plan, EState *estate);

/*-------------------------------------------------------------------------
 * jo_install_executor_hook
 *      Install the ExecutorEnd hook
 *-------------------------------------------------------------------------
 */
void
jo_install_executor_hook(void)
{
    prev_executor_end_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = jo_executor_end_hook;
}

/*-------------------------------------------------------------------------
 * jo_uninstall_executor_hook
 *      Restore original ExecutorEnd hook
 *-------------------------------------------------------------------------
 */
void
jo_uninstall_executor_hook(void)
{
    ExecutorEnd_hook = prev_executor_end_hook;
}

/*-------------------------------------------------------------------------
 * jo_executor_end_hook
 *      Main executor end hook - captures execution statistics
 *-------------------------------------------------------------------------
 */
static void
jo_executor_end_hook(QueryDesc *queryDesc)
{
    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: executor_end_hook called, collect_stats=%d, "
             "instrument_options=%d, planstate=%s",
             join_optimizer_collect_stats,
             queryDesc->instrument_options,
             queryDesc->planstate ? "yes" : "no");

    /* Only collect stats if enabled and instrumentation is available */
    if (join_optimizer_collect_stats && 
        queryDesc->instrument_options != 0 &&
        queryDesc->planstate != NULL)
    {
        PG_TRY();
        {
            jo_collect_join_stats(queryDesc);
        }
        PG_CATCH();
        {
            /* Don't let stats collection errors affect query execution */
            if (join_optimizer_debug)
                elog(WARNING, "join_optimizer: error collecting stats");
            FlushErrorState();
        }
        PG_END_TRY();
    }

    /* Call previous hook or standard executor end */
    if (prev_executor_end_hook)
        prev_executor_end_hook(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

/*-------------------------------------------------------------------------
 * jo_collect_join_stats
 *      Walk the plan tree and collect join statistics
 *-------------------------------------------------------------------------
 */
static void
jo_collect_join_stats(QueryDesc *queryDesc)
{
    if (queryDesc->planstate == NULL)
        return;

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: walking plan tree for stats collection");

    jo_walk_plan_tree(queryDesc->planstate, queryDesc);
}

/*-------------------------------------------------------------------------
 * jo_walk_plan_tree
 *      Recursively walk plan tree looking for join nodes
 *-------------------------------------------------------------------------
 */
static void
jo_walk_plan_tree(PlanState *planstate, QueryDesc *queryDesc)
{
    Plan       *plan;
    
    if (planstate == NULL)
        return;

    plan = planstate->plan;

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: visiting node type %d (%s)", 
             nodeTag(plan),
             nodeTag(plan) == T_HashJoin ? "HashJoin" :
             nodeTag(plan) == T_MergeJoin ? "MergeJoin" :
             nodeTag(plan) == T_NestLoop ? "NestLoop" :
             nodeTag(plan) == T_SeqScan ? "SeqScan" :
             nodeTag(plan) == T_IndexScan ? "IndexScan" :
             nodeTag(plan) == T_Hash ? "Hash" :
             nodeTag(plan) == T_Sort ? "Sort" :
             nodeTag(plan) == T_CustomScan ? "CustomScan" : "other");

    /* Check if this is a join node or custom scan */
    switch (nodeTag(plan))
    {
        case T_HashJoin:
        case T_MergeJoin:
        case T_NestLoop:
            jo_record_join_stats(planstate, queryDesc);
            break;
        case T_CustomScan:
            /* Handle distributed/custom scan nodes */
            jo_record_custom_scan_stats(planstate, queryDesc);
            break;
        default:
            break;
    }

    /* Walk left (outer) subtree */
    if (outerPlanState(planstate))
        jo_walk_plan_tree(outerPlanState(planstate), queryDesc);

    /* Walk right (inner) subtree */
    if (innerPlanState(planstate))
        jo_walk_plan_tree(innerPlanState(planstate), queryDesc);

    /* Handle CustomScan's custom_ps list (for distributed systems) */
    if (IsA(plan, CustomScan))
    {
        CustomScanState *css = (CustomScanState *) planstate;
        ListCell *lc;
        foreach(lc, css->custom_ps)
        {
            jo_walk_plan_tree((PlanState *) lfirst(lc), queryDesc);
        }
    }

    /* Handle Append/MergeAppend nodes */
    if (IsA(plan, Append))
    {
        AppendState *appendstate = (AppendState *) planstate;
        int i;
        for (i = 0; i < appendstate->as_nplans; i++)
        {
            jo_walk_plan_tree(appendstate->appendplans[i], queryDesc);
        }
    }
    else if (IsA(plan, MergeAppend))
    {
        MergeAppendState *mastate = (MergeAppendState *) planstate;
        int i;
        for (i = 0; i < mastate->ms_nplans; i++)
        {
            jo_walk_plan_tree(mastate->mergeplans[i], queryDesc);
        }
    }
}

/*-------------------------------------------------------------------------
 * jo_record_join_stats
 *      Record statistics for a join node
 *-------------------------------------------------------------------------
 */
static void
jo_record_join_stats(PlanState *planstate, QueryDesc *queryDesc)
{
    Plan       *plan = planstate->plan;
    Instrumentation *instrument = planstate->instrument;
    PlanState  *outer_ps = outerPlanState(planstate);
    PlanState  *inner_ps = innerPlanState(planstate);
    Oid         outer_relid = InvalidOid;
    Oid         inner_relid = InvalidOid;
    double      estimated_rows;
    double      actual_rows;
    double      cost;
    int         ret;
    char        query[1024];

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: recording join node type=%d", nodeTag(plan));

    /* Need instrumentation data */
    if (instrument == NULL)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: no instrumentation data");
        return;
    }

    /* Need both outer and inner subtrees */
    if (outer_ps == NULL || inner_ps == NULL)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: missing outer/inner planstate");
        return;
    }

    /* Get estimated and actual row counts */
    estimated_rows = plan->plan_rows;
    actual_rows = instrument->ntuples;
    cost = instrument->total * 1000.0;  /* Convert to milliseconds */

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: estimated=%.0f, actual=%.0f, nloops=%.0f",
             estimated_rows, actual_rows, instrument->nloops);

    /* Only record if we have meaningful data */
    if (actual_rows < 1 && instrument->nloops < 1)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: skipping - no meaningful data");
        return;
    }

    /* Try to identify the base relations involved */
    outer_relid = jo_get_relid_from_scan(outer_ps->plan, queryDesc->estate);
    inner_relid = jo_get_relid_from_scan(inner_ps->plan, queryDesc->estate);

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: outer_relid=%u, inner_relid=%u",
             outer_relid, inner_relid);

    /* Need both relation OIDs */
    if (outer_relid == InvalidOid || inner_relid == InvalidOid)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: could not identify base relations");
        return;
    }

    if (join_optimizer_debug)
    {
        elog(NOTICE, "join_optimizer: recording stats for join %u-%u: "
             "estimated=%.0f actual=%.0f cost=%.2f",
             outer_relid, inner_relid, estimated_rows, actual_rows, cost);
    }

    /* Update stats via SPI */
    if (SPI_connect() != SPI_OK_CONNECT)
        return;

    snprintf(query, sizeof(query),
             "INSERT INTO join_optimizer.join_stats "
             "(left_table, right_table, join_column_left, join_column_right, "
             "estimated_rows, actual_rows, selectivity, join_cost, execution_count) "
             "VALUES (%u, %u, 'auto', 'auto', %ld, %ld, "
             "CASE WHEN %ld > 0 THEN %ld::float / %ld::float ELSE 1.0 END, "
             "%.2f, 1) "
             "ON CONFLICT (left_table, right_table, join_column_left, join_column_right) "
             "DO UPDATE SET "
             "estimated_rows = %ld, "
             "actual_rows = (join_optimizer.join_stats.actual_rows + %ld) / 2, "
             "selectivity = (join_optimizer.join_stats.selectivity + "
             "CASE WHEN %ld > 0 THEN %ld::float / %ld::float ELSE 1.0 END) / 2, "
             "join_cost = (join_optimizer.join_stats.join_cost + %.2f) / 2, "
             "execution_count = join_optimizer.join_stats.execution_count + 1, "
             "last_updated = now()",
             outer_relid, inner_relid,
             (long)estimated_rows, (long)actual_rows,
             (long)estimated_rows, (long)actual_rows, (long)estimated_rows,
             cost,
             (long)estimated_rows,
             (long)actual_rows,
             (long)estimated_rows, (long)actual_rows, (long)estimated_rows,
             cost);

    ret = SPI_execute(query, false, 0);
    
    if (ret != SPI_OK_INSERT && join_optimizer_debug)
        elog(WARNING, "join_optimizer: failed to update stats: %d", ret);

    SPI_finish();
}

/*-------------------------------------------------------------------------
 * jo_record_custom_scan_stats
 *      Record statistics for a CustomScan node (distributed systems)
 *
 * CustomScan nodes are used by distributed database systems like Citus,
 * YugabyteDB, etc. We try to extract execution statistics from them.
 *-------------------------------------------------------------------------
 */
static void
jo_record_custom_scan_stats(PlanState *planstate, QueryDesc *queryDesc)
{
    Plan           *plan = planstate->plan;
    CustomScan     *cscan = (CustomScan *) plan;
    Instrumentation *instrument = planstate->instrument;
    double          estimated_rows;
    double          actual_rows;
    double          cost;
    int             ret;
    char            query[1024];
    List           *relids = NIL;
    Oid             relid1 = InvalidOid;
    Oid             relid2 = InvalidOid;
    int             rti = -1;

    if (join_optimizer_debug)
    {
        elog(NOTICE, "join_optimizer: CustomScan node, methods=%s, custom_relids count=%d",
             cscan->methods ? cscan->methods->CustomName : "unknown",
             bms_num_members(cscan->custom_relids));
    }

    /* Need instrumentation data */
    if (instrument == NULL)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: CustomScan has no instrumentation");
        return;
    }

    /* Get estimated and actual row counts */
    estimated_rows = plan->plan_rows;
    actual_rows = instrument->ntuples;
    cost = instrument->total * 1000.0;

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: CustomScan estimated=%.0f, actual=%.0f, nloops=%.0f",
             estimated_rows, actual_rows, instrument->nloops);

    /* Only record if we have meaningful data */
    if (actual_rows < 1 && instrument->nloops < 1)
        return;

    /* Try to get relation OIDs from custom_relids (Bitmapset of RT indexes) */
    while ((rti = bms_next_member(cscan->custom_relids, rti)) >= 0)
    {
        if (rti > 0 && rti <= list_length(queryDesc->estate->es_range_table))
        {
            RangeTblEntry *rte = rt_fetch(rti, queryDesc->estate->es_range_table);
            if (rte->rtekind == RTE_RELATION)
            {
                relids = lappend_oid(relids, rte->relid);
                if (relid1 == InvalidOid)
                    relid1 = rte->relid;
                else if (relid2 == InvalidOid)
                    relid2 = rte->relid;
            }
        }
    }

    /* If we found at least 2 relations, record as a join */
    if (relid1 != InvalidOid && relid2 != InvalidOid)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: recording CustomScan stats for %u-%u",
                 relid1, relid2);

        if (SPI_connect() != SPI_OK_CONNECT)
        {
            list_free(relids);
            return;
        }

        snprintf(query, sizeof(query),
                 "INSERT INTO join_optimizer.join_stats "
                 "(left_table, right_table, join_column_left, join_column_right, "
                 "estimated_rows, actual_rows, selectivity, join_cost, execution_count) "
                 "VALUES (%u, %u, 'distributed', 'distributed', %ld, %ld, "
                 "CASE WHEN %ld > 0 THEN %ld::float / %ld::float ELSE 1.0 END, "
                 "%.2f, 1) "
                 "ON CONFLICT (left_table, right_table, join_column_left, join_column_right) "
                 "DO UPDATE SET "
                 "estimated_rows = %ld, "
                 "actual_rows = (join_optimizer.join_stats.actual_rows + %ld) / 2, "
                 "selectivity = (join_optimizer.join_stats.selectivity + "
                 "CASE WHEN %ld > 0 THEN %ld::float / %ld::float ELSE 1.0 END) / 2, "
                 "join_cost = (join_optimizer.join_stats.join_cost + %.2f) / 2, "
                 "execution_count = join_optimizer.join_stats.execution_count + 1, "
                 "last_updated = now()",
                 relid1, relid2,
                 (long)estimated_rows, (long)actual_rows,
                 (long)estimated_rows, (long)actual_rows, (long)estimated_rows,
                 cost,
                 (long)estimated_rows,
                 (long)actual_rows,
                 (long)estimated_rows, (long)actual_rows, (long)estimated_rows,
                 cost);

        ret = SPI_execute(query, false, 0);
        
        if (ret != SPI_OK_INSERT && join_optimizer_debug)
            elog(WARNING, "join_optimizer: failed to update CustomScan stats: %d", ret);

        SPI_finish();
    }
    else if (list_length(relids) == 1)
    {
        /* Single table scan - could record table stats instead */
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: CustomScan on single table %u", relid1);
    }
    else
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: CustomScan has no identifiable relations");
    }

    list_free(relids);
}

/*-------------------------------------------------------------------------
 * jo_get_relid_from_scan
 *      Extract relation OID from a scan node
 *
 * Walks down the plan tree to find the base table being scanned.
 *-------------------------------------------------------------------------
 */
static Oid
jo_get_relid_from_scan(Plan *plan, EState *estate)
{
    RangeTblEntry *rte;
    Index       scanrelid;

    if (plan == NULL)
        return InvalidOid;

    switch (nodeTag(plan))
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_TidRangeScan:
            scanrelid = ((Scan *) plan)->scanrelid;
            if (scanrelid > 0 && scanrelid <= list_length(estate->es_range_table))
            {
                rte = rt_fetch(scanrelid, estate->es_range_table);
                if (rte->rtekind == RTE_RELATION)
                    return rte->relid;
            }
            break;

        case T_HashJoin:
        case T_MergeJoin:
        case T_NestLoop:
            /* For joins, recurse into outer side */
            return jo_get_relid_from_scan(outerPlan(plan), estate);

        case T_Hash:
        case T_Sort:
        case T_Material:
        case T_Memoize:
            /* Pass-through nodes: recurse into child */
            return jo_get_relid_from_scan(outerPlan(plan), estate);

        case T_BitmapAnd:
        case T_BitmapOr:
            /* Bitmap nodes: try first child */
            {
                BitmapAnd *ba = (BitmapAnd *) plan;
                if (ba->bitmapplans != NIL)
                    return jo_get_relid_from_scan(linitial(ba->bitmapplans), estate);
            }
            break;

        case T_BitmapIndexScan:
            scanrelid = ((BitmapIndexScan *) plan)->scan.scanrelid;
            if (scanrelid > 0 && scanrelid <= list_length(estate->es_range_table))
            {
                rte = rt_fetch(scanrelid, estate->es_range_table);
                if (rte->rtekind == RTE_RELATION)
                    return rte->relid;
            }
            break;

        case T_CustomScan:
            /* CustomScan: try to get relid from custom_relids or scan.scanrelid */
            {
                CustomScan *cscan = (CustomScan *) plan;
                int         first_member;
                
                /* First try scan.scanrelid */
                scanrelid = cscan->scan.scanrelid;
                if (scanrelid > 0 && scanrelid <= list_length(estate->es_range_table))
                {
                    rte = rt_fetch(scanrelid, estate->es_range_table);
                    if (rte->rtekind == RTE_RELATION)
                        return rte->relid;
                }
                
                /* Try custom_relids (Bitmapset) */
                if (!bms_is_empty(cscan->custom_relids))
                {
                    first_member = bms_next_member(cscan->custom_relids, -1);
                    if (first_member > 0 && first_member <= list_length(estate->es_range_table))
                    {
                        rte = rt_fetch(first_member, estate->es_range_table);
                        if (rte->rtekind == RTE_RELATION)
                            return rte->relid;
                    }
                }
                
                /* Try custom_plans */
                if (cscan->custom_plans != NIL)
                    return jo_get_relid_from_scan(linitial(cscan->custom_plans), estate);
            }
            break;

        default:
            /* Try recursing into outer plan for other node types */
            if (outerPlan(plan))
                return jo_get_relid_from_scan(outerPlan(plan), estate);
            break;
    }

    return InvalidOid;
}
