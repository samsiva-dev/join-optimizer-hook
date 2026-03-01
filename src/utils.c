/*-------------------------------------------------------------------------
 *
 * utils.c
 *      Utility functions for join optimizer
 *
 * General-purpose helper functions used throughout the extension.
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"
#include "nodes/bitmapset.h"
#include "utils/syscache.h"
#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "parser/parsetree.h"

/*-------------------------------------------------------------------------
 * jo_tables_can_join
 *      Check if two relations can be joined directly
 *
 * Examines the join info list to determine if there is a valid
 * join predicate between the two relations. Also handles the case
 * of cross joins between base relations.
 *
 * Returns true if:
 * - There is an explicit join condition in joininfos
 * - Both are base relations (allowing cross joins)
 *-------------------------------------------------------------------------
 */
bool
jo_tables_can_join(RelOptInfo *rel1, RelOptInfo *rel2, List *joininfos)
{
    ListCell   *lc;
    Relids      rel1_relids = rel1->relids;
    Relids      rel2_relids = rel2->relids;

    /* Check for explicit join conditions in SpecialJoinInfo list */
    foreach(lc, joininfos)
    {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);

        /* Check if rel1 matches left side and rel2 matches right side */
        if (bms_overlap(rel1_relids, sjinfo->syn_lefthand) &&
            bms_overlap(rel2_relids, sjinfo->syn_righthand))
            return true;
        
        /* Also check the reverse */
        if (bms_overlap(rel2_relids, sjinfo->syn_lefthand) &&
            bms_overlap(rel1_relids, sjinfo->syn_righthand))
            return true;
    }

    /* 
     * For simple joins (inner joins without special semantics),
     * allow joining any two base relations. The join conditions
     * will be enforced via the restrictinfo list.
     */
    if (rel1->reloptkind == RELOPT_BASEREL && 
        rel2->reloptkind == RELOPT_BASEREL)
        return true;

    /*
     * For join relations, check if their combined relids
     * form a valid join (neither is a subset of the other)
     */
    if (!bms_overlap(rel1_relids, rel2_relids))
    {
        /* No overlap - these can potentially be joined */
        return true;
    }

    return false;
}

/*-------------------------------------------------------------------------
 * jo_get_relation_name
 *      Get the name of a relation for debugging output
 *
 * Returns palloced string with the relation name or a placeholder.
 *-------------------------------------------------------------------------
 */
static char *
jo_get_relation_name(PlannerInfo *root, RelOptInfo *rel)
{
    if (rel->reloptkind == RELOPT_BASEREL && rel->relid > 0)
    {
        RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
        
        if (rte->rtekind == RTE_RELATION)
        {
            return get_rel_name(rte->relid);
        }
        else if (rte->alias && rte->alias->aliasname)
        {
            return pstrdup(rte->alias->aliasname);
        }
    }
    
    /* Return a placeholder for join relations or unknown */
    return psprintf("rel_%d", bms_singleton_member(rel->relids));
}

/*-------------------------------------------------------------------------
 * jo_print_join_order
 *      Print the join order for debugging
 *
 * Outputs the list of table names in join order to the log.
 *-------------------------------------------------------------------------
 */
static void
jo_print_join_order(PlannerInfo *root, List *join_order, const char *label)
{
    StringInfoData buf;
    ListCell   *lc;
    bool        first = true;

    if (!join_optimizer_debug)
        return;

    initStringInfo(&buf);
    appendStringInfo(&buf, "join_optimizer %s: [", label);

    foreach(lc, join_order)
    {
        RelOptInfo *rel = (RelOptInfo *) lfirst(lc);
        char *name = jo_get_relation_name(root, rel);
        
        if (!first)
            appendStringInfoString(&buf, ", ");
        appendStringInfoString(&buf, name);
        first = false;
        
        pfree(name);
    }
    
    appendStringInfoChar(&buf, ']');
    
    elog(NOTICE, "%s", buf.data);
    pfree(buf.data);
}

/*-------------------------------------------------------------------------
 * jo_relids_to_string
 *      Convert a Relids bitmap to a string representation
 *
 * Returns a palloced string like "{1,3,5}" for debugging.
 *-------------------------------------------------------------------------
 */
static char *
jo_relids_to_string(Relids relids)
{
    StringInfoData buf;
    int         x = -1;
    bool        first = true;

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '{');
    
    while ((x = bms_next_member(relids, x)) >= 0)
    {
        if (!first)
            appendStringInfoChar(&buf, ',');
        appendStringInfo(&buf, "%d", x);
        first = false;
    }
    
    appendStringInfoChar(&buf, '}');
    
    return buf.data;
}

/*-------------------------------------------------------------------------
 * jo_is_foreign_key_join
 *      Check if a join is along a foreign key relationship
 *
 * Foreign key joins typically have selectivity close to 1/|referenced|
 * and can be optimized accordingly.
 *-------------------------------------------------------------------------
 */
static bool
jo_is_foreign_key_join(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
{
    /* 
     * This would check pg_constraint for foreign key relationships
     * between the two relations. For now, return false.
     */
    return false;
}

/*-------------------------------------------------------------------------
 * jo_estimate_distinct_values
 *      Estimate the number of distinct values in a column
 *
 * Uses PostgreSQL's statistics if available.
 *-------------------------------------------------------------------------
 */
static double
jo_estimate_distinct_values(Oid relid, const char *attname)
{
    /* 
     * Could query pg_statistic for n_distinct.
     * For now, return -1 to indicate unknown.
     */
    return -1.0;
}

/*-------------------------------------------------------------------------
 * jo_get_table_size
 *      Get the estimated size of a table in bytes
 *
 * Uses pg_class statistics.
 *-------------------------------------------------------------------------
 */
static int64
jo_get_table_size(Oid relid)
{
    HeapTuple   tuple;
    Form_pg_class classform;
    int64       size = 0;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tuple))
    {
        classform = (Form_pg_class) GETSTRUCT(tuple);
        size = (int64)classform->relpages * BLCKSZ;
        ReleaseSysCache(tuple);
    }

    return size;
}
