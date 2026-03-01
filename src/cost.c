/*-------------------------------------------------------------------------
 *
 * cost.c
 *      Cost estimation functions for join optimizer
 *
 * This file implements cost estimation using learned statistics
 * and PostgreSQL's built-in cost model as a baseline.
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"
#include "parser/parsetree.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include <math.h>

/*-------------------------------------------------------------------------
 * jo_estimate_join_cost
 *      Estimate the cost of joining two relations
 *
 * Uses learned selectivity from the stats cache if available,
 * otherwise falls back to a default selectivity estimate.
 * The cost model considers:
 * - Startup cost (fixed overhead)
 * - Outer relation scan cost
 * - Join processing cost (proportional to result size)
 * - Intermediate result size penalty
 *-------------------------------------------------------------------------
 */
double
jo_estimate_join_cost(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
{
    double      rows1 = rel1->rows;
    double      rows2 = rel2->rows;
    double      selectivity = 0.1;  /* Default: 10% */
    double      cost;
    double      result_rows;
    Oid         relid1 = 0, relid2 = 0;
    JoinPairStats *stats;
    RangeTblEntry *rte;

    /* Get relation OIDs if these are base relations */
    if (rel1->reloptkind == RELOPT_BASEREL && rel1->relid > 0)
    {
        rte = planner_rt_fetch(rel1->relid, root);
        if (rte->rtekind == RTE_RELATION)
            relid1 = rte->relid;
    }
    
    if (rel2->reloptkind == RELOPT_BASEREL && rel2->relid > 0)
    {
        rte = planner_rt_fetch(rel2->relid, root);
        if (rte->rtekind == RTE_RELATION)
            relid2 = rte->relid;
    }

    /* Try to get learned selectivity from stats cache */
    if (relid1 != 0 && relid2 != 0)
    {
        stats = jo_get_join_stats(relid1, relid2);
        
        if (stats != NULL && stats->execution_count > 0)
        {
            selectivity = stats->selectivity;
            
            /* Clamp to reasonable bounds */
            if (selectivity < 0.0001)
                selectivity = 0.0001;
            if (selectivity > 1.0)
                selectivity = 1.0;
            
            if (join_optimizer_debug)
                elog(NOTICE, "join_optimizer: using learned selectivity %.6f for %u-%u "
                     "(based on %ld executions)",
                     selectivity, relid1, relid2, (long)stats->execution_count);
        }
    }

    /* Estimate result cardinality */
    result_rows = rows1 * rows2 * selectivity;
    
    /* Clamp to at least 1 row */
    if (result_rows < 1.0)
        result_rows = 1.0;

    /*
     * Cost model breakdown:
     * - Base startup cost: 100.0 (hash table construction, etc.)
     * - Outer scan: proportional to outer relation size
     * - Inner scan: depends on join method (assuming hash join)
     * - Join processing: proportional to output rows
     * - Memory penalty for large intermediates
     */
    
    /* Startup cost */
    cost = 100.0;
    
    /* Outer relation scan cost */
    cost += rows1 * cpu_tuple_cost;
    
    /* Inner relation scan cost (hash join: build + probe) */
    cost += rows2 * cpu_tuple_cost;      /* Build hash table */
    cost += rows1 * cpu_operator_cost;    /* Probe hash table */
    
    /* Join processing (output tuples) */
    cost += result_rows * cpu_tuple_cost;
    
    /* 
     * Penalty for large intermediate results.
     * This encourages earlier filtering and smaller intermediates.
     */
    cost += result_rows * 0.001;
    
    /*
     * Sort order preference: slightly prefer keeping sorted order
     * if both inputs might be sorted on the join key.
     * (This is a simplification - real implementation would check pathkeys)
     */

    return cost;
}

/*-------------------------------------------------------------------------
 * jo_estimate_join_rows
 *      Estimate the number of rows from a join
 *
 * multiplies the cardinalities by the selectivity estimate.
 *-------------------------------------------------------------------------
 */
int64
jo_estimate_join_rows(RelOptInfo *rel1, RelOptInfo *rel2, double selectivity)
{
    double rows;
    
    rows = rel1->rows * rel2->rows * selectivity;
    
    /* Ensure we return at least 1 row */
    if (rows < 1.0)
        rows = 1.0;
    
    /* Clamp to int64 range */
    if (rows > (double)INT64_MAX)
        rows = (double)INT64_MAX;
    
    return (int64) rows;
}

/*-------------------------------------------------------------------------
 * jo_estimate_hash_join_cost
 *      Detailed cost estimate for hash join
 *
 * Internal helper for more precise hash join costing.
 *-------------------------------------------------------------------------
 */
static double
jo_estimate_hash_join_cost(double outer_rows, double inner_rows, 
                            double result_rows, int inner_width)
{
    double      cost = 0.0;
    double      hash_table_size;
    int         num_batches;
    
    /* Hash table size estimation */
    hash_table_size = inner_rows * inner_width;
    
    /* Estimate number of batches needed */
    num_batches = 1;
    if (hash_table_size > work_mem * 1024L)
    {
        num_batches = (int) ceil(hash_table_size / (work_mem * 1024L));
    }
    
    /* Build phase cost */
    cost += inner_rows * cpu_tuple_cost;
    cost += inner_rows * cpu_operator_cost;  /* Hashing */
    
    /* Probe phase cost */
    cost += outer_rows * cpu_tuple_cost;
    cost += outer_rows * cpu_operator_cost;  /* Hash lookup */
    
    /* Multi-batch penalty */
    if (num_batches > 1)
    {
        /* I/O cost for writing and reading batches */
        cost += (inner_rows + outer_rows) * seq_page_cost * 0.1;
    }
    
    /* Output cost */
    cost += result_rows * cpu_tuple_cost;
    
    return cost;
}

/*-------------------------------------------------------------------------
 * jo_estimate_merge_join_cost
 *      Detailed cost estimate for merge join
 *
 * Internal helper for merge join costing when inputs are sorted.
 *-------------------------------------------------------------------------
 */
static double
jo_estimate_merge_join_cost(double outer_rows, double inner_rows,
                             double result_rows, bool outer_sorted,
                             bool inner_sorted)
{
    double      cost = 0.0;
    
    /* Sort cost if not already sorted */
    if (!outer_sorted && outer_rows > 1)
    {
        cost += outer_rows * log2(outer_rows) * cpu_operator_cost;
    }
    
    if (!inner_sorted && inner_rows > 1)
    {
        cost += inner_rows * log2(inner_rows) * cpu_operator_cost;
    }
    
    /* Merge cost - linear scan of both inputs */
    cost += (outer_rows + inner_rows) * cpu_tuple_cost;
    cost += (outer_rows + inner_rows) * cpu_operator_cost;  /* Comparison */
    
    /* Output cost */
    cost += result_rows * cpu_tuple_cost;
    
    return cost;
}

/*-------------------------------------------------------------------------
 * jo_estimate_nestloop_cost
 *      Detailed cost estimate for nested loop join
 *
 * Internal helper for nested loop costing.
 *-------------------------------------------------------------------------
 */
static double
jo_estimate_nestloop_cost(double outer_rows, double inner_rows,
                           double result_rows, bool has_index)
{
    double      cost = 0.0;
    double      inner_scan_cost;
    
    if (has_index && inner_rows > 1)
    {
        /* Index lookup cost per outer row */
        inner_scan_cost = log2(inner_rows) * cpu_operator_cost;
    }
    else
    {
        /* Full scan of inner for each outer row */
        inner_scan_cost = inner_rows * cpu_tuple_cost;
    }
    
    /* Total nested loop cost */
    cost += outer_rows * inner_scan_cost;
    cost += outer_rows * cpu_tuple_cost;  /* Process outer tuples */
    
    /* Output cost */
    cost += result_rows * cpu_tuple_cost;
    
    return cost;
}
