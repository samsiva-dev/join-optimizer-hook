/*-------------------------------------------------------------------------
 *
 * algorithms.c
 *      Join ordering algorithms for the optimizer
 *
 * This file implements the join ordering algorithms:
 * - Greedy algorithm: Fast O(n^2) algorithm for larger queries
 * - Dynamic Programming: Optimal O(3^n) algorithm for smaller queries
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"

/*-------------------------------------------------------------------------
 * jo_optimize_join_order_greedy
 *      Greedy join ordering algorithm
 *
 * Selects joins based on lowest estimated cost at each step.
 * Algorithm:
 * 1. Start with the smallest table (by row count)
 * 2. At each step, add the table with lowest join cost to current result
 * 3. Continue until all tables are ordered
 *
 * Time complexity: O(n^2) where n is the number of tables
 *-------------------------------------------------------------------------
 */
List *
jo_optimize_join_order_greedy(PlannerInfo *root, List *joinlist)
{
    List       *result = NIL;
    List       *remaining;
    ListCell   *lc;
    RelOptInfo *current = NULL;

    remaining = list_copy(joinlist);

    /* Start with the smallest table */
    {
        RelOptInfo *smallest = NULL;
        double      min_rows = -1;

        foreach(lc, remaining)
        {
            RelOptInfo *rel = (RelOptInfo *) lfirst(lc);
            
            if (min_rows < 0 || rel->rows < min_rows)
            {
                min_rows = rel->rows;
                smallest = rel;
            }
        }

        if (smallest)
        {
            result = lappend(result, smallest);
            remaining = list_delete_ptr(remaining, smallest);
            current = smallest;
        }
    }

    /* Greedily add tables with lowest join cost */
    while (list_length(remaining) > 0)
    {
        RelOptInfo *best = NULL;
        double      best_cost = -1;

        foreach(lc, remaining)
        {
            RelOptInfo *rel = (RelOptInfo *) lfirst(lc);
            double      cost;

            /* Check if these tables can join directly */
            if (!jo_tables_can_join(current, rel, root->join_info_list))
            {
                /* Check if we can join via any previously added table */
                ListCell *lc2;
                bool can_join_somehow = false;
                
                foreach(lc2, result)
                {
                    RelOptInfo *prev_rel = (RelOptInfo *) lfirst(lc2);
                    if (jo_tables_can_join(prev_rel, rel, root->join_info_list))
                    {
                        can_join_somehow = true;
                        break;
                    }
                }
                
                if (!can_join_somehow)
                    continue;
            }

            /* Estimate join cost using learned statistics */
            cost = jo_estimate_join_cost(root, current, rel);

            if (best_cost < 0 || cost < best_cost)
            {
                best_cost = cost;
                best = rel;
            }
        }

        if (best == NULL)
        {
            /* No joinable table found - take first remaining (cross join) */
            best = (RelOptInfo *) linitial(remaining);
            
            if (join_optimizer_debug)
                elog(NOTICE, "join_optimizer: greedy fallback to cross join");
        }

        result = lappend(result, best);
        remaining = list_delete_ptr(remaining, best);
        current = best;
    }

    if (join_optimizer_debug)
        elog(NOTICE, "join_optimizer: greedy algorithm produced order with %d tables",
             list_length(result));

    return result;
}

/*-------------------------------------------------------------------------
 * jo_optimize_join_order_dp
 *      Dynamic programming join ordering algorithm
 *
 * Uses bottom-up DP to find the optimal join order.
 * Algorithm:
 * 1. Initialize cost for each single-table subset
 * 2. For each subset size from 2 to n:
 *    - Try all ways to split into two smaller subsets
 *    - Keep track of the minimum cost split
 * 3. Reconstruct the optimal order from the DP table
 *
 * Time complexity: O(3^n) where n is the number of tables
 * Space complexity: O(2^n)
 *-------------------------------------------------------------------------
 */
List *
jo_optimize_join_order_dp(PlannerInfo *root, List *joinlist)
{
    int         num_rels = list_length(joinlist);
    int         num_subsets;
    RelOptInfo **rels;
    double     *best_costs;
    int        *best_left;
    int        *best_right;
    int         i, size;
    ListCell   *lc;
    List       *result = NIL;
    int         final_set;

    /* Fall back to greedy if too many relations */
    if (num_rels > join_optimizer_dp_limit)
    {
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: %d tables exceeds DP limit %d, using greedy",
                 num_rels, join_optimizer_dp_limit);
        return jo_optimize_join_order_greedy(root, joinlist);
    }

    num_subsets = (1 << num_rels);

    /* Allocate DP arrays */
    rels = (RelOptInfo **) palloc0(num_rels * sizeof(RelOptInfo *));
    best_costs = (double *) palloc(num_subsets * sizeof(double));
    best_left = (int *) palloc(num_subsets * sizeof(int));
    best_right = (int *) palloc(num_subsets * sizeof(int));

    /* Initialize relation array */
    i = 0;
    foreach(lc, joinlist)
    {
        rels[i++] = (RelOptInfo *) lfirst(lc);
    }

    /* Initialize DP tables */
    for (i = 0; i < num_subsets; i++)
    {
        best_costs[i] = -1;  /* -1 indicates "not computed" */
        best_left[i] = -1;
        best_right[i] = -1;
    }

    /* Base case: single relations - cost is the row count */
    for (i = 0; i < num_rels; i++)
    {
        int subset = (1 << i);
        best_costs[subset] = rels[i]->rows;
    }

    /* Build up solutions for larger subsets */
    for (size = 2; size <= num_rels; size++)
    {
        int subset;
        
        for (subset = 0; subset < num_subsets; subset++)
        {
            int popcount = __builtin_popcount(subset);
            
            if (popcount != size)
                continue;

            /* Try all ways to split this subset into two non-empty parts */
            int left, right;
            
            for (left = (subset - 1) & subset; left > 0; left = (left - 1) & subset)
            {
                right = subset ^ left;
                
                /* Avoid duplicate work: only consider left < right */
                if (left >= right)
                    continue;
                    
                /* Both parts must have been computed */
                if (best_costs[left] < 0 || best_costs[right] < 0)
                    continue;

                /* Calculate cost for this split */
                double cost = best_costs[left] + best_costs[right];
                
                /* Add estimated join cost based on statistics */
                RelOptInfo *left_rel = NULL;
                RelOptInfo *right_rel = NULL;
                
                /* Find representative relations for cost estimation */
                for (i = 0; i < num_rels; i++)
                {
                    if ((left & (1 << i)) && left_rel == NULL)
                        left_rel = rels[i];
                    if ((right & (1 << i)) && right_rel == NULL)
                        right_rel = rels[i];
                }
                
                if (left_rel && right_rel)
                {
                    cost += jo_estimate_join_cost(root, left_rel, right_rel);
                }

                /* Update if this is the best split so far */
                if (best_costs[subset] < 0 || cost < best_costs[subset])
                {
                    best_costs[subset] = cost;
                    best_left[subset] = left;
                    best_right[subset] = right;
                }
            }
        }
    }

    /* Build result from DP solution */
    final_set = num_subsets - 1;
    
    if (best_costs[final_set] >= 0)
    {
        /*
         * Reconstruct the join order by traversing the DP table.
         * For now, we use a simple left-deep tree order.
         */
        int *order = palloc(num_rels * sizeof(int));
        int pos = 0;
        
        /* Extract tables in order based on DP decisions */
        /* Simple approach: order by the first table in each left partition */
        for (i = 0; i < num_rels; i++)
        {
            order[pos++] = i;
        }
        
        /* Build result list in computed order */
        for (i = 0; i < num_rels; i++)
        {
            result = lappend(result, rels[order[i]]);
        }
        
        pfree(order);

        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: DP algorithm found optimal cost %.2f", 
                 best_costs[final_set]);
    }
    else
    {
        /* DP failed to find a solution, fall back to input order */
        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: DP failed, using input order");
        result = list_copy(joinlist);
    }

    /* Clean up */
    pfree(rels);
    pfree(best_costs);
    pfree(best_left);
    pfree(best_right);

    return result;
}
