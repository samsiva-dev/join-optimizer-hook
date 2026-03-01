/*-------------------------------------------------------------------------
 *
 * stats.c
 *      Statistics management for join optimizer
 *
 * This file handles loading, saving, and querying join statistics
 * from the database tables and shared memory cache.
 *
 *-------------------------------------------------------------------------
 */

#include "join_optimizer.h"

/*-------------------------------------------------------------------------
 * jo_load_stats_from_table
 *      Load join statistics from database into shared memory
 *
 * This function connects to SPI, queries the join_optimizer.join_stats
 * table, and populates the shared memory cache with the results.
 * Statistics are ordered by execution_count to prioritize frequently
 * used join pairs.
 *-------------------------------------------------------------------------
 */

/* Forward declaration for shmem initialization */
void jo_ensure_shmem_init(void);

void
jo_load_stats_from_table(void)
{
    int         ret;
    TupleDesc   tupdesc;
    int         i;

    if (!join_optimizer_collect_stats)
        return;

    /* Initialize shared memory if not done yet */
    jo_ensure_shmem_init();
    
    if (jo_shmem == NULL)
        return;

    /* Connect to SPI and query stats table */
    SPI_connect();

    ret = SPI_execute("SELECT left_table::oid, right_table::oid, "
                      "join_column_left, join_column_right, "
                      "estimated_rows, actual_rows, selectivity, join_cost, "
                      "execution_count "
                      "FROM join_optimizer.join_stats "
                      "WHERE execution_count > 0 "
                      "ORDER BY execution_count DESC "
                      "LIMIT 1000",
                      true, 0);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        tupdesc = SPI_tuptable->tupdesc;

        LWLockAcquire(jo_shmem->lock, LW_EXCLUSIVE);

        jo_shmem->num_entries = 0;

        for (i = 0; i < SPI_processed && i < JOIN_OPTIMIZER_STATS_SIZE; i++)
        {
            HeapTuple   tuple = SPI_tuptable->vals[i];
            bool        isnull;
            JoinPairStats *entry = &jo_shmem->stats[i];

            /* Extract relation OIDs */
            entry->left_relid = DatumGetObjectId(
                SPI_getbinval(tuple, tupdesc, 1, &isnull));
            entry->right_relid = DatumGetObjectId(
                SPI_getbinval(tuple, tupdesc, 2, &isnull));
            
            /* Copy column names */
            char *left_col = SPI_getvalue(tuple, tupdesc, 3);
            char *right_col = SPI_getvalue(tuple, tupdesc, 4);
            
            if (left_col)
                strlcpy(entry->left_col, left_col, NAMEDATALEN);
            else
                entry->left_col[0] = '\0';
                
            if (right_col)
                strlcpy(entry->right_col, right_col, NAMEDATALEN);
            else
                entry->right_col[0] = '\0';

            /* Extract numeric statistics */
            entry->estimated_rows = DatumGetInt64(
                SPI_getbinval(tuple, tupdesc, 5, &isnull));
            entry->actual_rows = DatumGetInt64(
                SPI_getbinval(tuple, tupdesc, 6, &isnull));
            entry->selectivity = DatumGetFloat8(
                SPI_getbinval(tuple, tupdesc, 7, &isnull));
            entry->avg_cost = DatumGetFloat8(
                SPI_getbinval(tuple, tupdesc, 8, &isnull));
            entry->execution_count = DatumGetInt64(
                SPI_getbinval(tuple, tupdesc, 9, &isnull));

            jo_shmem->num_entries++;
        }

        LWLockRelease(jo_shmem->lock);

        if (join_optimizer_debug)
            elog(NOTICE, "join_optimizer: loaded %d stats entries from table", 
                 jo_shmem->num_entries);
    }

    SPI_finish();
}

/*-------------------------------------------------------------------------
 * jo_save_stats_to_table
 *      Save statistics from shared memory to database
 *
 * Currently statistics are saved via SQL functions called from
 * the application layer. This is a placeholder for bulk save operations.
 *-------------------------------------------------------------------------
 */
void
jo_save_stats_to_table(void)
{
    /* Statistics are saved via the SQL functions */
    /* This could be implemented for bulk saves if needed */
}

/*-------------------------------------------------------------------------
 * jo_get_join_stats
 *      Look up statistics for a join pair in shared memory
 *
 * Returns a pointer to the JoinPairStats entry if found,
 * or NULL if no statistics are available. The lookup is
 * symmetric - (A,B) matches (B,A).
 *-------------------------------------------------------------------------
 */
JoinPairStats *
jo_get_join_stats(Oid left_rel, Oid right_rel)
{
    int i;

    if (jo_shmem == NULL)
        return NULL;

    LWLockAcquire(jo_shmem->lock, LW_SHARED);

    for (i = 0; i < jo_shmem->num_entries; i++)
    {
        JoinPairStats *entry = &jo_shmem->stats[i];
        
        /* Check both orderings since joins are symmetric */
        if ((entry->left_relid == left_rel && entry->right_relid == right_rel) ||
            (entry->left_relid == right_rel && entry->right_relid == left_rel))
        {
            LWLockRelease(jo_shmem->lock);
            return entry;
        }
    }

    LWLockRelease(jo_shmem->lock);
    return NULL;
}

/*-------------------------------------------------------------------------
 * jo_get_table_stats
 *      Look up statistics for a single table
 *
 * Returns a pointer to TableStats if found, or NULL.
 * Currently queries the join_optimizer.table_stats table.
 *-------------------------------------------------------------------------
 */
TableStats *
jo_get_table_stats(Oid relid)
{
    /* 
     * Could be implemented to query join_optimizer.table_stats table
     * or maintain a separate shared memory cache for table stats.
     * For now, we rely on PostgreSQL's built-in pg_statistic.
     */
    return NULL;
}

/*-------------------------------------------------------------------------
 * jo_update_join_stats
 *      Update join statistics in the database
 *
 * This function uses SPI to call the update_join_stats SQL function,
 * which handles the upsert logic with running averages.
 *-------------------------------------------------------------------------
 */
void
jo_update_join_stats(Oid left_rel, Oid right_rel,
                      const char *left_col, const char *right_col,
                      int64 estimated, int64 actual, double cost)
{
    int ret;
    char query[1024];

    if (!join_optimizer_collect_stats)
        return;

    SPI_connect();

    snprintf(query, sizeof(query),
             "SELECT join_optimizer.update_join_stats("
             "%u::regclass, %u::regclass, '%s', '%s', "
             "%ld, %ld, %f)",
             left_rel, right_rel, left_col, right_col,
             (long)estimated, (long)actual, cost);

    ret = SPI_execute(query, false, 0);

    if (ret != SPI_OK_SELECT)
    {
        elog(WARNING, "join_optimizer: failed to update stats for %u-%u",
             left_rel, right_rel);
    }

    SPI_finish();
}

/*-------------------------------------------------------------------------
 * jo_get_selectivity
 *      Get the selectivity for a join between two relations
 *
 * Returns the learned selectivity if available, or a default
 * value of 0.1 (10%) if no statistics exist.
 *-------------------------------------------------------------------------
 */
double
jo_get_selectivity(Oid left_rel, Oid right_rel,
                    const char *left_col, const char *right_col)
{
    JoinPairStats *stats = jo_get_join_stats(left_rel, right_rel);
    
    if (stats && stats->execution_count > 0)
    {
        /* Clamp selectivity to reasonable bounds */
        double sel = stats->selectivity;
        if (sel < 0.0001)
            sel = 0.0001;
        if (sel > 1.0)
            sel = 1.0;
        return sel;
    }
    
    return 0.1;  /* Default selectivity: 10% */
}

/*-------------------------------------------------------------------------
 * jo_ensure_shmem_init
 *      Initialize shared memory on first use (lazy initialization)
 *
 * In PostgreSQL 15+, shared memory is requested during postmaster
 * startup but initialized lazily on first backend access.
 *-------------------------------------------------------------------------
 */
void
jo_ensure_shmem_init(void)
{
    bool        found;

    /* Already initialized? */
    if (jo_shmem != NULL)
        return;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    jo_shmem = ShmemInitStruct("join_optimizer",
                               sizeof(JoinOptimizerShmemStruct),
                               &found);

    if (!found)
    {
        /* First time, initialize the structure */
        memset(jo_shmem, 0, sizeof(JoinOptimizerShmemStruct));
        jo_shmem->lock = &(GetNamedLWLockTranche("join_optimizer"))->lock;
        jo_shmem->num_entries = 0;
    }

    LWLockRelease(AddinShmemInitLock);
}
