//
// Created by Weinan Wang on 1/24/19.
//
#include "postgres.h"

#include <unistd.h>
#include <memory.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"

#include "funcapi.h"
#include "libpq-fe.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/acl.h"
#include "utils/attoptcache.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbpartition.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"

#include "alluxioop.h"
#include "alluxio_fdw.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(alluxio_fdw_handler);
PG_FUNCTION_INFO_V1(alluxio_fdw_validator);

static int
alluxio_acquire_sample_rows_on_segment(Relation onerel, int elevel,
                                       HeapTuple *rows, int targrows,
                                       double *totalrows, double *totaldeadrows);
static void
alluxioGetOptions(Oid foreigntableid,
                  char **filename, List **other_options);

Datum alluxio_fdw_validator(PG_FUNCTION_ARGS)
{
    List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid			catalog = PG_GETARG_OID(1);
    char	   *filename = NULL;
    ListCell   *cell;
    /*
     * Only superusers are allowed to set options of a alluxio_fdw foreign table.
     * This is because the filename is one of those options, and we don't want
     * non-superusers to be able to determine which file gets read.
     *
     * Putting this sort of permissions check in a validator is a bit of a
     * crock, but there doesn't seem to be any other place that can enforce
     * the check more cleanly.
     *
     * Note that the valid_options[] array disallows setting filename at any
     * options level other than foreign table --- otherwise there'd still be a
     * security hole.
     */
    if (catalog == ForeignTableRelationId && !superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("only superuser can change options of a alluxio_fdw foreign table")));

    foreach(cell, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(cell);
        if (strcmp(def->defname, "filename") == 0)
        {
            if (filename)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("conflicting or redundant options")));
            filename = defGetString(def);

            if (strcmp(def->defname, "format") == 0)
            {
                char	   *fmt = defGetString(def);

            }
        }
    }

    if (catalog == ForeignTableRelationId && filename == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                        errmsg("filename is required for file_fdw foreign tables")));

    PG_RETURN_VOID();
}

void
alluxioGetForeignRelSize(PlannerInfo *root,
                      RelOptInfo *baserel,
                      Oid foreigntableid)
{
    BlockNumber pages;
    double		ntuples;
    double		nrows;
    struct AlluxioFdwPlanState *fdwPlanState;

    fdwPlanState = (struct AlluxioFdwPlanState *) palloc0(sizeof(struct AlluxioFdwPlanState));
    alluxioGetOptions(foreigntableid,&fdwPlanState->filename,&fdwPlanState->options);
    baserel->fdw_private = fdwPlanState;

    alluxioInit();
    char    *bracket = strstr(fdwPlanState->filename,"<SEGID>");
    int     headLen = bracket - fdwPlanState->filename;

    int segNum = getgpsegmentCount();
    size_t  total_sz = 0;
    for(int i = 0; i < segNum; i++)
    {
        char            *segDir;
        alluxioHandler  *resHandle;
        ListCell        *cell;
        size_t          seg_sz;

        segDir = palloc0(strlen(fdwPlanState->filename) + 10);
        memcpy(segDir,fdwPlanState->filename,headLen);
        sprintf(segDir + headLen,"%d",i);

        resHandle = createGpalluxioHander();
        resHandle->url = segDir;
        AlluxioConnectDir(resHandle);

        seg_sz = 0;
        foreach(cell,resHandle->blocksinfo)
        {
            alluxioBlock *block = (alluxioBlock *) lfirst(cell);
            seg_sz += block->length;
        }

        total_sz += seg_sz;
        AlluxioDisconnectDir(resHandle);
        destoryGpalluxioHandler(resHandle);
        pfree(segDir);
    }

    pages = (total_sz + (BLCKSZ - 1)) / BLCKSZ;


    /*
     * Estimate the number of tuples in the file.
     */
    if (baserel->pages > 0)
    {
        /*
         * We have # of pages and # of tuples from pg_class (that is, from a
         * previous ANALYZE), so compute a tuples-per-page estimate and scale
         * that by the current file size.
         */
        double		density;

        density = baserel->tuples / (double) baserel->pages;
        ntuples = clamp_row_est(density * (double) pages);
    }
    else
    {
        /*
         * Otherwise we have to fake it.  We back into this estimate using the
         * planner's idea of the relation width; which is bogus if not all
         * columns are being read, not to mention that the text representation
         * of a row probably isn't the same size as its internal
         * representation.  Possibly we could do something better, but the
         * real answer to anyone who complains is "ANALYZE" ...
         */
        int			tuple_width;

        tuple_width = MAXALIGN(baserel->width) +
                MAXALIGN(sizeof(HeapTupleHeaderData));
        ntuples = clamp_row_est((double) total_sz /
                                        (double) tuple_width);
    }
    fdwPlanState->ntuples = ntuples;

    /*
     * Now estimate the number of rows returned by the scan after applying the
     * baserestrictinfo quals.
     */
    nrows = ntuples *
            clauselist_selectivity(root,
                                   baserel->baserestrictinfo,
                                   0,
                                   JOIN_INNER,
                                   NULL,
                                   false); /* GPDB_91_MERGE_FIXME: do we need damping? */

    nrows = clamp_row_est(nrows);

    /* Save the output-rows estimate for the planner */
    baserel->rows = nrows;
}

/*
 * check_selective_binary_conversion
 *
 * Check to see if it's useful to convert only a subset of the file's columns
 * to binary.  If so, construct a list of the column names to be converted,
 * return that at *columns, and return TRUE.  (Note that it's possible to
 * determine that no columns need be converted, for instance with a COUNT(*)
 * query.  So we can't use returning a NIL list to indicate failure.)
 */
static bool
check_selective_binary_conversion(RelOptInfo *baserel,
                                  Oid foreigntableid,
                                  List **columns)
{
    ForeignTable *table;
    ListCell   *lc;
    Relation	rel;
    TupleDesc	tupleDesc;
    AttrNumber	attnum;
    Bitmapset  *attrs_used = NULL;
    bool		has_wholerow = false;
    int			numattrs;
    int			i;

    *columns = NIL;				/* default result */

    /*
     * Check format of the file.  If binary format, this is irrelevant.
     */
    table = GetForeignTable(foreigntableid);
    foreach(lc, table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "format") == 0)
        {
            char	   *format = defGetString(def);

            if (strcmp(format, "binary") == 0)
                return false;
            break;
        }
    }

    /* Collect all the attributes needed for joins or final output. */
    pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
                   &attrs_used);

    /* Add all the attributes used by restriction clauses. */
    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        pull_varattnos((Node *) rinfo->clause, baserel->relid,
                       &attrs_used);
    }

    /* Convert attribute numbers to column names. */
    rel = heap_open(foreigntableid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);

    while ((attnum = bms_first_member(attrs_used)) >= 0)
    {
        /* Adjust for system attributes. */
        attnum += FirstLowInvalidHeapAttributeNumber;

        if (attnum == 0)
        {
            has_wholerow = true;
            break;
        }

        /* Ignore system attributes. */
        if (attnum < 0)
            continue;

        /* Get user attributes. */
        if (attnum > 0)
        {
            Form_pg_attribute attr = tupleDesc->attrs[attnum - 1];
            char	   *attname = NameStr(attr->attname);

            /* Skip dropped attributes (probably shouldn't see any here). */
            if (attr->attisdropped)
                continue;
            *columns = lappend(*columns, makeString(pstrdup(attname)));
        }
    }

    /* Count non-dropped user attributes while we have the tupdesc. */
    numattrs = 0;
    for (i = 0; i < tupleDesc->natts; i++)
    {
        Form_pg_attribute attr = tupleDesc->attrs[i];

        if (attr->attisdropped)
            continue;
        numattrs++;
    }

    heap_close(rel, AccessShareLock);

    /* If there's a whole-row reference, fail: we need all the columns. */
    if (has_wholerow)
    {
        *columns = NIL;
        return false;
    }

    /* If all the user attributes are needed, fail. */
    if (numattrs == list_length(*columns))
    {
        *columns = NIL;
        return false;
    }

    return true;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
               struct AlluxioFdwPlanState *fdw_private,
               Cost *startup_cost, Cost *total_cost)
{
    BlockNumber pages = fdw_private->pages;
    double		ntuples = fdw_private->ntuples;
    Cost		run_cost = 0;
    Cost		cpu_per_tuple;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan, to account
     * for the cost of parsing records.
     */
    run_cost += seq_page_cost * pages;

    *startup_cost = baserel->baserestrictcost.startup;
    cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;
    *total_cost = *startup_cost + run_cost;
}

/*
 * alluxioGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
void
alluxioGetForeignPaths(PlannerInfo *root,
                    RelOptInfo *baserel,
                    Oid foreigntableid)
{
    struct AlluxioFdwPlanState *fdw_private = (struct AlluxioFdwPlanState*) baserel->fdw_private;
    Cost		startup_cost;
    Cost		total_cost;
    List	   *columns;
    List	   *coptions = NIL;

    /* Decide whether to selectively perform binary conversion */
    if (check_selective_binary_conversion(baserel,
                                          foreigntableid,
                                          &columns))
        coptions = list_make1(makeDefElem("convert_selectively",
                                          (Node *) columns));

    /* Estimate costs */
    estimate_costs(root, baserel, fdw_private,
                   &startup_cost, &total_cost);

    /*
     * Create a ForeignPath node and add it as only possible path.  We use the
     * fdw_private list of the path to carry the convert_selectively option;
     * it will be propagated into the fdw_private list of the Plan node.
     */
    add_path(baserel, (Path *)
            create_foreignscan_path(root, baserel,
                                    baserel->rows,
                                    startup_cost,
                                    total_cost,
                                    NIL,		/* no pathkeys */
                                    NULL,		/* no outer rel either */
                                    coptions));

    /*
     * If data file was sorted, and we knew it somehow, we could insert
     * appropriate pathkeys into the ForeignPath node to tell the planner
     * that.
     */
}
/*
 * fileGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
ForeignScan *
alluxioGetForeignPlan(PlannerInfo *root,
                   RelOptInfo *baserel,
                   Oid foreigntableid,
                   ForeignPath *best_path,
                   List *tlist,
                   List *scan_clauses)
{
    ForeignScan *fScan;
    Index		scan_relid = baserel->relid;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Create the ForeignScan node */
    fScan = make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL,	/* no expressions to evaluate */
                            best_path->fdw_private);
    return fScan;
}

/*
 * fileEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
void
alluxioEndForeignScan(ForeignScanState *node)
{
    struct AlluxioFdwExecutionState *festate = (AlluxioFdwExecutionState*) node->fdw_state;

    /* if festate is NULL, we are in EXPLAIN; nothing to do */
    if (festate)
        EndCopyFrom(festate->cstate);
}

/*
 * fileBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
void
alluxioBeginForeignScan(ForeignScanState *node, int eflags)
{
    ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
    char	   *filename;
    List	   *options;
    CopyState	cstate;
    AlluxioFdwExecutionState *festate;

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Fetch options of foreign table */
    alluxioGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
                   &filename, &options);

    /* Add any options from the plan (currently only convert_selectively) */
    options = list_concat(options, plan->fdw_private);

    /*
     * Create CopyState from FDW options.  We always acquire all columns, so
     * as to match the expected ScanTupleSlot signature.
     */
    cstate = BeginCopyFrom(node->ss.ss_currentRelation,
                           filename,
                           false, /* is_program */
                           NULL,  /* data_source_cb */
                           NULL,  /* data_source_cb_extra */
                           NIL,   /* attnamelist */
                           options,
                           NIL);  /* ao_segnos */

    /*
     * Save state in node->fdw_state.  We must save enough information to call
     * BeginCopyFrom() again.
     */
    festate = (AlluxioFdwExecutionState *) palloc(sizeof(AlluxioFdwExecutionState));
    festate->filename = filename;
    festate->options = options;
    festate->cstate = cstate;

    node->fdw_state = (void *) festate;
}
/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
void
alluxioExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    char	   *filename;
    List	   *options;

    /* Fetch options --- we only need filename at this point */
    alluxioGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
                   &filename, &options);

   // ExplainPropertyText("Foreign File", filename, es);
   // if (es->costs)
   // {
   //     appendStringInfoSpaces(es->str, es->indent * 2);
   //     appendStringInfo(es->str,
   //                      " Foreign File cost=%.3f..%.3f rows=%.0f width=%d \n",
   //                      node->ss.ps.plan->startup_cost, node->ss.ps.plan->total_cost,
   //                      node->ss.ps.plan->plan_rows, node->ss.ps.plan->plan_width);
   // }
}

Datum
alluxio_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /*
    fdwroutine->IterateForeignScan = fileIterateForeignScan;
    fdwroutine->ReScanForeignScan = fileReScanForeignScan;
     */
    fdwroutine->GetForeignRelSize = alluxioGetForeignRelSize;
    fdwroutine->GetForeignPaths = alluxioGetForeignPaths;
    fdwroutine->GetForeignPlan = alluxioGetForeignPlan;
    fdwroutine->BeginForeignScan = alluxioBeginForeignScan;
    fdwroutine->ExplainForeignScan = alluxioExplainForeignScan;
    fdwroutine->EndForeignScan = alluxioEndForeignScan;

    fdwroutine->AnalyzeForeignTable = alluxioAnalyzeForeignTable;
    fdwroutine->AnalyzeForeignTableOnSeg = alluxio_acquire_sample_rows_on_segment;
    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Retrieve per-column generic options from pg_attribute and construct a list
 * of DefElems representing them.
 *
 * At the moment we only have "force_not_null", and "force_null",
 * which should each be combined into a single DefElem listing all such
 * columns, since that's what COPY expects.
 */
static List *
get_alluxio_fdw_attribute_options(Oid relid)
{
    Relation	rel;
    TupleDesc	tupleDesc;
    AttrNumber	natts;
    AttrNumber	attnum;
    List	   *fnncolumns = NIL;
    List	   *fncolumns = NIL;

    List	   *options = NIL;

    rel = heap_open(relid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);
    natts = tupleDesc->natts;

    /* Retrieve FDW options for all user-defined attributes. */
    for (attnum = 1; attnum <= natts; attnum++)
    {
        Form_pg_attribute attr = tupleDesc->attrs[attnum - 1];
        List	   *options;
        ListCell   *lc;

        /* Skip dropped attributes. */
        if (attr->attisdropped)
            continue;

        options = GetForeignColumnOptions(relid, attnum);
        foreach(lc, options)
        {
            DefElem    *def = (DefElem *) lfirst(lc);

            if (strcmp(def->defname, "force_not_null") == 0)
            {
                if (defGetBoolean(def))
                {
                    char	   *attname = pstrdup(NameStr(attr->attname));

                    fnncolumns = lappend(fnncolumns, makeString(attname));
                }
            }
            else if (strcmp(def->defname, "force_null") == 0)
            {
                if (defGetBoolean(def))
                {
                    char	   *attname = pstrdup(NameStr(attr->attname));

                    fncolumns = lappend(fncolumns, makeString(attname));
                }
            }
            /* maybe in future handle other options here */
        }
    }

    heap_close(rel, AccessShareLock);

    /*
     * Return DefElem only when some column(s) have force_not_null /
     * force_null options set
     */
    if (fnncolumns != NIL)
        options = lappend(options, makeDefElem("force_not_null", (Node *) fnncolumns));

    if (fncolumns != NIL)
        options = lappend(options, makeDefElem("force_null", (Node *) fncolumns));

    return options;
}

static void
alluxioGetOptions(Oid foreigntableid,
               char **filename, List **other_options)
{
    ForeignTable *table;
    ForeignServer *server;
    ForeignDataWrapper *wrapper;
    List	   *options;
    ListCell   *lc,
            *prev;

    /*
     * Extract options from FDW objects.  We ignore user mappings because
     * file_fdw doesn't have any options that can be specified there.
     *
     * (XXX Actually, given the current contents of valid_options[], there's
     * no point in examining anything except the foreign table's own options.
     * Simplify?)
     */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);
    wrapper = GetForeignDataWrapper(server->fdwid);

    options = NIL;
    options = list_concat(options, wrapper->options);
    options = list_concat(options, server->options);
    options = list_concat(options, table->options);
    options = list_concat(options, get_alluxio_fdw_attribute_options(foreigntableid));

    /*
     * Separate out the filename.
     */
    *filename = NULL;
    prev = NULL;
    foreach(lc, options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            *filename = defGetString(def);
            options = list_delete_cell(options, lc, prev);
            break;
        }
        prev = lc;
    }

    /*
     * The validator should have checked that a filename was included in the
     * options, but check again, just in case.
     */
    if (*filename == NULL)
        elog(ERROR, "filename is required for file_fdw foreign tables");


    if (table->exec_location == FTEXECLOCATION_ALL_SEGMENTS)
    {
        /*
         * pass the on_segment option to COPY, which will replace the required
         * placeholder "<SEGID>" in filename
         */
        options = list_append_unique(options, makeDefElem("on_segment", (Node *)makeInteger(TRUE)));
    }
    else if (table->exec_location == FTEXECLOCATION_ANY)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("file_fdw does not support mpp_execute option 'any'")));
    }

    *other_options = options;
}

static MemTuple
sampling_alluxio(alluxioHandler  *resHandle,MemoryContext tupcontext)
{
    MemTuple tuple = NULL;
    MemTuple tuple_ptr;
    MemoryContext oldcontext;
    static char* blockcache;
    static int32 bc_offset;
    static bool raw_buf_done = true;
    static size_t bytesread = 0;
    struct _alluxioCache *cache = NULL;

    MemoryContextReset(tupcontext);
    oldcontext = MemoryContextSwitchTo(tupcontext);
    if(raw_buf_done)
    {
        bc_offset = 0;
        cache = AlluxioDirectRead(resHandle);

        if(cache == NULL)
        {
            MemoryContextSwitchTo(oldcontext);
            return NULL;
        }

        bytesread = cache->end - cache->begin;
        blockcache = cache->begin;


        raw_buf_done = false;
    }

    tuple_ptr = (MemTuple)(blockcache + bc_offset);
    int32 sz = memtuple_get_size((MemTuple)(blockcache + bc_offset));
    bc_offset += sz;

    tuple = (MemTuple)palloc0(sz);
    memcpy(tuple,tuple_ptr,sz);

    if(bc_offset == bytesread)
        raw_buf_done = true;

    MemoryContextSwitchTo(oldcontext);

    return tuple;
}

static int
alluxio_acquire_sample_rows(Relation onerel, int elevel,
                         HeapTuple *rows, int targrows,
                         double *totalrows, double *totaldeadrows) {
    /*
     * 'colLargeRowIndexes' is essentially an argument, but it's passed via a
     * global variable to avoid changing the AcquireSampleRowsFunc prototype.
     */
    extern Bitmapset	**acquire_func_colLargeRowIndexes;
    Bitmapset **colLargeRowIndexes = acquire_func_colLargeRowIndexes;
    TupleDesc	relDesc = RelationGetDescr(onerel);
    TupleDesc	newDesc;
    AttInMetadata *attinmeta;
    StringInfoData str;
    int			sampleTuples;	/* 32 bit - assume that number of tuples will not > 2B */
    char	  **values;
    int			numLiveColumns;
    int			perseg_targrows;
    CdbPgResults cdb_pgresults = {NULL, 0};
    int			i;

    Assert(targrows > 0.0);

    int segNum = getgpsegmentCount();
    perseg_targrows = targrows / segNum;

    /*
     * Count the number of columns, excluding dropped columns. We'll need that
     * later.
     */
    numLiveColumns = 0;
    for (i = 0; i < relDesc->natts; i++)
    {
        Form_pg_attribute attr = relDesc->attrs[i];

        if (attr->attisdropped)
            continue;

        numLiveColumns++;
    }

    /*
     * Construct SQL command to dispatch to segments.
     */
    initStringInfo(&str);
    appendStringInfo(&str, "select * from pg_catalog.gp_fdw_acquire_sample_rows(%u, %d, '%s')",
                     RelationGetRelid(onerel),
                     perseg_targrows,
                     "alluxio" );

    /* special columns */
    appendStringInfoString(&str, " as (");
    appendStringInfoString(&str, "totalrows pg_catalog.float8, ");
    appendStringInfoString(&str, "totaldeadrows pg_catalog.float8, ");
    appendStringInfoString(&str, "oversized_cols_bitmap pg_catalog.text");

    /* table columns */
    for (i = 0; i < relDesc->natts; i++)
    {
        Form_pg_attribute attr = relDesc->attrs[i];
        Oid			typid = gp_acquire_sample_rows_col_type(attr->atttypid);

        if (attr->attisdropped)
            continue;

        appendStringInfo(&str, ", %s %s",
                         quote_identifier(NameStr(attr->attname)),
                         format_type_be(typid));
    }

    appendStringInfoString(&str, ")");

    /*
     * Execute it.
     */
    elog(elevel, "Executing SQL: %s", str.data);
    CdbDispatchCommand(str.data, DF_WITH_SNAPSHOT, &cdb_pgresults);

    /*
     * Build a modified tuple descriptor for the table.
     *
     * Some datatypes need special treatment, so we cannot use the relation's
     * original tupledesc.
     */
    newDesc = CreateTupleDescCopy(relDesc);
    for (i = 0; i < relDesc->natts; i++)
    {
        Form_pg_attribute attr = relDesc->attrs[i];
        Oid			typid = gp_acquire_sample_rows_col_type(attr->atttypid);

        newDesc->attrs[i]->atttypid = typid;
    }
    attinmeta = TupleDescGetAttInMetadata(newDesc);

    /*
     * Read the result set from each segment. Gather the sample rows *rows,
     * and sum up the summary rows for grand 'totalrows' and 'totaldeadrows'.
     */
    values = (char **) palloc0(relDesc->natts * sizeof(char *));
    sampleTuples = 0;
    *totalrows = 0;
    *totaldeadrows = 0;
    for (int resultno = 0; resultno < cdb_pgresults.numResults; resultno++)
    {
        struct pg_result *pgresult = cdb_pgresults.pg_results[resultno];
        bool		got_summary = false;
        double		this_totalrows = 0;
        double		this_totaldeadrows = 0;

        if (PQresultStatus(pgresult) != PGRES_TUPLES_OK)
        {
            cdbdisp_clearCdbPgResults(&cdb_pgresults);
            ereport(ERROR,
                    (errmsg("unexpected result from segment: %d",
                            PQresultStatus(pgresult))));
        }

        if (GpPolicyIsReplicated(onerel->rd_cdbpolicy))
        {
            /*
             * A replicated table has the same data in all segments. Arbitrarily,
             * use the sample from the first segment, and discard the rest.
             * (This is rather inefficient, of course. It would be better to
             * dispatch to only one segment, but there is no easy API for that
             * in the dispatcher.)
             */
            if (resultno > 0)
                continue;
        }

        for (int rowno = 0; rowno < PQntuples(pgresult); rowno++)
        {
            if (!PQgetisnull(pgresult, rowno, 0))
            {
                /* This is a summary row. */
                if (got_summary)
                    elog(ERROR, "got duplicate summary row from gp_acquire_sample_rows");

                this_totalrows = DatumGetFloat8(DirectFunctionCall1(float8in,
                                                                    CStringGetDatum(PQgetvalue(pgresult, rowno, 0))));
                this_totaldeadrows = DatumGetFloat8(DirectFunctionCall1(float8in,
                                                                        CStringGetDatum(PQgetvalue(pgresult, rowno, 1))));
                got_summary = true;
            }
            else
            {
                /* This is a sample row. */
                int			index;

                if (sampleTuples >= targrows)
                    elog(ERROR, "too many sample rows received from gp_acquire_sample_rows");

                /* Read the 'toolarge' bitmap, if any */
                if (colLargeRowIndexes && !PQgetisnull(pgresult, rowno, 2))
                {
                    char	   *toolarge;

                    toolarge = PQgetvalue(pgresult, rowno, 2);
                    if (strlen(toolarge) != numLiveColumns)
                        elog(ERROR, "'toolarge' bitmap has incorrect length");

                    index = 0;
                    for (i = 0; i < relDesc->natts; i++)
                    {
                        Form_pg_attribute attr = relDesc->attrs[i];

                        if (attr->attisdropped)
                            continue;

                        if (toolarge[index] == '1')
                            colLargeRowIndexes[i] = bms_add_member(colLargeRowIndexes[i], sampleTuples);
                        index++;
                    }
                }

                /* Process the columns */
                index = 0;
                for (i = 0; i < relDesc->natts; i++)
                {
                    Form_pg_attribute attr = relDesc->attrs[i];

                    if (attr->attisdropped)
                        continue;

                    if (PQgetisnull(pgresult, rowno, 3 + index))
                        values[i] = NULL;
                    else
                        values[i] = PQgetvalue(pgresult, rowno, 3 + index);
                    index++; /* Move index to the next result set attribute */
                }

                rows[sampleTuples] = BuildTupleFromCStrings(attinmeta, values);
                sampleTuples++;

                /*
                 * note: we don't set the OIDs in the sample. ANALYZE doesn't
                 * collect stats for them
                 */
            }
        }

        if (!got_summary)
            elog(ERROR, "did not get summary row from gp_acquire_sample_rows");

        if (resultno >= segNum)
        {
            /*
             * This result is for a segment that's not holding any data for this
             * table. Should get 0 rows.
             */
            if (this_totalrows != 0 || this_totalrows != 0)
                elog(WARNING, "table \"%s\" contains rows in segment %d, which is outside the # of segments for the table's policy (%d segments)",
                     RelationGetRelationName(onerel), resultno, onerel->rd_cdbpolicy->numsegments);
        }

        (*totalrows) += this_totalrows;
        (*totaldeadrows) += this_totaldeadrows;
    }

    cdbdisp_clearCdbPgResults(&cdb_pgresults);

    return sampleTuples;
}

static int
alluxio_acquire_sample_rows_on_segment(Relation onerel, int elevel,
                         HeapTuple *rows, int targrows,
                         double *totalrows, double *totaldeadrows)
{
    int             numrow = 0;
    TupleTableSlot  *slot;
    TupleDesc       tupDesc;
    char            *filename;
    char            *bracket;
    int             headLen;
    List            *options;

    MemoryContext   tupcontext;

    tupDesc = RelationGetDescr(onerel);
    alluxioGetOptions(RelationGetRelid(onerel),&filename,&options);
    slot = MakeSingleTupleTableSlot(tupDesc);
    bracket = strstr(filename,"<SEGID>");
    headLen = bracket - filename;
    int64 thissegrows = 0;
    *totalrows = 0;
    *totaldeadrows = 0;
    alluxioInit();

    tupcontext = AllocSetContextCreate(CurrentMemoryContext,
                                       "file_fdw temporary context",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE);


    int segNum = getgpsegmentCount();
    int i = GpIdentity.segindex;
    {
        char            *segDir;
        alluxioHandler  *resHandle;

        segDir = palloc0(strlen(filename) + 10);
        memcpy(segDir,filename,headLen);
        sprintf(segDir + headLen,"%d",i);
        resHandle = createGpalluxioHander();
        resHandle->url = segDir;
        AlluxioConnectDir(resHandle);
        resHandle->blockiter = list_head(resHandle->blocksinfo);

        for(;;) {
            vacuum_delay_point();


            MemTuple mtuple = sampling_alluxio(resHandle,tupcontext);

            if(!mtuple)
                break;

            thissegrows ++;
            if(numrow == targrows)
                continue;

            ExecStoreGenericTuple(mtuple,slot,false);
            slot_getallattrs(slot);
            rows[numrow++]= heap_form_tuple(slot->tts_tupleDescriptor,
                    slot_get_values(slot),
                    slot_get_isnull(slot));
        }

        AlluxioDisconnectDir(resHandle);
        destoryGpalluxioHandler(resHandle);
        pfree(segDir);
    }

    ExecDropSingleTupleTableSlot(slot);
    MemoryContextDelete(tupcontext);

    *totalrows = (double) thissegrows;
    return numrow;
}

bool
alluxioAnalyzeForeignTable(Relation relation,
                        AcquireSampleRowsFunc *func,
                        BlockNumber *totalpages)
{
    char    *filename;
    List    *options;

    alluxioInit();
    alluxioGetOptions(RelationGetRelid(relation), &filename, &options);
    char    *bracket = strstr(filename,"<SEGID>");
    int     headLen = bracket - filename;

    int segNum = getgpsegmentCount();
    size_t  total_sz = 0;
    for(int i = 0; i < segNum; i++)
    {
        char            *segDir;
        alluxioHandler  *resHandle;
        ListCell        *cell;
        size_t          seg_sz;

        segDir = palloc0(strlen(filename) + 10);
        memcpy(segDir,filename,headLen);
        sprintf(segDir + headLen,"%d",i);

        resHandle = createGpalluxioHander();
        resHandle->url = segDir;
        AlluxioConnectDir(resHandle);

        seg_sz = 0;
        foreach(cell,resHandle->blocksinfo)
        {
            alluxioBlock *block = (alluxioBlock *) lfirst(cell);
            seg_sz += block->length;
        }

        total_sz += seg_sz;
        AlluxioDisconnectDir(resHandle);
        destoryGpalluxioHandler(resHandle);
        pfree(segDir);
    }

    *totalpages = (total_sz + (BLCKSZ - 1)) / BLCKSZ;
    *func = alluxio_acquire_sample_rows;
    return true;
}
