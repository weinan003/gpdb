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
#include "utils/memutils.h"
#include "utils/rel.h"

#include "alluxioop.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(alluxio_fdw_handler);
PG_FUNCTION_INFO_V1(alluxio_fdw_validator);

static bool
alluxioAnalyzeForeignTable(Relation relation,
                           AcquireSampleRowsFunc *func,
                           BlockNumber *totalpages);

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

Datum
alluxio_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /*
    fdwroutine->GetForeignRelSize = fileGetForeignRelSize;
    fdwroutine->GetForeignPaths = fileGetForeignPaths;
    fdwroutine->GetForeignPlan = fileGetForeignPlan;
    fdwroutine->ExplainForeignScan = fileExplainForeignScan;
    fdwroutine->BeginForeignScan = fileBeginForeignScan;
    fdwroutine->IterateForeignScan = fileIterateForeignScan;
    fdwroutine->ReScanForeignScan = fileReScanForeignScan;
    fdwroutine->EndForeignScan = fileEndForeignScan;
     */

    fdwroutine->AnalyzeForeignTable = alluxioAnalyzeForeignTable;
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
    *totalrows = 0;
    *totaldeadrows = 0;

    tupcontext = AllocSetContextCreate(CurrentMemoryContext,
                                       "file_fdw temporary context",
                                       ALLOCSET_DEFAULT_MINSIZE,
                                       ALLOCSET_DEFAULT_INITSIZE,
                                       ALLOCSET_DEFAULT_MAXSIZE);


    int segNum = getgpsegmentCount();
    for(int i = 0;i < segNum && numrow < targrows; i++)
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

            ExecStoreGenericTuple(mtuple,slot,false);
            slot_getallattrs(slot);
            rows[numrow++]= heap_form_tuple(slot->tts_tupleDescriptor,
                    slot_get_values(slot),
                    slot_get_isnull(slot));

            if(numrow == targrows)
                break;
        }

        AlluxioDisconnectDir(resHandle);
        destoryGpalluxioHandler(resHandle);
        pfree(segDir);
    }

    ExecDropSingleTupleTableSlot(slot);
    MemoryContextDelete(tupcontext);

    *totalrows = numrow;
    return numrow;
}

static bool
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
