//
// Created by Weinan Wang on 1/28/19.
//

#ifndef GPDB_ALLUXIO_FDW_C_H
#define GPDB_ALLUXIO_FDW_C_H
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "commands/copy.h"
#include "commands/explain.h"

typedef struct AlluxioFdwPlanState
{
    char	   *filename;		/* file to read */
    List	   *options;		/* merged COPY options, excluding filename */
    BlockNumber pages;			/* estimate of file's physical size */
    double		ntuples;		/* estimate of number of rows in file */
} AlluxioFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct AlluxioFdwExecutionState
{
    char	   *filename;		/* file to read */
    List	   *options;		/* merged COPY options, excluding filename */
    CopyState	cstate;			/* state of reading file */
    int         rows;
} AlluxioFdwExecutionState;

extern bool
alluxioAnalyzeForeignTable(Relation relation,
                           AcquireSampleRowsFunc *func,
                           BlockNumber *totalpages);
extern void
alluxioGetForeignRelSize(PlannerInfo *root,
                         RelOptInfo *baserel,
                         Oid foreigntableid);
void
alluxioGetForeignPaths(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid);

ForeignScan *
alluxioGetForeignPlan(PlannerInfo *root,
                   RelOptInfo *baserel,
                   Oid foreigntableid,
                   ForeignPath *best_path,
                   List *tlist,
                   List *scan_clauses);
void
alluxioBeginForeignScan(ForeignScanState *node, int eflags);

void
alluxioExplainForeignScan(ForeignScanState *node, ExplainState *es);

void
alluxioEndForeignScan(ForeignScanState *node);
#endif //GPDB_ALLUXIO_FDW_C_H
