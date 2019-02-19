#include <string.h>
#include "postgres.h"

#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "libpq/auth.h"
#include "libpq/pqformat.h"
#include "utils/workfile_mgr.h"
#include "utils/guc.h"
#include "utils/faultinjector.h"

#include "utils/memutils.h"

#include "catalog/catalog.h"
#include <curl/curl.h>
#include <json-c/json.h>
#include "al_restclient.h"

#define ALLUXIO_BUFFER_SZ (4 * 1024 * 1024)

static CURL *curl = NULL;

AlluxioRelationHandler *BeginAlluxioHandler(char *relpath, char *localdir)
{
    AlluxioRelationHandler  *handler;
    MemoryContext           *oldcontext;

    DIR *dir = opendir(localdir);
    if(dir)
        closedir(dir);
    else
        elog(ERROR, "can not find directory: %s ",localdir);

    handler = (AlluxioRelationHandler*)MemoryContextAlloc(TopMemoryContext,sizeof(AlluxioRelationHandler));

    handler->alluxiocontext = AllocSetContextCreate(CurrentMemoryContext,
                                                    "alluxio_fdw temporary context",
                                                    ALLOCSET_DEFAULT_MINSIZE,
                                                    ALLOCSET_DEFAULT_INITSIZE,
                                                    ALLOCSET_DEFAULT_MAXSIZE);

    handler->blockLst = NULL;
    handler->blockItr = NULL;

    oldcontext = MemoryContextSwitchTo(handler->alluxiocontext);

    initStringInfoOfSize(&handler->buffer,ALLUXIO_BUFFER_SZ);
    handler->relpath = makeStringInfo();
    handler->localdir = makeStringInfo();
    appendStringInfoString(handler->relpath,relpath);
    appendStringInfoString(handler->localdir,localdir);

    AlluxioConnectDir(handler);

    handler->blockItr = handler->blockLst;
    MemoryContextSwitchTo(oldcontext);

    return handler;
}

void EndAlluxioHandler(AlluxioRelationHandler *handler)
{
    MemoryContextDelete(handler->alluxiocontext);
    pfree(handler);
}
