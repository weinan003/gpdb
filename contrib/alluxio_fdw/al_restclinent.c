#include <string.h>
#include "postgres.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
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
#include "commands/vacuum.h"

#include "utils/memutils.h"

#include "catalog/catalog.h"
#include <json-c/json.h>
#include "al_restclient.h"

#define ALLUXIO_BUFFER_SZ (4 * 1024 * 1024)
#define MAX_CHAR_LENGTH_OF_INT64 20

AlluxioRelationHandler *BeginAlluxioHandler(char *relpath, char *localdir)
{
    AlluxioRelationHandler  *handler;
    MemoryContext           oldcontext;

    /*
    DIR *dir = opendir(localdir);
    if(dir)
        closedir(dir);
    else
        elog(ERROR, "can not find directory: %s ",localdir);
    */

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
    appendStringInfoString(handler->relpath,relpath);

    if(localdir)
    {
        handler->localdir = makeStringInfo();
        appendStringInfoString(handler->localdir,localdir);
    }

    AlluxioConnectDir(handler);

    handler->blockItr = list_head(handler->blockLst);
    MemoryContextSwitchTo(oldcontext);

    return handler;
}

void EndAlluxioHandler(AlluxioRelationHandler *handler)
{
    AlluxioDisconnectDir(handler);
    MemoryContextDelete(handler->alluxiocontext);
    pfree(handler);
}


MemTuple NextTupleFromAlluxioHandler(AlluxioRelationHandler *handler)
{
    MemTuple tuple = NULL;
    int rollback_offset = 0;

    vacuum_delay_point();

    if(handler->buffer.len != handler->buffer.cursor)
    {
        tuple = (MemTuple) (handler->buffer.data + handler->buffer.cursor);
        uint32 tuple_len = memtuple_get_size(tuple);

        if (handler->buffer.cursor + tuple_len > handler->buffer.len)
        {
            rollback_offset = handler->buffer.len - handler->buffer.cursor;
            tuple = NULL;
        } else
            handler->buffer.cursor += tuple_len;
    }


    //if handler->blockItr is NULL , all data blocks have been iterated through
    if(!tuple && handler->blockItr)
    {
        datablock *pBlock = (datablock *)lfirst(handler->blockItr);
        pBlock->cur_offset = pBlock->cur_offset - rollback_offset;
        if(pBlock->localfd == 0)
        {
            char *localCachePath = palloc0(handler->localdir->len + MAX_CHAR_LENGTH_OF_INT64);
            sprintf(localCachePath,"%s/%ld",handler->localdir->data,pBlock->localid);

            struct stat stat_buf;

            //if data does not cache in local, pull down first
            if(stat(localCachePath,&stat_buf))
            {
                int     streammingid;
                char    *alluxio_url;
                alluxio_url = palloc0(handler->relpath->len + MAX_CHAR_LENGTH_OF_INT64);
                sprintf(alluxio_url,"%s/%d",handler->relpath->data,pBlock->id);

                streammingid = alluxioOpenFile(alluxio_url);
                alluxioCacheData(streammingid);
                alluxioClose(streammingid);

                if(stat(localCachePath,&stat_buf))
                    elog(ERROR,"Failed to cache block : %s, in local path : %s", alluxio_url, localCachePath);

                pfree(alluxio_url);
            }

            pBlock->localfd = open(localCachePath,O_RDONLY);

            if(pBlock->localfd < 0)
                elog(ERROR, "Failed to open data cache file: %s",localCachePath);

            pfree(localCachePath);
        }

        //refill buffer
        handler->buffer.cursor = 0;
        lseek(pBlock->localfd,pBlock->cur_offset,SEEK_SET);
        int sz = read(pBlock->localfd, handler->buffer.data, handler->buffer.maxlen);
        pBlock->cur_offset += sz;
        handler->buffer.len = sz;

        if (pBlock->cur_offset == pBlock->length)
        {
            close(pBlock->localfd);
            handler->blockItr = lnext(handler->blockItr);
        }

        tuple = (MemTuple) (handler->buffer.data + handler->buffer.cursor);
        uint32 tuple_len = memtuple_get_size(tuple);
        handler->buffer.cursor += tuple_len;

    }

    return tuple;
}
