#include "alluxioop.h"
#include "alluxiofs.h"
#include <json-c/json.h>
#include <curl/curl.h>
#include <string.h>

static alluxioHandler *currHandler = NULL;
extern struct _alluxioCache alluxioCache;

alluxioHandler* createGpalluxioHander()
{
    alluxioHandler *handler;

    handler = (alluxioHandler *)MemoryContextAlloc(TopMemoryContext,sizeof(alluxioHandler));

    handler->owner = CurrentResourceOwner;
    handler->next = currHandler;
    handler->prev = NULL;
    handler->blocksinfo = NULL;
    handler->blockiter = NULL;

    if(currHandler)
        currHandler->prev = handler;

    currHandler = handler;

    return handler;
}

void destoryGpalluxioHandler(alluxioHandler* handler)
{
    if (handler == NULL) return;

    if(handler->prev)
        handler->prev->next = handler->next;
    else
        currHandler = handler->next;

    if(handler->next)
        handler->next->prev = handler->prev;

    pfree(handler);
}

void abortGpalluxioCallback(ResourceReleasePhase phase,bool isCommit,bool isToplevel,void *arg)
{
    alluxioHandler *curr;

    if(phase != RESOURCE_RELEASE_AFTER_LOCKS) return;

    curr = currHandler;
    while (curr)
    {

        if(curr->owner == CurrentResourceOwner)
        {
            if(isCommit)
                elog(WARNING, "gpalluxio external table reference leak: %p still referenced", curr);

            AlluxioDisconnectDir(curr);
            destoryGpalluxioHandler(curr);
        }

        curr = curr->next;
    }
}

inline static
size_t traverseFileCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    char* ret = (char *)contents;
    AlluxioRelationHandler *handler = (AlluxioRelationHandler *)userp;
    List **blocklst = &(handler->blockLst);

    appendBinaryStringInfo(&handler->buffer, ret, size * nmemb);
    json_object *jobj = json_tokener_parse(handler->buffer.data);

    if(json_type_array == json_object_get_type(jobj)) {
        int blocknum = json_object_array_length(jobj);
        int dirLength = strlen(handler->relpath->data);

        datablock **ab_array = palloc0(sizeof(datablock *) * blocknum);

        for(int i = 0 ;i < blocknum; i ++)
        {
            json_object *blocksz,*blockname,*blockidarray,*blockid;
            json_object *block = json_object_array_get_idx(jobj,i);

            datablock* ab = palloc0(sizeof(datablock));

            json_object_object_get_ex(block,"length",&blocksz);
            json_object_object_get_ex(block,"name",&blockname);
            json_object_object_get_ex(block,"blockIds",&blockidarray);
            blockid = json_object_array_get_idx(blockidarray,0);
            ab->localid = json_object_get_int64(blockid);

            ab->length = json_object_get_int(blocksz);
            const char* name = json_object_get_string(blockname);
            ab->id = atoi(name);

            ab_array[ab->id - 1] = ab;
        }

        for (int i = 0; i < blocknum; i++)
            *blocklst = lappend(*blocklst, ab_array[i]);

        pfree(ab_array);
        resetStringInfo(&handler->buffer);
    }

    json_object_put(jobj);

    return size * nmemb;
}

void AlluxioConnectDir(AlluxioRelationHandler *handler)
{
    if(!alluxioPathExist(handler->relpath->data))
        alluxioMakeDirectory(handler->relpath->data,NULL);

    alluxioListStatus(handler->relpath->data,traverseFileCallback,handler);
}

void AlluxioDisconnectDir(alluxioHandler *handler)
{
    AlluxioFileSync(handler);

    ListCell *lc = NULL;
    foreach(lc,handler->blocksinfo)
    {
        pfree(((alluxioBlock *)lfirst(lc))->name);
        pfree((alluxioBlock *)lfirst(lc));
    }
    list_free(handler->blocksinfo);

    handler->url = NULL;
}

int32 AlluxioRead(alluxioHandler *handler,char *buffer,int32 length)
{
    int readLength;
    int ret;
    ret = 0;
    readLength = 0;
    char* ptr = buffer;
    int remainToRead = length;

    ret = alluxioCacheRead(0,ptr,remainToRead);
    remainToRead -= ret;
    ptr += ret;
    readLength +=ret;

    while (handler->blockiter && remainToRead)
    {
        ((alluxioBlock *)lfirst(handler->blockiter))->streammingid =
                alluxioOpenFile(((alluxioBlock *)lfirst(handler->blockiter))->name);
        size_t sz = ((alluxioBlock*)lfirst(handler->blockiter))->length;

        if(sz <= remainToRead)
            ret = alluxioRead(((alluxioBlock *)lfirst(handler->blockiter))->streammingid, ptr , remainToRead);
        else
            ret = alluxioCacheRead(((alluxioBlock *)lfirst(handler->blockiter))->streammingid, ptr , remainToRead);

        remainToRead -= ret;
        ptr += ret;
        readLength += ret;

        alluxioClose(((alluxioBlock *)lfirst(handler->blockiter))->streammingid);
        ((alluxioBlock *)lfirst(handler->blockiter))->streammingid = 0;
        handler->blockiter = lnext(handler->blockiter);
    }

    return readLength;
}

int32 AlluxioWrite(alluxioHandler *handler,char *buffer,int32 length)
{
    int32 returnCode;
    if(handler->blockiter &&
    (((alluxioBlock*)lfirst(handler->blockiter))->length + length> ALLUXIO_CACHE_SZ)
    )
    {
        AlluxioFileSync(handler);
    }

    if(!handler->blockiter || !((alluxioBlock*)lfirst(handler->blockiter))->writable )
    {
        int it_id = list_length(handler->blocksinfo) + 1;
        alluxioBlock *p_block = palloc0(sizeof(alluxioBlock));

        p_block->name = palloc0(strlen(handler->url) + 16);
        sprintf(p_block->name,"%s/%d",handler->url,it_id);
        p_block->streammingid = alluxioCreateFile(p_block->name);
        p_block->writable = true;
        p_block->length = 0;

        handler->blocksinfo = lappend(handler->blocksinfo,p_block);
        handler->blockiter = handler->blocksinfo->tail;

    }
    returnCode = alluxioWrite(((alluxioBlock *)lfirst(handler->blockiter))->streammingid,buffer,length);

    if (returnCode >= 0)
    {
        ((alluxioBlock *)lfirst(handler->blockiter))->length +=returnCode;
    }
    else
    {
        /* Trouble, so assume we don't know the file position anymore */
        ((alluxioBlock *)lfirst(handler->blockiter))->length = -1;
    }

    return returnCode;
}

void AlluxioFileSync(alluxioHandler *handler){

    if(handler->blockiter
            && (((alluxioBlock*)lfirst(handler->blockiter))->streammingid))
    {
        alluxioClose(((alluxioBlock *)lfirst(handler->blockiter))->streammingid);
        ((alluxioBlock *)lfirst(handler->blockiter))->streammingid = 0;
        ((alluxioBlock *)lfirst(handler->blockiter))->writable = false;
    }

}

struct _alluxioCache* AlluxioDirectRead(alluxioHandler *handler)
{
    struct _alluxioCache *cache = NULL;
    int streammingid;
    if(handler->blockiter) {
        streammingid = alluxioOpenFile(((alluxioBlock *)lfirst(handler->blockiter))->name);

        cache = alluxioDirectRead(streammingid);
        alluxioClose(streammingid);
        handler->blockiter = lnext(handler->blockiter);

    }

    return cache;
}
