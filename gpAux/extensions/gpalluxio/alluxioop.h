#ifndef __ALLUXIO_OP_H__
#define __ALLUXIO_OP_H__

#include "postgres.h"
#include "utils/resowner.h"

typedef struct alluxioHandler {
    ResourceOwner owner; /* owner of this handle */

    ListCell *blockiter; /* iterator for alluxio file blocks */
    List *blocksinfo;

    struct alluxioHandler *next;
    struct alluxioHandler *prev;

    char *url;
}alluxioHandler;

alluxioHandler* createGpalluxioHander(void );

void destoryGpalluxioHandler(alluxioHandler* handler);

void abortGpalluxioCallback(ResourceReleasePhase phase,bool isCommit,bool isToplevel,void *arg);

void AlluxioConnectDir(alluxioHandler *handler);

void AlluxioDisconnectDir(alluxioHandler *handler);

int32 AlluxioRead(alluxioHandler *handler,char *buffer,int32 length);

int32 AlluxioWrite(alluxioHandler *handler,char *buffer,int32 length);

void AlluxioFileSync(alluxioHandler *handler);

#endif