#ifndef __ALLUXIO_OP_H__
#define __ALLUXIO_OP_H__

#include "postgres.h"
#include "utils/resowner.h"
#include "alluxiofs.h"

typedef struct alluxioHandler {
    ResourceOwner owner; /* owner of this handle */

    ListCell *blockiter; /* iterator for alluxio file blocks */
    List *blocksinfo;

    struct alluxioHandler *next;
    struct alluxioHandler *prev;

    char *url;
}alluxioHandler;

typedef struct alluxioblock
{
    int		order;
    char 	*name; /* block name */
    size_t	length; /* block length, does not be assigned when do writing process */
    bool 	writable; /* redundant tag,for convenience mark block */
    int		streammingid; /* alluxio assiged id for r/w streaming , only work after create/open file ,default value is 0 */
}alluxioBlock;

alluxioHandler* createGpalluxioHander(void );

void destoryGpalluxioHandler(alluxioHandler* handler);

void abortGpalluxioCallback(ResourceReleasePhase phase,bool isCommit,bool isToplevel,void *arg);

void AlluxioConnectDir(alluxioHandler *handler);

void AlluxioDisconnectDir(alluxioHandler *handler);

int32 AlluxioRead(alluxioHandler *handler,char *buffer,int32 length);

struct _alluxioCache* AlluxioDirectRead(alluxioHandler *handler);

int32 AlluxioWrite(alluxioHandler *handler,char *buffer,int32 length);

void AlluxioFileSync(alluxioHandler *handler);

#endif