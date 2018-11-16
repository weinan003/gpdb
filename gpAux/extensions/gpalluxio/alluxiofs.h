#ifndef __ALLUXIO_FS_H__
#define __ALLUXIO_FS_H__

#include <dirent.h>
#include "postgres.h"

typedef struct _alluxioCache
{
    char *buffer;
    size_t sz;
    char *begin;
    char *end;
};

extern void alluxioInit();

extern void alluxioDestory();

extern void alluxioListStatus(char* path,
                  size_t (*callback)(void *contents, size_t size, size_t nmemb, void *userp),
                  void* ret);

extern int alluxioMakeDirectory(const char* path, mode_t mode);

extern bool alluxioPathExist(char *path);

extern int alluxioCreateFile(char *path);

extern int alluxioOpenFile(char *path);

extern int alluxioRead(int streammingid,char *buffer,int amount);

extern int alluxioCacheRead(int streammingid,char *buffer,int amount);

extern int alluxioWrite(int streammingid,const char *buffer,int amount);

extern int alluxioClose(int streammingid);

extern int alluxioDelete(char* fileName,bool recursive);

#define RESET_ALLUXIOBUFFER() do{ alluxioCache.begin = alluxioCache.end = alluxioCache.buffer; } while(0)
#define ALLUXIO_CACHE_SZ 64 * 1024 * 1024
#endif
