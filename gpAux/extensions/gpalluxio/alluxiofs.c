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
#include "alluxiofs.h"
#include "libpq/auth.h"
#include "libpq/pqformat.h"
#include "utils/workfile_mgr.h"
/* Debug_filerep_print guc temporaly added for troubleshooting */
#include "utils/guc.h"
#include "utils/faultinjector.h"

#include "utils/memutils.h"

#include "catalog/catalog.h"
#include <curl/curl.h>
#include <json-c/json.h>
#define  ALLUXIO_BUFFER_SZ	100000


static CURL *curl = 0;
static char alluxio_buffer[ALLUXIO_BUFFER_SZ];
char* alluxio_url = "localhost:39999/api/v1/paths//gpdb";
size_t alluxio_blocksize = 0;
struct _alluxioCache alluxioCache = {NULL,0,NULL,NULL};

void
alluxioInit()
{
    //if(!ISHDFSMOD)
    {
        curl_global_init(CURL_GLOBAL_ALL);

        if(!alluxioCache.buffer)
            alluxioCache.buffer = malloc(ALLUXIO_CACHE_SZ);

        alluxioCache.sz = ALLUXIO_CACHE_SZ;
        RESET_ALLUXIOBUFFER();
    }
}

void
alluxioDestory()
{
    //if(!ISHDFSMOD)
    {
        curl_global_cleanup();

        alluxioCache.sz = 0;
        alluxioCache.begin = NULL;
        alluxioCache.end = NULL;
    }
}

void
alluxioListStatus(char* path,
                  size_t (*callback)(void *contents, size_t size, size_t nmemb, void *userp),
                  void* ret)
{
    struct curl_slist *headers = NULL;
    if((curl = curl_easy_init()))
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);
        sprintf(alluxio_buffer,"%s/list-status/", path);
        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);

        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "{}");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, -1L);
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,callback);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,ret);

        curl_easy_perform(curl);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
}

int
alluxioMakeDirectory(const char* path, mode_t mode)
{
    curl = curl_easy_init();

    if(curl)
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);
        sprintf(alluxio_buffer,"%s/create-directory/", path);
        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);


        struct curl_slist *header = NULL;
        header = curl_slist_append(header,"Content-Type: application/json");
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,header);

        //set json body
        json_object *json = json_object_new_object();
        json_object_object_add(json,"recursive",json_object_new_boolean(true));

        json_object_object_add(json,"writeType",json_object_new_string("CACHE_THROUGH"));
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,json_object_to_json_string(json));
        curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);

        curl_easy_perform(curl);

        json_object_put(json);

        curl_slist_free_all(header);
        curl_easy_cleanup(curl);
    } else
    {
        elog(WARNING,"Failed to create directory by alluxio");
    }

    return 0;
}

bool alluxioPathExist(char *path)
{
    bool ret = false;
    struct curl_slist *headers = NULL;
    if((curl = curl_easy_init()))
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);
        sprintf(alluxio_buffer,"%s/get-status/", path);
        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);

        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "{}");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, -1L);
        CURLcode rcode = curl_easy_perform(curl);

        long http_code = 0;
        curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (rcode != CURLE_ABORTED_BY_CALLBACK && http_code == 200)
            ret = true;

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }

    return ret;
}

inline static
size_t int_callback(void* data, size_t size, size_t nmemb, void* content)
{
    long totalSize = size*nmemb;
    int* ret = (void *)content;
    json_object *jobj = json_tokener_parse((char *)data);
    *ret = json_object_get_int(jobj);
    json_object_put(jobj);
    return totalSize;
}

int alluxioCreateFile(char *path)
{
    int retCode = -1;

    curl = curl_easy_init();

    if(curl)
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);
        sprintf(alluxio_buffer,"%s/create-file?", path);
        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);


        struct curl_slist *header = NULL;
        header = curl_slist_append(header,"Content-Type: application/json");
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,header);

        //set json body
        json_object *json = json_object_new_object();
        json_object_object_add(json,"recursive",json_object_new_boolean(true));

        json_object_object_add(json,"writeType",json_object_new_string("CACHE_THROUGH"));
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,json_object_to_json_string(json));
        curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,int_callback);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,&retCode);

        curl_easy_perform(curl);

        json_object_put(json);
        curl_slist_free_all(header);
        curl_easy_cleanup(curl);
    } else
    {
        elog(WARNING,"Failed to create directory by alluxio");
    }

    return retCode;
}

/*
 * 先检查有没有cache
 *
 * 留一个动态的buffer char* 一次性把streaming中的数据在回调中读完
 *
 * 最多向buffer中拷贝 amount 这么多的，记录当前位置以及最大位置
 * */
struct readctx {
    char* buffer;
    int amount;
    int readsz;
} ;
inline static
size_t read_callback(void* contents, size_t size, size_t nmemb, void* userp)
{
    struct readctx* ctx = (struct readctx*)userp;
    if(ctx->amount >= size * nmemb)
    {
        memcpy(ctx->buffer,contents,size * nmemb);
        ctx->readsz = size * nmemb;
    }
    else
    {
        //TODO:
        elog(ERROR,"does noe handle this yet");
    }
    return size * nmemb;
}

inline static
size_t cacheread_callback(void* contents, size_t size, size_t nmemb, void* userp)
{
    memcpy(alluxioCache.end,contents,size * nmemb);
    alluxioCache.end += size * nmemb;

    return size * nmemb;
}

int alluxioCacheRead(int streammingid,char *buffer,int amount)
{
    int retCode = 0;
    //如果有id,读取data到cache中
    if(streammingid)
    {
        struct curl_slist *headers = NULL;
        curl = curl_easy_init();
        if(curl)
        {
            bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);

            strcpy(alluxio_buffer,alluxio_url);
            sprintf(alluxio_buffer,"http://%s",alluxio_url);
            char *pstr = strstr(alluxio_buffer,"/v1") + 3;
            int offset = sprintf(pstr,"/streams/%d/read?", streammingid);
            *(pstr + offset) = '\0';

            curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);

            headers = curl_slist_append(headers, "Content-Type: application/json");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl,CURLOPT_POSTFIELDS,"{}");
            curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);

            RESET_ALLUXIOBUFFER();

            curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,cacheread_callback);
            curl_easy_setopt(curl,CURLOPT_WRITEDATA,NULL);

            curl_easy_perform(curl);

            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
        }
    }

    //如果cache有内容，读取cache到buffer中
    if(alluxioCache.begin != alluxioCache.end)
    {
        size_t cachelth = alluxioCache.end - alluxioCache.begin;
        size_t copysz = cachelth < amount ? cachelth : amount;

        memcpy(buffer, alluxioCache.begin, copysz);
        alluxioCache.begin += copysz;
        retCode = copysz;
    }

    return retCode;
}

int alluxioRead(int streammingid,char *buffer,int amount)
{
    struct readctx ctx;
    struct curl_slist *headers = NULL;
    curl = curl_easy_init();
    if(curl)
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);


        strcpy(alluxio_buffer,alluxio_url);
        sprintf(alluxio_buffer,"http://%s",alluxio_url);
        char *pstr = strstr(alluxio_buffer,"/v1") + 3;
        int offset = sprintf(pstr,"/streams/%d/read?", streammingid);
        *(pstr + offset) = '\0';

        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);

        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,"{}");
        curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);

        ctx.buffer = buffer;
        ctx.amount = amount;
        ctx.readsz = 0;
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,read_callback);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,&ctx);

        curl_easy_perform(curl);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
    return ctx.readsz;

}

int alluxioWrite(int streammingid,const char *buffer,int amount)
{
    int ret = 0;
    struct curl_slist *headers = NULL;
    curl = curl_easy_init();
    if(curl)
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);

        strcpy(alluxio_buffer,alluxio_url);
        sprintf(alluxio_buffer,"http://%s",alluxio_url);
        char *pstr = strstr(alluxio_buffer,"/v1") + 3;
        int offset = sprintf(pstr,"/streams/%d/write?", streammingid);
        *(pstr + offset) = '\0';

        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);

        headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buffer);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, amount);
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,int_callback);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,&ret);
        curl_easy_perform(curl);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    } else
    {
        elog(WARNING,"Failed to write data by alluxio");
    }
    return ret;
}

int alluxioOpenFile(char *path)
{
    int retCode = -1;
    curl = curl_easy_init();

    if(curl)
    {
        bzero(alluxio_buffer,ALLUXIO_BUFFER_SZ);
        sprintf(alluxio_buffer,"%s/open-file?", path);
        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);


        struct curl_slist *header = NULL;
        header = curl_slist_append(header,"Content-Type: application/json");
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,header);

        json_object *json = json_object_new_object();
        json_object_object_add(json,"readType",json_object_new_string("CACHE"));
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,json_object_to_json_string(json));

        curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);
        curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,int_callback);
        curl_easy_setopt(curl,CURLOPT_WRITEDATA,&retCode);

        curl_easy_perform(curl);

        curl_slist_free_all(header);
        curl_easy_cleanup(curl);
    } else
    {
        elog(WARNING,"Failed to create directory by alluxio");
    }

    return retCode;
}

int alluxioClose(int streammingid)
{
    struct curl_slist *header = NULL;
    curl = curl_easy_init();
    if(curl) {
        bzero(alluxio_buffer, ALLUXIO_BUFFER_SZ);

        strcpy(alluxio_buffer,alluxio_url);
        sprintf(alluxio_buffer,"http://%s",alluxio_url);
        char *pstr = strstr(alluxio_buffer,"/v1") + 3;
        int offset = sprintf(pstr,"/streams/%d/close?", streammingid);
        *(pstr + offset) = '\0';

        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);
        header = curl_slist_append(header,"Content-Type: application/json");
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,header);
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,"{}");
        curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);
        curl_easy_perform(curl);

        curl_slist_free_all(header);
        curl_easy_cleanup(curl);
    }
    return 0;
}

int alluxioDelete(char* fileName,bool recursive)
{
    struct curl_slist *header = NULL;
    curl = curl_easy_init();
    if(curl) {

        bzero(alluxio_buffer, ALLUXIO_BUFFER_SZ);
        sprintf(alluxio_buffer,"%s/delete?", fileName);

        curl_easy_setopt(curl,CURLOPT_URL,alluxio_buffer);
        header = curl_slist_append(header,"Content-Type: application/json");
        curl_easy_setopt(curl,CURLOPT_HTTPHEADER,header);

        json_object *json = json_object_new_object();
        json_object_object_add(json,"recursive",json_object_new_boolean(true));
        json_object_object_add(json,"alluxioOnly",json_object_new_boolean(false));
        curl_easy_setopt(curl,CURLOPT_POSTFIELDS,json_object_to_json_string(json));

        curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,-1L);
        curl_easy_perform(curl);

        json_object_put(json);
        curl_slist_free_all(header);
        curl_easy_cleanup(curl);
    }
    return 0;
}
