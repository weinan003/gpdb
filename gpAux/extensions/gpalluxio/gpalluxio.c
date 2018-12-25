#include "postgres.h"

#include "cdb/cdbvars.h"
#include "access/extprotocol.h"
#include "access/xact.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_proc.h"
#include "fmgr.h"
#include "funcapi.h"
#include "port.h"  //for pg_strncasecmp
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "alluxioop.h"
#include "alluxiofs.h"


PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(alluxio_export);
PG_FUNCTION_INFO_V1(alluxio_import);

Datum alluxio_export(PG_FUNCTION_ARGS);
Datum alluxio_import(PG_FUNCTION_ARGS);
static bool isAbortCallbackRegistered;


Datum alluxio_export(PG_FUNCTION_ARGS) {
    /* Must be called via the external table format manager */
    if (!CALLED_AS_EXTPROTOCOL(fcinfo))
        elog(ERROR, "extprotocol_import: not called by external protocol manager");

    extern struct _alluxioCache alluxioCache;
    /* Get our internal description of the protocol */
    alluxioHandler *resHandle = (alluxioHandler *)EXTPROTOCOL_GET_USER_CTX(fcinfo);

    /* last call. destroy writer */
    if (EXTPROTOCOL_IS_LAST_CALL(fcinfo)) {

        if(alluxioCache.end - alluxioCache.begin > 0)
        {
            AlluxioWrite(resHandle,alluxioCache.begin,alluxioCache.end - alluxioCache.begin);

            bzero(alluxioCache.buffer,alluxioCache.sz);
            alluxioCache.end = alluxioCache.begin;
        }

        AlluxioDisconnectDir(resHandle);
        destoryGpalluxioHandler(resHandle);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
        PG_RETURN_INT32(0);
    }

    /* first call. do any desired init */
    if (resHandle == NULL) {
        if (!isAbortCallbackRegistered) {
            RegisterResourceReleaseCallback(abortGpalluxioCallback, NULL);
            alluxioInit();
            isAbortCallbackRegistered = true;
        }
        resHandle = createGpalluxioHander();

        const char* url = EXTPROTOCOL_GET_URL(fcinfo);
        resHandle->url = palloc0(strlen(url) + 4);
        sprintf(resHandle->url,"http://%s/%d",url + 10,GpIdentity.segindex);

        AlluxioConnectDir(resHandle);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, resHandle);
    }

    char *data_buf = EXTPROTOCOL_GET_DATABUF(fcinfo);
    int32 data_len = EXTPROTOCOL_GET_DATALEN(fcinfo);


    if(alluxioCache.end - alluxioCache.begin + data_len > alluxioCache.sz)
    {
        AlluxioWrite(resHandle,alluxioCache.begin,alluxioCache.end - alluxioCache.begin);

        bzero(alluxioCache.buffer,alluxioCache.sz);
        alluxioCache.end = alluxioCache.begin;
    }

    memcpy(alluxioCache.end,data_buf,data_len);
    alluxioCache.end += data_len;

    PG_RETURN_INT32(data_len);
}

Datum alluxio_import(PG_FUNCTION_ARGS)
{
    if (!CALLED_AS_EXTPROTOCOL(fcinfo))
        elog(ERROR, "extprotocol_import: not called by external protocol manager");

    /* Get our internal description of the protocol */
    alluxioHandler *resHandle = (alluxioHandler *)EXTPROTOCOL_GET_USER_CTX(fcinfo);

    /* last call. destroy writer */
    if (EXTPROTOCOL_IS_LAST_CALL(fcinfo)) {

        AlluxioDisconnectDir(resHandle);
        destoryGpalluxioHandler(resHandle);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
        PG_RETURN_INT32(0);
    }

    /* first call. do any desired init */
    if (resHandle == NULL) {
        if (!isAbortCallbackRegistered) {
            RegisterResourceReleaseCallback(abortGpalluxioCallback, NULL);
            alluxioInit();
            isAbortCallbackRegistered = true;
        }
        resHandle = createGpalluxioHander();

        const char* url = EXTPROTOCOL_GET_URL(fcinfo);
        resHandle->url = palloc0(strlen(url) + 4);
        sprintf(resHandle->url,"http://%s/%d",url + 10,GpIdentity.segindex);

        AlluxioConnectDir(resHandle);
        resHandle->blockiter = list_head(resHandle->blocksinfo);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, resHandle);
    }

    char *data_buf = EXTPROTOCOL_GET_DATABUF(fcinfo);
    int32 data_len = EXTPROTOCOL_GET_DATALEN(fcinfo);


    data_len = AlluxioRead(resHandle,data_buf,data_len);

    PG_RETURN_INT32(data_len);
}
