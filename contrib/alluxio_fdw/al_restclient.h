#ifndef GPDB_AL_RESTCLIENT_H
#define GPDB_AL_RESTCLIENT_H

#include "postgres.h"
#include "utils/resowner.h"
#include "alluxiofs.h"
#include "alluxioop.h"
#include "utils/guc.h"

AlluxioRelationHandler *BeginAlluxioHandler(char *relpathtmp, char *localdir);

void EndAlluxioHandler(AlluxioRelationHandler *handler);

MemTuple NextTupleFromAlluxioHandler(AlluxioRelationHandler *handler);
#endif
