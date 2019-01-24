# contrib/alluxio_fdw/Makefile

MODULE_big = alluxio_fdw
OBJS = alluxioop.o alluxio_fdw.o alluxiofs.o 
EXTENSION = alluxio_fdw
DATA = alluxio_fdw--1.0.sql

PG_CPPFLAGS += $(COMMON_CPP_FLAGS) -Iinclude -Ilib -I$(libpq_srcdir) -I$(libpq_srcdir)/postgresql/server/utils -I/usr/local/include
PG_CPPFLAGS += -g

SHLIB_LINK_INTERNAL = $(libpq)
SHLIB_LINK += $(COMMON_LINK_OPTIONS) -ljson-c -lcurl

LDLIBS += -ljson-c -lcurl

# REGRESS = file_fdw gp_file_fdw
# EXTRA_CLEAN = sql/file_fdw.sql expected/file_fdw.out sql/gp_file_fdw.sql expected/gp_file_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/alluxio_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
