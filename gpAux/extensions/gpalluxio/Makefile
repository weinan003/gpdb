SHLIB_LINK += $(COMMON_LINK_OPTIONS) -ljson-c -lcurl
PG_CPPFLAGS += $(COMMON_CPP_FLAGS) -Iinclude -Ilib -I$(libpq_srcdir) -I$(libpq_srcdir)/postgresql/server/utils

PG_CPPFLAGS += -g

# Targets
MODULE_big = gpalluxio
OBJS = gpalluxio.o alluxiofs.o alluxioop.o

# Launch
ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
top_builddir = ../../..
-include $(top_builddir)/src/Makefile.global
-include $(top_srcdir)/contrib/contrib-global.mk
endif


cleanall:
	rm -f *.o *.so *.a
