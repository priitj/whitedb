# $Id:  $
# $Source:  $
#
# Compile test program(s)


# ---- options ----

# ---- targets ----

noinst_PROGRAMS = demo query
#if RAPTOR
#noinst_PROGRAMS += raptortry
#endif

# ---- extra dependencies, flags, etc -----

LIBDEPS = -lm   # dependency from libm round() should be removed
if RAPTOR
LIBDEPS += `$(RAPTOR_CONFIG) --libs`
endif

#if RAPTOR
#raptortry_CFLAGS = $(AM_CFLAGS) `$(RAPTOR_CONFIG) --cflags`
#endif

AM_LDFLAGS = $(LIBDEPS)

# ----- all sources for the created programs -----

#raptortry_SOURCES = raptortry.c
#raptortry_LDADD = ../Main/libwgdb.la

demo_SOURCES = demo.c
demo_LDADD = ../Main/libwgdb.la

query_SOURCES = query.c
query_LDADD = ../Main/libwgdb.la ../Test/libTest.la
