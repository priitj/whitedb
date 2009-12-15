# $Id:  $
# $Source:  $
#
# Compile test program(s)


# ---- options ----

# ---- targets ----

noinst_PROGRAMS = basic_api raptortry 

# ---- extra dependencies, flags, etc -----

raptortry_CFLAGS = `raptor-config --cflags`
raptortry_LDFLAGS = -static `raptor-config --libs`

# ----- all sources for the created programs -----

raptortry_SOURCES = raptortry.c
raptortry_LDADD = ../Main/libwgdb.la

basic_api_SOURCES = basic_api.c
basic_api_LDADD = ../Main/libwgdb.la
