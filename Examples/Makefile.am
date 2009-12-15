# $Id:  $
# $Source:  $
#
# Compile test program(s)


# ---- options ----

# ---- targets ----

noinst_PROGRAMS = basic_api raptortry 

# ---- path variables ----

dbdir=../Db

dbfiles = $(dbdir)/dbmem.c $(dbdir)/dbmem.h\
  $(dbdir)/dballoc.c $(dbdir)/dballoc.h\
  $(dbdir)/dbdata.c $(dbdir)/dbdata.h\
  $(dbdir)/dbtest.c $(dbdir)/dbtest.h\
  $(dbdir)/dblock.c $(dbdir)/dblock.h\
  $(dbdir)/dbdump.c $(dbdir)/dbdump.h\
  $(dbdir)/dblog.c $(dbdir)/dblog.h\
  $(dbdir)/dbhash.c $(dbdir)/dbhash.h
#  $(dbdir)/dbindex.c $(dbdir)/dbindex.h

 
# ---- extra dependencies, flags, etc -----

basic_api_CFLAGS = -march=pentium4

raptortry_CFLAGS = `raptor-config --cflags`
raptortry_LDFLAGS = `raptor-config --libs`

# ----- all sources for the created programs -----

raptortry_SOURCES = raptortry.c $(dbfiles)

basic_api_SOURCES = basic_api.c
basic_api_LDADD = ../Main/libwgdb.la


