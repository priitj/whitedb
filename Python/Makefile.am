# $Id:  $
# $Source:  $
#
# Compile Python extension module


# ---- options ----

# ---- targets ----

pyexec_LTLIBRARIES = wgdb.la

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

INCLUDES = $(PYTHON_INCLUDES) -I $(dbdir)
wgdb_la_CFLAGS = -march=pentium4
wgdb_la_LDFLAGS = -module -avoid-version

# ----- all sources for the created programs -----

wgdb_la_SOURCES = wgdbmodule.c $(dbfiles)
