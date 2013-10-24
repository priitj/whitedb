# $Id:  $
# $Source:  $
#
# Creating the WhiteDB binaries


# ---- options ----

# ---- path variables ----

dbdir=../Db

# ---- targets ----

lib_LTLIBRARIES = libwgdb.la
bin_PROGRAMS = wgdb stresstest indextool
pkginclude_HEADERS = $(dbdir)/dbapi.h  $(dbdir)/rdfapi.h $(dbdir)/indexapi.h


# ---- WhiteDB sources in C -----

# - - - - main db sources - - - 

dbfiles = $(dbdir)/dbmem.c $(dbdir)/dbmem.h\
  $(dbdir)/dballoc.c $(dbdir)/dballoc.h $(dbdir)/dbfeatures.h\
  $(dbdir)/dbdata.c $(dbdir)/dbdata.h\
  $(dbdir)/dbtest.c $(dbdir)/dbtest.h\
  $(dbdir)/dblock.c $(dbdir)/dblock.h\
  $(dbdir)/dbdump.c $(dbdir)/dbdump.h $(dbdir)/crc1.h\
  $(dbdir)/dblog.c $(dbdir)/dblog.h\
  $(dbdir)/dbhash.c $(dbdir)/dbhash.h\
  $(dbdir)/dbindex.c $(dbdir)/dbindex.h\
  $(dbdir)/dbcompare.c $(dbdir)/dbcompare.h\
  $(dbdir)/dbquery.c $(dbdir)/dbquery.h\
  $(dbdir)/dbutil.c $(dbdir)/dbutil.h\
  $(dbdir)/dbmpool.c $(dbdir)/dbmpool.h

# ---- extra dependencies, flags, etc -----

# This should be automatically detected now. If not,
# the configure script provides a "--with-gcc-arch" option.
#libwgdb_la_CFLAGS = -march=pentium4
libwgdb_la_CFLAGS =
LIBDEPS =

if RAPTOR
libwgdb_la_CFLAGS += `$(RAPTOR_CONFIG) --cflags`
LIBDEPS += `$(RAPTOR_CONFIG) --libs`
endif

wgdb_LDFLAGS = $(LIBDEPS)

stresstest_LIBS=$(PTHREAD_LIBS)
stresstest_CFLAGS=$(PTHREAD_CFLAGS)
stresstest_LDFLAGS= -static $(PTHREAD_CFLAGS) $(LIBDEPS)
stresstest_CC=$(PTHREAD_CC)

indextool_LDFLAGS = $(LIBDEPS)

# ----- all sources for the created programs -----

libwgdb_la_SOURCES = $(dbfiles)

wgdb_SOURCES = wgdb.c
wgdb_LDADD = libwgdb.la

stresstest_SOURCES = stresstest.c
stresstest_LDADD = libwgdb.la

indextool_SOURCES = indextool.c
indextool_LDADD = libwgdb.la
