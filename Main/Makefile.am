# $Id:  $
# $Source:  $
#
# Creating the WhiteDB binaries


# ---- options ----

# ---- path variables ----

dbdir=../Db
printerdir=../Printer
parserdir=../Parser
reasonerdir=../Reasoner
jsondir=../json

# ---- targets ----

lib_LTLIBRARIES = libwgdb.la
bin_PROGRAMS = wgdb indextool
noinst_PROGRAMS = stresstest
pkginclude_HEADERS = $(dbdir)/dbapi.h  $(dbdir)/rdfapi.h $(dbdir)/indexapi.h

# ---- extra dependencies, flags, etc -----

LIBDEPS =
if RAPTOR
LIBDEPS += `$(RAPTOR_CONFIG) --libs`
endif

AM_LDFLAGS = $(LIBDEPS)

stresstest_LIBS=$(PTHREAD_LIBS)
stresstest_CFLAGS=$(AM_CFLAGS) $(PTHREAD_CFLAGS)
stresstest_LDFLAGS= -static $(PTHREAD_CFLAGS) $(LIBDEPS)
stresstest_CC=$(PTHREAD_CC)

libwgdb_la_LDFLAGS =

# ----- all sources for the created programs -----

libwgdb_la_SOURCES =
libwgdb_la_LIBADD = $(dbdir)/libDb.la ${jsondir}/libjson.la
if REASONER
libwgdb_la_LIBADD += $(parserdir)/libParser.la \
  $(printerdir)/libPrinter.la $(reasonerdir)/libReasoner.la
endif

wgdb_SOURCES = wgdb.c
wgdb_LDADD = libwgdb.la

stresstest_SOURCES = stresstest.c
stresstest_LDADD = libwgdb.la

indextool_SOURCES = indextool.c
indextool_LDADD = libwgdb.la
