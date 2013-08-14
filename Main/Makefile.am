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
  $(dbdir)/dbmpool.c $(dbdir)/dbmpool.h\
  $(dbdir)/dbjson.c $(dbdir)/dbjson.h\
  $(jsondir)/JSON_parser.c $(jsondir)/JSON_parser.h\
  $(dbdir)/dbschema.c $(dbdir)/dbschema.h

# - - - - reasoner sources - - - - 

printerfiles = $(printerdir)/dbotterprint.c $(printerdir)/dbotterprint.h

parsergenfiles = $(parserdir)/dbotter.tab.c $(parserdir)/dbotter.tab.h\
  $(parserdir)/dbotter.yy.c $(parserdir)/dbprolog.tab.c\
  $(parserdir)/dbprolog.tab.h $(parserdir)/dbprolog.yy.c

parserfiles =  $(parserdir)/dbparse.c $(parserdir)/dbparse.h $(parserdir)/dbgenparse.h\
  $(parserdir)/dbotterparse.h\
  $(parserdir)/dbprologparse.h\
  $(parsergenfiles)

reasonerfiles =  $(reasonerdir)/types.h $(reasonerdir)/rincludes.h \
  $(reasonerdir)/printerrutils.c $(reasonerdir)/printerrutils.h\
  $(reasonerdir)/rmain.c $(reasonerdir)/rmain.h\
  $(reasonerdir)/rtest.c $(reasonerdir)/rtest.h\
  $(reasonerdir)/glb.c $(reasonerdir)/glb.h\
  $(reasonerdir)/rgenloop.c $(reasonerdir)/rgenloop.h\
  $(reasonerdir)/derive.c $(reasonerdir)/derive.h\
  $(reasonerdir)/subsume.c $(reasonerdir)/subsume.h\
  $(reasonerdir)/unify.c $(reasonerdir)/unify.h\
  $(reasonerdir)/build.c $(reasonerdir)/build.h\
  $(reasonerdir)/clstore.c $(reasonerdir)/clstore.h\
  $(reasonerdir)/clterm.c $(reasonerdir)/clterm.h\
  $(reasonerdir)/mem.c $(reasonerdir)/mem.h

# - - - -  reasoner sources for flex and bison and their compilation to c - - - -

$(parserdir)/dbotter.tab.c $(parserdir)/dbotter.tab.h: $(parserdir)/dbotter.y $(parserdir)/dbotterparse.h $(parserdir)/dbgenparse.h
	$(BISON)  -pwg_yyotter -d -v $(parserdir)/dbotter.y -o $(parserdir)/dbotter.tab.c

$(parserdir)/dbotter.yy.c: $(parserdir)/dbotter.l $(parserdir)/dbotter.tab.h $(parserdir)/dbotterparse.h $(parserdir)/dbgenparse.h
	$(LEX) -Pwg_yyotter -o$(parserdir)/dbotter.yy.c $(parserdir)/dbotter.l 

$(parserdir)/dbprolog.tab.c $(parserdir)/dbprolog.tab.h: $(parserdir)/dbprolog.y $(parserdir)/dbprologparse.h $(parserdir)/dbgenparse.h
	$(BISON) -pwg_yyprolog -d -v $(parserdir)/dbprolog.y -o $(parserdir)/dbprolog.tab.c

$(parserdir)/dbprolog.yy.c: $(parserdir)/dbprolog.l $(parserdir)/dbprolog.tab.h $(parserdir)/dbprologparse.h $(parserdir)/dbgenparse.h
	$(LEX) -Pwg_yyprolog -o$(parserdir)/dbprolog.yy.c $(parserdir)/dbprolog.l 

MAINTAINERCLEANFILES = $(parsergenfiles)
 
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

if REASONER
libwgdb_la_SOURCES = $(dbfiles) $(parserfiles) $(printerfiles) $(reasonerfiles) 
else
libwgdb_la_SOURCES = $(dbfiles)
endif

wgdb_SOURCES = wgdb.c
wgdb_LDADD = libwgdb.la

stresstest_SOURCES = stresstest.c
stresstest_LDADD = libwgdb.la

indextool_SOURCES = indextool.c
indextool_LDADD = libwgdb.la
