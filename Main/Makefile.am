# $Id:  $
# $Source:  $
#
# Creating the wgandalf binaries


# ---- options ----

# ---- targets ----

lib_LTLIBRARIES = libwgdb.la
bin_PROGRAMS = wgdb stresstest

# ---- path variables ----

dbdir=../Db

#commondir=../Common
#printdir=../Print
#parsedir=../Parse
#provedir=../Prove


# ---- wgandalf sources in C -----


dbfiles = $(dbdir)/dbmem.c $(dbdir)/dbmem.h\
  $(dbdir)/dballoc.c $(dbdir)/dballoc.h\
  $(dbdir)/dbdata.c $(dbdir)/dbdata.h\
  $(dbdir)/dbtest.c $(dbdir)/dbtest.h\
  $(dbdir)/dblock.c $(dbdir)/dblock.h\
  $(dbdir)/dbdump.c $(dbdir)/dbdump.h\
  $(dbdir)/dblog.c $(dbdir)/dblog.h\
  $(dbdir)/dbhash.c $(dbdir)/dbhash.h
#  $(dbdir)/dbindex.c $(dbdir)/dbindex.h

#commonfiles = $(commondir)/standard.h $(commondir)/sysheaders.h $(commondir)/assert.h\
#  $(commondir)/err.h $(commondir)/mem.h $(commondir)/glb.h $(commondir)/str.h\
#  $(commondir)/file.h $(commondir)/gbtypes.h\
#  $(commondir)/err.c $(commondir)/mem.c $(commondir)/glb.c $(commondir)/str.c\
#  $(commondir)/file.c $(commondir)/gbtypes.c 

#dbfiles = $(dbdir)/dbtest.c $(dbdir)/dbtest.h\
#  $(dbdir)/ctab.c $(dbdir)/ctab.h\
#  $(dbdir)/dbmem.c $(dbdir)/dbmem.h\
#  $(dbdir)/dballoc.c $(dbdir)/dballoc.h

#printfiles = $(printdir)/clprint.c $(printdir)/clprint.h\
#  $(printdir)/csvprint.c $(printdir)/csvprint.h\
#  $(printdir)/dataprint.c $(printdir)/dataprint.h\
#  $(printdir)/printtest.c $(printdir)/printtest.h

#parsefiles = $(parsedir)/mainparse.c $(parsedir)/mainparse.h\
#  $(parsedir)/csvparse.c $(parsedir)/csvparse.h\
#  $(parsedir)/lex.c $(parsedir)/lex.h\
#  $(parsedir)/scmparse.c $(parsedir)/scmparse.h\
#  $(parsedir)/elmparse.h $(parsedir)/elmparse.c\
#  $(parsedir)/elm.tab.c $(parsedir)/elm.tab.h $(parsedir)/elm.yy.c\
#  $(parsedir)/otterparse.h $(parsedir)/otterparse.c\
#  $(parsedir)/prologparse.h $(parsedir)/prologparse.c\
#  $(parsedir)/otter.tab.c $(parsedir)/otter.tab.h $(parsedir)/otter.yy.c\
#  $(parsedir)/prolog.tab.c $(parsedir)/prolog.tab.h $(parsedir)/prolog.yy.c

#provefiles = $(provedir)/unify.h $(provedir)/unify.c\
#  $(provedir)/build.h $(provedir)/build.c\
#  $(provedir)/clstore.h $(provedir)/clstore.c\
#  $(provedir)/derive.h $(provedir)/derive.c\
#  $(provedir)/genloop.h $(provedir)/genloop.c\
#  $(provedir)/subsume.h $(provedir)/subsume.c\
#  $(provedir)/order.h $(provedir)/order.c\
#  $(provedir)/compute.h $(provedir)/compute.c
  

# ----- sources for flex and bison and their compilation to c ---

#$(parsedir)/elm.tab.c $(parsedir)/elm.tab.h: $(parsedir)/elm.y $(commondir)/mem.h
#	bison -d -v $(parsedir)/elm.y -o $(parsedir)/elm.tab.c

#$(parsedir)/elm.yy.c: $(parsedir)/elm.l $(parsedir)/elm.tab.h
#	flex -l -o$(parsedir)/elm.yy.c $(parsedir)/elm.l 

#$(parsedir)/otter.tab.c $(parsedir)/otter.tab.h: $(parsedir)/otter.y $(commondir)/mem.h
#	bison -pyyotter -d -v $(parsedir)/otter.y -o $(parsedir)/otter.tab.c

#$(parsedir)/otter.yy.c: $(parsedir)/otter.l $(parsedir)/otter.tab.h
#	flex -Pyyotter -l -o$(parsedir)/otter.yy.c $(parsedir)/otter.l 

#$(parsedir)/prolog.tab.c $(parsedir)/prolog.tab.h: $(parsedir)/prolog.y $(commondir)/mem.h
#	bison -pyyprolog -d -v $(parsedir)/prolog.y -o $(parsedir)/prolog.tab.c

#$(parsedir)/prolog.yy.c: $(parsedir)/prolog.l $(parsedir)/prolog.tab.h
#	flex -Pyyprolog -l -o$(parsedir)/prolog.yy.c $(parsedir)/prolog.l 

 
# ---- extra dependencies, flags, etc -----

libwgdb_la_CFLAGS = -march=pentium4

stresstest_LIBS=$(PTHREAD_LIBS)
stresstest_CFLAGS=$(PTHREAD_CFLAGS)
stresstest_LDFLAGS= -static
stresstest_CC=$(PTHREAD_CC)

# ----- all sources for the created programs -----

libwgdb_la_SOURCES = $(dbfiles)

wgdb_SOURCES = wgdb.c wgdb.h
wgdb_LDADD = libwgdb.la

stresstest_SOURCES = stresstest.c
stresstest_LDADD = libwgdb.la
