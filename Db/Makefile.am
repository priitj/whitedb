#
# - - - - main db sources - - -

noinst_LTLIBRARIES = libDb.la
libDb_la_SOURCES = dbmem.c dbmem.h\
  dballoc.c dballoc.h dbfeatures.h\
  dbdata.c dbdata.h\
  dblock.c dblock.h\
  dbdump.c dbdump.h crc1.h\
  dblog.c dblog.h\
  dbhash.c dbhash.h\
  dbindex.c dbindex.h\
  dbcompare.c dbcompare.h\
  dbquery.c dbquery.h\
  dbutil.c dbutil.h\
  dbmpool.c dbmpool.h\
  dbjson.c dbjson.h\
  dbschema.c dbschema.h

if RAPTOR
AM_CFLAGS += `$(RAPTOR_CONFIG) --cflags`
endif

