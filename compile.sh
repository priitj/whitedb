#/bin/sh

# alternative to compilation with automake/make: just run 

# current version does not build reasoner: added later

[ -f config.h ] || cp config-gcc.h config.h
if [ config-gcc.h -nt config.h ]; then
  echo "Warning: config.h is older than config-gcc.h, consider updating it"
fi
gcc  -O2 -Wall -o Main/wgdb Main/wgdb.c Db/dbmem.c \
  Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbindex.c Db/dbdump.c  \
  Db/dblog.c Db/dbhash.c Db/dbcompare.c Db/dbquery.c Db/dbutil.c Db/dbmpool.c \
  Db/dbjson.c Db/dbschema.c json/yajl_all.c -lm
gcc  -O2 -Wall -o Main/indextool  Main/indextool.c Db/dbmem.c \
  Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbindex.c Db/dblog.c \
  Db/dbhash.c Db/dbcompare.c Db/dbquery.c Db/dbutil.c Db/dbmpool.c \
  Db/dbjson.c Db/dbschema.c json/yajl_all.c -lm
#gcc  -O2 -Wall -o Main/selftest Main/selftest.c Db/dbmem.c \
#  Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbindex.c Test/dbtest.c Db/dbdump.c \
#  Db/dblog.c Db/dbhash.c Db/dbcompare.c Db/dbquery.c Db/dbutil.c Db/dbmpool.c \
#  Db/dbjson.c Db/dbschema.c json/yajl_all.c -lm
