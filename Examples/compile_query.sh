#!/bin/sh

# run unite.sh if needed
if [ ! -f ../whitedb.c ]; then
  cd ..; ./unite.sh; cd "$OLDPWD"
fi

# use output of unite.sh
gcc  -O2 -I.. -o query  query.c ../Test/dbtest.c ../whitedb.c -lm

#gcc  -O2 -o query  query.c ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c ../Db/dblock.c ../Db/dbindex.c ../Db/dblog.c ../Db/dbhash.c ../Db/dbcompare.c ../Db/dbquery.c ../Db/dbutil.c  ../Test/dbtest.c ../Db/dbmpool.c ../Db/dbjson.c ../Db/dbschema.c ../json/yajl_all.c -lm
