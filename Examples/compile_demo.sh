#/bin/sh

# run unite.sh if needed
if [ ! -f ../whitedb.c ]; then
  cd ..; ./unite.sh; cd -
fi

# use output of unite.sh
gcc  -O2 -I.. -o demo  demo.c ../whitedb.c -lm

#gcc  -O2 -o demo  demo.c ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c ../Db/dblock.c ../Db/dbindex.c ../Db/dblog.c ../Db/dbhash.c ../Db/dbcompare.c ../Db/dbquery.c ../Db/dbutil.c ../Db/dbmpool.c ../Db/dbjson.c ../Db/dbschema.c ../json/yajl_all.c -lm
