#!/bin/sh

[ -z "$CC" ] && CC="cc"

if [ -z "$(which $CC 2>/dev/null)" ]; then
    echo "Error: No compiler found"
    exit 1
fi

export PYDIR=/usr/include/python2.7

# run unite.sh if needed
if [ ! -f ../whitedb.c ]; then
  cd ..; ./unite.sh; cd "$OLDPWD"
fi

$CC -O3 -Wall -fPIC -shared -I.. -I../Db -I${PYDIR} -o wgdb.so wgdbmodule.c ../whitedb.c

#$CC -O3 -Wall -fPIC -shared -I../Db -I${PYDIR} -o wgdb.so wgdbmodule.c ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c ../Db/dblock.c ../Db/dbindex.c ../Db/dblog.c ../Db/dbhash.c  ../Db/dbcompare.c ../Db/dbquery.c ../Db/dbutil.c ../Db/dbmpool.c ../Db/dbjson.c ../Db/dbschema.c ../json/yajl_all.c
