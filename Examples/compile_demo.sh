#!/bin/sh

COMPILER="cc"

[[ -z "$CC" ]] || COMPILER="$CC"

if [[ -z "$COMPILER" && -z "$(which $COMPILER 2>/dev/null)" ]]; then
    echo "Error: No compiler found"
    exit 1
fi

# run unite.sh if needed
if [ ! -f ../whitedb.c ]; then
  cd ..; ./unite.sh; cd "$OLDPWD"
fi

# use output of unite.sh
$COMPILER  -O2 -I.. -o demo  demo.c ../whitedb.c -lm

#$COMPILER  -O2 -o demo  demo.c ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c ../Db/dblock.c ../Db/dbindex.c ../Db/dblog.c ../Db/dbhash.c ../Db/dbcompare.c ../Db/dbquery.c ../Db/dbutil.c ../Db/dbmpool.c ../Db/dbjson.c ../Db/dbschema.c ../json/yajl_all.c -lm
