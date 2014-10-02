#!/bin/sh

# alternative to compilation with automake/make: just run 

# current version does not build reasoner: added later

# Support for clang compilator
PARAM=${1:-"gcc"}
COMPILER="cc"

if [ $(which gcc&>/dev/null;echo $?) -eq 0 ] && [ "$PARAM" = "gcc" ]; then
    COMPILER="gcc"
    echo "Info: Compilation with GCC"
fi

if [ $(which clang&>/dev/null;echo $?) -eq 0 ] && [ "$PARAM" = "clang" ]; then
    COMPILER="clang"
    echo "Info: Compilation with CLANG"
fi


if [ -z "$COMPILER" ]; then
    echo "Error: No compiler $PARAM found"
    exit 1
fi

[ -f config.h ] || cp config-gcc.h config.h
if [ config-gcc.h -nt config.h ]; then
  echo "Warning: config.h is older than config-gcc.h, consider updating it"
fi
${COMPILER} -O2 -Wall -o Main/wgdb Main/wgdb.c Db/dbmem.c \
  Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbindex.c Db/dbdump.c  \
  Db/dblog.c Db/dbhash.c Db/dbcompare.c Db/dbquery.c Db/dbutil.c Db/dbmpool.c \
  Db/dbjson.c Db/dbschema.c json/yajl_all.c -lm
# debug and testing programs: uncomment as needed
#gcc  -O2 -Wall -o Main/indextool  Main/indextool.c Db/dbmem.c \
#  Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbindex.c Db/dblog.c \
#  Db/dbhash.c Db/dbcompare.c Db/dbquery.c Db/dbutil.c Db/dbmpool.c \
#  Db/dbjson.c Db/dbschema.c json/yajl_all.c -lm
#gcc  -O2 -Wall -o Main/selftest Main/selftest.c Db/dbmem.c \
#  Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbindex.c Test/dbtest.c Db/dbdump.c \
#  Db/dblog.c Db/dbhash.c Db/dbcompare.c Db/dbquery.c Db/dbutil.c Db/dbmpool.c \
#  Db/dbjson.c Db/dbschema.c json/yajl_all.c -lm
