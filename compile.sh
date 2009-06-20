#/bin/sh

# alternative to compilation with automake/make: just run 

[ -f config.h ] || touch config.h
gcc  -O2 -Wall -o Main/wgdb Main/wgdb.c Db/dbmem.c Db/dballoc.c Db/dbdata.c Db/dbindex.c Db/dbtest.c
gcc  -O2 -o Main/indextool  Main/indextool.c Db/dbmem.c Db/dballoc.c Db/dbdata.c Db/dbindex.c Db/dbtest.c
