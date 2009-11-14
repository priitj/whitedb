#/bin/sh

# alternative to compilation with automake/make: just run 

[ -f config.h ] || cp config-gcc.h config.h
gcc  -O2 -Wall -o Main/wgdb Main/wgdb.c Db/dbmem.c Db/dballoc.c Db/dbdata.c Db/dbindex.c Db/dbtest.c Db/dbdump.c  Db/dblog.c
gcc  -O2 -o Main/indextool  Main/indextool.c Db/dbmem.c Db/dballoc.c Db/dbdata.c Db/dbindex.c Db/dbtest.c  Db/dblog.c
gcc  -O3 -Wall -march=pentium4 -pthread -o Main/stresstest Main/stresstest.c Db/dbmem.c Db/dballoc.c Db/dbdata.c Db/dblock.c Db/dbtest.c  Db/dblog.c
