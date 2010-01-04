#/bin/sh

gcc  -O3 -march=pentium4 -lm -o demo  demo.c ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c ../Db/dblock.c ../Db/dbindex.c ../Db/dblog.c ../Db/dbtest.c ../Db/dbhash.c
