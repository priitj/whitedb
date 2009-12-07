#!/bin/sh

export PYDIR=/usr/include/python2.6

gcc -O3 -Wall -march=pentium4 -shared -I../Db -I${PYDIR} -o wgdb.so wgdbmodule.c ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c ../Db/dblock.c ../Db/dbtest.c  ../Db/dblog.c ../Db/dbhash.c
