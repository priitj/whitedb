#/bin/sh

# alternative to compilation with automake/make: just run 

gcc  -O2 -Wall -o Main/wgdb Main/wgdb.c Db/dbmem.c Db/dballoc.c

