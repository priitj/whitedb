#/bin/sh

# alternative to compiling dserve and dservehttps with automake/make: 
# just run it in the Server folder

# copy config.h to the current folder
[ -f config.h ] || cp ../config-gcc.h config.h
if [ ../config-gcc.h -nt ../config.h ]; then
  echo "Warning: config.h is older than config-gcc.h, consider updating it"
fi
# compile dserve
gcc  -O2 -Wall -o dserve dserve.c dserve_util.c dserve_net.c \
  ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c \
  ../Db/dblock.c ../Db/dbindex.c ../Db/dbtest.c ../Db/dbdump.c  \
  ../Db/dblog.c ../Db/dbhash.c ../Db/dbcompare.c ../Db/dbquery.c ../Db/dbutil.c ../Db/dbmpool.c \
  ../Db/dbjson.c ../Db/dbschema.c ../json/yajl_all.c \
  -lm -lpthread
# compile dservehttps  
gcc  -O2 -Wall  -DUSE_OPENSSL -o dservehttps dserve.c dserve_util.c dserve_net.c \
  ../Db/dbmem.c ../Db/dballoc.c ../Db/dbdata.c \
  ../Db/dblock.c ../Db/dbindex.c ../Db/dbtest.c ../Db/dbdump.c  \
  ../Db/dblog.c ../Db/dbhash.c ../Db/dbcompare.c ../Db/dbquery.c ../Db/dbutil.c ../Db/dbmpool.c \
  ../Db/dbjson.c ../Db/dbschema.c ../json/yajl_all.c \
  -lm -lpthread -lssl -lcrypto
