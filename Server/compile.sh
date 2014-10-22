#!/bin/sh

# alternative to compiling dserve and dservehttps with automake/make: 
# just run it in the Server folder

[ -z "$CC" ] && CC="cc"

if [ -z "$(which $CC 2>/dev/null)" ]; then
    echo "Error: No compiler found"
    exit 1
fi

# copy config.h to the current folder
[ -f config.h ] || cp ../config-gcc.h config.h
if [ ../config-gcc.h -nt ../config.h ]; then
  echo "Warning: config.h is older than config-gcc.h, consider updating it"
fi

# run unite.sh if needed
if [ ! -f ../whitedb.c ]; then
  cd ..; ./unite.sh; cd "$OLDPWD"
fi

# compile dserve
$CC -O2 -Wall -I.. -o dserve dserve.c dserve_util.c dserve_net.c \
  ../whitedb.c -lm -lpthread
# compile dservehttps  
$CC -O2 -Wall -I.. -DUSE_OPENSSL -o dservehttps dserve.c dserve_util.c dserve_net.c \
  ../whitedb.c -lm -lpthread -lssl -lcrypto
