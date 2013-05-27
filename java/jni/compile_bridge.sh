#!/bin/sh
#
[ "X${JAVA_HOME}" = "X" ] && JAVA_HOME=/usr/lib/jvm/java-6-openjdk
DBDIR=../../../Db

cd library
gcc  -O2 -march=pentium4 -lm -shared -I${JAVA_HOME}/include ../src/native/wgandalfDriver.c ${DBDIR}/dbmem.c ${DBDIR}/dballoc.c ${DBDIR}/dbdata.c ${DBDIR}/dblock.c ${DBDIR}/dbindex.c ${DBDIR}/dbtest.c ${DBDIR}/dblog.c ${DBDIR}/dbhash.c ${DBDIR}/dbcompare.c ${DBDIR}/dbquery.c ${DBDIR}/dbutil.c ${DBDIR}/dbmpool.c -o libwgandalfDriver.so

