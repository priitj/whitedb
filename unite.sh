#!/bin/sh

amal() {
  sed 's/#include\ "../\/\//' $1
}

[ -f config.h ] || cp config-gcc.h config.h
if [ config-gcc.h -nt config.h ]; then
  echo "Warning: config.h is older than config-gcc.h, consider updating it"
fi

cat << EOT > whitedb.h
#include <stdio.h>
$(amal config.h)
$(amal json/yajl_api.h)
$(amal json/yajl_all.h)
$(amal Db/dballoc.h)

$(amal Db/dbmem.h)
$(amal Db/dbfeatures.h)
$(amal Db/dbdata.h)
$(amal Db/dblog.h)
$(amal Db/dbdump.h)
$(amal Db/dbhash.h)
$(amal Db/dbindex.h)
$(amal Db/dbcompare.h)
$(amal Db/dbquery.h)
$(amal Db/dbutil.h)
$(amal Db/dbmpool.h)
$(amal Db/dbjson.h)
$(amal Db/dblock.h)
$(amal Db/dbschema.h)
EOT

cat << EOT > whitedb.c
#include <whitedb.h>
$(amal Db/crc1.h)

$(amal json/yajl_all.c)
$(amal Db/dbmem.c)
$(amal Db/dballoc.c)
$(amal Db/dbdata.c)
$(amal Db/dblog.c)
$(amal Db/dbdump.c)
$(amal Db/dbhash.c)
$(amal Db/dbindex.c)
$(amal Db/dbcompare.c)
$(amal Db/dbquery.c)
$(amal Db/dbutil.c)
$(amal Db/dbmpool.c)
$(amal Db/dbjson.c)
$(amal Db/dbschema.c)
$(amal Db/dblock.c)
EOT
