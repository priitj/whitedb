@rem current version does not build reasoner: added later

cl /Ox /W3 Main\wgdb.c Db\dbmem.c Db\dballoc.c Db\dbdata.c Db\dblock.c Db\dbtest.c DB\dbdump.c Db\dblog.c Db\dbhash.c Db\dbindex.c Db\dbcompare.c Db\dbquery.c Db\dbutil.c Db\dbmpool.c
cl /Ox /W3 Main\stresstest.c Db\dbmem.c Db\dballoc.c Db\dbdata.c Db\dblock.c Db\dbtest.c Db\dblog.c Db\dbhash.c Db\dbindex.c Db\dbcompare.c Db\dbquery.c Db\dbutil.c Db\dbmpool.c
cl /Ox /W3 Main\indextool.c Db\dbmem.c Db\dballoc.c Db\dbdata.c Db\dblock.c Db\dbtest.c Db\dblog.c Db\dbhash.c Db\dbindex.c Db\dbcompare.c Db\dbquery.c Db\dbutil.c Db\dbmpool.c

@rem build DLL. Currently we are not using it to link executables however.
@rem unlike gcc build, it is necessary to have all functions declared in
@rem wgdb.def file. Make sure it's up to date (should list same functions as
@rem Db/dbapi.h)
cl /Ox /W3 /MT /Fewgdb /LD Db\dbmem.c Db\dballoc.c Db\dbdata.c Db\dblock.c Db\dbtest.c DB\dbdump.c Db\dblog.c Db\dbhash.c  Db\dbindex.c Db\dbcompare.c Db\dbquery.c Db\dbutil.c Db\dbmpool.c /link /def:wgdb.def /incremental:no /MANIFEST:NO

@rem Example of linking against wgdb.dll
@rem cl /Ox /W3 Main\stresstest.c wgdb.lib
cl /Ox /W3 Main\wgdb.c wgdb.lib
