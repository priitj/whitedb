@rem current version does not build reasoner: added later

@rem build DLL.
@rem unlike gcc build, it is necessary to have all functions declared in
@rem wgdb.def file. Make sure it's up to date (should list same functions as
@rem Db/dbapi.h)
cl /Ox /W3 /MT /Fewgdb /LD Db\dbmem.c Db\dballoc.c Db\dbdata.c Db\dblock.c DB\dbdump.c Db\dblog.c Db\dbhash.c  Db\dbindex.c Db\dbcompare.c Db\dbquery.c Db\dbutil.c Db\dbmpool.c Db\dbjson.c Db\dbschema.c json\yajl_all.c /link /def:wgdb.def /incremental:no /MANIFEST:NO

@rem Link executables against wgdb.dll
@rem cl /Ox /W3 Main\stresstest.c wgdb.lib
cl /Ox /W3 Main\wgdb.c wgdb.lib
@rem cl /Ox /W3 Main\indextool.c wgdb.lib

@rem Example of building without the DLL
@rem the test module depends on many symbols not part of the API
cl /Ox /W3 Main\selftest.c Db\dbmem.c Db\dballoc.c Db\dbdata.c Db\dblock.c Test\dbtest.c DB\dbdump.c Db\dblog.c Db\dbhash.c Db\dbindex.c Db\dbcompare.c Db\dbquery.c Db\dbutil.c Db\dbmpool.c Db\dbjson.c Db\dbschema.c json\yajl_all.c
