@rem Check that this matches your Python path
set PYDIR=c:\Python25

@rem When compiling for Python 3, replace /export:initwgdb
@rem with /export:PyInit_wgdb

@cl /Ox /W3 /MT /I..\Db /I%PYDIR%\include wgdbmodule.c ..\Db\dbmem.c ..\Db\dballoc.c ..\Db\dbdata.c ..\Db\dblock.c ..\DB\dbdump.c ..\Db\dblog.c ..\Db\dbhash.c  ..\Db\dbindex.c ..\Db\dbcompare.c ..\Db\dbquery.c ..\Db\dbutil.c ..\Db\dbmpool.c  ..\Db\dbjson.c ..\Db\dbschema.c ..\json\yajl_all.c /link /dll /incremental:no /MANIFEST:NO /LIBPATH:%PYDIR%\libs /export:initwgdb /out:wgdb.pyd
@rem Currently this script produced a statically linked DLL for ease of
@rem testing and debugging. If dynamic linking is needed:
@rem 1. replace /MT with /MD
@rem 2. remove /manifest:no or just the "NO" part
@rem 3. uncomment the following:
@rem mt -manifest wgdb.pyd.manifest -outputresource:wgdb.pyd;2
@rem You may also need to:
@rem 4. distribute msvcrxx.dll from the compiler suite with the lib
