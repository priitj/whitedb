@rem Windows compilation script for the dserve tool.
@rem First compile whitedb in the parent folder, using compile.bat there!

@rem copy two necessary files (created by compiling the parent) here

copy ..\db\dbapi.h .
copy ..\wgdb.lib .
copy ..\wgdb.dll .

@rem compile a server version of dserve
@rem dserve.h must contain #define SERVEROPTION (does so by default) for this

cl /Ox /I"." dserve.c dserve_util.c dserve_net.c wgdb.lib

@rem or alternatively compile a non-server version of dserve
@rem remove #define SERVEROPTION from dserve.h before using this alternative
@rem cl /Ox /I"." dserve.c dserve_util.c wgdb.lib