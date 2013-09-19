# $Id:  $
# $Source:  $
#
# Compile Python extension module


# ---- options ----

# ---- targets ----

python_PYTHON = whitedb.py WGandalf.py
pyexec_LTLIBRARIES = wgdb.la

# ---- path variables ----

dbdir=../Db

# ---- extra dependencies, flags, etc -----

LIBDEPS = -lm   # dependency from libm round() should be removed
if RAPTOR
LIBDEPS += `$(RAPTOR_CONFIG) --libs`
endif

AM_CPPFLAGS = $(PYTHON_INCLUDES) -I $(dbdir)
wgdb_la_LDFLAGS = -module -avoid-version $(LIBDEPS)

# ----- all sources for the created programs -----

wgdb_la_SOURCES = wgdbmodule.c
wgdb_la_LIBADD = ../Main/libwgdb.la
