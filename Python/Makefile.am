# $Id:  $
# $Source:  $
#
# Compile Python extension module


# ---- options ----

# ---- targets ----

pyexec_LTLIBRARIES = wgdb.la

# ---- extra dependencies, flags, etc -----

INCLUDES = $(PYTHON_INCLUDES) -I $(dbdir)
wgdb_la_LDFLAGS = -module -avoid-version

# ----- all sources for the created programs -----

wgdb_la_SOURCES = wgdbmodule.c
wgdb_la_LIBADD = ../Main/libwgdb.la
