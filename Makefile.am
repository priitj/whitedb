# $Id: $
# $Source: $
#
# Top level Makefile.am of WhiteDB: calling makes in subdirs
#

#AUTOMAKE_OPTIONS = foreign
AUTOMAKE_OPTIONS = subdir-objects

SUBDIRS = Db json
if REASONER
SUBDIRS += Parser Reasoner Printer
endif
SUBDIRS += Main Examples
if PYTHON
SUBDIRS += Python
endif

# --- optional components ----


# -------- Extras -------------------


EXTRA_DIST = Bootstrap README INSTALL MANIFEST GPLLICENCE NEWS AUTHORS \
 Doc/python.txt Doc/Manual.txt \
 wgdb.def config-w32.h config-gcc.h Doxyfile compile.sh compile.bat \
 Db/dbapi.h Db/rdfapi.h Db/indexapi.h \
 Examples/compile_demo.sh  Examples/compile_demo.bat \
 Examples/compile_query.sh  Examples/compile_query.bat \
 Examples/dserve.c Examples/tut1.c Examples/tut2.c Examples/tut3.c \
 Examples/tut4.c Examples/tut5.c Examples/tut6.c Examples/tut7.c \
 Python/compile.bat Python/compile.sh Python/tests.py \
 config.h \
 Parser/dbotter.y Parser/dbotter.l Parser/dbprolog.y Parser/dbprolog.l \
 Rexamples

dist-hook:
	cp $(top_distdir)/config-gcc.h $(top_distdir)/config.h
