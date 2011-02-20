# $Id: $
# $Source: $
#
# Top level Makefile.am of wgandalf: calling makes in subdirs
#

SUBDIRS = Main Examples
if PYTHON
SUBDIRS += Python
endif

# --- optional components ----


# -------- Extras -------------------


EXTRA_DIST = Bootstrap README INSTALL MANIFEST GPLLICENCE NEWS AUTHORS \
 Doc/python.txt Doc/locks.txt Doc/Manual.txt \
 wgdb.def config-w32.h config-gcc.h Doxyfile compile.sh compile.bat \
 Db/dbapi.h Db/rdfapi.h Db/indexapi.h \
 Examples/compile_demo.sh  Examples/compile_demo.bat \
 Python/compile.bat Python/compile.sh \
 config.h

dist-hook:
	cp $(top_distdir)/config-gcc.h $(top_distdir)/config.h
