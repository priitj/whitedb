#
# - - - - reasoner sources - - - -

parsergenfiles = dbotter.tab.c dbotter.tab.h\
  dbotter.yy.c dbprolog.tab.c\
  dbprolog.tab.h dbprolog.yy.c

parserfiles =  dbparse.c dbparse.h dbgenparse.h\
  dbotterparse.h\
  dbprologparse.h\
  $(parsergenfiles)

# - - - -  reasoner sources for flex and bison and their compilation to c - - - -

dbotter.tab.c dbotter.tab.h: dbotter.y dbotterparse.h dbgenparse.h
	$(BISON)  -pwg_yyotter -d -v dbotter.y -o dbotter.tab.c

dbotter.yy.c: dbotter.l dbotter.tab.h dbotterparse.h dbgenparse.h
	$(LEX) -Pwg_yyotter -odbotter.yy.c dbotter.l

dbprolog.tab.c dbprolog.tab.h: dbprolog.y dbprologparse.h dbgenparse.h
	$(BISON) -pwg_yyprolog -d -v dbprolog.y -o dbprolog.tab.c

dbprolog.yy.c: dbprolog.l dbprolog.tab.h dbprologparse.h dbgenparse.h
	$(LEX) -Pwg_yyprolog -odbprolog.yy.c dbprolog.l

MAINTAINERCLEANFILES = $(parsergenfiles)

noinst_LTLIBRARIES = libParser.la
libParser_la_SOURCES = $(parserfiles)

