# Process this file with autoconf to produce a configure script.
# $Id:  $
# $Source:  $


# ------- Initialisation -------

m4_define([WHITEDB_MAJOR], [0])
m4_define([WHITEDB_MINOR], [6])
m4_define([WHITEDB_REV], [0])

# standard release
m4_define([WHITEDB_VERSION],
  m4_defn([WHITEDB_MAJOR]).m4_defn([WHITEDB_MINOR]).m4_defn([WHITEDB_REV]))
# release candidate; fill in manually
#m4_define([WHITEDB_VERSION], [0.6-RC1]))

AC_INIT(WhiteDB, [WHITEDB_VERSION])

AC_MSG_NOTICE([====== initialising ======])

# Add new configuration files

AC_CONFIG_SRCDIR(Db/dballoc.c)
AC_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE

#Initialize libtool
AC_PROG_LIBTOOL

# ------- Checking -------

AC_MSG_NOTICE([====== checking ======])

AC_PROG_CC
AM_PROG_CC_C_O # for per-target flags

#Checks for libraries.
ACX_PTHREAD()
AC_CHECK_LIB([m],[cos])

#Check for programs
AC_PROG_INSTALL

#Check for Python (optional)
AC_ARG_WITH([python],
    [AS_HELP_STRING([--with-python],
        [enable building Python bindings])],
    [],
    [with_python=no])

if test "x$with_python" != "xno"
then
    if test "x$with_python" != "xyes"
    then
        PYTHON="$with_python"
    fi
    AM_PATH_PYTHON(,,[])
else
    PYTHON=""
fi

if test "x$PYTHON" != "x"
then
    AM_CHECK_PYTHON_HEADERS(,[PYTHON=[]])
fi

# If PYTHON is non-empty, contents of Python subdir will be
# included in the build. This also implies that the check for
# headers was successful and PYTHON_INCLUDES contains something useful.
AM_CONDITIONAL(PYTHON, [test "x$PYTHON" != "x"])

#Check for Raptor (optional)

AC_CHECK_PROGS(RAPTOR_CONFIG, raptor-config, [])
AM_CONDITIONAL(RAPTOR, [test "x$RAPTOR_CONFIG" != "x"])
if test "x$RAPTOR_CONFIG" != "x"
then
    AC_DEFINE([HAVE_RAPTOR], [1], [Compile with raptor rdf library])
fi

# lex and bison (needed for reasoner)

AC_PROG_LEX
AC_CHECK_TOOL([BISON], [bison], [:])

# Set the journal directory
AC_ARG_WITH(logdir,
    [AC_HELP_STRING([--with-logdir=DIR],
        [Journal file directory @<:@default=/tmp@:>@])],
    with_logdir="$withval",
    with_logdir="/tmp")
if test "x$with_logdir" = "xyes" -o "x$with_logdir" = "xno"
then
    # define it anyway, even if the user picked --without
    LOGDIR="/tmp"
else
    LOGDIR="$with_logdir"
fi
AC_DEFINE_UNQUOTED([DBLOG_DIR], "$LOGDIR", Journal file directory)

# ----------- configuration options ----------

AC_MSG_NOTICE([====== setting configuration options ======])

AC_MSG_CHECKING(for logging)
AC_ARG_ENABLE(logging, [AS_HELP_STRING([--enable-logging],
    [enable transaction logging])],
    [logging=$enable_logging],logging=no)
if test "$logging" != no
then
    AC_DEFINE([USE_DBLOG], [1], [Use dblog module for transaction logging])
    AC_MSG_RESULT([enabled, journal directory is $LOGDIR])
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for locking protocol)
AC_ARG_ENABLE(locking, [AS_HELP_STRING([--enable-locking],
    [select locking protocol (rpspin,wpspin,tfqueue,no) @<:@default=rpspin@:>@])],
    [locking=$enable_locking],locking=rpspin)
if test "$locking" == no
then
    AC_MSG_RESULT(disabled)
elif test "$locking" == wpspin
then
    AC_DEFINE([LOCK_PROTO], [2],
      [Select locking protocol: writer-preference spinlock])
    AC_MSG_RESULT($locking)
elif test "$locking" == tfqueue
then
    AC_DEFINE([LOCK_PROTO], [3],
      [Select locking protocol: task-fair queued lock])
    AC_MSG_RESULT($locking)
else
    # unknown value, revert to default
    AC_DEFINE([LOCK_PROTO], [1],
      [Select locking protocol: reader-preference spinlock])
    AC_MSG_RESULT($locking)
fi

AC_MSG_CHECKING(for additional validation checks)
AC_ARG_ENABLE(checking, [AS_HELP_STRING([--disable-checking],
    [disable additional validation checks in API layer (small performance gain) ])],
    [checking=$enable_checking],checking=yes)
if test "$checking" != no
then
    AC_DEFINE([CHECK], [1], [Use additional validation checks])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for single-compare T-tree mode)
AC_ARG_ENABLE(single_compare, [AS_HELP_STRING([--disable-single-compare],
    [disable experimental single compare algorithm in T-tree search])],
    [single_compare=$enable_single_compare],single_compare=yes)
if test "$single_compare" != no
then
    AC_DEFINE([TTREE_SINGLE_COMPARE], [1], [Use single-compare T-tree mode])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for chained T-tree nodes algorithm)
AC_ARG_ENABLE(tstar, [AS_HELP_STRING([--disable-tstar],
    [disable experimental chained T-tree (similar to T* tree)
        index algorithm])],
    [tstar=$enable_tstar],tstar=yes)
if test "$tstar" != no
then
    AC_DEFINE([TTREE_CHAINED_NODES], [1], [Use chained T-tree index nodes])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for backlinking)
AC_ARG_ENABLE(backlink, [AS_HELP_STRING([--disable-backlink],
    [disable record backlinking])],
    [backlink=$enable_backlink],backlink=yes)
if test "$backlink" != no
then
    AC_DEFINE([USE_BACKLINKING], [1], [Use record banklinks])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for child db support)
AC_ARG_ENABLE(childdb, [AS_HELP_STRING([--enable-childdb],
    [enable child database support])],
    [childdb=$enable_childdb],childdb=no)
if test "$childdb" != no
then
    AC_DEFINE([USE_CHILD_DB], [1], [Enable child database support])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for index templates)
AC_ARG_ENABLE(index_templates, [AS_HELP_STRING([--disable-index-templates],
    [disable support for index templates])],
    [index_templates=$enable_index_templates],index_templates=yes)
if test "$index_templates" != no
then
    AC_DEFINE([USE_INDEX_TEMPLATE], [1], [Use match templates for indexes])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

AC_MSG_CHECKING(for reasoner)
AC_ARG_ENABLE(reasoner, [AS_HELP_STRING([--enable-reasoner],
    [enable reasoner])],
    [reasoner=$enable_reasoner],reasoner=no)
if test "$reasoner" != no
then
    if test "x$LEX" != "x:" -a "x$BISON" != "x:"
    then
      AC_DEFINE([USE_REASONER], [1], [Enable reasoner])
      AC_MSG_RESULT(enabled)
    else
      AC_MSG_RESULT([disabled, bison or lex missing])
      reasoner=no
    fi
else
    AC_MSG_RESULT(disabled)
fi
AM_CONDITIONAL(REASONER, [test "$reasoner" != no])

AC_MSG_CHECKING(for mpool usage in queries)
AC_ARG_ENABLE(query_mpool, [AS_HELP_STRING([--disable-query-mpool],
    [disable mpool usage in queries])],
    [query_mpool=$enable_query_mpool],query_mpool=yes)
if test "$query_mpool" != no
then
    AC_DEFINE([USE_MPOOL_QUERY], [1], [Use mpool in queries])
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(disabled)
fi

# ---------- Compiler flags --------

AC_MSG_NOTICE([====== setting compiler flags ======])

#CFLAGS="-g -Wall"

CFLAGS="-O3 -Wall"

AX_GCC_ARCHFLAG([no])

# add -m64 if possible (amd64)
restore_cflags="$CFLAGS"
CFLAGS="$CFLAGS -m64"
AC_MSG_CHECKING([whether CC supports -m64])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([])],
    [AC_MSG_RESULT([yes])]
    [AC_DEFINE([HAVE_64BIT_GINT], [1], [Encoded data is 64-bit])],
    [AC_MSG_RESULT([no])]
    [CFLAGS="$restore_cflags"]
)

AC_MSG_CHECKING(for gprof)
AC_ARG_ENABLE(gprof, [AS_HELP_STRING([--enable-gprof],
    [enable memory profiling with gprof])],
    gprof=yes,gprof=no)
if test "$gprof" != no
then
    CFLAGS="-pg $CFLAGS"
    AC_MSG_RESULT(enabled)
else
    AC_MSG_RESULT(no)
fi

# ---------- Final creation --------

AC_MSG_NOTICE([====== final steps ======])

AC_DEFINE([VERSION_MAJOR], [WHITEDB_MAJOR], [Package major version])
AC_DEFINE([VERSION_MINOR], [WHITEDB_MINOR], [Package minor version])
AC_DEFINE([VERSION_REV], [WHITEDB_REV], [Package revision number])

AC_OUTPUT([
Makefile
Main/Makefile
Examples/Makefile
Python/Makefile
])
