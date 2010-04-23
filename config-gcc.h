/*
 * XXX: put this file under license if needed
 *
 */

 /** @file config-gcc.h
 * Build configuration for gcc platform.
 *
 * Based on auto-generated config.h. Should be manually synced whenever
 * additional configuration parameters are added.
 */

/* Use additional validation checks */
#define CHECK 1

/* Allow compiling on platforms that do not have atomic operations needed for
   locking */
/* #undef DUMMY_LOCKS */

/* Define if you have POSIX threads libraries and header files. */
#define HAVE_PTHREAD 1

/* Compile with raptor rdf library */
/* #undef HAVE_RAPTOR */

/* Define to 1 if your C compiler doesn't accept -c and -o together. */
/* #undef NO_MINUS_C_MINUS_O */

/* Name of package */
#define PACKAGE "wgandalf"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT ""

/* Define to the full name of this package. */
#define PACKAGE_NAME "wgandalf"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "wgandalf 0.4.0"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "wgandalf"

/* Define to the version of this package. */
#define PACKAGE_VERSION "0.4.0"

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* Use queued locks with local spinning */
/* #undef QUEUED_LOCKS */

/* Use chained T-tree index nodes */
#define TTREE_CHAINED_NODES 1

/* Use single-compare T-tree mode */
#define TTREE_SINGLE_COMPARE 1

/* Use record banklinks */
#define USE_BACKLINKING 1

/* Enable child database support */
/* #undef USE_CHILD_DB */

/* Use dblog module for transaction logging */
/* #undef USE_DBLOG */

/* Version number of package */
#define VERSION "0.4.0"

/* Package major version */
#define VERSION_MAJOR 0

/* Package minor version */
#define VERSION_MINOR 4

/* Package revision number */
#define VERSION_REV 0
