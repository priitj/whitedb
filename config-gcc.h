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

/* Journal file directory */
#define DBLOG_DIR "/tmp"

/* Select locking protocol (undef to disable locking)
 * 1 - reader preference spinlock
 * 2 - writer preference spinlock
 * 3 - task-fair queued lock
 */
#define LOCK_PROTO 1

/* Encoded data is 64-bit */
/* #undef HAVE_64BIT_GINT */

/* Define if you have POSIX threads libraries and header files. */
#define HAVE_PTHREAD 1

/* Compile with raptor rdf library */
/* #undef HAVE_RAPTOR */

/* Define to 1 if your C compiler doesn't accept -c and -o together. */
/* #undef NO_MINUS_C_MINUS_O */

/* Name of package */
#define PACKAGE "whitedb"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT ""

/* Define to the full name of this package. */
#define PACKAGE_NAME "WhiteDB"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "WhiteDB 0.8-alpha"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "whitedb"

/* Define to the version of this package. */
#define PACKAGE_VERSION "0.8-alpha"

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* String hash size (% of db size) */
#define STRHASH_SIZE 2

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

/* Use match templates for indexes */
#define USE_INDEX_TEMPLATE 1

/* Enable runtime diagnostics via error callback */
#define USE_ERROR_CALLBACK 1

/* Enable reasoner */
/* #undef USE_REASONER */

/* Version number of package */
#define VERSION "0.8-alpha"

/* Package major version */
#define VERSION_MAJOR 0

/* Package minor version */
#define VERSION_MINOR 8

/* Package revision number */
#define VERSION_REV 0
