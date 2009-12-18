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

/* Define if you have POSIX threads libraries and header files. */
#define HAVE_PTHREAD 1

/* Define to 1 if your C compiler doesn't accept -c and -o together. */
/* #undef NO_MINUS_C_MINUS_O */

/* Name of package */
#define PACKAGE "wgandalf"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT ""

/* Define to the full name of this package. */
#define PACKAGE_NAME "wgandalf"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "wgandalf 0.2.0"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "wgandalf"

/* Define to the version of this package. */
#define PACKAGE_VERSION "0.2.0"

/* Define to necessary symbol if this constant uses a non-standard name on
   your system. */
/* #undef PTHREAD_CREATE_JOINABLE */

/* Use queued locks with local spinning */
/* #undef QUEUED_LOCKS */

/* Use dblog module for transaction logging */
/* #undef USE_DBLOG */

/* Version number of package */
#define VERSION "0.2.0"

/* Package major version */
#define VERSION_MAJOR 0

/* Package minor version */
#define VERSION_MINOR 2

/* Package revision number */
#define VERSION_REV 0
