/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
* Copyright (c) Priit Järv 2013,2014
*
* Contact: tanel.tammet@gmail.com
*
* This file is part of WhiteDB
*
* WhiteDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* WhiteDB is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with WhiteDB.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file dbmem.c
 *  Allocating/detaching system memory: shared memory and allocated ordinary memory
 *
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/shm.h>
#include <sys/errno.h>
#include <unistd.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbfeatures.h"
#include "dbmem.h"
#include "dblog.h"

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */

static int normalize_perms(int mode);
static void* link_shared_memory(int key, int *errcode);
static void* create_shared_memory(int key, gint size, int mode);
static int free_shared_memory(int key);

static int detach_shared_memory(void* shmptr);

#ifdef USE_DATABASE_HANDLE
static void *init_dbhandle(void);
static void free_dbhandle(void *dbhandle);
#endif

#ifndef _WIN32
static int memory_stats(void *db, struct shmid_ds *buf);
#endif

static gint show_memory_error(char *errmsg);
#ifdef _WIN32
static gint show_memory_error_nr(char* errmsg, int nr);
#endif

/* ====== Functions ============== */


/* ----------- dbase creation and deletion api funs ------------------ */

/* Check the header for compatibility.
 * XXX: this is not required for a fresh database. */
#define CHECK_SEGMENT(shmx) \
  if(shmx && dbmemsegh(shmx)) { \
    int err; \
    if((err = wg_check_header_compat(dbmemsegh(shmx)))) { \
      if(err < -1) { \
        show_memory_error("Existing segment header is incompatible"); \
        wg_print_code_version(); \
        wg_print_header_version(dbmemsegh(shmx), 1); \
      } \
      return NULL; \
    } \
  }

/** returns a pointer to the database, NULL if failure
 *
 * In case database with dbasename exists, the returned pointer
 * points to the existing database.
 *
 * If there exists no database with dbasename, a new database
 * is created in shared memory with size in bytes
 *
 * If size is not 0 and the database exists, the size of the
 * existing segment is required to be >= requested size,
 * otherwise the operation fails.
 *
 */


void* wg_attach_database(char* dbasename, gint size){
  void* shm = wg_attach_memsegment(dbasename, size, size, 1, 0, 0);
  CHECK_SEGMENT(shm)
  return shm;
}


/** returns a pointer to the existing database, NULL if
 *  there is no database with dbasename.
 *
 */

void* wg_attach_existing_database(char* dbasename){
  void* shm = wg_attach_memsegment(dbasename, 0, 0, 0, 0, 0);
  CHECK_SEGMENT(shm)
  return shm;
}

/**  returns a pointer to the database, NULL if failure
 *
 * Starts journal logging in the database.
 */

void* wg_attach_logged_database(char* dbasename, gint size){
  void* shm = wg_attach_memsegment(dbasename, size, size, 1, 1, 0);
  CHECK_SEGMENT(shm)
  return shm;
}

/**  returns a pointer to the database, NULL if failure
 *
 * Creates the database with given permission mode.
 * Otherwise performs like wg_attach_database().
 */

void* wg_attach_database_mode(char* dbasename, gint size, int mode){
  void* shm = wg_attach_memsegment(dbasename, size, size, 1, 0, mode);
  CHECK_SEGMENT(shm)
  return shm;
}

/**  returns a pointer to the database, NULL if failure
 *
 * Creates the database with given permission mode.
 * Otherwise performs like wg_attach_logged_database().
 */

void* wg_attach_logged_database_mode(char* dbasename, gint size, int mode){
  void* shm = wg_attach_memsegment(dbasename, size, size, 1, 1, mode);
  CHECK_SEGMENT(shm)
  return shm;
}


/** Normalize the mode for permissions.
 *
 */
static int normalize_perms(int mode) {
  /* Normalize the mode */
  mode &= 0666; /* kill the high bits and execute bits */
  mode |= 0600; /* owner can always read and write */
  /* group and others are either read-write or nothing */
  if((mode & 0060) != 0060)
    mode &= 0606;
  if((mode & 0006) != 0006)
    mode &= 0660;
  return mode;
}

/** Attach to shared memory segment.
 *  Normally called internally by wg_attach_database()
 *  May be called directly if the version compatibility of the
 *  memory image is not relevant (such as, when importing a dump
 *  file).
 */

void* wg_attach_memsegment(char* dbasename, gint minsize,
                               gint size, int create, int logging, int mode){
#ifdef USE_DATABASE_HANDLE
  void *dbhandle;
#endif
  void* shm;
  int err;
  int key=0;
#ifdef USE_DBLOG
  int omode;
#endif

#ifdef USE_DATABASE_HANDLE
  dbhandle = init_dbhandle();
  if(!dbhandle)
    return NULL;
#endif

  // default args handling
  if (dbasename!=NULL) key=strtol(dbasename,NULL,10);
  if (key<=0 || key==INT_MIN || key==INT_MAX) key=DEFAULT_MEMDBASE_KEY;
  if (minsize<0) minsize=0;
  if (size<minsize) size=minsize;

  // first try to link to already existing block with this key
  shm=link_shared_memory(key, &err);
  if (shm!=NULL) {
    /* managed to link to already existing shared memory block,
     * now check the header.
     */
    if(!dbcheckh(shm)) {
      show_memory_error("Existing segment header is invalid");
#ifdef USE_DATABASE_HANDLE
      free_dbhandle(dbhandle);
#endif
      return NULL;
    }
    if(minsize) {
      /* Check that the size of the segment is sufficient. We rely
       * on segment header being accurate. NOTE that shmget() also is capable
       * of checking the size, however under Windows the mapping size cannot
       * be checked accurately with system calls.
       */
      if(((db_memsegment_header *) shm)->size < minsize) {
        show_memory_error("Existing segment is too small");
#ifdef USE_DATABASE_HANDLE
        free_dbhandle(dbhandle);
#endif
        return NULL;
      }
    }
#ifdef USE_DATABASE_HANDLE
#ifdef USE_DBLOG
    if(logging) {
      /* If logging was requested and we're not initializing a new
       * segment, we should fail here if the existing database is
       * not actively logging.
       */
      if(!((db_memsegment_header *) shm)->logging.active) {
        show_memory_error("Existing memory segment has no journal");
        free_dbhandle(dbhandle);
        return NULL;
      }
    }
#endif
    ((db_handle *) dbhandle)->db = shm;
#ifdef USE_DBLOG
    /* Always set the umask for the logfile */
    omode = wg_memmode(dbhandle);
    if(omode == -1) {
      show_memory_error("Failed to get the access mode of the segment");
      free_dbhandle(dbhandle);
      return NULL;
    }
    wg_log_umask(dbhandle, ~omode);
#endif
#endif
#ifdef _WIN32
  } else if (!create) {
#else
  } else if (!create || err == EACCES) {
#endif
     /* linking to already existing block failed
        do not create a new base */
#ifdef USE_DATABASE_HANDLE
    free_dbhandle(dbhandle);
#endif
    return NULL;
  } else {
    /* linking to already existing block failed */
    /* create a new block if createnew_flag set
     *
     * When creating a new base, we have to select the size for the
     * memory segment. There are three possible scenarios:
     * - no size was requested. Use the default size.
     * - specific size was requested. Use it.
     * - a size and a minimum size were provided. First try the size
     *   given, if that fails fall back to minimum size.
     */
    if(!size) size = DEFAULT_MEMDBASE_SIZE;
    mode = normalize_perms(mode);
    shm = create_shared_memory(key, size, mode);
    if(!shm && minsize && minsize<size) {
      size = minsize;
      shm = create_shared_memory(key, size, mode);
    }

    if (shm==NULL) {
      show_memory_error("create_shared_memory failed");
#ifdef USE_DATABASE_HANDLE
      free_dbhandle(dbhandle);
#endif
      return NULL;
    } else {
#ifdef USE_DATABASE_HANDLE
      ((db_handle *) dbhandle)->db = shm;
      err=wg_init_db_memsegment(dbhandle, key, size);
#ifdef USE_DBLOG
      wg_log_umask(dbhandle, ~mode);
      if(!err && logging) {
        err = wg_start_logging(dbhandle);
      }
#endif
#else
      err=wg_init_db_memsegment(shm,key,size);
#endif
      if(err) {
        show_memory_error("Database initialization failed");
        free_shared_memory(key);
#ifdef USE_DATABASE_HANDLE
        free_dbhandle(dbhandle);
#endif
        return NULL;
      }
    }
  }
#ifdef USE_DATABASE_HANDLE
  return dbhandle;
#else
  return shm;
#endif
}


/** Detach database
 *
 * returns 0 if OK
 */
int wg_detach_database(void* dbase) {
  int err = detach_shared_memory(dbmemseg(dbase));
#ifdef USE_DATABASE_HANDLE
  if(!err) {
    free_dbhandle(dbase);
  }
#endif
  return err;
}


/** Delete a database
 *
 * returns 0 if OK
 */
int wg_delete_database(char* dbasename) {
  int key=0;

  // default args handling
  if (dbasename!=NULL) key=strtol(dbasename,NULL,10);
  if (key<=0 || key==INT_MIN || key==INT_MAX) key=DEFAULT_MEMDBASE_KEY;
  return free_shared_memory(key);
}



/* --------- local memory db creation and deleting ---------- */

/** Create a database in local memory
 * returns a pointer to the database, NULL if failure.
 */

void* wg_attach_local_database(gint size) {
  void* shm;
#ifdef USE_DATABASE_HANDLE
  void *dbhandle = init_dbhandle();
  if(!dbhandle)
    return NULL;
#endif

  if (size<=0) size=DEFAULT_MEMDBASE_SIZE;

  shm = (void *) malloc(size);
  if (shm==NULL) {
    show_memory_error("malloc failed");
    return NULL;
  } else {
    /* key=0 - no shared memory associated */
#ifdef USE_DATABASE_HANDLE
    ((db_handle *) dbhandle)->db = shm;
    if(wg_init_db_memsegment(dbhandle, 0, size)) {
#else
    if(wg_init_db_memsegment(shm, 0, size)) {
#endif
      show_memory_error("Database initialization failed");
      free(shm);
#ifdef USE_DATABASE_HANDLE
      free_dbhandle(dbhandle);
#endif
      return NULL;
    }
  }
#ifdef USE_DATABASE_HANDLE
  return dbhandle;
#else
  return shm;
#endif
}

/** Free a database in local memory
 * frees the allocated memory.
 */

void wg_delete_local_database(void* dbase) {
  if(dbase) {
    void *localmem = dbmemseg(dbase);
    if(localmem)
      free(localmem);
#ifdef USE_DATABASE_HANDLE
    free_dbhandle(dbase);
#endif
  }
}


/* -------------------- database handle management -------------------- */

#ifdef USE_DATABASE_HANDLE

static void *init_dbhandle() {
  void *dbhandle = malloc(sizeof(db_handle));
  if(!dbhandle) {
    show_memory_error("Failed to allocate the db handle");
    return NULL;
  } else {
    memset(dbhandle, 0, sizeof(db_handle));
  }
#ifdef USE_DBLOG
  if(wg_init_handle_logdata(dbhandle)) {
    free(dbhandle);
    return NULL;
  }
#endif
  return dbhandle;
}

static void free_dbhandle(void *dbhandle) {
#ifdef USE_DBLOG
  wg_cleanup_handle_logdata(dbhandle);
#endif
  free(dbhandle);
}

#endif

/* ----------------- memory image/dump compatibility ------------------ */

/** Check compatibility of memory image (or dump file) header
 *
 * Note: unlike API functions, this functions works directly on
 * the (db_memsegment_header *) pointer.
 *
 * returns 0 if header is compatible with current executable
 * returns -1 if header is not recognizable
 * returns -2 if header has wrong endianness
 * returns -3 if header version does not match
 * returns -4 if compile-time features do not match
 */
int wg_check_header_compat(db_memsegment_header *dbh) {
  /*
   * Check:
   * - magic marker (including endianness)
   * - version
   */

  if(!dbcheckh(dbh)) {
    gint32 magic = MEMSEGMENT_MAGIC_MARK;
    char *magic_bytes = (char *) &magic;
    char *header_bytes = (char *) dbh;

    if(magic_bytes[0]==header_bytes[3] && magic_bytes[1]==header_bytes[2] &&\
       magic_bytes[2]==header_bytes[1] && magic_bytes[3]==header_bytes[0]) {
      return -2; /* wrong endianness */
    }
    else {
      return -1; /* unknown marker (not a valid header) */
    }
  }
  if(dbh->version!=MEMSEGMENT_VERSION) {
    return -3;
  }
  if(dbh->features!=MEMSEGMENT_FEATURES) {
    return -4;
  }
  return 0;
}

void wg_print_code_version(void) {
  int i = 1;
  char *i_bytes = (char *) &i;

  printf("\nlibwgdb version: %d.%d.%d\n", VERSION_MAJOR, VERSION_MINOR,
    VERSION_REV);
  printf("byte order: %s endian\n", (i_bytes[0]==1 ? "little" : "big"));
  printf("compile-time features:\n"\
    "  64-bit encoded data: %s\n"\
    "  queued locks: %s\n"\
    "  chained nodes in T-tree: %s\n"\
    "  record backlinking: %s\n"\
    "  child databases: %s\n"\
    "  index templates: %s\n",
    (MEMSEGMENT_FEATURES & FEATURE_BITS_64BIT ? "yes" : "no"),
    (MEMSEGMENT_FEATURES & FEATURE_BITS_QUEUED_LOCKS ? "yes" : "no"),
    (MEMSEGMENT_FEATURES & FEATURE_BITS_TTREE_CHAINED ? "yes" : "no"),
    (MEMSEGMENT_FEATURES & FEATURE_BITS_BACKLINK ? "yes" : "no"),
    (MEMSEGMENT_FEATURES & FEATURE_BITS_CHILD_DB ? "yes" : "no"),
    (MEMSEGMENT_FEATURES & FEATURE_BITS_INDEX_TMPL ? "yes" : "no"));
}

void wg_print_header_version(db_memsegment_header *dbh, int verbose) {
  gint32 version, features;
  gint32 magic = MEMSEGMENT_MAGIC_MARK;
  char *magic_bytes = (char *) &magic;
  char *header_bytes = (char *) dbh;
  char magic_lsb = (char) (MEMSEGMENT_MAGIC_MARK & 0xff);

  /* Header might be incompatible, but to display version and feature
   * information, we still need to read it somehow, even if
   * it has wrong endianness.
   */
  if(magic_bytes[0]==header_bytes[3] && magic_bytes[1]==header_bytes[2] &&\
     magic_bytes[2]==header_bytes[1] && magic_bytes[3]==header_bytes[0]) {
    char *f1 = (char *) &(dbh->version);
    char *t1 = (char *) &version;
    char *f2 = (char *) &(dbh->features);
    char *t2 = (char *) &features;
    int i;
    for(i=0; i<4; i++) {
      t1[i] = f1[3-i];
      t2[i] = f2[3-i];
    }
  } else {
    version = dbh->version;
    features = dbh->features;
  }

  if(verbose) {
    printf("\nheader version: %d.%d.%d\n", (version & 0xff),
      ((version>>8) & 0xff), ((version>>16) & 0xff));
    printf("byte order: %s endian\n",
      (header_bytes[0]==magic_lsb ? "little" : "big"));
    printf("compile-time features:\n"\
      "  64-bit encoded data: %s\n"\
      "  queued locks: %s\n"\
      "  chained nodes in T-tree: %s\n"\
      "  record backlinking: %s\n"\
      "  child databases: %s\n"\
      "  index templates: %s\n",
      (features & FEATURE_BITS_64BIT ? "yes" : "no"),
      (features & FEATURE_BITS_QUEUED_LOCKS ? "yes" : "no"),
      (features & FEATURE_BITS_TTREE_CHAINED ? "yes" : "no"),
      (features & FEATURE_BITS_BACKLINK ? "yes" : "no"),
      (features & FEATURE_BITS_CHILD_DB ? "yes" : "no"),
      (features & FEATURE_BITS_INDEX_TMPL ? "yes" : "no"));
  } else {
    printf("%d.%d.%d%s\n",
      (version & 0xff), ((version>>8) & 0xff), ((version>>16) & 0xff),
      (features & FEATURE_BITS_64BIT ? " (64-bit)" : ""));
  }
}

/* --------------------  memory image stats --------------------------- */

#ifndef _WIN32
/** Get the shared memory stats structure.
 *  Returns 0 on success.
 *  Returns -1 if the database is local.
 *  Returns -2 on error.
 */
static int memory_stats(void *db, struct shmid_ds *buf) {
  db_memsegment_header* dbh = dbmemsegh(db);

  if(dbh->key) {
    int shmid = shmget((key_t) dbh->key, 0, 0);
    if(shmid < 0) {
      show_memory_error("memory_stats(): failed to get shmid");
      return -2;
    } else {
      int err;

      memset(buf, 0, sizeof(struct shmid_ds));
      err = shmctl(shmid, IPC_STAT, buf);
      if(err) {
        show_memory_error("memory_stats(): failed to stat shared memory");
        return -2;
      }
      return 0;
    }
  }
  return -1;
}
#endif

/** Return the mode bits of the shared memory permissions.
 *  Defaults to 0600 in cases where this does not apply directly.
 */
int wg_memmode(void *db) {
  int mode = 0600; /* default for local memory and Win32 */
#ifndef _WIN32
  struct shmid_ds buf;
  int err = memory_stats(db, &buf);
  if(!err) {
    mode = (int) buf.shm_perm.mode;
  } else if(err < -1) {
    return -1;
  }
#endif
  return mode;
}

/** Return the uid of the owner of the segment.
 *  returns -1 on error.
 */
int wg_memowner(void *db) {
#ifdef _WIN32
  int uid = 0;
#else
  int uid = getuid(); /* default for local memory */
  struct shmid_ds buf;
  int err = memory_stats(db, &buf);
  if(!err) {
    uid = (int) buf.shm_perm.uid;
  } else if(err < -1) {
    return -1;
  }
#endif
  return uid;
}

/** Return the gid of the owner of the segment.
 *  returns -1 on error.
 */
int wg_memgroup(void *db) {
#ifdef _WIN32
  int gid = 0;
#else
  int gid = getgid(); /* default for local memory */
  struct shmid_ds buf;
  int err = memory_stats(db, &buf);
  if(!err) {
    gid = (int) buf.shm_perm.gid;
  } else if(err < -1) {
    return -1;
  }
#endif
  return gid;
}

/* --------------- dbase create/delete ops not in api ----------------- */


static void* link_shared_memory(int key, int *errcode) {
  void *shm;

#ifdef _WIN32
  char fname[MAX_FILENAME_SIZE];
  HANDLE hmapfile;

  sprintf_s(fname,MAX_FILENAME_SIZE-1,"%d",key);
  hmapfile = OpenFileMapping(
                   FILE_MAP_ALL_ACCESS,   // read/write access
                   FALSE,                 // do not inherit the name
                   fname);               // name of mapping object
  errno = 0;
  *errcode = 0;
  if (hmapfile == NULL) {
      /* this is an expected error, message in most cases not wanted */
      return NULL;
   }
   shm = (void*) MapViewOfFile(hmapfile,   // handle to map object
                        FILE_MAP_ALL_ACCESS, // read/write permission
                        0,
                        0,
                        0);   // size of mapping
   if (shm == NULL)  {
      show_memory_error_nr("Could not map view of file",
        (int) GetLastError());
      CloseHandle(hmapfile);
      return NULL;
   }
   return shm;
#else
  int shmid; /* return value from shmget() */

  errno = 0;
  *errcode = 0;
  // Link to existing segment
  shmid=shmget((key_t)key, 0, 0);
  if (shmid < 0) {
    return NULL;
  }
  // Attach the segment to our data space
  shm=shmat(shmid,NULL,0);
  if (shm==(char *) -1) {
    *errcode = errno;
    if(*errcode == EACCES) {
      show_memory_error("cannot attach to shared memory (No permission)");
      return NULL;
    } else {
      show_memory_error("attaching shared memory segment failed");
      return NULL;
    }
  }
  return (void*) shm;
#endif
}



static void* create_shared_memory(int key, gint size, int mode) {
  void *shm;

#ifdef _WIN32
  char fname[MAX_FILENAME_SIZE];
  HANDLE hmapfile;

  sprintf_s(fname,MAX_FILENAME_SIZE-1,"%d",key);

  /* XXX: need to interpret the mode value here.
   * Right now the shared segment is created using the
   * default permissions, in the local namespace.
   */
  hmapfile = CreateFileMapping(
                 INVALID_HANDLE_VALUE,    // use paging file
                 NULL,                    // default security
                 PAGE_READWRITE,          // read/write access
                 0,                       // max. object size
                 size,                   // buffer size
                 fname);                 // name of mapping object
  errno = 0;
  if (hmapfile == NULL) {
      show_memory_error_nr("Could not create file mapping object",
        (int) GetLastError());
      return NULL;
   }
   shm = (void*) MapViewOfFile(hmapfile,   // handle to map object
                        FILE_MAP_ALL_ACCESS, // read/write permission
                        0,
                        0,
                        0);
   if (shm == NULL)  {
      show_memory_error_nr("Could not map view of file",
        (int) GetLastError());
      CloseHandle(hmapfile);
      return NULL;
   }
   return shm;
#else
  int shmflg; /* shmflg to be passed to shmget() */
  int shmid; /* return value from shmget() */

  // Create the segment
  shmflg=IPC_CREAT | IPC_EXCL | mode;
  shmid=shmget((key_t)key,size,shmflg);
  if (shmid < 0) {
    switch(errno) {
      case EEXIST:
        show_memory_error("creating shared memory segment: "\
          "Race condition detected when initializing");
        break;
      case EINVAL:
        show_memory_error("creating shared memory segment: "\
          "Specified segment size too large or too small");
        break;
      case ENOMEM:
        show_memory_error("creating shared memory segment: "\
          "Not enough physical memory");
        break;
      default:
        /* Generic error */
        show_memory_error("creating shared memory segment failed");
        break;
    }
    return NULL;
  }
  // Attach the segment to our data space
  shm=shmat(shmid,NULL,0);
  if (shm==(char *) -1) {
    show_memory_error("attaching shared memory segment failed");
    return NULL;
  }
  return (void*) shm;
#endif
}



static int free_shared_memory(int key) {
#ifdef _WIN32
  return 0;
#else
  int shmflg; /* shmflg to be passed to shmget() */
  int shmid; /* return value from shmget() */
  int tmp;

  errno = 0;
   // Link to existing segment
  shmflg=0666;
  shmid=shmget((key_t)key, 0, shmflg);
  if (shmid < 0) {
    switch(errno) {
      case EACCES:
        show_memory_error("linking to shared memory segment (for freeing): "\
          "Access denied");
        break;
      case ENOENT:
        show_memory_error("linking to shared memory segment (for freeing): "\
          "Segment does not exist");
        break;
      default:
        show_memory_error("linking to shared memory segment (for freeing) failed");
        break;
    }
    return -1;
  }
  // Free the segment
  tmp=shmctl(shmid, IPC_RMID, NULL);
  if (tmp==-1) {
    switch(errno) {
      case EPERM:
        show_memory_error("freeing shared memory segment: "\
          "Permission denied");
        break;
      default:
        show_memory_error("freeing shared memory segment failed");
        break;
    }
    return -2;
  }
  return 0;
#endif
}



static int detach_shared_memory(void* shmptr) {
#ifdef _WIN32
  return 0;
#else
  int tmp;

  // detach the segment
  tmp=shmdt(shmptr);
  if (tmp==-1) {
    show_memory_error("detaching shared memory segment failed");
    return -2;
  }
  return 0;
#endif
}


/* ------------ error handling ---------------- */

/** Handle memory error
 * since these errors mostly indicate a fatal error related to database
 * memory allocation, the db pointer is not very useful here and is
 * omitted.
 */
static gint show_memory_error(char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg memory error: %s.\n", errmsg);
#endif
  return -1;
}

#ifdef _WIN32
static gint show_memory_error_nr(char* errmsg, int nr) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"db memory allocation error: %s %d\n", errmsg, nr);
#endif
  return -1;
}
#endif

#ifdef __cplusplus
}
#endif
