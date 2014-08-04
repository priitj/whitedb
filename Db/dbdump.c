/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009, Priit Järv 2009,2010,2013,2014
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

 /** @file dbdump.c
 *  DB dumping support for WhiteDB memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <malloc.h>
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
#include "dbmem.h"
#include "dblock.h"
#include "dblog.h"

/* ====== Private headers and defs ======== */

#include "dbdump.h"
#include "crc1.h"

/* ======= Private protos ================ */


static gint show_dump_error(void *db, char *errmsg);
static gint show_dump_error_str(void *db, char *errmsg, char *str);


/* ====== Functions ============== */


/** dump shared memory to the disk.
 *  Returns 0 when successful (no error).
 *  -1 non-fatal error (db may continue)
 *  -2 fatal error (should abort db)
 *  This function is parallel-safe (may run during normal db usage)
 */

gint wg_dump(void * db,char fileName[]) {
  return wg_dump_internal(db, fileName, 1);
}

/** Handle the actual dumping (called by the API wrapper)
 *  if locking is non-zero, properly acquire locks on the database.
 *  Otherwise do a rescue dump by copying the memory image without locking.
 */
gint wg_dump_internal(void * db, char fileName[], int locking) {
  FILE *f;
  db_memsegment_header* dbh = dbmemsegh(db);
  gint dbsize = dbh->free; /* first unused offset - 0 = db size */
#ifdef USE_DBLOG
  gint active;
#endif
  gint err = -1;
  gint lock_id = 0;
  gint32 crc;

#ifdef CHECK
  if(dbh->extdbs.count != 0) {
    show_dump_error(db, "Database contains external references");
  }
#endif

  /* Open the dump file */
#ifdef _WIN32
  if(fopen_s(&f, fileName, "wb")) {
#else
  if(!(f = fopen(fileName, "wb"))) {
#endif
    show_dump_error(db, "Error opening file");
    return -1;
  }

#ifndef USE_DBLOG
  /* Get shared lock on the db */
  if(locking) {
    lock_id = db_rlock(db, DEFAULT_LOCK_TIMEOUT);
    if(!lock_id) {
      show_dump_error(db, "Failed to lock the database for dump");
      return -1;
    }
  }
#else
  /* Get exclusive lock on the db, we need to modify the logging area */
  if(locking) {
    lock_id = db_wlock(db, DEFAULT_LOCK_TIMEOUT);
    if(!lock_id) {
      show_dump_error(db, "Failed to lock the database for dump");
      return -1;
    }
  }

  active = dbh->logging.active;
  if(active) {
    wg_stop_logging(db);
  }
#endif

  /* Compute the CRC32 of the used area */
  crc = update_crc32(dbmemsegbytes(db), dbsize, 0x0);

  /* Now, write the memory area to file */
  if(fwrite(dbmemseg(db), dbsize, 1, f) == 1) {
    /* Overwrite checksum field */
    fseek(f, ptrtooffset(db, &(dbh->checksum)), SEEK_SET);
    if(fwrite(&crc, sizeof(gint32), 1, f) == 1) {
      err = 0;
    }
  }

  if(err)
    show_dump_error(db, "Error writing file");

#ifndef USE_DBLOG
  /* We're done writing */
  if(locking) {
    if(!db_rulock(db, lock_id)) {
      show_dump_error(db, "Failed to unlock the database");
      err = -2; /* This error should be handled as fatal */
    }
  }
#else
  /* restart logging */
  if(active) {
    dbh->logging.dirty = 0;
    if(wg_start_logging(db)) {
      err = -2; /* Failed to re-initialize log */
    }
  }

  if(locking) {
    if(!db_wulock(db, lock_id)) {
      show_dump_error(db, "Failed to unlock the database");
      err = -2; /* Write lock failure --> fatal */
    }
  }
#endif

  fflush(f);
  fclose(f);

  return err;
}


/* This has to be large enough to hold all the relevant
 * fields in the header during the first pass of the read.
 * (Currently this is the first 24 bytes of the dump file)
 */
#define BUFSIZE 8192

/** Check dump file for compatibility and errors.
 *  Returns 0 when successful (no error).
 *  -1 on system error (cannot open file, no memory)
 *  -2 header is incompatible
 *  -3 on file integrity error (size mismatch, CRC32 error).
 *
 *  Sets minsize to minimum required segment size and maxsize
 *  to original memory image size if check was successful. Otherwise
 *  the contents of these variables may be undefined.
 */
gint wg_check_dump(void *db, char fileName[], gint *minsize, gint *maxsize) {
  char *buf;
  FILE *f;
  gint len, filesize;
  gint32 crc, dump_crc;
  gint err = -1;

  /* Attempt to open the dump file */
#ifdef _WIN32
  if(fopen_s(&f, fileName, "rb")) {
#else
  if(!(f = fopen(fileName, "rb"))) {
#endif
    show_dump_error(db, "Error opening file");
    return -1;
  }

  buf = (char *) malloc(BUFSIZE);
  if(!buf) {
    show_dump_error(db, "malloc error in wg_import_dump");
    goto abort1;
  }

  /* First pass of reading. Examine the header. */
  if(fread(buf, BUFSIZE, 1, f) != 1) {
    show_dump_error(db, "Error reading dump header");
    goto abort2;
  }

  if(wg_check_header_compat((db_memsegment_header *) buf)) {
    show_dump_error_str(db, "Incompatible dump file", fileName);
    wg_print_code_version();
    wg_print_header_version((db_memsegment_header *) buf, 1);
    err = -2;
    goto abort2;
  }

  *minsize = ((db_memsegment_header *) buf)->free;
  *maxsize = ((db_memsegment_header *) buf)->size;

  /* Now check file integrity. */
  dump_crc = ((db_memsegment_header *) buf)->checksum;
  ((db_memsegment_header *) buf)->checksum = 0;
  len = BUFSIZE;
  filesize = 0;
  crc = 0;
  do {
    filesize += len;
    crc = update_crc32(buf, len, crc);
  } while((len=fread(buf,1,BUFSIZE,f)) > 0);

  if(filesize != *minsize) {
    show_dump_error_str(db, "File size incorrect", fileName);
    err = -3;
  }
  else if(crc != dump_crc) {
    show_dump_error_str(db, "File CRC32 incorrect", fileName);
    err = -3;
  }
  else
    err = 0;

abort2:
  free(buf);
abort1:
  fclose(f);

  return err;
}


/** Import database dump from disk.
 *  Returns 0 when successful (no error).
 *  -1 non-fatal error (db may continue)
 *  -2 fatal error (should abort db)
 *
 *  this function is NOT parallel-safe. Other processes accessing
 *  db concurrently may cause undefined behaviour (including data loss)
 */
gint wg_import_dump(void * db,char fileName[]) {
  db_memsegment_header* dumph;
  FILE *f;
  db_memsegment_header* dbh = dbmemsegh(db);
  gint dbsize = -1, newsize;
  gint err = -1;
#ifdef USE_DBLOG
  gint active = dbh->logging.active;
#endif


  /* Attempt to open the dump file */
#ifdef _WIN32
  if(fopen_s(&f, fileName, "rb")) {
#else
  if(!(f = fopen(fileName, "rb"))) {
#endif
    show_dump_error(db, "Error opening file");
    return -1;
  }

  /* Examine the dump header. We only read the size, it is
   * implied that the integrity and compatibility were verified
   * earlier.
   */
  dumph = (db_memsegment_header *) malloc(sizeof(db_memsegment_header));
  if(!dumph) {
    show_dump_error(db, "malloc error in wg_import_dump");
  }
  else if(fread(dumph, sizeof(db_memsegment_header), 1, f) != 1) {
    show_dump_error(db, "Error reading dump header");
  }
  else {
    dbsize = dumph->free;
    if(dumph->extdbs.count != 0) {
      show_dump_error(db, "Dump contains external references");
      goto abort;
    }
  }
  if(dumph) free(dumph);

  /* 0 > dbsize >= dbh->size indicates that we were able to read the dump
   * and it contained a memory image that fits in our current shared memory.
   */
  if(dbh->size < dbsize) {
    show_dump_error(db, "Data does not fit in shared memory area");
  } else if(dbsize > 0) {
    /* We have a compatible dump file. */
    newsize = dbh->size;
    fseek(f, 0, SEEK_SET);
    if(fread(dbmemseg(db), dbsize, 1, f) != 1) {
      show_dump_error(db, "Error reading dump file");
      err = -2; /* database is in undetermined state now */
    } else {
      err = 0;
      dbh->size = newsize;
      dbh->checksum = 0;
    }
  }

abort:
  fclose(f);

  /* any errors up to now? */
  if(err) return err;

  /* Initialize db state */
#ifdef USE_DBLOG
  /* restart logging */
  dbh->logging.dirty = 0;
  dbh->logging.active = 0;
  if(active) { /* state inherited from memory */
    if(wg_start_logging(db)) {
      return -2; /* Failed to re-initialize log */
    }
  }
#endif
  return wg_init_locks(db);
}

/* ------------ error handling ---------------- */

static gint show_dump_error(void *db, char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg dump error: %s.\n", errmsg);
#endif
  return -1;

}

static gint show_dump_error_str(void *db, char *errmsg, char *str) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg dump error: %s: %s.\n", errmsg, str);
#endif
  return -1;
}

#ifdef __cplusplus
}
#endif
