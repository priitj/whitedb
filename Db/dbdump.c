/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009, Priit Järv 2009,2010
*
* This file is part of wgandalf
*
* Wgandalf is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
* 
* Wgandalf is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with Wgandalf.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file dbdump.c
 *  DB dumping support for wgandalf memory database
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
/*#include "dbmem.h"*/

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
  FILE *f;
  db_memsegment_header* dbh = (db_memsegment_header *) db;
  gint dbsize = dbh->free; /* first unused offset - 0 = db size */
  gint err = -1;
  gint lock_id;
  gint32 crc;

#ifdef CHECK
  if(((db_memsegment_header *) db)->extdbs.count != 0) {
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

  /* Get shared lock on the db */
#ifdef USE_LOCK_TIMEOUT
  lock_id = wg_db_rlock(db, DEFAULT_LOCK_TIMEOUT);
#else
  lock_id = wg_db_rlock(db);
#endif
  if(!lock_id) {
    show_dump_error(db, "Failed to lock the database for dump");
    return -1;
  }

  /* Compute the CRC32 of the used area */
  crc = update_crc32((char *) db, dbsize, 0x0);

  /* Now, write the memory area to file */
  if(fwrite(db, dbsize, 1, f) == 1) {
    /* Overwrite checksum field */
    fseek(f, ptrtooffset(db, &(dbh->checksum)), SEEK_SET);
    if(fwrite(&crc, sizeof(gint32), 1, f) == 1) {
      err = 0;
    }
  }

  if(err)
    show_dump_error(db, "Error writing file");

  /* We're done writing */
  if(!wg_db_rulock(db, lock_id)) {
    show_dump_error(db, "Failed to unlock the database");
    err = -2; /* This error should be handled as fatal */
  }

  fflush(f);
  fclose(f);

  /* Get exclusive lock to modify the logging area */
#ifdef USE_LOCK_TIMEOUT
  lock_id = wg_db_wlock(db, DEFAULT_LOCK_TIMEOUT);
#else
  lock_id = wg_db_wlock(db);
#endif
  if(!lock_id) {
    show_dump_error(db, "Failed to lock the database for log reset");
    return -2; /* Logging area inconsistent --> fatal. */
  }

  //flush logging
  while(dbh->logging.logoffset>dbh->logging.firstoffset)
  {
    //write zeros to logging area
    dbstore(db,dbh->logging.logoffset,0);
    dbh->logging.logoffset--;        
  }

  if(!wg_db_wulock(db, lock_id)) {
    show_dump_error(db, "Failed to unlock the database");
    err = -2; /* Write lock failure --> fatal */
  }
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

  if(wg_check_header_compat((void *) buf)) {
    show_dump_error_str(db, "Incompatible dump file", fileName);
    wg_print_code_version();
    wg_print_header_version((void *) buf);
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

  /* Check for registered external data sources */
  if(((db_memsegment_header *) buf)->extdbs.count != 0) {
    show_dump_error(db, "Dump contains external references");
    err = -2;
  }

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
  db_memsegment_header* dbh = (db_memsegment_header *) db;
  gint dbsize = -1, newsize;
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
    if(fread(db, dbsize, 1, f) != 1) {
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
  /* XXX: logging ignored here, for now */
  return wg_init_locks(db);
}

/* ------------ error handling ---------------- */

static gint show_dump_error(void *db, char *errmsg) {
  fprintf(stderr,"wg dump error: %s.\n", errmsg);
  return -1;
}

static gint show_dump_error_str(void *db, char *errmsg, char *str) {
  fprintf(stderr,"wg dump error: %s: %s.\n", errmsg, str);
  return -1;
}

#ifdef __cplusplus
}
#endif
