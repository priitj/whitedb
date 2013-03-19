/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
* Copyright (c) Priit Järv 2013
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

 /** @file dblog.c
 *  DB logging support for wgandalf memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
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
#include "dbdata.h"

/* ====== Private headers and defs ======== */

#include "dblog.h"

#if defined(USE_DBLOG) && !defined(USE_DATABASE_HANDLE)
#error Logging requires USE_DATABASE_HANDLE
#endif

/*#define USE_FCNTL*/ /* lock the file when writing to it. */

#ifdef USE_FCNTL
#define JOURNAL_FAIL(f, e) \
  unlock_journal(fileno(f)); \
  fclose(f); \
  return e;
#else
#define JOURNAL_FAIL(f, e) \
  fclose(f); \
  return e;
#endif

#define TRAN_TABLE_BASE_SZ 10

#define GET_LOG_GINT(d, f, v) \
  if(fread((char *) &v, sizeof(gint), 1, f) != 1) { \
    return show_log_error(d, "Failed to read log entry"); \
  }

#define GET_LOG_GINT_CMD(d, f, v) \
  if(fread((char *) &v, sizeof(gint), 1, f) != 1) { \
    if(feof(f)) break; \
    else return show_log_error(d, "Failed to read log entry"); \
  }

#define GET_LOG_GINT_ERR(d, f, v, e) \
  if(fread((char *) &v, sizeof(gint), 1, f) != 1) { \
    show_log_error(d, "Failed to read log entry"); \
    return e; \
  }

/* ====== data structures ======== */

/* XXX: this defines a simplistic, linear table.
 * replace with a hash table for more performance.
 */
typedef struct {
  gint old;
  gint new;
} tran_table_entry;

typedef struct {
  tran_table_entry *subtable;
  int subt_sz;
  int subt_idx;
} tran_table_meta;

/* ======= Private protos ================ */

#ifdef USE_DBLOG
static gint lock_journal(int fd, gint try, gint exclusive);
static gint unlock_journal(int fd);
static gint check_journal(void *db, FILE *f);

static tran_table_meta *create_recover_tran_table(int sz);
static void free_recover_tran_table(tran_table_meta *table, int sz);
static gint add_tran_offset(tran_table_meta *table, int sz,
  gint old, gint new);
static gint add_tran_enc(tran_table_meta *table, int sz,
  gint old, gint new);
static gint translate_offset(tran_table_meta *table,
  int sz, gint offset);
static gint translate_encoded(tran_table_meta *table, int sz, gint enc);
static gint recover_encode(void *db, FILE *f, gint type);
static gint recover_journal(void *db, FILE *f, tran_table_meta *table,
  int sz);

static gint write_log_buffer(void *db, void *buf, int buflen);
#endif

static gint show_log_error(void *db, char *errmsg);

/* ====== Functions ============== */

#ifdef USE_DBLOG
/** Helper to lock a file.
 *
 */
static gint lock_journal(int fd, gint try, gint exclusive) {
  struct flock flbuf;
  memset(&flbuf, 0, sizeof(flbuf));
  flbuf.l_type = exclusive ? F_WRLCK : F_RDLCK;
  flbuf.l_whence = SEEK_SET;
  flbuf.l_start = 0;
  flbuf.l_len = 0;
  flbuf.l_pid = 0;
  if(fcntl(fd, (try ? F_SETLK : F_SETLKW), &flbuf) != 0) {
    return -1;
  } else {
    return 0;
  }
}

/** Helper to unlock a file.
 *
 */
static gint unlock_journal(int fd) {
  struct flock flbuf;
  memset(&flbuf, 0, sizeof(flbuf));
  flbuf.l_type = F_UNLCK;
  flbuf.l_whence = SEEK_SET;
  flbuf.l_start = 0;
  flbuf.l_len = 0;
  flbuf.l_pid = 0;
  if(fcntl(fd, F_SETLKW, &flbuf) != 0) {
    return -1;
  } else {
    return 0;
  }
}

/** Check the file magic of the journal file.
 *
 * Since the files are opened in append mode, we don't need to
 * seek before or after reading the header (on Linux).
 */
static gint check_journal(void *db, FILE *f) {
  char buf[WG_JOURNAL_MAGIC_BYTES + 1];
  /* fseek(f, 0, SEEK_SET); */
  if(fread(buf, WG_JOURNAL_MAGIC_BYTES, 1, f) != 1) {
    return show_log_error(db, "Error checking log file");
  }
  buf[WG_JOURNAL_MAGIC_BYTES] = '\0';
  if(strncmp(buf, WG_JOURNAL_MAGIC, WG_JOURNAL_MAGIC_BYTES)) {
    return show_log_error(db, "Bad log file magic");
  }
  return 0;
}


/** Set up the log recovery translation table
 * max memory is TRAN_TABLE_BASE_SZ * 2^(sz-1)
 */
static tran_table_meta *create_recover_tran_table(int sz)
{
  tran_table_meta *table = malloc(sizeof(tran_table_meta) * sz);
  if(!table)
    return NULL;
  memset(table, 0, sizeof(tran_table_meta) * sz);
  table[0].subtable = malloc(sizeof(tran_table_entry) * TRAN_TABLE_BASE_SZ);
  if(!table[0].subtable) {
    free(table);
    return NULL;
  }
  table[0].subt_sz = TRAN_TABLE_BASE_SZ;
  return table;
}


/** Free the log recovery translation table
 *
 */
static void free_recover_tran_table(tran_table_meta *table, int sz)
{
  int i;
  for(i=0; i<sz; i++) {
    if(table[i].subtable)
      free(table[i].subtable);
  }
  free(table);
}


/** Add a log recovery translation entry
 * Fails when out of memory or out of max subtable space.
 */
static gint add_tran_offset(tran_table_meta *table, int sz,
  gint old, gint new)
{
  int i;
  for(i=0; i<sz; i++) {
    if(!table[i].subtable) {
      /* allocate a new table. This won't happen at table[0]. */
      int nextsz = table[i-1].subt_sz << 1;
      table[i].subtable = malloc(sizeof(tran_table_entry) * nextsz);
      if(table[i].subtable) {
        table[i].subt_sz = nextsz;
        table[i].subtable[0].old = old;
        table[i].subtable[0].new = new;
        table[i].subt_idx = 1;
        return 0;
      } else {
        break;
      }
    } else if(table[i].subt_idx < table[i].subt_sz) {
      tran_table_entry *next = &table[i].subtable[table[i].subt_idx++];
      next->old = old;
      next->new = new;
      return 0;
    }
  }
  return -1;
}

/** Wrapper around add_tran_offset() to handle encoded data
 *
 */
static gint add_tran_enc(tran_table_meta *table, int sz,
  gint old, gint new)
{
  if(isptr(old)) {
    gint offset, newoffset;
    switch(old & NORMALPTRMASK) {
      case LONGSTRBITS:
        offset = decode_longstr_offset(old);
        newoffset = decode_longstr_offset(new);
        return add_tran_offset(table, sz, offset, newoffset);
      case SHORTSTRBITS:
        offset = decode_shortstr_offset(old);
        newoffset = decode_shortstr_offset(new);
        return add_tran_offset(table, sz, offset, newoffset);
      case FULLDOUBLEBITS:
        offset = decode_fulldouble_offset(old);
        newoffset = decode_fulldouble_offset(new);
        return add_tran_offset(table, sz, offset, newoffset);
      case FULLINTBITSV0:
      case FULLINTBITSV1:
        offset = decode_fullint_offset(old);
        newoffset = decode_fullint_offset(new);
        return add_tran_offset(table, sz, offset, newoffset);
      default:
        return 0;
    }
  }
  return 0;
}

/** Translate a log offset
 *
 */
static gint translate_offset(tran_table_meta *table, int sz, gint offset)
{
  int i;
  for(i=0; i<sz; i++) {
    tran_table_meta *meta = &table[i];
    if(meta->subtable) {
      int j, nextsz = meta->subt_sz;
      for(j=0; j<nextsz; j++) {
        if(meta->subtable[j].old == offset)
          return meta->subtable[j].new;
      }
    }
  }
  return offset; /* not found ==> no change */
}

/** Wrapper around translate_offset() to handle encoded data
 *
 */
static gint translate_encoded(tran_table_meta *table, int sz, gint enc)
{
  if(isptr(enc)) {
    gint offset;
    switch(enc & NORMALPTRMASK) {
      case LONGSTRBITS:
        offset = decode_longstr_offset(enc);
        return encode_longstr_offset(translate_offset(table, sz, offset));
      case SHORTSTRBITS:
        offset = decode_shortstr_offset(enc);
        return encode_shortstr_offset(translate_offset(table, sz, offset));
      case FULLDOUBLEBITS:
        offset = decode_fulldouble_offset(enc);
        return encode_fulldouble_offset(translate_offset(table, sz, offset));
      case FULLINTBITSV0:
      case FULLINTBITSV1:
        offset = decode_fullint_offset(enc);
        return encode_fullint_offset(translate_offset(table, sz, offset));
      default:
        return enc;
    }
  }
  return enc;
}

/** Parse an encode entry from the log.
 *
 */
gint recover_encode(void *db, FILE *f, gint type)
{
  char *strbuf, *extbuf;
  gint length, extlength, enc;
  int intval;
  double doubleval;

  switch(type) {
    case WG_INTTYPE:
      if(fread((char *) &intval, sizeof(int), 1, f) != 1) {
        show_log_error(db, "Failed to read log entry");
        return WG_ILLEGAL;
      }
      return wg_encode_int(db, intval);
    case WG_DOUBLETYPE:
      if(fread((char *) &doubleval, sizeof(double), 1, f) != 1) {
        show_log_error(db, "Failed to read log entry");
        return WG_ILLEGAL;
      }
      return wg_encode_double(db, doubleval);
    case WG_STRTYPE:
    case WG_URITYPE:
    case WG_XMLLITERALTYPE:
    case WG_ANONCONSTTYPE:
    case WG_BLOBTYPE: /* XXX: no encode func for this yet */
      /* strings with extdata */
      GET_LOG_GINT_ERR(db, f, length, WG_ILLEGAL)
      GET_LOG_GINT_ERR(db, f, extlength, WG_ILLEGAL)

      strbuf = (char *) malloc(length + 1);
      if(!strbuf) {
        show_log_error(db, "Failed to allocate buffers");
        return WG_ILLEGAL;
      }
      if(fread(strbuf, 1, length, f) != length) {
        show_log_error(db, "Failed to read log entry");
        free(strbuf);
        return WG_ILLEGAL;
      }
      strbuf[length] = '\0';

      if(extlength) {
        extbuf = (char *) malloc(extlength + 1);
        if(!extbuf) {
          free(strbuf);
          show_log_error(db, "Failed to allocate buffers");
          return WG_ILLEGAL;
        }
        if(fread(extbuf, 1, extlength, f) != extlength) {
          show_log_error(db, "Failed to read log entry");
          free(strbuf);
          free(extbuf);
          return WG_ILLEGAL;
        }
        extbuf[extlength] = '\0';
      } else {
        extbuf = NULL;
      }

      enc = wg_encode_unistr(db, strbuf, extbuf, type);
      free(strbuf);
      if(extbuf)
        free(extbuf);
      return enc;
    default:
      break;
  }

  return show_log_error(db, "Unsupported data type");
}

/** Parse the journal file. Used internally only.
 *
 */
static gint recover_journal(void *db, FILE *f, tran_table_meta *table,
  int sz)
{
  gint cmd;
  gint length, offset, newoffset;
  gint col, type, enc, newenc;
  void *rec;

  while(!feof(f)) {
    GET_LOG_GINT_CMD(db, f, cmd)
    switch(cmd) {
      case WG_JOURNAL_ENTRY_CRE:
        GET_LOG_GINT(db, f, length)
        GET_LOG_GINT(db, f, offset)
        rec = wg_create_record(db, length);
        if(offset != 0) {
          /* XXX: should we have even tried if this failed earlier? */
          if(!rec) {
            return show_log_error(db, "Failed to create a new record");
          }
          newoffset = ptrtooffset(db, rec);
          if(newoffset != offset) {
            if(add_tran_offset(table, sz, offset, newoffset)) {
              return show_log_error(db, "Failed to parse log "\
                "(out of translation memory)");
            }
          }
        }
        break;
      case WG_JOURNAL_ENTRY_DEL:
        GET_LOG_GINT(db, f, offset)
        newoffset = translate_offset(table, sz, offset);
        rec = offsettoptr(db, newoffset);
        if(wg_delete_record(db, rec) < -1) {
          return show_log_error(db, "Failed to delete a record");
        }
        break;
      case WG_JOURNAL_ENTRY_ENC:
        GET_LOG_GINT(db, f, type)
        newenc = recover_encode(db, f, type);
        GET_LOG_GINT(db, f, enc)
        if(enc != WG_ILLEGAL) {
          /* Encode was supposed to succeed */
          if(newenc == WG_ILLEGAL) {
            return -1;
          }
          if(newenc != enc) {
            if(add_tran_enc(table, sz, enc, newenc)) {
              return show_log_error(db, "Failed to parse log "\
                "(out of translation memory)");
            }
          }
        }
        break;
      case WG_JOURNAL_ENTRY_SET:
        GET_LOG_GINT(db, f, offset)
        GET_LOG_GINT(db, f, col)
        GET_LOG_GINT(db, f, enc)
        newoffset = translate_offset(table, sz, offset);
        rec = offsettoptr(db, newoffset);
        newenc = translate_encoded(table, sz, enc);
        if(wg_set_field(db, rec, col, newenc)) {
          return show_log_error(db, "Failed to set field data");
        }
        break;
      default:
        return show_log_error(db, "Invalid log entry");
    }
  }
  return 0;
}
#endif /* USE_DBLOG */

/** Set up the logging area in the database handle
 *  Normally called when opening the database connection.
 */
gint wg_init_handle_logdata(void *db) {
#ifdef USE_DBLOG
  db_handle_logdata **ld = \
    (db_handle_logdata **) &(((db_handle *) db)->logdata);
  *ld = malloc(sizeof(db_handle_logdata));
  if(!(*ld)) {
    return show_log_error(db, "Error initializing local log data");
  }
  memset(*ld, 0, sizeof(db_handle_logdata));
#endif
  return 0;
}

/** Clean up the state of logging in the database handle.
 *  Normally called when closing the database connection.
 */
void wg_cleanup_handle_logdata(void *db) {
#ifdef USE_DBLOG
  db_handle_logdata *ld = \
    (db_handle_logdata *) (((db_handle *) db)->logdata);
  if(ld) {
    if(ld->f) {
      fclose(ld->f);
      ld->f = NULL;
    }
    free(ld);
    ((db_handle *) db)->logdata = NULL;
  }
#endif
}

/** Activate logging
 *
 * When successful, does the following:
 *   opens the logfile and initializes it;
 *   sets the logging active flag.
 *
 * Security concerns:
 *   - the log file name is compiled in (so we can't trick other
 *   processes into writing over files they're not supposed to)
 *   - the log file has a magic header (see above, avoid accidentally
 *   destroying files)
 *   - the process that initialized logging needs to have write
 *   access to the log file.
 *
 * Returns 0 on success
 * Returns -1 when logging is already active
 * Returns -2 when the function failed and logging is not active
 * Returns -3 when additionally, the log file was possibly destroyed
 */
gint wg_start_logging(void *db)
{
#ifdef USE_DBLOG
  db_memsegment_header* dbh = dbmemsegh(db);
#ifndef _WIN32
  FILE *f;
#endif

  if(dbh->logging.active) {
    show_log_error(db, "Logging is already active");
    return -1;
  }

#ifndef _WIN32
  if(!(f = fopen(WG_JOURNAL_FILENAME, "ab+"))) {
    show_log_error(db, "Error opening log file");
    return -2;
  }

#ifdef USE_FCNTL
  if(lock_journal(fileno(f), 0, 1)) { /* exclusive access */
    show_log_error(db, "Error locking log file");
    fclose(f);
    return -2;
  }
#endif

  if(!dbh->logging.dirty) {
    /* logfile is clean, re-initialize */
    /* XXX: should maybe back up the existing logfile (otherwise we need
     * script wrappers to handle crash recovery).
     */
    /* fseek(f, 0, SEEK_SET); */
    ftruncate(fileno(f), 0);
    if(fwrite(WG_JOURNAL_MAGIC, WG_JOURNAL_MAGIC_BYTES, 1, f) != 1) {
      show_log_error(db, "Error initializing log file");
      JOURNAL_FAIL(f, -3)
    }
  } else {
    /* check the magic header */
    if(check_journal(db, f)) {
      JOURNAL_FAIL(f, -2)
    }
    /* fseek(f, 0, SEEK_END); */
  }

#ifdef USE_FCNTL
  if(unlock_journal(fileno(f))) {
    show_log_error(db, "Error unlocking log file");
    fclose(f);
    return -2; /* unclear state: file locked, can't do anything about it? */
  }
#endif

  /* Keep using this handle */
  ((db_handle_logdata *) (((db_handle *) db)->logdata))->f = f;
  
  dbh->logging.active = 1;
#else
#endif /* _WIN32 */
  return 0;
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}

/** Turn journal logging off.
 *
 * Returns 0 on success
 * Returns non-zero on failure
 */
gint wg_stop_logging(void *db)
{
#ifdef USE_DBLOG
  db_memsegment_header* dbh = dbmemsegh(db);

  if(!dbh->logging.active) {
    show_log_error(db, "Logging is not active");
    return -1;
  }

  dbh->logging.active = 0; /* XXX: do we need to set anything else? */
  return 0;
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}


/** Replay journal file.
 *
 * Requires exclusive access to the database.
 * Marks the log as clean, but does not re-initialize the file.
 *
 * Returns 0 on success
 * Returns -1 on non-fatal error (database unmodified)
 * Returns -2 on fatal error (database inconsistent)
 */
#define TRAN_TABLE_SZ 14 /* ~80k entries
                          * XXX: replace with a flexible, faster hash table */

gint wg_replay_log(void *db, char *filename)
{
#ifdef USE_DBLOG
  db_memsegment_header* dbh = dbmemsegh(db);
  gint active, err = 0;
  tran_table_meta *tran_tbl;
#ifndef _WIN32
  FILE *f;
#endif

#ifndef _WIN32
  if(!(f = fopen(filename, "rb"))) {
    show_log_error(db, "Error opening log file");
    return -1;
  }

#ifdef USE_FCNTL
  if(lock_journal(fileno(f), 0, 0)) { /* shared access */
    show_log_error(db, "Error locking log file");
    err = -1;
    goto abort2:
  }
#endif

  if(check_journal(db, f)) {
    err = -1;
    goto abort1;
  }

  active = dbh->logging.active;
  dbh->logging.active = 0; /* turn logging off before restoring */

  /* restore the log contents */
  tran_tbl = create_recover_tran_table(TRAN_TABLE_SZ);
  if(!tran_tbl) {
    show_log_error(db, "Failed to create log translation table");
    err = -1;
    goto abort1;
  }
  if(recover_journal(db, f, tran_tbl, TRAN_TABLE_SZ)) {
    err = -2;
    goto abort0;
  }

  dbh->logging.dirty = 0; /* on success, set the log as clean. */

abort0:
  free_recover_tran_table(tran_tbl, TRAN_TABLE_SZ);

abort1:
#ifdef USE_FCNTL
  if(unlock_journal(fileno(f))) {
    show_log_error(db, "Error unlocking log file");
    err = -1; /* this should probably be non-fatal under normal usage */
  }
abort2:
#endif

  fclose(f);

  if(!err && active) {
    if(wg_start_logging(db)) {
      show_log_error(db, "Log restored but failed to reactivate logging");
      err = -2;
    }
  }
  
#else
#endif /* _WIN32 */
  return err;
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}

#ifdef USE_DBLOG
/** Write a byte buffer to the log file.
 *
 */
static gint write_log_buffer(void *db, void *buf, int buflen)
{
  db_memsegment_header* dbh = dbmemsegh(db);
  db_handle_logdata *ld = \
    (db_handle_logdata *) (((db_handle *) db)->logdata);

#ifndef _WIN32
  if(!ld->f) {
    FILE *f;
    if(!(f = fopen(WG_JOURNAL_FILENAME, "ab+"))) {
      show_log_error(db, "Error opening log file");
    } else {
      if(check_journal(db, f)) {
        fclose(f);
      } else {
        /* fseek(f, 0, SEEK_END); */
        ld->f = f;
      }
    }
  }
  if(!ld->f)
    return -1;  

#ifdef USE_FCNTL
  if(lock_journal(fileno(ld->f), 0, 1)) { /* exclusive access */
    show_log_error(db, "Error locking log file");
    fclose(ld->f);
    return -11;
  }
#endif

  /* Always mark log as dirty when writing something */
  dbh->logging.dirty = 1;
  
  if(fwrite((char *) buf, 1, buflen, ld->f) != buflen) {
    show_log_error(db, "Error writing to log file");
    JOURNAL_FAIL(ld->f, -5)
  }

#ifdef USE_FCNTL
  if(unlock_journal(fileno(ld->f))) {
    show_log_error(db, "Error unlocking log file");
    fclose(ld->f);
    return -12;
  }
#endif

#else
#endif /* _WIN32 */
  return 0;
}
#endif /* USE_DBLOG */

/*
 * Operations (and data) logged:
 *
 * WG_JOURNAL_ENTRY_CRE - create a record (length)
 *   followed by a single gint field that contains the newly allocated offset
 * WG_JOURNAL_ENTRY_DEL - delete a record (offset)
 * WG_JOURNAL_ENTRY_ENC - encode a value (data bytes, extdata if applicable)
 *   followed by a single gint field that contains the encoded value
 * WG_JOURNAL_ENTRY_SET - set a field value (record offset, column, encoded value)
 *
 */

/** Log the creation of a record.
 *  This call should always be followed by wg_log_encval()
 *
 *  We assume that dbh->logging.active flag is checked before calling this.
 */
gint wg_log_create_record(void *db, gint length)
{
#ifdef USE_DBLOG
  gint buf[2];
  buf[0] = WG_JOURNAL_ENTRY_CRE;
  buf[1] = length;
  return write_log_buffer(db, (void *) &buf, sizeof(gint) * 2);
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}

/** Log the deletion of a record.
 *
 */
gint wg_log_delete_record(void *db, gint enc)
{
#ifdef USE_DBLOG
  gint buf[2];
  buf[0] = WG_JOURNAL_ENTRY_DEL;
  buf[1] = enc;
  return write_log_buffer(db, (void *) &buf, sizeof(gint) * 2);
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}

/** Log the result of an encode operation. Also handles records.
 *
 *  If the encode function or record creation failed, call this
 *  with WG_ILLEGAL to indicate the failure of the operation.
 */
gint wg_log_encval(void *db, gint enc)
{
#ifdef USE_DBLOG
  return write_log_buffer(db, (void *) &enc, sizeof(gint));
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}

/** Log an encode operation.
 *
 * This is the most expensive log operation as we need to write the
 * chunk of data to be encoded.
 */
gint wg_log_encode(void *db, gint type, void *data, gint length,
  void *extdata, gint extlength)
{
#ifdef USE_DBLOG
  char *buf, *optr, *oend, *iptr;
  int buflen = 0, err;

  switch(type) {
    case WG_NULLTYPE:
    case WG_RECORDTYPE:
    case WG_CHARTYPE:
    case WG_DATETYPE:
    case WG_TIMETYPE:
    case WG_VARTYPE:
    case WG_FIXPOINTTYPE:
      /* Shared memory not altered, don't log */
      return 0;
      break;
    case WG_INTTYPE:
      /* int argument */
      if(fits_smallint(*((int *) data))) {
        return 0; /* self-contained, don't log */
      } else {
        buflen = sizeof(gint) * 2 + sizeof(int);
        buf = (char *) malloc(buflen + 1);
        optr = buf + 2*sizeof(gint);
        *((int *) optr) = *((int *) data);
      }
      break;
    case WG_DOUBLETYPE:
      /* double precision argument */
      buflen = sizeof(gint) * 2 + sizeof(double);
      buf = (char *) malloc(buflen + 1);
      optr = buf + 2*sizeof(gint);
      *((double *) optr) = *((double *) data);
      break;
    case WG_STRTYPE:
    case WG_URITYPE:
    case WG_XMLLITERALTYPE:
    case WG_ANONCONSTTYPE:
    case WG_BLOBTYPE: /* XXX: no encode func for this yet */
      /* strings with extdata */
      buflen = sizeof(gint) * 4 + length + extlength;
      buf = (char *) malloc(buflen + 1);

      /* data and extdata length */
      optr = buf + 2*sizeof(gint);
      *((gint *) optr) = length;
      optr += sizeof(gint);
      *((gint *) optr) = extlength;
      optr += sizeof(gint);

      /* data */
      oend = optr + length;
      iptr = (char *) data;
      while(optr < oend) *(optr++) = *(iptr++);

      /* extdata */
      oend = optr + extlength;
      iptr = (char *) extdata;
      while(optr < oend) *(optr++) = *(iptr++);
      break;
      break;
    default:
      return show_log_error(db, "Unsupported data type");
  }

  /* Add a fixed prefix and terminate */
  ((gint *) buf)[0] = WG_JOURNAL_ENTRY_ENC;
  ((gint *) buf)[1] = type;
  buf[buflen] = '\0';

  err = write_log_buffer(db, (void *) buf, buflen);
  free(buf);
  return err;
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}

/** Log setting a data field.
 *
 *  We assume that dbh->logging.active flag is checked before calling this.
 */
gint wg_log_set_field(void *db, void *rec, gint col, gint data)
{
#ifdef USE_DBLOG
  gint buf[4];
  buf[0] = WG_JOURNAL_ENTRY_SET;
  buf[1] = ptrtooffset(db, rec);
  buf[2] = col;
  buf[3] = data;
  return write_log_buffer(db, (void *) &buf, sizeof(gint) * 4);
#else
  return show_log_error(db, "Logging is disabled");
#endif /* USE_DBLOG */
}


#if 0
/* Outdated code, fix or remove */
gint wg_dump_log(void *db,char fileName[])
{
    #ifdef _WIN32
    db_memsegment_header* dbh = dbmemsegh(db);
    //if file not open, open it and leave it open
    if(dbh->logging.fileopen==0)
    {
        void *hviewfile;
        HANDLE hmapfile,hfile;

        hfile = CreateFile(fileName,       // lpFileName
                    GENERIC_READ | GENERIC_WRITE , // dwDesiredAccess
                    FILE_SHARE_READ,              // dwShareMode
                    NULL,           // lpSecurityAttributes
                    CREATE_ALWAYS,  // dwCreationDisposition
                    FILE_ATTRIBUTE_NORMAL, // dwFlagsAndAttributes
                    NULL            // hTemplateFile
                  );
        
        hmapfile = CreateFileMapping(
                     hfile,    // use paging file
                     NULL,                    // default security 
                     PAGE_READWRITE,          // read/write access
                     0,                       // max. object size 
                     DEFAULT_MEMDBASE_SIZE,                   // buffer size  
                     NULL);                 // name of mapping object

        if(hmapfile==NULL)
        {
            printf("Error opening file mapping\n");
            CloseHandle(hmapfile);
        }
        else
        {
            hviewfile = (void*)MapViewOfFile( hmapfile,FILE_MAP_ALL_ACCESS,0,0,0);
            if(hviewfile==NULL)
            {
                printf("mapviewopenERR\n");
                UnmapViewOfFile(hviewfile);
            }
            else
            {

                CopyMemory(hviewfile,offsettoptr(db,dbh->logging.firstoffset),INITIAL_SUBAREA_SIZE);
                //no need for manual flushing, let windows do it's job, just in case
               /* if(!FlushViewOfFile (hviewfile,0))
                    printf("viewERR\n");
                else
                    printf("viewOK\n");
                
                if(!FlushFileBuffers(hfile))
                    printf("filebufferERR\n");
                else
                    printf("filebufferOK\n");*/
            }
        }
        //CloseHandle(hfile); //we dont need to close it... keep it open for further writing
        dbh->logging.fileopen=1;
        dbh->logging.filepointer=hviewfile;
    }
    //file already open, just copy and leave other for windows
    else
    {
        CopyMemory(dbh->logging.filepointer,offsettoptr(db,dbh->logging.firstoffset),INITIAL_SUBAREA_SIZE);
    }
    
    return 1;
    #else
    return 0;
    #endif
    
}

gint wg_import_log(void * db,char fileName[])
{
    db_memsegment_header* dbh = dbmemsegh(db);
    gint i=0;
    gint read;
    gint data;
    gint datatype;
    void * record;
    gint fieldnr;
    
    #ifdef _WIN32
    void *hviewfile;
    HANDLE hmapfile,hfile;
    
    printf("logoffset pointer4: %d\n",dbh->logging.logoffset);
    
    
    hfile = CreateFile(fileName,       // lpFileName
                GENERIC_READ | GENERIC_WRITE , // dwDesiredAccess
                FILE_SHARE_READ,              // dwShareMode
                NULL,           // lpSecurityAttributes
                OPEN_EXISTING,  // dwCreationDisposition
                FILE_ATTRIBUTE_NORMAL, // dwFlagsAndAttributes
                NULL            // hTemplateFile
              );
    
    if(GetLastError()==2)
        printf("File not found\n");
    else
    {
        printf("File exists, size: %d\n",GetFileSize(hfile,0));
        
        hmapfile = CreateFileMapping(
                 hfile,    // use paging file
                 NULL,                    // default security 
                 PAGE_READWRITE,          // read/write access
                 0,                       // max. object size 
                 DEFAULT_MEMDBASE_SIZE,   // buffer size  
                 NULL);                 // name of mapping object

        if(hmapfile==NULL)
        {
            printf("Error opening file mapping\n");
            CloseHandle(hmapfile);
        }
        else
        {
            hviewfile = (void*)MapViewOfFile(hmapfile,FILE_MAP_ALL_ACCESS,0,0,0);
            if(hviewfile==NULL)
            {
                printf("mapviewopenERR\n");
                
            }
            else
            {
                printf("copy memory\n");
                CopyMemory(offsettoptr(db,dbh->logging.firstoffset),hviewfile,INITIAL_SUBAREA_SIZE);
            }
        }
        CloseHandle(hfile);
        UnmapViewOfFile(hviewfile);
    }
    printf("returnlogimport\n");
    
    #else

    #endif
    
    //do not allow to write log if recovering
    dbh->logging.writelog=0;
    
    printf("LOG OUTPUT, start %d\n", (int) dbh->logging.firstoffset);
    while(1)
    {
        //read length (or record type)
        read=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i);
        if(read==WG_MAGIC_RECORD) //get record data (always 4 bytes)
        {
            printf("record2: %d, offset: %d, recordsize: %d\n",
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*2),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3));
            wg_create_record(db,dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3));
            i+=4;   
        }
        else if(read>2) //get int data (always 4 bytes)
        {
            printf("\tdata2: %d, record: %d, fieldnr: %d\n",
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3),
              (int) decode_fullint_offset(dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint))),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+2*sizeof(gint)));
            record=offsettoptr(db,decode_fullint_offset(dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint))));
            fieldnr=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+2*sizeof(gint));
            data=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3);
            //datatype encoded in record
            datatype=wg_get_encoded_type(db,dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)));
            if(datatype==WG_INTTYPE)
                wg_set_int_field(db,record,fieldnr,data);
            i+=read;
        }
        else 
            break;
    }
    //enable logging
    dbh->logging.writelog=1;
    return 1;
}
#endif

/* ------------ error handling ---------------- */

static gint show_log_error(void *db, char *errmsg) {
  fprintf(stderr,"wg log error: %s.\n", errmsg);
  return -1;
}

#ifdef __cplusplus
}
#endif
