/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
* Copyright (c) Priit Järv 2013
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

 /** @file dblog.c
 *  DB logging support for WhiteDB memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>
#include <sys/stat.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <errno.h>
#include <malloc.h>
#include <io.h>
#include <share.h>
#else
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/errno.h>
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
#include "dbhash.h"

/* ====== Private headers and defs ======== */

#include "dblog.h"

#if defined(USE_DBLOG) && !defined(USE_DATABASE_HANDLE)
#error Logging requires USE_DATABASE_HANDLE
#endif

#ifdef _WIN32
#define snprintf(s, sz, f, ...) _snprintf_s(s, sz+1, sz, f, ## __VA_ARGS__)
#endif

#define JOURNAL_FAIL(f, e) \
  close(f); \
  return e;

#define GET_LOG_GINT(d, f, v) \
  if(read(f, (char *) &v, sizeof(gint)) != sizeof(gint)) { \
    return show_log_error(d, "Failed to read log entry"); \
  }

#define GET_LOG_GINT_CMD(d, f, v) \
  if(read(f, (char *) &v, sizeof(gint)) != sizeof(gint)) { \
    break; \
  }

#define GET_LOG_GINT_ERR(d, f, v, e) \
  if(read(f, (char *) &v, sizeof(gint)) != sizeof(gint)) { \
    show_log_error(d, "Failed to read log entry"); \
    return e; \
  }

/* ====== data structures ======== */

/* ======= Private protos ================ */

#ifdef USE_DBLOG
static int backup_journal(void *db, char *journal_fn);
static gint check_journal(void *db, int fd);
static int open_journal(void *db, int create);

static gint add_tran_offset(void *db, void *table, gint old, gint new);
static gint add_tran_enc(void *db, void *table, gint old, gint new);
static gint translate_offset(void *db, void *table, gint offset);
static gint translate_encoded(void *db, void *table, gint enc);
static gint recover_encode(void *db, int fd, gint type);
static gint recover_journal(void *db, int fd, void *table);

static gint write_log_buffer(void *db, void *buf, int buflen);
#endif /* USE_DBLOG */

static gint show_log_error(void *db, char *errmsg);

/* ====== Functions ============== */

#ifdef USE_DBLOG

/** Check the file magic of the journal file.
 *
 * Since the files are opened in append mode, we don't need to
 * seek before or after reading the header (on Linux).
 */
static gint check_journal(void *db, int fd) {
  char buf[WG_JOURNAL_MAGIC_BYTES + 1];
  if(read(fd, buf, WG_JOURNAL_MAGIC_BYTES) != WG_JOURNAL_MAGIC_BYTES) {
    return show_log_error(db, "Error checking log file");
  }
  buf[WG_JOURNAL_MAGIC_BYTES] = '\0';
  if(strncmp(buf, WG_JOURNAL_MAGIC, WG_JOURNAL_MAGIC_BYTES)) {
    return show_log_error(db, "Bad log file magic");
  }
  return 0;
}


/** Rename the existing journal.
 *
 * Uses a naming scheme of xxx.yy where xxx is the journal filename
 * and yy is a sequence number that is incremented.
 *
 * Returns 0 on success.
 * Returns -1 on failure.
 */
static int backup_journal(void *db, char *journal_fn) {
  int i, logidx, err;
  time_t oldest = 0;
  /* keep this buffer large enough to fit the backup counter length */
  char journal_backup[WG_JOURNAL_FN_BUFSIZE + 10];

  for(i=0, logidx=0; i<WG_JOURNAL_MAX_BACKUPS; i++) {
#ifndef _WIN32
    struct stat tmp;
#else
    struct _stat tmp;
#endif
    snprintf(journal_backup, WG_JOURNAL_FN_BUFSIZE + 10, "%s.%d",
      journal_fn, i);
#ifndef _WIN32
    if(stat(journal_backup, &tmp) == -1) {
#else
    if(_stat(journal_backup, &tmp) == -1) {
#endif
      if(errno == ENOENT) {
        logidx = i;
        break;
      }
    } else if(!oldest || oldest > tmp.st_mtime) {
      oldest = tmp.st_mtime;
      logidx = i;
    }
  }

  /* at this point, logidx points to either an available backup
   * filename or the oldest existing backup (which will be overwritten).
   * If all else fails, filename xxx.0 is used.
   */
  snprintf(journal_backup, WG_JOURNAL_FN_BUFSIZE + 10, "%s.%d",
    journal_fn, logidx);
#ifdef _WIN32
  _unlink(journal_backup);
#endif
  err = rename(journal_fn, journal_backup);
  if(!err) {
    db_memsegment_header* dbh = dbmemsegh(db);
    dbh->logging.serial++; /* new journal file */
  }
  return err;
}


/** Open the journal file.
 *
 * In create mode, we also take care of the backup copy.
 */
static int open_journal(void *db, int create) {
  char journal_fn[WG_JOURNAL_FN_BUFSIZE];
  db_memsegment_header* dbh = dbmemsegh(db);
  int addflags = 0;
  int fd = -1;
#ifndef _WIN32
  mode_t savemask;
#endif

#ifndef _WIN32
  snprintf(journal_fn, WG_JOURNAL_FN_BUFSIZE, "%s.%td",
    WG_JOURNAL_FILENAME, dbh->key);
#else
  snprintf(journal_fn, WG_JOURNAL_FN_BUFSIZE, "%s.%Id",
    WG_JOURNAL_FILENAME, dbh->key);
#endif

  if(create) {
#ifndef _WIN32
    struct stat tmp;
    savemask = umask(WG_JOURNAL_UMASK);
#else
    struct _stat tmp;
#endif
    addflags |= O_CREAT;
#ifndef _WIN32
    if(!dbh->logging.dirty && !stat(journal_fn, &tmp)) {
#else
    if(!dbh->logging.dirty && !_stat(journal_fn, &tmp)) {
#endif
      if(backup_journal(db, journal_fn)) {
        show_log_error(db, "Failed to back up the existing journal.");
        goto abort;
      }
    }
  }

  if((fd = open(journal_fn, addflags|O_APPEND|O_RDWR,
    S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)) == -1) {
    show_log_error(db, "Error opening log file");
  }

abort:
  if(create) {
#ifndef _WIN32
    umask(savemask);
#endif
  }
  return fd;
}


/** Add a log recovery translation entry
 *  Uses extendible gint hashtable internally.
 */
static gint add_tran_offset(void *db, void *table, gint old, gint new)
{
  return wg_ginthash_addkey(db, table, old, new);
}

/** Wrapper around add_tran_offset() to handle encoded data
 *
 */
static gint add_tran_enc(void *db, void *table, gint old, gint new)
{
  if(isptr(old)) {
    gint offset, newoffset;
    switch(old & NORMALPTRMASK) {
      case LONGSTRBITS:
        offset = decode_longstr_offset(old);
        newoffset = decode_longstr_offset(new);
        return add_tran_offset(db, table, offset, newoffset);
      case SHORTSTRBITS:
        offset = decode_shortstr_offset(old);
        newoffset = decode_shortstr_offset(new);
        return add_tran_offset(db, table, offset, newoffset);
      case FULLDOUBLEBITS:
        offset = decode_fulldouble_offset(old);
        newoffset = decode_fulldouble_offset(new);
        return add_tran_offset(db, table, offset, newoffset);
      case FULLINTBITSV0:
      case FULLINTBITSV1:
        offset = decode_fullint_offset(old);
        newoffset = decode_fullint_offset(new);
        return add_tran_offset(db, table, offset, newoffset);
      default:
        return 0;
    }
  }
  return 0;
}

/** Translate a log offset
 *
 */
static gint translate_offset(void *db, void *table, gint offset)
{
  gint newoffset;
  if(wg_ginthash_getkey(db, table, offset, &newoffset))
    return offset;
  else
    return newoffset;
}

/** Wrapper around translate_offset() to handle encoded data
 *
 */
static gint translate_encoded(void *db, void *table, gint enc)
{
  if(isptr(enc)) {
    gint offset;
    switch(enc & NORMALPTRMASK) {
      case LONGSTRBITS:
        offset = decode_longstr_offset(enc);
        return encode_longstr_offset(translate_offset(db, table, offset));
      case SHORTSTRBITS:
        offset = decode_shortstr_offset(enc);
        return encode_shortstr_offset(translate_offset(db, table, offset));
      case FULLDOUBLEBITS:
        offset = decode_fulldouble_offset(enc);
        return encode_fulldouble_offset(translate_offset(db, table, offset));
      case FULLINTBITSV0:
      case FULLINTBITSV1:
        offset = decode_fullint_offset(enc);
        return encode_fullint_offset(translate_offset(db, table, offset));
      default:
        return enc;
    }
  }
  return enc;
}

/** Parse an encode entry from the log.
 *
 */
gint recover_encode(void *db, int fd, gint type)
{
  char *strbuf, *extbuf;
  gint length, extlength, enc;
  int intval;
  double doubleval;

  switch(type) {
    case WG_INTTYPE:
      if(read(fd, (char *) &intval, sizeof(int)) != sizeof(int)) {
        show_log_error(db, "Failed to read log entry");
        return WG_ILLEGAL;
      }
      return wg_encode_int(db, intval);
    case WG_DOUBLETYPE:
      if(read(fd, (char *) &doubleval, sizeof(double)) != sizeof(double)) {
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
      GET_LOG_GINT_ERR(db, fd, length, WG_ILLEGAL)
      GET_LOG_GINT_ERR(db, fd, extlength, WG_ILLEGAL)

      strbuf = (char *) malloc(length + 1);
      if(!strbuf) {
        show_log_error(db, "Failed to allocate buffers");
        return WG_ILLEGAL;
      }
      if(read(fd, strbuf, length) != length) {
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
        if(read(fd, extbuf, extlength) != extlength) {
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
static gint recover_journal(void *db, int fd, void *table)
{
  gint cmd;
  gint length, offset, newoffset;
  gint col, type, enc, newenc;
  void *rec;

  while(1) {
    GET_LOG_GINT_CMD(db, fd, cmd)
    switch(cmd) {
      case WG_JOURNAL_ENTRY_CRE:
        GET_LOG_GINT(db, fd, length)
        GET_LOG_GINT(db, fd, offset)
        rec = wg_create_record(db, length);
        if(offset != 0) {
          /* XXX: should we have even tried if this failed earlier? */
          if(!rec) {
            return show_log_error(db, "Failed to create a new record");
          }
          newoffset = ptrtooffset(db, rec);
          if(newoffset != offset) {
            if(add_tran_offset(db, table, offset, newoffset)) {
              return show_log_error(db, "Failed to parse log "\
                "(out of translation memory)");
            }
          }
        }
        break;
      case WG_JOURNAL_ENTRY_DEL:
        GET_LOG_GINT(db, fd, offset)
        newoffset = translate_offset(db, table, offset);
        rec = offsettoptr(db, newoffset);
        if(wg_delete_record(db, rec) < -1) {
          return show_log_error(db, "Failed to delete a record");
        }
        break;
      case WG_JOURNAL_ENTRY_ENC:
        GET_LOG_GINT(db, fd, type)
        newenc = recover_encode(db, fd, type);
        GET_LOG_GINT(db, fd, enc)
        if(enc != WG_ILLEGAL) {
          /* Encode was supposed to succeed */
          if(newenc == WG_ILLEGAL) {
            return -1;
          }
          if(newenc != enc) {
            if(add_tran_enc(db, table, enc, newenc)) {
              return show_log_error(db, "Failed to parse log "\
                "(out of translation memory)");
            }
          }
        }
        break;
      case WG_JOURNAL_ENTRY_SET:
        GET_LOG_GINT(db, fd, offset)
        GET_LOG_GINT(db, fd, col)
        GET_LOG_GINT(db, fd, enc)
        newoffset = translate_offset(db, table, offset);
        rec = offsettoptr(db, newoffset);
        newenc = translate_encoded(db, table, enc);
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
  (*ld)->fd = -1;
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
    if(ld->fd >= 0) {
      close(ld->fd);
      ld->fd = -1;
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
/*  db_handle_logdata *ld = ((db_handle *) db)->logdata;*/
  int fd;

  if(dbh->logging.active) {
    show_log_error(db, "Logging is already active");
    return -1;
  }

  if((fd = open_journal(db, 1)) == -1) {
    show_log_error(db, "Error opening log file");
    return -2;
  }

  if(!dbh->logging.dirty) {
    /* logfile is clean, re-initialize */
    /* fseek(f, 0, SEEK_SET); */
    ftruncate(fd, 0); /* XXX: this is a no-op with backups */
    if(write(fd, WG_JOURNAL_MAGIC, WG_JOURNAL_MAGIC_BYTES) != \
                                            WG_JOURNAL_MAGIC_BYTES) {
      show_log_error(db, "Error initializing log file");
      JOURNAL_FAIL(fd, -3)
    }
  } else {
    /* check the magic header */
    if(check_journal(db, fd)) {
      JOURNAL_FAIL(fd, -2)
    }
  }

#if 0
  /* Keep using this handle */
  ld->fd = fd;
  ld->serial = dbh->logging.serial;
#else
  close(fd);
#endif

  dbh->logging.active = 1;
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

  dbh->logging.active = 0;
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
gint wg_replay_log(void *db, char *filename)
{
#ifdef USE_DBLOG
  db_memsegment_header* dbh = dbmemsegh(db);
  gint active, err = 0;
  void *tran_tbl;
  int fd;

  if((fd = open(filename, O_RDONLY)) == -1) {
    show_log_error(db, "Error opening log file");
    return -1;
  }

  if(check_journal(db, fd)) {
    err = -1;
    goto abort1;
  }

  active = dbh->logging.active;
  dbh->logging.active = 0; /* turn logging off before restoring */

  /* restore the log contents */
  tran_tbl = wg_ginthash_init(db);
  if(!tran_tbl) {
    show_log_error(db, "Failed to create log translation table");
    err = -1;
    goto abort1;
  }
  if(recover_journal(db, fd, tran_tbl)) {
    err = -2;
    goto abort0;
  }

  dbh->logging.dirty = 0; /* on success, set the log as clean. */

abort0:
  wg_ginthash_free(db, tran_tbl);

abort1:
  close(fd);

  if(!err && active) {
    if(wg_start_logging(db)) {
      show_log_error(db, "Log restored but failed to reactivate logging");
      err = -2;
    }
  }

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

  if(ld->fd >= 0 && ld->serial != dbh->logging.serial) {
    /* Stale file descriptor, get a new one */
    close(ld->fd);
    ld->fd = -1;
  }
  if(ld->fd < 0) {
    int fd;
    if((fd = open_journal(db, 0)) == -1) {
      show_log_error(db, "Error opening log file");
    } else {
      if(check_journal(db, fd)) {
        close(fd);
      } else {
        /* fseek(f, 0, SEEK_END); */
        ld->fd = fd;
        ld->serial = dbh->logging.serial;
      }
    }
  }
  if(ld->fd < 0)
    return -1;

  /* Always mark log as dirty when writing something */
  dbh->logging.dirty = 1;

  if(write(ld->fd, (char *) buf, buflen) != buflen) {
    show_log_error(db, "Error writing to log file");
    JOURNAL_FAIL(ld->fd, -5)
  }

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

/* ------------ error handling ---------------- */

static gint show_log_error(void *db, char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else   
  fprintf(stderr,"wg log error: %s.\n", errmsg);
#endif  
  return -1;
}

#ifdef __cplusplus
}
#endif
