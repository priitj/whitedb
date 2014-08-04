/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
* Copyright (c) Priit Järv 2013,2014
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

 /** @file dblog.h
 * Public headers for the recovery journal.
 */

#ifndef DEFINED_DBLOG_H
#define DEFINED_DBLOG_H

#ifndef _WIN32
#define WG_JOURNAL_FILENAME DBLOG_DIR "/wgdb.journal"
#else
#define WG_JOURNAL_FILENAME DBLOG_DIR "\\wgdb_journal"
#endif
#define WG_JOURNAL_FN_BUFSIZE (sizeof(WG_JOURNAL_FILENAME) + 20)
#define WG_JOURNAL_MAX_BACKUPS 10
#define WG_JOURNAL_MAGIC "wgdb"
#define WG_JOURNAL_MAGIC_BYTES 4

#define WG_JOURNAL_ENTRY_ENC ((unsigned char) 0) /* top bits clear |= type */
#define WG_JOURNAL_ENTRY_CRE ((unsigned char) 0x40)
#define WG_JOURNAL_ENTRY_DEL ((unsigned char) 0x80)
#define WG_JOURNAL_ENTRY_SET ((unsigned char) 0xc0)
#define WG_JOURNAL_ENTRY_META ((unsigned char) 0x20)
#define WG_JOURNAL_ENTRY_CMDMASK (0xe0)
#define WG_JOURNAL_ENTRY_TYPEMASK (0x1f)


/* ====== data structures ======== */

typedef struct {
  FILE *f;
  int fd;
  gint serial;
  int umask;
} db_handle_logdata;

/* ==== Protos ==== */

gint wg_init_handle_logdata(void *db);
void wg_cleanup_handle_logdata(void *db);
int wg_log_umask(void *db, int cmask);

void wg_journal_filename(void *db, char *buf, size_t buflen);
gint wg_start_logging(void *db);
gint wg_stop_logging(void *db);
gint wg_replay_log(void *db, char *filename);

gint wg_log_create_record(void *db, gint length);
gint wg_log_delete_record(void *db, gint enc);
gint wg_log_encval(void *db, gint enc);
gint wg_log_encode(void *db, gint type, void *data, gint length,
  void *extdata, gint extlength);
gint wg_log_set_field(void *db, void *rec, gint col, gint data);
gint wg_log_set_meta(void *db, void *rec, gint meta);

#endif /* DEFINED_DBLOG_H */
