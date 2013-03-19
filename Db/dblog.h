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

 /** @file dblog.h
 * Public headers for the recovery journal.
 */

#ifndef __defined_dblog_h
#define __defined_dblog_h

#define WG_JOURNAL_FILENAME "/tmp/wgdb.journal"
#define WG_JOURNAL_MAGIC "wgdb"
#define WG_JOURNAL_MAGIC_BYTES 4

#define WG_JOURNAL_ENTRY_ENC ((gint) 0x1)
#define WG_JOURNAL_ENTRY_CRE ((gint) 0x2)
#define WG_JOURNAL_ENTRY_DEL ((gint) 0x4)
#define WG_JOURNAL_ENTRY_SET ((gint) 0x8)

/*#define WG_MAGIC_RECORD 2 */

/* ====== data structures ======== */

typedef struct {
  FILE *f;
} db_handle_logdata;

/* ==== Protos ==== */

gint wg_init_handle_logdata(void *db);
void wg_cleanup_handle_logdata(void *db);

gint wg_start_logging(void *db);
gint wg_stop_logging(void *db);
gint wg_replay_log(void *db, char *filename);

gint wg_log_create_record(void *db, gint length);
gint wg_log_delete_record(void *db, gint enc);
gint wg_log_encval(void *db, gint enc);
gint wg_log_encode(void *db, gint type, void *data, gint length,
  void *extdata, gint extlength);
gint wg_log_set_field(void *db, void *rec, gint col, gint data);

#endif /* __defined_dblog_h */
