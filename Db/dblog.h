/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
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

 /** @file dbdump.h
 * Public headers for memory dumping to the disk.
 */

#ifndef __defined_dblog_h
#define __defined_dblog_h

#define WG_MAGIC_RECORD 2

/* ====== data structures ======== */

gint wg_log_record(void * db,wg_int record,wg_int length);
gint wg_get_log_offset(void * db);
gint wg_log_int(void * db, void* record,wg_int fieldnr,gint data);
gint wg_print_log(void * db);
gint wg_dump_log(void * db,char fileName[]);
gint wg_import_log(void * db,char fileName[]);

/* ==== Protos ==== */


#endif /* __defined_dbdump_h */
