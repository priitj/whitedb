/*
* $Id:  $
* $Version: $
*
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

 /** @file dbschema.h
 * Public headers for the strucured data functions.
 */

#ifndef DEFINED_DBSCHEMA_H
#define DEFINED_DBSCHEMA_H

/* ==== Public macros ==== */

#define WG_SCHEMA_TRIPLE_SIZE 3
#define WG_SCHEMA_TRIPLE_OFFSET 0
#define WG_SCHEMA_KEY_OFFSET (WG_SCHEMA_TRIPLE_OFFSET + 1)
#define WG_SCHEMA_VALUE_OFFSET (WG_SCHEMA_TRIPLE_OFFSET + 2)

/* ====== data structures ======== */


/* ==== Protos ==== */

void *wg_create_triple(void *db, gint subj, gint prop, gint ob, gint isparam);
#define wg_create_kvpair(db, key, val, ip) \
  wg_create_triple(db, 0, key, val, ip)
void *wg_create_array(void *db, gint size, gint isdocument, gint isparam);
void *wg_create_object(void *db, gint size, gint isdocument, gint isparam);
void *wg_find_document(void *db, void *rec);
gint wg_delete_document(void *db, void *document);

#endif /* DEFINED_DBSCHEMA_H */
