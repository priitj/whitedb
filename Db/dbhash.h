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

 /** @file dbhash.h
 * Public headers for hash-related procedures.
 */

#ifndef DEFINED_DBHASH_H
#define DEFINED_DBHASH_H

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"

/* ==== Public macros ==== */

#define HASHIDX_META_POS        1
#define HASHIDX_RECLIST_POS     2
#define HASHIDX_HASHCHAIN_POS   3
#define HASHIDX_HEADER_SIZE     4

/* ==== Protos ==== */

int wg_hash_typedstr(void* db, char* data, char* extrastr, gint type, gint length);
gint wg_find_strhash_bucket(void* db, char* data, char* extrastr, gint type, gint size, gint hashchain);
int wg_right_strhash_bucket
            (void* db, gint longstr, char* cstr, char* cextrastr, gint ctype, gint cstrsize);
gint wg_remove_from_strhash(void* db, gint longstr);

gint wg_decode_for_hashing(void *db, gint enc, char **decbytes);
gint wg_idxhash_store(void* db, db_hash_area_header *ha,
  char* data, gint length, gint offset);
gint wg_idxhash_remove(void* db, db_hash_area_header *ha,
  char* data, gint length, gint offset);
gint wg_idxhash_find(void* db, db_hash_area_header *ha,
  char* data, gint length);

void *wg_ginthash_init(void *db);
gint wg_ginthash_addkey(void *db, void *tbl, gint key, gint val);
gint wg_ginthash_getkey(void *db, void *tbl, gint key, gint *val);
void wg_ginthash_free(void *db, void *tbl);

void *wg_dhash_init(void *db, size_t entries);
void wg_dhash_free(void *db, void *tbl);
gint wg_dhash_addkey(void *db, void *tbl, gint key);
gint wg_dhash_haskey(void *db, void *tbl, gint key);

#endif /* DEFINED_DBHASH_H */
