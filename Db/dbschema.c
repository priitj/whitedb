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

 /** @file dbschema.c
 * WhiteDB (semi-)structured data representation
 */

/* ====== Includes =============== */


/* ====== Private headers and defs ======== */

#ifdef __cplusplus
extern "C" {
#endif

#include "dbdata.h"

#define TRIPLE_SIZE 3
#define TRIPLE_OFFSET 0

/* ======== Data ========================= */

/* ======= Private protos ================ */


/* ====== Functions ============== */

void *wg_create_triple(void *db, gint subj, gint prop, gint ob) {
  void *rec = wg_create_record(db, TRIPLE_SIZE);
  if(rec) {
    if(wg_set_field(db, rec, TRIPLE_OFFSET, subj))
      return NULL;
    if(wg_set_field(db, rec, TRIPLE_OFFSET + 1, prop))
      return NULL;
    if(wg_set_field(db, rec, TRIPLE_OFFSET + 2, ob))
      return NULL;
  }
  return rec;
}

void *wg_create_array(void *db, gint size, gint isdocument) {
  void *rec = wg_create_raw_record(db, size);
  gint *meta;
  if(rec) {
    meta = ((gint *) rec + RECORD_META_POS);
    *meta |= RECORD_META_ARRAY;
    if(isdocument)
      *meta |= RECORD_META_DOC;
  }
  return rec;
}

void *wg_create_object(void *db, gint size, gint isdocument) {
  void *rec = wg_create_record(db, size);
  gint *meta;
  if(rec) {
    meta = ((gint *) rec + RECORD_META_POS);
    *meta |= RECORD_META_OBJECT;
    if(isdocument)
      *meta |= RECORD_META_DOC;
  }
  return rec;
}

#ifdef __cplusplus
}
#endif
