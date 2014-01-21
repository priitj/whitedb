/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2013, 2014
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

#include <stdio.h>

/* ====== Private headers and defs ======== */

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "dbdata.h"
#include "dbcompare.h"
#include "dbindex.h"
#include "dbschema.h"
#include "dblog.h"

/* ======== Data ========================= */

/* ======= Private protos ================ */

#ifdef USE_BACKLINKING
static void *find_document_recursive(void *db, gint *rec, int depth);
#endif
static gint delete_record_recursive(void *db, void *rec, int depth);
static gint show_schema_error(void *db, char *errmsg);

/* ====== Functions ============== */

/*
 * Create a data triple (subj, prop, ob)
 * May also be called to create key-value pairs with (NULL, key, value)
 * if isparam is non-0, the data is not indexed.
 * returns the new record
 * returns NULL on error.
 */
void *wg_create_triple(void *db, gint subj, gint prop, gint ob, gint isparam) {
  void *rec = wg_create_raw_record(db, WG_SCHEMA_TRIPLE_SIZE);
  gint *meta;
  if(rec) {
    meta = ((gint *) rec + RECORD_META_POS);
    if(isparam) {
      *meta |= (RECORD_META_NOTDATA|RECORD_META_MATCH);
    } else if(wg_index_add_rec(db, rec) < -1) {
      return NULL; /* index error */
    }

    if(wg_set_field(db, rec, WG_SCHEMA_TRIPLE_OFFSET, subj))
      return NULL;
    if(wg_set_field(db, rec, WG_SCHEMA_TRIPLE_OFFSET + 1, prop))
      return NULL;
    if(wg_set_field(db, rec, WG_SCHEMA_TRIPLE_OFFSET + 2, ob))
      return NULL;
  }
  return rec;
}

/*
 * Create an empty (JSON) array of given size.
 * if isparam is non-0, the data is not indexed (incl. when updating later)
 * if isdocument is non-0, the record represents a top-level document
 * returns the new record
 * returns NULL on error.
 */
void *wg_create_array(void *db, gint size, gint isdocument, gint isparam) {
  void *rec = wg_create_raw_record(db, size);
  gint *metap, meta;
  if(rec) {
    metap = ((gint *) rec + RECORD_META_POS);
    meta = *metap; /* Temp variable used for write-ahead logging */
    meta |= RECORD_META_ARRAY;
    if(isdocument)
      meta |= RECORD_META_DOC;
    if(isparam)
      meta |= (RECORD_META_NOTDATA|RECORD_META_MATCH);

#ifdef USE_DBLOG
    if(dbmemsegh(db)->logging.active) {
      if(wg_log_set_meta(db, rec, meta))
        return NULL;
    }
#endif
    *metap = meta;
    if(!isparam) {
      if(wg_index_add_rec(db, rec) < -1) {
        return NULL; /* index error */
      }
    }
  }
  return rec;
}

/*
 * Create an empty (JSON) object of given size.
 * if isparam is non-0, the data is not indexed (incl. when updating later)
 * if isdocument is non-0, the record represents a top-level document
 * returns the new record
 * returns NULL on error.
 */
void *wg_create_object(void *db, gint size, gint isdocument, gint isparam) {
  void *rec = wg_create_raw_record(db, size);
  gint *metap, meta;
  if(rec) {
    metap = ((gint *) rec + RECORD_META_POS);
    meta = *metap;
    meta |= RECORD_META_OBJECT;
    if(isdocument)
      meta |= RECORD_META_DOC;
    if(isparam)
      meta |= (RECORD_META_NOTDATA|RECORD_META_MATCH);

#ifdef USE_DBLOG
    if(dbmemsegh(db)->logging.active) {
      if(wg_log_set_meta(db, rec, meta))
        return NULL;
    }
#endif
    *metap = meta;
    if(!isparam) {
      if(wg_index_add_rec(db, rec) < -1) {
        return NULL; /* index error */
      }
    }
  }
  return rec;
}

/*
 * Find a top-level document that the record belongs to.
 * returns the document pointer on success
 * returns NULL if the document was not found.
 */
void *wg_find_document(void *db, void *rec) {
#ifndef USE_BACKLINKING
  show_schema_error(db, "Backlinks are required to find complete documents");
  return NULL;
#else
  return find_document_recursive(db, (gint *) rec, WG_COMPARE_REC_DEPTH-1);
#endif
}


#ifdef USE_BACKLINKING
/*
 *  Find a document recursively.
 *  iterates through the backlink chain and checks each parent recursively.
 *  Returns the pointer to the (first) found document.
 *  Returns NULL if nothing found.
 *  XXX: if a document links to the contents of another document, it
 *  can "hijack" it in the search results this way. The priority
 *  depends on the position(s) in the backlink chain, as this is a depth-first
 *  search.
 */
static void *find_document_recursive(void *db, gint *rec, int depth) {
  if(is_schema_document(rec))
    return rec;

  if(depth > 0) {
    gint backlink_list = *(rec + RECORD_BACKLINKS_POS);
    if(backlink_list) {
      gcell *next = (gcell *) offsettoptr(db, backlink_list);
      for(;;) {
        void *res = find_document_recursive(db,
          (gint *) offsettoptr(db, next->car),
          depth-1);
        if(res)
          return res; /* Something was found recursively */
        if(!next->cdr)
          break;
        next = (gcell *) offsettoptr(db, next->cdr);
      }
    }
  }

  return NULL; /* Depth exhausted or nothing found. */
}
#endif

/*
 * Delete a top-level document
 * returns 0 on success
 * returns -1 on error
 */
gint wg_delete_document(void *db, void *document) {
#ifdef CHECK
  if(!is_schema_document(document)) {
    return show_schema_error(db, "wg_delete_document: not a document");
  }
#endif
#ifndef USE_BACKLINKING
  return delete_record_recursive(db, document, 99);
#else
  return delete_record_recursive(db, document, WG_COMPARE_REC_DEPTH);
#endif
}

/*
 * Delete a record and all the records it points to.
 * This is safe to call on JSON documents.
 */
static gint delete_record_recursive(void *db, void *rec, int depth) {
  gint i, reclen;
  if(depth <= 0) {
    return show_schema_error(db, "deleting record: recursion too deep");
  }

  reclen = wg_get_record_len(db, rec);
  for(i=0; i<reclen; i++) {
    gint enc = wg_get_field(db, rec, i);
    gint type = wg_get_encoded_type(db, enc);
    if(type == WG_RECORDTYPE) {
      if(wg_set_field(db, rec, i, 0))
        return -1;
      if(delete_record_recursive(db, wg_decode_record(db, enc), depth-1))
        return -1;
    }
  }

  if(wg_delete_record(db, rec))
    return -1;

  return 0;
}

/* ------------ error handling ---------------- */

static gint show_schema_error(void *db, char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg schema error: %s.\n", errmsg);
#endif
  return -1;
}

#ifdef __cplusplus
}
#endif
