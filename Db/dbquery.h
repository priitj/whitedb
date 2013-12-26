/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010,2011,2013
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

 /** @file dbcompare.h
 * Public headers for WhiteDB query engine.
 */

#ifndef DEFINED_DBQUERY_H
#define DEFINED_DBQUERY_H
#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "dbdata.h"
#include "dbindex.h"

/* ==== Public macros ==== */

#define WG_COND_EQUAL       0x0001      /** = */
#define WG_COND_NOT_EQUAL   0x0002      /** != */
#define WG_COND_LESSTHAN    0x0004      /** < */
#define WG_COND_GREATER     0x0008      /** > */
#define WG_COND_LTEQUAL     0x0010      /** <= */
#define WG_COND_GTEQUAL     0x0020      /** >= */

#define WG_QTYPE_TTREE      0x01
#define WG_QTYPE_HASH       0x02
#define WG_QTYPE_SCAN       0x04
#define WG_QTYPE_PREFETCH   0x80

/* ====== data structures ======== */

/** Query argument list object */
typedef struct {
  gint column;      /** column (field) number this argument applies to */
  gint cond;        /** condition (equal, less than, etc) */
  gint value;       /** encoded value */
} wg_query_arg;

typedef struct {
  gint key;         /** encoded key */
  gint value;       /** encoded value */
} wg_json_query_arg;

/** Query object */
typedef struct {
  gint qtype;           /** Query type (T-tree, hash, full scan, prefetch) */
  /* Argument list based query is the only one supported at the moment. */
  wg_query_arg *arglist;    /** check each row in result set against these */
  gint argc;                /** number of elements in arglist */
  gint column;              /** index on this column used */
  /* Fields for T-tree query (XXX: some may be re-usable for
   * other types as well) */
  gint curr_offset;
  gint end_offset;
  gint curr_slot;
  gint end_slot;
  gint direction;
  /* Fields for full scan */
  gint curr_record;         /** offset of the current record */
  /* Fields for prefetch */
  void *mpool;              /** storage for row offsets */
  void *curr_page;          /** current page of results */
  gint curr_pidx;           /** current index on page */
  wg_uint res_count;          /** number of rows in results */
} wg_query;

/* ==== Protos ==== */

wg_query *wg_make_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc);
#define wg_make_prefetch_query wg_make_query
wg_query *wg_make_query_rc(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc, wg_uint rowlimit);
wg_query *wg_make_json_query(void *db, wg_json_query_arg *arglist, gint argc);
void *wg_fetch(void *db, wg_query *query);
void wg_free_query(void *db, wg_query *query);

gint wg_encode_query_param_null(void *db, char *data);
gint wg_encode_query_param_record(void *db, void *data);
gint wg_encode_query_param_char(void *db, char data);
gint wg_encode_query_param_fixpoint(void *db, double data);
gint wg_encode_query_param_date(void *db, int data);
gint wg_encode_query_param_time(void *db, int data);
gint wg_encode_query_param_var(void *db, gint data);
gint wg_encode_query_param_int(void *db, gint data);
gint wg_encode_query_param_double(void *db, double data);
gint wg_encode_query_param_str(void *db, char *data, char *lang);
gint wg_encode_query_param_xmlliteral(void *db, char *data, char *xsdtype);
gint wg_encode_query_param_uri(void *db, char *data, char *prefix);
gint wg_free_query_param(void* db, gint data);

void *wg_find_record(void *db, gint fieldnr, gint cond, gint data,
    void* lastrecord);
void *wg_find_record_null(void *db, gint fieldnr, gint cond, char *data,
    void* lastrecord);
void *wg_find_record_record(void *db, gint fieldnr, gint cond, void *data,
    void* lastrecord);
void *wg_find_record_char(void *db, gint fieldnr, gint cond, char data,
    void* lastrecord);
void *wg_find_record_fixpoint(void *db, gint fieldnr, gint cond, double data,
    void* lastrecord);
void *wg_find_record_date(void *db, gint fieldnr, gint cond, int data,
    void* lastrecord);
void *wg_find_record_time(void *db, gint fieldnr, gint cond, int data,
    void* lastrecord);
void *wg_find_record_var(void *db, gint fieldnr, gint cond, gint data,
    void* lastrecord);
void *wg_find_record_int(void *db, gint fieldnr, gint cond, int data,
    void* lastrecord);
void *wg_find_record_double(void *db, gint fieldnr, gint cond, double data,
    void* lastrecord);
void *wg_find_record_str(void *db, gint fieldnr, gint cond, char *data,
    void* lastrecord);
void *wg_find_record_xmlliteral(void *db, gint fieldnr, gint cond, char *data,
    char *xsdtype, void* lastrecord);
void *wg_find_record_uri(void *db, gint fieldnr, gint cond, char *data,
    char *prefix, void* lastrecord);

#endif /* DEFINED_DBQUERY_H */
