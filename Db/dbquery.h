/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
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

 /** @file dbcompare.h
 * Public headers for Wgandalf query engine.
 */

#ifndef __defined_dbquery_h
#define __defined_dbquery_h
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

/* ====== data structures ======== */

/** Query argument list object */
typedef struct {
  gint column;      /** column (field) number this argument applies to */
  gint cond;        /** condition (equal, less than, etc) */
  gint value;       /** encoded value */
} wg_query_arg;

/** Query object */
typedef struct {
  gint qtype;               /** Query type (T-tree, hash, full scan) */
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
} wg_query;

/* ==== Protos ==== */

wg_query *wg_make_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc);
void *wg_fetch(void *db, wg_query *query);
void wg_free_query(void *db, wg_query *query);

#endif /* __defined_dbquery_h */
