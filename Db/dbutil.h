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

 /** @file dbutil.h
 * Public headers for miscellaneous functions.
 */

#ifndef __defined_dbutil_h
#define __defined_dbutil_h

#ifdef HAVE_RAPTOR
#include <raptor.h>
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* ====== data structures ======== */

#ifdef HAVE_RAPTOR
struct wg_triple_handler_params {
  void *db;
  int pref_fields;  /** number of fields preceeding the triple */
  int suff_fields;  /** number of fields to reserve at the end */
  gint (*callback) (void *, void *);    /** function called after
                                         *the triple is stored */
  raptor_parser *rdf_parser;            /** parser object */
};
#endif

/* ==== Protos ==== */

/* API functions (copied in dbapi.h) */
void wg_print_db(void *db);
void wg_print_record(void *db, gint* rec);
void wg_snprint_value(void *db, gint enc, char *buf, int buflen);
gint wg_parse_and_encode(void *db, char *buf);
void wg_export_db_csv(void *db, char *filename);
gint wg_import_db_csv(void *db, char *filename);

/* Separate raptor API (copied in rdfapi.h) */
#ifdef HAVE_RAPTOR
gint wg_import_raptor_file(void *db, gint pref_fields, gint suff_fields,
  gint (*callback) (void *, void *), char *filename);
gint wg_import_raptor_xmlrdf_file(void *db, gint pref_fields, gint suff_fields,
  gint (*callback) (void *, void *), char *filename);
gint wg_rdfparse_default_callback(void *db, void *rec);
#endif

#endif /* __defined_dbutil_h */
