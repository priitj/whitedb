/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
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

 /** @file rdfapi.h
 *
 * RDF parsing API for WhiteDB, depends on libraptor.
 *
 */

#ifndef DEFINED_RDFAPI_H
#define DEFINED_RDFAPI_H

wg_int wg_import_raptor_file(void *db, wg_int pref_fields, wg_int suff_fields,
  wg_int (*callback) (void *, void *), char *filename);
wg_int wg_import_raptor_rdfxml_file(void *db, wg_int pref_fields,
  wg_int suff_fields, wg_int (*callback) (void *, void *), char *filename);
wg_int wg_rdfparse_default_callback(void *db, void *rec);
wg_int wg_export_raptor_file(void *db, wg_int pref_fields, char *filename,
  char *serializer);
wg_int wg_export_raptor_rdfxml_file(void *db, wg_int pref_fields,
  char *filename);

#endif /* DEFINED_RDFAPI_H */
