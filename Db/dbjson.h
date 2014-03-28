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

 /** @file dbjson.h
 * Public headers for JSON I/O.
 */

#ifndef DEFINED_DBJSON_H
#define DEFINED_DBJSON_H


/* ====== data structures ======== */


/* ==== Protos ==== */

gint wg_parse_json_file(void *db, char *filename);
gint wg_check_json(void *db, char *buf);
gint wg_parse_json_document(void *db, char *buf, void **document);
gint wg_parse_json_fragment(void *db, char *buf, void **document);
gint wg_parse_json_param(void *db, char *buf, void **document);
void wg_print_json_document(void *db, void *cb, void *cb_ctx, void *document);

#endif /* DEFINED_DBJSON_H */
