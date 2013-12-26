/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2011
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

 /** @file indexapi.h
 *
 * Index management API for WhiteDB.
 *
 * wg_int type is defined in dbapi.h
 */

#ifndef DEFINED_INDEXAPI_H
#define DEFINED_INDEXAPI_H

/* Public macros */

#define WG_INDEX_TYPE_TTREE 50
#define WG_INDEX_TYPE_TTREE_JSON    51
#define WG_INDEX_TYPE_HASH          60
#define WG_INDEX_TYPE_HASH_JSON     61

/* Public protos */

wg_int wg_create_index(void *db, wg_int column, wg_int type,
  wg_int *matchrec, wg_int reclen);
wg_int wg_create_multi_index(void *db, wg_int *columns, wg_int col_count,
  wg_int type, wg_int *matchrec, wg_int reclen);
wg_int wg_drop_index(void *db, wg_int index_id);
wg_int wg_column_to_index_id(void *db, wg_int column, wg_int type,
  wg_int *matchrec, wg_int reclen);
wg_int wg_multi_column_to_index_id(void *db, wg_int *columns,
  wg_int col_count, wg_int type, wg_int *matchrec, wg_int reclen);
wg_int wg_get_index_type(void *db, wg_int index_id);
void * wg_get_index_template(void *db, wg_int index_id, wg_int *reclen);
void * wg_get_all_indexes(void *db, wg_int *count);

#endif /* DEFINED_INDEXAPI_H */
