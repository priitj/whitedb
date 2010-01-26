/*
* $Id:  $
* $Version: $
*
* Copyright (c) Enar Reilent 2009, Priit Järv 2010
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

 /** @file dblock.h
 * Public headers for indexing routines
 */

#ifndef __defined_dbindex_h
#define __defined_dbindex_h

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* For gint data type */
#include "dbdata.h"

/* ====== data structures ======== */

/** structure of t-node
*   (array of data pointers, pointers to parent/children nodes, control data)
*   overall size is currently 64 bytes (cache line?) if array size is 10
*/
struct wg_tnode{
  gint parent_offset;
  unsigned char left_subtree_height;
  unsigned char right_subtree_height;
  gint current_max;
  gint current_min;
  short number_of_elements;
  gint array_of_values[WG_TNODE_ARRAY_SIZE];
  gint left_child_offset;
  gint right_child_offset;
};

/* ==== Protos ==== */

/* API functions (copied in dbapi.h) */

/* WGandalf internal functions */

gint wg_create_ttree_index(void *db, gint column);
gint wg_column_to_index_id(void *db, gint column);
gint wg_search_ttree_index(void *db, gint index_id, gint key);
gint wg_add_new_row_into_index(void *db, gint index_id, void *rec);
gint wg_remove_key_from_index(void *db, gint index_id, void *rec);
int wg_log_tree(void *db, char *file, struct wg_tnode *node);

#endif /* __defined_dbindex_h */
