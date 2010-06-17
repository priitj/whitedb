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

/* ==== Public macros ==== */

#define REALLY_BOUNDING_NODE 0
#define DEAD_END_LEFT_NOT_BOUNDING 1
#define DEAD_END_RIGHT_NOT_BOUNDING 2

#ifdef TTREE_CHAINED_NODES
#define TNODE_SUCCESSOR(d, x) (x->succ_offset)
#define TNODE_PREDECESSOR(d, x) (x->pred_offset)
#else
#define TNODE_SUCCESSOR(d, x) (x->right_child_offset ? \
                    wg_ttree_find_lub_node(d, x->right_child_offset) : \
                    wg_ttree_find_leaf_successor(d, ptrtooffset(d, x)))
#define TNODE_PREDECESSOR(d, x) (x->left_child_offset ? \
                    wg_ttree_find_glb_node(d, x->left_child_offset) : \
                    wg_ttree_find_leaf_predecessor(d, ptrtooffset(d, x)))
#endif

/* Check if record matches index (takes pointer arguments) */
#ifndef USE_INDEX_TEMPLATE
#define MATCH_TEMPLATE(d, h, r) 1
#else
#define MATCH_TEMPLATE(d, h, r) (h->template_offset ? \
        wg_match_template(d, \
        (wg_index_template *) offsettoptr(d, h->template_offset), r) : 1)
#endif

#define WG_INDEX_TYPE_TTREE 50

/* ====== data structures ======== */

/** structure of t-node
*   (array of data pointers, pointers to parent/children nodes, control data)
*   overall size is currently 64 bytes (cache line?) if array size is 10,
*   with extra node chaining pointers the array size defaults to 8.
*/
struct wg_tnode{
  gint parent_offset;
  gint current_max;     /** encoded value */
  gint current_min;     /** encoded value */
  short number_of_elements;
  unsigned char left_subtree_height;
  unsigned char right_subtree_height;
  gint array_of_values[WG_TNODE_ARRAY_SIZE];
  gint left_child_offset;
  gint right_child_offset;
#ifdef TTREE_CHAINED_NODES
  gint succ_offset;     /** forward (smaller to larger) sequential chain */
  gint pred_offset;     /** backward sequential chain */
#endif
};

/* ==== Protos ==== */

/* API functions (copied in dbapi.h) */

/* WGandalf internal functions */

gint wg_search_ttree_index(void *db, gint index_id, gint key);

#ifndef TTREE_CHAINED_NODES
gint wg_ttree_find_glb_node(void *db, gint nodeoffset);
gint wg_ttree_find_lub_node(void *db, gint nodeoffset);
gint wg_ttree_find_leaf_predecessor(void *db, gint nodeoffset);
gint wg_ttree_find_leaf_successor(void *db, gint nodeoffset);
#endif
gint wg_search_ttree_rightmost(void *db, gint rootoffset,
  gint key, gint *result, struct wg_tnode *rb_node);
gint wg_search_ttree_leftmost(void *db, gint rootoffset,
  gint key, gint *result, struct wg_tnode *lb_node);
gint wg_search_tnode_first(void *db, gint nodeoffset, gint key,
  gint column);
gint wg_search_tnode_last(void *db, gint nodeoffset, gint key,
  gint column);

#ifdef USE_INDEX_TEMPLATE
gint wg_match_template(void *db, wg_index_template *tmpl, void *rec);
#endif
gint wg_create_index(void *db, gint column, gint type,
  gint *matchrec, gint reclen);
gint wg_drop_index(void *db, gint index_id);
gint wg_column_to_index_id(void *db, gint column, gint type,
  gint template_offset);

gint wg_index_add_field(void *db, void *rec, gint column);
gint wg_index_add_rec(void *db, void *rec);
gint wg_index_del_field(void *db, void *rec, gint column);
gint wg_index_del_rec(void *db, void *rec);


#endif /* __defined_dbindex_h */
