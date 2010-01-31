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

 /** @file dbquery.c
 * Wgandalf query engine.
 */

/* ====== Includes =============== */

/* ====== Private headers and defs ======== */

#include <stdio.h>
#include <stdlib.h>

#include "dballoc.h"
#include "dbquery.h"
#include "dbcompare.h"

/* T-tree based scoring */
#define TTREE_SCORE_EQUAL 3
#define TTREE_SCORE_BOUND 1

/* ======= Private protos ================ */

static gint most_restricting_column(void *db,
  wg_query_arg *arglist, gint argc);

static gint show_query_error(void* db, char* errmsg);
/*static gint show_query_error_nr(void* db, char* errmsg, gint nr);*/


/* ====== Functions ============== */



/** Find most restricting column from query argument list
 *  This is probably a reasonable approach to optimize queries
 *  based on T-tree indexes, but might be difficult to combine
 *  with hash indexes.
 *  XXX: currently only considers the existence of T-tree
 *  index and nothing else.
 */
static gint most_restricting_column(void *db,
  wg_query_arg *arglist, gint argc) {

  struct column_score {
    gint column;
    int score;
  };
  struct column_score *sc;
  int i, j, mrc_score = -1;
  gint mrc;
  db_memsegment_header* dbh = (db_memsegment_header*) db;
  
  sc = (struct column_score *) malloc(argc * sizeof(struct column_score));
  if(!sc) {
    show_query_error(db, "Failed to allocate memory");
    return -1;
  }

  /* Scan through the arguments and calculate accumulated score
   * for each column. */
  for(i=0; i<argc; i++) {
    /* As a side effect, we're initializing the score array
     * in the same loop */
    sc[i].column = -1;
    sc[i].score = 0;

    /* Locate the slot for the column */
    for(j=0; j<argc; j++) {
      if(sc[j].column == -1) {
        sc[j].column = arglist[i].column;
        break;
      }
      if(sc[j].column == arglist[i].column) break;
    }
    
    /* Apply our primitive scoring */
    switch(arglist[i].cond) {
      case WG_COND_EQUAL:
        sc[j].score += TTREE_SCORE_EQUAL;
        break;
      case WG_COND_LESSTHAN:
      case WG_COND_GREATER:
      case WG_COND_LTEQUAL:
      case WG_COND_GTEQUAL:
        /* these all qualify as a bound. So two bounds
         * appearing in the argument list on the same column
         * score higher than one bound. */
        sc[j].score += TTREE_SCORE_BOUND;
        break;
      default:
        /* Note that we consider WG_COND_NOT_EQUAL near useless */
        break;
    }
  }

  /* Now loop over the scores to find the best. */
  for(i=0; i<argc; i++) {
    if(sc[i].column == -1) break;
    /* First of all, if there is no index on the column, the
     * score is reset to 0.
     *
     * XXX: type of the index should be checked here. If it's a hash
     * index and our score is based on <> bounds, the score for this
     * column will not be very meaningful.
     */
    if(sc[i].column >= MAX_INDEXED_FIELDNR ||\
      dbh->index_control_area_header.index_table[sc[i].column] == 0) {
      sc[i].score = 0; /* no index, score reset */
    }
    if(sc[i].score > mrc_score) {
      mrc_score = sc[i].score;
      mrc = sc[i].column;
    }
  }

  free(sc);
  return mrc;
}

/** Create a query object.
 *
 */
wg_query *wg_make_query(void *db, wg_query_arg *arglist, gint argc) {
  wg_query *query;
  gint col, index_id;
  
  if(!argc) {
    show_query_error(db, "Invalid number of arguments");
    return NULL;
  }

  query = (wg_query *) malloc(sizeof(wg_query));
  if(!query) {
    show_query_error(db, "Failed to allocate memory");
    return NULL;
  }

  /* Find the best (hopefully) index to base the query on.
   * Then initialise the query object to the first row in the
   * query result set.
   * XXX: only considering T-tree indexes now. */
  col = most_restricting_column(db, arglist, argc);

  index_id = wg_column_to_index_id(db, col, DB_INDEX_TYPE_1_TTREE);
  if(index_id > 0) {
    int i;
    int start_inclusive = 0, end_inclusive = 0;
    gint start_bound = WG_ILLEGAL; /* encoded values */
    gint end_bound = WG_ILLEGAL;
    wg_index_header *hdr;
    struct wg_tnode *node;

    query->qtype = WG_QTYPE_TTREE;
    query->column = col;
    query->curr_offset = 0;
    query->curr_slot = -1;
    query->end_offset = 0;
    query->end_slot = -1;
    query->direction = 1;

    /* Determine the bounds for the given column/index.
     *
     * Examples of using rightmost and leftmost bounds in T-tree queries:
     * val = 5  ==>
     *      find leftmost (A) and rightmost (B) nodes that contain value 5.
     *      Follow nodes sequentially from A until B is reached.
     * val > 1 & val < 7 ==>
     *      find rightmost node with value 1 (A). Find leftmost node with
     *      value 7 (B). Find the rightmost value in A that still equals 1.
     *      The value immediately to the right is the beginning of the result
     *      set and the value immediately to the left of the first occurrence
     *      of 7 in B is the end of the result set.
     * val > 1 & val <= 7 ==>
     *      A is the same as above. Find rightmost node with value 7 (B). The
     *      beginning of the result set is the same as above, the end is the
     *      last slot in B with value 7.
     * val <= 1 ==>
     *      find rightmost node with value 1. Find the last (rightmost) slot
     *      containing 1. The result set begins with that value, scan left
     *      until the end of chain is reached.
     *
     * A note about ordering: If left and right ends of chain are not known
     * somehow, queries with only one bound (e.g. val < 1) will be restricted
     * to either descending or ascending order only.
     */
    for(i=0; i<argc; i++) {
      if(arglist[i].column != col) continue;
      switch(arglist[i].cond) {
        case WG_COND_EQUAL:
          start_bound = arglist[i].value;
          end_bound = arglist[i].value;
          start_inclusive = end_inclusive = 1;
          /* We have no reason to prefer any specific value, so
           * finish looking for the bounds.
           * XXX: this allows invalid arguments like col = 0 & col = 2
           * where the first one simply wins and the second one is
           * ignored.
           */
          goto bounds_done;
        case WG_COND_LESSTHAN:
          /* No earlier left bound or new end bound is a smaller
           * value (reducing the result set) */
          if(end_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, end_bound, arglist[i].value)==WG_GREATER) {
            end_bound = arglist[i].value;
            end_inclusive = 0;
          }
          break;
        case WG_COND_GREATER:
          /* No earlier left bound or new left bound is a bigger value */
          if(start_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, start_bound, arglist[i].value)==WG_LESSTHAN) {
            start_bound = arglist[i].value;
            start_inclusive = 0;
          }
          break;
        case WG_COND_LTEQUAL:
          /* Similar to "less than", but inclusive */
          if(end_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, end_bound, arglist[i].value)==WG_GREATER) {
            end_bound = arglist[i].value;
            end_inclusive = 1;
          }
          break;
        case WG_COND_GTEQUAL:
          /* Similar to "greater", but inclusive */
          if(start_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, start_bound, arglist[i].value)==WG_LESSTHAN) {
            start_bound = arglist[i].value;
            start_inclusive = 1;
          }
          break;
        case WG_COND_NOT_EQUAL:
          /* Force use of full argument list to check each row in the result
           * set since we have a condition we cannot satisfy using
           * a continuous range of T-tree values alone
           */
          query->column = -1;
          break;
        default:
          show_query_error(db, "Invalid condition (ignoring)");
          break;
      }
    }

bounds_done:
    hdr = offsettoptr(db, index_id);

    /* Now find the bounding nodes for the query */
    if(start_bound==WG_ILLEGAL) {
      /* Find leftmost node in index */
#ifdef TTREE_CHAINED_NODES
      query->curr_offset = hdr->offset_min_node;
#else
      /* LUB node search function has the useful property
       * of returning the leftmost node when called directly
       * on index root node */
      query->curr_offset = wg_ttree_find_lub_node(db, hdr->offset_root_node);
#endif
      query->curr_slot = 0; /* leftmost slot */
    } else {
      gint boundtype;

      if(start_inclusive) {
        /* In case of inclusive range, we get the leftmost
         * node for the given value and the first slot that
         * is equal or greater than the given value.
         */
        query->curr_offset = wg_search_ttree_leftmost(db,
          hdr->offset_root_node, start_bound, &boundtype, NULL);
        if(boundtype == REALLY_BOUNDING_NODE) {
          /* XXX: temporarily, dbindex.c functions use decoded ints
           * as values. This will change soon.
           */
          query->curr_slot = wg_search_tnode_first(db, query->curr_offset,
            wg_decode_int(db, start_bound), query->column);
          if(query->curr_slot == -1) {
            show_query_error(db, "Starting index node was bad");
            free(query);
            return NULL;
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* No exact match, but the next node should be in
           * range. */
          node = offsettoptr(db, query->curr_offset);
          query->curr_offset = TNODE_SUCCESSOR(db, node);
          query->curr_slot = 0;
        } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
          /* Simplest case, values that are in range start
           * with this node. */
          query->curr_slot = 0;
        }
      } else {
        /* For non-inclusive, we need the rightmost node and
         * the last slot+1. The latter may overflow into next node.
         */
        query->curr_offset = wg_search_ttree_rightmost(db,
          hdr->offset_root_node, start_bound, &boundtype, NULL);
        if(boundtype == REALLY_BOUNDING_NODE) {
          /* XXX: decoded value for now */
          query->curr_slot = wg_search_tnode_last(db, query->curr_offset,
            wg_decode_int(db, start_bound), query->column);
          if(query->curr_slot == -1) {
            show_query_error(db, "Starting index node was bad");
            free(query);
            return NULL;
          }
          query->curr_slot++;
          node = offsettoptr(db, query->curr_offset);
          if(node->number_of_elements <= query->curr_slot) {
            /* Crossed node boundary */
            query->curr_offset = TNODE_SUCCESSOR(db, node);
            query->curr_slot = 0;
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* Since exact value was not found, this case is exactly
           * the same as with the inclusive range. */
          node = offsettoptr(db, query->curr_offset);
          query->curr_offset = TNODE_SUCCESSOR(db, node);
          query->curr_slot = 0;
        } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
          /* No exact value in tree, same as inclusive range */
          query->curr_slot = 0;
        }
      }
    }
    
    /* Finding of the end of the range is more or less opposite
     * of finding the beginning. */
    if(end_bound==WG_ILLEGAL) {
      /* Rightmost node in index */
#ifdef TTREE_CHAINED_NODES
      query->end_offset = hdr->offset_max_node;
#else
      /* GLB search on root node returns the rightmost node in tree */
      query->end_offset = wg_ttree_find_glb_node(db, hdr->offset_root_node);
#endif
      if(query->end_offset) {
        node = offsettoptr(db, query->end_offset);
        query->end_slot = node->number_of_elements - 1; /* rightmost slot */
      }
    } else {
      gint boundtype;

      if(end_inclusive) {
        /* Find the rightmost node with a given value and the
         * righmost slot that is equal or smaller than that value
         */
        query->end_offset = wg_search_ttree_rightmost(db,
          hdr->offset_root_node, end_bound, &boundtype, NULL);
        if(boundtype == REALLY_BOUNDING_NODE) {
          /* XXX: decoded value for now */
          query->end_slot = wg_search_tnode_last(db, query->end_offset,
            wg_decode_int(db, end_bound), query->column);
          if(query->end_slot == -1) {
            show_query_error(db, "Ending index node was bad");
            free(query);
            return NULL;
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* Last node containing values in range. */
          node = offsettoptr(db, query->end_offset);
          query->end_slot = node->number_of_elements - 1;
        } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
          /* Previous node should be in range. */
          node = offsettoptr(db, query->end_offset);
          query->end_offset = TNODE_PREDECESSOR(db, node);
          if(query->end_offset) {
            node = offsettoptr(db, query->end_offset);
            query->end_slot = node->number_of_elements - 1; /* rightmost slot */
          }
        }
      } else {
        /* For non-inclusive, we need the leftmost node and
         * the first slot-1.
         */
        query->end_offset = wg_search_ttree_leftmost(db,
          hdr->offset_root_node, end_bound, &boundtype, NULL);
        if(boundtype == REALLY_BOUNDING_NODE) {
          /* XXX: decoded value for now */
          query->end_slot = wg_search_tnode_first(db, query->end_offset,
            wg_decode_int(db, end_bound), query->column);
          if(query->end_slot == -1) {
            show_query_error(db, "Starting index node was bad");
            free(query);
            return NULL;
          }
          query->end_slot--;
          if(query->end_slot < 0) {
            /* Crossed node boundary */
            node = offsettoptr(db, query->end_offset);
            query->end_offset = TNODE_PREDECESSOR(db, node);
            if(query->end_offset) {
              node = offsettoptr(db, query->end_offset);
              query->end_slot = node->number_of_elements - 1;
            }
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* No exact value in tree, same as inclusive range */
          node = offsettoptr(db, query->end_offset);
          query->end_slot = node->number_of_elements - 1;
        } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
          /* No exact value in tree, same as inclusive range */
          node = offsettoptr(db, query->end_offset);
          query->end_offset = TNODE_PREDECESSOR(db, node);
          if(query->end_offset) {
            node = offsettoptr(db, query->end_offset);
            query->end_slot = node->number_of_elements - 1; /* rightmost slot */
          }
        }
      }
    }
  } else {
    /* Nothing better than full scan available */
    query->qtype = WG_QTYPE_SCAN;
    query->column = -1; /* no special column, entire argument list
                         * should be checked for each row */
  }
  
  /* XXX: here we can reverse the direction and switch the start and
   * end nodes/slots, if "descending" sort order is needed.
   */
  return query;
}

void *wg_make_match_query(void *db, void *matchlist) {
  /* XXX: not done yet */
  return NULL;
}

/** Return next record from the query object
 *  returns NULL if no more records
 */
void *wg_fetch(void *db, wg_query *query) {
  void *rec;

#ifdef CHECK
  if(!query) {
    show_query_error(db, "Invalid query object");
    return NULL;
  }
#endif
  if(query->qtype == WG_QTYPE_SCAN) {
    show_query_error(db, "Full scan query not yet implemented");
    return NULL;
  }
  else if(query->qtype == WG_QTYPE_TTREE) {
    struct wg_tnode *node;
    
    if(!query->curr_offset) {
      /* No more nodes to examine */
      return NULL;
    }
    node = offsettoptr(db, query->curr_offset);
    rec = offsettoptr(db, node->array_of_values[query->curr_slot]);

    /* XXX: check arglist here for all columns that do
     * not equal -1 */

    /* Update the current offset and slot before we return */

    if(query->curr_offset==query->end_offset && \
      query->curr_slot==query->end_slot) {
      /* Last slot reached, mark the query as exchausted */
      query->curr_offset = 0;
      return rec;
    }

    query->curr_slot += query->direction;
    if(query->curr_slot < 0) {
      if(!query->end_offset || query->end_offset==query->curr_offset) {
        /* No more nodes */
        query->curr_offset = 0;
      } else {
        node = offsettoptr(db, query->curr_offset);
        query->curr_offset = TNODE_PREDECESSOR(db, node);
        if(query->curr_offset) {
          node = offsettoptr(db, query->curr_offset);
          query->curr_slot = node->number_of_elements - 1;
        }
      }
    } else if(query->curr_slot >= node->number_of_elements) {
      if(!query->end_offset || query->end_offset==query->curr_offset) {
        /* No more nodes */
        query->curr_offset = 0;
      } else {
        node = offsettoptr(db, query->curr_offset);
        query->curr_offset = TNODE_SUCCESSOR(db, node);
        query->curr_slot = 0;
      }
    }
    return rec;
  }
  else {
    show_query_error(db, "Unsupported query type");
    return NULL;
  }
}

/** Release the memory allocated for the query
 */
void wg_free_query(void *db, wg_query *query) {
  free(query);
}

/* --------------- error handling ------------------------------*/

/** called with err msg
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_query_error(void* db, char* errmsg) {
  printf("query error: %s\n",errmsg);
  return -1;
} 

#if 0
/** called with err msg and additional int data
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_query_error_nr(void* db, char* errmsg, gint nr) {
  printf("query error: %s %d\n",errmsg,nr);
  return -1;
}  
#endif
