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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ====== Private headers and defs ======== */

#ifdef __cplusplus
extern "C" {
#endif

#include "dballoc.h"
#include "dbquery.h"
#include "dbcompare.h"

/* T-tree based scoring */
#define TTREE_SCORE_EQUAL 5
#define TTREE_SCORE_BOUND 2
#define TTREE_SCORE_NULL -1 /** penalty for null values, which
                             *  are likely to be abundant */
#define TTREE_SCORE_MASK 5  /** matching field in template */

/* ======= Private protos ================ */

static gint most_restricting_column(void *db,
  wg_query_arg *arglist, gint argc, gint *index_id);
static gint check_arglist(void *db, void *rec, wg_query_arg *arglist,
  gint argc);

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
  wg_query_arg *arglist, gint argc, gint *index_id) {

  struct column_score {
    gint column;
    int score;
    int index_id;
  };
  struct column_score *sc;
  int i, j, mrc_score = -1;
  gint mrc = -1;
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
    sc[i].index_id = 0;

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
        if(arglist[i].value == 0) /* NULL values get a small penalty */
          sc[j].score += TTREE_SCORE_NULL;
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
    /* Find the index on the column. The score is modified by the
     * estimated quality of the index (0 if no index found).
     */
    if(sc[i].column <= MAX_INDEXED_FIELDNR) {
      gint *ilist = &dbh->index_control_area_header.index_table[sc[i].column];
      while(*ilist) {
        gcell *ilistelem = (gcell *) offsettoptr(db, *ilist);
        if(ilistelem->car) {
          wg_index_header *hdr = \
            (wg_index_header *) offsettoptr(db, ilistelem->car);

          if(hdr->type == WG_INDEX_TYPE_TTREE) {
#ifdef USE_INDEX_TEMPLATE
            /* If index templates are available, we can increase the
             * score of the index if the template has any columns matching
             * the query parameters. On the other hand, in case of a
             * mismatch the index is unusable and has to be skipped.
             * The indexes are sorted in the order of fixed columns in
             * the template, so if there is a match, the search is
             * complete (remaining index are likely to be worse)
             */
            if(hdr->template_offset) {
              wg_index_template *tmpl = \
                (wg_index_template *) offsettoptr(db, hdr->template_offset);
              void *matchrec = offsettoptr(db, tmpl->offset_matchrec);
              gint reclen = wg_get_record_len(db, matchrec);
              for(j=0; j<reclen; j++) {
                gint enc = wg_get_field(db, matchrec, j);
                if(wg_get_encoded_type(db, enc) != WG_VARTYPE) {
                  /* defined column in matchrec. The score is increased
                   * if arglist has a WG_COND_EQUAL column with the same
                   * value. In any other case the index is not usable.
                   */
                  int match = 0, k;
                  for(k=0; k<argc; k++) {
                    if(arglist[k].column == j) {
                      if(arglist[k].cond == WG_COND_EQUAL &&\
                        WG_COMPARE(db, enc, arglist[k].value) == WG_EQUAL) {
                        match = 1;
                      }
                      else
                        goto nextindex;
                    }
                  }
                  if(match) {
                    sc[i].score += TTREE_SCORE_MASK;
                    if(!enc)
                      sc[i].score += TTREE_SCORE_NULL;
                  }
                  else
                    goto nextindex;
                }
              }
            }
#endif
            sc[i].index_id = ilistelem->car;
            break;
          }
        }
#ifdef USE_INDEX_TEMPLATE
nextindex:
#endif
        ilist = &ilistelem->cdr;
      }
    }
    if(!sc[i].index_id)
      sc[i].score = 0; /* no index, score reset */
    if(sc[i].score > mrc_score) {
      mrc_score = sc[i].score;
      mrc = sc[i].column;
      *index_id = sc[i].index_id;
    }
  }

  /* TODO: does the best score have no index? In that case,
   * try to locate an index that would restrict at least
   * some columns.
   */
  free(sc);
  return mrc;
}

/** Check a record against list of conditions
 *  returns 1 if the record matches
 *  returns 0 if the record fails at least one condition
 */
static gint check_arglist(void *db, void *rec, wg_query_arg *arglist,
  gint argc) {

  int i, reclen;

  reclen = wg_get_record_len(db, rec);
  for(i=0; i<argc; i++) {
    if(arglist[i].column < reclen) {
      gint encoded = wg_get_field(db, rec, arglist[i].column);
      switch(arglist[i].cond) {
        case WG_COND_EQUAL:
          if(WG_COMPARE(db, encoded, arglist[i].value) != WG_EQUAL)
            return 0;
          break;
        case WG_COND_LESSTHAN:
          if(WG_COMPARE(db, encoded, arglist[i].value) != WG_LESSTHAN)
            return 0;
          break;
        case WG_COND_GREATER:
          if(WG_COMPARE(db, encoded, arglist[i].value) != WG_GREATER)
            return 0;
          break;
        case WG_COND_LTEQUAL:
          if(WG_COMPARE(db, encoded, arglist[i].value) == WG_GREATER)
            return 0;
          break;
        case WG_COND_GTEQUAL:
          if(WG_COMPARE(db, encoded, arglist[i].value) == WG_LESSTHAN)
            return 0;
          break;
        case WG_COND_NOT_EQUAL:
          if(WG_COMPARE(db, encoded, arglist[i].value) == WG_EQUAL)
            return 0;
          break;
        default:
          break;
      }
    }
  }

  return 1;
}

/** Create a query object.
 *
 * matchrec - array of encoded integers. Can be a pointer to a database record
 * or a user-allocated array. If reclen is 0, it is treated as a native
 * database record. If reclen is non-zero, reclen number of gint-sized
 * words is read, starting from the pointer.
 *
 * Fields of type WG_VARTYPE in matchrec are treated as wildcards. Other
 * types, including NULL, are used as "equals" conditions.
 *
 * arglist - array of wg_query_arg objects. The size is must be given
 * by argc.
 *
 * returns NULL if constructing the query fails. Otherwise returns a pointer
 * to a wg_query object.
 */
wg_query *wg_make_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc) {
  
  wg_query *query;
  wg_query_arg *full_arglist;
  gint fargc;
  gint col, index_id;
  int i;

#ifdef CHECK
  if (!dbcheck(db)) {
    /* XXX: currently show_query_error would work too */
    fprintf(stderr, "Invalid database pointer in wg_fetch.\n");
    return NULL;
  }
#endif

  if(matchrec) {
    /* Get the correct length of matchrec data area and the pointer
     * to the beginning of the data. If matchrec is a plain array in
     * local memory (indicated by NON-zero reclen) we will skip this step.
     */
    if(!reclen) {
      reclen = wg_get_record_len(db, matchrec);
      matchrec = wg_get_record_dataarray(db, matchrec);
    }
#ifdef CHECK
    if(!reclen) {
      show_query_error(db, "Zero-length match record argument");
      return NULL;
    }
#endif
  }

  /* The simplest way to treat matchrec is to convert it to
   * arglist. While doing this, we will create a local copy of the
   * argument list, which has the side effect of allowing the caller
   * to free the original arglist after wg_make_query() returns. The
   * local copy will be attached to the query object and needs to
   * survive beyond that.
   */
  fargc = argc;
  if(matchrec) {
    for(i=0; i<reclen; i++) {
      if(wg_get_encoded_type(db, ((gint *) matchrec)[i]) != WG_VARTYPE)
        fargc++;
    }
  }

  if(!fargc) {
    show_query_error(db, "Invalid number of arguments");
    return NULL;
  }

  query = (wg_query *) malloc(sizeof(wg_query));
  if(!query) {
    show_query_error(db, "Failed to allocate memory");
    return NULL;
  }

  full_arglist = (wg_query_arg *) malloc(fargc * sizeof(wg_query_arg));
  if(!full_arglist) {
    show_query_error(db, "Failed to allocate memory");
    free(query);
    return NULL;
  }

  /* Copy the arglist contents */
  for(i=0; i<argc; i++) {
    full_arglist[i].column = arglist[i].column;
    full_arglist[i].cond = arglist[i].cond;
    full_arglist[i].value = arglist[i].value;
  }

  /* Append the matchrec data */
  if(matchrec) {
    int j;
    for(i=0, j=argc; i<reclen; i++) {
      if(wg_get_encoded_type(db, ((gint *) matchrec)[i]) != WG_VARTYPE) {
        full_arglist[j].column = i;
        full_arglist[j].cond = WG_COND_EQUAL;
        full_arglist[j++].value = ((gint *) matchrec)[i];
      }
    }
  }
  
  /* Find the best (hopefully) index to base the query on.
   * Then initialise the query object to the first row in the
   * query result set.
   * XXX: only considering T-tree indexes now. */
  col = most_restricting_column(db, full_arglist, fargc, &index_id);

  if(index_id > 0) {
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
     */
    for(i=0; i<fargc; i++) {
      if(full_arglist[i].column != col) continue;
      switch(full_arglist[i].cond) {
        case WG_COND_EQUAL:
          /* Set bounds as if we had val >= 1 & val <= 1 */
          if(start_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, start_bound, full_arglist[i].value)==WG_LESSTHAN) {
            start_bound = full_arglist[i].value;
            start_inclusive = 1;
          }
          if(end_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, end_bound, full_arglist[i].value)==WG_GREATER) {
            end_bound = full_arglist[i].value;
            end_inclusive = 1;
          }
          break;
        case WG_COND_LESSTHAN:
          /* No earlier right bound or new end bound is a smaller
           * value (reducing the result set). The result set is also
           * possibly reduced if the value is equal, because this
           * condition is non-inclusive. */
          if(end_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, end_bound, full_arglist[i].value)!=WG_LESSTHAN) {
            end_bound = full_arglist[i].value;
            end_inclusive = 0;
          }
          break;
        case WG_COND_GREATER:
          /* No earlier left bound or new left bound is >= of old value */
          if(start_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, start_bound, full_arglist[i].value)!=WG_GREATER) {
            start_bound = full_arglist[i].value;
            start_inclusive = 0;
          }
          break;
        case WG_COND_LTEQUAL:
          /* Similar to "less than", but inclusive */
          if(end_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, end_bound, full_arglist[i].value)==WG_GREATER) {
            end_bound = full_arglist[i].value;
            end_inclusive = 1;
          }
          break;
        case WG_COND_GTEQUAL:
          /* Similar to "greater", but inclusive */
          if(start_bound==WG_ILLEGAL ||\
            WG_COMPARE(db, start_bound, full_arglist[i].value)==WG_LESSTHAN) {
            start_bound = full_arglist[i].value;
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

    /* Simple sanity check. Is start_bound greater than end_bound? */
    if(start_bound!=WG_ILLEGAL && end_bound!=WG_ILLEGAL &&\
      WG_COMPARE(db, start_bound, end_bound) == WG_GREATER) {
      /* return empty query */
      query->argc = 0;
      query->arglist = NULL;
      free(full_arglist);
      return query;
    }

    hdr = (wg_index_header *) offsettoptr(db, index_id);

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
          query->curr_slot = wg_search_tnode_first(db, query->curr_offset,
            start_bound, col);
          if(query->curr_slot == -1) {
            show_query_error(db, "Starting index node was bad");
            free(query);
            free(full_arglist);
            return NULL;
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* No exact match, but the next node should be in
           * range. */
          node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
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
          query->curr_slot = wg_search_tnode_last(db, query->curr_offset,
            start_bound, col);
          if(query->curr_slot == -1) {
            show_query_error(db, "Starting index node was bad");
            free(full_arglist);
            free(query);
            return NULL;
          }
          query->curr_slot++;
          node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
          if(node->number_of_elements <= query->curr_slot) {
            /* Crossed node boundary */
            query->curr_offset = TNODE_SUCCESSOR(db, node);
            query->curr_slot = 0;
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* Since exact value was not found, this case is exactly
           * the same as with the inclusive range. */
          node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
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
        node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
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
          query->end_slot = wg_search_tnode_last(db, query->end_offset,
            end_bound, col);
          if(query->end_slot == -1) {
            show_query_error(db, "Ending index node was bad");
            free(full_arglist);
            free(query);
            return NULL;
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* Last node containing values in range. */
          node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
          query->end_slot = node->number_of_elements - 1;
        } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
          /* Previous node should be in range. */
          node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
          query->end_offset = TNODE_PREDECESSOR(db, node);
          if(query->end_offset) {
            node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
            query->end_slot = node->number_of_elements - 1; /* rightmost */
          }
        }
      } else {
        /* For non-inclusive, we need the leftmost node and
         * the first slot-1.
         */
        query->end_offset = wg_search_ttree_leftmost(db,
          hdr->offset_root_node, end_bound, &boundtype, NULL);
        if(boundtype == REALLY_BOUNDING_NODE) {
          query->end_slot = wg_search_tnode_first(db, query->end_offset,
            end_bound, col);
          if(query->end_slot == -1) {
            show_query_error(db, "Ending index node was bad");
            free(full_arglist);
            free(query);
            return NULL;
          }
          query->end_slot--;
          if(query->end_slot < 0) {
            /* Crossed node boundary */
            node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
            query->end_offset = TNODE_PREDECESSOR(db, node);
            if(query->end_offset) {
              node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
              query->end_slot = node->number_of_elements - 1;
            }
          }
        } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
          /* No exact value in tree, same as inclusive range */
          node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
          query->end_slot = node->number_of_elements - 1;
        } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
          /* No exact value in tree, same as inclusive range */
          node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
          query->end_offset = TNODE_PREDECESSOR(db, node);
          if(query->end_offset) {
            node = (struct wg_tnode *) offsettoptr(db, query->end_offset);
            query->end_slot = node->number_of_elements - 1; /* rightmost slot */
          }
        }
      }
    }

    /* Now detect the cases where the above bound search
     * has produced a result with an empty range.
     */
    if(query->curr_offset) {
      /* Value could be bounded inside a node, but actually
       * not present. Note that we require the end_slot to be
       * >= curr_slot, this implies that query->direction == 1.
       */
      if(query->end_offset == query->curr_offset &&\
        query->end_slot < query->curr_slot) {
        query->curr_offset = 0; /* query will return no rows */
        query->end_offset = 0;
      } else if(!query->end_offset) {
        /* If one offset is 0 the other should be forced to 0, so that
         * if we want to switch direction we won't run into any surprises.
         */
        query->curr_offset = 0;
      } else {
        /* Another case we have to watch out for is when we have a
         * range that fits in the space between two nodes. In that case
         * the end offset will end up directly left of the start offset.
         */
        node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
        if(query->end_offset == TNODE_PREDECESSOR(db, node)) {
          query->curr_offset = 0; /* no rows */
          query->end_offset = 0;
        }
      }
    } else
      query->end_offset = 0; /* again, if one offset is 0,
                              * the other should be, too */

    /* XXX: here we can reverse the direction and switch the start and
     * end nodes/slots, if "descending" sort order is needed.
     */

  } else {
    /* Nothing better than full scan available */
    void *rec;

    query->qtype = WG_QTYPE_SCAN;
    query->column = -1; /* no special column, entire argument list
                         * should be checked for each row */

    rec = wg_get_first_record(db);
    if(rec)
      query->curr_record = ptrtooffset(db, rec);
    else
      query->curr_record = 0;
  }
  
  /* Now attach the argument list to the query. If the query is based
   * on a column index, we will create a slimmer copy that does not contain
   * the conditions already satisfied by the index bounds.
   */
  if(query->column == -1) {
    query->arglist = full_arglist;
    query->argc = fargc;
  }
  else {
    int cnt = 0;
    for(i=0; i<fargc; i++) {
      if(full_arglist[i].column != query->column)
        cnt++;
    }

    /* The argument list is reduced, but still contains columns */
    if(cnt) {
      int j;
      query->arglist = (wg_query_arg *) malloc(cnt * sizeof(wg_query_arg));
      if(!query->arglist) {
        show_query_error(db, "Failed to allocate memory");
        free(query);
        free(full_arglist);
        return NULL;
      }
      for(i=0, j=0; i<fargc; i++) {
        if(full_arglist[i].column != query->column) {
          query->arglist[j].column = full_arglist[i].column;
          query->arglist[j].cond = full_arglist[i].cond;
          query->arglist[j++].value = full_arglist[i].value;
        }
      }
    } else
      query->arglist = NULL;
    query->argc = cnt;
    free(full_arglist); /* Now we have a reduced argument list, free
                         * the original one */
  }

  return query;
}

/** Create a query object and pre-fetch all data rows.
 *
 * Function arguments are identical to wg_query(). Allocates enough
 * space to hold all row offsets, fetches them and stores them in an array.
 * Isolation is not guaranteed in any way, shape or form, but can be
 * implemented on top by the user.
 *
 * returns NULL if constructing the query fails. Otherwise returns a pointer
 * to a wg_query object.
 */
wg_query *wg_make_prefetch_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc) {
  wg_query *query, tmp;
  void *rec;
  gint i;

  query = wg_make_query(db, matchrec, reclen, arglist, argc);
  if(!query)
    return NULL;

  /* Count the number of rows.
   * XXX: perhaps this can be optimised for some query types,
   * for example guesstimating the number of rows, doing immediate
   * copy in first loop and using realloc() if initial guess is
   * not enough to fit all data.
   */
  query->curr_res = 0;
  query->res_count = 0;
  memcpy(&tmp, query, sizeof(wg_query));
  rec = wg_fetch(db, &tmp); 
  while(rec) {
    query->res_count++;
    rec = wg_fetch(db, &tmp);
  }

  if(!query->res_count) {
    query->results = NULL;
    return query; /* empty set */
  }
  query->results = (gint *) malloc(query->res_count * sizeof(gint));
  if(!query->results) {
    show_query_error(db, "Failed to allocate result set");
    wg_free_query(db, query);
    return NULL;
  }

  /* Fetch the rows. This "exhausts" the original query since
   * we are no longer using a copy. */
  for(i=0; i<query->res_count; i++) {
    rec = wg_fetch(db, query);
    if(!rec) break;
    query->results[i] = ptrtooffset(db, rec);
  }

  /* Paranoia. */
  if(i < query->res_count) {
    show_query_error(db, "Warning: resultset shrinked");
    query->res_count = i;
  }

  /* Finally, convert the query type. */
  query->qtype = WG_QTYPE_PREFETCH;
  return query;
}

/** Return next record from the query object
 *  returns NULL if no more records
 */
void *wg_fetch(void *db, wg_query *query) {
  void *rec;

#ifdef CHECK
  if (!dbcheck(db)) {
    /* XXX: currently show_query_error would work too */
    fprintf(stderr, "Invalid database pointer in wg_fetch.\n");
    return NULL;
  }
  if(!query) {
    show_query_error(db, "Invalid query object");
    return NULL;
  }
#endif
  if(query->qtype == WG_QTYPE_SCAN) {
    for(;;) {
      void *next;

      if(!query->curr_record) {
        /* Query exhausted */
        return NULL;
      }

      rec = offsettoptr(db, query->curr_record);

      /* Pre-fetch the next record */
      next = wg_get_next_record(db, rec);
      if(next)
        query->curr_record = ptrtooffset(db, next);
      else
        query->curr_record = 0;
        
      /* Check the record against all conditions; if it does
       * not match, go to next iteration.
       */
      if(!query->arglist || \
        check_arglist(db, rec, query->arglist, query->argc))
        return rec;
    }
  }
  else if(query->qtype == WG_QTYPE_TTREE) {
    struct wg_tnode *node;
    
    for(;;) {
      if(!query->curr_offset) {
        /* No more nodes to examine */
        return NULL;
      }
      node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
      rec = offsettoptr(db, node->array_of_values[query->curr_slot]);

      /* Increment the slot/and or node cursors before we
       * return. If the current node does not satisfy the
       * argument list we may need to do this multiple times.
       */
      if(query->curr_offset==query->end_offset && \
        query->curr_slot==query->end_slot) {
        /* Last slot reached, mark the query as exchausted */
        query->curr_offset = 0;
      } else {
        /* Some rows still left */
        query->curr_slot += query->direction;
        if(query->curr_slot < 0) {
#ifdef CHECK
          if(query->end_offset==query->curr_offset) {
            /* This should not happen */
            show_query_error(db, "Warning: end slot mismatch, possible bug");
            query->curr_offset = 0;
          } else {
#endif
            node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
            query->curr_offset = TNODE_PREDECESSOR(db, node);
            if(query->curr_offset) {
              node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
              query->curr_slot = node->number_of_elements - 1;
            }
#ifdef CHECK
          }
#endif
        } else if(query->curr_slot >= node->number_of_elements) {
#ifdef CHECK
          if(query->end_offset==query->curr_offset) {
            /* This should not happen */
            show_query_error(db, "Warning: end slot mismatch, possible bug");
            query->curr_offset = 0;
          } else {
#endif
            node = (struct wg_tnode *) offsettoptr(db, query->curr_offset);
            query->curr_offset = TNODE_SUCCESSOR(db, node);
            query->curr_slot = 0;
#ifdef CHECK
          }
#endif
        }
      }

      /* If there are no extra conditions or the row satisfies
       * all the conditions, we can return.
       */
      if(!query->arglist || \
        check_arglist(db, rec, query->arglist, query->argc))
        return rec;
    }
  }
  if(query->qtype == WG_QTYPE_PREFETCH) {
    if(query->curr_res < query->res_count) {
#ifdef CHECK
      if(!query->results) {
        show_query_error(db, "Invalid resultset");
        return NULL;
      }
#endif

      /* XXX: could check the validity of
       * query->results[query->curr_res] here
       */
      return offsettoptr(db, query->results[query->curr_res++]);
    }
    else
      return NULL;
  }
  else {
    show_query_error(db, "Unsupported query type");
    return NULL;
  }
}

/** Release the memory allocated for the query
 */
void wg_free_query(void *db, wg_query *query) {
  if(query->arglist)
    free(query->arglist);
  if(query->qtype==WG_QTYPE_PREFETCH && query->results)
    free(query->results);
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

#ifdef __cplusplus
}
#endif
