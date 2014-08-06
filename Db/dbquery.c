/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010,2011,2013,2014
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

 /** @file dbquery.c
 * WhiteDB query engine.
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
#include "dbmpool.h"
#include "dbschema.h"
#include "dbhash.h"

/* T-tree based scoring */
#define TTREE_SCORE_EQUAL 5
#define TTREE_SCORE_BOUND 2
#define TTREE_SCORE_NULL -1 /** penalty for null values, which
                             *  are likely to be abundant */
#define TTREE_SCORE_MASK 5  /** matching field in template */

/* Query flags for internal use */
#define QUERY_FLAGS_PREFETCH 0x1000

#define QUERY_RESULTSET_PAGESIZE 63  /* mpool is aligned, so we can align
                                      * the result pages too by selecting an
                                      * appropriate size */

/* Emulate array index when doing a scan of key-value pairs
 * in a JSON query.
 * If this is not desirable, commenting this out makes
 * scans somewhat faster.
 */
#define JSON_SCAN_UNWRAP_ARRAY

struct __query_result_page {
  gint rows[QUERY_RESULTSET_PAGESIZE];
  struct __query_result_page *next;
};

typedef struct __query_result_page query_result_page;

typedef struct {
  query_result_page *page;        /** current page of results */
  gint pidx;                      /** current index on page (reading) */
} query_result_cursor;

typedef struct {
  void *mpool;                    /** storage for row offsets */
  query_result_page *first_page;  /** first page of results, for rewinding */
  query_result_cursor wcursor;    /** read cursor */
  query_result_cursor rcursor;    /** write cursor */
  gint res_count;                 /** number of rows in results */
} query_result_set;

/* ======= Private protos ================ */

static gint most_restricting_column(void *db,
  wg_query_arg *arglist, gint argc, gint *index_id);
static gint check_arglist(void *db, void *rec, wg_query_arg *arglist,
  gint argc);
static gint prepare_params(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc,
  wg_query_arg **farglist, gint *fargc);
static gint find_ttree_bounds(void *db, gint index_id, gint col,
  gint start_bound, gint end_bound, gint start_inclusive, gint end_inclusive,
  gint *curr_offset, gint *curr_slot, gint *end_offset, gint *end_slot);
static wg_query *internal_build_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc, gint flags, wg_uint rowlimit);

static query_result_set *create_resultset(void *db);
static void free_resultset(void *db, query_result_set *set);
static void rewind_resultset(void *db, query_result_set *set);
static gint append_resultset(void *db, query_result_set *set, gint offset);
static gint fetch_resultset(void *db, query_result_set *set);
static query_result_set *intersect_resultset(void *db,
  query_result_set *seta, query_result_set *setb);
static gint check_and_merge_by_kv(void *db, void *rec,
  wg_json_query_arg *arg, query_result_set *next_set);
static gint check_and_merge_by_key(void *db, void *rec,
  wg_json_query_arg *arg, query_result_set *next_set);
static gint check_and_merge_recursively(void *db, void *rec,
  wg_json_query_arg *arg, query_result_set *next_set, int depth);
static gint prepare_json_arglist(void *db, wg_json_query_arg *arglist,
  wg_json_query_arg **sorted_arglist, gint argc,
  gint *index_id, gint *vindex_id, gint *kindex_id);

static gint encode_query_param_unistr(void *db, char *data, gint type,
  char *extdata, int length);

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
  db_memsegment_header* dbh = dbmemsegh(db);

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
    gint encoded;
    if(arglist[i].column < reclen)
      encoded = wg_get_field(db, rec, arglist[i].column);
    else
      return 0; /* XXX: should shorter records always fail?
                 * other possiblities here: compare to WG_ILLEGAL
                 * or WG_NULLTYPE. Current idea is based on SQL
                 * concept of comparisons to NULL always failing.
                 */

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

  return 1;
}

/** Prepare query parameters
 *
 * - Validates matchrec and arglist
 * - Converts external pointers to locally allocated data
 * - Builds an unified argument list
 *
 * Returns 0 on success, non-0 on error.
 *
 * If the function was successful, *farglist will be set to point
 * to a newly allocated unified argument list and *fargc will be set
 * to indicate the size of *farglist.
 *
 * If there was an error, *farglist and *fargc may be in
 * an undetermined state.
 */
static gint prepare_params(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc,
  wg_query_arg **farglist, gint *fargc) {
  int i;

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
      return -1;
    }
#endif
  }

#ifdef CHECK
  if(arglist && !argc) {
    show_query_error(db, "Zero-length argument list");
    return -1;
  }
  if(!arglist && argc) {
    show_query_error(db, "Invalid argument list (NULL)");
    return -1;
  }
#endif

  /* Determine total number of query parameters (number of arguments
   * in arglist and non-wildcard fields of matchrec).
   */
  *fargc = argc;
  if(matchrec) {
    for(i=0; i<reclen; i++) {
      if(wg_get_encoded_type(db, ((gint *) matchrec)[i]) != WG_VARTYPE)
        (*fargc)++;
    }
  }

  if(*fargc) {
    wg_query_arg *tmp = NULL;

    /* The simplest way to treat matchrec is to convert it to
     * arglist. While doing this, we will create a local copy of the
     * argument list, which has the side effect of allowing the caller
     * to free the original arglist after wg_make_query() returns. The
     * local copy will be attached to the query object and needs to
     * survive beyond that.
     */
    tmp = (wg_query_arg *) malloc(*fargc * sizeof(wg_query_arg));
    if(!tmp) {
      show_query_error(db, "Failed to allocate memory");
      return -2;
    }

    /* Copy the arglist contents */
    for(i=0; i<argc; i++) {
      tmp[i].column = arglist[i].column;
      tmp[i].cond = arglist[i].cond;
      tmp[i].value = arglist[i].value;
    }

    /* Append the matchrec data */
    if(matchrec) {
      int j;
      for(i=0, j=argc; i<reclen; i++) {
        if(wg_get_encoded_type(db, ((gint *) matchrec)[i]) != WG_VARTYPE) {
          tmp[j].column = i;
          tmp[j].cond = WG_COND_EQUAL;
          tmp[j++].value = ((gint *) matchrec)[i];
        }
      }
    }

    *farglist = tmp;
  }
  else {
    *farglist = NULL;
  }

  return 0;
}

/*
 * Locate the node offset and slot for start and end bound
 * in a T-tree index.
 *
 * return -1 on error
 * return 0 on success
 */
static gint find_ttree_bounds(void *db, gint index_id, gint col,
  gint start_bound, gint end_bound, gint start_inclusive, gint end_inclusive,
  gint *curr_offset, gint *curr_slot, gint *end_offset, gint *end_slot)
{
  /* hold the offsets temporarily */
  gint co = *curr_offset;
  gint cs = *curr_slot;
  gint eo = *end_offset;
  gint es = *end_slot;
  wg_index_header *hdr = (wg_index_header *) offsettoptr(db, index_id);
  struct wg_tnode *node;

  if(start_bound==WG_ILLEGAL) {
    /* Find leftmost node in index */
#ifdef TTREE_CHAINED_NODES
    co = TTREE_MIN_NODE(hdr);
#else
    /* LUB node search function has the useful property
     * of returning the leftmost node when called directly
     * on index root node */
    co = wg_ttree_find_lub_node(db, TTREE_ROOT_NODE(hdr));
#endif
    cs = 0; /* leftmost slot */
  } else {
    gint boundtype;

    if(start_inclusive) {
      /* In case of inclusive range, we get the leftmost
       * node for the given value and the first slot that
       * is equal or greater than the given value.
       */
      co = wg_search_ttree_leftmost(db,
        TTREE_ROOT_NODE(hdr), start_bound, &boundtype, NULL);
      if(boundtype == REALLY_BOUNDING_NODE) {
        cs = wg_search_tnode_first(db, co, start_bound, col);
        if(cs == -1) {
          show_query_error(db, "Starting index node was bad");
          return -1;
        }
      } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
        /* No exact match, but the next node should be in
         * range. */
        node = (struct wg_tnode *) offsettoptr(db, co);
        co = TNODE_SUCCESSOR(db, node);
        cs = 0;
      } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
        /* Simplest case, values that are in range start
         * with this node. */
        cs = 0;
      }
    } else {
      /* For non-inclusive, we need the rightmost node and
       * the last slot+1. The latter may overflow into next node.
       */
      co = wg_search_ttree_rightmost(db,
        TTREE_ROOT_NODE(hdr), start_bound, &boundtype, NULL);
      if(boundtype == REALLY_BOUNDING_NODE) {
        cs = wg_search_tnode_last(db, co, start_bound, col);
        if(cs == -1) {
          show_query_error(db, "Starting index node was bad");
          return -1;
        }
        cs++;
        node = (struct wg_tnode *) offsettoptr(db, co);
        if(node->number_of_elements <= cs) {
          /* Crossed node boundary */
          co = TNODE_SUCCESSOR(db, node);
          cs = 0;
        }
      } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
        /* Since exact value was not found, this case is exactly
         * the same as with the inclusive range. */
        node = (struct wg_tnode *) offsettoptr(db, co);
        co = TNODE_SUCCESSOR(db, node);
        cs = 0;
      } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
        /* No exact value in tree, same as inclusive range */
        cs = 0;
      }
    }
  }

  /* Finding of the end of the range is more or less opposite
   * of finding the beginning. */
  if(end_bound==WG_ILLEGAL) {
    /* Rightmost node in index */
#ifdef TTREE_CHAINED_NODES
    eo = TTREE_MAX_NODE(hdr);
#else
    /* GLB search on root node returns the rightmost node in tree */
    eo = wg_ttree_find_glb_node(db, TTREE_ROOT_NODE(hdr));
#endif
    if(eo) {
      node = (struct wg_tnode *) offsettoptr(db, eo);
      es = node->number_of_elements - 1; /* rightmost slot */
    }
  } else {
    gint boundtype;

    if(end_inclusive) {
      /* Find the rightmost node with a given value and the
       * righmost slot that is equal or smaller than that value
       */
      eo = wg_search_ttree_rightmost(db,
        TTREE_ROOT_NODE(hdr), end_bound, &boundtype, NULL);
      if(boundtype == REALLY_BOUNDING_NODE) {
        es = wg_search_tnode_last(db, eo, end_bound, col);
        if(es == -1) {
          show_query_error(db, "Ending index node was bad");
          return -1;
        }
      } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
        /* Last node containing values in range. */
        node = (struct wg_tnode *) offsettoptr(db, eo);
        es = node->number_of_elements - 1;
      } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
        /* Previous node should be in range. */
        node = (struct wg_tnode *) offsettoptr(db, eo);
        eo = TNODE_PREDECESSOR(db, node);
        if(eo) {
          node = (struct wg_tnode *) offsettoptr(db, eo);
          es = node->number_of_elements - 1; /* rightmost */
        }
      }
    } else {
      /* For non-inclusive, we need the leftmost node and
       * the first slot-1.
       */
      eo = wg_search_ttree_leftmost(db,
        TTREE_ROOT_NODE(hdr), end_bound, &boundtype, NULL);
      if(boundtype == REALLY_BOUNDING_NODE) {
        es = wg_search_tnode_first(db, eo,
          end_bound, col);
        if(es == -1) {
          show_query_error(db, "Ending index node was bad");
          return -1;
        }
        es--;
        if(es < 0) {
          /* Crossed node boundary */
          node = (struct wg_tnode *) offsettoptr(db, eo);
          eo = TNODE_PREDECESSOR(db, node);
          if(eo) {
            node = (struct wg_tnode *) offsettoptr(db, eo);
            es = node->number_of_elements - 1;
          }
        }
      } else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING) {
        /* No exact value in tree, same as inclusive range */
        node = (struct wg_tnode *) offsettoptr(db, eo);
        es = node->number_of_elements - 1;
      } else if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
        /* No exact value in tree, same as inclusive range */
        node = (struct wg_tnode *) offsettoptr(db, eo);
        eo = TNODE_PREDECESSOR(db, node);
        if(eo) {
          node = (struct wg_tnode *) offsettoptr(db, eo);
          es = node->number_of_elements - 1; /* rightmost slot */
        }
      }
    }
  }

  /* Now detect the cases where the above bound search
   * has produced a result with an empty range.
   */
  if(co) {
    /* Value could be bounded inside a node, but actually
     * not present. Note that we require the end_slot to be
     * >= curr_slot, this implies that query->direction == 1.
     */
    if(eo == co && es < cs) {
      co = 0; /* query will return no rows */
      eo = 0;
    } else if(!eo) {
      /* If one offset is 0 the other should be forced to 0, so that
       * if we want to switch direction we won't run into any surprises.
       */
      co = 0;
    } else {
      /* Another case we have to watch out for is when we have a
       * range that fits in the space between two nodes. In that case
       * the end offset will end up directly left of the start offset.
       */
      node = (struct wg_tnode *) offsettoptr(db, co);
      if(eo == TNODE_PREDECESSOR(db, node)) {
        co = 0; /* no rows */
        eo = 0;
      }
    }
  } else {
    eo = 0; /* again, if one offset is 0,
             * the other should be, too */
  }

  *curr_offset = co;
  *curr_slot = cs;
  *end_offset = eo;
  *end_slot = es;
  return 0;
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
 * flags - type of query requested and other parameters
 *
 * rowlimit - maximum number of rows fetched. Only has an effect if
 * QUERY_FLAGS_PREFETCH is set.
 *
 * returns NULL if constructing the query fails. Otherwise returns a pointer
 * to a wg_query object.
 */
static wg_query *internal_build_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc, gint flags, wg_uint rowlimit) {

  wg_query *query;
  wg_query_arg *full_arglist;
  gint fargc = 0;
  gint col, index_id = -1;
  int i;

#ifdef CHECK
  if (!dbcheck(db)) {
    /* XXX: currently show_query_error would work too */
#ifdef WG_NO_ERRPRINT
#else
    fprintf(stderr, "Invalid database pointer in wg_make_query.\n");
#endif
    return NULL;
  }
#endif

  /* Check and prepare the parameters. If there was an error,
   * prepare_params() does it's own cleanup so we can (and should)
   * return immediately.
   */
  if(prepare_params(db, matchrec, reclen, arglist, argc,
    &full_arglist, &fargc)) {
    return NULL;
  }

  query = (wg_query *) malloc(sizeof(wg_query));
  if(!query) {
    show_query_error(db, "Failed to allocate memory");
    if(full_arglist) free(full_arglist);
    return NULL;
  }

  if(fargc) {
    /* Find the best (hopefully) index to base the query on.
     * Then initialise the query object to the first row in the
     * query result set.
     * XXX: only considering T-tree indexes now. */
    col = most_restricting_column(db, full_arglist, fargc, &index_id);
  }
  else {
    /* Create a "full scan" query with no arguments. */
    index_id = -1;
    full_arglist = NULL; /* redundant/paranoia */
  }

  if(index_id > 0) {
    int start_inclusive = 0, end_inclusive = 0;
    gint start_bound = WG_ILLEGAL; /* encoded values */
    gint end_bound = WG_ILLEGAL;

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

    /* Now find the bounding nodes for the query */
    if(find_ttree_bounds(db, index_id, col,
        start_bound, end_bound, start_inclusive, end_inclusive,
        &query->curr_offset, &query->curr_slot, &query->end_offset,
        &query->end_slot)) {
      free(query);
      free(full_arglist);
      return NULL;
    }

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

  /* Now handle any post-processing required.
   */
  if(flags & QUERY_FLAGS_PREFETCH) {
    query_result_page **prevnext;
    query_result_page *currpage;
    void *rec;

    query->curr_page = NULL; /* initialize as empty */
    query->curr_pidx = 0;
    query->res_count = 0;

    /* XXX: could move this inside the loop (speeds up empty
     * query, slows down other queries) */
    query->mpool = wg_create_mpool(db, sizeof(query_result_page));
    if(!query->mpool) {
      show_query_error(db, "Failed to allocate result memory pool");
      wg_free_query(db, query);
      return NULL;
    }

    i = QUERY_RESULTSET_PAGESIZE;
    prevnext = (query_result_page **) &(query->curr_page);

    while((rec = wg_fetch(db, query))) {
      if(i >= QUERY_RESULTSET_PAGESIZE) {
        currpage = (query_result_page *) \
          wg_alloc_mpool(db, query->mpool, sizeof(query_result_page));
        if(!currpage) {
          show_query_error(db, "Failed to allocate a resultset row");
          wg_free_query(db, query);
          return NULL;
        }
        memset(currpage->rows, 0, sizeof(gint) * QUERY_RESULTSET_PAGESIZE);
        *prevnext = currpage;
        prevnext = &(currpage->next);
        currpage->next = NULL;
        i = 0;
      }
      currpage->rows[i++] = ptrtooffset(db, rec);
      query->res_count++;
      if(rowlimit && query->res_count >= rowlimit)
        break;
    }

    /* Finally, convert the query type. */
    query->qtype = WG_QTYPE_PREFETCH;
  }

  return query;
}

/** Create a query object and pre-fetch all data rows.
 *
 * Allocates enough space to hold all row offsets, fetches them and stores
 * them in an array. Isolation is not guaranteed in any way, shape or form,
 * but can be implemented on top by the user.
 *
 * returns NULL if constructing the query fails. Otherwise returns a pointer
 * to a wg_query object.
 */
wg_query *wg_make_query(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc) {

  return internal_build_query(db,
    matchrec, reclen, arglist, argc, QUERY_FLAGS_PREFETCH, 0);
}

/** Create a query object and pre-fetch rowlimit number of rows.
 *
 * returns NULL if constructing the query fails. Otherwise returns a pointer
 * to a wg_query object.
 */
wg_query *wg_make_query_rc(void *db, void *matchrec, gint reclen,
  wg_query_arg *arglist, gint argc, wg_uint rowlimit) {

  return internal_build_query(db,
    matchrec, reclen, arglist, argc, QUERY_FLAGS_PREFETCH, rowlimit);
}


/** Return next record from the query object
 *  returns NULL if no more records
 */
void *wg_fetch(void *db, wg_query *query) {
  void *rec;

#ifdef CHECK
  if (!dbcheck(db)) {
    /* XXX: currently show_query_error would work too */
#ifdef WG_NO_ERRPRINT
#else
    fprintf(stderr, "Invalid database pointer in wg_fetch.\n");
#endif
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
    if(query->curr_page) {
      query_result_page *currpage = (query_result_page *) query->curr_page;
      gint offset = currpage->rows[query->curr_pidx++];
      if(!offset) {
        /* page not filled completely */
        query->curr_page = NULL;
        return NULL;
      } else {
        if(query->curr_pidx >= QUERY_RESULTSET_PAGESIZE) {
          query->curr_page = (void *) (currpage->next);
          query->curr_pidx = 0;
        }
      }
      return offsettoptr(db, offset);
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
  if(query->qtype==WG_QTYPE_PREFETCH && query->mpool)
    wg_free_mpool(db, query->mpool);
  free(query);
}

/* ----------- query parameter preparing functions -------------*/

/* Types that use no storage are encoded
 * using standard API functions.
 */

gint wg_encode_query_param_null(void *db, char *data) {
  return wg_encode_null(db, data);
}

gint wg_encode_query_param_record(void *db, void *data) {
  return wg_encode_record(db, data);
}

gint wg_encode_query_param_char(void *db, char data) {
  return wg_encode_char(db, data);
}

gint wg_encode_query_param_fixpoint(void *db, double data) {
  return wg_encode_fixpoint(db, data);
}

gint wg_encode_query_param_date(void *db, int data) {
  return wg_encode_date(db, data);
}

gint wg_encode_query_param_time(void *db, int data) {
  return wg_encode_time(db, data);
}

gint wg_encode_query_param_var(void *db, gint data) {
  return wg_encode_var(db, data);
}

/* Types using storage are encoded by emulating the behaviour
 * of dbdata.c functions. Some assumptions are made about storage
 * size of the data (but similar assumptions exist in dbdata.c)
 */

gint wg_encode_query_param_int(void *db, gint data) {
  void *dptr;

  if(fits_smallint(data)) {
    return encode_smallint(data);
  } else {
    dptr=malloc(sizeof(gint));
    if(!dptr) {
      show_query_error(db, "Failed to encode query parameter");
      return WG_ILLEGAL;
    }
    *((gint *) dptr) = data;
    return encode_fullint_offset(ptrtooffset(db, dptr));
  }
}

gint wg_encode_query_param_double(void *db, double data) {
  void *dptr;

  dptr=malloc(2*sizeof(gint));
  if(!dptr) {
    show_query_error(db, "Failed to encode query parameter");
    return WG_ILLEGAL;
  }
  *((double *) dptr) = data;
  return encode_fulldouble_offset(ptrtooffset(db, dptr));
}

gint wg_encode_query_param_str(void *db, char *data, char *lang) {
  if(data) {
    return encode_query_param_unistr(db, data, WG_STRTYPE, lang, strlen(data));
  } else {
    show_query_error(db, "NULL pointer given as parameter");
    return WG_ILLEGAL;
  }
}

gint wg_encode_query_param_xmlliteral(void *db, char *data, char *xsdtype) {
  if(data) {
    return encode_query_param_unistr(db, data, WG_XMLLITERALTYPE,
      xsdtype, strlen(data));
  } else {
    show_query_error(db, "NULL pointer given as parameter");
    return WG_ILLEGAL;
  }
}

gint wg_encode_query_param_uri(void *db, char *data, char *prefix) {
  if(data) {
    return encode_query_param_unistr(db, data, WG_URITYPE,
      prefix, strlen(data));
  } else {
    show_query_error(db, "NULL pointer given as parameter");
    return WG_ILLEGAL;
  }
}

/* Encode shortstr- or longstr-compatible data in local memory.
 * string type without lang is handled as "short", ignoring the
 * actual length. All other types require longstr storage to
 * handle the extdata field.
 */
static gint encode_query_param_unistr(void *db, char *data, gint type,
  char *extdata, int length) {

  void *dptr;
  if(type == WG_STRTYPE && extdata == NULL) {
    dptr=malloc(length+1);
    if(!dptr) {
      show_query_error(db, "Failed to encode query parameter");
      return WG_ILLEGAL;
    }
    memcpy((char *) dptr, data, length);
    ((char *) dptr)[length] = '\0';
    return encode_shortstr_offset(ptrtooffset(db, dptr));
  }
  else {
    size_t i;
    int extlen = 0;
    int dlen, lengints, lenrest;
    gint offset, meta;

    if(type != WG_BLOBTYPE)
      length++; /* include the terminating 0 */

    /* Determine storage size */
    lengints = length / sizeof(gint);
    lenrest = length % sizeof(gint);
    if(lenrest) lengints++;
    dlen = sizeof(gint) * (LONGSTR_HEADER_GINTS + lengints);

    /* Emulate the behaviour of wg_alloc_gints() */
    if(dlen < MIN_VARLENOBJ_SIZE) dlen = MIN_VARLENOBJ_SIZE;
    if(dlen % 8) dlen += 4;

    if(extdata) {
      extlen = strlen(extdata);
    }

    dptr=malloc(dlen + (extdata ? extlen + 1 : 0));
    if(!dptr) {
      show_query_error(db, "Failed to encode query parameter");
      return WG_ILLEGAL;
    }
    offset = ptrtooffset(db, dptr);

    /* Copy the data, fill the remainder with zeroes */
    memcpy((char *) dptr + (LONGSTR_HEADER_GINTS*sizeof(gint)), data, length);
    for(i=0; lenrest && i<sizeof(gint)-lenrest; i++) {
      *((char *)dptr + length + (LONGSTR_HEADER_GINTS*sizeof(gint)) + i) = '\0';
    }

    /* Use the rest of the allocated storage to encode extdata in
     * shortstr format.
     */
    if(extdata) {
      gint extenc;
      void *extptr = (char *) dptr + dlen;
      memcpy(extptr, extdata, extlen);
      ((char *) extptr)[extlen] = '\0';
      extenc = encode_shortstr_offset(ptrtooffset(db, extptr));
      dbstore(db, offset+LONGSTR_EXTRASTR_POS*sizeof(gint), extenc);
    } else {
      dbstore(db, offset+LONGSTR_EXTRASTR_POS*sizeof(gint), 0);
    }

    /* Metadata */
    dbstore(db, offset, dlen); /* Local memory, actual value OK here */
    meta = (dlen - length) << LONGSTR_META_LENDIFSHFT;
    meta = meta | type;
    dbstore(db, offset+LONGSTR_META_POS*sizeof(gint), meta);
    dbstore(db, offset+LONGSTR_REFCOUNT_POS*sizeof(gint), 0);
    dbstore(db, offset+LONGSTR_BACKLINKS_POS*sizeof(gint), 0);
    dbstore(db, offset+LONGSTR_HASHCHAIN_POS*sizeof(gint), 0);

    return encode_longstr_offset(offset);
  }
}

gint wg_free_query_param(void* db, gint data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_query_error(db,"wrong database pointer given to wg_free_query_param");
    return 0;
  }
#endif
  if (isptr(data)) {
    gint offset;

    switch(data&NORMALPTRMASK) {
      case DATARECBITS:
        break;
      case SHORTSTRBITS:
        offset = decode_shortstr_offset(data);
        free(offsettoptr(db, offset));
        break;
      case LONGSTRBITS:
        offset = decode_longstr_offset(data);
        free(offsettoptr(db, offset));
        break;
      case FULLDOUBLEBITS:
        offset = decode_fulldouble_offset(data);
        free(offsettoptr(db, offset));
        break;
      case FULLINTBITSV0:
      case FULLINTBITSV1:
        offset = decode_fullint_offset(data);
        free(offsettoptr(db, offset));
        break;
      default:
        show_query_error(db,"Bad encoded value given to wg_free_query_param");
        break;
    }
  }
  return 0;
}

/* ------------------ Resultset manipulation -------------------*/

/* XXX: consider converting the main query function to use this as well.
 * Currently only used to support the JSON/document query.
 */

/*
 * Allocate and initialize a new result set.
 */
static query_result_set *create_resultset(void *db) {
  query_result_set *set;

  if(!(set = malloc(sizeof(query_result_set)))) {
    show_query_error(db, "Failed to allocate result set");
    return NULL;
  }

  set->rcursor.page = NULL;                 /* initialize as empty */
  set->rcursor.pidx = 0;
  set->wcursor.page = NULL;
  set->wcursor.pidx = QUERY_RESULTSET_PAGESIZE; /* new page needed */
  set->first_page = NULL;
  set->res_count = 0;

  set->mpool = wg_create_mpool(db, sizeof(query_result_page));
  if(!set->mpool) {
    show_query_error(db, "Failed to allocate result memory pool");
    free(set);
    return NULL;
  }
  return set;
}

/*
 * Free the resultset and it's memory pool
 */
static void free_resultset(void *db, query_result_set *set) {
  if(set->mpool)
    wg_free_mpool(db, set->mpool);
  free(set);
}

/*
 * Set the resultset pointers to the beginning of the
 * first results page.
 */
static void rewind_resultset(void *db, query_result_set *set) {
  set->rcursor.page = set->first_page;
  set->rcursor.pidx = 0;
}

/*
 * Append an offset to the result set.
 * returns 0 on success.
 * returns -1 on error.
 */
static gint append_resultset(void *db, query_result_set *set, gint offset) {
  if(set->wcursor.pidx >= QUERY_RESULTSET_PAGESIZE) {
    query_result_page *newpage = (query_result_page *) \
        wg_alloc_mpool(db, set->mpool, sizeof(query_result_page));
    if(!newpage) {
      return show_query_error(db, "Failed to allocate a resultset page");
    }

    memset(newpage->rows, 0, sizeof(gint) * QUERY_RESULTSET_PAGESIZE);
    newpage->next = NULL;

    if(set->wcursor.page) {
      set->wcursor.page->next = newpage;
    } else {
      /* first_page==NULL implied */
      set->first_page = newpage;
      set->rcursor.page = newpage;
    }
    set->wcursor.page = newpage;
    set->wcursor.pidx = 0;
  }

  set->wcursor.page->rows[set->wcursor.pidx++] = offset;
  set->res_count++;
  return 0;
}

/*
 * Fetch the next offset from the result set.
 * returns 0 if the set is exhausted.
 */
static gint fetch_resultset(void *db, query_result_set *set) {
  if(set->rcursor.page) {
    gint offset = set->rcursor.page->rows[set->rcursor.pidx++];
    if(!offset) {
      /* page not filled completely. Mark set as exhausted. */
      set->rcursor.page = NULL;
    } else {
      if(set->rcursor.pidx >= QUERY_RESULTSET_PAGESIZE) {
        set->rcursor.page = set->rcursor.page->next;
        set->rcursor.pidx = 0;
      }
    }
    return offset;
  }
  return 0;
}

#define NESTEDLOOP 0
#define HASHJOIN 1

/*
 * Create an intersection of two result sets.
 * Join strategy:
 *   if the number of inner loops expected is low (i.e. the sets
 *   are small), nested loop join is used. Otherwise, hash join
 *   is used.
 *
 * Returns a new result set (can be empty).
 * Returns NULL on error.
 */
static query_result_set *intersect_resultset(void *db,
  query_result_set *seta, query_result_set *setb)
{
  query_result_set *intersection;
  int strategy = HASHJOIN;

  if(!(intersection = create_resultset(db))) {
    return NULL;
  }
  if(seta->res_count * setb->res_count < 200) {
    strategy = NESTEDLOOP; /* don't bother with hash table */
  }

  if(strategy == HASHJOIN) {
    void *hasht = NULL;
    gint offset;

    if(seta->res_count > setb->res_count) {
      query_result_set *tmp = seta;
      seta = setb;
      setb = tmp;
    }

    if(!(hasht = wg_dhash_init(db, seta->res_count))) {
      free_resultset(db, intersection);
      return NULL;
    }

    rewind_resultset(db, seta);
    while((offset = fetch_resultset(db, seta))) {
      if(wg_dhash_addkey(db, hasht, offset)) {
        free_resultset(db, intersection);
        wg_dhash_free(db, hasht);
        return NULL;
      }
    }
    rewind_resultset(db, setb);
    while((offset = fetch_resultset(db, setb))) {
      if(wg_dhash_haskey(db, hasht, offset)) {
        gint err = append_resultset(db, intersection, offset);
        if(err) {
          free_resultset(db, intersection);
          wg_dhash_free(db, hasht);
          return NULL;
        }
      }
    }
    wg_dhash_free(db, hasht);
  }
  else { /* nested loop strategy */
    gint offseta;
    rewind_resultset(db, seta);
    while((offseta = fetch_resultset(db, seta))) {
      gint offsetb;
      rewind_resultset(db, setb);
      while((offsetb = fetch_resultset(db, setb))) {
        if(offseta == offsetb) {
          gint err = append_resultset(db, intersection, offseta);
          if(err) {
            free_resultset(db, intersection);
            return NULL;
          }
          break;
        }
      }
    }
  }
  return intersection;
}

/*
 * Create a result set that contains only unique rows.
 * Uniqueness test uses similar strategy to the intersect function
 * (hash table for membership test, but revert to nested loop if
 * low number of elements).
 *
 * Returns a new result set (can be empty).
 * Returns NULL on error.
 */
static query_result_set *unique_resultset(void *db, query_result_set *set)
{
  gint offset;
  query_result_set *unique;
  int strategy = HASHJOIN;

  if(!(unique = create_resultset(db))) {
    return NULL;
  }
  if(set->res_count < 20) {
    strategy = NESTEDLOOP; /* don't bother with hash table */
  }

  rewind_resultset(db, set);

  if(strategy == HASHJOIN) {
    void *hasht = NULL;
    if(!(hasht = wg_dhash_init(db, set->res_count))) {
      free_resultset(db, unique);
      return NULL;
    }

    while((offset = fetch_resultset(db, set))) {
      if(!wg_dhash_haskey(db, hasht, offset)) {
        gint err = append_resultset(db, unique, offset);
        if(!err) {
          err = wg_dhash_addkey(db, hasht, offset);
        }
        if(err) {
          free_resultset(db, unique);
          wg_dhash_free(db, hasht);
          return NULL;
        }
      }
    }
    wg_dhash_free(db, hasht);
  }
  else { /* nested loop */
    while((offset = fetch_resultset(db, set))) {
      gint offsetu, found = 0;
      rewind_resultset(db, unique);
      while((offsetu = fetch_resultset(db, unique))) {
        if(offset == offsetu) {
          found = 1;
          break;
        }
      }
      if(!found) {
        /* We're now at the end of the set and may append normally. */
        gint err = append_resultset(db, unique, offset);
        if(err) {
          free_resultset(db, unique);
          return NULL;
        }
      }
    }
  }
  return unique;
}

/* ------------------- (JSON) document query -------------------*/

/* Note the non-conventional return code values:
 * -1 adding the document failed
 * 1 adding the document succeeded
 * (0 is reserved for using this macro in a recursive function
 * to differentiate between matches and non-matches)
 */
#define ADD_DOC_TO_RESULTSET(db, rec, ns, rc) \
  void *doc = wg_find_document(db, rec); \
  if(doc) { \
    if(!append_resultset(db, ns, ptrtooffset(db, doc))) \
      rc = 1; \
    else \
      rc = -1; \
  } else { \
    rc = show_query_error(db, "Failed to retrieve the document"); \
  }

#define IF_ERR_CLEAN_UP(db, cr, ns, al, rc) \
  if(rc < 0) { \
    free_resultset(db, ns); \
    if(cr) \
      free_resultset(db, cr); \
    if(al) \
      free(al); \
    return NULL; \
  }

#define ARGLIST_CLEANUP(al) \
  if(al) \
    free(al);

#define ADD_DOC_ARRAY_UNWRAP(db, rec, ns, rc, k, v) \
  void *arec = wg_decode_record(db, k); \
  if(is_schema_array(arec)) { \
    gint areclen = wg_get_record_len(db, arec); \
    int j; \
    for(j=0; j<areclen; j++) { \
      if(WG_COMPARE(db, wg_get_field(db, arec, j), v) == WG_EQUAL) { \
        ADD_DOC_TO_RESULTSET(db, rec, ns, rc) \
        break; \
      } \
    } \
  }

/*
 * Check if a record matches a key-value pair given in a query
 * clause. If the value is an array in the record, each member
 * of the array is compared to the value in the clause.
 * (this behaviour emulates the JSON hash index, but can be disabled
 * by #undef-ing JSON_SCAN_UNWRAP_ARRAY).
 *
 * returns 1 if the record matches and is added to the resultset
 * returns 0 if the record does not match
 * returns -1 if the record matches, but adding fails
 */
static gint check_and_merge_by_kv(void *db, void *rec,
  wg_json_query_arg *arg, query_result_set *next_set)
{
  gint rc = 0;
  gint reclen = wg_get_record_len(db, rec);
  if(reclen > WG_SCHEMA_VALUE_OFFSET) { /* XXX: assume key
                                         * before value */
#ifndef JSON_SCAN_UNWRAP_ARRAY
    if(WG_COMPARE(db, wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET),
      arg->key) == WG_EQUAL &&\
      WG_COMPARE(db, wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET),
      arg->value) == WG_EQUAL)
    {
      ADD_DOC_TO_RESULTSET(db, rec, next_set, rc)
    }
#else
    if(WG_COMPARE(db, wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET),
      arg->key) == WG_EQUAL) {
      gint k = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);

      if(WG_COMPARE(db, k, arg->value) == WG_EQUAL) {
        /* Direct match. */
        ADD_DOC_TO_RESULTSET(db, rec, next_set, rc)
      } else if(wg_get_encoded_type(db, k) == WG_RECORDTYPE) {
        /* No direct match, but if it is a record AND an array,
         * scan the array contents.
         */
        ADD_DOC_ARRAY_UNWRAP(db, rec, next_set, rc, k, arg->value)
      }
    }
#endif
  }
  return rc;
}

/*
 * Like check_and_merge_by_kv() except key comparison is skipped
 * (i.e. the caller is iterating over key index)
 */
static gint check_and_merge_by_key(void *db, void *rec,
  wg_json_query_arg *arg, query_result_set *next_set)
{
  gint rc = 0;
  gint reclen = wg_get_record_len(db, rec);
  if(reclen > WG_SCHEMA_VALUE_OFFSET) {
#ifndef JSON_SCAN_UNWRAP_ARRAY
    if(WG_COMPARE(db, wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET),
      arg->value) == WG_EQUAL)
    {
      ADD_DOC_TO_RESULTSET(db, rec, next_set, rc)
    }
#else
    gint k = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);

    if(WG_COMPARE(db, k, arg->value) == WG_EQUAL) {
      ADD_DOC_TO_RESULTSET(db, rec, next_set, rc)
    } else if(wg_get_encoded_type(db, k) == WG_RECORDTYPE) {
      ADD_DOC_ARRAY_UNWRAP(db, rec, next_set, rc, k, arg->value)
    }
#endif
  }
  return rc;
}

/*
 * Check if the record or any of its children matches
 * the given key/value pair. The search is stopped upon
 * the first match.
 *
 * returns 1 if the record matches and is added to the resultset
 * returns 0 if the record does not match
 * returns -1 if the record matches, but adding fails
 */
static gint check_and_merge_recursively(void *db, void *rec,
  wg_json_query_arg *arg, query_result_set *next_set, int depth)
{
  gint i, reclen, rc;
  rc = check_and_merge_by_kv(db, rec, arg, next_set);
  if(rc) /* successful match or an error */
    return rc;

  if(depth <= 0) {
    return show_query_error(db, "scanning document: recursion too deep");
  }
  reclen = wg_get_record_len(db, rec);
  for(i=0; i<reclen; i++) {
    gint enc = wg_get_field(db, rec, i);
    gint type = wg_get_encoded_type(db, enc);
    if(type == WG_RECORDTYPE) {
      rc = check_and_merge_recursively(db, wg_decode_record(db, enc),
        arg, next_set, depth-1);
      if(rc) /* successful match or an error */
        return rc;
    }
  }
  return 0; /* no match */
}

/* Prepare argument list. This sorts clauses that are either less
 * costly to query or restrict the following processing the most
 * (not yet implemented, depends on statistics). Also determines
 * which indexes can and should be used.
 *
 * Returns 0 on success.
 * Returns -1 on error.
 * in case of error, the contents of return parameters are unmodified.
 * in case of success, **sorted_arglist may be set to NULL, if the
 * argument list does not require sorting. The caller should always
 * check that.
 */
static gint prepare_json_arglist(void *db, wg_json_query_arg *arglist,
  wg_json_query_arg **sorted_arglist, gint argc,
  gint *index_id, gint *vindex_id, gint *kindex_id)
{
  gint icols[2], need_ttree = 0;
  wg_json_query_arg *tmp = NULL;

  /* Get index */
  icols[0] = WG_SCHEMA_KEY_OFFSET;
  icols[1] = WG_SCHEMA_VALUE_OFFSET;
  *index_id = wg_multi_column_to_index_id(db, icols, 2,
    WG_INDEX_TYPE_HASH_JSON, NULL, 0);
  *vindex_id = *kindex_id = -1;

  if(argc > 1) {
    /* There is something to sort. In the future we can also sort by
     * cardinality here (provided that stats are available). */
    gint i, j;
    tmp = malloc(sizeof(wg_json_query_arg) * argc);
    if(!tmp) {
      return show_query_error(db, "Failed to prepare query arguments");
    }

    /* First pass: literal values only */
    for(i=0, j=0; i<argc; i++) {
      if(wg_get_encoded_type(db, arglist[i].value) != WG_RECORDTYPE) {
        tmp[j].key = arglist[i].key;
        tmp[j++].value = arglist[i].value;
      }
    }

    /* Was there something left? In that case, we might need T-tree
     * to speed up scanning for the remainder of clauses. We also use
     * T-tree if hash is not available at all.
     */
    if(j<i) {
      need_ttree = 1;
    }

    /* Second pass: complex structures only */
    for(i=0; i<argc; i++) {
      if(wg_get_encoded_type(db, arglist[i].value) == WG_RECORDTYPE) {
        tmp[j].key = arglist[i].key;
        tmp[j++].value = arglist[i].value;
      }
    }
  } else {
    /* Complex structures are not present in the hash index */
    if(wg_get_encoded_type(db, arglist[0].value) == WG_RECORDTYPE) {
      need_ttree = 1;
    }
  }

  /* Get T-tree index if needed. Value index is preferred, but
   * it must be of the type that supports array unwrap. Otherwise
   * we'll settle for a key index.
   */
  if(*index_id == -1 || need_ttree) {
    *vindex_id = wg_multi_column_to_index_id(db, &icols[1], 1,
      WG_INDEX_TYPE_TTREE_JSON, NULL, 0);
    if(*vindex_id == -1) {
      *kindex_id = wg_multi_column_to_index_id(db, &icols[0], 1,
        WG_INDEX_TYPE_TTREE, NULL, 0);
    }
  }

  *sorted_arglist = tmp;
  return 0;
}

/*
 * Find a list of documents that contain the key-value pairs.
 * Returns a prefetch query object.
 * Returns NULL on error.
 */
wg_query *wg_make_json_query(void *db, wg_json_query_arg *arglist, gint argc) {
  wg_query *query = NULL;
  query_result_set *curr_res = NULL;
  wg_json_query_arg *sorted_arglist = NULL;
  gint index_id = -1, vindex_id = -1, kindex_id = -1;
  gint i;

#ifdef CHECK
  if(!arglist || argc < 1) {
    show_query_error(db, "Not enough parameters");
    return NULL;
  }
  if (!dbcheck(db)) {
#ifdef WG_NO_ERRPRINT
#else
    fprintf(stderr, "Invalid database pointer in wg_make_json_query.\n");
#endif
    return NULL;
  }
#endif

  /* Sort the argument list. This also checks for usable indexes, so
   * we're calling it even if we have just one argument.
   */
  prepare_json_arglist(db, arglist, &sorted_arglist, argc,
    &index_id, &vindex_id, &kindex_id);
  /* HACK: this way, the following code does not need to care
   * whether we sorted the argument list or not.
   */
  if(sorted_arglist)
    arglist = sorted_arglist;

  /* Iterate over the argument pairs.
   * XXX: it is possible that getting the first set from index and
   * doing a scan to check the remaining arguments is faster than
   * doing the intersect operation of sets retrieved from index.
   */
  for(i=0; i<argc; i++) {
    query_result_set *next_set, *tmp_set;

    /* Initialize the set produced by this iteration */
    next_set = create_resultset(db);
    if(!next_set) {
      if(curr_res)
        free_resultset(db, curr_res);
      return NULL;
    }

    if(index_id > 0 &&\
      wg_get_encoded_type(db, arglist[i].value) != WG_RECORDTYPE) {
      /* Fetch the matching rows from the index, then retrieve the
       * documents they belong to.
       */
      gint values[2];
      gint reclist_offset;

      values[0] = arglist[i].key;
      values[1] = arglist[i].value;
      reclist_offset = wg_search_hash(db, index_id, values, 2);

      if(reclist_offset > 0) {
        gint *nextoffset = &reclist_offset;
        while(*nextoffset) {
          gcell *rec_cell = (gcell *) offsettoptr(db, *nextoffset);
          gint rc = -1;
          ADD_DOC_TO_RESULTSET(db, offsettoptr(db, rec_cell->car),
            next_set, rc)
          IF_ERR_CLEAN_UP(db, curr_res, next_set, sorted_arglist, rc)
          nextoffset = &(rec_cell->cdr);
        }
      }
    }
#if 0
    else if(vindex_id > 0) {
      /* XXX: unimplemented: scan T-tree for values */
    }
#endif
    else if(kindex_id > 0) {
      /* Hash index not usable, do a scan but leverage an index on the
       * key field to reduce the number of records visited.
       */
      gint curr_offset = 0, curr_slot = -1, end_offset = 0, end_slot = -1;

      if(find_ttree_bounds(db, kindex_id, WG_SCHEMA_KEY_OFFSET,
          arglist[i].key, arglist[i].key, 1, 1,
          &curr_offset, &curr_slot, &end_offset, &end_slot)) {
        curr_offset = 0;
      }

      while(curr_offset) {
        gint rc;
        struct wg_tnode *node = (struct wg_tnode *) offsettoptr(db, curr_offset);
        void *rec = offsettoptr(db, node->array_of_values[curr_slot]);

        rc = check_and_merge_by_key(db, rec, &arglist[i], next_set);
        IF_ERR_CLEAN_UP(db, curr_res, next_set, sorted_arglist, rc)

        if(curr_offset==end_offset && curr_slot==end_slot) {
          break;
        } else {
          curr_slot += 1; /* direction implied as 1 */
          if(curr_slot >= node->number_of_elements) {
#ifdef CHECK
            if(end_offset==curr_offset) {
              show_query_error(db, "Warning: end slot mismatch, possible bug");
              break;
            } else {
#endif
              curr_offset = TNODE_SUCCESSOR(db, node);
              curr_slot = 0;
#ifdef CHECK
            }
#endif
          }
        }
      }
    }
    else if(curr_res) {
      /* No index, do a scan over the current resultset. This also happens if
       * the value is a complex structure.
       */
      gint offset;
      rewind_resultset(db, curr_res);
      while((offset = fetch_resultset(db, curr_res))) {
        gint *rec = offsettoptr(db, offset);
#ifndef USE_BACKLINKING
        gint rc = check_and_merge_recursively(db,
          rec, &arglist[i], next_set, 99);
#else
        gint rc = check_and_merge_recursively(db,
          rec, &arglist[i], next_set, WG_COMPARE_REC_DEPTH);
#endif
        IF_ERR_CLEAN_UP(db, curr_res, next_set, sorted_arglist, rc)
      }
      /* Skip merge in this iteration, next_set is a subset of curr_res */
      free_resultset(db, curr_res);
      curr_res = NULL;
    }
    else {
      /* No index and no intermediate result to use, full
       * scan of database required.
       */
      gint *rec = wg_get_first_record(db);
      while(rec) {
        gint rc = check_and_merge_by_kv(db, rec, &arglist[i], next_set);
        IF_ERR_CLEAN_UP(db, curr_res, next_set, sorted_arglist, rc)
        rec = wg_get_next_record(db, rec);
      }
    }

    /* Delete duplicate documents */
    tmp_set = unique_resultset(db, next_set);
    free_resultset(db, next_set);
    if(!tmp_set) {
      if(curr_res)
        free_resultset(db, curr_res);
      ARGLIST_CLEANUP(sorted_arglist)
      return NULL;
    } else {
      next_set = tmp_set;
    }

    /* Update the query result */
    if(curr_res) {
      /* Working resultset exists, create an intersection */
      tmp_set = intersect_resultset(db, curr_res, next_set);
      free_resultset(db, curr_res);
      free_resultset(db, next_set);
      if(!tmp_set) {
        ARGLIST_CLEANUP(sorted_arglist)
        return NULL;
      } else {
        curr_res = tmp_set;
      }
    } else {
      /* This set becomes the working resultset */
      curr_res = next_set;
    }
  }
  ARGLIST_CLEANUP(sorted_arglist)

  /* Initialize query object */
  query = (wg_query *) malloc(sizeof(wg_query));
  if(!query) {
    free_resultset(db, curr_res);
    show_query_error(db, "Failed to allocate memory");
    return NULL;
  }
  query->qtype = WG_QTYPE_PREFETCH;
  query->arglist = NULL;
  query->argc = 0;
  query->column = -1;

  /* Copy the result. */
  query->curr_page = curr_res->first_page;
  query->curr_pidx = 0;
  query->res_count = curr_res->res_count;
  query->mpool = curr_res->mpool;
  free(curr_res); /* contents were inherited, dispose of the struct */

  return query;
}

/* ------------------ simple query functions -------------------*/

void *wg_find_record(void *db, gint fieldnr, gint cond, gint data,
    void* lastrecord) {
  gint index_id = -1;

  /* find index on colum */
  if(cond != WG_COND_NOT_EQUAL) {
    index_id = wg_multi_column_to_index_id(db, &fieldnr, 1,
      WG_INDEX_TYPE_TTREE, NULL, 0);
  }

  if(index_id > 0) {
    int start_inclusive = 1, end_inclusive = 1;
    /* WG_ILLEGAL is interpreted as "no bound" */
    gint start_bound = WG_ILLEGAL;
    gint end_bound = WG_ILLEGAL;
    gint curr_offset = 0, curr_slot = -1, end_offset = 0, end_slot = -1;
    void *prev = NULL;

    switch(cond) {
      case WG_COND_EQUAL:
        start_bound = end_bound = data;
        break;
      case WG_COND_LESSTHAN:
        end_bound = data;
        end_inclusive = 0;
        break;
      case WG_COND_GREATER:
        start_bound = data;
        start_inclusive = 0;
        break;
      case WG_COND_LTEQUAL:
        end_bound = data;
        break;
      case WG_COND_GTEQUAL:
        start_bound = data;
        break;
      default:
        show_query_error(db, "Invalid condition (ignoring)");
        return NULL;
    }

    if(find_ttree_bounds(db, index_id, fieldnr,
        start_bound, end_bound, start_inclusive, end_inclusive,
        &curr_offset, &curr_slot, &end_offset, &end_slot)) {
      return NULL;
    }

    /* We have the bounds, scan to lastrecord */
    while(curr_offset) {
      struct wg_tnode *node = (struct wg_tnode *) offsettoptr(db, curr_offset);
      void *rec = offsettoptr(db, node->array_of_values[curr_slot]);

      if(prev == lastrecord) {
        /* if lastrecord is NULL, first match returned */
        return rec;
      }

      prev = rec;
      if(curr_offset==end_offset && curr_slot==end_slot) {
        /* Last slot reached */
        break;
      } else {
        /* Some rows still left */
        curr_slot += 1; /* direction implied as 1 */
        if(curr_slot >= node->number_of_elements) {
#ifdef CHECK
          if(end_offset==curr_offset) {
            /* This should not happen */
            show_query_error(db, "Warning: end slot mismatch, possible bug");
            break;
          } else {
#endif
            curr_offset = TNODE_SUCCESSOR(db, node);
            curr_slot = 0;
#ifdef CHECK
          }
#endif
        }
      }
    }
  }
  else {
    /* no index (or cond == WG_COND_NOT_EQUAL), do a scan */
    wg_query_arg arg;
    void *rec;

    if(lastrecord) {
      rec = wg_get_next_record(db, lastrecord);
    } else {
      rec = wg_get_first_record(db);
    }

    arg.column = fieldnr;
    arg.cond = cond;
    arg.value = data;

    while(rec) {
      if(check_arglist(db, rec, &arg, 1)) {
        return rec;
      }
      rec = wg_get_next_record(db, rec);
    }
  }

  /* No records found (this can also happen if matching records were
   * found but lastrecord does not match any of them or matches the
   * very last one).
   */
  return NULL;
}

/*
 * Wrapper function for wg_find_record with unencoded data (null)
 */
void *wg_find_record_null(void *db, gint fieldnr, gint cond, char *data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_null(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (record)
 */
void *wg_find_record_record(void *db, gint fieldnr, gint cond, void *data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_record(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (char)
 */
void *wg_find_record_char(void *db, gint fieldnr, gint cond, char data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_char(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (fixpoint)
 */
void *wg_find_record_fixpoint(void *db, gint fieldnr, gint cond, double data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_fixpoint(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (date)
 */
void *wg_find_record_date(void *db, gint fieldnr, gint cond, int data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_date(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (time)
 */
void *wg_find_record_time(void *db, gint fieldnr, gint cond, int data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_time(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (var)
 */
void *wg_find_record_var(void *db, gint fieldnr, gint cond, gint data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_var(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (int)
 */
void *wg_find_record_int(void *db, gint fieldnr, gint cond, int data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_int(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  wg_free_query_param(db, enc);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (double)
 */
void *wg_find_record_double(void *db, gint fieldnr, gint cond, double data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_double(db, data);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  wg_free_query_param(db, enc);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (string)
 */
void *wg_find_record_str(void *db, gint fieldnr, gint cond, char *data,
    void* lastrecord) {
  gint enc = wg_encode_query_param_str(db, data, NULL);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  wg_free_query_param(db, enc);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (xmlliteral)
 */
void *wg_find_record_xmlliteral(void *db, gint fieldnr, gint cond, char *data,
    char *xsdtype, void* lastrecord) {
  gint enc = wg_encode_query_param_xmlliteral(db, data, xsdtype);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  wg_free_query_param(db, enc);
  return rec;
}

/*
 * Wrapper function for wg_find_record with unencoded data (uri)
 */
void *wg_find_record_uri(void *db, gint fieldnr, gint cond, char *data,
    char *prefix, void* lastrecord) {
  gint enc = wg_encode_query_param_uri(db, data, prefix);
  void *rec = wg_find_record(db, fieldnr, cond, enc, lastrecord);
  wg_free_query_param(db, enc);
  return rec;
}

/* --------------- error handling ------------------------------*/

/** called with err msg
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_query_error(void* db, char* errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"query error: %s\n",errmsg);
#endif
  return -1;
}

#if 0
/** called with err msg and additional int data
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_query_error_nr(void* db, char* errmsg, gint nr) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"query error: %s %d\n",errmsg,nr);
#endif
  return -1;
}
#endif

#ifdef __cplusplus
}
#endif
