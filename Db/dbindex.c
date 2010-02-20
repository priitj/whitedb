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

 /** @file dbindex.c
 *  Implementation of T-tree index
 */

/* ====== Includes =============== */

#include <stdio.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "dbdata.h"
#include "dbindex.h"
#include "dbcompare.h"


/* ====== Private defs =========== */

#define LL_CASE 0
#define LR_CASE 1
#define RL_CASE 2
#define RR_CASE 3

#ifndef max
#define max(a,b) (a>b ? a : b)
#endif


/* ======= Private protos ================ */

#ifndef TTREE_SINGLE_COMPARE
static wg_int db_find_bounding_tnode(void *db, wg_int rootoffset, wg_int key,
  wg_int *result, struct wg_tnode *rb_node);
#endif
static int db_which_branch_causes_overweight(void *db, struct wg_tnode *root);
static int db_rotate_ttree(void *db, wg_int index_id, struct wg_tnode *root,
  int overw);
static wg_int ttree_add_row(void *db, wg_int index_id, void *rec);
static wg_int ttree_remove_row(void *db, wg_int index_id, void * rec);

static gint show_index_error(void* db, char* errmsg);
static gint show_index_error_nr(void* db, char* errmsg, gint nr);


/* ====== Functions ============== */

/*
 * Index implementation:
 * - T-Tree, as described by Lehman & Carey '86
 *   This includes search with a single compare per node, enabled by
 *   defining TTREE_SINGLE_COMPARE
 *
 * - improvements loosely based on T* tree (Kim & Choi '96)
 *   Nodes have predecessor and successor pointers. This is turned
 *   on by defining TTREE_CHAINED_NODES. Other alterations described in
 *   the original T* tree paper were not implemented.
 *
 * - hash index (allows multi-column indexes) (not done yet)
 *
 * Index metainfo:
 * data about indexes in system is stored in dbh->index_control_area_header
 *
 *  index_table[]  - 0 - 0 - v - 0 - 0 - v - 0
 *                           |           |
 *      index hdr A <--- list elem    list elem ---> index hdr B
 *            ^             0            v
 *            |                          |
 *            ----------------------- list elem
 *                                       0
 *
 *  index_table is a fixed size array that contains offsets to index
 *  lists by database field (column) number. Index lists themselves contain
 *  offsets to index headers. This arrangement is used so that one
 *  index can be referred to from several fields (index headers are
 *  unique, index list elements are not).
 *
 *  In the above example, A is a (hash) index on columns 2 and 5, while B
 *  is an index on column 5.
 *
 * Note: offset to index header struct is also used as an index id.
 */


/* ------------------- T-tree private functions ------------- */

#ifndef TTREE_SINGLE_COMPARE
/**
*  returns bounding node offset or if no really bounding node exists, then the closest node
*/
static wg_int db_find_bounding_tnode(void *db, wg_int rootoffset, wg_int key,
  wg_int *result, struct wg_tnode *rb_node) {

  struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,rootoffset);

  /* Original tree search algorithm: compares both bounds of
   * the node to determine immediately if the value falls between them.
   */

  if(WG_COMPARE(db, key, node->current_min) == WG_LESSTHAN) { 
    /* if(key < node->current_max) */
    if(node->left_child_offset != 0)
      return db_find_bounding_tnode(db, node->left_child_offset,
        key, result, NULL);
    else {
      *result = DEAD_END_LEFT_NOT_BOUNDING;
      return rootoffset;
    }
  } else if(WG_COMPARE(db, key, node->current_max) != WG_GREATER) {
    *result = REALLY_BOUNDING_NODE;
    return rootoffset;
  }
  else { /* if(key > node->current_max) */
    if(node->right_child_offset != 0)
      return db_find_bounding_tnode(db, node->right_child_offset,
        key, result, NULL);
    else{
      *result = DEAD_END_RIGHT_NOT_BOUNDING;
      return rootoffset;
    }
  }
}
#else
/* "rightmost" node search is the improved tree search described in
 * the original T-tree paper.
 */
#define db_find_bounding_tnode wg_search_ttree_rightmost
#endif

/**
*  returns the description of imbalance - 4 cases possible
*  LL - left child of the left child is overweight
*  LR - right child of the left child is overweight
*  etc
*/
static int db_which_branch_causes_overweight(void *db, struct wg_tnode *root){
  struct wg_tnode *child;
  if(root->left_subtree_height > root->right_subtree_height){
    child = (struct wg_tnode *)offsettoptr(db,root->left_child_offset);
    if(child->left_subtree_height >= child->right_subtree_height)return LL_CASE;
    else return LR_CASE;
  }else{
    child = (struct wg_tnode *)offsettoptr(db,root->right_child_offset);
    if(child->left_subtree_height > child->right_subtree_height)return RL_CASE;
    else return RR_CASE;
  }
}

static int db_rotate_ttree(void *db, wg_int index_id, struct wg_tnode *root, int overw){
  wg_int grandparent = root->parent_offset;
  wg_int initialrootoffset = ptrtooffset(db,root);
  struct wg_tnode *r = NULL;
  struct wg_tnode *g = (struct wg_tnode *)offsettoptr(db,grandparent);
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);
  wg_int column = hdr->rec_field_index[0]; /* always one column for T-tree */

  if(overw == LL_CASE){

/*                       A                          B
*                     B     C                    D     A
*                   D  E             ->        N     E  C 
*                  N
*/
    //printf("LL_CASE\n");
    //save some stuff into variables for later use
    wg_int offset_left_child = root->left_child_offset;
    wg_int offset_right_grandchild = ((struct wg_tnode *)offsettoptr(db,offset_left_child))->right_child_offset;
    wg_int right_grandchild_height = ((struct wg_tnode *)offsettoptr(db,offset_left_child))->right_subtree_height;

    
    //first switch: E goes to A's left child
    root->left_child_offset = offset_right_grandchild;
    root->left_subtree_height = right_grandchild_height;
    if(offset_right_grandchild != 0){
      ((struct wg_tnode *)offsettoptr(db,offset_right_grandchild))->parent_offset = ptrtooffset(db,root);
    }
    //second switch: A goes to B's right child
    ((struct wg_tnode *)offsettoptr(db,offset_left_child)) -> right_child_offset = ptrtooffset(db,root);
    ((struct wg_tnode *)offsettoptr(db,offset_left_child)) -> right_subtree_height = max(root->left_subtree_height,root->right_subtree_height)+1;
    root->parent_offset = offset_left_child;
    //for later grandparent fix
    r = (struct wg_tnode *)offsettoptr(db,offset_left_child);

  }else if(overw == RR_CASE){

/*                       A                          C
*                     B     C                    A     E
*                         D   E         ->     B  D      N 
*                              N
*/
    //printf("RR_CASE\n");
    //save some stuff into variables for later use
    wg_int offset_right_child = root->right_child_offset;
    wg_int offset_left_grandchild = ((struct wg_tnode *)offsettoptr(db,offset_right_child))->left_child_offset;
    wg_int left_grandchild_height = ((struct wg_tnode *)offsettoptr(db,offset_right_child))->left_subtree_height;
    //first switch: D goes to A's right child
    root->right_child_offset = offset_left_grandchild;
    root->right_subtree_height = left_grandchild_height;
    if(offset_left_grandchild != 0){
      ((struct wg_tnode *)offsettoptr(db,offset_left_grandchild))->parent_offset = ptrtooffset(db,root);
    }
    //second switch: A goes to C's left child
    ((struct wg_tnode *)offsettoptr(db,offset_right_child)) -> left_child_offset = ptrtooffset(db,root);
    ((struct wg_tnode *)offsettoptr(db,offset_right_child)) -> left_subtree_height = max(root->right_subtree_height,root->left_subtree_height)+1;
    root->parent_offset = offset_right_child;
    //for later grandparent fix
    r = (struct wg_tnode *)offsettoptr(db,offset_right_child);
      
  }else if(overw == LR_CASE){
/*               A                    E
*             B     C             B       A
*          D    E        ->     D  F    G    C
*             F  G                 N
*             N
*/
    struct wg_tnode *bb, *ee;
    //save some stuff into variables for later use
    wg_int offset_left_child = root->left_child_offset;
    wg_int offset_right_grandchild = ((struct wg_tnode *)offsettoptr(db,offset_left_child))->right_child_offset;
    
    //first swtich: G goes to A's left child
    ee = (struct wg_tnode *)offsettoptr(db,offset_right_grandchild);
    root -> left_child_offset = ee -> right_child_offset;
    root -> left_subtree_height = ee -> right_subtree_height;
    if(ee -> right_child_offset != 0){
      ((struct wg_tnode *)offsettoptr(db,ee->right_child_offset))->parent_offset = ptrtooffset(db, root);
    }
    //second switch: F goes to B's right child
    bb = (struct wg_tnode *)offsettoptr(db,offset_left_child);
    bb -> right_child_offset = ee -> left_child_offset;
    bb -> right_subtree_height = ee -> left_subtree_height;
    if(ee -> left_child_offset != 0){
      ((struct wg_tnode *)offsettoptr(db,ee->left_child_offset))->parent_offset = offset_left_child;
    }
    //third switch: B goes to E's left child
    /* The Lehman/Carey "special" LR rotation - instead of creating
     * an internal node with one element, the values of what will become the
     * left child will be moved over to the parent, thus ensuring the internal
     * node is adequately filled. This is only allowed if E is a leaf.
     */
    if(ee->number_of_elements == 1 && !ee->right_child_offset &&\
      !ee->left_child_offset && bb->number_of_elements == WG_TNODE_ARRAY_SIZE){
      int i;

      /* Create space for elements from B */
      ee->array_of_values[bb->number_of_elements - 1] = ee->array_of_values[0];

      /* All the values moved are smaller than in E */
      for(i=1; i<bb->number_of_elements; i++)
        ee->array_of_values[i-1] = bb->array_of_values[i];
      ee->number_of_elements = bb->number_of_elements;

      /* Examine the new leftmost element to find current_min */
      ee->current_min = wg_get_field(db, (void *)offsettoptr(db,
        ee->array_of_values[0]), column);

      bb -> number_of_elements = 1;
      bb -> current_max = bb -> current_min;
    }
    
    //then switch the nodes 
    ee -> left_child_offset = offset_left_child;
    ee -> left_subtree_height = max(bb->right_subtree_height,bb->left_subtree_height)+1;
    bb -> parent_offset = offset_right_grandchild;
    //fourth switch: A goes to E's right child
    ee -> right_child_offset = ptrtooffset(db, root);
    ee -> right_subtree_height = max(root->right_subtree_height,root->left_subtree_height)+1;
    root -> parent_offset = offset_right_grandchild;
    //for later grandparent fix
    r = ee;

  }else if(overw == RL_CASE){

/*               A                    E
*             C     B             A       B
*                 E   D  ->     C  G    F   D
*               G  F                    N
*                  N
*/
    struct wg_tnode *bb, *ee;
    //save some stuff into variables for later use
    wg_int offset_right_child = root->right_child_offset;
    wg_int offset_left_grandchild = ((struct wg_tnode *)offsettoptr(db,offset_right_child))->left_child_offset;
    
    //first swtich: G goes to A's left child
    ee = (struct wg_tnode *)offsettoptr(db,offset_left_grandchild);
    root -> right_child_offset = ee -> left_child_offset;
    root -> right_subtree_height = ee -> left_subtree_height;
    if(ee -> left_child_offset != 0){
      ((struct wg_tnode *)offsettoptr(db,ee->left_child_offset))->parent_offset = ptrtooffset(db, root);
    }

    //second switch: F goes to B's right child
    bb = (struct wg_tnode *)offsettoptr(db,offset_right_child);
    bb -> left_child_offset = ee -> right_child_offset;
    bb -> left_subtree_height = ee -> right_subtree_height;
    if(ee -> right_child_offset != 0){
      ((struct wg_tnode *)offsettoptr(db,ee->right_child_offset))->parent_offset = offset_right_child;
    }

    //third switch: B goes to E's right child
    /* "special" RL rotation - see comments for LR_CASE */
    if(ee->number_of_elements == 1 && !ee->right_child_offset &&\
      !ee->left_child_offset &&  bb->number_of_elements == WG_TNODE_ARRAY_SIZE){
      int i;

      /* All the values moved are larger than in E */
      for(i=1; i<bb->number_of_elements; i++)
        ee->array_of_values[i] = bb->array_of_values[i-1];
      ee->number_of_elements = bb->number_of_elements;

      /* Examine the new rightmost element to find current_max */
      ee->current_max = wg_get_field(db, (void *)offsettoptr(db,
        ee->array_of_values[ee->number_of_elements - 1]), column);

      /* Remaining B node array element should sit in slot 0 */
      bb->array_of_values[0] = \
        bb->array_of_values[bb->number_of_elements - 1];
      bb -> number_of_elements = 1;
      bb -> current_min = bb -> current_max;
    }

    ee -> right_child_offset = offset_right_child;
    ee -> right_subtree_height = max(bb->right_subtree_height,bb->left_subtree_height)+1;
    bb -> parent_offset = offset_left_grandchild;
    //fourth switch: A goes to E's right child

    ee -> left_child_offset = ptrtooffset(db, root);
    ee -> left_subtree_height = max(root->right_subtree_height,root->left_subtree_height)+1;
    root -> parent_offset = offset_left_grandchild;
    //for later grandparent fix
    r = ee;

  }

  //fix grandparent - regardless of current 'overweight' case
  
  if(grandparent == 0){//'grandparent' is index header data
    r->parent_offset = 0;
    //TODO more error check here
    hdr->offset_root_node = ptrtooffset(db,r);
  }else{//grandparent is usual node
    //printf("change grandparent node\n");
    r -> parent_offset = grandparent;
    if(g->left_child_offset == initialrootoffset){//new subtree must replace the left child of grandparent
      g->left_child_offset = ptrtooffset(db,r);
      g->left_subtree_height = max(r->left_subtree_height,r->right_subtree_height)+1;
    }else{
      g->right_child_offset = ptrtooffset(db,r);
      g->right_subtree_height = max(r->left_subtree_height,r->right_subtree_height)+1;
    }
  }

  return 0;
}

/**  inserts pointer to data row into index tree structure
*
*  returns:
*  0 - on success
*  -1 - if error
*/
static wg_int ttree_add_row(void *db, wg_int index_id, void *rec) {
  wg_int rootoffset, column;
  wg_int newvalue, boundtype, bnodeoffset, new;
  struct wg_tnode *node;
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);
  db_memsegment_header* dbh = (db_memsegment_header*) db;

  rootoffset = hdr->offset_root_node;
#ifdef CHECK
  if(rootoffset == 0){
    printf("index at offset %d does not exist\n",index_id);
    return -1;
  }
#endif
  column = hdr->rec_field_index[0]; /* always one column for T-tree */

  //extract real value from the row (rec)
  newvalue = wg_get_field(db, rec, column);

  //find bounding node for the value
  bnodeoffset = db_find_bounding_tnode(db, rootoffset, newvalue, &boundtype, NULL);
  node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
  new = 0;//save here the offset of newly created tnode - 0 if no node added into the tree
  //if bounding node exists - follow one algorithm, else the other
  if(boundtype == REALLY_BOUNDING_NODE){

    //check if the node has room for a new entry
    if(node->number_of_elements < WG_TNODE_ARRAY_SIZE){
      int i, j;
      wg_int cr;

      /* add array entry and update control data. We keep the
       * array sorted, smallest values left. */
      for(i=0; i<node->number_of_elements; i++) {
        /* The node is small enough for naive scans to be
         * "good enough" inside the node. Note that we
         * branch into re-sort loop as early as possible
         * with >= operator (> would be algorithmically correct too)
         * since here the compare is more expensive than the slot
         * copying.
         */
        cr = WG_COMPARE(db, wg_get_field(db,
          (void *)offsettoptr(db,node->array_of_values[i]), column),
          newvalue);

        if(cr != WG_LESSTHAN) { /* value >= newvalue */
          /* Push remaining values to the right */
          for(j=node->number_of_elements; j>i; j--)
            node->array_of_values[j] = node->array_of_values[j-1];
          break;
        }
      }
      /* i is either number_of_elements or a vacated slot
       * in the array now. */
      node->array_of_values[i] = ptrtooffset(db,rec);
      node->number_of_elements++;

      /* Update min. Due to the >= comparison max is preserved
       * in this case. Note that we are overwriting values that
       * WG_COMPARE() may deem equal. This is intentional, because other
       * parts of T-tree algorithm rely on encoded values of min/max fields
       * to be in sync with the leftmost/rightmost slots.
       */
      if(i==0) {
        node->current_min = newvalue;
      }
    }
    else{
      //still, insert the value here, but move minimum out of this node
      //get the minimum element from this node
      int i, j;
      wg_int cr, minvalue, minvaluerowoffset;

      minvalue = node->current_min;
      minvaluerowoffset = node->array_of_values[0];

      /* Now scan for the matching slot. However, since
       * we already know the 0 slot will be re-filled, we
       * do this scan (and sort) in reverse order, compared to the case
       * where array had some space left. */
      for(i=WG_TNODE_ARRAY_SIZE-1; i>0; i--) {
        cr = WG_COMPARE(db, wg_get_field(db,
          (void *)offsettoptr(db,node->array_of_values[i]), column),
          newvalue);
        if(cr != WG_GREATER) { /* value <= newvalue */
          /* Push remaining values to the left */
          for(j=0; j<i; j++)
            node->array_of_values[j] = node->array_of_values[j+1];
          break;
        }
      }
      /* i is either 0 or a freshly vacated slot */
      node->array_of_values[i] = ptrtooffset(db,rec);

      /* Update minimum. Thanks to the sorted array, we know for a fact
       * that the minimum sits in slot 0. */
      if(i==0) {
        node->current_min = newvalue;
      } else {
        node->current_min = wg_get_field(db,
          (void *)offsettoptr(db,node->array_of_values[0]), column);
        /* The scan for the free slot starts from the right and
         * tries to exit as fast as possible. So it's possible that
         * the rightmost slot was changed.
         */
        if(i == WG_TNODE_ARRAY_SIZE-1) {
          node->current_max = newvalue;
        }
      }

      //proceed to the node that holds greatest lower bound - must be leaf (can be the initial bounding node)
      if(node->left_child_offset != 0){
#ifndef TTREE_CHAINED_NODES
        wg_int greatestlb = wg_ttree_find_glb_node(db,node->left_child_offset);
#else
        wg_int greatestlb = node->pred_offset;
#endif
        node = (struct wg_tnode *)offsettoptr(db, greatestlb);  
      }
      //if the greatest lower bound node has room, insert value
      //otherwise make the new node as right child and put the value there
      if(node->number_of_elements < WG_TNODE_ARRAY_SIZE){
        //add array entry and update control data
        node->array_of_values[node->number_of_elements] = minvaluerowoffset;//save offset, use first free slot
        node->number_of_elements++;
        node->current_max = minvalue;  
        
      }else{
        //create, initialize and save first value
        struct wg_tnode *leaf;
        gint newnode = wg_alloc_fixlen_object(db, &dbh->tnode_area_header);
        if(newnode == 0)return -1;
        leaf =(struct wg_tnode *)offsettoptr(db,newnode);
        leaf->parent_offset = ptrtooffset(db,node);
        leaf->left_subtree_height = 0;
        leaf->right_subtree_height = 0;
        leaf->current_max = minvalue;
        leaf->current_min = minvalue;
        leaf->number_of_elements = 1;
        leaf->left_child_offset = 0;
        leaf->right_child_offset = 0;
        leaf->array_of_values[0] = minvaluerowoffset;
        /* If the original, full node did not have a left child, then
         * there also wasn't a separate GLB node, so we are adding one now
         * as the left child. Otherwise, the new node is added as the right
         * child to the current GLB node.
         */
        if(bnodeoffset == ptrtooffset(db,node)) {
          node->left_child_offset = newnode;
#ifdef TTREE_CHAINED_NODES
          /* Create successor / predecessor relationship */
          leaf->succ_offset = ptrtooffset(db, node);
          leaf->pred_offset = node->pred_offset;

          if(node->pred_offset) {
            struct wg_tnode *pred = offsettoptr(db, node->pred_offset);
            pred->succ_offset = newnode;
          } else {
            hdr->offset_min_node = newnode;
          }
          node->pred_offset = newnode;
#endif
        } else {
#ifdef TTREE_CHAINED_NODES
          struct wg_tnode *succ;
#endif
          node->right_child_offset = newnode;
#ifdef TTREE_CHAINED_NODES
          /* Insert the new node in the sequential chain between
           * the original node and the GLB node found */
          leaf->succ_offset = node->succ_offset;
          leaf->pred_offset = ptrtooffset(db, node);

#ifdef CHECK
          if(!node->succ_offset) {
            show_index_error(db, "GLB with no successor, panic");
            return -1;
          } else {
#endif
            succ = offsettoptr(db, leaf->succ_offset);
            succ->pred_offset = newnode;
#ifdef CHECK
          }
#endif
          node->succ_offset = newnode;
#endif /* TTREE_CHAINED_NODES */
        }
        new = newnode;
      }
    }

  }//the bounding node existed - first algorithm
  else{// bounding node does not exist
    //try to insert the new value to that node - becoming new min or max
    //if the node has room for a new entry
    if(node->number_of_elements < WG_TNODE_ARRAY_SIZE){
      int i;

      /* add entry, keeping the array sorted (see also notes for the
       * bounding node case. The difference this time is that we already
       * know if this value is becoming the new min or max).
       */
      if(boundtype == DEAD_END_LEFT_NOT_BOUNDING) {
        /* our new value is the new min, push everything right */
        for(i=node->number_of_elements; i>0; i--)
          node->array_of_values[i] = node->array_of_values[i-1];
        node->array_of_values[0] = ptrtooffset(db,rec);
        node->current_min = newvalue;
      } else { /* DEAD_END_RIGHT_NOT_BOUNDING */
        /* even simpler case, new value is added to the right */
        node->array_of_values[node->number_of_elements] = ptrtooffset(db,rec);
        node->current_max = newvalue;
      }

      node->number_of_elements++;

      /* XXX: not clear if the empty node can occur here. Until this
       * is checked, we'll be paranoid and overwrite both min and max. */
      if(node->number_of_elements==1) {
        node->current_max = newvalue;
        node->current_min = newvalue;
      }
    }else{
      //make a new node and put data there
      struct wg_tnode *leaf;
      gint newnode = wg_alloc_fixlen_object(db, &dbh->tnode_area_header);
      if(newnode == 0)return -1;
      leaf =(struct wg_tnode *)offsettoptr(db,newnode);
      leaf->parent_offset = ptrtooffset(db,node);
      leaf->left_subtree_height = 0;
      leaf->right_subtree_height = 0;
      leaf->current_max = newvalue;
      leaf->current_min = newvalue;
      leaf->number_of_elements = 1;
      leaf->left_child_offset = 0;
      leaf->right_child_offset = 0;
      leaf->array_of_values[0] = ptrtooffset(db,rec);
      new = newnode;
      //set new node as left or right leaf
      if(boundtype == DEAD_END_LEFT_NOT_BOUNDING){
        node->left_child_offset = newnode;
#ifdef TTREE_CHAINED_NODES
        /* Set the new node as predecessor of the parent */
        leaf->succ_offset = ptrtooffset(db, node);
        leaf->pred_offset = node->pred_offset;

        if(node->pred_offset) {
          /* Notify old predecessor that the node following
           * it changed */
          struct wg_tnode *pred = offsettoptr(db, node->pred_offset);
          pred->succ_offset = newnode;
        } else {
          hdr->offset_min_node = newnode;
        }
        node->pred_offset = newnode;
#endif
      }else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING){
        node->right_child_offset = newnode;
#ifdef TTREE_CHAINED_NODES
        /* Set the new node as successor of the parent */
        leaf->succ_offset = node->succ_offset;
        leaf->pred_offset = ptrtooffset(db, node);

        if(node->succ_offset) {
          /* Notify old successor that the node preceding
           * it changed */
          struct wg_tnode *succ = offsettoptr(db, node->succ_offset);
          succ->pred_offset = newnode;
        } else {
          hdr->offset_max_node = newnode;
        }
        node->succ_offset = newnode;
#endif
      }
    }
  }//no bounding node found - algorithm 2

  //if new node was added to tree - must update child height data in nodes from leaf to root
  //or until find a node with imbalance
  //then determine the bad balance case: LL, LR, RR or RL and execute proper rotation
  if(new){
    struct wg_tnode *child = (struct wg_tnode *)offsettoptr(db,new);
    struct wg_tnode *parent;
    int left = 0;
    while(child->parent_offset != 0){//this is not a root
      int balance;
      parent = (struct wg_tnode *)offsettoptr(db,child->parent_offset);
      //determine which child the child is, left or right one
      if(parent->left_child_offset == ptrtooffset(db,child)) left = 1;
      else left = 0;
      //increment parent left or right subtree height
      if(left)parent->left_subtree_height++;
      else parent->right_subtree_height++;

      //check balance
      balance = parent->left_subtree_height - parent->right_subtree_height;
      if(balance == 0) {
        /* As a result of adding a new node somewhere below, left
         * and right subtrees of the node we're checking became
         * of EQUAL height. This means that changes in subtree heights
         * do not propagate any further (the max depth in this node
         * dit NOT change).
         */
        break;
      }
      if(balance > 1 || balance < -1){//must rebalance
        //the current parent is root for balancing operation
        //determine the branch that causes overweight
        int overw = db_which_branch_causes_overweight(db,parent);
        //fix balance
        db_rotate_ttree(db,index_id,parent,overw);
        break;//while loop because balance does not change in the next levels
      }else{//just proceed to the parent node
        child = parent;
      }
    }
  }
  return 0;
}

/**  removes pointer to data row from index tree structure
*
*  returns:
*  0 - on success
*  -1 - if error, index doesnt exist
*  -2 - if error, no bounding node for key
*  -3 - if error, boundig node exists, value not
*  -4 - if error, tree not in balance
*/
static wg_int ttree_remove_row(void *db, wg_int index_id, void * rec) {
  int i, found;
  wg_int key, rootoffset, column, boundtype, bnodeoffset;
  wg_int rowoffset;
  struct wg_tnode *node, *parent;
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);

  rootoffset = hdr->offset_root_node;
#ifdef CHECK
  if(rootoffset == 0){
    printf("index at offset %d does not exist\n",index_id);
    return -1;
  }
#endif
  column = hdr->rec_field_index[0]; /* always one column for T-tree */
  key = wg_get_field(db, rec, column);
  rowoffset = ptrtooffset(db, rec);

  /* find bounding node for the value. Since non-unique values
   * are allowed, we will find the leftmost node and scan
   * right from there (we *need* the exact row offset).
   */

  bnodeoffset = wg_search_ttree_leftmost(db,
          rootoffset, key, &boundtype, NULL);
  node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
  
  //if bounding node does not exist - error
  if(boundtype != REALLY_BOUNDING_NODE) return -2;
  
  /* find the record inside the node. This is an expensive loop if there
   * are many repeated values, so unnecessary deleting should be avoided
   * on higher level.
   */
  found = -1;
  for(;;) {
    for(i=0;i<node->number_of_elements;i++){
      if(node->array_of_values[i] == rowoffset) {
        found = i;
        goto found_row;
      }
    }
    bnodeoffset = TNODE_SUCCESSOR(db, node);
    if(!bnodeoffset)
      break; /* no more successors */
    node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
    if(WG_COMPARE(db, node->current_min, key) == WG_GREATER)
      break; /* successor is not a bounding node */
  }

found_row:
  if(found == -1) return -3;

  //delete the key and rearrange other elements
  node->number_of_elements--;
  if(found < node->number_of_elements) { /* not the last element */
    /* slide the elements to the right of the found value
     * one step to the left */
    for(i=found; i<node->number_of_elements; i++)
      node->array_of_values[i] = node->array_of_values[i+1];
  }

  /* Update min/max */
  if(found==node->number_of_elements && node->number_of_elements != 0) {
    /* Rightmost element was removed, so new max should be updated to
     * the new rightmost value */
    node->current_max = wg_get_field(db, (void *)offsettoptr(db,
      node->array_of_values[node->number_of_elements - 1]), column);
  } else if(found==0 && node->number_of_elements != 0) {
    /* current_min removed, update to new leftmost value */
    node->current_min = wg_get_field(db, (void *)offsettoptr(db,
      node->array_of_values[0]), column);
  }

  //check underflow and take some actions if needed
  if(node->number_of_elements < 5){//TODO use macro
    //if the node is internal node - borrow its gratest lower bound from the node where it is
    if(node->left_child_offset != 0 && node->right_child_offset != 0){//internal node
#ifndef TTREE_CHAINED_NODES
      wg_int greatestlb = wg_ttree_find_glb_node(db,node->left_child_offset);
#else
      wg_int greatestlb = node->pred_offset;
#endif
      struct wg_tnode *glbnode = (struct wg_tnode *)offsettoptr(db, greatestlb);

      /* Make space for a new min value */
      for(i=node->number_of_elements; i>0; i--)
        node->array_of_values[i] = node->array_of_values[i-1];

      /* take the glb value (always the rightmost in the array) and
       * insert it in our node */
      node -> array_of_values[0] = \
        glbnode->array_of_values[glbnode->number_of_elements-1];
      node -> number_of_elements++;
      node -> current_min = glbnode -> current_max;
      if(node->number_of_elements == 1) /* we just got our first element */
        node->current_max = glbnode -> current_max;
      glbnode -> number_of_elements--;

      //reset new max for glbnode
      if(glbnode->number_of_elements != 0) {
        glbnode->current_max = wg_get_field(db, (void *)offsettoptr(db,
          glbnode->array_of_values[glbnode->number_of_elements - 1]), column);
      }

      node = glbnode;
    }
  }

  //now variable node points to the node which really lost an element
  //this is definitely leaf or half-leaf
  //if the node is empty - free it and rebalanc the tree
  parent = NULL;
  //delete the empty leaf
  if(node->left_child_offset == 0 && node->right_child_offset == 0 && node->number_of_elements == 0){
    if(node->parent_offset != 0){
      parent = (struct wg_tnode *)offsettoptr(db, node->parent_offset);
      //was it left or right child
      if(parent->left_child_offset == ptrtooffset(db,node)){
        parent->left_child_offset=0;
        parent->left_subtree_height=0;
      }else{
        parent->right_child_offset=0;
        parent->right_subtree_height=0;
      }
    }
#ifdef TTREE_CHAINED_NODES
    /* Remove the node from sequential chain */
    if(node->succ_offset) {
      struct wg_tnode *succ = offsettoptr(db, node->succ_offset);
      succ->pred_offset = node->pred_offset;
    } else {
      hdr->offset_max_node = node->pred_offset;
    }
    if(node->pred_offset) {
      struct wg_tnode *pred = offsettoptr(db, node->pred_offset);
      pred->succ_offset = node->succ_offset;
    } else {
      hdr->offset_min_node = node->succ_offset;
    }
#endif
    //free
    wg_free_tnode(db, ptrtooffset(db,node));
    //rebalance if needed
  }

  //or if the node was a half-leaf, see if it can be merged with its leaf
  if((node->left_child_offset == 0 && node->right_child_offset != 0) || (node->left_child_offset != 0 && node->right_child_offset == 0)){
    int elements = node->number_of_elements;
    int left;
    struct wg_tnode *child;
    if(node->left_child_offset != 0){
      child = (struct wg_tnode *)offsettoptr(db, node->left_child_offset);
      left = 1;//true
    }else{
      child = (struct wg_tnode *)offsettoptr(db, node->right_child_offset);
      left = 0;//false
    }
    elements += child->number_of_elements;
    if(!(child->left_subtree_height == 0 && child->right_subtree_height == 0)){
      show_index_error(db,
        "index tree is not balanced, deleting algorithm doesn't work");
      return -4;
    }
    //if possible move all elements from child to node and free child
    if(elements <= WG_TNODE_ARRAY_SIZE){
      int i = node->number_of_elements;
      int j;
      node->number_of_elements = elements;
      if(left){
        /* Left child elements are all smaller than in current node */
        for(j=i-1; j>=0; j--){
          node->array_of_values[j + child->number_of_elements] = \
            node->array_of_values[j];
        }
        for(j=0;j<child->number_of_elements;j++){
          node->array_of_values[j]=child->array_of_values[j];
        }
        node->left_subtree_height=0;
        node->left_child_offset=0;
        node->current_min=child->current_min;
        if(!i) node->current_max=child->current_max; /* parent was empty */
      }else{
        /* Right child elements are all larger than in current node */
        for(j=0;j<child->number_of_elements;j++){
          node->array_of_values[i+j]=child->array_of_values[j];
        }
        node->right_subtree_height=0;
        node->right_child_offset=0;
        node->current_max=child->current_max;
        if(!i) node->current_min=child->current_min; /* parent was empty */
      }
#ifdef TTREE_CHAINED_NODES
      /* Remove the child from sequential chain */
      if(child->succ_offset) {
        struct wg_tnode *succ = offsettoptr(db, child->succ_offset);
        succ->pred_offset = child->pred_offset;
      } else {
        hdr->offset_max_node = child->pred_offset;
      }
      if(child->pred_offset) {
        struct wg_tnode *pred = offsettoptr(db, child->pred_offset);
        pred->succ_offset = child->succ_offset;
      } else {
        hdr->offset_min_node = child->succ_offset;
      }
#endif
      wg_free_tnode(db, ptrtooffset(db, child));
      parent = (struct wg_tnode *)offsettoptr(db, node->parent_offset);
      if(parent->left_child_offset==ptrtooffset(db,node)){
        parent->left_subtree_height=1;
      }else{
        parent->right_subtree_height=1;
      }
    }
  }

  //check balance and update subtree height data
  //stop when find a node where subtree heights dont change 
  if(parent != NULL){
    int balance, height;
    for(;;) {
      balance = parent->left_subtree_height - parent->right_subtree_height;
      if(balance > 1 || balance < -1){//must rebalance
        //the current parent is root for balancing operation
        //rotarion fixes subtree heights in grandparent
        //determine the branch that causes overweight
        int overw = db_which_branch_causes_overweight(db,parent);
        //fix balance
        db_rotate_ttree(db,index_id,parent,overw);
      }
      else if(parent->parent_offset) {
        struct wg_tnode *gp;
        //manually set grandparent subtree heights
        height = max(parent->left_subtree_height,parent->right_subtree_height);
        gp = (struct wg_tnode *)offsettoptr(db, parent->parent_offset);
        if(gp->left_child_offset==ptrtooffset(db,parent)){
          gp->left_subtree_height=1+height;
        }else{
          gp->right_subtree_height=1+height;
        }
      }
      if(!parent->parent_offset)
        break; /* root node reached */
      parent = (struct wg_tnode *)offsettoptr(db, parent->parent_offset);
    }
  }
  return 0;
}


/* ------------------- T-tree public functions ---------------- */

/**
*  returns offset to data row:
*  -1 - error, index does not exist
*  0 - if key NOT found
*  other integer - if key found (= offset to data row)
*  XXX: with duplicate values, which one is returned is somewhat
*  undetermined, so this function is mainly for early development/testing
*/
wg_int wg_search_ttree_index(void *db, wg_int index_id, wg_int key){
  int i;
  wg_int rootoffset, bnodetype, bnodeoffset;
  wg_int rowoffset, column;
  struct wg_tnode * node;
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);

  rootoffset = hdr->offset_root_node;
#ifdef CHECK
  /* XXX: This is a rather weak check but might catch some errors */
  if(rootoffset == 0){
    printf("index at offset %d does not exist\n",index_id);
    return -1;
  }
#endif

  /* Find the leftmost bounding node */
  bnodeoffset = wg_search_ttree_leftmost(db,
          rootoffset, key, &bnodetype, NULL);
  node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
  
  if(bnodetype != REALLY_BOUNDING_NODE) return 0;
  
  column = hdr->rec_field_index[0]; /* always one column for T-tree */
  /* find the record inside the node. */
  for(;;) {
    for(i=0;i<node->number_of_elements;i++){
      rowoffset = node->array_of_values[i];
      if(WG_COMPARE(db,
        wg_get_field(db, (void *)offsettoptr(db,rowoffset), column),
        key) == WG_EQUAL) {
        return rowoffset;
      }
    }
    /* Normally we cannot end up here. We'll keep the code in case
     * implementation of wg_compare() changes in the future.
     */
    bnodeoffset = TNODE_SUCCESSOR(db, node);
    if(!bnodeoffset)
      break; /* no more successors */
    node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
    if(WG_COMPARE(db, node->current_min, key) == WG_GREATER)
      break; /* successor is not a bounding node */
  }

  return 0;
}

/*
 * The following pairs of functions implement tree traversal. Only
 * wg_ttree_find_glb_node() is used for the upkeep of T-tree (insert, delete,
 * re-balance), the rest are required for sequential scan and range queries
 * when the tree is implemented without predecessor and successor pointers.
 */

#ifndef TTREE_CHAINED_NODES

/** find greatest lower bound node
*  returns offset of the (half-) leaf node with greatest lower bound
*  goes only right - so: must call on the left child of the internal
*  which we are looking the GLB node for.
*/
wg_int wg_ttree_find_glb_node(void *db, wg_int nodeoffset) {
  struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,nodeoffset);
  if(node->right_child_offset != 0)
    return wg_ttree_find_glb_node(db, node->right_child_offset);
  else
    return nodeoffset;
}

/** find least upper bound node
*  returns offset of the (half-) leaf node with least upper bound
*  Call with the right child of an internal node as argument.
*/
wg_int wg_ttree_find_lub_node(void *db, wg_int nodeoffset) {
  struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,nodeoffset);
  if(node->left_child_offset != 0)
    return wg_ttree_find_lub_node(db, node->left_child_offset);
  else
    return nodeoffset;
}

/** find predecessor of a leaf.
*  Returns offset of the internal node which holds the value
*  immediately preceeding the current_min of the leaf.
*  If the search hit root (the leaf could be the leftmost one in
*  the tree) the function returns 0.
*  This is the reverse of finding the LUB node.
*/
wg_int wg_ttree_find_leaf_predecessor(void *db, wg_int nodeoffset) {
  struct wg_tnode *node, *parent;

  node = (struct wg_tnode *)offsettoptr(db,nodeoffset);
  if(node->parent_offset) {
    parent = (struct wg_tnode *) offsettoptr(db, node->parent_offset);
    /* If the current node was left child of the parent, the immediate
     * parent has larger values, so we need to climb to the next
     * level with our search. */
    if(parent->left_child_offset == nodeoffset)
      return wg_ttree_find_leaf_predecessor(db, node->parent_offset);
  }
  return node->parent_offset;
}

/** find successor of a leaf.
*  Returns offset of the internal node which holds the value
*  immediately succeeding the current_max of the leaf.
*  Returns 0 if there is no successor.
*  This is the reverse of finding the GLB node.
*/
wg_int wg_ttree_find_leaf_successor(void *db, wg_int nodeoffset) {
  struct wg_tnode *node, *parent;

  node = (struct wg_tnode *)offsettoptr(db,nodeoffset);
  if(node->parent_offset) {
    parent = (struct wg_tnode *) offsettoptr(db, node->parent_offset);
    if(parent->right_child_offset == nodeoffset)
      return wg_ttree_find_leaf_successor(db, node->parent_offset);
  }
  return node->parent_offset;
}

#endif /* TTREE_CHAINED_NODES */

/*
 * Functions to support range queries (and fetching multiple
 * duplicate values) using T-tree index. Since the nodes can be
 * traversed sequentially, the simplest way to implement queries that
 * have result sets is to find leftmost (or rightmost) value that
 * meets the query conditions and scan right (or left) from there.
 */
 
/** Find rightmost node containing given value
 *  returns NULL if node was not found
 */
wg_int wg_search_ttree_rightmost(void *db, wg_int rootoffset,
  wg_int key, wg_int *result, struct wg_tnode *rb_node) {

  struct wg_tnode * node;

#ifdef TTREE_SINGLE_COMPARE
  node = (struct wg_tnode *)offsettoptr(db,rootoffset);

  /* Improved(?) tree search algorithm with a single compare per node.
   * only lower bound is examined, if the value is larger the right subtree
   * is selected immediately. If the search ends in a dead end, the node where
   * the right branch was taken is examined again.
   */
  if(WG_COMPARE(db, key, node->current_min) == WG_LESSTHAN) {
    /* key < node->current_min */
    if(node->left_child_offset != 0) {
      return wg_search_ttree_rightmost(db, node->left_child_offset, key,
        result, rb_node);
    } else if (rb_node) {
      /* Dead end, but we still have an unexamined node left */
      if(WG_COMPARE(db, key, rb_node->current_max) != WG_GREATER) {
        /* key<=rb_node->current_max */
        *result = REALLY_BOUNDING_NODE;
        return ptrtooffset(db, rb_node);
      }
    }
    /* No left child, no rb_node or it's right bound was not interesting */
    *result = DEAD_END_LEFT_NOT_BOUNDING;
    return rootoffset;
  }
  else {
    if(node->right_child_offset != 0) {
      /* Here we jump the gun and branch to right, ignoring the
       * current_max of the node (therefore avoiding one expensive
       * compare operation).
       */
      return wg_search_ttree_rightmost(db, node->right_child_offset, key,
        result, node);
    } else if(WG_COMPARE(db, key, node->current_max) != WG_GREATER) {
      /* key<=node->current_max */
      *result = REALLY_BOUNDING_NODE;
      return rootoffset;
    }
    /* key is neither left of or inside this node and
     * there is no right child */
    *result = DEAD_END_RIGHT_NOT_BOUNDING;
    return rootoffset;
  }
#else
  wg_int bnodeoffset;

  bnodeoffset = db_find_bounding_tnode(db, rootoffset, key, result, NULL);
  if(*result != REALLY_BOUNDING_NODE)
    return bnodeoffset;

  /* There is at least one node with the key we're interested in,
   * now make sure we have the rightmost */
  node = offsettoptr(db, bnodeoffset);
  while(WG_COMPARE(db, node->current_max, key) == WG_EQUAL) {
    wg_int nextoffset = TNODE_SUCCESSOR(db, node);
    if(nextoffset) {
      struct wg_tnode *next = offsettoptr(db, nextoffset);
        if(WG_COMPARE(db, next->current_min, key) == WG_GREATER)
          /* next->current_min > key */
          break; /* overshot */
      node = next;
    }
    else
      break; /* last node in chain */
  }
  return ptrtooffset(db, node);
#endif
}

/** Find leftmost node containing given value
 *  returns NULL if node was not found
 */
wg_int wg_search_ttree_leftmost(void *db, wg_int rootoffset,
  wg_int key, wg_int *result, struct wg_tnode *lb_node) {

  struct wg_tnode * node;

#ifdef TTREE_SINGLE_COMPARE
  node = (struct wg_tnode *)offsettoptr(db,rootoffset);

  /* Rightmost bound search mirrored */
  if(WG_COMPARE(db, key, node->current_max) == WG_GREATER) {
    /* key > node->current_max */
    if(node->right_child_offset != 0) {
      return wg_search_ttree_leftmost(db, node->right_child_offset, key,
        result, lb_node);
    } else if (lb_node) {
      /* Dead end, but we still have an unexamined node left */
      if(WG_COMPARE(db, key, lb_node->current_min) != WG_LESSTHAN) {
        /* key>=lb_node->current_min */
        *result = REALLY_BOUNDING_NODE;
        return ptrtooffset(db, lb_node);
      }
    }
    *result = DEAD_END_RIGHT_NOT_BOUNDING;
    return rootoffset;
  }
  else {
    if(node->left_child_offset != 0) {
      return wg_search_ttree_leftmost(db, node->left_child_offset, key,
        result, node);
    } else if(WG_COMPARE(db, key, node->current_min) != WG_LESSTHAN) {
      /* key>=node->current_min */
      *result = REALLY_BOUNDING_NODE;
      return rootoffset;
    }
    *result = DEAD_END_LEFT_NOT_BOUNDING;
    return rootoffset;
  }
#else
  wg_int bnodeoffset;

  bnodeoffset = db_find_bounding_tnode(db, rootoffset, key, result, NULL);
  if(*result != REALLY_BOUNDING_NODE)
    return bnodeoffset;

  /* One (we don't know which) bounding node found, traverse the
   * tree to the leftmost. */
  node = offsettoptr(db, bnodeoffset);
  while(WG_COMPARE(db, node->current_min, key) == WG_EQUAL) {
    wg_int prevoffset = TNODE_PREDECESSOR(db, node);
    if(prevoffset) {
      struct wg_tnode *prev = offsettoptr(db, prevoffset);
      if(WG_COMPARE(db, prev->current_max, key) == WG_LESSTHAN)
        /* prev->current_max < key */
        break; /* overshot */
      node = prev;
    }
    else
      break; /* first node in chain */
  }
  return ptrtooffset(db, node);
#endif
}
       
/** Find first occurrence of a value in a T-tree node
 *  returns the number of the slot. If the value itself
 *  is missing, the location of the first value that
 *  exceeds it is returned.
 */
wg_int wg_search_tnode_first(void *db, wg_int nodeoffset, wg_int key,
  wg_int column) {

  wg_int i, encoded;
  struct wg_tnode *node = (struct wg_tnode *) offsettoptr(db, nodeoffset);

  for(i=0; i<node->number_of_elements; i++) {
    /* Naive scan is ok for small values of WG_TNODE_ARRAY_SIZE. */
    encoded = wg_get_field(db,
      (void *)offsettoptr(db,node->array_of_values[i]), column);
    if(WG_COMPARE(db, encoded, key) != WG_LESSTHAN)
      /* encoded >= key */
      return i;
  }

  return -1;
}

/** Find last occurrence of a value in a T-tree node
 *  returns the number of the slot. If the value itself
 *  is missing, the location of the first value that
 *  is smaller (when scanning from right to left) is returned.
 */
wg_int wg_search_tnode_last(void *db, wg_int nodeoffset, wg_int key,
  wg_int column) {

  wg_int i, encoded;
  struct wg_tnode *node = (struct wg_tnode *) offsettoptr(db, nodeoffset);

  for(i=node->number_of_elements -1; i>=0; i--) {
    encoded = wg_get_field(db,
      (void *)offsettoptr(db,node->array_of_values[i]), column);
    if(WG_COMPARE(db, encoded, key) != WG_GREATER)
      /* encoded <= key */
      return i;
  }

  return -1;
}

/** Create T-tree index on a column
*  returns:
*  0 - on success
*  -1 - error (failed to create the index)
*/
wg_int wg_create_ttree_index(void *db, wg_int column){
  int fields;
  gint node, tmp, index_id;
  unsigned int rowsprocessed;
  struct wg_tnode *nodest;
  wg_index_header *hdr;
  wg_index_list *ilist, *nexti;
  void *rec;
  db_memsegment_header* dbh = (db_memsegment_header*) db;
  
  if(column>=MAX_INDEXED_FIELDNR) {
    show_index_error_nr(db, "Max allowed column number",
      MAX_INDEXED_FIELDNR-1);
    return -1;
  }

  ilist = NULL;
  /* Check if T-tree index already exists on this column */
  if(dbh->index_control_area_header.index_table[column]) {
    ilist = offsettoptr(db, dbh->index_control_area_header.index_table[column]);
    for(;;) {
      if(!ilist->header_offset) {
        show_index_error(db, "Invalid header in index list");
        return -1;
      }
      hdr = offsettoptr(db, ilist->header_offset);
      if(hdr->type==DB_INDEX_TYPE_1_TTREE) {
        show_index_error(db, "TTree index already exists on column");
        return -1;
      }
      if(!ilist->next_offset)
        break;
      ilist = offsettoptr(db, ilist->next_offset);
    }
  }    

  /* Add new element to index list */
  tmp = wg_alloc_fixlen_object(db, &dbh->indexlist_area_header);
  nexti = offsettoptr(db, tmp);
  if(ilist) {
    ilist->next_offset = tmp;
    nexti->prev_offset = ptrtooffset(db, ilist);
  } else {
    dbh->index_control_area_header.index_table[column] = tmp;
    nexti->prev_offset = 0;
  }

  /* Add new index header */
  index_id = wg_alloc_fixlen_object(db, &dbh->indexhdr_area_header);
  hdr = offsettoptr(db, index_id);
  nexti->header_offset = index_id;

  //increase index counter
  dbh->index_control_area_header.number_of_indexes++;
  
  //allocate (+ init) root node for new index tree and save the offset into index_array
  
  node = wg_alloc_fixlen_object(db, &dbh->tnode_area_header);
  nodest =(struct wg_tnode *)offsettoptr(db,node);
  nodest->parent_offset = 0;
  nodest->left_subtree_height = 0;
  nodest->right_subtree_height = 0;
  nodest->current_max = WG_ILLEGAL;
  nodest->current_min = WG_ILLEGAL;
  nodest->number_of_elements = 0;
  nodest->left_child_offset = 0;
  nodest->right_child_offset = 0;
#ifdef TTREE_CHAINED_NODES
  nodest->succ_offset = 0;
  nodest->pred_offset = 0;
#endif

/*  printf("allocated (root)node offset = %d\n",node); */

  hdr->offset_root_node = node;
  hdr->type = DB_INDEX_TYPE_1_TTREE;
  hdr->fields = 1;
  hdr->rec_field_index[0] = column;
#ifdef TTREE_CHAINED_NODES
  hdr->offset_min_node = node;
  hdr->offset_max_node = node;
#endif

  //scan all the data - make entry for every suitable row
  rec = wg_get_first_record(db);
  rowsprocessed = 0;
  
  while(rec != NULL) {
    fields = wg_get_record_len(db, rec);
    if(column >= fields) {
      rec=wg_get_next_record(db,rec);
      continue;
    }
    ttree_add_row(db, index_id, rec);
    rowsprocessed++;
    rec=wg_get_next_record(db,rec);
  }

/*  printf("index slot %d root is %d\n", index_id, hdr->offset_root_node); */
  printf("new index created on rec field %d into slot %d and %d data rows inserted\n",
    column, index_id, rowsprocessed);

  return 0;
}

/** Drop T-tree index from a column
*  Frees the memory in the T-node area
*  returns:
*  0 - on success
*  -1 - error
*/
wg_int wg_drop_ttree_index(void *db, wg_int column){
  struct wg_tnode *node;
  wg_index_header *hdr;
  wg_index_list *ilist, *found;
  db_memsegment_header* dbh = (db_memsegment_header*) db;
  
  if(column>=MAX_INDEXED_FIELDNR) {
    show_index_error_nr(db, "Max allowed column number",
      MAX_INDEXED_FIELDNR-1);
    return -1;
  }

  /* Find the T-tree index on the column */
  found = NULL;
  if(dbh->index_control_area_header.index_table[column]) {
    ilist = offsettoptr(db, dbh->index_control_area_header.index_table[column]);
    for(;;) {
      if(!ilist->header_offset) {
        show_index_error(db, "Invalid header in index list");
        return -1;
      }
      hdr = offsettoptr(db, ilist->header_offset);
      if(hdr->type==DB_INDEX_TYPE_1_TTREE) {
        found = ilist;
        break;
      }
      if(!ilist->next_offset)
        break;
      ilist = offsettoptr(db, ilist->next_offset);
    }
  }    

  if(!found)
    return -1;
  hdr = offsettoptr(db, found->header_offset);

  /* Free the T-node memory. This is trivial for chained nodes, since
   * once we've found a successor for a node it can be deleted and
   * forgotten about. For plain T-tree this does not work since tree
   * traversal often runs down and up parent-child chains, which means
   * that some parents cannot be deleted before their children.
   */
  node = NULL;
#ifdef TTREE_CHAINED_NODES
  if(hdr->offset_min_node)
    node = offsettoptr(db, hdr->offset_min_node);
  else if(hdr->offset_root_node) /* normally this does not happen */
    node = offsettoptr(db, hdr->offset_root_node);
  while(node) {
    gint deleteme = ptrtooffset(db, node);
    if(node->succ_offset)
      node = offsettoptr(db, node->succ_offset);
    else
      node = NULL;
    wg_free_tnode(db, deleteme);
  }
#else
  /* XXX: not implemented */
  show_index_error(db, "Warning: T-node memory cannot be deallocated");
#endif

  /* Now free the header */
  wg_free_fixlen_object(db, &dbh->indexhdr_area_header, found->header_offset);
  
  /* Update index list chain and index table */
  if(found->next_offset) {
    ilist = offsettoptr(db, found->next_offset);
    ilist->prev_offset = found->prev_offset;
  }
  if(found->prev_offset) {
    ilist = offsettoptr(db, found->prev_offset);
    ilist->prev_offset = found->next_offset;
  } else {
    dbh->index_control_area_header.index_table[column] = found->next_offset;
  }
  
  /* Free the vacated list element */
  wg_free_fixlen_object(db, &dbh->indexlist_area_header,
    ptrtooffset(db, found));

  return 0;
}


/* ----------------- General index functions --------------- */

/** Find index id (index header) by column
* Supports all types of indexes, calling program should examine the
* header of returned index to decide how to proceed. Alternatively,
* if type is not 0 then only indexes of the given type are
* returned.
*
*  returns:
*  -1 if no index found
*  offset > 0 if index found - index id
*  XXX: please note that this function may be replaced by query optimiser
*  inside query functions, directly. Current version is for early
*  development/testing.
*/
wg_int wg_column_to_index_id(void *db, wg_int column, wg_int type) {
  db_memsegment_header* dbh = (db_memsegment_header*) db;
  wg_index_list *ilist;
  wg_index_header *hdr;

  if(!dbh->index_control_area_header.index_table[column])
    return -1;
    
  ilist = offsettoptr(db, dbh->index_control_area_header.index_table[column]);
  for(;;) {
    if(ilist->header_offset) {
      int i;
      hdr = offsettoptr(db, ilist->header_offset);
      if(!type || type==hdr->type) {
        for(i=0; i<hdr->fields; i++) {
          if(hdr->rec_field_index[i]==column)
            return ilist->header_offset; /* index id */
        }
      }
    }
    if(!ilist->next_offset)
      break;
    ilist = offsettoptr(db, ilist->next_offset);
  }
    
  return -1;
}

/** Add data of one field to all indexes
 * Loops over indexes in one field and inserts the data into
 * each one of them.
 * returns 0 for success
 * returns -1 for invalid arguments
 * returns -2 for error (insert failed, index is no longer consistent)
 */
wg_int wg_index_add_field(void *db, void *rec, wg_int column) {
  wg_index_list *ilist;
  db_memsegment_header* dbh = (db_memsegment_header*) db;

#ifdef CHECK
  /* XXX: if used from wg_set_field() only, this is redundant */
  if(column >= MAX_INDEXED_FIELDNR || column >= wg_get_record_len(db, rec))
    return -1;
#endif

  /* XXX: if used from wg_set_field() only, this is redundant */
  if(!dbh->index_control_area_header.index_table[column])
    return -1;

  ilist = offsettoptr(db, dbh->index_control_area_header.index_table[column]);
  /* Find all indexes on the column */
  for(;;) {
    if(ilist->header_offset) {
      wg_index_header *hdr = offsettoptr(db, ilist->header_offset);
      if(hdr->type == DB_INDEX_TYPE_1_TTREE) {
        if(ttree_add_row(db, ilist->header_offset, rec))
          return -2;
      }
      else
        show_index_error(db, "unknown index type, ignoring");
    }
    if(!ilist->next_offset)
      break;
    ilist = offsettoptr(db, ilist->next_offset);
  }
  return 0;
}

/** Add data of one record to all indexes
 * Convinience function to add an entire record into
 * all indexes in the database.
 * returns 0 on success, -2 on error
 * (-1 is skipped to have consistent error codes for add/del functions)
 */
wg_int wg_index_add_rec(void *db, void *rec) {
  gint i;
  db_memsegment_header* dbh = (db_memsegment_header*) db;
  gint reclen = wg_get_record_len(db, rec);

  for(i=0;i<reclen;i++){
    wg_index_list *ilist;

    if(!dbh->index_control_area_header.index_table[i])
      continue; /* no indexes on this column */

    ilist = offsettoptr(db, dbh->index_control_area_header.index_table[i]);
    /* Find all indexes on the column */
    for(;;) {
      if(ilist->header_offset) {
        wg_index_header *hdr = offsettoptr(db, ilist->header_offset);
        if(hdr->rec_field_index[0] >= i) {
          /* A little trick: we only update index if the
           * first column in the column list matches. The reasoning
           * behind this is that we only want to update each index
           * once, for multi-column indexes we can rest assured that
           * the work was already done.
           * XXX: case where there is no data in a column unclear
           */
          if(hdr->type == DB_INDEX_TYPE_1_TTREE) {
            if(ttree_add_row(db, ilist->header_offset, rec))
              return -2;
          }
          else
            show_index_error(db, "unknown index type, ignoring");
        }
      }
      if(!ilist->next_offset)
        break;
      ilist = offsettoptr(db, ilist->next_offset);
    }
  }
  return 0;
}

/** Delete data of one field from all indexes
 * Loops over indexes in one column and removes the references
 * to the record from all of them.
 * returns 0 for success
 * returns -1 for invalid arguments
 * returns -2 for error (delete failed, possible index corruption)
 */
wg_int wg_index_del_field(void *db, void *rec, wg_int column) {
  wg_index_list *ilist;
  db_memsegment_header* dbh = (db_memsegment_header*) db;

#ifdef CHECK
  /* XXX: if used from wg_set_field() only, this is redundant */
  if(column >= MAX_INDEXED_FIELDNR || column >= wg_get_record_len(db, rec))
    return -1;
#endif

  /* XXX: if used from wg_set_field() only, this is redundant */
  if(!dbh->index_control_area_header.index_table[column])
    return -1;

  ilist = offsettoptr(db, dbh->index_control_area_header.index_table[column]);
  /* Find all indexes on the column */
  for(;;) {
    if(ilist->header_offset) {
      wg_index_header *hdr = offsettoptr(db, ilist->header_offset);
      if(hdr->type == DB_INDEX_TYPE_1_TTREE) {
        gint err = ttree_remove_row(db, ilist->header_offset, rec);
        if(err < -2) {
/*          fprintf(stderr, "err: %d\n", err);*/
          return -2;
        }
      }
      else
        show_index_error(db, "unknown index type, ignoring");
    }
    if(!ilist->next_offset)
      break;
    ilist = offsettoptr(db, ilist->next_offset);
  }
  return 0;
}

/* Delete data of one record from all indexes
 * Should be called from wg_delete_record()
 * returns 0 for success
 * returns -2 for error (delete failed, index presumably corrupt)
 */
wg_int wg_index_del_rec(void *db, void *rec) {
  gint i;
  db_memsegment_header* dbh = (db_memsegment_header*) db;
  gint reclen = wg_get_record_len(db, rec);

  for(i=0;i<reclen;i++){
    wg_index_list *ilist;

    if(!dbh->index_control_area_header.index_table[i])
      continue; /* no indexes on this column */

    ilist = offsettoptr(db, dbh->index_control_area_header.index_table[i]);
    /* Find all indexes on the column */
    for(;;) {
      if(ilist->header_offset) {
        wg_index_header *hdr = offsettoptr(db, ilist->header_offset);
        if(hdr->rec_field_index[0] >= i) {
          /* Ignore second, third etc references to multi-column
           * indexes. XXX: This only works if index table is scanned
           * sequentially, from position 0. See also comment for
           * wg_index_del_rec command.
           */
          if(hdr->type == DB_INDEX_TYPE_1_TTREE) {
            if(ttree_remove_row(db, ilist->header_offset, rec) < -2)
              return -2;
          }
          else
            show_index_error(db, "unknown index type, ignoring");
        }
      }
      if(!ilist->next_offset)
        break;
      ilist = offsettoptr(db, ilist->next_offset);
    }
  }
  return 0;
}

/* --------------- error handling ------------------------------*/

/** called with err msg
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_index_error(void* db, char* errmsg) {
  printf("index error: %s\n",errmsg);
  return -1;
} 

/** called with err msg and additional int data
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_index_error_nr(void* db, char* errmsg, gint nr) {
  printf("index error: %s %d\n",errmsg,nr);
  return -1;
}  
