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
#include "dbtest.h"


/* ====== Private defs =========== */

#define REALLY_BOUNDING_NODE 0
#define DEAD_END_LEFT_NOT_BOUNDING 1
#define DEAD_END_RIGHT_NOT_BOUNDING 2

#define LL_CASE 0
#define LR_CASE 1
#define RL_CASE 2
#define RR_CASE 3

#ifndef max
#define max(a,b) (a>b ? a : b)
#endif


/* ======= Private protos ================ */

static wg_int db_find_bounding_tnode(void *db, wg_int rootoffset, wg_int key,
  wg_int *result, struct wg_tnode *rb_node);

static gint show_index_error(void* db, char* errmsg);
static gint show_index_error_nr(void* db, char* errmsg, gint nr);


/* ====== Functions ============== */

/*
 * Index implementation:
 * - T-Tree, as described by Lehman & Carey '86 (XXX: check source)
 * - improvements from T* tree (Kim & Choi '96) (not done yet)
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

/**
*  returns bounding node offset or if no really bounding node exists, then the closest node
*/
static wg_int db_find_bounding_tnode(void *db, wg_int rootoffset, wg_int key,
  wg_int *result, struct wg_tnode *rb_node) {

  struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,rootoffset);

#ifndef TTREE_SINGLE_COMPARE
  /* Original tree search algorithm: compares both bounds of
   * the node to determine immediately if the value falls between them.
   */
  if(key>=node->current_min && key<=node->current_max){
    *result = REALLY_BOUNDING_NODE;
    return rootoffset;
  }

  if(key < node->current_min){
    if(node->left_child_offset != 0) return db_find_bounding_tnode(db, node->left_child_offset, key, result, NULL);
    else{
      *result = DEAD_END_LEFT_NOT_BOUNDING;
      return rootoffset;
    }
  }
  else { /* if(key > node->current_max) */
    if(node->right_child_offset != 0) return db_find_bounding_tnode(db, node->right_child_offset, key, result, NULL);
    else{
      *result = DEAD_END_RIGHT_NOT_BOUNDING;
      return rootoffset;
    }
  }
#else
  /* Improved(?) tree search algorithm with a single compare per node.
   * only lower bound is examined, if the value is larger the right subtree
   * is selected immediately. If the search ends in a dead end, the node where
   * the right branch was taken is examined again.
   */
  if(key < node->current_min) {
    if(node->left_child_offset != 0) {
      return db_find_bounding_tnode(db, node->left_child_offset, key,
        result, rb_node);
    } else if (rb_node) {
      /* Dead end, but we still have an unexamined node left */
      if(key<=rb_node->current_max){
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
      return db_find_bounding_tnode(db, node->right_child_offset, key,
        result, node);
    } else if(key<=node->current_max){
      *result = REALLY_BOUNDING_NODE;
      return rootoffset;
    }
    /* key is neither left of or inside this node and
     * there is no right child */
    *result = DEAD_END_RIGHT_NOT_BOUNDING;
    return rootoffset;
  }
#endif
}

/**
*  returns offset of the node with greatest lower bound
*  goes only right - so: must call on the left child of the real node under processing
*        not on the real node under processing
*/
static wg_int db_find_node_with_greatest_lower_bound(void *db, wg_int nodeoffset){
  struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,nodeoffset);
  if(node->right_child_offset != 0) return db_find_node_with_greatest_lower_bound(db, node->right_child_offset);
  else return nodeoffset;
}

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
    if(child->left_subtree_height > child->right_subtree_height)return LL_CASE;
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
     * node is adequately filled.
     */
    if(ee->number_of_elements == 1 && bb->number_of_elements == WG_TNODE_ARRAY_SIZE){
      int i;
      wg_int encoded;

      /* Create space for elements from B */
      ee->array_of_values[bb->number_of_elements - 1] = ee->array_of_values[0];

      /* All the values moved are smaller than in E */
      for(i=1; i<bb->number_of_elements; i++)
        ee->array_of_values[i-1] = bb->array_of_values[i];
      ee->number_of_elements = bb->number_of_elements;

      /* Examine the new leftmost element to find current_min */
      encoded = wg_get_field(db, (void *)offsettoptr(db,
        ee->array_of_values[0]), column);
      ee->current_min = wg_decode_int(db, encoded);

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

    //third switch: B goes to E's left child
    /* "special" RL rotation - see comments for LR_CASE */
    if(ee->number_of_elements == 1 && bb->number_of_elements == WG_TNODE_ARRAY_SIZE){
      int i;
      wg_int encoded;

      /* All the values moved are larger than in E */
      for(i=1; i<bb->number_of_elements; i++)
        ee->array_of_values[i] = bb->array_of_values[i-1];
      ee->number_of_elements = bb->number_of_elements;

      /* Examine the new rightmost element to find current_max */
      encoded = wg_get_field(db, (void *)offsettoptr(db,
        ee->array_of_values[ee->number_of_elements - 1]), column);
      ee->current_max = wg_decode_int(db, encoded);

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


/**
*  returns offset to data row:
*  -1 - error, index does not exist
*  0 - if key NOT found
*  other integer - if key found (= offset to data row)
*/
wg_int wg_search_ttree_index(void *db, wg_int index_id, wg_int key){
  int i;
  wg_int rootoffset, bnodetype, bnodeoffset;
  wg_int rowoffset, column, encoded;
  struct wg_tnode * node;
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);

  rootoffset = hdr->offset_root_node;
#if 0
  /* XXX: This is a rather weak check but might catch some errors */
  if(rootoffset == 0){
    printf("index at offset %d does not exist\n",index_id);
    return -1;
  }
#endif

  //(binary) search for bounding node
  bnodeoffset = db_find_bounding_tnode(db, rootoffset, key, &bnodetype, NULL);
  node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);

  //search for the key inside the bounding node if the node was not a dead end
  if(bnodetype==DEAD_END_LEFT_NOT_BOUNDING)return 0;
  if(bnodetype==DEAD_END_RIGHT_NOT_BOUNDING)return 0;

  column = hdr->rec_field_index[0]; /* always one column for T-tree */
  for(i=0;i<node->number_of_elements;i++){
    rowoffset = node->array_of_values[i];
    encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
    if(wg_decode_int(db,encoded) == key) return rowoffset;
  }

  return 0;
}

/**  removes pointer to data row from index tree structure
*
*  returns:
*  0 - on success
*  1 - if error, index doesnt exist
*  2 - if error, no bounding node for key
*  3 - if error, boundig node exists, value not
*  4 - if error, tree not in balance
*/
wg_int wg_remove_key_from_index(void *db, wg_int index_id, void * rec){
  int i, found;
  wg_int key, rootoffset, column, boundtype, bnodeoffset;
  wg_int encoded, rowoffset;
  struct wg_tnode *node, *parent;
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);

  rootoffset = hdr->offset_root_node;
#if 0
  if(rootoffset == 0){
    printf("index at offset %d does not exist\n",index_id);
    return 1;
  }
#endif
  column = hdr->rec_field_index[0]; /* always one column for T-tree */
  encoded = wg_get_field(db, rec, column);
  key = wg_decode_int(db, encoded);
  rowoffset = ptrtooffset(db, rec);

  //find bounding node for the value
  bnodeoffset = db_find_bounding_tnode(db, rootoffset, key, &boundtype, NULL);
  node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
  
  //if bounding node does not exist - error
  if(boundtype != REALLY_BOUNDING_NODE) return 2;
  
  /* find the record inside the node */
  found = -1;
  for(i=0;i<node->number_of_elements;i++){
    if(node->array_of_values[i] == rowoffset) {
      found = i;
      break;
    }
  }

  if(found == -1) return 3;

  //delete the key and rearrange other elements
  node->number_of_elements--;
  if(found < node->number_of_elements) { /* not the last element */
    /* slide the elements to the right of the found value
     * one step to the left */
    for(i=found; i<node->number_of_elements; i++)
      node->array_of_values[i] = node->array_of_values[i+1];
  }

  //maybe fix min or max variables
  if(key == node->current_max && node->number_of_elements != 0) {
    /* One element was removed, so new max should be updated to
     * the new rightmost value */
    encoded = wg_get_field(db, (void *)offsettoptr(db,
      node->array_of_values[node->number_of_elements - 1]), column);
    node -> current_max = wg_decode_int(db, encoded);
  } else if(key == node->current_min && node->number_of_elements != 0) {
    /* current_min possibly removed, update to new leftmost value */
    encoded = wg_get_field(db, (void *)offsettoptr(db,
      node->array_of_values[0]), column);
    node -> current_min = wg_decode_int(db, encoded);
  }

  //check underflow and take some actions if needed
  if(node->number_of_elements < 5){//TODO use macro
    //if the node is internal node - borrow its gratest lower bound from the node where it is
    if(node->left_child_offset != 0 && node->right_child_offset != 0){//internal node
      wg_int greatestlb = db_find_node_with_greatest_lower_bound(db,node->left_child_offset);
      struct wg_tnode *glbnode = (struct wg_tnode *)offsettoptr(db, greatestlb);

      /* Make space for a new min value */
      for(i=0; i<node->number_of_elements; i++)
        node->array_of_values[i+1] = node->array_of_values[i];

      /* take the glb value (always the rightmost in the array) and
       * insert it in our node */
      node -> array_of_values[0] = \
        glbnode->array_of_values[glbnode->number_of_elements-1];
      node -> number_of_elements++;
      node -> current_min = glbnode -> current_max;
      glbnode -> number_of_elements--;

      //reset new max for glbnode
      if(glbnode->number_of_elements != 0) {
        encoded = wg_get_field(db, (void *)offsettoptr(db,
          glbnode->array_of_values[glbnode->number_of_elements - 1]), column);
        glbnode -> current_max = wg_decode_int(db, encoded);
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
      printf("ERROR, index tree is not balanced, deleting algorithm doesn't work\n");
      return 4;
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
      }else{
        /* Right child elements are all larger than in current node */
        for(j=0;j<child->number_of_elements;j++){
          node->array_of_values[i+j]=child->array_of_values[j];
        }
        node->right_subtree_height=0;
        node->right_child_offset=0;
        node->current_max=child->current_max;
      }
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
    while(parent->parent_offset != 0){
      balance = parent->left_subtree_height - parent->right_subtree_height;
      if(balance > 1 || balance < -1){//must rebalance
        //the current parent is root for balancing operation
        //rotarion fixes subtree heights in grandparent
        //determine the branch that causes overweight
        int overw = db_which_branch_causes_overweight(db,parent);
        //fix balance
        db_rotate_ttree(db,index_id,parent,overw);
        if(parent->parent_offset==0)break;//this is root, cannot go any more up
        
      }else{
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
      parent = (struct wg_tnode *)offsettoptr(db, parent->parent_offset);
    }
  }
  return 0;
}

/**  inserts pointer to data row into index tree structure
*
*  returns:
*  0 - on success
*  1 - if error
*/
wg_int wg_add_new_row_into_index(void *db, wg_int index_id, void *rec){
  wg_int rootoffset, column, encoded;
  wg_int newvalue, boundtype, bnodeoffset, new;
  struct wg_tnode *node;
  wg_index_header *hdr = (wg_index_header *)offsettoptr(db,index_id);
  db_memsegment_header* dbh = (db_memsegment_header*) db;

  rootoffset = hdr->offset_root_node;
#if 0
  if(rootoffset == 0){
    printf("index at offset %d does not exist\n",index_id);
    return 1;
  }
#endif
  column = hdr->rec_field_index[0]; /* always one column for T-tree */

  //extract real value from the row (rec)
  encoded = wg_get_field(db, rec, column);
  newvalue = wg_decode_int(db,encoded);

  //find bounding node for the value
  bnodeoffset = db_find_bounding_tnode(db, rootoffset, newvalue, &boundtype, NULL);
  node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
  new = 0;//save here the offset of newly created tnode - 0 if no node added into the tree
  //if bounding node exists - follow one algorithm, else the other
  if(boundtype == REALLY_BOUNDING_NODE){

    //check if the node has room for a new entry
    if(node->number_of_elements < WG_TNODE_ARRAY_SIZE){
      int i, j;
      wg_int encoded;

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
        encoded = wg_get_field(db,
          (void *)offsettoptr(db,node->array_of_values[i]), column);
        if(wg_decode_int(db,encoded) >= newvalue) {
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
    }
    else{
      //still, insert the value here, but move minimum out of this node
      //get the minimum element from this node
      int i, j;
      wg_int encoded, minvalue, minvaluerowoffset;

      minvalue = node->current_min;
      minvaluerowoffset = node->array_of_values[0];

      /* Now scan for the matching slot. However, since
       * we already know the 0 slot will be re-filled, we
       * do this scan (and sort) in reverse order, compared to the case
       * where array had some space left. */
      for(i=WG_TNODE_ARRAY_SIZE-1; i>0; i--) {
        encoded = wg_get_field(db,
          (void *)offsettoptr(db,node->array_of_values[i]), column);
        if(wg_decode_int(db,encoded) <= newvalue) {
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
        encoded = wg_get_field(db,
          (void *)offsettoptr(db,node->array_of_values[0]), column);
        node->current_min = wg_decode_int(db, encoded);
      }

      //proceed to the node that holds greatest lower bound - must be leaf (can be the initial bounding node)
      if(node->left_child_offset != 0){
        wg_int greatestlb = db_find_node_with_greatest_lower_bound(db,node->left_child_offset);
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
        if(newnode == 0)return 1;
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
        //here it seems that we must check one more thing
        if(bnodeoffset == ptrtooffset(db,node))node->left_child_offset = newnode;
        else node->right_child_offset = newnode;
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
      if(newnode == 0)return 1;
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
      }else if(boundtype == DEAD_END_RIGHT_NOT_BOUNDING){
        node->right_child_offset = newnode;
      }
    }
  }//no bounding node found - algorithm 2

  //if new node was added to tree - must update child height data in nodes from leaf to root
  //or until find a node with imbalance
  //then determine the bad balance case: LL, LR, RR or RL and execute proper rotation
  if(new){
    //printf("new node added with offset %d\n",new);

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
  /*
  wg_log_tree(db,"debug.xml",offsettoptr(db,dbh->index_control_area_header.index_array[index_id].offset_root_node));
  char a;
  scanf("%c",&a);
  printf("%d added\n",newvalue);*/
  return 0;
}

/**
*  returns:
*  0 - on success
*  1 - error (failed to create the index)
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
  
  printf("number of indexes in db currently = %d\n",
    dbh->index_control_area_header.number_of_indexes);

  if(column>=MAX_INDEXED_FIELDNR) {
    show_index_error_nr(db, "Max allowed column number",
      MAX_INDEXED_FIELDNR-1);
    return 1;
  }

  ilist = NULL;
  /* Check if T-tree index already exists on this column */
  if(dbh->index_control_area_header.index_table[column]) {
    ilist = offsettoptr(db, dbh->index_control_area_header.index_table[column]);
    for(;;) {
      if(!ilist->header_offset) {
        show_index_error(db, "Invalid header in index list");
        return 1;
      }
      hdr = offsettoptr(db, ilist->header_offset);
      if(hdr->type==DB_INDEX_TYPE_1_TTREE) {
        show_index_error(db, "TTree index already exists on column");
        return 1;
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
  nodest->current_max = 0;
  nodest->current_min = 0;
  nodest->number_of_elements = 0;
  nodest->left_child_offset = 0;
  nodest->right_child_offset = 0;

  printf("allocated (root)node offset = %d\n",node);

  hdr->offset_root_node = node;
  hdr->type = DB_INDEX_TYPE_1_TTREE;
  hdr->fields = 1;
  hdr->rec_field_index[0] = column;

  //scan all the data - make entry for every suitable row
  rec = wg_get_first_record(db);
  rowsprocessed = 0;
  
  while(rec != NULL) {
    fields = wg_get_record_len(db, rec);
    //check if column exists and type?!
    if(column >= fields){//|| wg_get_field_type(db, rec, column) != WG_INTTYPE
      rec=wg_get_next_record(db,rec);
      continue;
    }
    wg_add_new_row_into_index(db, index_id, rec);
    rowsprocessed++;
    rec=wg_get_next_record(db,rec);
  }

  printf("index slot %d root is %d\n", index_id, hdr->offset_root_node);
  printf("new index created on rec field %d into slot %d and %d data rows inserted\n",
    column, index_id, rowsprocessed);

  return 0;
}

/** Find index id (index header) by column
* Supports all types of indexes, calling program should examine the
* header of returned index to decide how to proceed.
*  returns:
*  -1 if no index found
*  offset > 0 if index found - index id
*/
wg_int wg_column_to_index_id(void *db, wg_int column){
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
      for(i=0; i<hdr->fields; i++) {
        if(hdr->rec_field_index[i]==column)
          return ilist->header_offset; /* index id */
      }
    }
    if(!ilist->next_offset)
      break;
    ilist = offsettoptr(db, ilist->next_offset);
  }
    
  return -1;
}

static void print_tree(void *db, FILE *file, struct wg_tnode *node){
  int i;

  fprintf(file,"<node>\n");
  fprintf(file,"<data_count>%d",node->number_of_elements);
  fprintf(file,"</data_count>\n");
  fprintf(file,"<left_subtree_height>%d",node->left_subtree_height);
  fprintf(file,"</left_subtree_height>\n");
  fprintf(file,"<right_subtree_height>%d",node->right_subtree_height);
  fprintf(file,"</right_subtree_height>\n");
  fprintf(file,"<min_max>%d %d",node->current_min,node->current_max);
  fprintf(file,"</min_max>\n");  
  fprintf(file,"<data>");
  for(i=0;i<node->number_of_elements;i++){
    wg_int encoded = wg_get_field(db, offsettoptr(db,node->array_of_values[i]), 0);
    fprintf(file,"%d ",wg_decode_int(db,encoded));
  }

  fprintf(file,"</data>\n");
  fprintf(file,"<left_child>\n");
  if(node->left_child_offset == 0)fprintf(file,"null");
  else{
    print_tree(db,file,offsettoptr(db,node->left_child_offset));
  }
  fprintf(file,"</left_child>\n");
  fprintf(file,"<right_child>\n");
  if(node->right_child_offset == 0)fprintf(file,"null");
  else{
    print_tree(db,file,offsettoptr(db,node->right_child_offset));
  }
  fprintf(file,"</right_child>\n");
  fprintf(file,"</node>\n");
}

int wg_log_tree(void *db, char *file, struct wg_tnode *node){
  db_memsegment_header* dbh = (db_memsegment_header*) db;
#ifdef _WIN32
  FILE *filee;
  fopen_s(&filee, file, "w");
#else
  FILE *filee = fopen(file,"w");
#endif
  print_tree(dbh,filee,node);
  fflush(filee);
  fclose(filee);
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
