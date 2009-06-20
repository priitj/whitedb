#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <conio.h> // for _getch
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
//#include "../config.h"
#endif
#include "dbmem.h"
#include "dballoc.h"
#include "dbdata.h"
//#include "../Db/dbapi.h"

#include "dbtest.h"



#define REALLY_BOUNDING_NODE 0
#define DEAD_END_LEFT_NOT_BOUNDING 1
#define DEAD_END_RIGHT_NOT_BOUNDING 2

#define LL_CASE 0
#define LR_CASE 1
#define RL_CASE 2
#define RR_CASE 3

int log_tree(void *db, char *file, struct wg_tnode *node);

/**
*	returns bounding node offset or if no really bounding node exists, then the closest node
*/
wg_int db_find_bounding_tnode(void *db, wg_int rootoffset, wg_int key, wg_int *result){
	struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,rootoffset);
	if(key>=node->current_min && key<=node->current_max){
		*result = REALLY_BOUNDING_NODE;
		return rootoffset;
	}

	if(key < node->current_min){
		if(node->left_child_offset != 0) return db_find_bounding_tnode(db, node->left_child_offset, key, result);
		else{
			*result = DEAD_END_LEFT_NOT_BOUNDING;
			return rootoffset;
		}
	}
	if(key > node->current_max){
		if(node->right_child_offset != 0) return db_find_bounding_tnode(db, node->right_child_offset, key, result);
		else{
			*result = DEAD_END_RIGHT_NOT_BOUNDING;
			return rootoffset;
		}
	}
}

/**
*	returns offset of the node with greatest lower bound
*	goes only right - so: must call on the left child of the real node under processing
*				not on the real node under processing
*/
static wg_int db_find_node_with_greatest_lower_bound(void *db, wg_int nodeoffset){
	struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,nodeoffset);
	if(node->right_child_offset != 0) return db_find_node_with_greatest_lower_bound(db, node->right_child_offset);
	else return nodeoffset;
}

/**
*	returns the description of imbalance - 4 cases possible
*	LL - left child of the left child is overweight
*	LR - right child of the left child is overweight
*	etc
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

static wg_int max(wg_int a, wg_int b){
	if(a > b) return a;
	else return b;
}

static int db_rotate_ttree(void *db, wg_int index_id, struct wg_tnode *root, int overw){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	wg_int column = dbh->index_control_area_header.index_array[index_id].rec_field_index;
	wg_int grandparent = root->parent_offset;
	wg_int initialrootoffset = ptrtooffset(db,root);
	struct wg_tnode *r;
	struct wg_tnode *g = (struct wg_tnode *)offsettoptr(db,grandparent);

	if(overw == LL_CASE){

/*				A			B
*			     B     C		     D     A
*			   D  E		->         N     E  C 
*			  N
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

/*				A			C
*			     B     C		     A     E
*			         D   E	   ->      B  D      N 
*			              N
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

/*			A                    E
*		     B     C             B       A
*	         D    E        ->     D  F    G    C
*	            F  G                 N
*	            N
*/
		//printf("LR_CASE\n");

		//save some stuff into variables for later use
		wg_int offset_left_child = root->left_child_offset;
		wg_int offset_right_grandchild = ((struct wg_tnode *)offsettoptr(db,offset_left_child))->right_child_offset;
		
		//first swtich: G goes to A's left child
		struct wg_tnode *ee = (struct wg_tnode *)offsettoptr(db,offset_right_grandchild);
		root -> left_child_offset = ee -> right_child_offset;
		root -> left_subtree_height = ee -> right_subtree_height;
		if(ee -> right_child_offset != 0){
			((struct wg_tnode *)offsettoptr(db,ee->right_child_offset))->parent_offset = ptrtooffset(db, root);
		}
		//second switch: F goes to B's right child
		struct wg_tnode *bb = (struct wg_tnode *)offsettoptr(db,offset_left_child);
		bb -> right_child_offset = ee -> left_child_offset;
		bb -> right_subtree_height = ee -> left_subtree_height;
		if(ee -> left_child_offset != 0){
			((struct wg_tnode *)offsettoptr(db,ee->left_child_offset))->parent_offset = offset_left_child;
		}
		//third switch: B goes to E's left child
		//maybe slide some elements first (from full node to empty node)
		if(ee -> number_of_elements == 1 && bb -> number_of_elements == WG_TNODE_ARRAY_SIZE){
			int i;
			int minindex = -1;
			wg_int tmpmin = ee -> current_min;
			wg_int val;
			for(i=0;i<WG_TNODE_ARRAY_SIZE;i++){
				//take one value from b
				val = wg_decode_int(db,wg_get_field(db,(void *)offsettoptr(db,bb->array_of_values[i]),column));
				//leave minimum (only one copy) to node bb, other elements move from b to e
				if(minindex != -1 || val != bb->current_min){//minimum found or this is not the minimum
					ee -> array_of_values[ee->number_of_elements] = bb->array_of_values[i];
					ee -> number_of_elements++;
					if(tmpmin > val) tmpmin = val;
					bb -> array_of_values[i] = 0;
				}else if(val == bb->current_min){
					minindex = i;
				}
			}
			ee -> current_min = tmpmin;
			bb -> number_of_elements = 1;
			bb -> current_max = bb -> current_min;
			//put the only value left in b to the first position
			if(minindex != 0){
				bb -> array_of_values[0] = bb -> array_of_values[minindex];
				bb -> array_of_values[minindex] = 0;
			}
			
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

/*			A                    E
*		     C     B             A       B
*	         	 E   D  ->     C  G    F   D
*	               G  F                    N
*	                  N
*/
		//printf("RL_CASE\n");
		//save some stuff into variables for later use
		wg_int offset_right_child = root->right_child_offset;
		wg_int offset_left_grandchild = ((struct wg_tnode *)offsettoptr(db,offset_right_child))->left_child_offset;
		
		//first swtich: G goes to A's left child
		struct wg_tnode *ee = (struct wg_tnode *)offsettoptr(db,offset_left_grandchild);
		root -> right_child_offset = ee -> left_child_offset;
		root -> right_subtree_height = ee -> left_subtree_height;
		if(ee -> left_child_offset != 0){
			((struct wg_tnode *)offsettoptr(db,ee->left_child_offset))->parent_offset = ptrtooffset(db, root);
		}

		//second switch: F goes to B's right child
		struct wg_tnode *bb = (struct wg_tnode *)offsettoptr(db,offset_right_child);
		bb -> left_child_offset = ee -> right_child_offset;
		bb -> left_subtree_height = ee -> right_subtree_height;
		if(ee -> right_child_offset != 0){
			((struct wg_tnode *)offsettoptr(db,ee->right_child_offset))->parent_offset = offset_right_child;
		}

		//third switch: B goes to E's left child
		if(ee -> number_of_elements == 1 && bb -> number_of_elements == WG_TNODE_ARRAY_SIZE){//slide elements first
			int i;
			int maxindex = -1;
			wg_int tmpmax = ee -> current_max;
			wg_int val;
			for(i=0;i<WG_TNODE_ARRAY_SIZE;i++){
				val = wg_decode_int(db,wg_get_field(db,(void *)offsettoptr(db,bb->array_of_values[i]),column));
				//leave maximum (only one copy) to node bb, other elements move from b to e
				if(maxindex != -1 || val != bb->current_max){//maximum found or this is not the minimum
					ee -> array_of_values[ee->number_of_elements] = bb->array_of_values[i];
					ee -> number_of_elements++;
					if(tmpmax < val) tmpmax = val;
					bb -> array_of_values[i] = 0;
				}else if(val == bb->current_max){
					maxindex = i;
				}
			}
			ee -> current_max = tmpmax;
			bb -> number_of_elements = 1;
			bb -> current_min = bb -> current_max;
			if(maxindex != 0){//move the value to the first position
				bb -> array_of_values[0] = bb -> array_of_values[maxindex];
				bb -> array_of_values[maxindex] = 0;
			}
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
		//printf("change index header\n");
		db_memsegment_header* dbh;
		dbh = (db_memsegment_header*) db;
		//TODO more error check here
		dbh->index_control_area_header.index_array[index_id].offset_root_node = ptrtooffset(db,r);
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
*	returns offset to data row:
*	-1 - error, index does not exist
*	0 - if key NOT found
*	other integer - if key found (= offset to data row)
*/
wg_int db_search_ttree_index(void *db, wg_int index_id, wg_int key){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	//identify index and check something if necessary
	if(index_id >= maxnumberofindexes) return -1;
	wg_int rootoffset = dbh->index_control_area_header.index_array[index_id].offset_root_node;
	
	if(rootoffset == 0){
		printf("index number %d does not exist\n",index_id);
		return -1;
	}
	//(binary) search for bounding node
	wg_int bnodetype;
	wg_int bnodeoffset = db_find_bounding_tnode(db, rootoffset, key, &bnodetype);
	struct wg_tnode * node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);

	//search for the key inside the bounding node if the node was not a dead end
	if(bnodetype==DEAD_END_LEFT_NOT_BOUNDING)return 0;
	if(bnodetype==DEAD_END_RIGHT_NOT_BOUNDING)return 0;

	int i;
	wg_int rowoffset;
	wg_int column = dbh->index_control_area_header.index_array[index_id].rec_field_index;
	wg_int encoded;
	for(i=0;i<node->number_of_elements;i++){
		rowoffset = node->array_of_values[i];
		encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
		if(wg_decode_int(db,encoded) == key) return rowoffset;
	}

	return 0;
}

/**	removes pointer to data row from index tree structure
*
*	returns:
*	0 - on success
*	1 - if error, index doesnt exist
*	2 - if error, no bounding node for key
*	3 - if error, boundig node exists, value not
*	4 - if error, tree not in balance
*/
wg_int db_remove_key_from_index(void *db, wg_int index_id, wg_int key){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	//get root node of this index tree
	if(index_id >= maxnumberofindexes) return 1;
	wg_int rootoffset = dbh->index_control_area_header.index_array[index_id].offset_root_node;
	wg_int column = dbh->index_control_area_header.index_array[index_id].rec_field_index;

	if(rootoffset == 0){
		printf("index number %d does not exist\n",index_id);
		return 1;
	}

	//find bounding node for the value
	wg_int boundtype;
	wg_int bnodeoffset = db_find_bounding_tnode(db, rootoffset, key, &boundtype);
	struct wg_tnode *node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
	
	//if bounding node does not exist - error
	if(boundtype != REALLY_BOUNDING_NODE) return 2;
	
	//find the value inside the node
	int i;
	int found = -1;
	wg_int encoded;
	for(i=0;i<node->number_of_elements;i++){
		encoded = wg_get_field(db, offsettoptr(db,node->array_of_values[i]), column);
		if(wg_decode_int(db,encoded)==key){found = i; break;}
	}

	if(found == -1) return 3;

	//delete the key and rearrange other elements
	if(found == node->number_of_elements - 1){//last element
		node -> number_of_elements--;
	}else{
		node -> array_of_values[found] = node -> array_of_values[node->number_of_elements - 1];
		node -> number_of_elements--;
	}
	//maybe fix min or max variables
	wg_int rowoffset;
	if(key == node->current_max && node -> number_of_elements != 0){
		wg_int tmpmax = wg_decode_int(db,wg_get_field(db,(void *)offsettoptr(db,node->array_of_values[0]),column));
		for(i=1;i<node->number_of_elements;i++){
			rowoffset = node->array_of_values[i];
			encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
			if(wg_decode_int(db,encoded) > tmpmax) tmpmax = wg_decode_int(db,encoded);
		}
		node -> current_max = tmpmax;
	}else if(key == node->current_min && node -> number_of_elements != 0){
		wg_int tmpmin = wg_decode_int(db,wg_get_field(db,(void *)offsettoptr(db,node->array_of_values[0]),column));
		for(i=1;i<node->number_of_elements;i++){
			rowoffset = node->array_of_values[i];
			encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
			if(wg_decode_int(db,encoded) < tmpmin) tmpmin = wg_decode_int(db,encoded);
		}
		node -> current_min = tmpmin;
	}

	//check underflow and take some actions if needed
	if(node->number_of_elements < 5){//TODO use macro
		//if the node is internal node - borrow its gratest lower bound from the node where it is
		if(node->left_child_offset != 0 && node->right_child_offset != 0){//internal node
			wg_int greatestlb = db_find_node_with_greatest_lower_bound(db,node->left_child_offset);
			struct wg_tnode *glbnode = (struct wg_tnode *)offsettoptr(db, greatestlb);
			//get the glb value position
			for(i=0;i<glbnode->number_of_elements;i++){
				rowoffset = glbnode->array_of_values[i];
				encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
				if(wg_decode_int(db,encoded) == glbnode -> current_max) break;
			}
			//insert this value into node
			node -> array_of_values[node->number_of_elements]=glbnode->array_of_values[i];
			node -> number_of_elements++;
			node -> current_min = glbnode -> current_max;
			//remove it from glbnode (leave no gap in array)
			glbnode -> array_of_values[i] = glbnode -> array_of_values[glbnode->number_of_elements-1];
			glbnode -> number_of_elements--;
			
			//reset new max for glbnode
			wg_int tmpmax = glbnode -> array_of_values[0];
			for(i=1;i<glbnode->number_of_elements;i++){
				rowoffset = glbnode->array_of_values[i];
				encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
				if(wg_decode_int(db,encoded) > tmpmax) tmpmax = wg_decode_int(db,encoded);
			}
			glbnode->current_max = tmpmax;
			node = glbnode;
		}
	}

	//now variable node points to the node which really lost an element
	//this is definitely leaf or half-leaf
	//if the node is empty - free it and rebalanc the tree
	struct wg_tnode *parent = NULL;
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
		free_tnode(db, ptrtooffset(db,node));
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
			for(j=0;j<child->number_of_elements;j++){
				node->array_of_values[i]=child->array_of_values[j];
			}
			node->number_of_elements = elements;
			if(left){
				node->left_subtree_height=0;
				node->left_child_offset=0;
				node->current_min=child->current_min;
			}else{
				node->right_subtree_height=0;
				node->right_child_offset=0;
				node->current_max=child->current_max;
			}
			free_tnode(db, ptrtooffset(db,node));
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
		int balance;
		int height;
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
				//manually set grandparent subtree heights
				height = max(parent->left_subtree_height,parent->right_subtree_height);
				struct wg_tnode *gp = (struct wg_tnode *)offsettoptr(db, parent->parent_offset);
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

/**	inserts pointer to data row into index tree structure
*
*	returns:
*	0 - on success
*	1 - if error
*/
wg_int db_add_new_row_into_index(void *db, wg_int index_id, void *rec){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	//get root node of this index tree
	if(index_id >= maxnumberofindexes) return 1;
	wg_int rootoffset = dbh->index_control_area_header.index_array[index_id].offset_root_node;
	
	if(rootoffset == 0){
		printf("index number %d does not exist\n",index_id);
		return 1;
	}
	//extract real value from the row (rec)
	wg_int column = dbh->index_control_area_header.index_array[index_id].rec_field_index;
	wg_int encoded = wg_get_field(db, rec, column);
	wg_int newvalue = wg_decode_int(db,encoded);

	//find bounding node for the value
	wg_int boundtype;
	wg_int bnodeoffset = db_find_bounding_tnode(db, rootoffset, newvalue, &boundtype);
	struct wg_tnode *node = (struct wg_tnode *)offsettoptr(db,bnodeoffset);
	wg_int new = 0;//save here the offset of newly created tnode - 0 if no node added into the tree
	//if bounding node exists - follow one algorithm, else the other
	if(boundtype == REALLY_BOUNDING_NODE){

		//check if the node has room for a new entry
		if(node->number_of_elements < WG_TNODE_ARRAY_SIZE){

			//add array entry and update control data
			node->array_of_values[node->number_of_elements] = ptrtooffset(db,rec);//save offset, use first free slot
			node->number_of_elements++;
			if(node->number_of_elements==1){//it was empty node, must set max and min for the first time
				node->current_max = newvalue;
				node->current_min = newvalue;
			}else{
				if(node->current_max < newvalue)node->current_max = newvalue;
				if(node->current_min > newvalue)node->current_min = newvalue;
			}
		}else{
			//still, insert the value here, but move minimum out of this node
			//get the minimum element from this node
			int i;
			wg_int rowoffset;
			wg_int encoded;
			for(i=0;i<node->number_of_elements;i++){
				rowoffset = node->array_of_values[i];
				encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
				if(wg_decode_int(db,encoded) == node->current_min) break;//i has the wanted value
			}
			wg_int minvalue = node->current_min;
			wg_int minvaluerowoffset = node->array_of_values[i];

			//insert new value in the place of old minimum
			node->array_of_values[i]=ptrtooffset(db,rec);
			//change current_min of the node
			node->current_min = newvalue;
			for(i=0;i<node->number_of_elements;i++){
				rowoffset = node->array_of_values[i];
				encoded = wg_get_field(db, (void *)offsettoptr(db,rowoffset), column);
				if(wg_decode_int(db,encoded) < node->current_min) node->current_min = wg_decode_int(db,encoded);
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
				gint newnode = alloc_fixlen_object(db, &dbh->tnode_area_header);
				if(newnode == 0)return 1;
				struct wg_tnode *leaf =(struct wg_tnode *)offsettoptr(db,newnode);
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

			//add array entry and update control data
			node->array_of_values[node->number_of_elements] = ptrtooffset(db,rec);//save offset, use first free slot
			node->number_of_elements++;
			if(node->number_of_elements==1){//it was empty node, must set max and min for the first time
				node->current_max = newvalue;
				node->current_min = newvalue;
			}else{
				if(node->current_max < newvalue)node->current_max = newvalue;
				if(node->current_min > newvalue)node->current_min = newvalue;
			}
		}else{
			//make a new node and put data there
			gint newnode = alloc_fixlen_object(db, &dbh->tnode_area_header);
			if(newnode == 0)return 1;
			struct wg_tnode *leaf =(struct wg_tnode *)offsettoptr(db,newnode);
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
			parent = (struct wg_tnode *)offsettoptr(db,child->parent_offset);
			//determine which child the child is, left or right one
			if(parent->left_child_offset == ptrtooffset(db,child)) left = 1;
			else left = 0;
			//increment parent left or right subtree height
			if(left)parent->left_subtree_height++;
			else parent->right_subtree_height++;

			//check balance
			int balance = parent->left_subtree_height - parent->right_subtree_height;
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
	log_tree(db,"debug.xml",offsettoptr(db,dbh->index_control_area_header.index_array[index_id].offset_root_node));
	char a;
	scanf("%c",&a);
	printf("%d added\n",newvalue);*/
	return 0;
}

/**
*	returns:
*	0 - on success
*	1 - if no room for index header
*/
wg_int db_create_ttree_index(void *db, wg_int column){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	
	printf("number of indexes in db currently = %d\n",dbh->index_control_area_header.number_of_indexes);
	int i=0;
	//search for empty slot in index_array
	for(i=0;i<maxnumberofindexes;i++){
		//printf("index %d root is %d\n",i,dbh->index_control_area_header.index_array[i].offset_root_node);
		if(dbh->index_control_area_header.index_array[i].offset_root_node==0)break;
	}
	if(i==maxnumberofindexes){
		printf("no free slot for new index, cannot create index\n");
		return 1;
	}
	//increase index counter
	dbh->index_control_area_header.number_of_indexes++;
	
	
	//allocate (+ init) root node for new index tree and save the offset into index_array
	
	gint node = alloc_fixlen_object(db, &dbh->tnode_area_header);
	struct wg_tnode *nodest =(struct wg_tnode *)offsettoptr(db,node);
	nodest->parent_offset = 0;
	nodest->left_subtree_height = 0;
	nodest->right_subtree_height = 0;
	nodest->current_max = 0;
	nodest->current_min = 0;
	nodest->number_of_elements = 0;
	nodest->left_child_offset = 0;
	nodest->right_child_offset = 0;

	printf("allocated (root)node offset = %d\n",node);
	dbh->index_control_area_header.index_array[i].offset_root_node = node;
	dbh->index_control_area_header.index_array[i].type = DB_INDEX_TYPE_1_TTREE;
	dbh->index_control_area_header.index_array[i].rec_field_index = column;
	

	//scan all the data - make entry for every suitable row
	void *rec = wg_get_first_record(db);
	unsigned int rowsprocessed = 0;
	int fields;
	
	while(rec != NULL) {
		fields = wg_get_record_len(db, rec);
		//check if column exists and type?!
		if(column >= fields){//|| wg_get_field_type(db, rec, column) != WG_INTTYPE
			rec=wg_get_next_record(db,rec);
			continue;
		}
		db_add_new_row_into_index(db, i, rec);
		rowsprocessed++;
		rec=wg_get_next_record(db,rec);
	}

	printf("index slot %d root is %d\n",i,dbh->index_control_area_header.index_array[i].offset_root_node);
	printf("new index created on rec field %d into slot %d and %d data rows inserted\n",column,i,rowsprocessed);

	return 0;
}

/**
*	scans index array and finds out index id by column
*	returns:
*	-1 if no index found
*	integer >= 0 if index found - index id
*/
wg_int db_column_to_index_id(void *db, wg_int column){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	int i;
	for(i=0;i<maxnumberofindexes;i++){
		if(dbh->index_control_area_header.index_array[i].rec_field_index == column)return i;
	}
	return -1;
}

int print_tree(void *db, FILE *file, struct wg_tnode *node){

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
	int i;
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

int log_tree(void *db, char *file, struct wg_tnode *node){
	db_memsegment_header* dbh;
	dbh = (db_memsegment_header*) db;
	FILE *filee = fopen(file,"w");
	print_tree(db,filee,node);
	fflush(filee);
	fclose(filee);
	return 0;
}
