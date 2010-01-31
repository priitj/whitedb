/*
* $Id:  $
* $Version: $
*
* Copyright (c) Enar Reilent 2009
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
#include <string.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "../Db/dbmem.h"
#include "../Db/dbindex.h"
#include "../Db/dbtest.h"
#include "../Db/dbquery.h"

/* ====== Private headers and defs ======== */

#ifdef _WIN32
#define sscanf sscanf_s  /* XXX: This will break for string parameters */
#endif


/* ======= Private protos ================ */

void print_tree(void *db, FILE *file, struct wg_tnode *node);
int wg_log_tree(void *db, char *file, struct wg_tnode *node);


/* ====== Functions ============== */

/* XXX: this will be moved to wgdb.c */
int selectdata(void *db, int howmany, int startingat){

  void *rec = wg_get_first_record(db);
  int i, fields, count;
  wg_int encoded;

  for(i=0;i<startingat;i++){
    if(rec == NULL) return 0;
    rec=wg_get_next_record(db,rec); 
  }

  count=0;
  while(rec != NULL) {
    fields = wg_get_record_len(db, rec);
    for(i=0;i<fields;i++){
      encoded = wg_get_field(db, rec, i);
      printf("%7d\t",wg_decode_int(db,encoded));
    }
    printf("\n");
    count++;
    if(count == howmany)break;
    rec=wg_get_next_record(db,rec);
  }

  return 0;
}

int findslow(void *db, int column, int key){

  void *rec = wg_get_first_record(db);
  int i, fields;
  wg_int encoded;

  while(rec != NULL) {
    encoded = wg_get_field(db, rec, column);
    if(wg_decode_int(db,encoded) == key){
      fields = wg_get_record_len(db, rec);
      printf("data row found: ");
      for(i=0;i<fields;i++){
        encoded = wg_get_field(db, rec, i);
        printf("%7d\t",wg_decode_int(db,encoded));
      }
      printf("\n");
      return 0;
    }
    
    rec=wg_get_next_record(db,rec);
  }
  printf("key %d not found\n",key);
  return 0;
}

static int printhelp(){
  printf("\nindextool user commands:\n");
  printf("indextool <base name> fill <nr of rows> [asc | desc | mix] - fill db\n");
  printf("indextool <base name> createindex <column> - create ttree index\n");
  printf("indextool <base name> select  <number of rows> <start from row> - print db contents\n");
  printf("indextool <base name> logtree <column> [filename] - log tree\n");
  printf("indextool <base name> header - print header data\n");
  printf("indextool <base name> fast <column> <key> - search data from index\n");
  printf("indextool <base name> slow <column> <key> - search data with sequencial scan\n");
  printf("indextool <base name> add <value1> <value2> <value3> - store data row and make an index entries\n");
  printf("indextool <base name> del <column> <key> - delete data row and its index entries\n");
  printf("indextool <base name> query <column> <cond> <val> .. - primitive query test\n");
  printf("indextool <base name> free - free db\n\n");
  return 0;
}


int main(int argc, char **argv) {
 
  char* shmname;
  char* shmptr;
  char* command;
  void *db;
  int recordsize = 3;
  
  if(argc < 3){
    printhelp();
    return 0;
  }

  command = argv[2];
  shmname = argv[1];

  shmptr=wg_attach_database(shmname,10000000); // 0 size causes default size to be used
  db = shmptr;
  if(shmptr==NULL){
    printf("wg_attach_database on (shmname=%s) gave fatal error\n",shmname);
    return 0;
  }

  if(strcmp(command,"free")==0) {
    // free shared memory and exit
    wg_delete_database(shmname);
    return 0;    
  }

  if(strcmp(command,"header")==0) {
    wg_show_db_memsegment_header(shmptr);
    return 0;    
  }

  if(strcmp(command,"fill")==0) {
    int n;
    if(argc < 4){printhelp();return 0;}
    sscanf(argv[3],"%d",&n);
    if(argc > 4 && strcmp(argv[4],"mix")==0)
      wg_genintdata_mix(db, n, recordsize);
    else if(argc > 4 && strcmp(argv[4],"desc")==0)
      wg_genintdata_desc(db, n, recordsize);
    else
      wg_genintdata_asc(db, n, recordsize);
    printf("data inserted\n");
    return 0;    
  }

  if(strcmp(command,"select")==0) {
    int s,c;
    if(argc < 5){printhelp();return 0;}
    sscanf(argv[3],"%d",&s);
    sscanf(argv[4],"%d",&c);
    selectdata(db,s,c);
    return 0;    
  }

  if(strcmp(command,"createindex")==0) {
    int col;
    if(argc < 4){printhelp();return 0;}
    sscanf(argv[3],"%d",&col);
    wg_create_ttree_index(db, col);
    return 0;    
  }

  if(strcmp(command,"logtree")==0) {
    int col, i;
    char *a = "tree.xml";

    if(argc < 4){printhelp();return 0;}
    sscanf(argv[3],"%d",&col);
    if(argc > 4)a = argv[4];
    i = wg_column_to_index_id(db, col, DB_INDEX_TYPE_1_TTREE);
    if(i!=-1) {
      wg_index_header *hdr = offsettoptr(db, i);
      wg_log_tree(db,a,offsettoptr(db,hdr->offset_root_node));
    }
    return 0;
  }
    
  if(strcmp(command,"slow")==0) {
    int c,k;
    if(argc < 5){printhelp();return 0;}
    sscanf(argv[3],"%d",&c);
    sscanf(argv[4],"%d",&k);
    findslow(db,c,k);
    return 0;    
  }  
  if(strcmp(command,"fast")==0) {
    int c,k,i;
    if(argc < 5){printhelp();return 0;}
    sscanf(argv[3],"%d",&c);
    sscanf(argv[4],"%d",&k);
    i = wg_column_to_index_id(db, c, DB_INDEX_TYPE_1_TTREE);
    if(i!=-1){
      wg_int encoded;
      wg_int offset = wg_search_ttree_index(db, i, k);
      if(offset != 0){
        void *rec = offsettoptr(db,offset);
        int fields = wg_get_record_len(db, rec);
        for(i=0;i<fields;i++){
          encoded = wg_get_field(db, rec, i);
          printf("%7d\t",wg_decode_int(db,encoded));
        }
        printf("\n");
      }else{
        printf("cannot find key %d in index\n",k);
      }
    }
    return 0;    
  }

  if(strcmp(command,"add")==0) {
    int a,b,c;
    void *rec;
    if(argc < 6){printhelp();return 0;}
    a=b=c=0;
    sscanf(argv[3],"%d",&a);
    sscanf(argv[4],"%d",&b);
    sscanf(argv[5],"%d",&c);
    rec = wg_create_record(db,3);
    if (rec==NULL) { 
      printf("rec creation error\n");
      return 1;
    }else{
      wg_set_int_field(db,rec,0,a);
      wg_set_int_field(db,rec,1,b);
      wg_set_int_field(db,rec,2,c);
      wg_index_add_rec(db, rec);
    }
    return 0;    
  }
  
  if(strcmp(command,"del")==0) {
    int c,k,i;
    void *rec = NULL;

    if(argc < 5){printhelp();return 0;}
    sscanf(argv[3],"%d",&c);
    sscanf(argv[4],"%d",&k);
    
    i = wg_column_to_index_id(db, c, DB_INDEX_TYPE_1_TTREE);
    if(i!=-1){
      wg_int offset = wg_search_ttree_index(db, i, k);
      if(offset != 0){
        rec = offsettoptr(db,offset);
      }
    }
    if(rec==NULL){
      printf("no such data\n");
      return 0;
    }
    wg_index_del_rec(db, rec);

    printf("deleted data from indexes, but no function for deleting the record\n");//wg_delete_record(db,rec);
    return 0;    
  }

  if(strcmp(command,"query")==0) {
    /* XXX: temporary testing code, will NOT stay here
     * for long. */
    int c,k,i;
    char cond[80];
    void *rec = NULL;
    wg_query *q;
    wg_query_arg arglist; /* space for one arg */

    if(argc < 6){printhelp();return 0;}
    sscanf(argv[3],"%d",&c);
    sscanf(argv[4],"%s",cond);
    sscanf(argv[5],"%d",&k);

    arglist.column = c;
    arglist.value = wg_encode_int(db, k);
    if(!strncmp(cond, "=", 1))
        arglist.cond = WG_COND_EQUAL;
    else if(!strncmp(cond, "!=", 2))
        arglist.cond = WG_COND_NOT_EQUAL;
    else if(!strncmp(cond, "<", 1))
        arglist.cond = WG_COND_LESSTHAN;
    else if(!strncmp(cond, ">", 1))
        arglist.cond = WG_COND_GREATER;
    else if(!strncmp(cond, "<=", 2))
        arglist.cond = WG_COND_LTEQUAL;
    else if(!strncmp(cond, ">=", 2))
        arglist.cond = WG_COND_GTEQUAL;
    else {
      fprintf(stderr, "invalid condition\n");
      return 0;
    }

    q = wg_make_query(db, &arglist, 1);
    if(!q)
      return 0;

    rec = wg_fetch(db, q);
    while(rec) {
      int fields = wg_get_record_len(db, rec);
      gint encoded;

      for(i=0;i<fields;i++){
        encoded = wg_get_field(db, rec, i);
        printf("%7d\t",wg_decode_int(db,encoded));
      }
      printf("\n");
      rec = wg_fetch(db, q);
    }
    wg_free_query(db, q);
    return 0;    
  }

  printhelp();
  return 0;  
}

void print_tree(void *db, FILE *file, struct wg_tnode *node){
  int i;

  fprintf(file,"<node offset = \"%d\">\n", ptrtooffset(db, node));
  fprintf(file,"<data_count>%d",node->number_of_elements);
  fprintf(file,"</data_count>\n");
  fprintf(file,"<left_subtree_height>%d",node->left_subtree_height);
  fprintf(file,"</left_subtree_height>\n");
  fprintf(file,"<right_subtree_height>%d",node->right_subtree_height);
  fprintf(file,"</right_subtree_height>\n");
#ifdef TTREE_CHAINED_NODES
  fprintf(file,"<successor>%d</successor>\n", node->succ_offset);
  fprintf(file,"<predecessor>%d</predecessor>\n", node->pred_offset);
#endif
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
