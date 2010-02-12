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

 /** @file indextool.c
 *  Command line utility for index manipulation
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
#include "../Db/dbutil.h"

/* ====== Private headers and defs ======== */

#ifdef _WIN32
#define sscanf sscanf_s  /* XXX: This will break for string parameters */
#endif


/* ======= Private protos ================ */

void print_tree(void *db, FILE *file, struct wg_tnode *node);
int wg_log_tree(void *db, char *file, struct wg_tnode *node);


/* ====== Functions ============== */

static int printhelp(){
  printf("\nindextool user commands:\n");
  printf("indextool [shmname] createindex <column> - create ttree index\n");
  printf("indextool [shmname] dropindex <column> - delete ttree index\n");
  printf("indextool [shmname] logtree <column> [filename] - log tree\n\n");
  return 0;
}


int main(int argc, char **argv) {
 
  char* shmname = NULL;
  void *db;
  int i, scan_to, shmsize;
  
  if(argc < 3) scan_to = argc;
  else scan_to = 3;
  shmsize = 0; /* 0 size causes default size to be used */

  /* Similar command parser as in wgdb.c */
  for(i=1; i<scan_to;) {
    if (!strcmp(argv[i],"help") || !strcmp(argv[i],"-h")) {
      printhelp();
      return 0;
    }

    if(!strcmp(argv[i], "createindex")) {
      int col;
      if(argc < (i+2)) {
        printhelp();
        return 0;
      }
      db = (void *) wg_attach_database(shmname, shmsize);
      if(!db) {
        fprintf(stderr, "Failed to attach to database.\n");
        return 0;
      }
      sscanf(argv[i+1], "%d", &col);
      wg_create_ttree_index(db, col);
      return 0;    
    }

    else if(!strcmp(argv[i], "dropindex")) {
      int col;
      if(argc < (i+2)) {
        printhelp();
        return 0;
      }
      db = (void *) wg_attach_database(shmname, shmsize);
      if(!db) {
        fprintf(stderr, "Failed to attach to database.\n");
        return 0;
      }
      sscanf(argv[i+1], "%d", &col);
      if(wg_drop_ttree_index(db, col))
        fprintf(stderr, "Failed to drop index.\n");
      else
        printf("Index dropped.\n");
      return 0;
    }

    else if(!strcmp(argv[i], "logtree")) {
      int col, j;
      char *a = "tree.xml";

      if(argc < (i+2)) {
        printhelp();
        return 0;
      }
      db = (void *) wg_attach_database(shmname, shmsize);
      if(!db) {
        fprintf(stderr, "Failed to attach to database.\n");
        return 0;
      }
      sscanf(argv[i+1], "%d", &col);
      if(argc > (i+2)) a = argv[i+2];
      j = wg_column_to_index_id(db, col, DB_INDEX_TYPE_1_TTREE);
      if(j!=-1) {
        wg_index_header *hdr = offsettoptr(db, j);
        wg_log_tree(db, a, offsettoptr(db, hdr->offset_root_node));
      }
      return 0;
    }
    
#if 0
    /* Old test query, finds 1 row from T-tree index by
     * column and integer value */
    else if(!strcmp(argv[i], "fast")) {
      int c,k,j;
      if(argc < (i+3)) {
        printhelp();
        return 0;
      }
      sscanf(argv[i+1],"%d",&c);
      sscanf(argv[i+2],"%d",&k);
      j = wg_column_to_index_id(db, c, DB_INDEX_TYPE_1_TTREE);
      if(j!=-1){
        wg_int encoded = wg_encode_int(db, k);
        wg_int offset = wg_search_ttree_index(db, j, encoded);
        if(offset != 0){
          void *rec = offsettoptr(db,offset);
          int fields = wg_get_record_len(db, rec);
          for(j=0;j<fields;j++){
            encoded = wg_get_field(db, rec, j);
            printf("%7d\t",wg_decode_int(db,encoded));
          }
          printf("\n");
        }else{
          printf("cannot find key %d in index\n",k);
        }
      }
      return 0;    
    }
#endif
    shmname = argv[1]; /* assuming two loops max */
    i++;
  }

  printhelp();
  return 0;  
}

void print_tree(void *db, FILE *file, struct wg_tnode *node){
  int i;
  char strbuf[256];

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
  wg_snprint_value(db, node->current_min, strbuf, 255);
  fprintf(file,"<min_max>%s ",strbuf);
  wg_snprint_value(db, node->current_max, strbuf, 255);
  fprintf(file,"%s</min_max>\n",strbuf);  
  fprintf(file,"<data>");
  for(i=0;i<node->number_of_elements;i++){
    wg_int encoded = wg_get_field(db, offsettoptr(db,node->array_of_values[i]), 0);
    wg_snprint_value(db, encoded, strbuf, 255);
    fprintf(file, "%s ", strbuf);
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
