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

/* ====== Private headers and defs ======== */

#ifdef _WIN32
#define sscanf sscanf_s  /* XXX: This will break for string parameters */
#endif


/* ====== Functions ============== */

int filldb(void *db, int databasesize, int recordsize){

  int i, j, tmp;
  void *rec;
  wg_int value = 0;
  int increment = 1;
  int incrementincrement = 17;
  int k = 0;
  for (i=0;i<databasesize;i++) {
    rec=wg_create_record(db,recordsize);
    if (rec==NULL) { 
      printf("rec creation error\n");
      continue;
    }
    
    for(j=0;j<recordsize;j++){
      tmp=wg_set_int_field(db,rec,j,value+j);
      if (tmp!=0) { 
        printf("int storage error\n");   
      }
    }
    value += increment;
    if(k % 2 == 0)increment += 2;
    else increment -= 1;
    k++;
    if(k == incrementincrement) {increment = 1; k = 0;}
  } 

  return 0;
}

int filldb2(void *db, int databasesize, int recordsize){

  int i, j, tmp;
  void *rec;
  wg_int value = 100000;
  int increment = -1;
  int incrementincrement = 17;
  int k = 0;
  for (i=0;i<databasesize;i++) {
    rec=wg_create_record(db,recordsize);
    if (rec==NULL) { 
      printf("rec creation error\n");
      continue;
    }
    
    for(j=0;j<recordsize;j++){
      tmp=wg_set_int_field(db,rec,j,value+j);
      if (tmp!=0) { 
        printf("int storage error\n");   
      }
    }
    value += increment;
    if(k % 2 == 0)increment -= 2;
    else increment += 1;
    k++;
    if(k == incrementincrement) {increment = -1; k = 0;}
  } 

  return 0;
}


int filldb3(void *db, int databasesize, int recordsize){

  int i, j, tmp;
  void *rec;
  wg_int value = 1;
  int increment = 1;
  int incrementincrement = 18;
  int k = 0;
  for (i=0;i<databasesize;i++) {
    rec=wg_create_record(db,recordsize);
    if (rec==NULL) { 
      printf("rec creation error\n");
      continue;
    }
    if(k % 2)value = 10000 - value;
    for(j=0;j<recordsize;j++){
      tmp=wg_set_int_field(db,rec,j,value+j);
      if (tmp!=0) { 
        printf("int storage error\n");   
      }
    }
    if(k % 2)value = 10000 - value;
    value += increment;
    

    if(k % 2 == 0)increment += 2;
    else increment -= 1;
    k++;
    if(k == incrementincrement) {increment = 1; k = 0;}
  } 

  return 0;
}

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
    if(argc > 4 && strcmp(argv[4],"mix")==0) filldb3(db, n, recordsize);
    else if(argc > 4 && strcmp(argv[4],"desc")==0)filldb2(db, n, recordsize);
    else filldb(db, n, recordsize);
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
    i = wg_column_to_index_id(db, col);
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
    i = wg_column_to_index_id(db, c);
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
      int i;
      db_memsegment_header* dbh = (db_memsegment_header*) db;

      wg_set_int_field(db,rec,0,a);
      wg_set_int_field(db,rec,1,b);
      wg_set_int_field(db,rec,2,c);

      /* Following code demonstrates updating indexes when inserting a data
       * row. XXX: this should probably be moved to dbindex.c as a "wg_"
       * function.
       */

      for(i=0;i<3;i++){
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
              wg_add_new_row_into_index(db, ilist->header_offset, rec);
            }
          }
          if(!ilist->next_offset)
            break;
          ilist = offsettoptr(db, ilist->next_offset);
        }
      }
    }
    return 0;    
  }
  
  if(strcmp(command,"del")==0) {
    int c,k,i,reclen;
    db_memsegment_header* dbh = (db_memsegment_header*) db;
    void *rec = NULL;

    if(argc < 5){printhelp();return 0;}
    sscanf(argv[3],"%d",&c);
    sscanf(argv[4],"%d",&k);
    
    i = wg_column_to_index_id(db, c);
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
    
    reclen = wg_get_record_len(db, rec);

    /* Delete record from all indexes.
     * XXX: this should probably be a "wg_" function.
     */
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
             * "add" command.
             */
            wg_remove_key_from_index(db, ilist->header_offset, rec);
          }
        }
        if(!ilist->next_offset)
          break;
        ilist = offsettoptr(db, ilist->next_offset);
      }
    }

    printf("deleted data from indexes, but no function for deleting the record\n");//wg_delete_record(db,rec);
    return 0;    
  }

  printhelp();
  return 0;  
}
