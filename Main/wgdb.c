/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
*
* Contact: tanel.tammet@gmail.com                 
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

 /** @file wgdb.c
 *  wgandalf database tool: command line utility
 */

/* ====== Includes =============== */



#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <conio.h> // for _getch
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
//#include "../Db/dbapi.h"
#include "../Db/dbtest.h"
#include "../Db/dbdump.h"
#include "../Db/dblog.h"
#include "wgdb.h"



/* ====== Private defs =========== */


/* ======= Private protos ================ */


int db_write(void* db);
int db_read(void* db);


/* ====== Global vars ======== */


/* ====== Private vars ======== */


/* ====== Functions ============== */


/*
how to set 500 meg of shared memory:

su
echo 500000000  > /proc/sys/kernel/shmmax 
*/

/** top level for the database command line tool
*
*
*/

int main(int argc, char **argv) {
 
  char* shmname;
  char* shmptr;
    //void *shm;
  //int tmp;
  
  printf("hello from wgdb, argc: %d \n",argc);
  // memdbase command? if yes, perform and exit.
  if (argc>1) shmname=argv[1];
  else shmname=NULL;
  
  if (argc>2 && !strcmp(argv[2],"free")) {
    // free shared memory and exit
    wg_delete_database(shmname);
    exit(0);    
  } 
  
  shmptr=wg_attach_database(shmname,0); // 0 size causes default size to be used
    
  printf("wg_attach_database on %d gave ptr %x\n",DEFAULT_MEMDBASE_KEY,(int)shmptr);
  if (shmptr==NULL) return 0;
  
  //db_read(shmptr);
  
  if(argc>2 && !strcmp(argv[2],"import")){
    //import dump
    wg_import_dump(shmptr,argv[3]);       
  } else if(argc>2 && !strcmp(argv[2],"export")){
    db_write(shmptr);
    wg_dump(shmptr,argv[3]); 
  } else if(argc>2 && !strcmp(argv[2],"log")) {
    db_write(shmptr);
    wg_print_log(shmptr);
    wg_dump_log(shmptr,argv[3]);
  } else if(argc>2 && !strcmp(argv[2],"importlog")) {    
    wg_import_log(shmptr,argv[3]);
  } else if(argc>=2 && !strcmp(argv[1],"test")) {
    printf("cp1\n");    
    check_datatype_writeread(shmptr);
    show_strhash(shmptr);
    //wg_delete_database(shmname);
    return 0;
  } else {
    db_write(shmptr);
  }  
  
  //show_db_memsegment_header(shmptr);
  //tmp=db_test1(shmptr);
  //printf("db_test returned %d \n",tmp);
  //show_db_memsegment_header(shmptr);
  //wg_detach_database(shmptr);   
  //wg_delete_database(shmname);
  
  //db_example(shmptr);  
  db_read(shmptr);
  wg_delete_database(shmname);  
#ifdef _WIN32  
  _getch();  
#endif  
  return 0;  
}


/*

db_example is for simple functionality testing

*/

int db_write(void* db) {
  void* rec=(char*)1;
  int i; 
  int j;
  int c;
  int flds;
  int records=3;
  int tmp=0;
  
  printf("********* db_example starts ************\n");
  flds=6;
  c=1;
  for (i=0;i<records;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }      
    printf("wg_create_record(db) gave new adr %d offset %d\n",(int)rec,ptrtooffset(db,rec));      
    for(j=0;j<flds;j++) {
      tmp=wg_set_int_field(db,rec,j,c);
      //tmp=wg_set_field(db,rec,j,wg_encode_int(db,c));
      //fieldadr=((gint*)rec)+RECORD_HEADER_GINTS+j;
      //*fieldadr=wg_encode_int(db,c);
      //printf("computed fieldadr %d\n",fieldadr);
      //tmp2=wg_get_field(db,rec,j);
      //printf("wg_get_field gave raw %d\n",(int)tmp2);
      //printf("at fieldadr %d\n",(int)*fieldadr);
      c++;
      if (tmp!=0) { 
        printf("int storage error");
        exit(0);    
      }
    }           
  } 
  printf("gint: %d\n",sizeof(gint));
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("first record adr %x offset %d\n",(int)rec,ptrtooffset(db,rec));
  printf("********* db_example ended ************\n");
  return c;
}

/*
    db read example
*/

int db_read(void* db) {
  void* rec=(char*)1;
  int i; 
  int c;
  int records=1;
  int tmp=0;
  
  printf("********* db_example starts ************\n");
  printf("logoffset: %d\n",wg_get_log_offset(db));
  c=0;
  for(i=0;i<records;i++) {
    rec=wg_get_first_record(db);
    printf("wg_get_first_record(db) gave adr %d offset %d\n",(int)rec,ptrtooffset(db,rec)); 
    tmp=wg_get_field(db,rec,0);
    printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
      tmp=wg_get_field(db,rec,1);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
        tmp=wg_get_field(db,rec,2);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    c++;
    while(rec!=NULL) {
      rec=wg_get_next_record(db,rec); 
      if (rec==NULL) break;
      c++;
      printf("wg_get_next_record(db) gave new adr %d offset %d\n",(int)rec,ptrtooffset(db,rec));
      tmp=wg_get_field(db,rec,0);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    tmp=wg_get_field(db,rec,1);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
        tmp=wg_get_field(db,rec,2);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    }
  }    
 // printf("c is %d\n",c);  
  
  printf("********* db_example ended ************\n");
  return c;
}
