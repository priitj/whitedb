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

#include "../config.h"
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
//#include "../Db/dbapi.h"
#include "../Db/dbtest.h"
#include "../Db/dbdump.h"
#include "wgdb.h"



/* ====== Private defs =========== */


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
  }
  else if(argc>2 && !strcmp(argv[2],"export")){
      db_write(shmptr);
     wg_dump(shmptr,argv[3]); 
  }
  else
  {
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
  int records=2;
  int tmp=0;
  
  printf("********* db_example starts ************\n");
  flds=3;
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
  int records=2;
  int tmp=0;
  
  printf("********* db_example starts ************\n");
  c=0;
  for(i=0;i<records;i++) {
    rec=wg_get_first_record(db);
    printf("wg_get_first_record(db) gave adr %d offset %d\n",(int)rec,ptrtooffset(db,rec)); 
    tmp=wg_get_field(db,rec,0);
    printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    c++;
    while(rec!=NULL) {
      rec=wg_get_next_record(db,rec); 
      if (rec==NULL) break;
      c++;
      printf("wg_get_next_record(db) gave new adr %d offset %d\n",(int)rec,ptrtooffset(db,rec));
      tmp=wg_get_field(db,rec,0);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    }
  }    
 // printf("c is %d\n",c);  
  
  printf("********* db_example ended ************\n");
  return c;
}



/*

db_test5

on tanel xps laptop using linux and shared mem, 
echo 3000000000  > /proc/sys/kernel/shmmax

10 000 000 recs of 5 fields (plus 1 for size) of full integers (4 bytes),
no filling with numbers:
real    0m0.499s
user    0m0.240s
sys     0m0.256s


creation+scanning through them 100 times (1 bill recs scanned altogether):
real    0m9.008s
user    0m8.753s
sys     0m0.256s

hence we can scan ca 120 000 000 recs per sec

*/

int db_test5(void* db) {
  void* rec=(char*)1;
  int i; 
  //int j;
  int c;
  int flds;
  //int tmp=0;
  
  printf("********* db_test4 starts ************\n");
  flds=5;
  c=1;
  for (i=0;i<10000000;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }
    /*      
    c=1;    
    for(j=0;j<flds;j++) {
      tmp=wg_set_int_field(db,rec,j,c);
      c++;
      if (tmp!=0) { 
        printf("int storage error");
        exit(0);    
      }
    } 
    */      
  } 
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("first record adr %x offset %d\n",(int)rec,ptrtooffset(db,rec));
  
  c=0;
  for(i=0;i<100;i++) {
    rec=wg_get_first_record(db);
    printf("wg_get_first_record(db) gave adr %d offset %d\n",(int)rec,ptrtooffset(db,rec)); 
    c++;
    while(rec!=NULL) {
      rec=wg_get_next_record(db,rec); 
      c++;
      //printf("wg_get_next_record(db) gave new adr %d offset %d\n",(int)rec,ptrtooffset(db,rec));
    }
  }    
  printf("c is %d\n",c);  
  
  printf("********* db_test4 ended ************\n");
  return c;
}


/*

db_test4

on tanel xps laptop using linux and shared mem, 
10 000 000 recs of 5 fields (plus 1 for size) of full integers (4 bytes),
no filling:

real    0m0.502s
user    0m0.300s
sys     0m0.204s

*/

int db_test4(void* db) {
  void* rec=(char*)1;
  int i; 
  int c;
  int flds;
  
  printf("********* db_test3 starts ************\n");
  flds=5;
  c=1<<30;
  for (i=0;i<10000000;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }      
  }
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("********* db_test3 ended ************\n");
  return c;
}

/*

db_test3a

on tanel xps laptop using linux and shared mem, 
10 000 000 recs of 5 fields (plus 1 for size) of small integers:

real    0m0.843s
user    0m0.616s
sys     0m0.228s


*/

int db_test3a(void* db) {
  void* rec=(char*)1;
  int i; 
  int j;
  int c;
  int flds;
  int tmp=0;
  
  printf("********* db_test3 starts ************\n");
  flds=5;
  c=1<<30;
  for (i=0;i<10000000;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }      
    c=1;    
    for(j=0;j<flds;j++) {
      tmp=wg_set_int_field(db,rec,j,c);
      c++;
      if (tmp!=0) { 
        printf("int storage error");
        exit(0);    
      }
    }         
  }
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("********* db_test3 ended ************\n");
  return c;
}

/*

db_test3

on tanel xps laptop using linux and shared mem, 
10 000 000 recs of 5 fields (plus 1 for size) of full integers (4 bytes):
real    0m1.552s
user    0m1.076s
sys     0m0.476s


*/

int db_test3(void* db) {
  void* rec=(char*)1;
  int i; 
  int j;
  int c;
  int flds;
  int tmp=0;
  
  printf("********* db_test3 starts ************\n");
  flds=5;
  c=1<<30;
  for (i=0;i<10000000;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }      
    c=1<<30;    
    for(j=0;j<flds;j++) {
      tmp=wg_set_int_field(db,rec,j,c);
      c++;
      if (tmp!=0) { 
        printf("int storage error");
        exit(0);    
      }
    }         
  }
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("********* db_test3 ended ************\n");
  return c;
}

/*

db_test2 

on tanel xps laptop using linux and shared mem, 
10 000 000 recs of 10 fields (plus 1 for size):
real    0m2.589s
user    0m2.148s
sys     0m0.432s

on tanel xps laptop using linux and shared mem, 
10 000 000 recs of 5 fields (plus 1 for size):
real    0m1.573s
user    0m1.304s
sys     0m0.252s

on tanel xps laptop using linux and shared mem, 
1 000 000 recs of 5 fields (plus 1 for size):
real    0m0.170s
user    0m0.140s
sys     0m0.020s


*/

int db_test2(void* db) {
  void* rec=(char*)1;
  int i; 
  int j;
  int c;
  int flds;
  int tmp=0;
  
  printf("********* db_test2 starts ************\n");
  flds=10;
  c=0;
  for (i=0;i<10000000;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }         
    for(j=0;j<flds;j++) {
      tmp=wg_set_int_field(db,rec,j,c);
      c++;
      if (tmp!=0) { 
        printf("int storage error");
        exit(0);    
      }
    }         
  }
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("********* db_test2 ended ************\n");
  return c;
}


int db_test1(void* shmptr) {
  // gint tmp1,
  gint tmp2,tmp3,tmp4,tmp5,tmp6,tmp7,tmp8,tmp9,tmp10,tmp11,tmp12; //,tmp13,tmp14;
  //gint i;
  void* db;
  void* darea;
  
  printf("db_test start\n");
  printf("=============\n");
  
  db=shmptr;
  darea=&(((db_memsegment_header*)db)->datarec_area_header);
  
  /*
  tmp1=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp1);
  
  tmp13=0;
  for(i=0;i<10000000;i++) {
    tmp14=alloc_listcell(db);
    if (!tmp14) {
      printf("could not get listcell nr %d ",i);
      break;
    }      
    //dbstore(db,car(tmp14),i);
    //if (tmp13==0) { tmp13=tmp14; tmp12=tmp13; }
    //else dbstore(db,cdr(tmp13),tmp14);
    //tmp13=tmp14;
  } 
  */  
  /*
  for(i=0,tmp13=tmp12;tmp12; tmp12=dbfetch(db,cdr(tmp12))) {
    i++;
  } 
  
  printf("built list of %d elems\n",i);
  */

 
  
  tmp2=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp2);

  tmp3=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp3);
  tmp4=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp4);
  tmp5=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp5);
  tmp6=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp6);
  tmp7=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints returned: %d \n",tmp7);
  tmp8=alloc_gints(shmptr,darea,2024);
  printf("alloc_gints returned: %d \n",tmp8);    
  
  free_object(shmptr,darea,tmp2);
  free_object(shmptr,darea,tmp3);
  free_object(shmptr,darea,tmp4);
  free_object(shmptr,darea,tmp5);
  free_object(shmptr,darea,tmp6);
  free_object(shmptr,darea,tmp7);
  free_object(shmptr,darea,tmp8);
  
  
  tmp9=alloc_gints(shmptr,darea,512);  
  printf("alloc_gints returned: %d \n",tmp9);
  tmp10=alloc_gints(shmptr,darea,128);
  printf("alloc_gints returned: %d \n",tmp10);
  tmp11=alloc_gints(shmptr,darea,128);
  printf("alloc_gints 11 returned: %d \n",tmp11);
  tmp12=alloc_gints(shmptr,darea,32);
  printf("alloc_gints 12 returned: %d \n",tmp12);
  
  
  //show_db_memsegment_header(shmptr);
  
  free_object(shmptr,darea,tmp10);  
  
  
  free_object(shmptr,darea,tmp11);
  
  /*
  free_object(shmptr,darea,tmp3);
  
  
  free_object(shmptr,darea,tmp5);
  
  
  free_object(shmptr,darea,tmp6);
  free_object(shmptr,darea,tmp7);
  
  
  free_object(shmptr,darea,tmp8);
  
  free_object(shmptr,darea,tmp9);
  free_object(shmptr,darea,tmp10);
  free_object(shmptr,darea,tmp11);  
  free_object(shmptr,darea,tmp12);
  
  //show_db_memsegment_header(shmptr);

  tmp11=alloc_gints(shmptr,darea,128);
  printf("alloc_gints 11 returned: %d \n",tmp11);
  tmp12=alloc_gints(shmptr,darea,32);  
  printf("alloc_gints 12 returned: %d \n",tmp12);  
  tmp13=alloc_gints(shmptr,darea,1024);
  printf("alloc_gints 13 returned: %d \n",tmp13);
  
  //show_db_memsegment_header(shmptr);
  
  tmp14=alloc_gints(shmptr,darea,256);
  printf("alloc_gints 14 returned: %d \n",tmp14);  
  
  */
  
  check_db(shmptr);
  printf("db_test end\n");
  printf("===========\n");    
  return 0;
}  
