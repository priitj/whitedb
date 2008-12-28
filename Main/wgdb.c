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
  int tmp;
  
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
  
  show_db_memsegment_header(shmptr);
  tmp=db_test(shmptr);
  printf("db_test returned %d \n",tmp);
  show_db_memsegment_header(shmptr);
  //wg_detach_database(shmptr);   
  wg_delete_database(shmname);
  
#ifdef _WIN32  
  _getch();  
#endif  
  return 0;  
}

int db_test(void* shmptr) {
  gint tmp1,tmp2,tmp3,tmp4,tmp5,tmp6,tmp7,tmp8,tmp9,tmp10,tmp11,tmp12,tmp13,tmp14;
  gint i;
  void* db;
  
  printf("db_test start\n");
  printf("=============\n");
  
  db=shmptr;
  tmp1=alloc_gints(shmptr,1024);
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
  /*
  for(i=0,tmp13=tmp12;tmp12; tmp12=dbfetch(db,cdr(tmp12))) {
    i++;
  } 
  
  printf("built list of %d elems\n",i);
  */
  /*
  tmp2=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp2);  
  tmp3=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp3); 
  tmp3=alloc_gints(shmptr,32000);
  printf("alloc_gints returned: %d \n",tmp3);
  tmp3=alloc_gints(shmptr,320000000);
  printf("alloc_gints returned: %d \n",tmp3);
  
  printf("db_test end\n");
  printf("===========\n");    
  return 0;
  */
  /*
  tmp2=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp2);
  tmp3=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp3);
  tmp4=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp4);
  tmp5=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp5);
  tmp6=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp6);
  tmp7=alloc_gints(shmptr,1024);
  printf("alloc_gints returned: %d \n",tmp7);
  tmp8=alloc_gints(shmptr,2024);
  printf("alloc_gints returned: %d \n",tmp8);    
  
  tmp9=alloc_gints(shmptr,512);  
  printf("alloc_gints returned: %d \n",tmp9);
  tmp10=alloc_gints(shmptr,128);
  printf("alloc_gints returned: %d \n",tmp10);
  tmp11=alloc_gints(shmptr,128);
  printf("alloc_gints 11 returned: %d \n",tmp11);
  tmp12=alloc_gints(shmptr,32);
  printf("alloc_gints 12 returned: %d \n",tmp12);
  
  
  show_db_memsegment_header(shmptr);
  
  //free_object(shmptr,tmp1);  
  
  
  free_object(shmptr,tmp2);
  free_object(shmptr,tmp3);
  free_object(shmptr,tmp4);
  free_object(shmptr,tmp5);
  free_object(shmptr,tmp6);
  free_object(shmptr,tmp7);
  free_object(shmptr,tmp8);
  
  free_object(shmptr,tmp9);
  free_object(shmptr,tmp10);
  free_object(shmptr,tmp11);  
  free_object(shmptr,tmp12);
  
  show_db_memsegment_header(shmptr);

  tmp11=alloc_gints(shmptr,128);
  printf("alloc_gints 11 returned: %d \n",tmp11);
  tmp12=alloc_gints(shmptr,32);  
  printf("alloc_gints 12 returned: %d \n",tmp12);  
  tmp13=alloc_gints(shmptr,1024);
  printf("alloc_gints 13 returned: %d \n",tmp13);
  
  //show_db_memsegment_header(shmptr);
  
  tmp14=alloc_gints(shmptr,256);
  printf("alloc_gints 14 returned: %d \n",tmp14);
  */
  
  printf("db_test end\n");
  printf("===========\n");    
  return 0;
}  
