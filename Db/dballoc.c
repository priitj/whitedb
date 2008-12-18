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

 /** @file dballoc.c
 *  Database initialisation and common allocation/deallocation procedures: 
 *  areas, subareas, objects, strings etc.
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#include "../config.h"
#include "dballoc.h"

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */




/* ====== Functions ============== */


/* -------- segment header initialisation ---------- */

/** starts and completes memsegment initialisation
*
* should be called after new memsegment is allocated
*/

gint init_db_memsegment(void* db, gint key, gint size) {  
  db_memsegment_header* dbh;
  gint tmp;
  gint free;
  gint i;
  
  dbh=(db_memsegment_header*) db;
  // set main global values for db
  dbh->mark=DB_MEMSEGMENT_MARK;
  dbh->size=size;
  dbh->initialadr=(gint)db;
  dbh->key=key;
   
  // set correct alignment for free
  free=sizeof(db_memsegment_header);
  for(i=0;i<ALIGNMENT_BYTES;i++) {
    if ((free+i)%ALIGNMENT_BYTES==0) break;
  }  
  dbh->free=free+i;
 
  // allocate and initialise subareas
  
  //datarec
  tmp=init_db_subarea(dbh,&(dbh->datarec_area_header),INITIAL_SUBAREA_SIZE);
  if (!tmp) {  show_dballoc_error(dbh," cannot create datarec area"); return -1; }
  (dbh->datarec_area_header).fixedlength=0;
  //string
  tmp=init_db_subarea(dbh,&(dbh->string_area_header),INITIAL_SUBAREA_SIZE);
  if (!tmp) {  show_dballoc_error(dbh," cannot create string area"); return -1; }
  (dbh->string_area_header).fixedlength=0;
  //list
  tmp=init_db_subarea(dbh,&(dbh->list_area_header),INITIAL_SUBAREA_SIZE);
  if (!tmp) {  show_dballoc_error(dbh," cannot create list area"); return -1; }
  (dbh->list_area_header).fixedlength=1;
  (dbh->list_area_header).objlength=sizeof(gcell);
  make_subarea_freelist(db,&(dbh->list_area_header),0); // freelist into subarray 0
  
  return 0; 
}  

/** initializes a subarea. subarea is used for actual data obs allocation
*
* called - several times - first by init_db_memsegment, then as old subareas 
* get filled up
*/

gint init_db_subarea(void* db, void* area_header, gint size) {
  db_area_header* areah;
  gint segmentchunk;
  
  segmentchunk=alloc_db_segmentchunk(db,size);
  if (!segmentchunk) return -1; // errcase
  
  areah=(db_area_header*)area_header;  
  ((areah->subarea_array)[0]).size=size;     
  ((areah->subarea_array)[0]).offset=segmentchunk;  
  ((areah->subarea_array)[0]).free=segmentchunk; 
  areah->last_subarea_index=0;  
  areah->freelist=0;
  return 0;
}  

/** allocates a new segment chunk from the segment
*
* used for allocating all subareas 
* 
*/

gint alloc_db_segmentchunk(void* db, gint size) {
  db_memsegment_header* dbh;
  gint lastfree;
  gint nextfree; 
  gint i;
  
  dbh=(db_memsegment_header*)db;
  lastfree=dbh->free;
  nextfree=lastfree+size;
  //printf("lastfree %d nextfree %d \n",lastfree,nextfree);
  // set correct alignment for nextfree
  for(i=0;i<ALIGNMENT_BYTES;i++) {
    if ((nextfree+i)%ALIGNMENT_BYTES==0) break;
  }  
  nextfree=nextfree+i;
  
  if (nextfree>=(dbh->size)) {
    show_dballoc_error_nr(dbh,"segment does not have enough space for the required chunk of size",size);
    return 0;
  }
  dbh->free=nextfree;
  return lastfree;  
}  


/* -------- freelists creation  ---------- */

/** create freelist for an area
*
* used for initialising (sub)areas used for fixed-size allocation
*
* speed stats:
*
* 10000 * creation of   100000 elems (1 000 000 000 or 1G ops) takes 1.2 sec on penryn 
* 1000 * creation of  1000000 elems (1 000 000 000 or 1G ops) takes 3.4 sec on penryn 
*
*/

void make_subarea_freelist(void* db, void* area_header, gint arrayindex) {
  db_area_header* areah;  
  gint freelist;
  gint objlength;
  gint max;
  gint size;
  gint offset;  
  gint i;
  
  // general area info
  areah=(db_area_header*)area_header;
  freelist=areah->freelist;
  objlength=areah->objlength; 
  
  //subarea info  
  size=((areah->subarea_array)[arrayindex]).size;
  offset=((areah->subarea_array)[arrayindex]).offset;    
  // create freelist
  max=(offset+size)-(2*objlength);
  for(i=offset;i<=max;i=i+objlength) {
    dbstore(db,i,i+objlength);     
  } 
  dbstore(db,i,0);  
  (areah->freelist)=offset; //
  printf("(areah->freelist) %d \n",(areah->freelist));
}  



/* -------- fixed length object allocation and freeing ---------- */

/** allocate a new list cell
*
*
*/

gint alloc_cell(void* db) {
  db_memsegment_header* dbh;
  gint freelist;
  
  dbh=(db_memsegment_header*)db;
  freelist=(dbh->list_area_header).freelist;
  if (!freelist) {
    if(extend_fixedlen_area(db,&(dbh->list_area_header))) {
      show_dballoc_error(db,"cannot extend list area");
      return 0;
    }  
    freelist=(dbh->list_area_header).freelist;
    if (!freelist) {
      show_dballoc_error(db,"no free list cells available");
      return 0;       
    } else {
      return freelist;
    }        
  } else {
    (dbh->list_area_header).freelist=dbfetch(db,freelist);
    return freelist;
  }         
}  

/** create and initialise a new subarea for fixed-len obs area
* 
* used when the area has no more free space
*
*/

gint extend_fixedlen_area(void* db, void* area_header) {
  printf("trying to extend fixedlen area, not implemented\n"); 
  return -1;
}  

/** free an existing list cell
*
* the cell is added to the freelist
*
*/

void free_cell(void* db, gint cell) {
  dbstore(db,cell,(((db_memsegment_header*)db)->list_area_header).freelist); 
  dbstore(db,(((db_memsegment_header*)db)->list_area_header).freelist,cell);   
}  


/*
gint alloc_fixedlen(void* db, void* area_header) {
  area_header* ah;
  gint freelist;
  
  ah=(area_header*)db;
  freelist=(dbh->list_area_header).freelist;
  if (!freelist) {
    if(extend_fixedlen_area(db,&(dbh->list_area_header)) {
      show_dballoc_error(db,"cannot extend list area");
      return 0;
    }  
    freelist=(dbh->list_area_header).freelist;
    if (!freelist) {
      show_dballoc_error(db,"no free list cells available");
      return 0;       
    } else {
      return freelist;
    }        
  } else {
    (dbh->list_area_header).freelist=cdr(freelist);
    return freelist;
  }         
}  
*/




/* ---------------- overviews, statistics ---------------------- */

/** print an overview of full memsegment memory  usage and addresses
*
*
*/


void show_db_memsegment_header(void* db) {
  db_memsegment_header* dbh;
  
  dbh=(db_memsegment_header*) db;
  
  printf("Showing db segment information\n");
  printf("==============================\n");
  printf("mark %d\n",dbh->mark);
  printf("size %d\n",dbh->size);
  printf("free %d\n",dbh->free);
  printf("initialadr %x\n",dbh->initialadr);
  printf("key  %d\n",dbh->key);  
  printf("segment header size %d\n",sizeof(db_memsegment_header));
  printf("subarea  array size %d\n",SUBAREA_ARRAY_SIZE);
  
  printf("datarec_area\n");
  printf("------------\n");  
  show_db_area_header(dbh,&(dbh->datarec_area_header));
  printf("string_area\n");
  printf("------------\n");  
  show_db_area_header(dbh,&(dbh->string_area_header));
  printf("list_area\n");
  printf("------------\n");  
  show_db_area_header(dbh,&(dbh->list_area_header));
  printf("freelist len: %d\n",count_freelist(db,(dbh->list_area_header).freelist));
  
}

/** print an overview of a single area memory usage and addresses
*
*
*/

void show_db_area_header(void* db, void* area_header) {
  db_area_header* areah;  
  gint i;
  
  areah=(db_area_header*)area_header;  
  printf("freelist %d\n",areah->freelist);
  printf("last_subarea_index %d\n",areah->last_subarea_index);  
  for (i=0;i<=(areah->last_subarea_index);i++) {
    printf("subarea nr %d \n",i);
    printf("  size   %d\n",((areah->subarea_array)[i]).size);
    printf("  offset %d\n",((areah->subarea_array)[i]).offset);
    printf("  free   %d\n",((areah->subarea_array)[i]).free);
  }  
}

/** count elements in a freelist
*
*/

gint count_freelist(void* db, gint freelist) {
  gint i;
  printf("freelist %d dbfetch(db,freelist) %d\n",freelist,dbfetch(db,freelist));
  
  for(i=0;freelist; freelist=dbfetch(db,freelist)) {
    i++;
  }  
  return i;
}  



/* --------------- error handling ------------------------------*/

/** called with err msg when an allocation error occurs
*
*  may print or log an error
*  does not do any jumps etc
*/

gint show_dballoc_error(void* db, char* errmsg) {
  printf("db memory allocation error: %s\n",errmsg);
  return -1;
} 

/** called with err msg and err nr when an allocation error occurs
*
*  may print or log an error
*  does not do any jumps etc
*/

gint show_dballoc_error_nr(void* db, char* errmsg, gint nr) {
  printf("db memory allocation error: %s %d\n",errmsg,nr);
  return -1;
}  



/* ---------------- dbase contents testing ------------------- */


