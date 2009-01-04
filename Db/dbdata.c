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

 /** @file dbdata.c
 *  Procedures for handling actual data: records, strings, integers etc
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#include "../config.h"
#include "dballoc.h"
#include "dbdata.h"

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */


/* ====== Functions ============== */



void* wg_create_record(void* db, int length) {
  gint offset;
  gint i;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error_nr(db,"wrong database pointer given to wg_create_record with length ",length); 
    return NULL;
  }  
#endif  
  offset=alloc_gints(db,
                     &(((db_memsegment_header*)db)->datarec_area_header),
                    length+RECORD_HEADER_GINTS);
  if (!offset) {
    show_data_error_nr(db,"cannot create a record of size ",length); 
    return NULL;
  }      
  for(i=RECORD_HEADER_GINTS;i<length+RECORD_HEADER_GINTS;i++) {
    dbstore(db,offset+RECORD_HEADER_GINTS,0);
  }     
  return offsettoptr(db,offset);
}  

int wg_set_int_field(void* db, void* record, int fieldnr, int data) {
  gint offset;
  
#ifdef CHECK
  recordcheck(db,record,fieldnr,"wg_set_int_field");
#endif  
  if (fits_smallint(data)) {
    *(((gint*)record)+RECORD_HEADER_GINTS+fieldnr)=encode_smallint(data);
    //dbstore(db,ptrtoffset(record)+RECORD_HEADER_GINTS+fieldnr,encode_smallint(data));
  } else {
    offset=alloc_word(db);
    if (!offset) {
      show_data_error_nr(db,"cannot store an integer in wg_set_int_field: ",data); 
      return -3;
    }    
    dbstore(db,offset,data);
    *(((gint*)record)+RECORD_HEADER_GINTS+fieldnr)=encode_fullint_offset(offset);
    //dbstore(db,ptrtoffset(record)+RECORD_HEADER_GINTS+fieldnr,encode_fullint_offset(offset));
  }
  return 0;     
}  



/* ------------ errors ---------------- */


gint show_data_error(void* db, char* errmsg) {
  printf("wg data handling error: %s\n",errmsg);
  return -1;
}

gint show_data_error_nr(void* db, char* errmsg, gint nr) {
  printf("wg data handling error: %s %d\n",errmsg,nr);
  return -1;
}

gint show_data_error_str(void* db, char* errmsg, char* str) {
  printf("wg data handling error: %s %s\n",errmsg,str);
  return -1;
}

