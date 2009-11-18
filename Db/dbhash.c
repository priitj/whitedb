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

 /** @file dbhash.c
 *  Hash operations for strings and other datatypes. 
 *  
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dbhash.h"
#include "dbdata.h"


/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */




/* ====== Functions ============== */


/* ------------- strhash operations ------------------- */




/* Hash function for two-part strings and blobs.
*
* Based on sdbm.
*
*/

int wg_hash_typedstr(void* db, char* data, char* extrastr, gint type, gint length) {
  char* endp;
  unsigned long hash = 0;
  int c;  
  
  if (data!=NULL) {
    for(endp=data+length; data<endp; data++) {
      c = (int)(*data);
      hash = c + (hash << 6) + (hash << 16) - hash;
    }
  }  
  if (extrastr!=NULL) {
    while ((c = *extrastr++))
      hash = c + (hash << 6) + (hash << 16) - hash;    
  }  
  
  return (int)(hash % (((db_memsegment_header*)db)->strhash_area_header).arraylength);
}



/* Find longstr from strhash bucket chain
*
*
*/

gint wg_find_strhash_bucket(void* db, char* data, char* extrastr, gint type, gint size, gint hashchain) {  
  
  for(;hashchain!=0;
      hashchain=dbfetch(db,decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint))) {    
    if (wg_right_strhash_bucket(db,hashchain,data,extrastr,type,size)) {
      // found equal longstr, return it
      return hashchain;
    }          
  }
  return 0;  
}

/* Check whether longstr hash bucket matches given new str
*
*
*/

static int wg_right_strhash_bucket
            (void* db, gint longstr, char* cstr, char* cextrastr, gint ctype, gint cstrsize) {
  char* str;
  char* extrastr;
  int strsize;
  gint type;
  void* objptr;
  gint objsize;
  
  type=wg_get_encoded_type(db,longstr);
  if (type!=ctype) return 0;
  strsize=wg_decode_str_len(db,longstr)+1;    
  if (strsize!=cstrsize) return 0;
  str=wg_decode_str(db,longstr); 
  if ((cstr==NULL && str!=NULL) || (cstr!=NULL && str==NULL)) return 0;
  if ((cstr!=NULL) && (memcmp(str,cstr,cstrsize))) return 0;
  extrastr=wg_decode_str_lang(db,longstr);
  if ((cextrastr==NULL && extrastr!=NULL) || (cextrastr!=NULL && extrastr==NULL)) return 0;
  if ((cextrastr!=NULL) && (memcmp(extrastr,cextrastr,cstrsize))) return 0;
  return 1;
}  

