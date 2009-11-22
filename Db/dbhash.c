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

/* Remove longstr from strhash
*
*  Internal langstr etc are not removed by this op.
*
*/

gint wg_remove_from_strhash(void* db, gint longstr) {
  db_memsegment_header* dbh;
  gint type;
  char* extrastr;
  char* data;
  gint length;
  gint hash;
  gint chainoffset;  
  gint hashchain;
  gint nextchain;
  gint offset;
  gint* objptr;
  gint fldval;
  gint objsize;
  gint strsize;
  gint* typeptr;
  
  printf("wg_remove_from_strhash called on %d\n",longstr);  
  dbh=(db_memsegment_header*)db;
  offset=decode_longstr_offset(longstr);
  objptr=offsettoptr(db,offset);
  // get string data elements  
  //type=objptr=offsettoptr(db,decode_longstr_offset(data));       
  extrastr=((char*)(objptr))+(LONGSTR_EXTRASTR_POS*sizeof(gint));
  fldval=*extrastr;
  if (fldval==0) extrastr=NULL;
  else extrastr=wg_decode_str(db,fldval);
  data=((char*)(objptr))+(LONGSTR_HEADER_GINTS*sizeof(gint));
  objsize=getusedobjectsize(*objptr);         
  strsize=objsize-(((*(objptr+LONGSTR_META_POS))&LONGSTR_META_LENDIFMASK)>>LONGSTR_META_LENDIFSHFT); 
  length=strsize;  
  typeptr=(gint*)(((char*)(objptr))+(+LONGSTR_META_POS*sizeof(gint)));
  type=(*typeptr)&LONGSTR_META_TYPEMASK;
  printf("  type %d data %s extrastr %s length %d\n",type,data,extrastr,length);
  // get hash of data elements and find the location in hashtable/chains   
  hash=wg_hash_typedstr(dbh,data,extrastr,type,length);  
  chainoffset=((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash);
  hashchain=dbfetch(db,chainoffset);    
  printf("  hash %d chainoffset %d hashchain %d\n",hash,chainoffset,hashchain);
  while(hashchain!=0) {
    if (hashchain==longstr) {
      nextchain=dbfetch(db,decode_longstr_offset(hashchain)+(LONGSTR_HASHCHAIN_POS*sizeof(gint)));  
      dbstore(db,chainoffset,nextchain);     
      return 0;  
    }        
    chainoffset=decode_longstr_offset(hashchain)+(LONGSTR_HASHCHAIN_POS*sizeof(gint));
    hashchain=dbfetch(db,chainoffset);
  }    
  show_consistency_error_nr(db,"string not found in hash during deletion, offset",offset);
  return -1;  
}




gint show_consistency_error(void* db, char* errmsg) {
  printf("wg consistency error: %s\n",errmsg);
  return -1;
}

gint show_consistency_error_nr(void* db, char* errmsg, gint nr) {
  printf("wg consistency error: %s %d\n",errmsg,nr);
  return -1;
}

gint show_consistency_error_double(void* db, char* errmsg, double nr) {
  printf("wg consistency error: %s %f\n",errmsg,nr);
  return -1;
}

gint show_consistency_error_str(void* db, char* errmsg, char* str) {
  printf("wg consistency error: %s %s\n",errmsg,str);
  return -1;
}


/*

#include "pstdint.h" // Replace with <stdint.h> if appropriate 
#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

uint32_t SuperFastHash (const char * data, int len) {
uint32_t hash = len, tmp;
int rem;

    if (len <= 0 || data == NULL) return 0;

    rem = len & 3;
    len >>= 2;

    // Main loop 
    for (;len > 0; len--) {
        hash  += get16bits (data);
        tmp    = (get16bits (data+2) << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        data  += 2*sizeof (uint16_t);
        hash  += hash >> 11;
    }

    // Handle end cases 
    switch (rem) {
        case 3: hash += get16bits (data);
                hash ^= hash << 16;
                hash ^= data[sizeof (uint16_t)] << 18;
                hash += hash >> 11;
                break;
        case 2: hash += get16bits (data);
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1: hash += *data;
                hash ^= hash << 10;
                hash += hash >> 1;
    }

    // Force "avalanching" of final 127 bits 
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}

*/