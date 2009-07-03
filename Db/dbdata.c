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
 *  Procedures for handling actual data: strings, integers, records,  etc
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
#include "dballoc.h"
#include "dbdata.h"
#include "dblog.h"

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */


/* ====== Functions ============== */



/* ------------ full record handling ---------------- */


void* wg_create_record(void* db, wg_int length) {
  gint offset;
  gint i;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error_nr(db,"wrong database pointer given to wg_create_record with length ",length); 
    return 0;
  }  
#endif  
  offset=alloc_gints(db,
                     &(((db_memsegment_header*)db)->datarec_area_header),
                    length+RECORD_HEADER_GINTS);
  if (!offset) {
    show_data_error_nr(db,"cannot create a record of size ",length); 
    return 0;
  }      
  //logging
  wg_log_record(db,offset,length);
  
  for(i=RECORD_HEADER_GINTS;i<length+RECORD_HEADER_GINTS;i++) {
    dbstore(db,offset+RECORD_HEADER_GINTS,0);
  }     
  return offsettoptr(db,offset);
}  


void* wg_get_first_record(void* db) {
  db_subarea_header* arrayadr;
  gint firstoffset;
  void* res;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_get_first_record"); 
    return NULL;
  }  
#endif 
  arrayadr=&((((db_memsegment_header*)db)->datarec_area_header).subarea_array[0]);
  firstoffset=((arrayadr[0]).alignedoffset); // do NOT skip initial "used" marker
  //printf("arrayadr %x firstoffset %d \n",(uint)arrayadr,firstoffset);
  res=wg_get_next_record(db,offsettoptr(db,firstoffset));
  return res;  
}

void* wg_get_next_record(void* db, void* record) {
  gint curoffset;
  gint head;
  db_subarea_header* arrayadr;
  gint last_subarea_index;  
  gint i;
  gint found; 
  gint subareastart;
  gint subareaend;
  gint freemarker;

  curoffset=ptrtooffset(db,record);
  //printf("curroffset %d record %x\n",curoffset,(uint)record);
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_get_first_record"); 
    return NULL;
  }  
  head=dbfetch(db,curoffset);
  if (isfreeobject(head)) {
    show_data_error(db,"wrong record pointer (free) given to wg_get_next_record"); 
    return NULL;
  }  
#endif   
  freemarker=0; //assume input pointer to used object
  head=dbfetch(db,curoffset);
  while(1) {
    // increase offset to next memory block           
    curoffset=curoffset+(freemarker ? getfreeobjectsize(head) : getusedobjectsize(head));   
    head=dbfetch(db,curoffset);
    //printf("new curoffset %d head %d isnormaluseobject %d isfreeobject %d \n",
    //       curoffset,head,isnormalusedobject(head),isfreeobject(head));
    // check if found a normal used object
    if (isnormalusedobject(head)) return offsettoptr(db,curoffset); //return ptr to normal used object
    if (isfreeobject(head)) {
      freemarker=1;      
      // loop start leads us to next object
    } else {      
      // found a special object (dv or end marker)
      freemarker=0;
      if (dbfetch(db,curoffset+sizeof(gint))==SPECIALGINT1DV) {
        // we have reached a dv object
        continue; // loop start leads us to next object
      } else {        
        // we have reached an end marker, have to find the next subarea
        // first locate subarea for this offset 
        arrayadr=&((((db_memsegment_header*)db)->datarec_area_header).subarea_array[0]);
        last_subarea_index=(((db_memsegment_header*)db)->datarec_area_header).last_subarea_index;
        found=0;
        for(i=0;(i<=last_subarea_index)&&(i<SUBAREA_ARRAY_SIZE);i++) {
          subareastart=((arrayadr[i]).alignedoffset);
          subareaend=((arrayadr[i]).offset)+((arrayadr[i]).size);
          if (curoffset>=subareastart && curoffset<subareaend) {
            found=1;
            break; 
          }             
        }          
        if (!found) {
          show_data_error(db,"wrong record pointer (out of area) given to wg_get_next_record"); 
          return NULL;
        } 
        // take next subarea, while possible
        i++;
        if (i>last_subarea_index || i>=SUBAREA_ARRAY_SIZE) {
          //printf("next used object not found: i %d curoffset %d \n",i,curoffset); 
          return NULL; 
        }        
        //printf("taking next subarea i %d\n",i);
        curoffset=((arrayadr[i]).alignedoffset);  // curoffset is now the special start marker
        head=dbfetch(db,curoffset);
        // loop start will lead us to next object from special marker
      }        
    }
  }
}


/* ------------ field handling: data storage and fetching ---------------- */


wg_int wg_get_record_len(void* db, void* record) {
 
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_get_record_len");
    return -1;
  }  
#endif   
  return ((gint)(getusedobjectwantedgintsnr(*((gint*)record))))-RECORD_HEADER_GINTS;  
}

wg_int* wg_get_record_dataarray(void* db, void* record) {
 
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_get_record_dataarray");
    return NULL;
  } 
#endif   
  return (((gint*)record)+RECORD_HEADER_GINTS);  
}

wg_int wg_set_field(void* db, void* record, wg_int fieldnr, wg_int data) {
  gint* fieldadr;
  gint fielddata;
   
#ifdef CHECK
  recordcheck(db,record,fieldnr,"wg_set_field");
#endif 
  fieldadr=((gint*)record)+RECORD_HEADER_GINTS+fieldnr;
  fielddata=*fieldadr;
  //printf("wg_set_field adr %d offset %d\n",fieldadr,ptrtooffset(db,fieldadr));
  if (isptr(fielddata)) {
    //printf("wg_set_field freeing old data\n"); 
    free_field_encoffset(db,fielddata,ptrtooffset(db,record),fieldnr);
  }  
  (*fieldadr)=data;
  return 0;
}
  
wg_int wg_set_int_field(void* db, void* record, wg_int fieldnr, gint data) {
  gint fielddata;
  fielddata=wg_encode_int(db,data);
  //printf("wg_set_int_field data %d encoded %d\n",data,fielddata);
  if (!fielddata) return -1;
    wg_log_int(db,record,fieldnr,data);
  return wg_set_field(db,record,fieldnr,fielddata);
}  
  
wg_int wg_set_double_field(void* db, void* record, wg_int fieldnr, double data) {  
  gint fielddata;
  
  fielddata=wg_encode_double(db,data);
  if (!fielddata) return -1;
  return wg_set_field(db,record,fieldnr,fielddata);
} 

wg_int wg_set_str_field(void* db, void* record, wg_int fieldnr, char* data) {
  gint fielddata;

  fielddata=wg_encode_str(db,data,NULL);
  if (!fielddata) return -1;
  return wg_set_field(db,record,fieldnr,fielddata);
} 
  
wg_int wg_set_rec_field(void* db, void* record, wg_int fieldnr, void* data) {
  gint fielddata;

  fielddata=wg_encode_record(db,data);
  if (!fielddata) return -1;
  return wg_set_field(db,record,fieldnr,fielddata);
} 

wg_int wg_get_field(void* db, void* record, wg_int fieldnr) {
 
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error_nr(db,"wrong database pointer given to wg_get_field",fieldnr);
    return 0;
  }
  if (fieldnr<0 || (getusedobjectwantedgintsnr(*((gint*)record))<=fieldnr+RECORD_HEADER_GINTS)) {
    show_data_error_nr(db,"wrong field number given to wg_get_field",fieldnr);\
    return 0;
  } 
#endif   
  //printf("wg_get_field adr %d offset %d\n",
  //       (((gint*)record)+RECORD_HEADER_GINTS+fieldnr),
  //       ptrtooffset(db,(((gint*)record)+RECORD_HEADER_GINTS+fieldnr)));
  return *(((gint*)record)+RECORD_HEADER_GINTS+fieldnr);
}

wg_int wg_get_field_type(void* db, void* record, wg_int fieldnr) {
 
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error_nr(db,"wrong database pointer given to wg_get_field_type",fieldnr);\
    return 0;
  }
  if (fieldnr<0 || (getusedobjectwantedgintsnr(*((gint*)record))<=fieldnr+RECORD_HEADER_GINTS)) {  
    show_data_error_nr(db,"wrong field number given to wg_get_field_type",fieldnr);\
    return 0;
  } 
#endif   
  return wg_get_encoded_type(db,*(((gint*)record)+RECORD_HEADER_GINTS+fieldnr));
}

/* ------------- general operations -------------- */



wg_int wg_free_encoded(void* db, wg_int data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_free_encoded");
    return 0;
  }
#endif  
  if (isptr(data)) return free_field_encoffset(db,data,0,0);   
  return 0;
}  

/** properly removes ptr (offset) to data
* 
* assumes fielddata is offset to allocated data
* depending on type of fielddata either deallocates pointed data or
* removes data back ptr or decreases refcount
*
* in case fielddata points to record or longstring, these
* are freed only if they have no more pointers
*
* returns non-zero in case of error
*/

gint free_field_encoffset(void* db,gint encoffset, gint fromrecoffset, gint fromrecfield) {
  gint offset;
  gint* dptr;
  gint* dendptr;
  gint data;     
  gint tmp;
  gint i;
  
  // takes last three bits to decide the type
  // fullint is represented by two options: 001 and 101
  switch(encoffset&NORMALPTRMASK) {    
    case DATARECBITS:         
      if (fromrecoffset<=0) break;
      // remove fromrecoffset from list
      offset=decode_datarec_offset(encoffset);      
      tmp=dbfetch(db,offset+LONGSTR_REFCOUNT_POS);
      if (0) {
        // free frompointers structure
        // loop over fields, freeing them
        dptr=offsettoptr(db,offset);       
        dendptr=(gint*)(((char*)dptr)+datarec_size_bytes(*dptr));
        for(i=0,dptr=dptr+RECORD_HEADER_GINTS;dptr<dendptr;dptr++,i++) {
          data=*dptr;
          if (isptr(data)) free_field_encoffset(db,data,offset,i);
        }         
        // really free object from area
        free_object(db,&(((db_memsegment_header*)db)->datarec_area_header),offset);          
      }  
      break;
    case LONGSTRBITS:
      offset=decode_longstr_offset(encoffset);      
      tmp=dbfetch(db,offset+LONGSTR_REFCOUNT_POS);    
      tmp--;           
      if (tmp>0) {
        dbstore(db,offset+LONGSTR_REFCOUNT_POS,tmp);
      } else {
        // free frompointers structure
        // really free object from area  
        free_object(db,&(((db_memsegment_header*)db)->longstr_area_header),offset);
      }  
      break;
    case SHORTSTRBITS:
      free_shortstr(db,decode_shortstr_offset(encoffset));
      break;      
    case FULLDOUBLEBITS:
      free_doubleword(db,decode_fulldouble_offset(encoffset));
      break;
    case FULLINTBITSV0:
      free_word(db,decode_fullint_offset(encoffset));
      break;
    case FULLINTBITSV1:
      free_word(db,decode_fullint_offset(encoffset));
      break;
    
  }  
  return 0;
}  



/* ------------- data encoding and decoding ------------ */


/** determines the type of encoded data
*
* returns a zero-or-bigger macro integer value from wg_db_api.h beginning:
*
* #define WG_NULLTYPE 1
* #define WG_RECORDTYPE 2
* #define WG_INTTYPE 3
* #define WG_DOUBLETYPE 4
* #define WG_STRTYPE 5
* ... etc ...
* 
* returns a negative number -1 in case of error
*
*/


wg_int wg_get_encoded_type(void* db, wg_int data) {
  gint fieldoffset;
  gint tmp;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_get_encoded_type");
    return 0;
  }
#endif  
  if (!data) return WG_NULLTYPE;  
  if (((data)&NONPTRBITS)==NONPTRBITS) {
    // data is one of the non-pointer types     
    if (isvar(data)) return (gint)WG_VARTYPE;
    if (issmallint(data)) return (gint)WG_INTTYPE;
    switch(data&LASTBYTEMASK) {
      case CHARBITS: return WG_CHARTYPE;
      case DATEBITS: return WG_DATETYPE;
      case TIMEBITS: return WG_TIMETYPE;
      case TINYSTRBITS: return WG_STRTYPE;
      case ANONCONSTBITS: return WG_ANONCONSTTYPE;
      default: return -1;
    }    
  }  
  // here we know data must be of ptr type
  // takes last three bits to decide the type
  // fullint is represented by two options: 001 and 101
  switch(data&NORMALPTRMASK) {    
    case DATARECBITS: return (gint)WG_RECORDTYPE;              
    case LONGSTRBITS:
      fieldoffset=decode_longstr_offset(data)+LONGSTR_REFCOUNT_POS;
      tmp=dbfetch(db,fieldoffset); 
      if (0) return (gint)WG_STRTYPE;
      if (0) return (gint)WG_URITYPE;
      if (0) return (gint)WG_XMLLITERALTYPE;
      return -1;
    case SHORTSTRBITS:   return (gint)WG_STRTYPE;
    case FULLDOUBLEBITS: return (gint)WG_DOUBLETYPE;
    case FULLINTBITSV0:  return (gint)WG_INTTYPE;
    case FULLINTBITSV1:  return (gint)WG_INTTYPE;     
    default: return -1;      
  }  
  return 0;
}  




wg_int wg_encode_int(void* db, wg_int data) {
  gint offset;
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_int");
    return 0;
  }
#endif  
  if (fits_smallint(data)) {
    return encode_smallint(data);
  } else {
    offset=alloc_word(db);
    if (!offset) {
      show_data_error_nr(db,"cannot store an integer in wg_set_int_field: ",data);       
      return 0;
    }
    dbstore(db,offset,data);
    return encode_fullint_offset(offset);
  }
}   

wg_int wg_decode_int(void* db, wg_int data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_int");
    return 0;
  }
#endif  
  if (issmallint(data)) return decode_smallint(data);
  if (isfullint(data)) return dbfetch(db,decode_fullint_offset(data)); 
  show_data_error_nr(db,"data given to wg_decode_int is not an encoded int: ",data); 
  return 0;
}  
    

wg_int wg_encode_double(void* db, double data) {
  gint offset;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_double");
    return 0;
  }
#endif  
  if (0) {
    // possible future case for tiny floats
  } else {
    offset=alloc_doubleword(db);
    if (!offset) {
      show_data_error_double(db,"cannot store a double in wg_set_double_field: ",data);       
      return 0;
    }    
    dbstore(db,offset,data);
    return encode_fulldouble_offset(offset);
  }
}   

double wg_decode_double(void* db, wg_int data) {

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_double");
    return 0;
  }
#endif  
  if (isfulldouble(data)) return *((double*)(offsettoptr(db,decode_fulldouble_offset(data))));
  show_data_error_nr(db,"data given to wg_decode_double is not an encoded double: ",data);
  return 0;
} 

gint wg_encode_str(void* db, char* str, char* lang) {
  gint offset;
  gint len;
  char* dptr;
  char* sptr;
  char* dendptr;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_str");
    return 0;
  }
  if (str==NULL) {
    show_data_error(db,"NULL string ptr given to wg_encode_str");
    return 0;
  }
#endif  
  len=(gint)(strlen(str));
  /*
  if (len<sizeof(gint)) {
    // tiny string, store directly
    data=TINYSTRBITS; // first zero the field and set last byte to mask
    // loop over bytes, storing them to data gint by shifting
    for(i=0,shft=(gint)(sizeof(gint)-1)*8; i<len; i++,shft=shft-8) {
      data = data |  ((*(str+i))<<shft); // shift byte to correct position
    }      
    return data;
  } else 
  */
  if (len<SHORTSTR_SIZE) {
    // short string, store in a fixlen area
    offset=alloc_shortstr(db);
    if (!offset) {
      show_data_error_str(db,"cannot store a string in wg_set_str_field",str);     
      return 0;     
    }    
    // loop over bytes, storing them starting from offset
    dptr=offsettoptr(db,offset);
    dendptr=dptr+SHORTSTR_SIZE;
    //
    //strcpy(dptr,sptr);
    //memset(dptr+len,0,SHORTSTR_SIZE-len);
    //
    for(sptr=str; (*dptr=*sptr)!=0; sptr++, dptr++) {}; // copy string
    for(dptr++; dptr<dendptr; dptr++) { *dptr=0; }; // zero the rest 
    // store offset to field    
    return encode_shortstr_offset(offset);
    //dbstore(db,ptrtoffset(record)+RECORD_HEADER_GINTS+fieldnr,encode_shortstr_offset(offset));    
  } else {
    offset=find_create_longstr(db,str,lang,WG_STRTYPE,strlen(str)+1);
    if (!offset) {
      show_data_error_nr(db,"cannot create a string of size ",strlen(str)); 
      return 0;
    }     
    return encode_longstr_offset(offset);        
  }
}  

gint find_create_longstr(void* db, char* data, char* extrastr, gint type, gint length) {
  gint offset;  
  gint i; 
  gint tmp;
  gint lengints;
  gint lenrest;
  char* lstrptr; 
  
  if (0) {
  } else {
    // find hash, check if exists and point or allocate new

    // allocate a new string    
    lengints=length/sizeof(gint);  // 7/4=1, 8/4=2, 9/4=2,  
    lenrest=length%sizeof(gint);  // 7%4=3, 8%4=0, 9%4=1,
    if (lenrest) lengints++;
    offset=alloc_gints(db,
                     &(((db_memsegment_header*)db)->longstr_area_header),
                    lengints+LONGSTR_HEADER_GINTS);
    if (!offset) {
      //show_data_error_nr(db,"cannot create a data string/blob of size ",length); 
      return 0;
    }      
    lstrptr=(char*)(offsettoptr(db,offset));
    // store string contents
    memcpy(lstrptr+(LONGSTR_HEADER_GINTS*sizeof(gint)),data,length);
    // zero the rest
    for(i=0;i<lenrest;i++) {
      *(lstrptr+(LONGSTR_HEADER_GINTS*sizeof(gint))+i)=0;
    }  
    // if extrastr exists, encode extrastr and store ptr to longstr record field
    if (extrastr!=NULL) {
      tmp=wg_encode_str(db,extrastr,NULL);      
      if (!tmp) {
        //show_data_error_nr(db,"cannot create an (extra)string of size ",strlen(extrastr)); 
        return 0;
      }          
      dbstore(db,offset+LONGSTR_EXTRASTR_POS,tmp);
      // increase extrastr refcount
      // if .... 
    } else {
      dbstore(db,offset+LONGSTR_EXTRASTR_POS,0); // no extrastr ptr
    }      
    // store metainfo: obj len and str len difference, plus type
    tmp=(((lengints+LONGSTR_HEADER_GINTS)*sizeof(gint))-lenrest)<<8; 
    tmp=tmp|type;
    dbstore(db,offset+LONGSTR_META_POS,tmp); // type and str length diff
    dbstore(db,offset+LONGSTR_REFCOUNT_POS,0); // not pointed from anywhere yet
    dbstore(db,offset+LONGSTR_BACKLINKS_POS,0); // no baclinks yet
    dbstore(db,offset+LONGSTR_HASHCHAIN_POS,0); // no hashchain ptr    
    // return result
    return encode_longstr_offset(offset);        
  }
  
}  


/**
* return length of a copied string, including terminating 0
*
* return 0 in case of error
*
*/

gint wg_decode_str(void* db, gint data, char* strbuf, gint buflen) { 
  gint i;
  char* dataptr;
  
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_str");
    return 0;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str is 0, not an encoded string"); 
    return 0;
  }
  if (strbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_str is 0, not a valid buffer pointer"); 
    return 0;
  }  
#endif  
  /*
  if (istinystr(data)) {    
    if (buflen<(sizeof(gint))) {
      show_data_error_nr(db,"insufficient buffer length given to wg_decode_str:",buflen); 
      return 0; 
    }      
    // loop over bytes, storing them to strbuf by shifting
    for(i=0,shft=(gint)8; i<sizeof(gint); i++,strbuf++,shft=shft+(gint)8) {
      data = data>>shft; // shift byte to correct position
      *strbuf=(unsigned char)(data&0xff);
      if ((data&0xff)==0) return i+1;
    }
    *strbuf=0;    
    return i+1;
  }
  */
  
  // limit=SHORTSTR_SIZE; ...
  if (islongstr(data)) {    
    dataptr=(char*)(offsettoptr(db,decode_longstr_offset(data)));
    // limit= ...
  } else {

  }
    
  if (isshortstr(data)) {
    dataptr=(char*)(offsettoptr(db,decode_shortstr_offset(data)));  
    for (i=1;i<SHORTSTR_SIZE && (*dataptr)!=0; i++,dataptr++,strbuf++) {
      if (i>=buflen) {
        show_data_error_nr(db,"insufficient buffer length given to wg_decode_str:",buflen); 
        return 0; 
      }  
      *strbuf=*dataptr;
    }      
    *strbuf=0;
    return i;    
  }    
  if (islongstr(data)) {
    dataptr=(char*)(offsettoptr(db,decode_longstr_offset(data)));
    for (i=1;i<SHORTSTR_SIZE && (*dataptr)!=0; i++,dataptr++,strbuf++) {
      if (i>=buflen) {
        show_data_error_nr(db,"insufficient buffer length given to wg_decode_str:",buflen); 
        return 0; 
      }  
      *strbuf=*dataptr;
    }      
    *strbuf=0;
    return i;    
  } 
  show_data_error(db,"data given to wg_decode_str is not an encoded string"); 
  return 0;
} 


wg_int wg_encode_record(void* db, void* data) {
  return (wg_int)(encode_datarec_offset(ptrtooffset(db,data)));
}  


void* wg_decode_record(void* db, wg_int data) {  
  return (void*)(offsettoptr(db,decode_datarec_offset(data)));
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

gint show_data_error_double(void* db, char* errmsg, double nr) {
  printf("wg data handling error: %s %f\n",errmsg,nr);
  return -1;
}

gint show_data_error_str(void* db, char* errmsg, char* str) {
  printf("wg data handling error: %s %s\n",errmsg,str);
  return -1;
}


