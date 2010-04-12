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
#include <time.h>
#include <sys/timeb.h>
//#include <math.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbdata.h"
#include "dbhash.h"
#include "dblog.h"
#include "dbindex.h"
#include "dbcompare.h"

/* ====== Private headers and defs ======== */

#ifdef _WIN32
//Thread-safe localtime_r appears not to be present on windows: emulate using win localtime, which is thread-safe.
static struct tm * localtime_r (const time_t *timer, struct tm *result);
#define sscanf sscanf_s  // warning: needs extra buflen args for string etc params
#define snprintf sprintf_s
#endif


/* ======= Private protos ================ */

#ifdef USE_BACKLINKING
static gint remove_backlink_index_entries(void *db, gint *record,
  gint value, gint depth);
static gint restore_backlink_index_entries(void *db, gint *record,
  gint value, gint depth);
#endif

static int isleap(unsigned yr);
static unsigned months_to_days (unsigned month);
static long years_to_days (unsigned yr);
static long ymd_to_scalar (unsigned yr, unsigned mo, unsigned day);
static void scalar_to_ymd (long scalar, unsigned *yr, unsigned *mo, unsigned *day);

static gint free_field_encoffset(void* db,gint encoffset);
static gint find_create_longstr(void* db, char* data, char* extrastr, gint type, gint length);

#ifdef USE_CHILD_DB
static void *get_offset_owner(void *db, gint offset);
#endif

static gint show_data_error(void* db, char* errmsg);
static gint show_data_error_nr(void* db, char* errmsg, gint nr);
static gint show_data_error_double(void* db, char* errmsg, double nr);
static gint show_data_error_str(void* db, char* errmsg, char* str);


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
  offset=wg_alloc_gints(db,
                     &(((db_memsegment_header*)db)->datarec_area_header),
                    length+RECORD_HEADER_GINTS);
  if (!offset) {
    show_data_error_nr(db,"cannot create a record of size ",length); 
    return 0;
  }      
  
#ifdef USE_DBLOG
  //logging
  wg_log_record(db,offset,length);
#endif
  
  /* Init header */
/*  dbstore(db, offset+RECORD_META_POS*sizeof(gint), 0); */
  dbstore(db, offset+RECORD_BACKLINKS_POS*sizeof(gint), 0);
  for(i=RECORD_HEADER_GINTS;i<length+RECORD_HEADER_GINTS;i++) {
    dbstore(db,offset+(i*(sizeof(gint))),0);
  }     
  
  return offsettoptr(db,offset);
}  

/** Delete record from database
 * returns 0 on success
 * returns -1 if the record is referenced by others and cannot be deleted.
 * returns -2 on general error
 * returns -3 on fatal error
 *
 * XXX: when USE_BACKLINKING is off, this function should be used
 * with extreme care.
 */
gint wg_delete_record(void* db, void *rec) {
  gint offset;
  gint* dptr;
  gint* dendptr;
  gint data;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db, "wrong database pointer given to wg_delete_record"); 
    return -2;
  }  
#endif 

#ifdef USE_BACKLINKING
  if(*((gint *) rec + RECORD_BACKLINKS_POS))
    return -1;
#endif

  /* Remove data from index */
  if(wg_index_del_rec(db, rec) < -1)
    return -3; /* index error */

  offset = ptrtooffset(db, rec);
#if defined(CHECK) && defined(USE_CHILD_DB)
  /* Check if it's a local record */
  if(get_offset_owner(db, offset) != db) {
    show_data_error(db, "not deleting an external record");
    return -2;
  }
#endif

  /* Loop over fields, freeing them */
  dendptr = (gint *) (((char *) rec) + datarec_size_bytes(*((gint *)rec)));
  for(dptr=(gint *)rec+RECORD_HEADER_GINTS; dptr<dendptr; dptr++) {
    data = *dptr;

#ifdef USE_BACKLINKING
    /* Is the field value a record pointer? If so, remove the backlink. */
#ifdef USE_CHILD_DB
    if(wg_get_encoded_type(db, data) == WG_RECORDTYPE &&
      get_offset_owner(db, decode_datarec_offset(data)) == db) {
#else
    if(wg_get_encoded_type(db, data) == WG_RECORDTYPE) {
#endif
      gint *child = wg_decode_record(db, data);
      gint *next_offset = child + RECORD_BACKLINKS_POS;
      gcell *old = NULL;

      while(*next_offset) {
        old = (gcell *) offsettoptr(db, *next_offset);
        if(old->car == offset) {
          gint old_offset = *next_offset;
          *next_offset = old->cdr; /* remove from list chain */
          wg_free_listcell(db, old_offset); /* free storage */
          goto recdel_backlink_removed;
        }
        next_offset = &(old->cdr);
      }
      show_data_error(db, "Corrupt backlink chain");
      return -3; /* backlink error */
    }
recdel_backlink_removed:
#endif

    if(isptr(data)) free_field_encoffset(db,data);
  }         

  /* Free the record storage */
  wg_free_object(db,
    &(((db_memsegment_header*)db)->datarec_area_header),
    offset);

  return 0;
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


/* ------------ backlink chain recursive functions ------------------- */

#ifdef USE_BACKLINKING

/** Remove index entries in backlink chain recursively.
 *  Needed for index maintenance when records are compared by their
 *  contens, as change in contents also changes the value of the entire
 *  record and thus affects it's placement in the index.
 *  Returns 0 for success
 *  Returns -1 in case of errors.
 */
static gint remove_backlink_index_entries(void *db, gint *record,
  gint value, gint depth) {
  gint col, length, err = 0;
  db_memsegment_header *dbh = (db_memsegment_header *) db;

  /* Find all fields in the record that match value (which is actually
   * a reference to a child record in encoded form) and remove it from
   * indexes. It will be recreated in the indexes by wg_set_field() later.
   */
  length = getusedobjectwantedgintsnr(*record) - RECORD_HEADER_GINTS;
  if(length > MAX_INDEXED_FIELDNR)
    length = MAX_INDEXED_FIELDNR + 1;

  for(col=0; col<length; col++) {
    if(*(record + RECORD_HEADER_GINTS + col) == value) {
      if(dbh->index_control_area_header.index_table[col]) {
        if(wg_index_del_field(db, record, col) < -1)
          return -1;
      }
    }
  }

  /* If recursive depth is not exchausted, continue with the parents
   * of this record.
   */
  if(depth > 0) {
    gint backlink_list = *(record + RECORD_BACKLINKS_POS);
    if(backlink_list) {
      gcell *next = (gcell *) offsettoptr(db, backlink_list);
      for(;;) {
        err = remove_backlink_index_entries(db, offsettoptr(db, next->car),
          wg_encode_record(db, record), depth-1);
        if(err)
          return err;
        if(!next->cdr)
          break;
        next = (gcell *) offsettoptr(db, next->cdr);
      }
    }
  }

  return 0;
}

/** Add index entries in backlink chain recursively.
 *  Called after doing remove_backling_index_entries() and updating
 *  data in the record that originated the call. This recreates the
 *  entries in the indexes for all the records that were affected.
 *  Returns 0 for success
 *  Returns -1 in case of errors.
 */
static gint restore_backlink_index_entries(void *db, gint *record,
  gint value, gint depth) {
  gint col, length, err = 0;
  db_memsegment_header *dbh = (db_memsegment_header *) db;

  /* Find all fields in the record that match value (which is actually
   * a reference to a child record in encoded form) and add it back to
   * indexes.
   */
  length = getusedobjectwantedgintsnr(*record) - RECORD_HEADER_GINTS;
  if(length > MAX_INDEXED_FIELDNR)
    length = MAX_INDEXED_FIELDNR + 1;

  for(col=0; col<length; col++) {
    if(*(record + RECORD_HEADER_GINTS + col) == value) {
      if(dbh->index_control_area_header.index_table[col]) {
        if(wg_index_add_field(db, record, col) < -1)
          return -1;
      }
    }
  }

  /* Continue to the parents until depth==0 */
  if(depth > 0) {
    gint backlink_list = *(record + RECORD_BACKLINKS_POS);
    if(backlink_list) {
      gcell *next = (gcell *) offsettoptr(db, backlink_list);
      for(;;) {
        err = restore_backlink_index_entries(db, offsettoptr(db, next->car),
          wg_encode_record(db, record), depth-1);
        if(err)
          return err;
        if(!next->cdr)
          break;
        next = (gcell *) offsettoptr(db, next->cdr);
      }
    }
  }

  return 0;
}

#endif

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

/** Update contents of one field
 *  returns 0 if successful
 *  returns -1 if invalid db pointer passed (by recordcheck macro)
 *  returns -2 if invalid record passed (by recordcheck macro)
 *  returns -3 for fatal index error
 *  returns -4 for backlink-related error
 */
wg_int wg_set_field(void* db, void* record, wg_int fieldnr, wg_int data) {
  gint* fieldadr;
  gint fielddata;
  gint* strptr;
#ifdef USE_BACKLINKING
  gint backlink_list;           /** start of backlinks for this record */
  gint rec_enc = WG_ILLEGAL;    /** this record as encoded value. */
#endif

#ifdef CHECK
  recordcheck(db,record,fieldnr,"wg_set_field");
#endif 

  /* Read the old encoded value */
  fieldadr=((gint*)record)+RECORD_HEADER_GINTS+fieldnr;
  fielddata=*fieldadr;

  /* Update index while the old value is still in the db */
  if(fieldnr<=MAX_INDEXED_FIELDNR &&\
    ((db_memsegment_header *) db)->index_control_area_header.index_table[fieldnr]) {
    if(wg_index_del_field(db, record, fieldnr) < -1)
      if(fielddata) /* NULL-s are allowed to be missing (currently) */
        return -3; /* index error */
  }

  /* If there are backlinks, go up the chain and remove the reference
   * to this record from all indexes (updating a field in the record
   * causes the value of the record to change). Note that we only go
   * as far as the recursive comparison depth - records higher in the
   * hierarchy are not affected.
   */
#if defined(USE_BACKLINKING) && (WG_COMPARE_REC_DEPTH > 0)
  backlink_list = *((gint *) record + RECORD_BACKLINKS_POS);
  if(backlink_list) {
    gint err;
    gcell *next = (gcell *) offsettoptr(db, backlink_list);
    rec_enc = wg_encode_record(db, record);
    for(;;) {
      err = remove_backlink_index_entries(db, offsettoptr(db, next->car),
        rec_enc, WG_COMPARE_REC_DEPTH-1);
      if(err) {
        return -4; /* override the error code, for now. */
      }
      if(!next->cdr)
        break;
      next = (gcell *) offsettoptr(db, next->cdr);
    }
  }
#endif

#ifdef USE_BACKLINKING
  /* Is the old field value a record pointer? If so, remove the backlink.
   * XXX: this can be optimized to use a custom macro instead of
   * wg_get_encoded_type().
   */
#ifdef USE_CHILD_DB
  /* Only touch local records */
  if(wg_get_encoded_type(db, fielddata) == WG_RECORDTYPE &&
    get_offset_owner(db, decode_datarec_offset(fielddata)) == db) {
#else
  if(wg_get_encoded_type(db, fielddata) == WG_RECORDTYPE) {
#endif
    gint *rec = wg_decode_record(db, fielddata);
    gint *next_offset = rec + RECORD_BACKLINKS_POS;
    gint parent_offset = ptrtooffset(db, record);
    gcell *old = NULL;

    while(*next_offset) {
      old = (gcell *) offsettoptr(db, *next_offset);
      if(old->car == parent_offset) {
        gint old_offset = *next_offset;
        *next_offset = old->cdr; /* remove from list chain */
        wg_free_listcell(db, old_offset); /* free storage */
        goto setfld_backlink_removed;
      }
      next_offset = &(old->cdr);
    }
    show_data_error(db, "Corrupt backlink chain");
    return -4; /* backlink error */
  }
setfld_backlink_removed:
#endif
  
  //printf("wg_set_field adr %d offset %d\n",fieldadr,ptrtooffset(db,fieldadr));
  if (isptr(fielddata)) {
    //printf("wg_set_field freeing old data\n"); 
    free_field_encoffset(db,fielddata);
  }    
  (*fieldadr)=data; // store data to field
#ifdef USE_CHILD_DB
  if (islongstr(data) &&
    get_offset_owner(db, decode_longstr_offset(data)) == db) {
#else
  if (islongstr(data)) {
#endif
    // increase data refcount for longstr-s 
    strptr=offsettoptr(db,decode_longstr_offset(data)); 
    ++(*(strptr+LONGSTR_REFCOUNT_POS));               
  }                        

  /* Update index after new value is written */
  if(fieldnr<=MAX_INDEXED_FIELDNR &&\
    ((db_memsegment_header *) db)->index_control_area_header.index_table[fieldnr]) {
    if(wg_index_add_field(db, record, fieldnr) < -1)
      return -3;
  }

#ifdef USE_BACKLINKING
  /* Is the new field value a record pointer? If so, add a backlink */
#ifdef USE_CHILD_DB
  if(wg_get_encoded_type(db, data) == WG_RECORDTYPE &&
    get_offset_owner(db, decode_datarec_offset(data)) == db) {
#else
  if(wg_get_encoded_type(db, data) == WG_RECORDTYPE) {
#endif
    gint *rec = wg_decode_record(db, data);
    gint *next_offset = rec + RECORD_BACKLINKS_POS;
    gint new_offset = wg_alloc_fixlen_object(db, 
      &(((db_memsegment_header *) db)->listcell_area_header));
    gcell *new = (gcell *) offsettoptr(db, new_offset);

    while(*next_offset)
      next_offset = &(((gcell *) offsettoptr(db, *next_offset))->cdr);
    new->car = ptrtooffset(db, record);
    new->cdr = 0;
    *next_offset = new_offset;
  }
#endif
  
#if defined(USE_BACKLINKING) && (WG_COMPARE_REC_DEPTH > 0)
  /* Create new entries in indexes in all referring records */
  if(backlink_list) {
    gint err;
    gcell *next = (gcell *) offsettoptr(db, backlink_list);
    for(;;) {
      err = restore_backlink_index_entries(db, offsettoptr(db, next->car),
        rec_enc, WG_COMPARE_REC_DEPTH-1);
      if(err) {
        return -4;
      }
      if(!next->cdr)
        break;
      next = (gcell *) offsettoptr(db, next->cdr);
    }
  }
#endif

  return 0;
}
  
wg_int wg_set_int_field(void* db, void* record, wg_int fieldnr, gint data) {
  gint fielddata;
  fielddata=wg_encode_int(db,data);
  //printf("wg_set_int_field data %d encoded %d\n",data,fielddata);
  if (fielddata==WG_ILLEGAL) return -1;
#ifdef USE_DBLOG
  wg_log_int(db,record,fieldnr,data);
#endif
  return wg_set_field(db,record,fieldnr,fielddata);
}  
  
wg_int wg_set_double_field(void* db, void* record, wg_int fieldnr, double data) {  
  gint fielddata;
  
  fielddata=wg_encode_double(db,data);
  if (fielddata==WG_ILLEGAL) return -1;
  return wg_set_field(db,record,fieldnr,fielddata);
} 

wg_int wg_set_str_field(void* db, void* record, wg_int fieldnr, char* data) {
  gint fielddata;

  fielddata=wg_encode_str(db,data,NULL);
  if (fielddata==WG_ILLEGAL) return -1;
  return wg_set_field(db,record,fieldnr,fielddata);
} 
  
wg_int wg_set_rec_field(void* db, void* record, wg_int fieldnr, void* data) {
  gint fielddata;

  fielddata=wg_encode_record(db,data);
  if (fielddata==WG_ILLEGAL) return -1;
  return wg_set_field(db,record,fieldnr,fielddata);
} 

wg_int wg_get_field(void* db, void* record, wg_int fieldnr) {
 
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error_nr(db,"wrong database pointer given to wg_get_field",fieldnr);
    return WG_ILLEGAL;
  }
  if (fieldnr<0 || (getusedobjectwantedgintsnr(*((gint*)record))<=fieldnr+RECORD_HEADER_GINTS)) {
    show_data_error_nr(db,"wrong field number given to wg_get_field",fieldnr);\
    return WG_ILLEGAL;
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
  if (isptr(data)) return free_field_encoffset(db,data);   
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

static gint free_field_encoffset(void* db,gint encoffset) {
  gint offset;
#if 0
  gint* dptr;
  gint* dendptr;
  gint data;     
  gint i;
#endif
  gint tmp;
  gint* objptr;
  gint* extrastr;
  
  // takes last three bits to decide the type
  // fullint is represented by two options: 001 and 101
  switch(encoffset&NORMALPTRMASK) {    
    case DATARECBITS:               
#if 0
/* This section of code in quarantine */
      // remove from list
      // refcount check
      offset=decode_datarec_offset(encoffset);      
      tmp=dbfetch(db,offset+sizeof(gint)*LONGSTR_REFCOUNT_POS);
      tmp--;
      if (tmp>0) {
        dbstore(db,offset+LONGSTR_REFCOUNT_POS,tmp);
      } else {
        // free frompointers structure
        // loop over fields, freeing them
        dptr=offsettoptr(db,offset);       
        dendptr=(gint*)(((char*)dptr)+datarec_size_bytes(*dptr));
        for(i=0,dptr=dptr+RECORD_HEADER_GINTS;dptr<dendptr;dptr++,i++) {
          data=*dptr;
          if (isptr(data)) free_field_encoffset(db,data);
        }         
        // really free object from area
        wg_free_object(db,&(((db_memsegment_header*)db)->datarec_area_header),offset);          
      }  
#endif
      break;
    case LONGSTRBITS:
      offset=decode_longstr_offset(encoffset);
#ifdef USE_CHILD_DB
      if(get_offset_owner(db, offset) != db)
        break; /* Non-local reference, ignore it */
#endif
      // refcount check
      tmp=dbfetch(db,offset+sizeof(gint)*LONGSTR_REFCOUNT_POS);    
      tmp--;           
      if (tmp>0) {
        dbstore(db,offset+sizeof(gint)*LONGSTR_REFCOUNT_POS,tmp);
      } else {
        objptr=offsettoptr(db,offset);        
        extrastr=(gint*)(((char*)(objptr))+(sizeof(gint)*LONGSTR_EXTRASTR_POS));
        tmp=*extrastr;        
        // remove from hash
        wg_remove_from_strhash(db,encoffset);
        // remove extrastr
        if (tmp!=0) free_field_encoffset(db,tmp);
        *extrastr=0;        
        // really free object from area  
        wg_free_object(db,&(((db_memsegment_header*)db)->longstr_area_header),offset);
      }  
      break;      
    case SHORTSTRBITS:
#ifdef USE_CHILD_DB
      offset = decode_shortstr_offset(encoffset);
      if(get_offset_owner(db, offset) != db)
        break; /* Non-local reference, ignore it */
      wg_free_shortstr(db, offset);
#else
      wg_free_shortstr(db,decode_shortstr_offset(encoffset));
#endif
      break;      
    case FULLDOUBLEBITS:
#ifdef USE_CHILD_DB
      offset = decode_fulldouble_offset(encoffset);
      if(get_offset_owner(db, offset) != db)
        break; /* Non-local reference, ignore it */
      wg_free_doubleword(db, offset);
#else
      wg_free_doubleword(db,decode_fulldouble_offset(encoffset));
#endif
      break;
    case FULLINTBITSV0:
#ifdef USE_CHILD_DB
      offset = decode_fullint_offset(encoffset);
      if(get_offset_owner(db, offset) != db)
        break; /* Non-local reference, ignore it */
      wg_free_word(db, offset);
#else
      wg_free_word(db,decode_fullint_offset(encoffset));
#endif
      break;
    case FULLINTBITSV1:
#ifdef USE_CHILD_DB
      offset = decode_fullint_offset(encoffset);
      if(get_offset_owner(db, offset) != db)
        break; /* Non-local reference, ignore it */
      wg_free_word(db, offset);
#else
      wg_free_word(db,decode_fullint_offset(encoffset));
#endif
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
      case FIXPOINTBITS: return WG_FIXPOINTTYPE;
      case DATEBITS: return WG_DATETYPE;
      case TIMEBITS: return WG_TIMETYPE;
      case TINYSTRBITS: return WG_STRTYPE;
      case VARBITS: return WG_VARTYPE;
      case ANONCONSTBITS: return WG_ANONCONSTTYPE;
      default: return -1;
    }    
  }  
  // here we know data must be of ptr type
  // takes last three bits to decide the type
  // fullint is represented by two options: 001 and 101
  //printf("cp0\n");
  switch(data&NORMALPTRMASK) {        
    case DATARECBITS: return (gint)WG_RECORDTYPE;              
    case LONGSTRBITS:
      //printf("cp1\n");
      fieldoffset=decode_longstr_offset(data)+LONGSTR_META_POS*sizeof(gint);
      //printf("fieldoffset %d\n",fieldoffset);
      tmp=dbfetch(db,fieldoffset); 
      //printf("str meta %d lendiff %d subtype %d\n",
      //  tmp,(tmp&LONGSTR_META_LENDIFMASK)>>LONGSTR_META_LENDIFSHFT,tmp&LONGSTR_META_TYPEMASK);      
      return tmp&LONGSTR_META_TYPEMASK; // WG_STRTYPE, WG_URITYPE, WG_XMLLITERALTYPE     
    case SHORTSTRBITS:   return (gint)WG_STRTYPE;
    case FULLDOUBLEBITS: return (gint)WG_DOUBLETYPE;
    case FULLINTBITSV0:  return (gint)WG_INTTYPE;
    case FULLINTBITSV1:  return (gint)WG_INTTYPE;     
    default: return -1;      
  }  
  return 0;
}  


char* wg_get_type_name(void* db, wg_int type) {
  switch (type) {
    case WG_NULLTYPE: return "null";
    case WG_RECORDTYPE: return "record";
    case WG_INTTYPE: return "int";
    case WG_DOUBLETYPE: return "double";
    case WG_STRTYPE: return "string";
    case WG_XMLLITERALTYPE: return "xmlliteral";
    case WG_URITYPE: return "uri";
    case WG_BLOBTYPE: return "blob";
    case WG_CHARTYPE: return "char";
    case WG_FIXPOINTTYPE: return "fixpoint";
    case WG_DATETYPE: return "date";
    case WG_TIMETYPE: return "time";
    case WG_ANONCONSTTYPE: return "anonconstant";
    case WG_VARTYPE: return "var";
    default: return "unknown";
  }    
}  


wg_int wg_encode_null(void* db, char* data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_null");
    return WG_ILLEGAL;
  }
  if (data!=NULL) {
    show_data_error(db,"data given to wg_encode_null is not NULL");
    return WG_ILLEGAL;
  }
#endif   
  return (gint)0;
}   

char* wg_decode_null(void* db,wg_int data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_null");
    return NULL;
  }
  if (data!=(gint)0) {
    show_data_error(db,"data given to wg_decode_null is not an encoded NULL");
    return NULL;
  }
#endif   
  return NULL;
}  

wg_int wg_encode_int(void* db, wg_int data) {
  gint offset;
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_int");
    return WG_ILLEGAL;
  }
#endif  
  if (fits_smallint(data)) {
    return encode_smallint(data);
  } else {
    offset=alloc_word(db);
    if (!offset) {
      show_data_error_nr(db,"cannot store an integer in wg_set_int_field: ",data);       
      return WG_ILLEGAL;
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



wg_int wg_encode_char(void* db, char data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_char");
    return WG_ILLEGAL;
  }
#endif  
  return (wg_int)(encode_char((wg_int)data));
}  
  

char wg_decode_char(void* db, wg_int data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_char");
    return 0;
  }
#endif  
  return (char)(decode_char(data));
}  


wg_int wg_encode_double(void* db, double data) {
  gint offset;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_double");
    return WG_ILLEGAL;
  }
#endif  
  if (0) {
    // possible future case for tiny floats
  } else {
    offset=alloc_doubleword(db);   
    if (!offset) {
      show_data_error_double(db,"cannot store a double in wg_set_double_field: ",data);       
      return WG_ILLEGAL;
    }        
    *((double*)(offsettoptr(db,offset)))=data;
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


wg_int wg_encode_fixpoint(void* db, double data) {
 
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_fixpoint");
    return WG_ILLEGAL;
  } 
  if (!fits_fixpoint(data)) {
    show_data_error(db,"argument given to wg_encode_fixpoint too big or too small");
    return WG_ILLEGAL;
  }
#endif  
  return encode_fixpoint(data); 
}   

double wg_decode_fixpoint(void* db, wg_int data) {

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_double");
    return 0;
  }
#endif  
  if (isfixpoint(data)) return decode_fixpoint(data);
  show_data_error_nr(db,"data given to wg_decode_fixpoint is not an encoded fixpoint: ",data);
  return 0;
} 


wg_int wg_encode_date(void* db, int data) {

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_date");
    return WG_ILLEGAL;
  }
  if (!fits_date(data)) {
    show_data_error(db,"argument given to wg_encode_date too big or too small");
    return WG_ILLEGAL;
  }
#endif    
  return encode_date(data);  
}   

int wg_decode_date(void* db, wg_int data) {

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_date");
    return 0;
  }
#endif  
  if (isdate(data)) return decode_date(data);
  show_data_error_nr(db,"data given to wg_decode_date is not an encoded date: ",data);
  return 0;
} 

wg_int wg_encode_time(void* db, int data) {
  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_time");
    return WG_ILLEGAL;
  }
  if (!fits_time(data)) {
    show_data_error(db,"argument given to wg_encode_time too big or too small");
    return WG_ILLEGAL;
  }
#endif  
  return encode_time(data);
}   

int wg_decode_time(void* db, wg_int data) {

#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_time");
    return 0;
  }
#endif  
  if (istime(data)) return decode_time(data);
  show_data_error_nr(db,"data given to wg_decode_time is not an encoded time: ",data);
  return 0;
} 

int wg_current_utcdate(void* db) {
  time_t ts;
  int epochadd=719163; // y 1970 m 1 d 1
  
  ts=time(NULL); // secs since Epoch 1970
  return (int)(ts/(24*60*60))+epochadd;                    
}  

int wg_current_localdate(void* db) {  
  time_t esecs;    
  int res;
  struct tm *now;
  struct tm ctime;
  
  esecs=time(NULL); // secs since Epoch 1970tstruct.time;  
  now=localtime_r(&esecs,&ctime);
  res=ymd_to_scalar(ctime.tm_year+1900,ctime.tm_mon+1,ctime.tm_mday);
  return res;    
}


int wg_current_utctime(void* db) {  
  struct timeb tstruct; 
  int esecs;  
  int days;
  int secs;
  int milli;
  int secsday=24*60*60;
  
  ftime(&tstruct);
  esecs=(int)(tstruct.time);
  milli=tstruct.millitm;  
  days=esecs/secsday;
  secs=esecs-(days*secsday);   
  return (secs*100)+(milli/10);  
} 

int wg_current_localtime(void* db) {
  struct timeb tstruct; 
  time_t esecs;    
  int secs;
  int milli;
  struct tm *now;
  struct tm ctime;
  
  ftime(&tstruct);
  esecs=tstruct.time;
  milli=tstruct.millitm;  
  now=localtime_r(&esecs,&ctime);
  secs=ctime.tm_hour*60*60+ctime.tm_min*60+ctime.tm_sec;  
  return (secs*100)+(milli/10);    
}

int wg_strf_iso_datetime(void* db, int date, int time, char* buf) {
  unsigned yr, mo, day, hr, min, sec, spart;
  int t=time;
  int tmp;
  int c;
  
  hr=t/(60*60*100);
  t=t-(hr*(60*60*100));
  min=t/(60*100);
  t=t-(min*(60*100));
  sec=t/100;
  t=t-(sec*(100));
  spart=t;
  
  tmp=hr*(60*60*100)+min*(60*100)+sec*(100)+spart;
  scalar_to_ymd(date,&yr,&mo,&day);
  c=snprintf(buf,24,"%04d-%02d-%02dT%02d:%02d:%02d.%02d",yr,mo,day,hr,min,sec,spart);
  return(c);
}  

int wg_strp_iso_date(void* db, char* inbuf) {
  int sres;
  int yr=0;
  int mo=0;
  int day=0;
  int res;
  
  sres=sscanf(inbuf,"%4d-%2d-%2d",&yr,&mo,&day);      
  if (sres<3 || yr<0 || mo<1 || mo>12 || day<1 || day>31) return -1;
  res=ymd_to_scalar(yr,mo,day);  
  return res;      
}  


int wg_strp_iso_time(void* db, char* inbuf) {    
  int sres;
  int hr=0;
  int min=0;
  int sec=0;
  int prt=0;

  sres=sscanf(inbuf,"%2d:%2d:%2d.%2d",&hr,&min,&sec,&prt);     
  if (sres<3 || hr<0 || hr>24 || min<0 || min>60 || sec<0 || sec>60 || prt<0 || prt>99) return -1;  
  return hr*(60*60*100)+min*(60*100)+sec*100+prt;
}  


int wg_ymd_to_date(void* db, int yr, int mo, int day) {
  if (yr<0 || mo<1 || mo>12 || day<1 || day>31) return -1;
  return ymd_to_scalar(yr,mo,day);  
}


int wg_hms_to_time(void* db, int hr, int min, int sec, int prt) {
  if (hr<0 || hr>24 || min<0 || min>60 || sec<0 || sec>60 || prt<0 || prt>99)
    return -1;
  return hr*(60*60*100)+min*(60*100)+sec*100+prt;
}


void wg_date_to_ymd(void* db, int date, int *yr, int *mo, int *day) {
  unsigned int y, m, d;

  scalar_to_ymd(date, &y, &m, &d);
  *yr=y;
  *mo=m;
  *day=d;
}


void wg_time_to_hms(void* db, int time, int *hr, int *min, int *sec, int *prt) {
  int t=time;
  
  *hr=t/(60*60*100);
  t=t-(*hr * (60*60*100));
  *min=t/(60*100);
  t=t-(*min * (60*100));
  *sec=t/100;
  t=t-(*sec * (100));
  *prt=t;
}


// record

wg_int wg_encode_record(void* db, void* data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_char");
    return WG_ILLEGAL;
  }
#endif 
  return (wg_int)(encode_datarec_offset(ptrtooffset(db,data)));
}  


void* wg_decode_record(void* db, wg_int data) {  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_char");
    return 0;
  }
#endif   
  return (void*)(offsettoptr(db,decode_datarec_offset(data)));
} 


  


/* ============================================

Separate string, xmlliteral, uri, blob funs
call universal funs defined later

============================================== */

/* string */

wg_int wg_encode_str(void* db, char* str, char* lang) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_str");
    return WG_ILLEGAL;
  }
  if (str==NULL) {
    show_data_error(db,"NULL string ptr given to wg_encode_str");
    return WG_ILLEGAL;
  }
#endif 
  return wg_encode_unistr(db,str,lang,WG_STRTYPE);
}
  

char* wg_decode_str(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_str");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str is 0, not an encoded string"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr(db,data,WG_STRTYPE);
}


wg_int wg_decode_str_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_str_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str_len is 0, not an encoded string"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_len(db,data,WG_STRTYPE);
}  



wg_int wg_decode_str_copy(void* db, wg_int data, char* strbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_str_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str_copy is 0, not an encoded string"); 
    return -1;
  }
  if (strbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_str_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_str_copy is 0 or less"); 
    return -1;
  }   
#endif  
  return wg_decode_unistr_copy(db,data,strbuf,buflen,WG_STRTYPE);
}  


char* wg_decode_str_lang(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_str");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str_lang is 0, not an encoded string"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr_lang(db,data,WG_STRTYPE);
}


wg_int wg_decode_str_lang_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_str_lang_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str_lang_len is 0, not an encoded string"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_len(db,data,WG_STRTYPE);
}



wg_int wg_decode_str_lang_copy(void* db, wg_int data, char* langbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_str_lang_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_str_lang_copy is 0, not an encoded string"); 
    return -1;
  }
  if (langbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_str_lang_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_str_lang_copy is 0 or less"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_copy(db,data,langbuf,buflen,WG_STRTYPE);
}  


/* xmlliteral */


wg_int wg_encode_xmlliteral(void* db, char* str, char* xsdtype) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_xmlliteral");
    return WG_ILLEGAL;
  }
  if (str==NULL) {
    show_data_error(db,"NULL string ptr given to wg_encode_xmlliteral");
    return WG_ILLEGAL;
  }
  if (xsdtype==NULL) {
    show_data_error(db,"NULL xsdtype ptr given to wg_encode_xmlliteral");
    return WG_ILLEGAL;
  } 
#endif    
  return wg_encode_unistr(db,str,xsdtype,WG_XMLLITERALTYPE);
}
  

char* wg_decode_xmlliteral(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_xmlliteral");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_xmlliteral is 0, not an encoded xmlliteral"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr(db,data,WG_XMLLITERALTYPE);
}


wg_int wg_decode_xmlliteral_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_xmlliteral_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_xmlliteral_len is 0, not an encoded xmlliteral"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_len(db,data,WG_XMLLITERALTYPE);
}  



wg_int wg_decode_xmlliteral_copy(void* db, wg_int data, char* strbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_xmlliteral_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_xmlliteral_copy is 0, not an encoded xmlliteral"); 
    return -1;
  }
  if (strbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_xmlliteral_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_xmlliteral_copy is 0 or less"); 
    return -1;
  }   
#endif  
  return wg_decode_unistr_copy(db,data,strbuf,buflen,WG_XMLLITERALTYPE);
}  


char* wg_decode_xmlliteral_xsdtype(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_xmlliteral");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_xmlliteral_xsdtype is 0, not an encoded xmlliteral"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr_lang(db,data,WG_XMLLITERALTYPE);
}


wg_int wg_decode_xmlliteral_xsdtype_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_xmlliteral_xsdtype_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_xmlliteral_lang_xsdtype is 0, not an encoded xmlliteral"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_len(db,data,WG_XMLLITERALTYPE);
}



wg_int wg_decode_xmlliteral_xsdtype_copy(void* db, wg_int data, char* langbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_xmlliteral_xsdtype_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_xmlliteral_xsdtype_copy is 0, not an encoded xmlliteral"); 
    return -1;
  }
  if (langbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_xmlliteral_xsdtype_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_xmlliteral_xsdtype_copy is 0 or less"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_copy(db,data,langbuf,buflen,WG_XMLLITERALTYPE);
}  


/* uri */


wg_int wg_encode_uri(void* db, char* str, char* prefix) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_uri");
    return WG_ILLEGAL;
  }
  if (str==NULL) {
    show_data_error(db,"NULL string ptr given to wg_encode_uri");
    return WG_ILLEGAL;
  }
#endif 
  return wg_encode_unistr(db,str,prefix,WG_URITYPE);
}
  

char* wg_decode_uri(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_uri");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_uri is 0, not an encoded string"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr(db,data,WG_URITYPE);
}


wg_int wg_decode_uri_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_uri_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_uri_len is 0, not an encoded string"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_len(db,data,WG_URITYPE);
}  



wg_int wg_decode_uri_copy(void* db, wg_int data, char* strbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_uri_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_uri_copy is 0, not an encoded string"); 
    return -1;
  }
  if (strbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_uri_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_uri_copy is 0 or less"); 
    return -1;
  }   
#endif  
  return wg_decode_unistr_copy(db,data,strbuf,buflen,WG_URITYPE);
}  


char* wg_decode_uri_prefix(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_uri_prefix");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_uri_prefix is 0, not an encoded uri"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr_lang(db,data,WG_URITYPE);
}


wg_int wg_decode_uri_prefix_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_uri_prefix_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_uri_prefix_len is 0, not an encoded string"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_len(db,data,WG_URITYPE);
}



wg_int wg_decode_uri_prefix_copy(void* db, wg_int data, char* langbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_uri_prefix_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_uri_prefix_copy is 0, not an encoded string"); 
    return -1;
  }
  if (langbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_uri_prefix_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_uri_prefix_copy is 0 or less"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_copy(db,data,langbuf,buflen,WG_URITYPE);
}  


/* blob */


wg_int wg_encode_blob(void* db, char* str, char* type, wg_int len) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_blob");
    return WG_ILLEGAL;
  }
  if (str==NULL) {
    show_data_error(db,"NULL string ptr given to wg_encode_blob");
    return WG_ILLEGAL;
  }
#endif 
  return wg_encode_uniblob(db,str,type,WG_BLOBTYPE,len);
}
  

char* wg_decode_blob(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_blob");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_blob is 0, not an encoded string"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr(db,data,WG_BLOBTYPE);
}


wg_int wg_decode_blob_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_blob_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_blob_len is 0, not an encoded string"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_len(db,data,WG_BLOBTYPE)+1;
}  



wg_int wg_decode_blob_copy(void* db, wg_int data, char* strbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_blob_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_blob_copy is 0, not an encoded string"); 
    return -1;
  }
  if (strbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_blob_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_blob_copy is 0 or less"); 
    return -1;
  }   
#endif  
  return wg_decode_unistr_copy(db,data,strbuf,buflen,WG_BLOBTYPE);
}  


char* wg_decode_blob_type(void* db, wg_int data) {   
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_blob_type");
    return NULL;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_blob_type is 0, not an encoded blob"); 
    return NULL;
  }
#endif  
  return wg_decode_unistr_lang(db,data,WG_BLOBTYPE);
}


wg_int wg_decode_blob_type_len(void* db, wg_int data) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_blob_type_len");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_blob_type_len is 0, not an encoded string"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_len(db,data,WG_BLOBTYPE);
}



wg_int wg_decode_blob_type_copy(void* db, wg_int data, char* langbuf, wg_int buflen) {
#ifdef CHECK  
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_blob_type_copy");
    return -1;
  }
  if (!data) {
    show_data_error(db,"data given to wg_decode_blob_type_copy is 0, not an encoded string"); 
    return -1;
  }
  if (langbuf==NULL) {
     show_data_error(db,"buffer given to wg_decode_blob_type_copy is 0, not a valid buffer pointer"); 
    return -1;
  }
  if (buflen<1) {
     show_data_error(db,"buffer len given to wg_decode_blob_type_copy is 0 or less"); 
    return -1;
  }
#endif  
  return wg_decode_unistr_lang_copy(db,data,langbuf,buflen,WG_BLOBTYPE);
}


/* ============================================

Universal funs for string, xmlliteral, uri, blob

============================================== */


gint wg_encode_unistr(void* db, char* str, char* lang, gint type) {
  gint offset;
  gint len;
#ifdef USETINYSTR  
  gint res;
#endif  
  char* dptr;
  char* sptr;
  char* dendptr;

  len=(gint)(strlen(str));  
#ifdef USETINYSTR  
  if (lang==NULL && type==WG_STRTYPE && len<(sizeof(gint)-1)) {
    res=TINYSTRBITS; // first zero the field and set last byte to mask
    if (LITTLEENDIAN) {
      dptr=((char*)(&res))+1; // type bits stored in lowest addressed byte
    } else {
      dptr=((char*)(&res));  // type bits stored in highest addressed byte
    }    
    memcpy(dptr,str,len+1);     
    return res;
  }   
#endif      
  if (lang==NULL && type==WG_STRTYPE && len<SHORTSTR_SIZE) {
    // short string, store in a fixlen area
    offset=alloc_shortstr(db);
    if (!offset) {
      show_data_error_str(db,"cannot store a string in wg_encode_unistr",str);     
      return WG_ILLEGAL;     
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
    offset=find_create_longstr(db,str,lang,type,strlen(str)+1);
    if (!offset) {
      show_data_error_nr(db,"cannot create a string of size ",strlen(str)); 
      return WG_ILLEGAL;
    }     
    return encode_longstr_offset(offset);        
  }
}  


gint wg_encode_uniblob(void* db, char* str, char* lang, gint type, gint len) {
  gint offset;

  if (0) {
  } else {
    offset=find_create_longstr(db,str,lang,type,len);
    if (!offset) {
      show_data_error_nr(db,"cannot create a blob of size ",len); 
      return WG_ILLEGAL;
    }     
    return encode_longstr_offset(offset);        
  }
}  


static gint find_create_longstr(void* db, char* data, char* extrastr, gint type, gint length) {
  db_memsegment_header* dbh;
  gint offset;  
  gint i; 
  gint tmp;
  gint lengints;
  gint lenrest;
  char* lstrptr;
  gint old=0; 
  int hash;
  gint hasharrel;
  gint res;
   
  dbh=(db_memsegment_header*)db;
  if (0) {
  } else {

    // find hash, check if exists and use if found   
    hash=wg_hash_typedstr(dbh,data,extrastr,type,length);
    //hasharrel=((gint*)(offsettoptr(db,((db->strhash_area_header).arraystart))))[hash];       
    hasharrel=dbfetch(db,((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash));
    //printf("hash %d((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash) %d hasharrel %d\n",
    //        hash,((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash), hasharrel);  
    if (hasharrel) old=wg_find_strhash_bucket(db,data,extrastr,type,length,hasharrel);
    //printf("old %d \n",old);
    if (old) {
      //printf("str found in hash\n");
      return old; 
    } 
    //printf("str not found in hash\n");    
    //printf("hasharrel 1 %d \n",hasharrel);     
    // equal string not found in hash
    // allocate a new string    
    lengints=length/sizeof(gint);  // 7/4=1, 8/4=2, 9/4=2,  
    lenrest=length%sizeof(gint);  // 7%4=3, 8%4=0, 9%4=1,   
    if (lenrest) lengints++;
    offset=wg_alloc_gints(db,
                     &(((db_memsegment_header*)db)->longstr_area_header),
                    lengints+LONGSTR_HEADER_GINTS);         
    if (!offset) {
      //show_data_error_nr(db,"cannot create a data string/blob of size ",length); 
      return 0;
    }      
    lstrptr=(char*)(offsettoptr(db,offset));
    // store string contents
    memcpy(lstrptr+(LONGSTR_HEADER_GINTS*sizeof(gint)),data,length);
    //zero the rest
    for(i=0;i<lenrest;i++) {
      *(lstrptr+length+(LONGSTR_HEADER_GINTS*sizeof(gint))+i)=0;
    }  
    // if extrastr exists, encode extrastr and store ptr to longstr record field
    if (extrastr!=NULL) {
      tmp=wg_encode_str(db,extrastr,NULL);            
      if (tmp==WG_ILLEGAL) {
        //show_data_error_nr(db,"cannot create an (extra)string of size ",strlen(extrastr)); 
        return 0;
      }          
      dbstore(db,offset+LONGSTR_EXTRASTR_POS*sizeof(gint),tmp);
      // increase extrastr refcount
      // if .... 
    } else {
      dbstore(db,offset+LONGSTR_EXTRASTR_POS*sizeof(gint),0); // no extrastr ptr
    }      
    // store metainfo: full obj len and str len difference, plus type  
    tmp=(getusedobjectsize(*((gint*)lstrptr))-length)<<LONGSTR_META_LENDIFSHFT; 
    tmp=tmp|type; // subtype of str stored in lowest byte of meta
    //printf("storing obj size %d, str len %d lengints %d lengints*4 %d lenrest %d lendiff %d metaptr %d meta %d \n",
    //  getusedobjectsize(*((gint*)lstrptr)),strlen(data),lengints,lengints*4,lenrest,
    //  (getusedobjectsize(*((gint*)lstrptr))-length),
    //  ((gint*)(offsettoptr(db,offset)))+LONGSTR_META_POS,
    //  tmp); 
    dbstore(db,offset+LONGSTR_META_POS*sizeof(gint),tmp); // type and str length diff
    dbstore(db,offset+LONGSTR_REFCOUNT_POS*sizeof(gint),0); // not pointed from anywhere yet
    dbstore(db,offset+LONGSTR_BACKLINKS_POS*sizeof(gint),0); // no backlinks yet
    // encode
    res=encode_longstr_offset(offset);
    // store to hash and update hashchain
    dbstore(db,((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash),res);
    //printf("hasharrel 2 %d \n",hasharrel); 
    dbstore(db,offset+LONGSTR_HASHCHAIN_POS*sizeof(gint),hasharrel); // store old hash array el
    // return result
    return res;        
  }
  
}  



char* wg_decode_unistr(void* db, gint data, gint type) {   
  gint* objptr;  
  char* dataptr;      
#ifdef USETINYSTR    
  if (type==WG_STRTYPE && istinystr(data)) {     
    if (LITTLEENDIAN) {
      dataptr=((char*)(&data))+1; // type bits stored in lowest addressed byte
    } else {
      dataptr=((char*)(&data));  // type bits stored in highest addressed byte
    }           
    return dataptr;
  }  
#endif  
  if (isshortstr(data)) {    
    dataptr=(char*)(offsettoptr(db,decode_shortstr_offset(data)));            
    return dataptr;    
  }      
  if (islongstr(data)) {      
    objptr=offsettoptr(db,decode_longstr_offset(data));      
    dataptr=((char*)(objptr))+(LONGSTR_HEADER_GINTS*sizeof(gint));        
    return dataptr;    
  } 
  show_data_error(db,"data given to wg_decode_unistr is not an encoded string"); 
  return NULL;
} 


char* wg_decode_unistr_lang(void* db, gint data, gint type) {   
  gint* objptr;  
  gint* fldptr; 
  gint fldval;
  char* res;
    
#ifdef USETINYSTR  
  if (type==WG_STRTYPE && istinystr(data)) {          
    return NULL;
  }  
#endif  
  if (type==WG_STRTYPE && isshortstr(data)) {                   
    return NULL;    
  }      
  if (islongstr(data)) {      
    objptr=offsettoptr(db,decode_longstr_offset(data));       
    fldptr=((gint*)objptr)+LONGSTR_EXTRASTR_POS;        
    fldval=*fldptr;
    if (fldval==0) return NULL;   
    res=wg_decode_unistr(db,fldval,type);  
    return res;    
  } 
  show_data_error(db,"data given to wg_decode_unistr_lang is not an encoded string"); 
  return NULL;
} 

/**
* return length of the main string, not including terminating 0
*
*
*/

gint wg_decode_unistr_len(void* db, gint data, gint type) { 
  char* dataptr;
  gint* objptr;  
  gint objsize;
  gint strsize;
    
#ifdef USETINYSTR  
  if (type==WG_STRTYPE && istinystr(data)) {              
    if (LITTLEENDIAN) {
      dataptr=((char*)(&data))+1; // type bits stored in lowest addressed byte
    } else {
      dataptr=((char*)(&data));  // type bits stored in highest addressed byte
    }      
    strsize=strlen(dataptr);
    return strsize;
  }  
#endif  
  if (isshortstr(data)) {  
    dataptr=(char*)(offsettoptr(db,decode_shortstr_offset(data)));       
    strsize=strlen(dataptr);
    return strsize; 
  }      
  if (islongstr(data)) {      
    objptr=offsettoptr(db,decode_longstr_offset(data));
    objsize=getusedobjectsize(*objptr);    
    dataptr=((char*)(objptr))+(LONGSTR_HEADER_GINTS*sizeof(gint));
    //printf("dataptr to read from %d str '%s' of len %d\n",dataptr,dataptr,strlen(dataptr));     
    strsize=objsize-(((*(objptr+LONGSTR_META_POS))&LONGSTR_META_LENDIFMASK)>>LONGSTR_META_LENDIFSHFT); 
    return strsize-1; 
  } 
  show_data_error(db,"data given to wg_decode_unistr_len is not an encoded string"); 
  return 0;
} 

/**
* copy string, return length of a copied string, not including terminating 0
*
* return -1 in case of error
*
*/

gint wg_decode_unistr_copy(void* db, gint data, char* strbuf, gint buflen, gint type) { 
  gint i;
  gint* objptr;  
  char* dataptr;
  gint objsize;
  gint strsize;
    
#ifdef USETINYSTR  
  if (type==WG_STRTYPE && istinystr(data)) {       
    if (LITTLEENDIAN) {
      dataptr=((char*)(&data))+1; // type bits stored in lowest addressed byte
    } else {
      dataptr=((char*)(&data));  // type bits stored in highest addressed byte
    }      
    strsize=strlen(dataptr)+1;
    if (strsize>=sizeof(gint)) {
      show_data_error_nr(db,"wrong data stored as tinystr, impossible length:",strsize); 
      return 0; 
    }  
    if (buflen<strsize) {
      show_data_error_nr(db,"insufficient buffer length given to wg_decode_unistr_copy:",buflen); 
      return 0; 
    }
    memcpy(strbuf,dataptr,strsize);     
    //printf("tinystr was read to strbuf '%s'\n",strbuf);     
    return strsize-1;
  }  
#endif  
  if (type==WG_STRTYPE && isshortstr(data)) {    
    dataptr=(char*)(offsettoptr(db,decode_shortstr_offset(data)));      
    for (i=1;i<SHORTSTR_SIZE && (*dataptr)!=0; i++,dataptr++,strbuf++) {
      if (i>=buflen) {
        show_data_error_nr(db,"insufficient buffer length given to wg_decode_unistr_copy:",buflen); 
        return -1; 
      }        
      *strbuf=*dataptr;     
    }      
    *strbuf=0;   
    return i-1;    
  }      
  if (islongstr(data)) {      
    objptr=offsettoptr(db,decode_longstr_offset(data));
    objsize=getusedobjectsize(*objptr);    
    dataptr=((char*)(objptr))+(LONGSTR_HEADER_GINTS*sizeof(gint));
    //printf("dataptr to read from %d str '%s' of len %d\n",dataptr,dataptr,strlen(dataptr));     
    strsize=objsize-(((*(objptr+LONGSTR_META_POS))&LONGSTR_META_LENDIFMASK)>>LONGSTR_META_LENDIFSHFT); 
    //printf("objsize %d metaptr %d meta %d lendiff %d strsize %d \n",
    //  objsize,((gint*)objptr+LONGSTR_META_POS),*((gint*)objptr+LONGSTR_META_POS),
    //  (((*(objptr+LONGSTR_META_POS))&LONGSTR_META_LENDIFMASK)>>LONGSTR_META_LENDIFSHFT),strsize);
    memcpy(strbuf,dataptr,strsize);
    //*(dataptr+strsize)=0;
    //printf("copied str %s with strsize %d\n",strbuf,strlen(strbuf));    
    if (type==WG_BLOBTYPE) return strsize;
    else return strsize-1;    
  } 
  show_data_error(db,"data given to wg_decode_unistr_copy is not an encoded string"); 
  return -1;
} 

/**
* return length of the lang string, not including terminating 0
*
*
*/

gint wg_decode_unistr_lang_len(void* db, gint data, gint type) { 
  char* langptr;  
  gint len;
  
  langptr=wg_decode_unistr_lang(db,data,type);
  if (langptr==NULL) {    
    return 0;
  }  
  len=strlen(langptr);
  return len;
}  
  

/**
* copy lang string, return length of a copied string, not including terminating 0
* in case of NULL lang write a single 0 to beginning of buffer and return 0
* 
* return -1 in case of error
*
*/

gint wg_decode_unistr_lang_copy(void* db, gint data, char* strbuf, gint buflen, gint type) { 
  char* langptr;  
  gint len;
  
  langptr=wg_decode_unistr_lang(db,data,type); 
  if (langptr==NULL) {
    *strbuf=0;
    return 0;
  }  
  len=strlen(langptr);  
  if (len>=buflen) {
    show_data_error_nr(db,"insufficient buffer length given to wg_decode_unistr_lang_copy:",buflen); 
    return -1; 
  } 
  memcpy(strbuf,langptr,len+1);  
  return len;
}

/* Var type encoding is very similar to small int */

gint wg_encode_var(void* db, gint data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_encode_var");
    return WG_ILLEGAL;
  }
#endif
  /* bits available for var are always less than full gint length */
  if(!fits_var(data)) {
    show_data_error(db,"variable identifier too large");
    return WG_ILLEGAL;
  }
  return (gint)(encode_var(data));
}
  

gint wg_decode_var(void* db, gint data) {
#ifdef CHECK
  if (!dbcheck(db)) {
    show_data_error(db,"wrong database pointer given to wg_decode_var");
    return 0;
  }
#endif
  return (gint)(decode_var(data));
}




/* ----------- calendar and time functions ------------------- */

/*

Scalar date routines used are written and given to public domain by Ray Gardner.

*/

static int isleap(unsigned yr) {
  return yr % 400 == 0 || (yr % 4 == 0 && yr % 100 != 0);
}

static unsigned months_to_days (unsigned month) {
  return (month * 3057 - 3007) / 100;
}

static long years_to_days (unsigned yr) {
  return yr * 365L + yr / 4 - yr / 100 + yr / 400;
}

static long ymd_to_scalar (unsigned yr, unsigned mo, unsigned day) {
  long scalar;
  scalar = day + months_to_days(mo);
  if ( mo > 2 )                         /* adjust if past February */
      scalar -= isleap(yr) ? 1 : 2;
  yr--;
  scalar += years_to_days(yr);
  return scalar;
}

static void scalar_to_ymd (long scalar, unsigned *yr, unsigned *mo, unsigned *day) {
  unsigned n;                /* compute inverse of years_to_days() */

  for ( n = (unsigned)((scalar * 400L) / 146097L); years_to_days(n) < scalar;) n++; /* 146097 == years_to_days(400) */
  *yr = n;
  n = (unsigned)(scalar - years_to_days(n-1));
  if ( n > 59 ) {                       /* adjust if past February */
    n += 2;
    if (isleap(*yr))  n -= n > 62 ? 1 : 2;
  }
  *mo = (n * 100 + 3007) / 3057;    /* inverse of months_to_days() */
  *day = n - months_to_days(*mo);
}

/*

Thread-safe localtime_r appears not to be present on windows: emulate using win localtime_s, which is thread-safe

*/

#ifdef _WIN32
static struct tm * localtime_r (const time_t *timer, struct tm *result) {    
   struct tm local_result;
   int res; 
  
   res = localtime_s (&local_result,timer);
   if (!res) return NULL;
   //if (local_result == NULL || result == NULL) return NULL;
   memcpy (result, &local_result, sizeof (result));
   return result;
}
#endif


/* ------ value offset translation ---- */

#ifdef USE_CHILD_DB

/* Translate encoded value in relation to child base address
 *
 * parent is the offset of the parent database in relation to
 * the child database. Encoded value is the value "native" to
 * the database. Returned value is translated so that it can
 * be used in Wgandalf API functions with the child database.
 */
gint wg_encode_parent_data(gint parent, gint encoded) {
  /* Only pointer-type values need translating */
  if(isptr(encoded)) {
    switch(encoded&NORMALPTRMASK) {
      case DATARECBITS:
        return encode_datarec_offset(
          decode_datarec_offset(encoded) + parent);
      case LONGSTRBITS:
        return encode_longstr_offset(
          decode_longstr_offset(encoded) + parent);
      case SHORTSTRBITS:
        return encode_shortstr_offset(
          decode_shortstr_offset(encoded) + parent);
      case FULLDOUBLEBITS:
        return encode_fulldouble_offset(
          decode_fulldouble_offset(encoded) + parent);
      case FULLINTBITSV0:
      case FULLINTBITSV1:
        return encode_fullint_offset(
          decode_fullint_offset(encoded) + parent);
      default:
        /* XXX: it's not entirely correct to fail silently here, but
         * we can only end up here if new pointer types are added without
         * updating this function.
         */
        break;
    }
  }
  return encoded;
}

/* Return base address that a offset is "native" to.
 *
 * Mostly this applies to child databases. Current implementation
 * works so that if the offset is not local to db, it's assumed
 * to belong to the parent database of db.
 */
static void *get_offset_owner(void *db, gint offset) {
  if(offset > 0 && offset < ((db_memsegment_header *) db)->size) {
      return db;  /* "Local" record */
  }
  return (void *) ((char *) db + ((db_memsegment_header *) db)->parent);
}

/** Calculate the offset between the current base address and
 *  the base address that a record belongs to.
 *
 *  Takes pointer values as arguments.
 *  Returns 0 if the record belongs to memory area owned
 *  by db pointer (possibly a child database). Returns an offset
 *  between the base memory area pointers if the record is an
 *  external reference.
 */
gint wg_get_rec_base_offset(void *db, void *rec) {
  if((int) rec > (int) db) {
    void *eodb = (void *) ((char *) db)+((db_memsegment_header *) db)->size;
    if((int) rec < (int) eodb)
      return 0;  /* "Local" record */
  }
  return ((db_memsegment_header *) db)->parent;
}

#endif

/* ------------ errors ---------------- */


static gint show_data_error(void* db, char* errmsg) {
  printf("wg data handling error: %s\n",errmsg);
  return -1;
}

static gint show_data_error_nr(void* db, char* errmsg, gint nr) {
  printf("wg data handling error: %s %d\n",errmsg,nr);
  return -1;
}

static gint show_data_error_double(void* db, char* errmsg, double nr) {
  printf("wg data handling error: %s %f\n",errmsg,nr);
  return -1;
}

static gint show_data_error_str(void* db, char* errmsg, char* str) {
  printf("wg data handling error: %s %s\n",errmsg,str);
  return -1;
}



