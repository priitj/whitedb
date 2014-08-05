/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
* Copyright (c) Priit Järv 2013
*
* Contact: tanel.tammet@gmail.com
*
* This file is part of WhiteDB
*
* WhiteDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* WhiteDB is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with WhiteDB.  If not, see <http://www.gnu.org/licenses/>.
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

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbfeatures.h"
#include "dblock.h"
#include "dbindex.h"

/* don't output 'segment does not have enough space' messages */
#define SUPPRESS_LOWLEVEL_ERR 1

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */

static gint init_db_subarea(void* db, void* area_header, gint index, gint size);
static gint alloc_db_segmentchunk(void* db, gint size); // allocates a next chunk from db memory segment
static gint init_syn_vars(void* db);
static gint init_extdb(void* db);
static gint init_db_index_area_header(void* db);
static gint init_logging(void* db);
static gint init_strhash_area(void* db, db_hash_area_header* areah);
static gint init_hash_subarea(void* db, db_hash_area_header* areah, gint arraylength);
static gint init_db_recptr_bitmap(void* db);
#ifdef USE_REASONER
static gint init_anonconst_table(void* db);
static gint intern_anonconst(void* db, char* str, gint enr);
#endif

static gint make_subarea_freelist(void* db, void* area_header, gint arrayindex);
static gint init_area_buckets(void* db, void* area_header);
static gint init_subarea_freespace(void* db, void* area_header, gint arrayindex);

static gint extend_fixedlen_area(void* db, void* area_header);

static gint split_free(void* db, void* area_header, gint nr, gint* freebuckets, gint i);
static gint extend_varlen_area(void* db, void* area_header, gint minbytes);

static gint show_dballoc_error_nr(void* db, char* errmsg, gint nr);
static gint show_dballoc_error(void* db, char* errmsg);


/* ====== Functions ============== */


/* -------- segment header initialisation ---------- */

/** starts and completes memsegment initialisation
*
* should be called after new memsegment is allocated
*/

gint wg_init_db_memsegment(void* db, gint key, gint size) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint tmp;
  gint free;
  gint i;

  // set main global values for db
  dbh->mark=(gint32) MEMSEGMENT_MAGIC_INIT;
  dbh->version=(gint32) MEMSEGMENT_VERSION;
  dbh->features=(gint32) MEMSEGMENT_FEATURES;
  dbh->checksum=0;
  dbh->size=size;
  dbh->initialadr=(gint)dbh; /* XXX: this assumes pointer size. Currently harmless
                             * because initialadr isn't used much. */
  dbh->key=key;  /* might be 0 if local memory used */

#ifdef CHECK
  if(((gint) dbh)%SUBAREA_ALIGNMENT_BYTES)
    show_dballoc_error(dbh,"db base pointer has bad alignment (ignoring)");
#endif

  // set correct alignment for free
  free=sizeof(db_memsegment_header);
  // set correct alignment for free
  i=SUBAREA_ALIGNMENT_BYTES-(free%SUBAREA_ALIGNMENT_BYTES);
  if (i==SUBAREA_ALIGNMENT_BYTES) i=0;
  dbh->free=free+i;

  // allocate and initialise subareas

  //datarec
  tmp=init_db_subarea(db,&(dbh->datarec_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create datarec area"); return -1; }
  (dbh->datarec_area_header).fixedlength=0;
  tmp=init_area_buckets(db,&(dbh->datarec_area_header)); // fill buckets with 0-s
  if (tmp) {  show_dballoc_error(db," cannot initialize datarec area buckets"); return -1; }
  tmp=init_subarea_freespace(db,&(dbh->datarec_area_header),0); // mark and store free space in subarea 0
  if (tmp) {  show_dballoc_error(db," cannot initialize datarec subarea 0"); return -1; }
  //longstr
  tmp=init_db_subarea(db,&(dbh->longstr_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create longstr area"); return -1; }
  (dbh->longstr_area_header).fixedlength=0;
  tmp=init_area_buckets(db,&(dbh->longstr_area_header)); // fill buckets with 0-s
  if (tmp) {  show_dballoc_error(db," cannot initialize longstr area buckets"); return -1; }
  tmp=init_subarea_freespace(db,&(dbh->longstr_area_header),0); // mark and store free space in subarea 0
  if (tmp) {  show_dballoc_error(db," cannot initialize longstr subarea 0"); return -1; }
  //listcell
  tmp=init_db_subarea(db,&(dbh->listcell_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create listcell area"); return -1; }
  (dbh->listcell_area_header).fixedlength=1;
  (dbh->listcell_area_header).objlength=sizeof(gcell);
  tmp=make_subarea_freelist(db,&(dbh->listcell_area_header),0); // freelist into subarray 0
  if (tmp) {  show_dballoc_error(db," cannot initialize listcell area"); return -1; }
  //shortstr
  tmp=init_db_subarea(db,&(dbh->shortstr_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create short string area"); return -1; }
  (dbh->shortstr_area_header).fixedlength=1;
  (dbh->shortstr_area_header).objlength=SHORTSTR_SIZE;
  tmp=make_subarea_freelist(db,&(dbh->shortstr_area_header),0); // freelist into subarray 0
  if (tmp) {  show_dballoc_error(db," cannot initialize shortstr area"); return -1; }
  //word
  tmp=init_db_subarea(db,&(dbh->word_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create word area"); return -1; }
  (dbh->word_area_header).fixedlength=1;
  (dbh->word_area_header).objlength=sizeof(gint);
  tmp=make_subarea_freelist(db,&(dbh->word_area_header),0); // freelist into subarray 0
  if (tmp) {  show_dballoc_error(db," cannot initialize word area"); return -1; }
  //doubleword
  tmp=init_db_subarea(db,&(dbh->doubleword_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create doubleword area"); return -1; }
  (dbh->doubleword_area_header).fixedlength=1;
  (dbh->doubleword_area_header).objlength=2*sizeof(gint);
  tmp=make_subarea_freelist(db,&(dbh->doubleword_area_header),0); // freelist into subarray 0
  if (tmp) {  show_dballoc_error(db," cannot initialize doubleword area"); return -1; }

  /* index structures also user fixlen object storage:
   *   tnode area - contains index nodes
   *   index header area - contains index headers
   *   index template area - contains template headers
   *   index hash area - varlen storage for hash buckets
   * index lookup data takes up relatively little space so we allocate
   * the smallest chunk allowed for the headers.
   */
  tmp=init_db_subarea(db,&(dbh->tnode_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create tnode area"); return -1; }
  (dbh->tnode_area_header).fixedlength=1;
  (dbh->tnode_area_header).objlength=sizeof(struct wg_tnode);
  tmp=make_subarea_freelist(db,&(dbh->tnode_area_header),0);
  if (tmp) {  show_dballoc_error(db," cannot initialize tnode area"); return -1; }

  tmp=init_db_subarea(db,&(dbh->indexhdr_area_header),0,MINIMAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create index header area"); return -1; }
  (dbh->indexhdr_area_header).fixedlength=1;
  (dbh->indexhdr_area_header).objlength=sizeof(wg_index_header);
  tmp=make_subarea_freelist(db,&(dbh->indexhdr_area_header),0);
  if (tmp) {  show_dballoc_error(db," cannot initialize index header area"); return -1; }

#ifdef USE_INDEX_TEMPLATE
  tmp=init_db_subarea(db,&(dbh->indextmpl_area_header),0,MINIMAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create index header area"); return -1; }
  (dbh->indextmpl_area_header).fixedlength=1;
  (dbh->indextmpl_area_header).objlength=sizeof(wg_index_template);
  tmp=make_subarea_freelist(db,&(dbh->indextmpl_area_header),0);
  if (tmp) {  show_dballoc_error(db," cannot initialize index header area"); return -1; }
#endif

  tmp=init_db_subarea(db,&(dbh->indexhash_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create indexhash area"); return -1; }
  (dbh->indexhash_area_header).fixedlength=0;
  tmp=init_area_buckets(db,&(dbh->indexhash_area_header)); // fill buckets with 0-s
  if (tmp) {  show_dballoc_error(db," cannot initialize indexhash area buckets"); return -1; }
  tmp=init_subarea_freespace(db,&(dbh->indexhash_area_header),0);
  if (tmp) {  show_dballoc_error(db," cannot initialize indexhash subarea 0"); return -1; }

  /* initialize other structures */

  /* initialize strhash array area */
  tmp=init_strhash_area(db,&(dbh->strhash_area_header));
  if (tmp) {  show_dballoc_error(db," cannot create strhash array area"); return -1; }


  /* initialize synchronization */
  tmp=init_syn_vars(db);
  if (tmp) { show_dballoc_error(db," cannot initialize synchronization area"); return -1; }

  /* initialize external database register */
  tmp=init_extdb(db);
  if (tmp) { show_dballoc_error(db," cannot initialize external db register"); return -1; }

  /* initialize index structures */
  tmp=init_db_index_area_header(db);
  if (tmp) { show_dballoc_error(db," cannot initialize index header area"); return -1; }

  /* initialize bitmap for record pointers: really allocated only if USE_RECPTR_BITMAP defined */
  tmp=init_db_recptr_bitmap(db);
  if (tmp) { show_dballoc_error(db," cannot initialize record pointer bitmap"); return -1; }

#ifdef USE_REASONER
  /* initialize anonconst table */
  tmp=init_anonconst_table(db);
  if (tmp) { show_dballoc_error(db," cannot initialize anonconst table"); return -1; }
#endif

  /* initialize logging structures */


  tmp=init_logging(db);
 /* tmp=init_db_subarea(db,&(dbh->logging_area_header),0,INITIAL_SUBAREA_SIZE);
  if (tmp) {  show_dballoc_error(db," cannot create logging area"); return -1; }
  (dbh->logging_area_header).fixedlength=0;
  tmp=init_area_buckets(db,&(dbh->logging_area_header)); // fill buckets with 0-s
  if (tmp) {  show_dballoc_error(db," cannot initialize logging area buckets"); return -1; }*/


  /* Database is initialized, mark it as valid */
  dbh->mark=(gint32) MEMSEGMENT_MAGIC_MARK;
  return 0;
}




/** initializes a subarea. subarea is used for actual data obs allocation
*
* returns 0 if ok, negative otherwise;
*
* called - several times - first by wg_init_db_memsegment, then as old subareas
* get filled up
*/

static gint init_db_subarea(void* db, void* area_header, gint index, gint size) {
  db_area_header* areah;
  gint segmentchunk;
  gint i;
  gint asize;

  //printf("init_db_subarea called with size %d \n",size);
  if (size<MINIMAL_SUBAREA_SIZE) return -1; // errcase
  segmentchunk=alloc_db_segmentchunk(db,size);
  if (!segmentchunk) return -2; // errcase
  areah=(db_area_header*)area_header;
  ((areah->subarea_array)[index]).size=size;
  ((areah->subarea_array)[index]).offset=segmentchunk;
  // set correct alignment for alignedoffset
  i=SUBAREA_ALIGNMENT_BYTES-(segmentchunk%SUBAREA_ALIGNMENT_BYTES);
  if (i==SUBAREA_ALIGNMENT_BYTES) i=0;
  ((areah->subarea_array)[index]).alignedoffset=segmentchunk+i;
  // set correct alignment for alignedsize
  asize=(size-i);
  i=asize-(asize%MIN_VARLENOBJ_SIZE);
  ((areah->subarea_array)[index]).alignedsize=i;
  // set last index and freelist
  areah->last_subarea_index=index;
  areah->freelist=0;
  return 0;
}

/** allocates a new segment chunk from the segment
*
* returns offset if successful, 0 if no more space available
* if 0 returned, no allocation performed: can try with a smaller value
* used for allocating all subareas
*
* Alignment is guaranteed to SUBAREA_ALIGNMENT_BYTES
*/

static gint alloc_db_segmentchunk(void* db, gint size) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint lastfree;
  gint nextfree;
  gint i;

  lastfree=dbh->free;
  nextfree=lastfree+size;
  if (nextfree<0) {
    show_dballoc_error_nr(db,"trying to allocate next segment exceeds positive int limit",size);
    return 0;
  }
  // set correct alignment for nextfree
  i=SUBAREA_ALIGNMENT_BYTES-(nextfree%SUBAREA_ALIGNMENT_BYTES);
  if (i==SUBAREA_ALIGNMENT_BYTES) i=0;
  nextfree=nextfree+i;
  if (nextfree>=(dbh->size)) {
#ifndef SUPPRESS_LOWLEVEL_ERR
    show_dballoc_error_nr(db,"segment does not have enough space for the required chunk of size",size);
#endif
    return 0;
  }
  dbh->free=nextfree;
  return lastfree;
}

/** initializes sync variable storage
*
* returns 0 if ok, negative otherwise;
* Note that a basic spinlock area is initialized even if locking
* is disabled, this is done for better memory image compatibility.
*/

static gint init_syn_vars(void* db) {

  db_memsegment_header* dbh = dbmemsegh(db);
  gint i;

#if !defined(LOCK_PROTO) || (LOCK_PROTO < 3) /* rpspin, wpspin */
  /* calculate aligned pointer */
  i = ((gint) (dbh->locks._storage) + SYN_VAR_PADDING - 1) & -SYN_VAR_PADDING;
  dbh->locks.global_lock = dbaddr(db, (void *) i);
  dbh->locks.writers = dbaddr(db, (void *) (i + SYN_VAR_PADDING));
#else
  i = alloc_db_segmentchunk(db, SYN_VAR_PADDING * (MAX_LOCKS+2));
  if(!i) return -1;
  /* re-align (SYN_VAR_PADDING <> SUBAREA_ALIGNMENT_BYTES) */
  i = (i + SYN_VAR_PADDING - 1) & -SYN_VAR_PADDING;
  dbh->locks.queue_lock = i;
  dbh->locks.storage = i + SYN_VAR_PADDING;
  dbh->locks.max_nodes = MAX_LOCKS;
  dbh->locks.freelist = dbh->locks.storage; /* dummy, wg_init_locks()
                                                will overwrite this */
#endif

  /* allocating space was successful, set the initial state */
  return wg_init_locks(db);
}

/** initializes external database register
*
* returns 0 if ok, negative otherwise;
*/

static gint init_extdb(void* db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  int i;

  dbh->extdbs.count = 0;
  for(i=0; i<MAX_EXTDB; i++) {
    dbh->extdbs.offset[i] = 0;
    dbh->extdbs.size[i] = 0;
  }
  return 0;
}

/** initializes main index area
* Currently this function only sets up an empty index table. The rest
* of the index storage is initialized by wg_init_db_memsegment().
* returns 0 if ok
*/
static gint init_db_index_area_header(void* db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  dbh->index_control_area_header.number_of_indexes=0;
  memset(dbh->index_control_area_header.index_table, 0,
    (MAX_INDEXED_FIELDNR+1)*sizeof(gint));
  dbh->index_control_area_header.index_list=0;
#ifdef USE_INDEX_TEMPLATE
  dbh->index_control_area_header.index_template_list=0;
  memset(dbh->index_control_area_header.index_template_table, 0,
    (MAX_INDEXED_FIELDNR+1)*sizeof(gint));
#endif
  return 0;
}

/** initializes logging area
*
*/
static gint init_logging(void* db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  dbh->logging.active = 0;
  dbh->logging.dirty = 0;
  dbh->logging.serial = 1; /* non-zero, so that zero value in db handle
                            * indicates uninitialized state. */
  return 0;
}

/** initializes strhash area
*
*/
static gint init_strhash_area(void* db, db_hash_area_header* areah) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint arraylength;

  if(STRHASH_SIZE > 0.01 && STRHASH_SIZE < 50) {
    arraylength = (gint) ((dbh->size+1) * (STRHASH_SIZE/100.0)) / sizeof(gint);
  } else {
    arraylength = DEFAULT_STRHASH_LENGTH;
  }
  return init_hash_subarea(db, areah, arraylength);
}

/** initializes hash area
*
*/
static gint init_hash_subarea(void* db, db_hash_area_header* areah, gint arraylength) {
  gint segmentchunk;
  gint i;
  gint asize;
  gint j;

  //printf("init_hash_subarea called with arraylength %d \n",arraylength);
  asize=((arraylength+1)*sizeof(gint))+(2*SUBAREA_ALIGNMENT_BYTES); // 2* just to be safe
  //printf("asize: %d \n",asize);
  //if (asize<100) return -1; // errcase to filter out stupid requests
  segmentchunk=alloc_db_segmentchunk(db,asize);
  //printf("segmentchunk: %d \n",segmentchunk);
  if (!segmentchunk) return -2; // errcase
  areah->offset=segmentchunk;
  areah->size=asize;
  areah->arraylength=arraylength;
  // set correct alignment for arraystart
  i=SUBAREA_ALIGNMENT_BYTES-(segmentchunk%SUBAREA_ALIGNMENT_BYTES);
  if (i==SUBAREA_ALIGNMENT_BYTES) i=0;
  areah->arraystart=segmentchunk+i;
  i=areah->arraystart;
  for(j=0;j<arraylength;j++) dbstore(db,i+(j*sizeof(gint)),0);
  //show_strhash(db);
  return 0;
}

static gint init_db_recptr_bitmap(void* db) {    
  db_memsegment_header* dbh = dbmemsegh(db);

#ifdef USE_RECPTR_BITMAP
  gint segmentchunk;
  gint asize;
  
  // recs minimal alignment 8 bytes, multiply by 8 bits in byte = 64
  asize=((dbh->size)/64)+16; 
  segmentchunk=alloc_db_segmentchunk(db,asize);
  if (!segmentchunk) return -2; // errcase
  dbh->recptr_bitmap.offset=segmentchunk;
  dbh->recptr_bitmap.size=asize;
  memset(offsettoptr(db,segmentchunk),0,asize);
  return 0;
#else  
  dbh->recptr_bitmap.offset=0;
  dbh->recptr_bitmap.size=0;  
  return 0;
#endif     
}


#ifdef USE_REASONER

/** initializes anonymous constants (special uris with attached funs)
*
*/
static gint init_anonconst_table(void* db) {
  int i;
  db_memsegment_header* dbh = dbmemsegh(db);

  dbh->anonconst.anonconst_nr=0;
  dbh->anonconst.anonconst_funs=0;
  // clearing is not really necessary
  for(i=0;i<ANONCONST_TABLE_SIZE;i++) {
    (dbh->anonconst.anonconst_table)[i]=0;
  }

  if (intern_anonconst(db,ACONST_TRUE_STR,ACONST_TRUE)) return 1;
  if (intern_anonconst(db,ACONST_FALSE_STR,ACONST_FALSE)) return 1;
  if (intern_anonconst(db,ACONST_IF_STR,ACONST_IF)) return 1;

  if (intern_anonconst(db,ACONST_NOT_STR,ACONST_NOT)) return 1;
  if (intern_anonconst(db,ACONST_AND_STR,ACONST_AND)) return 1;
  if (intern_anonconst(db,ACONST_OR_STR,ACONST_OR)) return 1;
  if (intern_anonconst(db,ACONST_IMPLIES_STR,ACONST_IMPLIES)) return 1;
  if (intern_anonconst(db,ACONST_XOR_STR,ACONST_XOR)) return 1;

  if (intern_anonconst(db,ACONST_LESS_STR,ACONST_LESS)) return 1;
  if (intern_anonconst(db,ACONST_EQUAL_STR,ACONST_EQUAL)) return 1;
  if (intern_anonconst(db,ACONST_GREATER_STR,ACONST_GREATER)) return 1;
  if (intern_anonconst(db,ACONST_LESSOREQUAL_STR,ACONST_LESSOREQUAL)) return 1;
  if (intern_anonconst(db,ACONST_GREATEROREQUAL_STR,ACONST_GREATEROREQUAL)) return 1;
  if (intern_anonconst(db,ACONST_ISZERO_STR,ACONST_ISZERO)) return 1;

  if (intern_anonconst(db,ACONST_ISEMPTYSTR_STR,ACONST_ISEMPTYSTR)) return 1;

  if (intern_anonconst(db,ACONST_PLUS_STR,ACONST_PLUS)) return 1;
  if (intern_anonconst(db,ACONST_MINUS_STR,ACONST_MINUS)) return 1;
  if (intern_anonconst(db,ACONST_MULTIPLY_STR,ACONST_MULTIPLY)) return 1;
  if (intern_anonconst(db,ACONST_DIVIDE_STR,ACONST_DIVIDE)) return 1;

  if (intern_anonconst(db,ACONST_STRCONTAINS_STR,ACONST_STRCONTAINS)) return 1;
  if (intern_anonconst(db,ACONST_STRCONTAINSICASE_STR,ACONST_STRCONTAINSICASE)) return 1;
  if (intern_anonconst(db,ACONST_SUBSTR_STR,ACONST_SUBSTR)) return 1;
  if (intern_anonconst(db,ACONST_STRLEN_STR,ACONST_STRLEN)) return 1;

  ++(dbh->anonconst.anonconst_nr); // max used slot + 1
  dbh->anonconst.anonconst_funs=dbh->anonconst.anonconst_nr;
  return 0;
}

/** internalizes new anonymous constants: used in init
*
*/
static gint intern_anonconst(void* db, char* str, gint enr) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint nr;
  gint uri;

  nr=decode_anonconst(enr);
  if (nr<0 || nr>=ANONCONST_TABLE_SIZE) {
    show_dballoc_error_nr(db,"inside intern_anonconst: nr given out of range: ", nr);
    return 1;
  }
  uri=wg_encode_unistr(db,str,NULL,WG_URITYPE);
  if (uri==WG_ILLEGAL) {
    show_dballoc_error_nr(db,"inside intern_anonconst: cannot create an uri of size ",strlen(str));
    return 1;
  }
  (dbh->anonconst.anonconst_table)[nr]=uri;
  if (dbh->anonconst.anonconst_nr<nr) (dbh->anonconst.anonconst_nr)=nr;
  return 0;
}

#endif

/* -------- freelists creation  ---------- */

/** create freelist for an area
*
* used for initialising (sub)areas used for fixed-size allocation
*
* returns 0 if ok
*
* speed stats:
*
* 10000 * creation of   100000 elems (1 000 000 000 or 1G ops) takes 1.2 sec on penryn
* 1000 * creation of  1000000 elems (1 000 000 000 or 1G ops) takes 3.4 sec on penryn
*
*/

static gint make_subarea_freelist(void* db, void* area_header, gint arrayindex) {
  db_area_header* areah;
  gint objlength;
  gint max;
  gint size;
  gint offset;
  gint i;

  // general area info
  areah=(db_area_header*)area_header;
  objlength=areah->objlength;

  //subarea info
  size=((areah->subarea_array)[arrayindex]).alignedsize;
  offset=((areah->subarea_array)[arrayindex]).alignedoffset;
  // create freelist
  max=(offset+size)-(2*objlength);
  for(i=offset;i<=max;i=i+objlength) {
    dbstore(db,i,i+objlength);
  }
  dbstore(db,i,0);
  (areah->freelist)=offset; //
  //printf("(areah->freelist) %d \n",(areah->freelist));
  return 0;
}




/* -------- buckets creation  ---------- */

/** fill bucket data for an area
*
* used for initialising areas used for variable-size allocation
*
* returns 0 if ok, not 0 if error
*
*/

gint init_area_buckets(void* db, void* area_header) {
  db_area_header* areah;
  gint* freebuckets;
  gint i;

  // general area info
  areah=(db_area_header*)area_header;
  freebuckets=areah->freebuckets;

  // empty all buckets
  for(i=0;i<EXACTBUCKETS_NR+VARBUCKETS_NR+CACHEBUCKETS_NR;i++) {
    freebuckets[i]=0;
  }
  return 0;
}

/** mark up beginning and end for a subarea, set free area as a new victim
*
* used for initialising new subareas used for variable-size allocation
*
* returns 0 if ok, not 0 if error
*
*/

gint init_subarea_freespace(void* db, void* area_header, gint arrayindex) {
  db_area_header* areah;
  gint* freebuckets;
  gint size;
  gint offset;
  gint dv;
  gint dvindex;
  gint dvsize;
  gint freelist;
  gint endmarkobj;
  gint freeoffset;
  gint freesize;
  //gint i;

  // general area info
  areah=(db_area_header*)area_header;
  freebuckets=areah->freebuckets;

  //subarea info
  size=((areah->subarea_array)[arrayindex]).alignedsize;
  offset=((areah->subarea_array)[arrayindex]).alignedoffset;

  // if the previous area exists, store current victim to freelist
  if (arrayindex>0) {
    dv=freebuckets[DVBUCKET];
    dvsize=freebuckets[DVSIZEBUCKET];
    if (dv!=0 && dvsize>=MIN_VARLENOBJ_SIZE) {
      dbstore(db,dv,makefreeobjectsize(dvsize)); // store new size with freebit to the second half of object
      dbstore(db,dv+dvsize-sizeof(gint),makefreeobjectsize(dvsize));
      dvindex=wg_freebuckets_index(db,dvsize);
      freelist=freebuckets[dvindex];
      if (freelist!=0) dbstore(db,freelist+2*sizeof(gint),dv); // update prev ptr
      dbstore(db,dv+sizeof(gint),freelist); // store previous freelist
      dbstore(db,dv+2*sizeof(gint),dbaddr(db,&freebuckets[dvindex])); // store ptr to previous
      freebuckets[dvindex]=dv; // store offset to correct bucket
      //printf("in init_subarea_freespace: \n PUSHED DV WITH SIZE %d TO FREELIST TO BUCKET %d:\n",
      //        dvsize,dvindex);
      //show_bucket_freeobjects(db,freebuckets[dvindex]);
    }
  }
  // create two minimal in-use objects never to be freed: marking beginning
  // and end of free area via in-use bits in size
  // beginning of free area
  dbstore(db,offset,makespecialusedobjectsize(MIN_VARLENOBJ_SIZE)); // lowest bit 0 means in use
  dbstore(db,offset+sizeof(gint),SPECIALGINT1START); // next ptr
  dbstore(db,offset+2*sizeof(gint),0); // prev ptr
  dbstore(db,offset+MIN_VARLENOBJ_SIZE-sizeof(gint),MIN_VARLENOBJ_SIZE); // len to end as well
  // end of free area
  endmarkobj=offset+size-MIN_VARLENOBJ_SIZE;
  dbstore(db,endmarkobj,makespecialusedobjectsize(MIN_VARLENOBJ_SIZE)); // lowest bit 0 means in use
  dbstore(db,endmarkobj+sizeof(gint),SPECIALGINT1END); // next ptr
  dbstore(db,endmarkobj+2*sizeof(gint),0); // prev ptr
  dbstore(db,endmarkobj+MIN_VARLENOBJ_SIZE-sizeof(gint),MIN_VARLENOBJ_SIZE); // len to end as well
  // calc where real free area starts and what is the size
  freeoffset=offset+MIN_VARLENOBJ_SIZE;
  freesize=size-2*MIN_VARLENOBJ_SIZE;
  // put whole free area into one free object
  // store the single free object as a designated victim
  dbstore(db,freeoffset,makespecialusedobjectsize(freesize)); // length without free bits: victim not marked free
  dbstore(db,freeoffset+sizeof(gint),SPECIALGINT1DV); // marks that it is a dv kind of special object
  freebuckets[DVBUCKET]=freeoffset;
  freebuckets[DVSIZEBUCKET]=freesize;
  // alternative: store the single free object to correct bucket
  /*
  dbstore(db,freeoffset,setcfree(freesize)); // size with free bits stored to beginning of object
  dbstore(db,freeoffset+sizeof(gint),0); // empty ptr to remaining obs stored after size
  i=freebuckets_index(db,freesize);
  if (i<0) {
    show_dballoc_error_nr(db,"initialising free object failed for ob size ",freesize);
    return -1;
  }
  dbstore(db,freeoffset+2*sizeof(gint),dbaddr(db,&freebuckets[i])); // ptr to previous stored
  freebuckets[i]=freeoffset;
  */
  return 0;
}



/* -------- fixed length object allocation and freeing ---------- */


/** allocate a new fixed-len object
*
* return offset if ok, 0 if allocation fails
*/

gint wg_alloc_fixlen_object(void* db, void* area_header) {
  db_area_header* areah;
  gint freelist;

  areah=(db_area_header*)area_header;
  freelist=areah->freelist;
  if (!freelist) {
    if(!extend_fixedlen_area(db,areah)) {
      show_dballoc_error_nr(db,"cannot extend fixed length object area for size ",areah->objlength);
      return 0;
    }
    freelist=areah->freelist;
    if (!freelist) {
      show_dballoc_error_nr(db,"no free fixed length objects available for size ",areah->objlength);
      return 0;
    } else {
      areah->freelist=dbfetch(db,freelist);
      return freelist;
    }
  } else {
    areah->freelist=dbfetch(db,freelist);
    return freelist;
  }
}

/** create and initialise a new subarea for fixed-len obs area
*
* returns allocated size if ok, 0 if failure
* used when the area has no more free space
*
*/

static gint extend_fixedlen_area(void* db, void* area_header) {
  gint i;
  gint tmp;
  gint size, newsize;
  db_area_header* areah;

  areah=(db_area_header*)area_header;
  i=areah->last_subarea_index;
  if (i+1>=SUBAREA_ARRAY_SIZE) {
    show_dballoc_error_nr(db,
      " no more subarea array elements available for fixedlen of size: ",areah->objlength);
    return 0; // no more subarea array elements available
  }
  size=((areah->subarea_array)[i]).size; // last allocated subarea size
  // make tmp power-of-two times larger
  newsize=size<<1;
  //printf("fixlen OLD SUBAREA SIZE WAS %d NEW SUBAREA SIZE SHOULD BE %d\n",size,newsize);

  while(newsize >= MINIMAL_SUBAREA_SIZE) {
    if(!init_db_subarea(db,areah,i+1,newsize)) {
      goto done;
    }
    /* fall back to smaller size */
    newsize>>=1;
    //printf("REQUIRED SPACE FAILED, TRYING %d\n",newsize);
  }
  show_dballoc_error_nr(db," cannot extend datarec area with a new subarea of size: ",newsize<<1);
  return 0;
done:
  // here we have successfully allocated a new subarea
  tmp=make_subarea_freelist(db,areah,i+1);  // fill with a freelist, store ptrs
  if (tmp) {  show_dballoc_error(db," cannot initialize new subarea"); return 0; }
  return newsize;
}



/** free an existing listcell
*
* the object is added to the freelist
*
*/

void wg_free_listcell(void* db, gint offset) {
  dbstore(db,offset,(dbmemsegh(db)->listcell_area_header).freelist);
  (dbmemsegh(db)->listcell_area_header).freelist=offset;
}


/** free an existing shortstr object
*
* the object is added to the freelist
*
*/

void wg_free_shortstr(void* db, gint offset) {
  dbstore(db,offset,(dbmemsegh(db)->shortstr_area_header).freelist);
  (dbmemsegh(db)->shortstr_area_header).freelist=offset;
}

/** free an existing word-len object
*
* the object is added to the freelist
*
*/

void wg_free_word(void* db, gint offset) {
  dbstore(db,offset,(dbmemsegh(db)->word_area_header).freelist);
  (dbmemsegh(db)->word_area_header).freelist=offset;
}



/** free an existing doubleword object
*
* the object is added to the freelist
*
*/

void wg_free_doubleword(void* db, gint offset) {
  dbstore(db,offset,(dbmemsegh(db)->doubleword_area_header).freelist); //bug fixed here
  (dbmemsegh(db)->doubleword_area_header).freelist=offset;
}

/** free an existing tnode object
*
* the object is added to the freelist
*
*/

void wg_free_tnode(void* db, gint offset) {
  dbstore(db,offset,(dbmemsegh(db)->tnode_area_header).freelist);
  (dbmemsegh(db)->tnode_area_header).freelist=offset;
}

/** free generic fixlen object
*
* the object is added to the freelist
*
*/

void wg_free_fixlen_object(void* db, db_area_header *hdr, gint offset) {
  dbstore(db,offset,hdr->freelist);
  hdr->freelist=offset;
}


/* -------- variable length object allocation and freeing ---------- */


/** allocate a new object of given length
*
* returns correct offset if ok, 0 in case of error
*
*/

gint wg_alloc_gints(void* db, void* area_header, gint nr) {
  gint wantedbytes;   // actually wanted size in bytes, stored in object header
  gint usedbytes;     // amount of bytes used: either wantedbytes or bytes+4 (obj must be 8 aligned)
  gint* freebuckets;
  gint res, nextobject;
  gint nextel;
  gint i;
  gint j;
  gint tmp;
  gint size;
  db_area_header* areah;

  areah=(db_area_header*)area_header;
  wantedbytes=nr*sizeof(gint); // object sizes are stored in bytes
  if (wantedbytes<0) return 0; // cannot allocate negative or zero sizes
  if (wantedbytes<=MIN_VARLENOBJ_SIZE) usedbytes=MIN_VARLENOBJ_SIZE;
  /* XXX: modifying the next line breaks encode_query_param_unistr().
   * Rewrite this using macros to reduce the chance of accidental breakage */
  else if (wantedbytes%8) usedbytes=wantedbytes+4;
  else usedbytes=wantedbytes;
  //printf("wg_alloc_gints called with nr %d and wantedbytes %d and usedbytes %d\n",nr,wantedbytes,usedbytes);
  // first find if suitable length free object is available
  freebuckets=areah->freebuckets;
  if (usedbytes<EXACTBUCKETS_NR && freebuckets[usedbytes]!=0) {
    res=freebuckets[usedbytes];  // first freelist element in that bucket
    nextel=dbfetch(db,res+sizeof(gint)); // next element in freelist of that bucket
    freebuckets[usedbytes]=nextel;
    // change prev ptr of next elem
    if (nextel!=0) dbstore(db,nextel+2*sizeof(gint),dbaddr(db,&freebuckets[usedbytes]));
    // prev elem cannot be free (no consecutive free elems)
    dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
    /* next object should be marked as "prev used" */
    nextobject=res+usedbytes;
    tmp=dbfetch(db,nextobject);
    if (isnormalusedobject(tmp)) dbstore(db,nextobject,makeusedobjectsizeprevused(tmp));
    return res;
  }
  // next try to find first free object in a few nearest exact-length buckets (shorter first)
  for(j=0,i=usedbytes+1;i<EXACTBUCKETS_NR && j<3;i++,j++) {
    if (freebuckets[i]!=0 &&
        getfreeobjectsize(dbfetch(db,freebuckets[i]))>=usedbytes+MIN_VARLENOBJ_SIZE) {
      // found one somewhat larger: now split and store the rest
      res=freebuckets[i];
      tmp=split_free(db,areah,usedbytes,freebuckets,i);
      if (tmp<0) return 0; // error case
      // prev elem cannot be free (no consecutive free elems)
      dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
      return res;
    }
  }
  // next try to use the cached designated victim for creating objects off beginning
  // designated victim is not marked free by header and is not present in any freelist
  size=freebuckets[DVSIZEBUCKET];
  if (usedbytes<=size && freebuckets[DVBUCKET]!=0) {
    res=freebuckets[DVBUCKET];
    if (usedbytes==size) {
      // found a designated victim of exactly right size, dv is used up and disappears
      freebuckets[DVBUCKET]=0;
      freebuckets[DVSIZEBUCKET]=0;
      // prev elem of dv cannot be free
      dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
      return res;
    } else if (usedbytes+MIN_VARLENOBJ_SIZE<=size) {
      // found a designated victim somewhat larger: take the first part and keep the rest as dv
      dbstore(db,res+usedbytes,makespecialusedobjectsize(size-usedbytes)); // store smaller size to victim, turn off free bits
      dbstore(db,res+usedbytes+sizeof(gint),SPECIALGINT1DV); // marks that it is a dv kind of special object
      freebuckets[DVBUCKET]=res+usedbytes; // point to rest of victim
      freebuckets[DVSIZEBUCKET]=size-usedbytes; // rest of victim becomes shorter
      // prev elem of dv cannot be free
      dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
      return res;
    }
  }
  // next try to find first free object in exact-length buckets (shorter first)
  for(i=usedbytes+1;i<EXACTBUCKETS_NR;i++) {
    if (freebuckets[i]!=0 &&
        getfreeobjectsize(dbfetch(db,freebuckets[i]))>=usedbytes+MIN_VARLENOBJ_SIZE) {
      // found one somewhat larger: now split and store the rest
      res=freebuckets[i];
      tmp=split_free(db,areah,usedbytes,freebuckets,i);
      if (tmp<0) return 0; // error case
      // prev elem cannot be free (no consecutive free elems)
      dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
      return res;
    }
  }
  // next try to find first free object in var-length buckets (shorter first)
  for(i=wg_freebuckets_index(db,usedbytes);i<EXACTBUCKETS_NR+VARBUCKETS_NR;i++) {
    if (freebuckets[i]!=0) {
      size=getfreeobjectsize(dbfetch(db,freebuckets[i]));
      if (size==usedbytes) {
        // found one of exactly right size
        res=freebuckets[i];  // first freelist element in that bucket
        nextel=dbfetch(db,res+sizeof(gint)); // next element in freelist of that bucket
        freebuckets[i]=nextel;
        // change prev ptr of next elem
        if (nextel!=0) dbstore(db,nextel+2*sizeof(gint),dbaddr(db,&freebuckets[i]));
        // prev elem cannot be free (no consecutive free elems)
        dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
        return res;
      } else if (size>=usedbytes+MIN_VARLENOBJ_SIZE) {
        // found one somewhat larger: now split and store the rest
        res=freebuckets[i];
        //printf("db %d,nr %d,freebuckets %d,i %d\n",db,(int)nr,(int)freebuckets,(int)i);
        tmp=split_free(db,areah,usedbytes,freebuckets,i);
        if (tmp<0) return 0; // error case
        // prev elem cannot be free (no consecutive free elems)
        dbstore(db,res,makeusedobjectsizeprevused(wantedbytes)); // store wanted size to the returned object
        return res;
      }
    }
  }
  // down here we have found no suitable dv or free object to use for allocation
  // try to get a new memory area
  //printf("ABOUT TO CREATE A NEW SUBAREA\n");
  tmp=extend_varlen_area(db,areah,usedbytes);
  if (!tmp) {  show_dballoc_error(db," cannot initialize new varlen subarea"); return 0; }
  // here we have successfully allocated a new subarea
  // call self recursively: this call will use the new free area
  tmp=wg_alloc_gints(db,areah,nr);
  //show_db_memsegment_header(db);
  return tmp;
}



/** create and initialise a new subarea for var-len obs area
*
* returns allocated size if ok, 0 if failure
* used when the area has no more free space
*
* bytes indicates the minimal required amount:
* could be extended much more, but not less than bytes
*
*/

static gint extend_varlen_area(void* db, void* area_header, gint minbytes) {
  gint i;
  gint tmp;
  gint size, minsize, newsize;
  db_area_header* areah;

  areah=(db_area_header*)area_header;
  i=areah->last_subarea_index;
  if (i+1>=SUBAREA_ARRAY_SIZE) {
    show_dballoc_error_nr(db," no more subarea array elements available for datarec: ",i);
    return 0; // no more subarea array elements available
  }
  size=((areah->subarea_array)[i]).size; // last allocated subarea size
  minsize=minbytes+SUBAREA_ALIGNMENT_BYTES+2*(MIN_VARLENOBJ_SIZE); // minimum allowed
#ifdef CHECK
  if(minsize<0) { /* sanity check */
    show_dballoc_error_nr(db, "invalid number of bytes requested: ", minbytes);
    return 0;
  }
#endif
  if(minsize<MINIMAL_SUBAREA_SIZE)
    minsize=MINIMAL_SUBAREA_SIZE;

  // make newsize power-of-two times larger so that it would be enough for required bytes
  for(newsize=size<<1; newsize>=0 && newsize<minsize; newsize<<=1);
  //printf("OLD SUBAREA SIZE WAS %d NEW SUBAREA SIZE SHOULD BE %d\n",size,newsize);

  while(newsize >= minsize) {
    if(!init_db_subarea(db,areah,i+1,newsize)) {
      goto done;
    }
    /* fall back to smaller size */
    newsize>>=1;
    //printf("REQUIRED SPACE FAILED, TRYING %d\n",newsize);
  }
  show_dballoc_error_nr(db," cannot extend datarec area with a new subarea of size: ",newsize<<1);
  return 0;
done:
  // here we have successfully allocated a new subarea
  tmp=init_subarea_freespace(db,areah,i+1); // mark beg and end, store new victim
  if (tmp) {  show_dballoc_error(db," cannot initialize new subarea"); return 0; }
  return newsize;
}



/** splits a free object into a smaller new object and the remainder, stores remainder to right list
*
* returns 0 if ok, negative nr in case of error
* we assume we always split the first elem in a bucket freelist
* we also assume the remainder is >=MIN_VARLENOBJ_SIZE
*
*/

static gint split_free(void* db, void* area_header, gint nr, gint* freebuckets, gint i) {
  gint object;
  gint oldsize;
  gint oldnextptr;
  gint splitsize;
  gint splitobject;
  gint splitindex;
  gint freelist;
  gint dv;
  gint dvsize;
  gint dvindex;

  object=freebuckets[i]; // object offset
  oldsize=dbfetch(db,object); // first gint at offset
  if (!isfreeobject(oldsize)) return -1; // not really a free object!
  oldsize=getfreeobjectsize(oldsize); // remove free bits, get real size
  // observe object is first obj in freelist, hence no free obj at prevptr
  oldnextptr=dbfetch(db,object+sizeof(gint)); // second gint at offset
  // store new size at offset (beginning of object) and mark as used with used prev
  // observe that a free object cannot follow another free object, hence we know prev is used
  dbstore(db,object,makeusedobjectsizeprevused(nr));
  freebuckets[i]=oldnextptr; // store ptr to next elem into bucket ptr
  splitsize=oldsize-nr; // remaining size
  splitobject=object+nr;  // offset of the part left
  // we may store the splitobject as a designated victim instead of a suitable freelist
  // but currently this is disallowed and the underlying code is not really finished:
  // marking of next used object prev-free/prev-used is missing
  // instead of this code we rely on using a newly freed object as dv is larger than dv
  dvsize=freebuckets[DVSIZEBUCKET];
  if (0) { // (splitsize>dvsize) {
    // store splitobj as a new designated victim, but first store current victim to freelist, if possible
    dv=freebuckets[DVBUCKET];
    if (dv!=0) {
      if (dvsize<MIN_VARLENOBJ_SIZE) {
        show_dballoc_error(db,"split_free notices corruption: too small designated victim");
        return -1; // error case
      }
      dbstore(db,dv,makefreeobjectsize(dvsize)); // store new size with freebits to dv
      dbstore(db,dv+dvsize-sizeof(gint),makefreeobjectsize(dvsize));
      dvindex=wg_freebuckets_index(db,dvsize);
      freelist=freebuckets[dvindex];
      if (freelist!=0) dbstore(db,freelist+2*sizeof(gint),dv); // update prev ptr
      dbstore(db,dv+sizeof(gint),freelist); // store previous freelist
      dbstore(db,dv+2*sizeof(gint),dbaddr(db,&freebuckets[dvindex])); // store ptr to previous
      freebuckets[dvindex]=dv; // store offset to correct bucket
      //printf("PUSHED DV WITH SIZE %d TO FREELIST TO BUCKET %d:\n",dvsize,dvindex);
      //show_bucket_freeobjects(db,freebuckets[dvindex]);
    }
    // store splitobj as a new victim
    //printf("REPLACING DV WITH OBJ AT %d AND SIZE %d\n",splitobject,splitsize);
    dbstore(db,splitobject,makespecialusedobjectsize(splitsize)); // length with special used object mark
    dbstore(db,splitobject+sizeof(gint),SPECIALGINT1DV); // marks that it is a dv kind of special object
    freebuckets[DVBUCKET]=splitobject;
    freebuckets[DVSIZEBUCKET]=splitsize;
    return 0;
  } else {
    // store splitobj in a freelist, no changes to designated victim
    dbstore(db,splitobject,makefreeobjectsize(splitsize)); // store new size with freebit to the second half of object
    dbstore(db,splitobject+splitsize-sizeof(gint),makefreeobjectsize(splitsize));
    splitindex=wg_freebuckets_index(db,splitsize); // bucket to store the split remainder
    if (splitindex<0) return splitindex; // error case
    freelist=freebuckets[splitindex];
    if (freelist!=0) dbstore(db,freelist+2*sizeof(gint),splitobject); // update prev ptr
    dbstore(db,splitobject+sizeof(gint),freelist); // store previous freelist
    dbstore(db,splitobject+2*sizeof(gint),dbaddr(db,&freebuckets[splitindex])); // store ptr to previous
    freebuckets[splitindex]=splitobject; // store remainder offset to correct bucket
    return 0;
  }
}

/** returns a correct freebuckets index for a given size of object
*
* returns -1 in case of error, 0,...,EXACBUCKETS_NR+VARBUCKETS_NR-1 otherwise
*
* sizes 0,1,2,...,255 in exactbuckets (say, EXACBUCKETS_NR=256)
* longer sizes in varbuckets:
* sizes 256-511 in bucket 256,
*       512-1023 in bucket 257 etc
* 256*2=512, 512*2=1024, etc
*/

gint wg_freebuckets_index(void* db, gint size) {
  gint i;
  gint cursize;

  if (size<EXACTBUCKETS_NR) return size;
  cursize=EXACTBUCKETS_NR*2;
  for(i=0; i<VARBUCKETS_NR; i++) {
    if (size<cursize) return EXACTBUCKETS_NR+i;
    cursize=cursize*2;
  }
  return -1; // too large size, not enough buckets
}

/** frees previously alloc_bytes obtained var-length object at offset
*
* returns 0 if ok, negative value if error (likely reason: wrong object ptr)
* merges the freed object with free neighbours, if available, to get larger free objects
*
*/

gint wg_free_object(void* db, void* area_header, gint object) {
  gint size;
  gint i;
  gint* freebuckets;

  gint objecthead;
  gint prevobject;
  gint prevobjectsize;
  gint prevobjecthead;
  gint previndex;
  gint nextobject;
  gint nextobjecthead;
  gint nextindex;
  gint freelist;
  gint prevnextptr;
  gint prevprevptr;
  gint nextnextptr;
  gint nextprevptr;
  gint bucketfreelist;
  db_area_header* areah;

  gint dv;
  gint dvsize;
  gint tmp;

  areah=(db_area_header*)area_header;
  if (!dbcheck(db)) {
    show_dballoc_error(db,"wg_free_object first arg is not a db address");
    return -1;
  }
  //printf("db %u object %u \n",db,object);
  //printf("freeing object %d with size %d and end %d\n",
  //        object,getusedobjectsize(dbfetch(db,object)),object+getusedobjectsize(dbfetch(db,object)));
  objecthead=dbfetch(db,object);
  if (isfreeobject(objecthead)) {
    show_dballoc_error(db,"wg_free_object second arg is already a free object");
    return -2; // attempting to free an already free object
  }
  size=getusedobjectsize(objecthead); // size stored at first gint of object
  if (size<MIN_VARLENOBJ_SIZE) {
    show_dballoc_error(db,"wg_free_object second arg has a too small size");
    return -3; // error: wrong size info (too small)
  }
  freebuckets=areah->freebuckets;

  // first try to merge with the previous free object, if so marked
  if (isnormalusedobjectprevfree(objecthead)) {
    //printf("**** about to merge object %d on free with prev %d !\n",object,prevobject);
    // use the size of the previous (free) object stored at the end of the previous object
    prevobjectsize=getfreeobjectsize(dbfetch(db,(object-sizeof(gint))));
    prevobject=object-prevobjectsize;
    prevobjecthead=dbfetch(db,prevobject);
    if (!isfreeobject(prevobjecthead) || !getfreeobjectsize(prevobject)==prevobjectsize) {
      show_dballoc_error(db,"wg_free_object notices corruption: previous object is not ok free object");
      return -4; // corruption noticed
    }
    // remove prev object from its freelist
    // first, get necessary information
    prevnextptr=dbfetch(db,prevobject+sizeof(gint));
    prevprevptr=dbfetch(db,prevobject+2*sizeof(gint));
    previndex=wg_freebuckets_index(db,prevobjectsize);
    freelist=freebuckets[previndex];
    // second, really remove prev object from freelist
    if (freelist==prevobject) {
      // prev object pointed to directly from bucket
      freebuckets[previndex]=prevnextptr;  // modify prev prev
      if (prevnextptr!=0) dbstore(db,prevnextptr+2*sizeof(gint),prevprevptr); // modify prev next
    } else {
      // prev object pointed to from another object, not directly bucket
      // next of prev of prev will point to next of next
      dbstore(db,prevprevptr+sizeof(gint),prevnextptr);
      // prev of next of prev will prev-point to prev of prev
      if (prevnextptr!=0) dbstore(db,prevnextptr+2*sizeof(gint),prevprevptr);
    }
    // now treat the prev object as the current object to be freed!
    object=prevobject;
    size=size+prevobjectsize;
  } else if ((freebuckets[DVBUCKET]+freebuckets[DVSIZEBUCKET])==object) {
    // should merge with a previous dv
    object=freebuckets[DVBUCKET];
    size=size+freebuckets[DVSIZEBUCKET]; // increase size to cover dv as well
    // modify dv size information in area header: dv will extend to freed object
    freebuckets[DVSIZEBUCKET]=size;
    // store dv size and marker to dv head
    dbstore(db,object,makespecialusedobjectsize(size));
    dbstore(db,object+sizeof(gint),SPECIALGINT1DV);
    return 0;    // do not store anything to freebuckets!!
  }

  // next, try to merge with the next object: either free object or dv
  // also, if next object is normally used instead, mark it as following the free object
  nextobject=object+size;
  nextobjecthead=dbfetch(db,nextobject);
  if (isfreeobject(nextobjecthead)) {
    // should merge with a following free object
    //printf("**** about to merge object %d on free with next %d !\n",object,nextobject);
    size=size+getfreeobjectsize(nextobjecthead); // increase size to cover next object as well
    // remove next object from its freelist
    // first, get necessary information
    nextnextptr=dbfetch(db,nextobject+sizeof(gint));
    nextprevptr=dbfetch(db,nextobject+2*sizeof(gint));
    nextindex=wg_freebuckets_index(db,getfreeobjectsize(nextobjecthead));
    freelist=freebuckets[nextindex];
    // second, really remove next object from freelist
    if (freelist==nextobject) {
      // next object pointed to directly from bucket
      freebuckets[nextindex]=nextnextptr;  // modify next prev
      if (nextnextptr!=0) dbstore(db,nextnextptr+2*sizeof(gint),nextprevptr); // modify next next
    } else {
      // next object pointed to from another object, not directly bucket
      // prev of next will point to next of next
      dbstore(db,nextprevptr+sizeof(gint),nextnextptr);
      // next of next will prev-point to prev of next
      if (nextnextptr!=0) dbstore(db,nextnextptr+2*sizeof(gint),nextprevptr);
    }
  } else if (isspecialusedobject(nextobjecthead) && nextobject==freebuckets[DVBUCKET]) {
    // should merge with a following dv
    size=size+freebuckets[DVSIZEBUCKET]; // increase size to cover next object as well
    // modify dv information in area header
    freebuckets[DVBUCKET]=object;
    freebuckets[DVSIZEBUCKET]=size;
    // store dv size and marker to dv head
    dbstore(db,object,makespecialusedobjectsize(size));
    dbstore(db,object+sizeof(gint),SPECIALGINT1DV);
    return 0;    // do not store anything to freebuckets!!
  }  else if (isnormalusedobject(nextobjecthead)) {
    // mark the next used object as following a free object
    dbstore(db,nextobject,makeusedobjectsizeprevfree(dbfetch(db,nextobject)));
  }  // we do no special actions in case next object is end marker

  // maybe the newly freed object is larger than the designated victim?
  // if yes, use the newly freed object as a new designated victim
  // and afterwards put the old dv to freelist
  if (size>freebuckets[DVSIZEBUCKET]) {
    dv=freebuckets[DVBUCKET];
    dvsize=freebuckets[DVSIZEBUCKET];
    freebuckets[DVBUCKET]=object;
    freebuckets[DVSIZEBUCKET]=size;
    dbstore(db,object,makespecialusedobjectsize(size));
    dbstore(db,object+sizeof(gint),SPECIALGINT1DV);
    // set the next used object mark to prev-used!
    nextobject=object+size;
    tmp=dbfetch(db,nextobject);
    if (isnormalusedobject(tmp)) dbstore(db,nextobject,makeusedobjectsizeprevused(tmp));
    // dv handling
    if (dv==0) return 0; // if no dv actually, then nothing to put to freelist
    // set the object point to dv to make it put into freelist after
    // but first mark the next object after dv as following free
    nextobject=dv+dvsize;
    tmp=dbfetch(db,nextobject);
    if (isnormalusedobject(tmp)) dbstore(db,nextobject,makeusedobjectsizeprevfree(tmp));
    // let the old dv be handled as object to be put to freelist after
    object=dv;
    size=dvsize;
  }
  // store freed (or freed and merged) object to the correct bucket,
  // except for dv-merge cases above (returns earlier)
  i=wg_freebuckets_index(db,size);
  bucketfreelist=freebuckets[i];
  if (bucketfreelist!=0) dbstore(db,bucketfreelist+2*sizeof(gint),object); // update prev ptr
  dbstore(db,object,makefreeobjectsize(size)); // store size and freebit
  dbstore(db,object+size-sizeof(gint),makefreeobjectsize(size)); // store size and freebit
  dbstore(db,object+sizeof(gint),bucketfreelist); // store previous freelist
  dbstore(db,object+2*sizeof(gint),dbaddr(db,&freebuckets[i])); // store prev ptr
  freebuckets[i]=object;
  return 0;
}


/*
Tanel Tammet
http://www.epl.ee/?i=112121212
Kuiv tn 9, Tallinn, Estonia
+3725524876

len |  refcount |   xsd:type |  namespace |  contents .... |

header: 4*4=16 bytes

128 bytes

*/

/***************** Child database functions ******************/


/* Register external database offset
 *
 * Stores offset and size of an external database. This allows
 * recognizing external pointers/offsets and computing their
 * base offset.
 *
 * Once external data is stored to the database, the memory
 * image can no longer be saved/restored.
 */
gint wg_register_external_db(void *db, void *extdb) {
#ifdef USE_CHILD_DB
  db_memsegment_header* dbh = dbmemsegh(db);

#ifdef CHECK
  if(dbh->key != 0) {
    show_dballoc_error(db,
      "external references not allowed in a shared memory db");
    return -1;
  }
#endif

  if(dbh->index_control_area_header.number_of_indexes > 0) {
    return show_dballoc_error(db,
      "Database has indexes, external references not allowed");
  }
  if(dbh->extdbs.count >= MAX_EXTDB) {
    show_dballoc_error(db, "cannot register external database");
  } else {
    dbh->extdbs.offset[dbh->extdbs.count] = ptrtooffset(db, dbmemsegh(extdb));
    dbh->extdbs.size[dbh->extdbs.count++] = \
      dbmemsegh(extdb)->size;
  }
  return 0;
#else
  show_dballoc_error(db, "child database support is not enabled");
  return -1;
#endif
}

/******************** Hash index support *********************/

/*
 * Initialize a new hash table for an index.
 */
gint wg_create_hash(void *db, db_hash_area_header* areah, gint size) {
  if(size <= 0)
    size = DEFAULT_IDXHASH_LENGTH;
  if(init_hash_subarea(db, areah, size)) {
    return show_dballoc_error(db," cannot create strhash array area");
  }
  return 0;
}

/********** Helper functions for accessing the header ********/

/*
 * Return free space in segment (in bytes)
 * Also tries to predict whether it is possible to allocate more
 * space in the segment.
 */
gint wg_database_freesize(void *db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint freesize = dbh->size - dbh->free;
  return (freesize < MINIMAL_SUBAREA_SIZE ? 0 : freesize);
}

/*
 * Return total segment size (in bytes)
 */
gint wg_database_size(void *db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  return dbh->size;
}


/* --------------- error handling ------------------------------*/

/** called with err msg when an allocation error occurs
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_dballoc_error(void* db, char* errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"db memory allocation error: %s\n",errmsg);
#endif
  return -1;
}

/** called with err msg and err nr when an allocation error occurs
*
*  may print or log an error
*  does not do any jumps etc
*/

static gint show_dballoc_error_nr(void* db, char* errmsg, gint nr) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"db memory allocation error: %s %d\n", errmsg, (int) nr);
#endif
  return -1;

}

#ifdef __cplusplus
}
#endif
