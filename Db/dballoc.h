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

 /** @file dballoc.h
 * Public headers for database heap allocation procedures.
 */

#ifndef __defined_dballoc_h
#define __defined_dballoc_h

#include "../config.h"

/*


Levels of allocation used:

- Memory segment allocation: gives a large contiguous area of memory (typically shared memory).
  Could be extended later (has to be contiguous).

- Inside the contiguous memory segment: Allocate usage areas for different heaps
  (data records, strings, doubles, lists, etc).
  Each area is typically not contiguous: can consist of several subareas of different length.
  
  Areas have different object allocation principles:
  - fixed-length object area (e.g. list cells) allocation uses pre-calced freelists
  - various-length object area (e.g. data records) allocation uses ordinary allocation techniques:
    - objects initialised from next free space / unused object, split as needed
    - short freed objects are put into freelists in size-corresponding buckets
    - large freed object lists contain objects of different sizes
  
- Data object allocation: data records, strings, list cells etc. 
  Allocated in corresponding subareas.

*/

#define MEMSEGMENT_MAGIC_MARK 1232319011  /** enables to check that we really have db pointer */
#define SUBAREA_ARRAY_SIZE 10      /** nr of possible subareas in each area  */
#define INITIAL_SUBAREA_SIZE 8192  /** size of the first created subarea (bytes)  */
#define MINIMAL_SUBAREA_SIZE 8192  /** checked before subarea creation to filter out stupid requests */
#define ALIGNMENT_BYTES 8          /** subarea alignment     */

#define EXACTBUCKETS_NR 256                  /** amount of free ob buckets with exact length */
#define VARBUCKETS_NR 32                   /** amount of free ob buckets with varying length */
#define CACHEBUCKETS_NR 2                  /** buckets used as special caches */
#define DVBUCKET EXACTBUCKETS_NR+VARBUCKETS_NR     /** cachebucket: designated victim offset */
#define DVSIZEBUCKET EXACTBUCKETS_NR+VARBUCKETS_NR+1 /** cachebucket: byte size of designated victim */
#define MIN_VARLENOBJ_SIZE (4*(gint)(sizeof(gint)))  /** minimal size of variable length object */
#define OBJSIZE_GRANULARITY ((gint)(sizeof(gint)))   /** object size must be multiple of OBJSIZE_GRANULARITY */

#define SHORTSTR_SIZE 32 /** max len of short strings  */

/* ====== general typedefs and macros ======= */

// integer and address fetch and store

typedef int gint;  /** always used instead of int. Pointers are also handled as gint. */

#define dbfetch(db,offset) (*((gint*)(((char*)(db))+(offset)))) /** get gint from address */
#define dbstore(db,offset,data) (*((gint*)(((char*)(db))+(offset)))=data) /** store gint to address */
#define dbaddr(db,realptr) ((gint)(((char*)(realptr))-((char*)(db)))) /** give offset of real adress */
#define dbcheck(db) (dbfetch((db),0)==MEMSEGMENT_MAGIC_MARK) /** check that correct db ptr */

/* ==== fixlen object allocation macros ==== */

#define alloc_listcell(db) alloc_fixlen_object((db),&(((db_memsegment_header*)(db))->listcell_area_header))
#define alloc_shortstr(db) alloc_fixlen_object((db),&(((db_memsegment_header*)(db))->shortstr_area_header))
#define alloc_word(db) alloc_fixlen_object((db),&(((db_memsegment_header*)(db))->word_area_header))
#define alloc_doubleword(db) alloc_fixlen_object((db),&(((db_memsegment_header*)(db))->doubleword_area_header))

/* ==== allocation special macros ==== */

#define isfreeobject(i)  ((i) & 1) /** object is free if lowest bit is 1 */ 
#define getobjectsize(i) ((i) & ~3) /** mask off two lowest bits: just keep all higher */
#define setcfree(i)  ((i) | 1) /** set lowest bit to 1: current object is free */
// #define setpfree(i)  ((i) | 2) /** set next lowest bit to 1: previous object is free ???? */

/* ===  data structures used in allocated areas  ===== */


/** general list cell: a pair of two integers (both can be also used as pointers) */

typedef struct {
  gint car;  /** first element */
  gint cdr;} /** second element, often a pointer to the rest of the list */
gcell;

#define car(cell)  (((gint)((gcell*)(cell)))->car)  /** get list cell first elem gint */
#define cdr(cell)  (((gint)((gcell*)(cell)))->car)  /** get list cell second elem gint */
  

/* ====== segment/area header data structures ======== */

/*
memory segment structure:
  
-------------  
db_memsegment_header
- - - - - - - 
db_area_header
-   -   -  -  
db_subarea_header
...
db_subarea_header
- - - - - - -
...  
- - - - - - - 
db_area_header
-   -   -  -  
db_subarea_header
...
db_subarea_header  
----------------
various actual subareas 
----------------
*/
  

/** located inside db_area_header: one single memory subarea header
*
*/
  
typedef struct _db_subarea_header {    
  gint size; /** size of subarea */
  gint offset; /** subarea offset from segment start */
  gint free; /** pointer to first free area in subarea (not used in case freelist used) */
} db_subarea_header;


/** located inside db_memsegment_header: one single memory area header
*
*/

typedef struct _db_area_header {   
  gint fixedlength;        /** 1 if fixed length area, 0 if variable length */
  gint objlength;          /** only for fixedlength: length of allocatable obs in bytes */ 
  gint freelist;           /** freelist start: if 0, then no free objects available */ 
  gint last_subarea_index; /** last used subarea index (0,...,) */
  db_subarea_header subarea_array[SUBAREA_ARRAY_SIZE]; /** array of subarea headers */
  gint freebuckets[EXACTBUCKETS_NR+VARBUCKETS_NR+CACHEBUCKETS_NR]; /** array of subarea headers */
} db_area_header;

/** located at the very beginning of the memory segment
*
*/

typedef struct _db_memsegment_header {  
  gint mark;       /** fixed uncommon int to check if really a segment */ 
  gint size;       /** segment size in bytes  */
  gint free;       /** pointer to first free area in segment */
  gint initialadr; /** initial segment address, only valid for creator */
  gint key;        /** global shared mem key */
  db_area_header datarec_area_header;     
  db_area_header listcell_area_header;
  db_area_header shortstr_area_header;
  db_area_header word_area_header;
  db_area_header doubleword_area_header;  
} db_memsegment_header;




/* ==== Protos ==== */

gint init_db_memsegment(void* db, gint key, gint size); // creates initial memory structures for a new db
gint init_db_subarea(void* db, void* area_header, gint index, gint size);
gint alloc_db_segmentchunk(void* db, gint size); // allocates a next chunk from db memory segment

gint make_subarea_freelist(void* db, void* area_header, gint arrayindex);
gint init_area_buckets(void* db, void* area_header);
gint init_subarea_freespace(void* db, void* area_header, gint arrayindex);

gint alloc_fixlen_object(void* db, void* area_header);
gint extend_fixedlen_area(void* db, void* area_header);
void free_cell(void* db, gint cell);

gint alloc_gints(void* db, gint nr);
gint split_free(void* db, gint nr, gint* freebuckets, gint i);
gint freebuckets_index(void* db, gint size);
gint free_object(void* db, gint object) ;

void show_db_memsegment_header(void* db);
void show_db_area_header(void* db, void* area_header);
void show_bucket_freeobjects(void* db, gint freelist);
gint count_freelist(void* db, gint freelist); 

gint show_dballoc_error_nr(void* db, char* errmsg, gint nr);
gint show_dballoc_error(void* db, char* errmsg);

/* ------- testing ------------ */

#endif
