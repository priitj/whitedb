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

#define DB_MEMSEGMENT_MARK 111111  /** enables to check that we really have db pointer */
#define SUBAREA_ARRAY_SIZE 10      /** nr of possible subareas in each area  */
#define INITIAL_SUBAREA_SIZE 10000 /** size of the first created subarea (bytes)  */
#define ALIGNMENT_BYTES 8          /** subarea alignment     */


/* ====== general typedefs and macros ======= */

// integer and address fetch and store

typedef int gint;  /** always used instead of int. Pointers are also handled as gint. */

#define dbfetch(db,offset) (*((gint*)(((char*)(db))+(offset)))) /** get gint from address */
#define dbstore(db,offset,data) (*((gint*)(((char*)(db))+(offset)))=data) /** store gint to address */


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
  db_subarea_header subarea_array[SUBAREA_ARRAY_SIZE]; // array of subarea headers */
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
  db_area_header string_area_header; 
  db_area_header list_area_header;
  db_area_header word_area_header;
  db_area_header doubleword_area_header;
} db_memsegment_header;




/* ==== Protos ==== */

gint init_db_memsegment(void* db, gint key, gint size); // creates initial memory structures for a new db
gint init_db_subarea(void* db, void* area_header, gint size);
gint alloc_db_segmentchunk(void* db, gint size); // allocates a next chunk from db memory segment

void make_subarea_freelist(void* db, void* area_header, gint arrayindex);

gint alloc_cell(void* db);
gint extend_fixedlen_area(void* db, void* area_header);

void show_db_memsegment_header(void* db);
void show_db_area_header(void* db, void* area_header);
gint count_freelist(void* db, gint freelist); 

gint show_dballoc_error_nr(void* db, char* errmsg, gint nr);
gint show_dballoc_error(void* db, char* errmsg);

/* ------- testing ------------ */

#endif
