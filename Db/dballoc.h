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

 /** @file dballoc.h
 * Public headers for database heap allocation procedures.
 */

#ifndef DEFINED_DBALLOC_H
#define DEFINED_DBALLOC_H

/* For gint/wg_int types */
#include <stddef.h>
#ifndef _MSC_VER
#include <stdint.h>
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#define USE_DATABASE_HANDLE
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
    - objects initialised from next free  / designated victim object, split as needed
    - short freed objects are put into freelists in size-corresponding buckets
    - large freed object lists contain objects of different sizes

- Data object allocation: data records, strings, list cells etc.
  Allocated in corresponding subareas.

list area: 8M  is filled
  16 M area
  32
datarec area:
  8M is filled
  16 M area
  32 M area


Fixlen allocation:

- Fixlen objects are allocated using a pre-calced singly-linked freelist. When one subarea
  is exhausted(freelist empty), a new subarea is taken, it is organised into a long
  freelist and the beginning of the freelist is stored in db_area_header.freelist.

- Each freelist element is one fixlen object. The first gint of the object is an offset of
  the next freelist element. The list is terminated with 0.

Varlen allocation follows the main ideas of the Doug Lea allocator:

- the minimum size to allocate is 4 gints (MIN_VARLENOBJ_SIZE) and all objects
  should be aligned at least to a gint.

- each varlen area contains a number of gint-size buckets for storing different
  doubly-linked freelists. The buckets are:
  - EXACTBUCKETS_NR of buckets for exact object size. Contains an offset of the first
      free object of this size.
  - VARBUCKETS_NR of buckets for variable (interval between prev and next) object size,
      growing exponentially. Contains an offset of the first free object in this size interval.
  - EXACTBUCKETS_NR+VARBUCKETS_NR+1 is a designated victim (marked as in use):
      offset of the preferred place to split off new objects.
      Initially the whole free area is made one big designated victim.
  - EXACTBUCKETS_NR+VARBUCKETS_NR+2 is a size of the designated victim.

- a free object contains gints:
  - size (in bytes) with last two bits marked (i.e. not part of size!):
    - last bits: 00
  - offset of the next element in the freelist (terminated with 0).
  - offset of the previous element in the freelist (can be offset of the bucket!)
  ... arbitrary nr of bytes ...
  - size (in bytes) with last two bits marked as the initial size gint.
    This repeats the initial size gint and is located at the very end of the
    memory block.

- an in-use object contains gints:
  - size (in bytes) with mark bits and assumptions:
     - last 2 bits markers, not part of size:
        - for normal in-use objects with in-use predecessor 00
        - for normal in-use objects with free predecessor 10
        - for specials (dv area and start/end markers) 11
     - real size taken is always 8-aligned (minimal granularity 8 bytes)
     - size gint may be not 8-aligned if 32-bit gint used (but still has to be 4-aligned). In this case:
        - if size gint is not 8-aligned, real size taken either:
           - if size less than MIN_VARLENOBJ_SIZE, then MIN_VARLENOBJ_SIZE
           - else size+4 bytes (but used size is just size, no bytes added)
  - usable gints following

- a designated victim is marked to be in use:
  - the first gint has last bits 11 to differentiate from normal in-use objects (00 or 10 bits)
  - the second gint contains 0 to indicate that it is a dv object, and not start marker (1) or end marker (2)
  - all the following gints are arbitrary and contain no markup.

- the first 4 gints and the last 4 gints of each subarea are marked as in-use objects, although
  they should be never used! The reason is to give a markup for subarea beginning and end.
  - last bits 10 to differentiate from normal in-use objects (00 bits)
  - the next gint is 1 for start marker an 2 for end marker
  - the following 2 gints are arbitrary and contain no markup

 - summary of end bits for various objects:
   - 00  in-use normal object with in-use previous object
   - 10 in-use normal object with a free previous object
   - 01 free object
   - 11 in-use special object (dv or start/end marker)

*/

#define MEMSEGMENT_MAGIC_MARK 1232319011  /** enables to check that we really have db pointer */
#define MEMSEGMENT_MAGIC_INIT 1916950123  /** init time magic */
#define MEMSEGMENT_VERSION ((VERSION_REV<<16)|\
  (VERSION_MINOR<<8)|(VERSION_MAJOR)) /** written to dump headers for compatibilty checking */
#define SUBAREA_ARRAY_SIZE 64      /** nr of possible subareas in each area  */
#define INITIAL_SUBAREA_SIZE 8192  /** size of the first created subarea (bytes)  */
#define MINIMAL_SUBAREA_SIZE 8192  /** checked before subarea creation to filter out stupid requests */
#define SUBAREA_ALIGNMENT_BYTES 8          /** subarea alignment     */
#define SYN_VAR_PADDING 128          /** sync variable padding in bytes */
#if (LOCK_PROTO==3)
#define MAX_LOCKS 64                /** queue size (currently fixed :-() */
#endif

#define EXACTBUCKETS_NR 256                  /** amount of free ob buckets with exact length */
#define VARBUCKETS_NR 32                   /** amount of free ob buckets with varying length */
#define CACHEBUCKETS_NR 2                  /** buckets used as special caches */
#define DVBUCKET EXACTBUCKETS_NR+VARBUCKETS_NR     /** cachebucket: designated victim offset */
#define DVSIZEBUCKET EXACTBUCKETS_NR+VARBUCKETS_NR+1 /** cachebucket: byte size of designated victim */
#define MIN_VARLENOBJ_SIZE (4*(gint)(sizeof(gint)))  /** minimal size of variable length object */

#define SHORTSTR_SIZE 32 /** max len of short strings  */

/* defaults, used when there is no user-supplied or computed value */
#define DEFAULT_STRHASH_LENGTH 10000  /** length of the strhash array (nr of array elements) */
#define DEFAULT_IDXHASH_LENGTH 10000  /** hash index hash size */

#define ANONCONST_TABLE_SIZE 200 /** length of the table containing predefined anonconst uri ptrs */

/* ====== general typedefs and macros ======= */

// integer and address fetch and store

typedef ptrdiff_t gint;  /** always used instead of int. Pointers are also handled as gint. */
#ifndef _MSC_VER /* MSVC on Win32 */
typedef int32_t gint32;    /** 32-bit fixed size storage */
typedef int64_t gint64;    /** 64-bit fixed size storage */
#else
typedef __int32 gint32;    /** 32-bit fixed size storage */
typedef __int64 gint64;    /** 64-bit fixed size storage */
#endif

#ifdef USE_DATABASE_HANDLE
#define dbmemseg(x) ((void *)(((db_handle *) x)->db))
#define dbmemsegh(x) ((db_memsegment_header *)(((db_handle *) x)->db))
#define dbmemsegbytes(x) ((char *)(((db_handle *) x)->db))
#else
#define dbmemseg(x) ((void *)(x))
#define dbmemsegh(x) ((db_memsegment_header *)(x))
#define dbmemsegbytes(x) ((char *)(x))
#endif

#define dbfetch(db,offset) (*((gint*)(dbmemsegbytes(db)+(offset)))) /** get gint from address */
#define dbstore(db,offset,data) (*((gint*)(dbmemsegbytes(db)+(offset)))=data) /** store gint to address */
#define dbaddr(db,realptr) ((gint)(((char*)(realptr))-dbmemsegbytes(db))) /** give offset of real adress */
#define offsettoptr(db,offset) ((void*)(dbmemsegbytes(db)+(offset))) /** give real address from offset */
#define ptrtooffset(db,realptr) (dbaddr((db),(realptr)))
#define dbcheckh(dbh) (dbh!=NULL && *((gint32 *) dbh)==MEMSEGMENT_MAGIC_MARK) /** check that correct db ptr */
#define dbcheck(db) dbcheckh(dbmemsegh(db)) /** check that correct db ptr */
#define dbcheckhinit(dbh) (dbh!=NULL && *((gint32 *) dbh)==MEMSEGMENT_MAGIC_INIT)
#define dbcheckinit(db) dbcheckhinit(dbmemsegh(db))

/* ==== fixlen object allocation macros ==== */

#define alloc_listcell(db) wg_alloc_fixlen_object((db),&(dbmemsegh(db)->listcell_area_header))
#define alloc_shortstr(db) wg_alloc_fixlen_object((db),&(dbmemsegh(db)->shortstr_area_header))
#define alloc_word(db) wg_alloc_fixlen_object((db),&(dbmemsegh(db)->word_area_header))
#define alloc_doubleword(db) wg_alloc_fixlen_object((db),&(dbmemsegh(db)->doubleword_area_header))

/* ==== varlen object allocation special macros ==== */

#define isfreeobject(i)  (((i) & 3)==1) /** end bits 01 */
#define isnormalusedobject(i)  (!((i) & 1)) /** end bits either 00 or 10, i.e. last bit 0 */
#define isnormalusedobjectprevused(i)  (!((i) & 3)) /**  end bits 00 */
#define isnormalusedobjectprevfree(i)  (((i) & 3)==2) /** end bits 10 */
#define isspecialusedobject(i)  (((i) & 3) == 3) /**  end bits 11 */

#define getfreeobjectsize(i) ((i) & ~3) /** mask off two lowest bits: just keep all higher */
/** small size marks always use MIN_VARLENOBJ_SIZE,
* non-8-aligned size marks mean obj really takes 4 more bytes (all real used sizes are 8-aligned)
*/
#define getusedobjectsize(i) (((i) & ~3)<=MIN_VARLENOBJ_SIZE ?  MIN_VARLENOBJ_SIZE : ((((i) & ~3)%8) ? (((i) & ~3)+4) : ((i) & ~3)) )
#define getspecialusedobjectsize(i) ((i) & ~3) /** mask off two lowest bits: just keep all higher */

#define getusedobjectwantedbytes(i) ((i) & ~3)
#define getusedobjectwantedgintsnr(i) (((i) & ~3)>>((sizeof(gint)==4) ? 2 : 3)) /** divide pure size by four or eight */

#define makefreeobjectsize(i)  (((i) & ~3)|1) /** set lowest bits to 01: current object is free */
#define makeusedobjectsizeprevused(i) ((i) & ~3) /** set lowest bits to 00 */
#define makeusedobjectsizeprevfree(i) (((i) & ~3)|2) /** set lowest bits to 10 */
#define makespecialusedobjectsize(i) ((i)|3) /** set lowest bits to 11 */

#define SPECIALGINT1DV 1    /** second gint of a special in use dv area */
#define SPECIALGINT1START 0 /** second gint of a special in use start marker area, should be 0 */
#define SPECIALGINT1END 0 /** second gint of a special in use end marker area, should be 0 */

// #define setpfree(i)  ((i) | 2) /** set next lowest bit to 1: previous object is free ???? */

/* ===  data structures used in allocated areas  ===== */


/** general list cell: a pair of two integers (both can be also used as pointers) */

typedef struct {
  gint car;  /** first element */
  gint cdr;} /** second element, often a pointer to the rest of the list */
gcell;

#define car(cell)  (((gint)((gcell*)(cell)))->car)  /** get list cell first elem gint */
#define cdr(cell)  (((gint)((gcell*)(cell)))->cdr)  /** get list cell second elem gint */


/* index related stuff */
#define MAX_INDEX_FIELDS 10       /** maximum number of fields in one index */
#define MAX_INDEXED_FIELDNR 127   /** limits the size of field/index table */

#ifndef TTREE_CHAINED_NODES
#define WG_TNODE_ARRAY_SIZE 10
#else
#define WG_TNODE_ARRAY_SIZE 8
#endif

/* logging related */
#define maxnumberoflogrows 10

/* external database stuff */
#define MAX_EXTDB   20

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
*  alignedoffset should be always used: it may come some bytes after offset
*/

typedef struct _db_subarea_header {
  gint size; /** size of subarea */
  gint offset;          /** subarea exact offset from segment start: do not use for objects! */
  gint alignedsize;     /** subarea object alloc usable size: not necessarily to end of area */
  gint alignedoffset;   /** subarea start as to be used for object allocation */
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

/** synchronization structures in shared memory
*
* Note that due to the similarity we can keep the memory images
* using the wpspin and rpspin protocols compatible.
*/

typedef struct {
#if !defined(LOCK_PROTO) || (LOCK_PROTO < 3) /* rpspin, wpspin */
  gint global_lock;        /** db offset to cache-aligned sync variable */
  gint writers;            /** db offset to cache-aligned writer count */
  char _storage[SYN_VAR_PADDING*3];  /** padded storage */
#else               /* tfqueue */
  gint tail;        /** db offset to last queue node */
  gint queue_lock;  /** db offset to cache-aligned sync variable */
  gint storage;     /** db offset to queue node storage */
  gint max_nodes;   /** number of cells in queue node storage */
  gint freelist;    /** db offset to the top of the allocation stack */
#endif
} syn_var_area;


/** hash area header
*
*/

typedef struct _db_hash_area_header {
  gint size;           /** size of subarea */
  gint offset;         /** subarea exact offset from segment start: do not use for array! */
  gint arraysize;      /** subarea object alloc usable size: not necessarily to end of area */
  gint arraystart;     /** subarea start as to be used for object allocation */
  gint arraylength;    /** nr of elements in the hash array */
} db_hash_area_header;

/**
 * T-tree specific index header fields
 */
struct __wg_ttree_header {
  gint offset_root_node;
#ifdef TTREE_CHAINED_NODES
  gint offset_max_node;     /** last node in chain */
  gint offset_min_node;     /** first node in chain */
#endif
};

/**
 * Hash-specific index header fields
 */
struct __wg_hashidx_header {
  db_hash_area_header hasharea;
};


/** control data for one index
*
*/
typedef struct {
  gint type;
  gint fields;                            /** number of fields in index */
  gint rec_field_index[MAX_INDEX_FIELDS]; /** field numbers for this index */
  union {
    struct __wg_ttree_header t;
    struct __wg_hashidx_header h;
  } ctl;                    /** shared fields for different index types */
  gint template_offset;     /** matchrec template, 0 if full index */
} wg_index_header;


/** index mask meta-info
*
*/
#ifdef USE_INDEX_TEMPLATE
typedef struct {
  gint fixed_columns;       /** number of fixed columns in the template */
  gint offset_matchrec;     /** offset to the record that stores the fields */
  gint refcount;            /** number of indexes using this template */
} wg_index_template;
#endif


/** highest level index management data
*  contains lookup table by field number and memory management data
*/
typedef struct {
  gint number_of_indexes;       /** unused, reserved */
  gint index_list;              /** master index list */
  gint index_table[MAX_INDEXED_FIELDNR+1];    /** index lookup by column */
#ifdef USE_INDEX_TEMPLATE
  gint index_template_list;     /** sorted list of index masks */
  gint index_template_table[MAX_INDEXED_FIELDNR+1]; /** masks indexed by column */
#endif
} db_index_area_header;


/** Registered external databases
*   Offsets of data in these databases are recognized properly
*   by the data store/retrieve/compare functions.
*/
typedef struct {
  gint count; /** number of records */
  gint offset[MAX_EXTDB];   /** offsets of external databases */
  gint size[MAX_EXTDB];     /** corresponding sizes of external databases */
} extdb_area;


/** logging management
*
*/
typedef struct {
  gint active;          /** logging mode on/off */
  gint dirty;           /** log file is clean/dirty */
  gint serial;          /** incremented when the log file is backed up */
} db_logging_area_header;


/** bitmap area header
*
*/

typedef struct {
  gint offset; /** actual start of bitmap as used */
  gint size; /** actual used size in bytes */  
} db_recptr_bitmap_header;

/** anonconst area header
*
*/

#ifdef USE_REASONER
typedef struct _db_anonconst_area_header {
  gint anonconst_nr;
  gint anonconst_funs;
  gint anonconst_table[ANONCONST_TABLE_SIZE];
} db_anonconst_area_header;
#endif

/** located at the very beginning of the memory segment
*
*/

typedef struct _db_memsegment_header {
  // core info about segment
  /****** fixed size part of the header. Do not edit this without
   * also editing the code that checks the header in dbmem.c
   */
  gint32 mark;       /** fixed uncommon int to check if really a segment */
  gint32 version;    /** db engine version to check dump file compatibility */
  gint32 features;   /** db engine compile-time features */
  gint32 checksum;   /** dump file checksum */
  /* end of fixed size header ******/
  gint size;       /** segment size in bytes  */
  gint free;       /** pointer to first free area in segment (aligned) */
  gint initialadr; /** initial segment address, only valid for creator */
  gint key;        /** global shared mem key */
  // areas
  db_area_header datarec_area_header;
  db_area_header longstr_area_header;
  db_area_header listcell_area_header;
  db_area_header shortstr_area_header;
  db_area_header word_area_header;
  db_area_header doubleword_area_header;
  // hash structures
  db_hash_area_header strhash_area_header;
  // index structures
  db_index_area_header index_control_area_header;
  db_area_header tnode_area_header;
  db_area_header indexhdr_area_header;
  db_area_header indextmpl_area_header;
  db_area_header indexhash_area_header;
  // logging structures
  db_logging_area_header logging;
  // recptr bitmap
  db_recptr_bitmap_header recptr_bitmap;
  // anonconst table
#ifdef USE_REASONER
  db_anonconst_area_header anonconst;
#endif
  // statistics
  // field/table name structures
  syn_var_area locks;   /** currently holds a single global lock */
  extdb_area extdbs;    /** offset ranges of external databases */
} db_memsegment_header;

#ifdef USE_DATABASE_HANDLE
/** Database handle in local memory. Contains the pointer to the
*  shared memory area.
*/
typedef struct {
  db_memsegment_header *db; /** shared memory header */
  void *logdata;            /** log data structure in local memory */
} db_handle;
#endif

/* ---------  anonconsts: special uris with attached funs ----------- */

#ifdef USE_REASONER

#define ACONST_FALSE_STR "false"
#define ACONST_FALSE encode_anonconst(0)
#define ACONST_TRUE_STR "true"
#define ACONST_TRUE encode_anonconst(1)
#define ACONST_IF_STR "if"
#define ACONST_IF encode_anonconst(2)
#define ACONST_NOT_STR "not"
#define ACONST_NOT encode_anonconst(3)
#define ACONST_AND_STR "and"
#define ACONST_AND encode_anonconst(4)
#define ACONST_OR_STR "or"
#define ACONST_OR encode_anonconst(5)
#define ACONST_IMPLIES_STR "implies"
#define ACONST_IMPLIES encode_anonconst(6)
#define ACONST_XOR_STR "xor"
#define ACONST_XOR encode_anonconst(7)

#define ACONST_LESS_STR "<"
#define ACONST_LESS encode_anonconst(8)
#define ACONST_EQUAL_STR "="
#define ACONST_EQUAL encode_anonconst(9)
#define ACONST_GREATER_STR ">"
#define ACONST_GREATER encode_anonconst(10)
#define ACONST_LESSOREQUAL_STR "<="
#define ACONST_LESSOREQUAL encode_anonconst(11)
#define ACONST_GREATEROREQUAL_STR ">="
#define ACONST_GREATEROREQUAL encode_anonconst(12)
#define ACONST_ISZERO_STR "zero"
#define ACONST_ISZERO encode_anonconst(13)
#define ACONST_ISEMPTYSTR_STR "strempty"
#define ACONST_ISEMPTYSTR encode_anonconst(14)
#define ACONST_PLUS_STR "+"
#define ACONST_PLUS encode_anonconst(15)
#define ACONST_MINUS_STR "!-"
#define ACONST_MINUS encode_anonconst(16)
#define ACONST_MULTIPLY_STR "*"
#define ACONST_MULTIPLY encode_anonconst(17)
#define ACONST_DIVIDE_STR "/"
#define ACONST_DIVIDE encode_anonconst(18)
#define ACONST_STRCONTAINS_STR "strcontains"
#define ACONST_STRCONTAINS encode_anonconst(19)
#define ACONST_STRCONTAINSICASE_STR "strcontainsicase"
#define ACONST_STRCONTAINSICASE encode_anonconst(20)
#define ACONST_SUBSTR_STR "substr"
#define ACONST_SUBSTR encode_anonconst(21)
#define ACONST_STRLEN_STR "strlen"
#define ACONST_STRLEN encode_anonconst(22)

#endif

/* ==== Protos ==== */

gint wg_init_db_memsegment(void* db, gint key, gint size); // creates initial memory structures for a new db

gint wg_alloc_fixlen_object(void* db, void* area_header);
gint wg_alloc_gints(void* db, void* area_header, gint nr);

void wg_free_listcell(void* db, gint offset);
void wg_free_shortstr(void* db, gint offset);
void wg_free_word(void* db, gint offset);
void wg_free_doubleword(void* db, gint offset);
void wg_free_tnode(void* db, gint offset);
void wg_free_fixlen_object(void* db, db_area_header *hdr, gint offset);

gint wg_freebuckets_index(void* db, gint size);
gint wg_free_object(void* db, void* area_header, gint object) ;

#if 0
void *wg_create_child_db(void* db, gint size);
#endif
gint wg_register_external_db(void *db, void *extdb);
gint wg_create_hash(void *db, db_hash_area_header* areah, gint size);

gint wg_database_freesize(void *db);
gint wg_database_size(void *db);

/* ------- testing ------------ */

#endif /* DEFINED_DBALLOC_H */
