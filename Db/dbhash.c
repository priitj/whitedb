/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
* Copyright (c) Priit Järv 2013,2014
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

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dbhash.h"
#include "dbdata.h"
#include "dbmpool.h"


/* ====== Private headers and defs ======== */

/* Bucket capacity > 1 reduces the impact of collisions */
#define GINTHASH_BUCKETCAP 7

/* Level 24 hash consumes approx 640MB with bucket capacity 3 on 32-bit
 * architecture and about twice as much on 64-bit systems. With bucket
 * size increased to 7 (which is more space efficient due to imperfect
 * hash distribution) we can reduce the level by 1 for the same space
 * requirements.
 */
#define GINTHASH_MAXLEVEL 23

/* rehash keys (useful for lowering the impact of bad distribution) */
#define GINTHASH_SCRAMBLE(v) (rehash_gint(v))
/*#define GINTHASH_SCRAMBLE(v) (v)*/

typedef struct {
  gint level;                         /* local level */
  gint fill;                          /* slots filled / next slot index */
  gint key[GINTHASH_BUCKETCAP + 1];   /* includes one overflow slot */
  gint value[GINTHASH_BUCKETCAP + 1];
} ginthash_bucket;

/* Dynamic local memory hashtable for gint key/value pairs. Resize
 * is handled using the extendible hashing algorithm.
 * Note: we don't use 0-level hash, so buckets[0] is unused.
 */
typedef struct {
  gint level;                  /* global level */
  ginthash_bucket **directory; /* bucket pointers, contiguous memory */
  void *mpool;                 /* dbmpool storage */
} ext_ginthash;

/* Static local memory hash table for existence tests (double hashing) */
typedef struct {
  size_t dhash_size;
  gint *keys;
} dhash_table;

#ifdef HAVE_64BIT_GINT
#define FNV_offset_basis ((wg_uint) 14695981039346656037ULL)
#define FNV_prime ((wg_uint) 1099511628211ULL)
#else
#define FNV_offset_basis ((wg_uint) 2166136261UL)
#define FNV_prime ((wg_uint) 16777619UL)
#endif

/* ======= Private protos ================ */



// static gint show_consistency_error(void* db, char* errmsg);
static gint show_consistency_error_nr(void* db, char* errmsg, gint nr) ;
// static gint show_consistency_error_double(void* db, char* errmsg, double nr);
// static gint show_consistency_error_str(void* db, char* errmsg, char* str);
static gint show_hash_error(void* db, char* errmsg);
static gint show_ginthash_error(void *db, char* errmsg);

static wg_uint hash_bytes(void *db, char *data, gint length, gint hashsz);
static gint find_idxhash_bucket(void *db, char *data, gint length,
  gint *chainoffset);

static gint rehash_gint(gint val);
static gint grow_ginthash(void *db, ext_ginthash *tbl);
static ginthash_bucket *ginthash_newbucket(void *db, ext_ginthash *tbl);
static ginthash_bucket *ginthash_splitbucket(void *db, ext_ginthash *tbl,
  ginthash_bucket *bucket);
static gint add_to_bucket(ginthash_bucket *bucket, gint key, gint value);
static gint remove_from_bucket(ginthash_bucket *bucket, int idx);

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

  //printf("in wg_hash_typedstr %s %s %d %d \n",data,extrastr,type,length);
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

  return (int)(hash % (dbmemsegh(db)->strhash_area_header).arraylength);
}



/* Find longstr from strhash bucket chain
*
*
*/

gint wg_find_strhash_bucket(void* db, char* data, char* extrastr, gint type, gint size, gint hashchain) {
  //printf("wg_find_strhash_bucket called %s %s type %d size %d hashchain %d\n",data,extrastr,type,size,hashchain);
  for(;hashchain!=0;
      hashchain=dbfetch(db,decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint))) {
    if (wg_right_strhash_bucket(db,hashchain,data,extrastr,type,size)) {
      // found equal longstr, return it
      //printf("wg_find_strhash_bucket found hashchain %d\n",hashchain);
      return hashchain;
    }
  }
  return 0;
}

/* Check whether longstr hash bucket matches given new str
*
*
*/

int wg_right_strhash_bucket
            (void* db, gint longstr, char* cstr, char* cextrastr, gint ctype, gint cstrsize) {
  char* str;
  char* extrastr;
  int strsize;
  gint type;
  //printf("wg_right_strhash_bucket called with %s %s type %d size %d\n",
  //              cstr,cextrastr,ctype,cstrsize);
  type=wg_get_encoded_type(db,longstr);
  if (type!=ctype) return 0;
  strsize=wg_decode_str_len(db,longstr)+1;
  if (strsize!=cstrsize) return 0;
  str=wg_decode_str(db,longstr);
  if ((cstr==NULL && str!=NULL) || (cstr!=NULL && str==NULL)) return 0;
  if ((cstr!=NULL) && (memcmp(str,cstr,cstrsize))) return 0;
  extrastr=wg_decode_str_lang(db,longstr);
  if ((cextrastr==NULL && extrastr!=NULL) || (cextrastr!=NULL && extrastr==NULL)) return 0;
  if ((cextrastr!=NULL) && (strcmp(extrastr,cextrastr))) return 0;
  return 1;
}

/* Remove longstr from strhash
*
*  Internal langstr etc are not removed by this op.
*
*/

gint wg_remove_from_strhash(void* db, gint longstr) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint type;
  gint* extrastrptr;
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

  //printf("wg_remove_from_strhash called on %d\n",longstr);
  //wg_debug_print_value(db,longstr);
  //printf("\n\n");
  offset=decode_longstr_offset(longstr);
  objptr=(gint*) offsettoptr(db,offset);
  // get string data elements
  //type=objptr=offsettoptr(db,decode_longstr_offset(data));
  extrastrptr=(gint *) (((char*)(objptr))+(LONGSTR_EXTRASTR_POS*sizeof(gint)));
  fldval=*extrastrptr;
  if (fldval==0) extrastr=NULL;
  else extrastr=wg_decode_str(db,fldval);
  data=((char*)(objptr))+(LONGSTR_HEADER_GINTS*sizeof(gint));
  objsize=getusedobjectsize(*objptr);
  strsize=objsize-(((*(objptr+LONGSTR_META_POS))&LONGSTR_META_LENDIFMASK)>>LONGSTR_META_LENDIFSHFT);
  length=strsize;
  typeptr=(gint*)(((char*)(objptr))+(+LONGSTR_META_POS*sizeof(gint)));
  type=(*typeptr)&LONGSTR_META_TYPEMASK;
  //type=wg_get_encoded_type(db,longstr);
  // get hash of data elements and find the location in hashtable/chains
  hash=wg_hash_typedstr(db,data,extrastr,type,length);
  chainoffset=((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash);
  hashchain=dbfetch(db,chainoffset);
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


/* -------------- hash index support ------------------ */

#define CONCAT_FOR_HASHING(d, b, e, l, bb, en) \
  if(e) { \
    gint xl = wg_decode_xmlliteral_xsdtype_len(d, en); \
    bb = malloc(xl + l + 1); \
    if(!bb) \
      return 0; \
    memcpy(bb, e, xl); \
    bb[xl] = '\0'; \
    memcpy(bb + xl + 1, b, l); \
    b = bb; \
    l += xl + 1; \
  }

/*
 * Return an encoded value as a decoded byte array.
 * It should be freed afterwards.
 * returns the number of bytes in the array.
 * returns 0 if the decode failed.
 *
 * NOTE: to differentiate between identical byte strings
 * the value is prefixed with a type identifier.
 * TODO: For values with varying length that can contain
 * '\0' bytes, add length to the prefix.
 */
gint wg_decode_for_hashing(void *db, gint enc, char **decbytes) {
  gint len;
  gint type;
  gint ptrdata;
  int intdata;
  double doubledata;
  char *bytedata;
  char *exdata, *buf = NULL, *outbuf;

  type = wg_get_encoded_type(db, enc);
  switch(type) {
    case WG_NULLTYPE:
      len = sizeof(gint);
      ptrdata = 0;
      bytedata = (char *) &ptrdata;
      break;
    case WG_RECORDTYPE:
      len = sizeof(gint);
      ptrdata = enc;
      bytedata = (char *) &ptrdata;
      break;
    case WG_INTTYPE:
      len = sizeof(int);
      intdata = wg_decode_int(db, enc);
      bytedata = (char *) &intdata;
      break;
    case WG_DOUBLETYPE:
      len = sizeof(double);
      doubledata = wg_decode_double(db, enc);
      bytedata = (char *) &doubledata;
      break;
    case WG_FIXPOINTTYPE:
      len = sizeof(double);
      doubledata = wg_decode_fixpoint(db, enc);
      bytedata = (char *) &doubledata;
      break;
    case WG_STRTYPE:
      len = wg_decode_str_len(db, enc);
      bytedata = wg_decode_str(db, enc);
      break;
    case WG_URITYPE:
      len = wg_decode_uri_len(db, enc);
      bytedata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      CONCAT_FOR_HASHING(db, bytedata, exdata, len, buf, enc)
      break;
    case WG_XMLLITERALTYPE:
      len = wg_decode_xmlliteral_len(db, enc);
      bytedata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      CONCAT_FOR_HASHING(db, bytedata, exdata, len, buf, enc)
      break;
    case WG_CHARTYPE:
      len = sizeof(int);
      intdata = wg_decode_char(db, enc);
      bytedata = (char *) &intdata;
      break;
    case WG_DATETYPE:
      len = sizeof(int);
      intdata = wg_decode_date(db, enc);
      bytedata = (char *) &intdata;
      break;
    case WG_TIMETYPE:
      len = sizeof(int);
      intdata = wg_decode_time(db, enc);
      bytedata = (char *) &intdata;
      break;
    case WG_VARTYPE:
      len = sizeof(int);
      intdata = wg_decode_var(db, enc);
      bytedata = (char *) &intdata;
      break;
    case WG_ANONCONSTTYPE:
      /* Ignore anonconst */
    default:
      return 0;
  }

  /* Form the hashable buffer. It is not 0-terminated */
  outbuf = malloc(len + 1);
  if(outbuf) {
    outbuf[0] = (char) type;
    memcpy(outbuf + 1, bytedata, len++);
    *decbytes = outbuf;
  } else {
    /* Indicate failure */
    len = 0;
  }

  if(buf)
    free(buf);
  return len;
}

/*
 * Calculate a hash for a byte buffer. Truncates the hash to given size.
 */
static wg_uint hash_bytes(void *db, char *data, gint length, gint hashsz) {
  char* endp;
  wg_uint hash = 0;

  if (data!=NULL) {
    for(endp=data+length; data<endp; data++) {
      hash = *data + (hash << 6) + (hash << 16) - hash;
    }
  }
  return hash % hashsz;
}

/*
 * Finds a matching bucket in hash chain.
 * chainoffset should point to the offset storing the chain head.
 * If the call is successful, it will point to the offset storing
 * the matching bucket.
 */
static gint find_idxhash_bucket(void *db, char *data, gint length,
  gint *chainoffset)
{
  gint bucket = dbfetch(db, *chainoffset);
  while(bucket) {
    gint meta = dbfetch(db, bucket + HASHIDX_META_POS*sizeof(gint));
    if(meta == length) {
      /* Currently, meta stores just size */
      char *bucket_data = offsettoptr(db, bucket + \
        HASHIDX_HEADER_SIZE*sizeof(gint));
      if(!memcmp(bucket_data, data, length))
        return bucket;
    }
    *chainoffset = bucket + HASHIDX_HASHCHAIN_POS*sizeof(gint);
    bucket = dbfetch(db, *chainoffset);
  }
  return 0;
}

/*
 * Store a hash string and an offset to the index hash.
 * Based on longstr hash, with some simplifications.
 *
 * Returns 0 on success
 * Returns -1 on error.
 */
gint wg_idxhash_store(void* db, db_hash_area_header *ha,
  char* data, gint length, gint offset)
{
  db_memsegment_header* dbh = dbmemsegh(db);
  wg_uint hash;
  gint head_offset, head, bucket;
  gint rec_head, rec_offset;
  gcell *rec_cell;

  hash = hash_bytes(db, data, length, ha->arraylength);
  head_offset = (ha->arraystart)+(sizeof(gint) * hash);
  head = dbfetch(db, head_offset);

  /* Traverse the hash chain to check if there is a matching
   * hash string already
   */
  bucket = find_idxhash_bucket(db, data, length, &head_offset);
  if(!bucket) {
    size_t i;
    gint lengints, lenrest;
    char* dptr;

    /* Make a new bucket */
    lengints = length / sizeof(gint);
    lenrest = length % sizeof(gint);
    if(lenrest) lengints++;
    bucket = wg_alloc_gints(db,
         &(dbh->indexhash_area_header),
        lengints + HASHIDX_HEADER_SIZE);
    if(!bucket) {
      return -1;
    }

    /* Copy the byte data */
    dptr = (char *) (offsettoptr(db,
      bucket + HASHIDX_HEADER_SIZE*sizeof(gint)));
    memcpy(dptr, data, length);
    for(i=0;lenrest && i<sizeof(gint)-lenrest;i++) {
      *(dptr + length + i)=0; /* XXX: since we have the length, in meta,
                               * this is possibly unnecessary. */
    }

    /* Metadata */
    dbstore(db, bucket + HASHIDX_META_POS*sizeof(gint), length);
    dbstore(db, bucket + HASHIDX_RECLIST_POS*sizeof(gint), 0);

    /* Prepend to hash chain */
    dbstore(db, ((ha->arraystart)+(sizeof(gint) * hash)), bucket);
    dbstore(db, bucket + HASHIDX_HASHCHAIN_POS*sizeof(gint), head);
  }

  /* Add the record offset to the list. */
  rec_head = dbfetch(db, bucket + HASHIDX_RECLIST_POS*sizeof(gint));
  rec_offset = wg_alloc_fixlen_object(db, &(dbh->listcell_area_header));
  rec_cell = (gcell *) offsettoptr(db, rec_offset);
  rec_cell->car = offset;
  rec_cell->cdr = rec_head;
  dbstore(db, bucket + HASHIDX_RECLIST_POS*sizeof(gint), rec_offset);

  return 0;
}

/*
 * Remove an offset from the index hash.
 *
 * Returns 0 on success
 * Returns -1 on error.
 */
gint wg_idxhash_remove(void* db, db_hash_area_header *ha,
  char* data, gint length, gint offset)
{
  wg_uint hash;
  gint bucket_offset, bucket;
  gint *next_offset, *reclist_offset;

  hash = hash_bytes(db, data, length, ha->arraylength);
  bucket_offset = (ha->arraystart)+(sizeof(gint) * hash); /* points to head */

  /* Find the correct bucket. */
  bucket = find_idxhash_bucket(db, data, length, &bucket_offset);
  if(!bucket) {
    return show_hash_error(db, "wg_idxhash_remove: Hash value not found.");
  }

  /* Remove the record offset from the list. */
  reclist_offset = offsettoptr(db, bucket + HASHIDX_RECLIST_POS*sizeof(gint));
  next_offset = reclist_offset;
  while(*next_offset) {
    gcell *rec_cell = (gcell *) offsettoptr(db, *next_offset);
    if(rec_cell->car == offset) {
      gint rec_offset = *next_offset;
      *next_offset = rec_cell->cdr; /* remove from list chain */
      wg_free_listcell(db, rec_offset); /* free storage */
      goto is_bucket_empty;
    }
    next_offset = &(rec_cell->cdr);
  }
  return show_hash_error(db, "wg_idxhash_remove: Offset not found");

is_bucket_empty:
  if(!(*reclist_offset)) {
    gint nextchain = dbfetch(db, bucket + HASHIDX_HASHCHAIN_POS*sizeof(gint));
    dbstore(db, bucket_offset, nextchain);
    wg_free_object(db, &(dbmemsegh(db)->indexhash_area_header), bucket);
  }

  return 0;
}

/*
 * Retrieve the list of matching offsets from the hash.
 *
 * Returns the offset to head of the linked list.
 * Returns 0 if value was not found.
 */
gint wg_idxhash_find(void* db, db_hash_area_header *ha,
  char* data, gint length)
{
  wg_uint hash;
  gint head_offset, bucket;

  hash = hash_bytes(db, data, length, ha->arraylength);
  head_offset = (ha->arraystart)+(sizeof(gint) * hash); /* points to head */

  /* Find the correct bucket. */
  bucket = find_idxhash_bucket(db, data, length, &head_offset);
  if(!bucket)
    return 0;

  return dbfetch(db, bucket + HASHIDX_RECLIST_POS*sizeof(gint));
}

/* ------- local-memory extendible gint hash ---------- */

/*
 * Dynamically growing gint hash.
 *
 * Implemented in local memory for temporary usage (database memory is not well
 * suited as it is not resizable). Uses the extendible hashing algorithm
 * proposed by Fagin et al '79 as this allows the use of simple, easily
 * disposable data structures.
 */

/** Initialize the hash table.
 *  The initial hash level is 1.
 *  returns NULL on failure.
 */
void *wg_ginthash_init(void *db) {
  ext_ginthash *tbl = malloc(sizeof(ext_ginthash));
  if(!tbl) {
    show_ginthash_error(db, "Failed to allocate table.");
    return NULL;
  }

  memset(tbl, 0, sizeof(ext_ginthash));
  if(grow_ginthash(db, tbl)) { /* initial level is set to 1 */
    free(tbl);
    return NULL;
  }
  return tbl;
}

/** Add a key/value pair to the hash table.
 *  tbl should be created with wg_ginthash_init()
 *  Returns 0 on success
 *  Returns -1 on failure
 */
gint wg_ginthash_addkey(void *db, void *tbl, gint key, gint val) {
  size_t dirsize = 1<<((ext_ginthash *)tbl)->level;
  size_t hash = GINTHASH_SCRAMBLE(key) & (dirsize - 1);
  ginthash_bucket *bucket = ((ext_ginthash *)tbl)->directory[hash];
  /*static gint keys = 0;*/
  /* printf("add: %d hash %d items %d\n", key, hash, ++keys); */
  if(!bucket) {
    /* allocate a new bucket, store value, we're done */
    bucket = ginthash_newbucket(db, (ext_ginthash *) tbl);
    if(!bucket)
      return -1;
    bucket->level = ((ext_ginthash *) tbl)->level;
    add_to_bucket(bucket, key, val); /* Always fits, no check needed */
    ((ext_ginthash *)tbl)->directory[hash] = bucket;
  }
  else {
    add_to_bucket(bucket, key, val);
    while(bucket->fill > GINTHASH_BUCKETCAP) {
      ginthash_bucket *newb;
      /* Overflow, bucket split needed. */
      if(!(newb = ginthash_splitbucket(db, (ext_ginthash *)tbl, bucket)))
        return -1;
      /* Did everything flow to the new bucket, causing another overflow? */
      if(newb->fill > GINTHASH_BUCKETCAP) {
        bucket = newb; /* Keep splitting */
      }
    }
  }
  return 0;
}

/** Fetch a value from the hash table.
 *  If the value is not found, returns -1 (val is unmodified).
 *  Otherwise returns 0; contents of val is replaced with the
 *  value from the hash table.
 */
gint wg_ginthash_getkey(void *db, void *tbl, gint key, gint *val) {
  size_t dirsize = 1<<((ext_ginthash *)tbl)->level;
  size_t hash = GINTHASH_SCRAMBLE(key) & (dirsize - 1);
  ginthash_bucket *bucket = ((ext_ginthash *)tbl)->directory[hash];
  if(bucket) {
    int i;
    for(i=0; i<bucket->fill; i++) {
      if(bucket->key[i] == key) {
        *val = bucket->value[i];
        return 0;
      }
    }
  }
  return -1;
}

/** Release all memory allocated for the hash table.
 *
 */
void wg_ginthash_free(void *db, void *tbl) {
  if(tbl) {
    if(((ext_ginthash *) tbl)->directory)
      free(((ext_ginthash *) tbl)->directory);
    if(((ext_ginthash *) tbl)->mpool)
      wg_free_mpool(db, ((ext_ginthash *) tbl)->mpool);
    free(tbl);
  }
}

/** Scramble a gint value
 *  This is useful when dealing with aligned offsets, that are
 *  multiples of 4, 8 or larger values and thus waste the majority
 *  of the directory space when used directly.
 *  Uses FNV-1a.
 */
static gint rehash_gint(gint val) {
  int i;
  wg_uint hash = FNV_offset_basis;

  for(i=0; i<sizeof(gint); i++) {
    hash ^= ((unsigned char *) &val)[i];
    hash *= FNV_prime;
  }
  return (gint) hash;
}

/** Grow the hash directory and allocate a new bucket pool.
 *
 */
static gint grow_ginthash(void *db, ext_ginthash *tbl) {
  void *tmp;
  gint newlevel = tbl->level + 1;
  if(newlevel >= GINTHASH_MAXLEVEL)
    return show_ginthash_error(db, "Maximum level exceeded.");

  if((tmp = realloc((void *) tbl->directory,
    (1<<newlevel) * sizeof(ginthash_bucket *)))) {
    tbl->directory = (ginthash_bucket **) tmp;

    if(tbl->level) {
      size_t i;
      size_t dirsize = 1<<tbl->level;
      /* duplicate the existing pointers. */
      for(i=0; i<dirsize; i++)
        tbl->directory[dirsize + i] = tbl->directory[i];
    } else {
      /* Initialize the memory pool (2 buckets) */
      if((tmp = wg_create_mpool(db, 2*sizeof(ginthash_bucket)))) {
        tbl->mpool = tmp;
        /* initial directory is empty */
        memset(tbl->directory, 0, 2*sizeof(ginthash_bucket *));
      } else {
        return show_ginthash_error(db, "Failed to allocate bucket pool.");
      }
    }
  } else {
    return show_ginthash_error(db, "Failed to reallocate directory.");
  }
  tbl->level = newlevel;
  return 0;
}

/** Allocate a new bucket.
 *
 */
static ginthash_bucket *ginthash_newbucket(void *db, ext_ginthash *tbl) {
  ginthash_bucket *bucket = (ginthash_bucket *) \
    wg_alloc_mpool(db, tbl->mpool, sizeof(ginthash_bucket));
  if(bucket) {
    /* bucket->level = tbl->level; */
    bucket->fill = 0;
  }
  return bucket;
}

/** Split a bucket.
 *  Returns the newly created bucket on success
 *  Returns NULL on failure (likely cause being out of memory)
 */
static ginthash_bucket *ginthash_splitbucket(void *db, ext_ginthash *tbl,
  ginthash_bucket *bucket)
{
  gint msbmask, lowbits;
  int i;
  ginthash_bucket *newbucket;

  if(bucket->level == tbl->level) {
    /* can't split at this level anymore, extend directory */
    /*printf("grow: curr level %d\n", tbl->level);*/
    if(grow_ginthash(db, (ext_ginthash *) tbl))
      return NULL;
  }

  /* Hash values for the new level (0+lowbits, msb+lowbits) */
  msbmask = (1<<(bucket->level++));
  lowbits = GINTHASH_SCRAMBLE(bucket->key[0]) & (msbmask - 1);

  /* Create a bucket to split into */
  newbucket = ginthash_newbucket(db, tbl);
  if(!newbucket)
    return NULL;
  newbucket->level = bucket->level;

  /* Split the entries based on the most significant
   * bit for the local level hash (the ones with msb set are relocated)
   */
  for(i=bucket->fill-1; i>=0; i--) {
    gint k_i = bucket->key[i];
    if(GINTHASH_SCRAMBLE(k_i) & msbmask) {
      add_to_bucket(newbucket, k_i, remove_from_bucket(bucket, i));
      /* printf("reassign: %d hash %d --> %d\n",
        k_i, lowbits, msbmask | lowbits); */
    }
  }

  /* Update the directory */
  if(bucket->level == tbl->level) {
    /* There are just two pointers pointing to bucket,
     * we can compute the location of the one that has the index
     * with msb set. The other one's contents do not need to be
     * modified.
     */
    tbl->directory[msbmask | lowbits] = newbucket;
  } else {
    /* The pointers that need to be updated have indexes
     * of xxx1yyyy where 1 is the msb in the index of the new
     * bucket, yyyy is the hash value of the bucket masked
     * by the previous level and xxx are all combinations of
     * bits that still remain masked by the local level after
     * the split. The pointers xxx0yyyy will remain pointing
     * to the old bucket.
     */
    size_t msbbuckets = 1<<(tbl->level - bucket->level), j;
    for(j=0; j<msbbuckets; j++) {
      size_t k = (j<<bucket->level) | msbmask | lowbits;
      /* XXX: paranoia check, remove in production */
      if(tbl->directory[k] != bucket)
        return NULL;
      tbl->directory[k] = newbucket;
    }
  }
  return newbucket;
}

/** Add a key/value pair to bucket.
 *  Returns bucket fill.
 */
static gint add_to_bucket(ginthash_bucket *bucket, gint key, gint value) {
#ifdef CHECK
  if(bucket->fill > GINTHASH_BUCKETCAP) { /* Should never happen */
    return bucket->fill + 1;
  } else {
#endif
    bucket->key[bucket->fill] = key;
    bucket->value[bucket->fill] = value;
    return ++(bucket->fill);
#ifdef CHECK
  }
#endif
}

/** Remove an indexed value from bucket.
 *  Returns the value.
 */
static gint remove_from_bucket(ginthash_bucket *bucket, int idx) {
  int i;
  gint val = bucket->value[idx];
  for(i=idx; i<GINTHASH_BUCKETCAP; i++) {
    /* Note we ignore the last slot. Generally keys/values
     * in slots indexed >=bucket->fill are always undefined
     * and shouldn't be accessed directly.
     */
    bucket->key[i] = bucket->key[i+1];
    bucket->value[i] = bucket->value[i+1];
  }
  bucket->fill--;
  return val;
}

/* ------- set membership hash (double hashing)  --------- */

/*
 * Compute a suitable hash table size for the known number of
 * entries. Returns 0 if the size is not supported.
 * Max hash table size is 63GB (~2G entries on 64-bit), this can
 * be extended by adding more primes.
 * Size is chosen so that the table load would be < 0.5
 */
static size_t dhash_size(size_t entries) {
  /* List of primes lifted from stlport
   * (http://sourceforge.net/projects/stlport/) */
  size_t primes[] = {
    389UL, 769UL, 1543UL, 3079UL, 6151UL,
    12289UL, 24593UL, 49157UL, 98317UL, 196613UL,
    393241UL, 786433UL, 1572869UL, 3145739UL, 6291469UL,
    12582917UL, 25165843UL, 50331653UL, 100663319UL, 201326611UL,
    402653189UL, 805306457UL, 1610612741UL, 3221225473UL, 4294967291UL
  };
  size_t const p_count = 20;
  size_t wantsize = entries<<1, i;
  if(entries > 2147483645UL) {
    return 0; /* give up here for now */
  }
  for(i=0; i<p_count-1; i++) {
    if(primes[i] > wantsize) {
      break;
    }
  }
  return primes[i];
}

#define DHASH_H1(k, sz) ((k) % (sz))
#define DHASH_H2(k, sz) (1 + ((k) % ((sz)-1)))
#define DHASH_PROBE(h1, h2, i, sz) (((h1) + (i)*(h2)) % sz)

/*
 * Find a slot matching the key.
 * Always returns a slot. Interpreting the results:
 * *b == 0 --> key not present in table, slot may be used to store it
 * *b == key --> key found
 * otherwise --> hash table full
 */
static gint *dhash_lookup(dhash_table *tbl, gint key) {
  gint h = rehash_gint(key);
  size_t sz = tbl->dhash_size;
  size_t h1 = DHASH_H1(h, sz), h2;
  size_t i;
  gint *bb = tbl->keys, *b = bb + h1;

  if(*b == key || *b == 0)
    return b;

  h2 = DHASH_H2(h, sz);
  for(i=1; i<sz; i++) {
    b = bb + DHASH_PROBE(h1, h2, i, sz);
    if(*b == key || *b == 0)
      break;
  }
  return b;
}

/*
 * Creates the hash table for the given number of entries.
 * The returned hash table should be treated as an opaque pointer
 * of type (void *). Returns NULL if memory allocation fails.
 * wg_dhash_free() should be called to free the structure after use.
 */
void *wg_dhash_init(void *db, size_t entries) {
  dhash_table *tbl = malloc(sizeof(dhash_table));
  if(tbl) {
    tbl->dhash_size = dhash_size(entries);
    tbl->keys = calloc(tbl->dhash_size, sizeof(gint)); /* set to 0x0 */
    if(!tbl->keys || !tbl->dhash_size) {
      free(tbl);
      tbl = NULL;
    }
  }
  return (void *) tbl;
}

/*
 * Free the structure created by wg_dhash_init()
 */
void wg_dhash_free(void *db, void *tbl) {
  if(tbl) {
    if(((dhash_table *) tbl)->keys)
      free(((dhash_table *) tbl)->keys);
    free(tbl);
  }
}

/*
 * Add an entry to the hash table.
 * returns 0 on success (including when they key is already present).
 * returns -1 on failure.
 */
gint wg_dhash_addkey(void *db, void *tbl, gint key) {
  gint *b = dhash_lookup((dhash_table *) tbl, key);
  if(*b == 0) {
    *b = key; /* key not present, free slot found, add the key */
  } else if (*b != key) {
    return -1; /* key not present and no free slot */
  }
  return 0;
}

/*
 * Find a key in the hash table.
 * Returns 1 if key is present.
 * Returns 0 if key is not present.
 */
gint wg_dhash_haskey(void *db, void *tbl, gint key) {
  gint *b = dhash_lookup((dhash_table *) tbl, key);
  return (*b == key);
}

/* -------------    error handling  ------------------- */

/*

static gint show_consistency_error(void* db, char* errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg consistency error: %s\n",errmsg);
#endif
  return -1;
}
*/

static gint show_consistency_error_nr(void* db, char* errmsg, gint nr) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg consistency error: %s %d\n", errmsg, (int) nr);
  return -1;
#endif
}

/*
static gint show_consistency_error_double(void* db, char* errmsg, double nr) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg consistency error: %s %f\n",errmsg,nr);
#endif
  return -1;
}

static gint show_consistency_error_str(void* db, char* errmsg, char* str) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg consistency error: %s %s\n",errmsg,str);
#endif
  return -1;
}
*/

static gint show_hash_error(void* db, char* errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg hash error: %s\n",errmsg);
#endif
  return -1;
}

static gint show_ginthash_error(void *db, char* errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg gint hash error: %s\n", errmsg);
#endif
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

#ifdef __cplusplus
}
#endif
