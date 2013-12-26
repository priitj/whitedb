/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
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

 /** @file dbmpool.c
 *  Allocating data using a temporary memory pool.
 *
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/shm.h>
#include <sys/errno.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbmem.h"
#include "dbapi.h"

/* ====== Private headers and defs ======== */

#define NROF_SUBAREAS 100           // size of subarea array
#define MIN_FIRST_SUBAREA_SIZE 1024 // first free area minimum: if less asked, this given
#define ALIGNMENT_BYTES 4           // every val returned by wg_alloc_mpool is aligned to this

#define TYPEMASK 1    // memory pool convenience objects type mask for address
#define PAIRBITS 0    // memory pool convenience objects type bit for pairs (lists)
#define ATOMBITS 1    // memory pool convenience objects type bit for atoms (strings etc)


/** located inside mpool_header: one single memory subarea header
*
*
*/

typedef struct _wg_mpoolsubarea_header {
  int size;           /** size of subarea in bytes */
  void* area_start;   /** pointer to the first byte of the subarea */
  void* area_end;     /** pointer to the first byte after the subarea */
} mpool_subarea_header;


/** memory pool management data
*  stored in the beginning of the first segment of mempool
*
*/

typedef struct {
  void* freeptr;     /** pointer to the next free location in the pool */
  int cur_subarea;   /** index of the currently used subarea in the subarea_table (starts with 0) */
  int nrof_subareas; /** full nr of rows in the subarea table */
  mpool_subarea_header subarea_table[NROF_SUBAREAS];    /** subarea information (mpool_subarea_header) table */
} mpool_header;


/* ======= Private protos ================ */

static int extend_mpool(void* db, void* mpool, int minbytes);
static int show_mpool_error(void* db, char* errmsg);
static int show_mpool_error_nr(void* db, char* errmsg, int nr);
static void wg_mpool_print_aux(void* db, void* ptr, int depth, int pflag);

/* ====== Functions for mpool creating/extending/allocating/destroying ============== */


/** create and initialise a new memory pool
*
* initial pool has at least origbytes of free space
* mpool is extended automatically later when space used up
* returns void* pointer to mpool if OK, NULL if failure
*
* does a single malloc (latex extensions do further mallocs)
*/



void* wg_create_mpool(void* db, int origbytes) {
  int bytes;
  void* mpool;
  mpool_header* mpoolh;
  int puresize;
  void* nextptr;
  int i;

  if (origbytes<MIN_FIRST_SUBAREA_SIZE+ALIGNMENT_BYTES)
    bytes=sizeof(mpool_header)+MIN_FIRST_SUBAREA_SIZE+ALIGNMENT_BYTES;
  else
    bytes=sizeof(mpool_header)+origbytes+ALIGNMENT_BYTES;
  puresize=bytes-sizeof(mpool_header);
  mpool=malloc(bytes);
  if (mpool==NULL) {
    show_mpool_error_nr(db,
      " cannot create an mpool with size: ",origbytes);
    return NULL;
  }
  mpoolh=(mpool_header*)mpool;
  nextptr=(void*)(((char*)mpool)+sizeof(mpool_header));
  // set correct alignment for nextptr
  i=((size_t)nextptr)%ALIGNMENT_BYTES;
  if (i!=0) nextptr=((char*)nextptr)+(ALIGNMENT_BYTES-i);
  // aligment now ok
  (mpoolh->freeptr)=nextptr;
  (mpoolh->cur_subarea)=0;
  ((mpoolh->subarea_table)[0]).size=puresize;
  ((mpoolh->subarea_table)[0]).area_start=mpool;
  ((mpoolh->subarea_table)[0]).area_end=(void*)(((char*)mpool)+bytes);
  return mpool;
}


/** extend an existing memory pool
*
* called automatically when mpool space used up
* does one malloc for a new subarea
*
*/


static int extend_mpool(void* db, void* mpool, int minbytes) {
  int cursize;
  int bytes;
  void* subarea;
  mpool_header* mpoolh;
  int i;
  void* nextptr;

  mpoolh=(mpool_header*)mpool;
  cursize=((mpoolh->subarea_table)[(mpoolh->cur_subarea)]).size;
  bytes=cursize;
  for(i=0;i<100;i++) {
    bytes=bytes*2;
    if (bytes>=(minbytes+ALIGNMENT_BYTES)) break;
  }
  subarea=malloc(bytes);
  if (subarea==NULL) {
    show_mpool_error_nr(db,
      " cannot extend mpool to size: ",minbytes);
    return -1;
  }
  (mpoolh->freeptr)=subarea;
  (mpoolh->cur_subarea)++;
  ((mpoolh->subarea_table)[mpoolh->cur_subarea]).size=bytes;
  nextptr=subarea;
  // set correct alignment for nextptr
  i=((size_t)nextptr)%ALIGNMENT_BYTES;
  if (i!=0) nextptr=((char*)nextptr)+(ALIGNMENT_BYTES-i);
  // aligment now ok
  (mpoolh->freeptr)=nextptr;
  ((mpoolh->subarea_table)[mpoolh->cur_subarea]).area_start=subarea;
  ((mpoolh->subarea_table)[mpoolh->cur_subarea]).area_end=(void*)(((char*)subarea)+bytes);
  return 0;
}

/** free the whole memory pool
*
* frees all the malloced subareas and initial mpool
*
*/

void wg_free_mpool(void* db, void* mpool) {
  int i;
  mpool_header* mpoolh;

  mpoolh=(mpool_header*)mpool;
  i=mpoolh->cur_subarea;
  for(;i>0;i--) {
    free(((mpoolh->subarea_table)[i]).area_start);
  }
  free(mpool);
}

/** allocate bytes from a memory pool: analogous to malloc
*
* mpool is extended automatically if not enough free space present
* returns void* pointer to a memory block if OK, NULL if failure
*
*/

void* wg_alloc_mpool(void* db, void* mpool, int bytes) {
  void* curptr;
  void* nextptr;
  mpool_header* mpoolh;
  void* curend;
  int tmp;
  int i;

  if (bytes<=0) {
    show_mpool_error_nr(db,
      " trying to allocate too little from mpool: ",bytes);
    return NULL;
  }
  if (mpool==NULL) {
    show_mpool_error(db," mpool passed to wg_alloc_mpool is NULL ");
    return NULL;
  }
  mpoolh=(mpool_header*)mpool;
  nextptr=(void*)(((char*)(mpoolh->freeptr))+bytes);
  curend=((mpoolh->subarea_table)[(mpoolh->cur_subarea)]).area_end;
  if (nextptr>curend) {
    tmp=extend_mpool(db,mpool,bytes);
    if (tmp!=0) {
      show_mpool_error_nr(db," cannot extend mpool size by: ",bytes);
      return NULL;
    }
    nextptr=((char*)(mpoolh->freeptr))+bytes;
  }
  curptr=mpoolh->freeptr;
  // set correct alignment for nextptr
  i=((size_t)nextptr)%ALIGNMENT_BYTES;
  if (i!=0) nextptr=((char*)nextptr)+(ALIGNMENT_BYTES-i);
  // alignment now ok
  mpoolh->freeptr=nextptr;
  return curptr;
}



/* ====== Convenience functions for using data allocated from mpool ================= */

/*

Core object types are pairs and atoms plus 0 (NULL).

Lists are formed by pairs of gints. Each pair starts at address with two last bits 0.
The first element of the pair points to the contents of the cell, the second to rest.

Atoms may contain strings, ints etc etc. Each atom starts at address with last bit 1.

The first byte of the atom indicates its type. The following bytes are content, always
encoded as a 0-terminated string or TWO consequent 0-terminated strings.

The atom type byte contains dbapi.h values:

STRING, CONVERSION TO BE DETERMINED LATER: 0
#define WG_NULLTYPE 1
#define WG_RECORDTYPE 2
#define WG_INTTYPE 3
#define WG_DOUBLETYPE 4
#define WG_STRTYPE 5
#define WG_XMLLITERALTYPE 6
#define WG_URITYPE 7
#define WG_BLOBTYPE 8
#define WG_CHARTYPE 9
#define WG_FIXPOINTTYPE 10
#define WG_DATETYPE 11
#define WG_TIMETYPE 12
#define WG_ANONCONSTTYPE 13
#define WG_VARTYPE 14
#define WG_ILLEGAL 0xff

Atom types 5-8 (strings, xmlliterals, uris, blobs) contain TWO
consequent strings, first the main, terminating 0, then the
second (lang, namespace etc) and the terminating 0. Two terminating
0-s after the first indicates the missing second string (NULL).

Other types are simply terminated by two 0-s.

*/


// ------------- pairs ----------------


int wg_ispair(void* db, void* ptr) {
  return (ptr!=NULL && ((((gint)ptr)&TYPEMASK)==PAIRBITS));
}

void* wg_mkpair(void* db, void* mpool, void* x, void* y) {
  void* ptr;

  ptr=wg_alloc_mpool(db,mpool,sizeof(gint)*2);
  if (ptr==NULL) {
    show_mpool_error(db,"cannot create a pair in mpool");
    return NULL;
  }
  *((gint*)ptr)=(gint)x;
  *((gint*)ptr+1)=(gint)y;
  return ptr;
}

void* wg_first(void* db, void* ptr) {
  return (void*)(*((gint*)ptr));
}

void* wg_rest(void* db, void *ptr) {
  return (void*)(*((gint*)ptr+1));
}

int wg_listtreecount(void* db, void *ptr) {
  if (wg_ispair(db,ptr))
    return wg_listtreecount(db,wg_first(db,ptr)) + wg_listtreecount(db,wg_rest(db,ptr));
  else
    return 1;
}

// ------------ atoms ------------------


int wg_isatom(void* db, void* ptr) {
  return (ptr!=NULL && ((((gint)ptr)&TYPEMASK)==ATOMBITS));

}

void* wg_mkatom(void* db, void* mpool, int type, char* str1, char* str2) {
  char* ptr;
  char* curptr;
  int size=2;

  if (str1!=NULL) size=size+strlen(str1);
  size++;
  if (str2!=NULL) size=size+strlen(str2);
  size++;
  ptr=(char*)(wg_alloc_mpool(db,mpool,size));
  if (ptr==NULL) {
    show_mpool_error(db,"cannot create an atom in mpool");
    return NULL;
  }
  ptr++; // shift one pos right to set address last byte 1
  curptr=ptr;
  *curptr=(char)type;
  curptr++;
  if (str1!=NULL) {
    while((*curptr++ = *str1++));
  } else {
    *curptr=(char)0;
    curptr++;
  }
  if (str2!=NULL) {
    while((*curptr++ = *str2++));
  } else {
    *curptr=(char)0;
    curptr++;
  }
  return ptr;
}

int wg_atomtype(void* db, void* ptr) {
  if (ptr==NULL) return 0;
  else return (gint)*((char*)ptr);
}


char* wg_atomstr1(void* db, void* ptr) {
  if (ptr==NULL) return NULL;
  if (*(((char*)ptr)+1)==(char)0) return NULL;
  else return ((char*)ptr)+1;
}

char* wg_atomstr2(void* db, void* ptr) {
  if (ptr==NULL) return NULL;
  ptr=(char*)ptr+strlen((char*)ptr)+1;
  if (*(((char*)ptr)+1)==(char)0) return NULL;
  else return ((char*)ptr);
}


// ------------ printing ------------------

void wg_mpool_print(void* db, void* ptr) {
  wg_mpool_print_aux(db,ptr,0,1);
}

static void wg_mpool_print_aux(void* db, void* ptr, int depth, int pflag) {
  int type;
  char* p;
  int count;
  int ppflag=0;
  int i;
  void *curptr;

  if (ptr==NULL) {
    printf("()");
  } else if (wg_isatom(db,ptr)) {
    type=wg_atomtype(db,ptr);
    switch (type) {
      case 0: printf("_:"); break;
      case WG_NULLTYPE: printf("n:"); break;
      case WG_RECORDTYPE: printf("r:"); break;
      case WG_INTTYPE: printf("i:"); break;
      case WG_DOUBLETYPE: printf("d:"); break;
      case WG_STRTYPE: printf("s:"); break;
      case WG_XMLLITERALTYPE: printf("x:"); break;
      case WG_URITYPE: printf("u:"); break;
      case WG_BLOBTYPE: printf("b:"); break;
      case WG_CHARTYPE: printf("c:"); break;
      case WG_FIXPOINTTYPE: printf("f:"); break;
      case WG_DATETYPE: printf("date:"); break;
      case WG_TIMETYPE: printf("time:"); break;
      case WG_ANONCONSTTYPE: printf("a:"); break;
      case WG_VARTYPE: printf("?:"); break;
      default: printf("!:");
    }
    p=wg_atomstr1(db,ptr);
    if (p!=NULL) {
      if (strchr(p,' ')!=NULL || strchr(p,'\n')!=NULL || strchr(p,'\t')!=NULL) {
        printf("\"%s\"",p);
      } else {
        printf("%s",p);
      }
    } else {
      printf("\"\"");
    }
    p=wg_atomstr2(db,ptr);
    if (p!=NULL) {
      if (strchr(p,' ')!=NULL || strchr(p,'\n')!=NULL || strchr(p,'\t')!=NULL) {
        printf("^^\"%s\"",p);
      } else {
        printf("^^%s",p);
      }
    }
  } else {
    if (pflag && wg_listtreecount(db,ptr)>10) ppflag=1;
    printf ("(");
    for(curptr=ptr, count=0;curptr!=NULL && !wg_isatom(db,curptr);curptr=wg_rest(db,curptr), count++) {
      if (count>0) {
        if (ppflag) {
          printf("\n");
          for(i=0;i<depth;i++) printf(" ");
        }
        printf(" ");
      }
      wg_mpool_print_aux(db,wg_first(db,curptr),depth+1,0);
    }
    if (wg_isatom(db,curptr)) {
      printf(" . ");
      wg_mpool_print_aux(db,curptr,depth+1,ppflag);
    }
    printf (")");
    if (ppflag) printf("\n");
  }
}



// ------------- ints ---------------------



// ------------- floats --------------------





/* ============== error handling ==================== */

/** called with err msg when an mpool allocation error occurs
*
*  may print or log an error
*  does not do any jumps etc
*/

static int show_mpool_error(void* db, char* errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"db memory pool allocation error: %s\n",errmsg);
#endif
  return -1;
}

/** called with err msg and err nr when an mpool allocation error occurs
*
*  may print or log an error
*  does not do any jumps etc
*/

static int show_mpool_error_nr(void* db, char* errmsg, int nr) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"db memory pool allocation error: %s %d\n",errmsg,nr);
#endif
  return -1;
}

#ifdef __cplusplus
}
#endif
