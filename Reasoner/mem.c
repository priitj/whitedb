/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
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

 /** @file mem.c
 *  Specific memory allocation functions: vectors etc.
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "rincludes.h"

  
/* ====== Private headers and defs ======== */



/* ======= Private protos ================ */


/* ====== Functions ============== */

  
/* low-level wrapper funs for alloc, realloc, and free */
  
void* wr_malloc(glb* g, int bytes) {
  ++(g->stat_wr_mallocs);
  (g->stat_wr_malloc_bytes)+=bytes;
  //printf("!!! wr malloc %d \n",bytes);
  return sys_malloc(bytes);  
}  

void* wr_realloc(glb* g, void* p, int bytes) {
  ++(g->stat_wr_reallocs);
  (g->stat_wr_realloc_bytes)+=bytes;
  //printf("!!! wr realloc %d \n",bytes);
  return sys_realloc(p,bytes);  
}

void wr_free(glb* g, void* p) {
  ++(g->stat_wr_frees);
  sys_free(p);
  return;  
}


/* ====== Functions for vec: word arrays with length at pos 0 ========== */


/** Allocate a new vec with length len, set element 0 to length, set elements to NULL
*
*/

vec wr_vec_new(glb* g,int len) {
  vec res;
  int i;
  
  res = (vec) wr_malloc(g,((len+1)*sizeof(gint))+OVER_MALLOC_BYTES);   
  if (res==NULL) {
    (g->alloc_err)=1;    
    wr_alloc_err2int(g,"Cannot allocate memory for a vec with length",len);
    return NULL;
  }  
  // set correct alignment for res
  i=VEC_ALIGNMENT_BYTES-(((gint)res)%VEC_ALIGNMENT_BYTES);
  if (i==VEC_ALIGNMENT_BYTES) i=0;  
  res=(gptr)((char*)res+i);
  
  res[0]=(gint)len;
  //for (i=VEC_START; i<=len; i++) res[i]=0;
    
  return res;
}

/** Allocate a new vec with length len, set element 0 to length, set elements to NULL,
*   set counter (pos 1) to first empty (2)
*
*/

cvec wr_cvec_new(glb* g,int len) {
  vec res;
  int i;
  
  if (g->alloc_err) return NULL;
  res = (vec) wr_malloc(g,(len+2)*sizeof(gint));
  if (res==NULL) {
      (g->alloc_err)=1;
      wr_alloc_err2int(g,"Cannot reallocate memory for a cvec with length",len);    
      return NULL;
  }  
  // set correct alignment for res
  i=VEC_ALIGNMENT_BYTES-(((gint)res)%VEC_ALIGNMENT_BYTES);
  if (i==VEC_ALIGNMENT_BYTES) i=0;  
  res=(gptr)((char*)res+i);
  
  res[0]=(gint)len;
  res[1]=(gint)CVEC_START; 
  //for (i=CVEC_START; i<=len; i++) res[i]=0;
  //memset(res+CVEC_START,0,(len-CVEC_START));  
  return res;
}

/** Free the passed vec.
*
*/

void wr_vec_free(glb* g,vec v) {
  if (v!=NULL) wr_free(g,v);
}  
  

/** Free the passed vec and free all strings inside (assuming vec contains strings)
*
*/

void wr_vecstr_free(glb* g,vec v) {
  int i;
  
  if (v==NULL) return;
    
  for(i=1;i<=(int)(v[0]);i++) {
    if (v[i]!=(gint)NULL) wr_str_free(g,(char*)(v[i]));
  }  
  sys_free(v);
}  


/** Free the passed vec and free all vecs and their strings inside 
*
*/

void wr_vecvecstr_free(glb* g,vec v) {
  int i;
  
  if (v==NULL) return;
    
  for(i=1;i<=(int)(v[0]);i++) {
    if (v[i]!=(gint)NULL) wr_vecstr_free(g,(vec)(v[i]));
  }  
  sys_free(v);
}  


/** Reallocate vec to contain at least i elements.
*
* Normally the reallocated vec contains more than i elems.
*
*/

vec wr_vec_realloc(glb* g,vec v, int i) {
  int vlen;
  vec nvec;
  int nlen;
  
  vlen=(int)v[0]; 
  if (i<=vlen) {
    return v;
  } else {
    if (g->alloc_err) return NULL;
    for(nlen=(vlen<=0 ? 2 : vlen*2); i>nlen; nlen=nlen*2);
      
    //printf("Reallocing vec from %d to %d\n",vlen,nlen);
    
    nvec=wr_realloc(g,v,(nlen+1)*sizeof(gint));
    if (nvec==NULL) {
      (g->alloc_err)=1;
      wr_alloc_err2int(g,"Cannot reallocate memory for a vec with length",nlen);    
      return NULL;
    }  
    nvec[0]=(gint)nlen;
    //for (i=vlen+1; i<=nlen; i++) nvec[i]=0; // set new elems to 0
    //memset(nvec+vlen+1,0,(nlen-vlen)-1);
    return nvec;    
  }            
}  

/** Store element to pos i in the vec
*
* If vec is not big enough, it is automatically reallocated
*
*/

vec wr_vec_store(glb* g,vec v, int i, gint e) {
  vec nvec;
  
  if (i<=(int)v[0]) {
    v[i]=(gint)e;
    return v;
  } else {
    nvec=wr_vec_realloc(g,v,i);
    if (nvec==NULL) {
      (g->alloc_err)=1;
      wr_alloc_err2int(g,"vec_store cannot allocate enough memory to store at",i);    
      return NULL;
    }  
    nvec[i]=(gint)e;
    return nvec;    
  }            
}  

/** Store element to pos i in the cvec
*
* If vec is not big enough, it is automatically reallocated
* Free pos is automatically moved to i+i
*
*/

cvec wr_cvec_store(glb* g,cvec v, int i, gint e) {
  cvec nvec;
  
  nvec=wr_vec_store(g,v,i,e);
  if (nvec==NULL) return NULL;
  if (nvec[1]<=i) {
   nvec[1]=(gint)(i+1);
  }      
  return nvec;  
}  

/** Store element to next free pos in the cvec
*
* If vec is not big enough, it is automatically reallocated
* Free pos is automatically moved to i+i
*
*/

cvec wr_cvec_push(glb* g,cvec v, gint e) {
  cvec nvec;
  
  nvec=wr_cvec_store(g,v,v[1],e);
  return nvec;  
} 




gptr wr_alloc_from_cvec(glb* g, cvec buf, gint gints) {
  gint pos;
  gint i;
  
  pos=CVEC_NEXT(buf);
  // set correct alignment for pos
  //printf("wr_alloc_from_cvec initial pos %d buf+pos %d remainder with VEC_ALIGNMENT_BYTES %d\n",
  //        pos,buf+pos,((gint)(buf+pos))%VEC_ALIGNMENT_BYTES);
  i=VEC_ALIGNMENT_BYTES-(((gint)(buf+pos))%VEC_ALIGNMENT_BYTES);
  //printf("first i %d \n",i);
  if (i==VEC_ALIGNMENT_BYTES) i=0;  
  if (i) pos++;
  //printf("wr_alloc_from_cvec final pos %d buf+pos %d remainder with VEC_ALIGNMENT_BYTES %d\n",
  //        pos,buf+pos,((gint)(buf+pos))%VEC_ALIGNMENT_BYTES);
  if ((pos+gints)>=CVEC_LEN(buf)) {
    wr_alloc_err(g," local temp buffer overflow");
    (g->alloc_err)=1;
    return NULL;    
  }  
  CVEC_NEXT(buf)=pos+gints;
  return buf+pos;   
}  

/* ====== Functions for strings ============== */


/** Allocate a new string with length len, set last element to 0
*
*/

char* wr_str_new(glb* g, int len) {
  char* res;
  
  res = (char*) wr_malloc(g,len*sizeof(char));
  if (res==NULL) {
    (g->alloc_err)=1;
    wr_sys_exiterr2int(g,"Cannot allocate memory for a string with length",len);
    return NULL;
  }  
  res[len-1]=0;  
  return res;
}


/** Guarantee string space: realloc if necessary, then set last byte to 0
*
*/

void wr_str_guarantee_space(glb* g, char** stradr, int* strlenadr, int needed) {
  char* tmp;
  int newlen;
  int j;
  
  //printf("str_guarantee_space, needed: %d, *strlenadr: %d\n",needed,*strlenadr);
  if (needed>(*strlenadr)) {    
    newlen=(*strlenadr)*2;
    tmp=wr_realloc(g,*stradr,newlen);
    //printf("str_guarantee_space, realloc done, newlen: %d\n",newlen);
    if (tmp==NULL) {
      wr_sys_exiterr2int(g,"Cannot reallocate memory for a string with length",newlen);    
      return;
    }  
    for(j=(*strlenadr)-1;j<newlen;j++) *(tmp+j)=' '; // clear new space
    tmp[newlen-1]=0;   // set last byte to 0  
    *stradr=tmp;
    *strlenadr=newlen;
    wr_str_guarantee_space(g,stradr,strlenadr,needed); // maybe still not enough?    
  }  
}
    
/** Free the passed string.
*
*/

void wr_str_free(glb* g, char* str) {
  if (str!=NULL) wr_free(g,str);
}  


/** Free the string pointed to at passed address and set address ptr to NULL
*
*/

void wr_str_freeref(glb* g, char** strref) {
  if (*strref!=NULL) wr_free(g,*strref);
  *strref=NULL;  
}  


#ifdef __cplusplus
}
#endif
