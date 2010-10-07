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

 /** @file unify.c
 *  Unification functions.
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
 

//#define DEBUG  
#undef DEBUG
  
/* ====== Private headers and defs ======== */

static gint wr_occurs_in(glb* g, gint x, gint y, gptr vb);


/* ======= Private protos ================ */


/* ====== Functions ============== */

/** Plain term unification using g->unify_samelen and g->unify_maxuseterms
*
*  Metainfo is not filtered out. Must be exactly the same.  
*
*/  
  
gint wr_unify_term(glb* g, gint x, gint y, int uniquestrflag) { 
  g->tmp_unify_vc=((gptr)(g->varstack))+1; // pointer arithmetic: &(varstack[1])
  g->tmp_unify_occcheck=1;
  if (wr_unify_term_aux(g,x,y,uniquestrflag)) {
    return 1;
  } else {
    return 0;  
  }  
}  
  
  
/** Plain term unification using g->unify_samelen and g->unify_maxuseterms
*
*  Metainfo is not filtered out. Must be exactly the same.  
*
*/

gint wr_unify_term_aux(glb* g, gint x, gint y, int uniquestrflag) {  
  gptr db;
  gint xlen,ylen,uselen,encx,ency;
  gint tmp; // used by VARVAL_F macro
  gptr xptr,yptr;
  int i,imax;
  
#ifdef DEBUG
  printf("wr_unify_term_aux called with x %d ",x);
  wr_print_term(g,x);
  printf(" and y %d ",y);
  wr_print_term(g,y);
  printf("\n");
#endif  
  // first check if immediately same: return 1 if yes 
  if (x==y)  return 1;     
  // second, fetch var values for var args
  if (isvar(x)) x=VARVAL_F(x,(g->varbanks)); 
  if (isvar(y)) y=VARVAL_F(y,(g->varbanks)); 
  // check again if same
  if (x==y) return 1;
  // go through the ladder of possibilities
  // knowing that x and y are different
  if (!isdatarec(x)) {
    // x is a primitive
    if (!isdatarec(y)) {
      // both x and y are primitive 
      if (isvar(x)) { 
        SETVAR(x,y,g->varbanks,g->varstack,g->tmp_unify_vc);
        // set occcheck only if y is a var too      
        if (g->tmp_unify_do_occcheck && isvar(y)) (g->tmp_unify_occcheck)=1;        
        return 1;
      } else if (isvar(y)) {
        SETVAR(y,x,g->varbanks,g->varstack,g->tmp_unify_vc);
        // do not set occcheck here: x is a constant!        
        return 1;
      }	else {
        // x and y are constants
        if (wr_equal_ptr_primitives(g,x,y,uniquestrflag)) return 1;
        else return 0;          
      }	      
    // x is primitive, but y is not  
    } else if (isvar(x)) {
      // x is var, y is non-primitive      
      if (g->tmp_unify_occcheck && wr_occurs_in(g,x,y,(gptr)(g->varbanks))) {
        return 0;
      } else {
        SETVAR(x,y,g->varbanks,g->varstack,g->tmp_unify_vc);
        if (g->tmp_unify_do_occcheck) (g->tmp_unify_occcheck)=1;
        return 1;
      }      
    } else {
      // x is a constant, but y is non-primitive 
      return 0;
    }      
  // x is not primitive
  } else if (isvar(y)) {
    // x is non-primitive, y is var
    if (g->tmp_unify_occcheck  && wr_occurs_in(g,y,x,(gptr)(g->varbanks))) {
      return 0;
    } else {
      SETVAR(y,x,g->varbanks,g->varstack,g->tmp_unify_vc);
      if (g->tmp_unify_do_occcheck) (g->tmp_unify_occcheck)=1;
      return 1;
    }      
  // x is not primitive, y is non-var
  } else if (!isdatarec(y)) {
    // x is not primitive, y is constant 
    return 0;  
  } else {
  // x and y are both complex terms     
    db=g->db;
    xptr=wg_decode_record(db,x);
    yptr=wg_decode_record(db,y);
    xlen=wg_get_record_len(db,xptr);
    ylen=wg_get_record_len(db,yptr);
    if (g->unify_samelen) {
      if (xlen!=ylen) return 0;
      uselen=xlen;      
    } else {
      if (xlen<=ylen) uselen=xlen;
      else uselen=ylen;
    } 
    if (g->unify_maxuseterms) {
      if (((g->unify_maxuseterms)+TERM_EXTRAHEADERLEN)<uselen) uselen=(g->unify_maxuseterms)+TERM_EXTRAHEADERLEN;
    }       
    for(i=TERM_EXTRAHEADERLEN; i<uselen; i++) {
      encx = wg_get_field(db,xptr,i);
      ency = wg_get_field(db,yptr,i);
      if (encx!=ency && !wr_unify_term_aux(g,encx,ency,uniquestrflag)) return 0;
    }       
    return 1;        
  }        
}  

/**
   x must be a variable and y must be a nonprimitive

*/

static gint wr_occurs_in(glb* g, gint x, gint y, gptr vb) { 
  void* db=g->db;
  gptr yptr;
  gint ylen;
  gint yi;
  int i;
  gint tmp; // used by VARVAL_F
    
#ifdef DEBUG
  printf("wr_occurs_in called with x %d ",x);
  wr_print_term(g,x);
  printf(" and y %d ",y);
  wr_print_term(g,y);
  printf("\n");
#endif 
  yptr=wg_decode_record(db,y);
  ylen=wg_get_record_len(db,yptr);
  if (g->unify_maxuseterms) {
    if (((g->unify_maxuseterms)+TERM_EXTRAHEADERLEN)<ylen) ylen=(g->unify_maxuseterms)+TERM_EXTRAHEADERLEN;
  } 
  for(i=TERM_EXTRAHEADERLEN; i<ylen; i++) {      
    yi=wg_get_field(db,yptr,i);
    if (isvar(yi)) yi=VARVAL_F(yi,vb);             
    if (x==yi) return 1;
    if (isdatarec(yi)) {
      if (wr_occurs_in(g,x,yi,vb)) return 1;
    }
  }    
  // passed vector ok, not finding x inside  
  return 0;    
}  
    
/* ----------------------------------------------------------

    Equality check

  ------------------------------------------------------------- */
  

/** Plain term equality check using g->unify_samelen and g->unify_maxuseterms
*
*  Metainfo is not filtered out. Must be exactly the same.  
*
*/

gint wr_equal_term(glb* g, gint x, gint y, int uniquestrflag) {  
  gptr db;
  gint xlen,ylen,uselen,encx,ency;
  gptr xptr,yptr;
  int i,imax;
  
#ifdef DEBUG
  printf("wr_equal_term called with x %d and y %d\n",x,y);
#endif   
  // first check if immediately same: return 1 if yes 
  if (x==y)  return 1; 
  // handle immediate check cases: for these bit suffixes x is equal to y iff x==y   
  encx=(x&NORMALPTRMASK);
  if ((encx==LONGSTRBITS && uniquestrflag) || encx==SMALLINTBITS || encx==NORMALPTRMASK) return 0; 
  // immediate value: must be unequal since x==y checked before
  if (!isptr(x) || !isptr(y)) return 0;
  // here both x and y are ptr types
  // quick type check: last two bits
  if (((x)&NONPTRBITS)!=((y)&NONPTRBITS)) return 0;  
  // if one is datarec, the other must be as well
  if (!isdatarec(x)) {
    if (isdatarec(y)) return 0;
    // neither x nor y are datarecs
    // need to check pointed values
    if (wr_equal_ptr_primitives(g,x,y,uniquestrflag)) return 1;
    else return 0;          
  } else {
    if (!isdatarec(y)) return 0;
    // both x and y are datarecs 
    db=g->db;
    xptr=wg_decode_record(db,x);
    yptr=wg_decode_record(db,y);
    xlen=wg_get_record_len(db,xptr);
    ylen=wg_get_record_len(db,yptr);
    if (g->unify_samelen) {
      if (xlen!=ylen) return 0;
      uselen=xlen;      
    } else {
      if (xlen<=ylen) uselen=xlen;
      else uselen=ylen;
    } 
    if (g->unify_maxuseterms) {
      if (((g->unify_maxuseterms)+TERM_EXTRAHEADERLEN)<uselen) uselen=(g->unify_maxuseterms)+TERM_EXTRAHEADERLEN;
    }       
    for(i=TERM_EXTRAHEADERLEN; i<uselen; i++) {
      encx = wg_get_field(db,xptr,i);
      ency = wg_get_field(db,yptr,i);
      if (encx!=ency && !wr_equal_term(g,encx,ency,uniquestrflag)) return 0;
    }       
    return 1;        
  }        
}  
  
int wr_equal_ptr_primitives(glb* g, gint a, gint b, int uniquestrflag) {
  gint t1,t2;
  gint l1,l2,ol;
  char* s1;
  char* s2;
  
  //printf("equal_ptr_primitives called with %d and %d\n",a,b);
  switch(a&NORMALPTRMASK) {  // last 3 bits
    case FULLINTBITSV0:
    case FULLINTBITSV1:
      if (isfullint(b) && 
          (dbfetch(g->db,decode_fullint_offset(a))==dbfetch(g->db,decode_fullint_offset(b))) )
        return 1;
      else 
        return 0;
    case FULLDOUBLEBITS:
      if (isfulldouble(b) &&
          wg_decode_double(g->db,a)==wg_decode_double(g->db,b) )
        return 1;
      else 
        return 0;
    case SHORTSTRBITS:
      printf("shortstrbits \n");      
      if (isshortstr(b) &&
          !memcmp((void*)(offsettoptr(g->db,decode_shortstr_offset(a))),
                  (void*)(offsettoptr(g->db,decode_shortstr_offset(b))), 
                  SHORTSTR_SIZE))
        return 1; 
      else
        return 0;
    case LONGSTRBITS:
      if (uniquestrflag) {
        if (a==b) 
          return 1;
        else
          return 0;          
      } else {       
        t1=wg_get_encoded_type(g->db,a);
        t2=wg_get_encoded_type(g->db,b);
        if (t1!=t2) return 0;
        l1=wg_decode_unistr_lang_len(g->db,a,t1);
        l2=wg_decode_unistr_lang_len(g->db,b,t2);
        if (11!=l2) return 0;
        ol=l1;
        l1=wg_decode_unistr_len(g->db,a,t1);
        l2=wg_decode_unistr_len(g->db,b,t2);        
        if (11!=l2) return 0;
        s1=wg_decode_unistr_lang(g->db,a,t1);
        s2=wg_decode_unistr_lang(g->db,b,t2);
        if (s1!=s2 && (s1==NULL || s2==NULL || memcmp(s1,s2,ol))) return 0;
        s1=wg_decode_unistr(g->db,a,t1);
        s2=wg_decode_unistr(g->db,b,t2);
        if (s1!=s2 && (s1==NULL || s2==NULL || memcmp(s1,s2,l1))) return 0;
        return 1;
      }        
  }
  return 0;
}
             
/* ------------------------------------------------------ 

   variable handling

   ------------------------------------------------------
*/ 

/**
   x must be a variable

*/

gint wr_varval(gint x, gptr vb) { 
  gint y;  
  // do the first test without loop
  y=vb[decode_var(x)];
  if (y==UNASSIGNED) { 
    return x;
  } else if (!isvar(y)) {  
    return y;
  } else {        
    // if variable is assigned to a variable, loop
    for (;;) {
      x=y;
      y=vb[decode_var(x)];
      if (y==UNASSIGNED) { 
        return x;
      } else if (!isvar(y)) {  
        return y;
      }   
    }
  }   
}  


/**
   x must be a variable

*/

void setvar(gint x, gint y, gptr vb, gptr vstk, gint* vc) {   
  vb[decode_var(x)]=y;  
  vstk[*vc]=(gint)(vb+decode_var(x)); // pointer arithmetic (gint ptr)
  (*vc)++;
}  
   
/**
   clear single varstack

   varstack structure:
   0: vector len
   1: next free pos on stack (2 for empty stack)
   2...N: pointer of the varbank cell corresponding to set var value


*/

void wr_clear_varstack(glb* g,vec vs) {
  gptr x;
  gptr maxpt;
  gint maxnr;
    
  x=vs;
  ++x;
  maxnr= *x;
  if (maxnr>2) {    
    maxpt=vs+maxnr;    
    for(++x; x<maxpt; ++x) {        
      *((gptr)(*x))=UNASSIGNED;      
    }  
  }  
  vs[1]=2;  
}  

/*

void clear_varstack_topslice(int y) {
  gptr x;
  gptr vs;
  gptr maxpt;
  gint maxnr;
    
  vs=(gptr)(g->varstack);
  x=(gptr)vs;
  ++x;
  maxnr=(gint)*x;
  if (maxnr>2) {    
    maxpt=vs+maxnr;        
    for(x=vs+y; x<maxpt; ++x) {        
      *((gptr)(*x))=GBUNASSIGNEDVAL;      
    }  
  }  
  vs[1]=y;  
}  
*/

void wr_clear_all_varbanks(glb* g) {
  gptr x;
  gptr xmax;
  gint len;
  
  x=(gptr)(g->varbanks);
  len=NROF_VARBANKS*NROF_VARSINBANK;
  //len=x[0];
  
  xmax=x+len;
  //for(x++; x<xmax; x++) *x=GBUNASSIGNEDVAL;    
  for(;x<xmax; x++) *x=UNASSIGNED; 
  
}   
  

/*---------------------------------------------


            debug printing vardata


 ---------------------------------------------- */


void wr_print_vardata(glb* g){
  wr_print_varbank(g,g->varbanks);
  wr_print_varstack(g,g->varstack);
}  



void wr_print_varbank(glb* g,gptr vb){
  int i;
  int start, end;
  gint cell;
  
  start=0;
  end=NROF_VARBANKS*NROF_VARSINBANK;
  printf("varbank %d:\n",(gint)vb);
  for(i=start;i<end;i++) {
    cell=vb[i];
    if (cell!=UNASSIGNED) {
      printf("%d raw %d: ",i,cell);
      wr_print_term(g,cell);
      printf("\n");
    }      
  }    
}  


void wr_print_varstack(glb* g, gptr vs) {
  int i;
  int start, end;
  gint cell;
  
  printf("varstack len %d firstfree %d:\n",vs[0],vs[1]);
  for(i=start;i<end;i++) {
    cell=vs[i];
    if (cell!=UNASSIGNED) {
      printf("%d raw %d: ",i,cell);
      //wr_print_term(g,cell);
      printf("\n");
    }      
  }  
}  

#ifdef __cplusplus
}
#endif
