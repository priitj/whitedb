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

 /** @file build.h
 *  Term and clause building functions.
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

#define PRINT_LIMITS

/* ======= Private protos ================ */


/* ====== Functions ============== */

/* ** subst and calc term
  
  uses global flags set before:
  g->build_subst    // subst var values into vars
  g->build_calc     // do fun and pred calculations
  g->build_dcopy    // copy nonimmediate data (vs return ptr)
  g->build_buffer;  // build everything into tmp local buffer area (vs main area)
  
*/


gptr wr_build_calc_cl(glb* g, gptr xptr) {
  void* db;
  int ruleflag;  
  gptr yptr;    
  gint xlen;
  gint xatomnr; 
  gint xmeta, xatom, yatom;  
  int i;
  gint tmp;
  int ilimit;
  
  //printf("wr_build_calc_cl called\n"); 
  db=g->db;
  if (g->build_rename) (g->build_rename_vc)=0;
  ruleflag=wg_rec_is_rule_clause(db,xptr);  
  if (!ruleflag) {
    // in some cases, no change, no copy: normally copy
    //yptr=xptr; 
    tmp=wr_build_calc_term(g,encode_datarec_offset(pto(db,xptr)));
    if (tmp==WG_ILLEGAL)  return NULL; // could be memory err
    yptr=rotp(g,tmp);    
  } else {        
    xlen=get_record_len(xptr);
    // allocate space
    if ((g->build_buffer)!=NULL) {
      yptr=wr_alloc_from_cvec(g,g->build_buffer,(RECORD_HEADER_GINTS+xlen));       
    } else {
      yptr=wg_create_raw_record(db,xlen);                   
    }        
    if (yptr==NULL) return NULL;
    // copy rec header and clause header
    ilimit=RECORD_HEADER_GINTS+(g->unify_firstuseterm);
    for(i=0;i<ilimit;i++) {
      yptr[i]=xptr[i];     
    }  
    //wr_print_varbank(g,g->varbanks);
    // loop over clause elems
    xatomnr=wg_count_clause_atoms(db,xptr);
    for(i=0;i<xatomnr;i++) {
      xmeta=wg_get_rule_clause_atom_meta(db,xptr,i);
      xatom=wg_get_rule_clause_atom(db,xptr,i);
      wr_set_rule_clause_atom_meta(g,yptr,i,xmeta);
      yatom=wr_build_calc_term(g,xatom);
      if (yatom==WG_ILLEGAL) return NULL; // could be memory err
      wr_set_rule_clause_atom(g,yptr,i,yatom);
    }   
    //wr_print_varbank(g,g->varbanks);
  }
  ++(g->stat_built_cl);
  return yptr;  
} 


/* ** subst and calc term
  
  uses global flags set before:
  g->build_subst    // subst var values into vars
  g->build_calc     // do fun and pred calculations
  g->build_dcopy    // copy nonimmediate data (vs return ptr)
  g->build_buffer;  // build everything into tmp local buffer area (vs main area)
  
*/


gint wr_build_calc_term(glb* g, gint x) {
  void* db;
  gptr xptr,yptr;
  gint xlen,uselen;
  gint tmp; // used by VARVAL_F
  gint vnr;
  gint newvar;
  int i;
  gint res;
  int ilimit;
  int substflag;
    
  //printf("wr_build_calc_term called with x %d type %d\n",x,wg_get_encoded_type(g->db,x));
  if (isvar(x) && (g->build_subst || g->build_rename))  x=VARVAL_F(x,(g->varbanks));
  if (!isdatarec(x)) {
    // now we have a simple value  
    if (!isvar(x) || !(g->build_rename)) return x;
    vnr=decode_var(x);      
    if (vnr<FIRST_UNREAL_VAR_NR) {
      //normal variable, has to be renamed 
      newvar=(g->build_rename_vc)+FIRST_UNREAL_VAR_NR;
      if ((g->build_rename_vc)>=NROF_VARSINBANK) {
        ++(g->stat_internlimit_discarded_cl); 
        (g->alloc_err)=3;      
#ifdef PRINT_LIMITS         
        printf("limiterr in wr_build_calc_term for renamed var nrs\n");                    
#endif        
        return WG_ILLEGAL;          
      }  
      ++(g->build_rename_vc);
      SETVAR(x,encode_var(newvar),(g->varbanks),(g->varstack),(g->tmp_unify_vc));
      return encode_var(((g->build_rename_banknr)*NROF_VARSINBANK)+(newvar-FIRST_UNREAL_VAR_NR));
    } else {
      return encode_var(((g->build_rename_banknr)*NROF_VARSINBANK)+(vnr-FIRST_UNREAL_VAR_NR));
    }    
  }   
  // now we have a datarec
  if (0) {
  } else {  
    db=g->db;
    xptr=decode_record(db,x);
    xlen=get_record_len(xptr);
    //printf("wr_build_calc_term xptr %d xlen %d\n",(gint)xptr,xlen);
    // allocate space
    if ((g->build_buffer)!=NULL) {       
      yptr=wr_alloc_from_cvec(g,g->build_buffer,(RECORD_HEADER_GINTS+xlen));     
      //yptr=malloc(64);
    } else {
      yptr=wg_create_raw_record(db,xlen);     
    }    
    if (yptr==NULL) return WG_ILLEGAL;
    // copy rec header and term header
    ilimit=RECORD_HEADER_GINTS+(g->unify_firstuseterm);
    for(i=0;i<ilimit;i++) yptr[i]=xptr[i];        
    //printf("wr_build_calc_term i %d \n",i);
    // loop over term elems, i already correct
    if (g->unify_maxuseterms) {
      if (((g->unify_maxuseterms)+(g->unify_firstuseterm))<xlen) 
        uselen=((g->unify_maxuseterms)+(g->unify_firstuseterm)+RECORD_HEADER_GINTS);
      else
        uselen=xlen+RECORD_HEADER_GINTS; 
    } else {    
      uselen=xlen+RECORD_HEADER_GINTS;
    }  
    substflag=(g->build_subst || g->build_rename);
    for(;i<uselen;i++) {
      //printf("wr_build_calc_term loop i %d xptr[i] %d\n",i,xptr[i]);
      if (!substflag && !isdatarec(xptr[i])) yptr[i]=xptr[i];       
      else {tmp=wr_build_calc_term(g,xptr[i]);
            if (tmp==WG_ILLEGAL) return WG_ILLEGAL;
            //printf("wr_build_calc_term loop tmp %d \n",(gint)tmp);
            yptr[i]=tmp;
      }  
    }
    // copy term footer (in addition to rec/term header), i already correct
    if (g->unify_maxuseterms) {
      ilimit=RECORD_HEADER_GINTS+xlen;
      for(;i<ilimit;i++) {
        yptr[i]=xptr[i];     
      }  
    }          
    if ((g->use_comp_funs) && wr_computable_termptr(g,yptr)) {
      res=wr_compute_from_termptr(g,yptr); 
      if (res==WG_ILLEGAL) return WG_ILLEGAL;
    } else {
      res=encode_record(db,yptr);
    }     
    return res;
  }   
}  

int wr_computable_termptr(glb* g, gptr tptr) {
  gint fun;
  gint nr;
  
  //printf("wr_computable_termptr called with rec\n");
  //wg_print_record(g->db,tptr);
  //printf("\n");
  fun=tptr[RECORD_HEADER_GINTS+(g->unify_funpos)];
  //printf("cp1 fun %d type %d :\n",fun,wg_get_encoded_type(g->db,fun));
  //wg_debug_print_value(g->db,fun);
  //printf("\n");
  if (isanonconst(fun)) {    
    nr=decode_anonconst(fun);
    //printf("nr %d\n",nr);
    if (nr<(dbmemsegh(g->db)->anonconst.anonconst_nr) && nr>=0) return 1;    
  }
  return 0;  
}  

gint wr_compute_from_termptr(glb* g, gptr tptr) {  
  gint fun;
  gint res;
  
  //printf("wr_compute_from_termptr called\n");
  fun=tptr[RECORD_HEADER_GINTS+(g->unify_funpos)];
  // assume fun is anonconst!! 
  switch (fun) {
    case ACONST_PLUS:
      res=wr_compute_fun_plus(g,tptr);
      break;
    case ACONST_EQUAL:  
      res=wr_compute_fun_equal(g,tptr);
      break;
    default:
      res=encode_record(g->db,tptr);
  }
  return res;
}


gint wr_compute_fun_plus(glb* g, gptr tptr) {
  void* db=g->db;
  gint len;
  gint a,b;
  gint atype, btype;
  gint ri;
  double ad,bd,rd;
   
  //printf("wr_compute_fun_plus called\n");
  len=get_record_len(tptr); 
  if (len<(g->unify_firstuseterm)+3) return encode_record(db,tptr);     
  a=tptr[RECORD_HEADER_GINTS+(g->unify_funarg1pos)];
  atype=wg_get_encoded_type(db,a);
  if (atype!=WG_INTTYPE && atype!=WG_DOUBLETYPE) return encode_record(db,tptr);
  b=tptr[RECORD_HEADER_GINTS+(g->unify_funarg2pos)];
  btype=wg_get_encoded_type(db,b);
  if (btype!=WG_INTTYPE && btype!=WG_DOUBLETYPE) return encode_record(db,tptr);
  if (atype==WG_INTTYPE && btype==WG_INTTYPE) {
    // integer res case
    ri=wg_decode_int(db,a)+wg_decode_int(db,b);
    return wg_encode_int(db,ri);    
  } else { 
    // double res case
    if (atype==WG_INTTYPE) ad=(double)(wg_decode_int(db,a));
    else ad=wg_decode_double(db,a);
    if (btype==WG_INTTYPE) bd=(double)(wg_decode_int(db,b));
    else bd=wg_decode_double(db,b);
    rd=ad+bd;
    return wg_encode_double(db,rd);        
  }  
}  


gint wr_compute_fun_equal(glb* g, gptr tptr) {
  void* db=g->db;
  int len;
  gint a,b;
  gint atype, btype;
    
  len=get_record_len(tptr);
  if (len<(g->unify_firstuseterm)+3) return encode_record(db,tptr);  
  a=tptr[RECORD_HEADER_GINTS+(g->unify_funarg1pos)];
  atype=wg_get_encoded_type(db,a);
  b=tptr[RECORD_HEADER_GINTS+(g->unify_funarg2pos)];
  btype=wg_get_encoded_type(db,b);
  if (wr_equal_term(g,a,b,1)) return ACONST_TRUE;
  atype=wg_get_encoded_type(db,a);
  if (atype==WG_VARTYPE) return encode_record(db,tptr);
  btype=wg_get_encoded_type(db,b);
  if (btype==WG_VARTYPE) return encode_record(db,tptr);
  // here we have not equal a and b with non-var types
  if (atype==WG_RECORDTYPE || atype==WG_URITYPE || atype==WG_ANONCONSTTYPE || 
      btype==WG_RECORDTYPE || btype==WG_URITYPE || btype==WG_ANONCONSTTYPE) {
    return encode_record(db,tptr);
  } else {
    return ACONST_FALSE;
  }    
}


#ifdef __cplusplus
}
#endif
