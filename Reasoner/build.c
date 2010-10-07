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
  
  //printf("wr_build_calc_cl called\n"); 
  ruleflag=wg_rec_is_rule_clause(db,xptr);
  if (!ruleflag) {
    // in some cases, no change, no copy: normally copy
    //yptr=xptr; 
    yptr=wr_build_calc_term(g,encode_datarec_offset(pto(db,xptr)));    
  } else {
    db=g->db;    
    xlen=wg_get_record_len(db,xptr);
    // allocate space
    if ((g->build_buffer)!=NULL) {
      yptr=wr_alloc_from_cvec(g,g->build_buffer,(RECORD_HEADER_GINTS+xlen));       
    } else {
      yptr=wg_create_raw_record(db,xlen); 
    }        
    if (yptr==NULL) return NULL;
    // copy rec header
    for(i=0;i<RECORD_HEADER_GINTS;i++) yptr[i]=xptr[i];  
    // copy clause header (in addition to rec header)
    for(i=RECORD_HEADER_GINTS;i<(RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN);i++) {
      yptr[i]=xptr[i];     
    }  
    //wr_print_varbank(g,g->varbanks);
    // loop over clause elems
    xatomnr=wg_count_clause_atoms(db,xptr);
    for(i=0;i<xatomnr;i++) {
      xmeta=wg_get_rule_clause_atom_meta(db,xptr,i);
      xatom=wg_get_rule_clause_atom(db,xptr,i);
      wg_set_rule_clause_atom_meta(db,yptr,i,xmeta);
      yatom=wr_build_calc_term(g,xatom);
      wg_set_rule_clause_atom(db,yptr,i,yatom);
    }
    //wr_print_varbank(g,g->varbanks);
  }
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
  gint xlen,y,uselen;
  gint tmp; // used by VARVAL_F
  gint vnr;
  gint newvar;
  int i;
  
  
  //printf("wr_build_calc_term called with x %d type %d\n",x,wg_get_encoded_type(g->db,x));
  if (isvar(x) && g->build_subst) x=VARVAL_F(x,(g->varbanks));
  if (!isdatarec(x)) {
    // now we have a simple value  
    if (!isvar(x) || !(g->build_rename)) return x;
    // replace var
    vnr=decode_var(x);
    // alternative untested renaming
/*    
    if ((vnr>= ((g->build_rename_banknr)*NROF_VARSINBANK)) &&
        (vnr<  (((g->build_rename_banknr)*NROF_VARSINBANK)+NROF_VARSINBANK)) ) {
      // new var, keep as is
      return x;          
    } else {
      // var not seen yet            
      ++(g->build_rename_vc);
      if ((g->build_rename_vc)>=NROF_VARSINBANK) {
        ++(g->stat_internlimit_discarded_cl);        
#ifdef PRINT_LIMITS         
        printf("limiterr in wr_build_calc_term for renamed var nrs\n");                    
#endif        
        return WG_ILLEGAL;          
      }  
      newvar=encode_var((g->build_rename_vc)+(g->build_rename_banknr));      
      SETVAR(x,newvar,g->varbanks,g->varstack,g->tmp_unify_vc);          
    }      
*/        
    if (vnr<=(g->build_rename_maxseenvnr)) {
      for(i=0;i<g->build_rename_vc;i++) {
        if ((g->build_rename_bank)[i]==vnr) {
          return encode_var(i+((g->build_rename_banknr)*NROF_VARSINBANK));	       	
        }  
      }
    }      
    // var not seen yet         
    (g->build_rename_bank)[g->build_rename_vc]=vnr;    
    if (vnr>(g->build_rename_maxseenvnr)) (g->build_rename_maxseenvnr)=vnr;
    newvar=encode_var((g->build_rename_vc)+((g->build_rename_banknr)*NROF_VARSINBANK));                        
    ++(g->build_rename_vc);
    if ((g->build_rename_vc)>=NROF_VARSINBANK) {
      ++(g->stat_internlimit_discarded_cl);        
#ifdef PRINT_LIMITS         
      printf("limiterr in wr_build_calc_term for renamed var nrs\n");                    
#endif        
      return WG_ILLEGAL;          
    }  
    return newvar;
  }  
  // now we have a datarec
  if (0) {
  } else {  
    db=g->db;
    xptr=wg_decode_record(db,x);
    xlen=wg_get_record_len(db,xptr);
    //printf("wr_build_calc_term xptr %d xlen %d\n",(gint)xptr,xlen);
    // allocate space
    if ((g->build_buffer)!=NULL) {      
      yptr=wr_alloc_from_cvec(g,g->build_buffer,(RECORD_HEADER_GINTS+xlen));     
      //yptr=malloc(64);
    } else {
      yptr=wg_create_raw_record(db,xlen);     
    }    
    if (yptr==NULL) return WG_ILLEGAL;
    // copy rec header
    for(i=0;i<RECORD_HEADER_GINTS;i++) yptr[i]=xptr[i];
    //printf("wr_build_calc_term yptr %d \n",(gint)yptr);    
    // copy term header (in addition to rec header)
    for(i=RECORD_HEADER_GINTS;i<(RECORD_HEADER_GINTS+TERM_EXTRAHEADERLEN);i++) {
      yptr[i]=xptr[i];     
    }  
    //printf("wr_build_calc_term i %d \n",i);
    // loop over term elems
    uselen=xlen+RECORD_HEADER_GINTS;
    for(i=RECORD_HEADER_GINTS+TERM_EXTRAHEADERLEN;i<uselen;i++) {
      //printf("wr_build_calc_term loop i %d xptr[i] %d\n",i,xptr[i]);     
      tmp=wr_build_calc_term(g,xptr[i]);
      //printf("wr_build_calc_term loop tmp %d \n",(gint)tmp);
      yptr[i]=tmp;
    }
    //exit(0);   
    //printf("***** built yptr %d encoded %d type %d\n",
    //  yptr,(gint)(wg_encode_record(db,yptr)),wg_get_encoded_type(db,wg_encode_record(db,yptr)));
    return wg_encode_record(db,yptr); 
  }   
}  



#ifdef __cplusplus
}
#endif
