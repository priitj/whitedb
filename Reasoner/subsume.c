/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
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

 /** @file subsume.c
 *  Subsumption functions.
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


/* ======= Private protos ================ */


/* ====== Functions ============== */

  /* ------------------------------------------------------ 

   clause-to-clause-list subsumption

   ------------------------------------------------------
*/


int wr_given_cl_subsumed(glb* g, gptr given_cl) {
  gptr cl;
  int iactive;
  gptr actptr;
  gint iactivelimit;
  int sres;

#ifdef DEBUG
  printf("wr_given_cl_is_subsumed is called with \n");
  wr_print_clause(g,given_cl); 
#endif  
  if(1) {    
    actptr=rotp(g,g->clactive);
    iactivelimit=CVEC_NEXT(actptr);
    for(iactive=CVEC_START; iactive<iactivelimit; iactive++) {
      cl=(gptr)(actptr[iactive]);
      if (cl!=NULL) {      
#ifdef DEBUG
        printf("iactive %d\n",iactive); 
        wr_print_clause(g,cl);
#endif            
        // try to subsume
        sres=wr_subsume_cl(g,cl,given_cl,1);
        if (sres) {
#ifdef DEBUG
          printf("subsumer found\n");         
#endif           
          return 1;
        }  
      }	      
    }
    return 0;
  }    
}  
  

/* ------------------------------------------------------ 

   clause-to-clause subsumption

   ------------------------------------------------------
*/   
  
/*
  Each gcl literal must match a different scl literal.


  Take first gcl literal glit
    Loop over all scl literals     
      if match found glit<->slit and
         recursive subsumption without glit and slit present is OK
      then return OK
      else restore variable settings before last match attempt
    If match found during internal loop 

    otherwise 

  Loop over all gcl literals
    Loop over all scl literals     
      if match found glit<->slit then stop internal loop
      else restore variable settings before last match attempt


*/


gint wr_subsume_cl(glb* g, gptr cl1, gptr cl2, int uniquestrflag) {
  void* db=g->db;
  int cllen1,cllen2;
  int i2;
  gint meta1,meta2;
  gint lit1,lit2;
  int vc_tmp;
  int mres;
  
#ifdef DEBUG  
  printf("wr_subsume_cl called on %d %d \n",(int)cl1,(int)cl2);
  wr_print_clause(g,cl1);
  wr_print_clause(g,cl2);
#endif

  ++(g->stat_clsubs_attempted);  

  // check fact clause cases first
  if (!wg_rec_is_rule_clause(db,cl1)) {
    if (!wg_rec_is_rule_clause(db,cl2)) {
#ifdef DEBUG  
       printf("both clauses are facts \n");
#endif            
      ++(g->stat_clsubs_unit_attempted);
      if (wr_equal_term(g,encode_record(db,cl1),encode_record(db,cl2),uniquestrflag))
        return 1;
      else
        return 0;
    } else {
      cllen2=wg_count_clause_atoms(db,cl2);
      lit1=encode_record(db,cl1);      
      ++(g->stat_clsubs_unit_attempted);
      for(i2=0;i2<cllen2;i2++) {
        meta2=wg_get_rule_clause_atom_meta(db,cl2,i2);
        lit2=wg_get_rule_clause_atom(db,cl2,i2);
        if (!wg_atom_meta_is_neg(db,meta2) && wr_equal_term(g,lit1,lit2,uniquestrflag)) return 1;        
      }  
      return 0;
    }      
  } 
  cllen1=wg_count_clause_atoms(db,cl1);
  
  if (!wg_rec_is_rule_clause(db,cl2)) {
    // unit rule clause subsuming a unit fact clause case
    ++(g->stat_clsubs_unit_attempted);    
    cllen2=1;
    if (cllen1>1) return 0;
    meta1=wg_get_rule_clause_atom_meta(db,cl1,0);
    if (wg_atom_meta_is_neg(d,meta1)) return 0;
    lit1=wg_get_rule_clause_atom(db,cl1,0);    
    lit2=rpto(g,cl2);
    vc_tmp=2;
    mres=wr_match_term(g,lit1,lit2,uniquestrflag);
    if (vc_tmp!=*((g->varstack)+1)) wr_clear_varstack(g,g->varstack);
    return mres;      
  } else {
    // cl2 is a rule clause
    cllen2=wg_count_clause_atoms(db,cl2); 
  }    
  // now both clauses are rule clauses   

#ifdef DEBUG  
  printf("cllen1 %d cllen2 %d\n",cllen1,cllen2);
#endif   
  // check unit rule clause case
  if (cllen1==1) { 
#ifdef DEBUG  
    printf("unit clause subsumption case \n");
#endif        
    ++(g->stat_clsubs_unit_attempted);    
    ++(g->stat_clsubs_unit_attempted);
    meta1=wg_get_rule_clause_atom_meta(db,cl1,0);
    lit1=wg_get_rule_clause_atom(db,cl1,0);
    vc_tmp=2;
    *((g->varstack)+1)=vc_tmp; // zero varstack pointer 
    for(i2=0;i2<cllen2;i2++) {
        meta2=wg_get_rule_clause_atom_meta(db,cl2,i2);
        lit2=wg_get_rule_clause_atom(db,cl2,i2);
        if (!litmeta_negpolarities(meta1,meta2)) {  
          mres=wr_match_term(g,lit1,lit2,uniquestrflag);
          if (vc_tmp!=*((g->varstack)+1)) wr_clear_varstack(g,g->varstack);
          if (mres) return 1;
        }                    
    }  
    return 0;
  }    
  if (cllen1>cllen2) return 0;
  
  // now both clauses are nonunit rule clauses and we do full subsumption
  // prepare for subsumption: set globals etc 
#ifdef DEBUG  
  printf("general subsumption case \n");
#endif  
  g->tmp_unify_vc=(g->varstack)+1;  // var counter for varstack   
  // clear lit information vector (0 pos holds vec len)
  for(i2=1;i2<=cllen2;i2++) (g->tmp_litinf_vec)=wr_vec_store(g,g->tmp_litinf_vec,i2,0);
  ++(g->stat_clsubs_full_attempted);
  mres=wr_subsume_cl_aux(g,cl1,cl2,
                   cl1+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN,
                   cl2+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN,
                   0,0,
                   cllen1,cllen2,
                   uniquestrflag);
  wr_clear_varstack(g,g->varstack);
  return mres;  
}   



/*
  Each gcl literal must match a different scl literal.


  Take first gcl literal glit
    Loop over all scl literals     
      if match found glit<->slit and
         recursive subsumption without glit and slit present is OK
      then return OK
      else restore variable settings before last match attempt
    If match found during internal loop 

    in other words

  Loop over all gcl literals
    Loop over all scl literals     
      if match found glit<->slit then stop internal loop
      else restore variable settings before last match attempt

*/

gint wr_subsume_cl_aux(glb* g,gptr cl1vec, gptr cl2vec, 
	                  gptr litpt1, gptr litpt2, 
	                  int litind1, int litind2, 
                    int cllen1, int cllen2,
                    int uniquestrflag) {
  int i1,i2; 
  gint lit1,lit2;
  gptr pt1,pt2;  
  gint meta1,meta2;	    
  int foundflag;	    
  int vc_tmp;	    
  int nobackflag;	    

#ifdef DEBUG 
  printf("wr_subsume_cl_aux called with litind1 %d \n",litind1);                     
#endif            
  if(litind1<cllen1) {   
    i1=litind1;
    pt1=litpt1;
    meta1=*(pt1+LIT_META_POS); 
    lit1=*(pt1+LIT_ATOM_POS);    
    nobackflag=0; // backtracing will be prohibited if match found without vars set
    for(i2=0,pt2=litpt2; !nobackflag && i2<cllen2; i2++,pt2+=LIT_WIDTH) {
#ifdef DEBUG      
      printf("cp0 i2 %d nobackflag %d cllen2 %d\n",i2,nobackflag,cllen2);
#endif      
      if ((g->tmp_litinf_vec)[i2+1]==0) {
	      // literal not bound by subsumption yet
        meta2=*(pt2+LIT_META_POS); 
        lit2=*(pt2+LIT_ATOM_POS);         
        foundflag=0;    
        if (!litmeta_negpolarities(meta1,meta2)) {          
          vc_tmp=*(g->tmp_unify_vc); // store current value of varstack pointer ????????
          if (wr_match_term_aux(g,lit1,lit2,uniquestrflag)) {
#ifdef DEBUG             
	          printf("lit match ok with *(g->tmp_unify_vc): %d\n",*(g->tmp_unify_vc));
            wr_print_vardata(g);
#endif            
            // literals were successfully matched
	          (g->tmp_litinf_vec)[i2+1]=1; // mark as a bound literal
	          if (vc_tmp==*(g->tmp_unify_vc)) nobackflag=1; // no need to backtrack
            if ((i1+1>=cllen1) ||
	               wr_subsume_cl_aux(g,cl1vec,cl2vec,pt1+(LIT_WIDTH),litpt2,
	                             i1+1,i2,cllen1,cllen2,uniquestrflag)) {
	            // found a right match for current gcl lit
#ifdef DEBUG                                  
              printf("rest ok with *(g->tmp_unify_vc): %d\n",*(g->tmp_unify_vc));		
              wr_print_vardata(g);                                  
#endif                                 
	            return 1;	    
	          } 
#ifdef DEBUG             
	          printf("rest failed with *(g->tmp_unify_vc): %d\n",*(g->tmp_unify_vc));
            wr_print_vardata(g);
#endif            
	          if (vc_tmp!=*(g->tmp_unify_vc)) wr_clear_varstack_topslice(g,g->varstack,vc_tmp);
            (g->tmp_litinf_vec)[i2+1]=0; // clear as a bound literal	      
          } else {
   	        //printf("lit match failed with *(g->tmp_unify_vc): %d\n",*(g->tmp_unify_vc));
  	        if (vc_tmp!=*(g->tmp_unify_vc)) wr_clear_varstack_topslice(g,g->varstack,vc_tmp);	 
          }		  
        } 
      }		
    }
    // all literals checked, no way to subsume using current lit1
    return 0;        
  }
  // clause 
  printf("REASONER ERROR! something wrong in calling wr_subsume_cl_aux\n");
  //printf("litind1: %d cllen1: %d i1: %d \n", litind1,cllen1,i1);  
  return 0;  
}     
  

#ifdef __cplusplus
}
#endif
