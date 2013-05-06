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

 /** @file glb.c
 *  Reasoner globals.
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


/** Creates and fills in the glb structure.
*
*/

glb* wr_glb_new_full(void* db) {     
  glb* g;
  int tmp;  
   
  g=wr_glb_new_simple(db);
  if (g==NULL) return NULL;
  tmp=wr_glb_init_shared_complex(g); // creates and fills in shared tables, substructures, etc
  if (tmp) {
    wr_glb_free_shared_complex(g);
    sys_free(g);
    return NULL; 
  }  
  tmp=wr_glb_init_local_complex(g); // creates and fills in local tables, substructures, etc
  if (tmp) {
    wr_glb_free_shared_complex(g);
    wr_glb_free_local_complex(g);
    sys_free(g);
    return NULL; 
  } 
  return g;
  
}  
  
glb* wr_glb_new_simple(void* db) {     
  glb* g;  
  
  g=sys_malloc(sizeof(glb)); // allocate space
  if (g==NULL) return NULL;  
  (g->db)=db;          // store database pointer to glb structure
  wr_glb_init_simple(g);  // fills in simple values (ints, strings etc)   
  return g;  
} 

/** Fills in simple slots of glb structure.
*
*/

int wr_glb_init_simple(glb* g) {         
  
  (g->proof_found)=0; // becomes 1 if proof found
  (g->alloc_err)=0; // 0 if ok, becomes 1 or larger if alloc error occurs:
                    // 3 out of varspace err
  
  /* unification/matching configuration */
  
  (g->unify_samelen)=1;
  (g->unify_maxuseterms)=0;
  (g->unify_firstuseterm)=1;
  (g->unify_funpos)=1;
  (g->unify_funarg1pos)=2; // rec pos of a fun/pred first arg
  (g->unify_funarg2pos)=3; // rec pos of a fun/pred second arg
  (g->unify_footerlen)=0; 
  
  /* strategy selection */    

  (g->hyperres_strat)=1;
  (g->pick_given_queue_ratio)=4;
  (g->pick_given_queue_ratio_counter)=0;
  (g->cl_keep_weightlimit)=100000;
  
  (g->unitres_strat)=0;
  (g->negpref_strat)=1;
  (g->pospref_strat)=0;
  (g->use_comp_funs_strat)=1;
  (g->use_comp_funs)=1;
  //(g->cl_maxweight)=1000000;
  //(g->cl_maxdepth)=1000000;
  //(g->cl_limitkept)=1;
  
  /*  printout configuration */
  
  (g->print_flag)=1; // if 0: no printout: rmain sets other flags accordingly
  (g->print_level_flag)=-1; // rmain uses this to set other flags accordingly 
                           // -1: use default, 0: none, 10: normal, 20: medium, 30: detailed
  
  (g->parser_print_level)=1;
  (g->print_initial_parser_result)=1;
  (g->print_generic_parser_result)=1;
  
  (g->print_initial_active_list)=1;
  (g->print_initial_passive_list)=1;
  
  (g->print_initial_given_cl)=1;
  (g->print_final_given_cl)=1;
  (g->print_active_cl)=1;
  (g->print_partial_derived_cl)=1;
  (g->print_derived_cl)=1;
  
  (g->print_clause_detaillevel)=1;
  (g->print_stats)=1;
  
  /* tmp variables */
  
  /* build control: changed in code */
  
  (g->build_subst)=1;    // subst var values into vars
  (g->build_calc)=1;     // do fun and pred calculations
  (g->build_dcopy)=0;    // copy nonimmediate data (vs return ptr)
  (g->build_buffer)=NULL; // build everything into local tmp buffer area (vs main area)
  
  /* statistics */
  
  (g->stat_wr_mallocs)=0;
  (g->stat_wr_reallocs)=0;
  (g->stat_wr_frees)=0;
  (g->stat_wr_malloc_bytes)=0;
  (g->stat_wr_realloc_bytes)=0;
  
  (g->stat_built_cl)=0;
  (g->stat_derived_cl)=0;
  (g->stat_binres_derived_cl)=0;
  (g->stat_factor_derived_cl)=0;
  (g->stat_kept_cl)=0;
  (g->stat_hyperres_partial_cl)=0;
  (g->stat_weight_discarded_building)=0;
  (g->stat_weight_discarded_cl)=0;
  (g->stat_internlimit_discarded_cl)=0;
  (g->stat_given_candidates)=0;
  (g->stat_given_used)=0;
  (g->stat_simplified_given)=0;
  (g->stat_simplified_derived)=0;
  (g->stat_backward_subsumed)=0;
  
  (g->stat_clsubs_attempted)=0;
  (g->stat_clsubs_meta_attempted)=0;
  (g->stat_clsubs_predsymbs_attempted)=0;
  (g->stat_clsubs_unit_attempted)=0;
  (g->stat_clsubs_full_attempted)=0;
  
  (g->stat_lit_hash_computed)=0;
  (g->stat_lit_hash_match_found)=0;
  (g->stat_lit_hash_match_miss)=0;
  (g->stat_lit_hash_cut_ok)=0;    
  (g->stat_lit_hash_subsume_ok)=0;  
  
  return 0;
}  


/** Fills in shared complex slots of glb structure.
*
*/

int wr_glb_init_shared_complex(glb* g) {     
  
  // first NULL all vars
  
  (g->clbuilt)=(gint)NULL;
  (g->clqueue)=(gint)NULL;
  (g->clqueue_given)=(gint)NULL; 
  (g->clpickstack)=(gint)NULL;  
  (g->clactive)=(gint)NULL;
  (g->clweightqueue)=(gint)NULL;  
  (g->hash_neg_atoms)=(gint)NULL; 
  (g->hash_pos_atoms)=(gint)NULL; 
  (g->hash_units)=(gint)NULL; 
  (g->hash_para_terms)=(gint)NULL; 
  
  // then create space 
  
  (g->clbuilt)=rpto(g,wr_cvec_new(g,NROF_DYNALLOCINITIAL_ELS));  
  (g->clactive)=rpto(g,wr_cvec_new(g,NROF_DYNALLOCINITIAL_ELS));
  (g->clpickstack)=rpto(g,wr_cvec_new(g,NROF_DYNALLOCINITIAL_ELS));  
  (g->clqueue)=rpto(g,wr_cvec_new(g,NROF_DYNALLOCINITIAL_ELS));
  (g->clqueue_given)=1;
  
  (g->clweightqueue)=rpto(g,wr_vec_new(g,NROF_WEIGHTQUEUE_ELS));  
  
  (g->hash_neg_atoms)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  (g->hash_pos_atoms)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  (g->hash_units)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  (g->hash_para_terms)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  
  if (g->alloc_err) {
    return 1;
  }  
      
  return 0; 
}  



/** Fills in local complex slots of glb structure.
*
*/

int wr_glb_init_local_complex(glb* g) {     
    
  // first NULL all vars
  
  (g->varbanks)=NULL;
  (g->varstack)=NULL;         
  (g->given_termbuf)=NULL;
  (g->derived_termbuf)=NULL;
  (g->queue_termbuf)=NULL;
  (g->active_termbuf)=NULL;
  (g->tmp_litinf_vec)=NULL; 
  
  // then create space
  
  (g->varbanks)=wr_vec_new(g,NROF_VARBANKS*NROF_VARSINBANK);
  //(g->varbankrepl)=wr_vec_new(g,3*NROF_VARSINBANK);
  (g->varstack)=wr_cvec_new(g,NROF_VARBANKS*NROF_VARSINBANK); 
  (g->varstack)[1]=2; // first free elem
 
  //(g->tmp1_cl_vec)=wr_vec_new(g,100);    
  //(g->tmp2_cl_vec)=wr_vec_new(g,100); 

  //(g->tmp_litinf_vec)=wr_vec_new(g,100); 
        
  (g->given_termbuf)=wr_cvec_new(g,NROF_GIVEN_TERMBUF_ELS);
  (g->given_termbuf)[1]=2;
  //(g->given_termbuf_freeindex)=2;
  
  (g->derived_termbuf)=wr_cvec_new(g,NROF_DERIVED_TERMBUF_ELS);
  (g->derived_termbuf)[1]=2;
 
  (g->queue_termbuf)=wr_cvec_new(g,NROF_QUEUE_TERMBUF_ELS);
  (g->queue_termbuf)[1]=2;
  
  (g->active_termbuf)=wr_cvec_new(g,NROF_ACTIVE_TERMBUF_ELS);
  (g->active_termbuf)[1]=2;

  (g->tmp_litinf_vec)=wr_vec_new(g,MAX_CLAUSE_LEN); // used by subsumption
  
  //(g->derived_termbuf_freeindex)=2;
  
  //(g->use_termbuf)=0;
  
  //(g->pick_given_queue_ratio)=4;
  //(g->pick_given_queue_ratio_counter)=0;
  
  if ((g->alloc_err)==1) {
    return 1;
  }   
  return 0; 
}  

/** Frees the glb structure and subitems in glb.
*
*/  

int wr_glb_free(glb* g) {
  
  // first free subitems
    
  wr_glb_free_shared_simple(g);
  wr_glb_free_shared_complex(g);
  wr_glb_free_local_complex(g);
  wr_glb_free_local_simple(g);
  
  sys_free(g); // free whole spaces
  return 0;
}  

/** Frees the glb shared simple subitems.
*
*/  

int wr_glb_free_shared_simple(glb* g) {
  
  //str_freeref(g,&(g->info)); 

  return 0;  
}

/** Frees the glb local simple subitems.
*
*/  

int wr_glb_free_local_simple(glb* g) {
  
  //str_freeref(g,&(g->info)); 

  return 0;  
}  


/** Frees the glb shared complex subitems.
*
*/  

int wr_glb_free_shared_complex(glb* g) {

  wr_vec_free(g,rotp(g,g->clbuilt)); 
  wr_vec_free(g,rotp(g,g->clactive));   
  wr_vec_free(g,rotp(g,g->clpickstack));
  wr_vec_free(g,rotp(g,g->clqueue));   
  wr_vec_free(g,rotp(g,g->clweightqueue)); 

  wr_clterm_hashlist_free(g,rotp(g,g->hash_neg_atoms));  
  wr_clterm_hashlist_free(g,rotp(g,g->hash_pos_atoms)); 
  wr_clterm_hashlist_free(g,rotp(g,g->hash_units));
  wr_clterm_hashlist_free(g,rotp(g,g->hash_para_terms));
  
    
  
  return 0;
}  

/** Frees the local glb complex subitems.
*
*/  

int wr_glb_free_local_complex(glb* g) {
 
  
  //wr_vec_free(g,g->varstrvec);  
 
  wr_vec_free(g,g->varbanks); 
  //wr_vec_free(g,g->varbankrepl); 
  wr_vec_free(g,g->varstack); 
 
  //wr_vec_free(g,g->tmp1_cl_vec);     
  //wr_vec_free(g,g->tmp2_cl_vec); 

  //wr_vec_free(g,g->tmp_litinf_vec);  
    
  //wr_vec_free(g,g->termbuf); 
  
  wr_vec_free(g,g->given_termbuf);
  wr_vec_free(g,g->derived_termbuf);
  wr_vec_free(g,g->queue_termbuf);
  wr_vec_free(g,g->active_termbuf);
  
  wr_vec_free(g,g->tmp_litinf_vec);
  
  return 0;
}  

#ifdef __cplusplus
}
#endif
