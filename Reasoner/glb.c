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

glb* wr_glb_new(void* db) {     
  glb* g;  
  
  g=sys_malloc(sizeof(glb)); // allocate space
  (g->db)=db;          // store database pointer to glb structure
  wr_glb_init_simple(g);  // fills in simple values (ints, strings etc)
  wr_glb_init_shared_complex(g); // creates and fills in shared tables, substructures, etc
  wr_glb_init_local_complex(g); // creates and fills in local tables, substructures, etc
  
  return g;
  
}  
  

/** Fills in simple slots of glb structure.
*
*/

int wr_glb_init_simple(glb* g) {     
  
  (g->pick_given_queue_ratio)=4;
  (g->pick_given_queue_ratio_counter)=0;
  (g->cl_keep_weightlimit)=100000;
  
  (g->unify_samelen)=1;
  (g->unify_maxuseterms)=0;
  (g->unitres_strat)=0;
  (g->negpref_strat)=1;
  (g->pospref_strat)=0;
  //(g->cl_maxweight)=1000000;
  //(g->cl_maxdepth)=1000000;
  //(g->cl_limitkept)=1;
  
  (g->pick_given_queue_ratio)=4;
  (g->pick_given_queue_ratio_counter)=0;
  
  //(g->last_forw_subsumer)=NULL;
  (g->proof_found)=0;
  /*
  (g->last_start_datetime)=0;
  (g->last_start_usec)=0;
  
  (g->this_start_datetime)=0;
  (g->this_start_usec)=0;
  */
  (g->build_subst)=1;    // subst var values into vars
  (g->build_calc)=1;     // do fun and pred calculations
  (g->build_dcopy)=0;    // copy nonimmediate data (vs return ptr)
  (g->build_buffer)=NULL; // build everything into local tmp buffer area (vs main area)
  
  (g->stat_built_cl)=0;
  (g->stat_derived_cl)=0;
  (g->stat_binres_derived_cl)=0;
  (g->stat_factor_derived_cl)=0;
  (g->stat_kept_cl)=0;
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
  int i;
  
  (g->clbuilt)=rpto(g,wr_cvec_new(g,100));
  (g->clqueue)=rpto(g,wr_cvec_new(g,100));
  (g->clqueue_given)=1;  
  (g->clactive)=rpto(g,wr_cvec_new(g,100));
  
  (g->clweightqueue)=rpto(g,wr_vec_new(g,NROF_WEIGHTQUEUE_ELS));  
  
  (g->hash_neg_atoms)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  (g->hash_pos_atoms)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  (g->hash_units)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  (g->hash_para_terms)=rpto(g,wr_vec_new(g,NROF_CLTERM_HASHVEC_ELS)); 
  
  return 0; 
}  



/** Fills in local complex slots of glb structure.
*
*/

int wr_glb_init_local_complex(glb* g) {     
  int i;

  
  //(g->varstrvec)=wr_vec_new(g,3);  
 
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

  //(g->derived_termbuf_freeindex)=2;
  
  //(g->use_termbuf)=0;
  
  //(g->pick_given_queue_ratio)=4;
  //(g->pick_given_queue_ratio_counter)=0;
  
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

  
  wr_vec_free(g,rotp(g,g->clqueue)); 
  
  wr_vec_free(g,rotp(g,g->clweightqueue)); 

  wr_clterm_hashlist_free(g,rotp(g,g->hash_neg_atoms));  
  wr_clterm_hashlist_free(g,rotp(g,g->hash_pos_atoms)); 
  wr_clterm_hashlist_free(g,rotp(g,g->hash_units));
  wr_clterm_hashlist_free(g,rotp(g,g->hash_para_terms));
  
  wr_vec_free(g,rotp(g,g->clactive));     
  
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
  
  return 0;
}  

#ifdef __cplusplus
}
#endif
