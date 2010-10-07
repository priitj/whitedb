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


 /** @file glb.h
 *  Reasoner globals.
 *
 */


#ifndef __defined_glb_h
#define __defined_glb_h

#include "types.h"

//#define INITIAL_MALLOC_SIZE 10000000 //  this amount of bytes taken for heap 

//#define GMAX_PROCESSES 10
//#define PROCG (g)

//#define NROF_SCM_PROTECTED 5

#define NROF_GIVEN_TERMBUF_ELS      100000
#define NROF_DERIVED_TERMBUF_ELS    100000
#define NROF_QUEUE_TERMBUF_ELS    10000000
#define NROF_ACTIVE_TERMBUF_ELS    1000000


#define NROF_VARBANKS   4 
#define NROF_VARSINBANK 10

#define NROF_WEIGHTQUEUE_ELS 50
#define NROF_CLTERM_HASHVEC_ELS 50


//#define WG_MAX_MEMDBASE_ROW 0  // purely for Gandalf memdbase handling test-limit



/* ======== Structures ========== */

/** glb contains global values for requests.
* 
* requests should use a single global variable g, which
* is a pointer to glb structure.
* 
*/

typedef struct {    
  
  void* db;             /**< shared mem database */  
  
  /* shared data block */
  
  cveco clbuilt;        /**< vector containing built clauses, newest last. 0: vec len, 1: index of next unused vec elem */
  cveco clqueue;        /**< vector containing kept clauses, newest last. 0: vec len, 1: index of next unused vec elem */
  gint clqueue_given;  /**< index of next clause to be taken from clqueue */  
  cveco clactive;  
  
  veco clweightqueue;
  // vec res_terms;
  veco hash_neg_atoms;
  veco hash_pos_atoms;
  veco hash_units;
  veco hash_para_terms;
  
  /* local data block */
  
  int pick_given_queue_ratio;
  int pick_given_queue_ratio_counter;
  int cl_keep_weightlimit;    
  
  gint proof_found;    
  
  //void*  varstrvec;
  vec varbanks; //0: input, 1: given clause renamed, 2: active clauses, 3: derived clauses
  //void*  varbankrepl;
  cvec varstack;            
  
  gint* tmp_unify_vc;       // var count in unification
  gint  tmp_unify_occcheck; // occcheck necessity in unification (changes)
  gint  tmp_unify_do_occcheck;
  
  gint build_subst;    // subst var values into vars
  gint build_calc;     // do fun and pred calculations
  gint build_dcopy;    // copy nonimmediate data (vs return ptr)
  gptr build_buffer;   // build everything into tmp buffer (vs main area)
                       // points to NULL or given_termbuf, derived_termbuf etc
  
  gint build_rename;   // do var renaming
  gint build_rename_maxseenvnr; // tmp var for var renaming
  gint build_rename_vc;    // tmp var for var renaming
  gptr build_rename_bank;  // points to bank of created vars
  gint build_rename_banknr; // nr of bank of created vars
  
    
  //int tmp_build_weight;
  //vec tmp1_cl_vec;
  //vec tmp2_cl_vec;
  
  //vec tmp_litinf_vec;    
  
  cvec given_termbuf;
  cvec derived_termbuf;
  cvec queue_termbuf;
  cvec active_termbuf;
  
  int unify_samelen;
  int unify_maxuseterms;
  int unitres_strat;
  int negpref_strat;
  int pospref_strat;
  //int cl_maxweight;
  //int cl_maxdepth;
  //int cl_limitkept;
  
  //vec last_forw_subsumer;
  
  //int last_start_datetime;
  //int last_start_usec;
  //int last_end_datetime;
  //int last_end_usec;
  //int this_start_datetime;
  //int this_start_usec;
  
  int stat_built_cl;
  int stat_derived_cl;
  int stat_binres_derived_cl;
  int stat_factor_derived_cl;
  int stat_kept_cl;
  int stat_weight_discarded_building;
  int stat_weight_discarded_cl;
  int stat_internlimit_discarded_cl;
  int stat_given_candidates;
  int stat_given_used;
  int stat_simplified_given;
  int stat_simplified_derived;
  int stat_backward_subsumed;
  int stat_clsubs_attempted;
  int stat_clsubs_meta_attempted;
  int stat_clsubs_predsymbs_attempted;
  int stat_clsubs_unit_attempted;
  int stat_clsubs_full_attempted;
  int stat_lit_hash_computed;
  int stat_lit_hash_match_found;
  int stat_lit_hash_match_miss;
  int stat_lit_hash_cut_ok;  
  int stat_lit_hash_subsume_ok;
  
  int log_level;
} glb;



/* === Protos for funs in glb.c === */


glb* wr_glb_new(void* db);
int wr_glb_free(glb* g);

int wr_glb_init_shared_simple(glb* g);
int wr_glb_init_shared_complex(glb* g);
int wr_glb_free_shared_simple(glb* g);
int wr_glb_free_shared_complex(glb* g);

int wr_glb_init_local_simple(glb* g);
int wr_glb_init_local_complex(glb* g);
int wr_glb_free_local_simple(glb* g);
int wr_glb_free_local_complex(glb* g);

#endif
