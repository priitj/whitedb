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


 /** @file glb.h
 *  Reasoner globals.
 *
 */


#ifndef __defined_glb_h
#define __defined_glb_h

#include "types.h"


#define NROF_DYNALLOCINITIAL_ELS  10 

#define NROF_GIVEN_TERMBUF_ELS      100000
#define NROF_DERIVED_TERMBUF_ELS    100000
#define NROF_QUEUE_TERMBUF_ELS   100000000
#define NROF_ACTIVE_TERMBUF_ELS   10000000


#define NROF_VARBANKS   5 
#define NROF_VARSINBANK 1000
#define FIRST_UNREAL_VAR_NR ((NROF_VARBANKS-1)*NROF_VARSINBANK)

#define NROF_WEIGHTQUEUE_ELS 50
#define NROF_CLTERM_HASHVEC_ELS 500

#define MAX_CLAUSE_LEN 1000



/* ======== Structures ========== */

/** glb contains global values for requests.
* 
* requests should use a single global variable g, which
* is a pointer to glb structure.
* 
*/

typedef struct {    
  
  void* db;             /**< shared mem database */  
  
  /* === shared data block === */
  
  cveco clbuilt;        /**< vector containing built clauses, newest last. 0: vec len, 1: index of next unused vec elem */
  cveco clactive;  
  cveco clpickstack;   /**< vector containing built clause stack to be selected as given before using queue (hyperres eg) */
  cveco clqueue;        /**< vector containing kept clauses, newest last. 0: vec len, 1: index of next unused vec elem */
  gint clqueue_given;  /**< index of next clause to be taken from clqueue */     
  
  veco clweightqueue;
  veco hash_neg_atoms;
  veco hash_pos_atoms;
  veco hash_units;
  veco hash_para_terms;
  
  /* == local data block === */
       
  gint proof_found;
  gint alloc_err; // set to 1 in case of alloc errors: should cancel search  
  
  vec varbanks; // 0: input (passive), 1: given clause renamed, 
                // 2: active clauses, 3: derived clauses, 
                // 4: tmp rename area (vals always UNASSIGNED, never set to other vals!)
  cvec varstack;                  
    
  //int tmp_build_weight;
  //vec tmp1_cl_vec;
  //vec tmp2_cl_vec;
  
  vec tmp_litinf_vec;  // used by subsumption  
  
  cvec given_termbuf;
  cvec derived_termbuf;
  cvec queue_termbuf;
  cvec active_termbuf;
  
  /* unification/matching configuration */
  
  int unify_samelen;      // 1 if unifiable terms need not have same length, 0 otherwise
  int unify_maxuseterms;  // max nr of rec elems unified one after another: t1,t2,t3 gives 3
                          // 0 if no limit 
  int unify_firstuseterm; // rec pos where we start to unify 
  int unify_funpos;       // rec pos of a fun/pred uri
  int unify_funarg1pos;   // rec pos of a fun/pred first arg
  int unify_funarg2pos;   // rec pos of a fun/pred second arg
  int unify_footerlen;    // obligatory amount of unused gints to add to end of each created term 
  
  /* strategy selection */
  
  int pick_given_queue_ratio;
  int pick_given_queue_ratio_counter;
  int cl_keep_weightlimit; 
  
  int hyperres_strat;
  int unitres_strat;
  int negpref_strat;
  int pospref_strat;  
  int use_comp_funs_strat; // general strategy
  int use_comp_funs; // current principle
  
  //int cl_maxweight;
  //int cl_maxdepth;
  //int cl_limitkept;
  
  /*  printout configuration */
  
  int print_flag;
  int print_level_flag;
  
  int parser_print_level;
  int print_initial_parser_result;
  int print_generic_parser_result;
  
  int print_initial_active_list;
  int print_initial_passive_list;
  
  int print_initial_given_cl;
  int print_final_given_cl;
  int print_active_cl;
  int print_partial_derived_cl;
  int print_derived_cl;
  
  int print_clause_detaillevel;
  int print_stats;
  
  /* tmp variables */
  
  gint* tmp_unify_vc;       // var count in unification
  gint  tmp_unify_occcheck; // occcheck necessity in unification (changes)
  gint  tmp_unify_do_occcheck;
  
  /* build control: changed in code */
  
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
  
  /* statistics */
  
  int stat_wr_mallocs;
  int stat_wr_reallocs;
  int stat_wr_frees;
  int stat_wr_malloc_bytes;
  int stat_wr_realloc_bytes;
  
  int stat_built_cl;
  int stat_derived_cl;
  int stat_binres_derived_cl;
  int stat_factor_derived_cl;
  int stat_kept_cl;
  int stat_hyperres_partial_cl;
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


glb* wr_glb_new_full(void* db);
glb* wr_glb_new_simple(void* db);

int wr_glb_free(glb* g);

int wr_glb_init_simple(glb* g);

int wr_glb_init_shared_simple(glb* g);
int wr_glb_init_shared_complex(glb* g);
int wr_glb_free_shared_simple(glb* g);
int wr_glb_free_shared_complex(glb* g);

int wr_glb_init_local_simple(glb* g);
int wr_glb_init_local_complex(glb* g);
int wr_glb_free_local_simple(glb* g);
int wr_glb_free_local_complex(glb* g);

#endif
