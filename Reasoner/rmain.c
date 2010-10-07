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

 /** @file rmain.c
 *  Reasoner top level: initialisation and startup.
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

static int init_active_passive_lists_std(glb* g);
static int init_active_passive_lists_factactive(glb* g);
static int init_active_passive_lists_ruleactive(glb* g);


/* ====== Functions ============== */

int wg_run_reasoner(void *db, int argc, char **argv) {
  glb* g;
  int res=1;

  dprintf("wg_run_reasoner starts\n"); 
  g=wg_init_reasoner(db,argc,argv);
  res=wr_genloop(g);
  printf("\nresult %d\n",res);
  printf("----------------------------------\n");
  wr_show_stats(g);
  printf("----------------------------------\n");
  wr_glb_free(g);  
  dprintf("wg_run_reasoner returns res %d\n",res); 
  return res;
  
}  


glb* wg_init_reasoner(void *db, int argc, char **argv) {
  glb* g;
    
  dprintf("init starts\n");
  g=wr_glb_new(db);
  dprintf("glb made\n");
  dprintf("cycling over clauses to make active passive lists\n"); 
  init_active_passive_lists_std(g);
  //init_active_passive_lists_factactive(g);   
  //init_active_passive_lists_ruleactive(g);
  dprintf("active passive lists made\n");  
  return g;  
}  


static int init_active_passive_lists_std(glb* g) {
  void *rec;
  void* db=(g->db);
  //int i;
  
  (g->proof_found)=0;
  //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
#ifdef DEBUG      
      wg_print_rule_clause_otter(db, (gint *) rec);
      printf("\n"); 
#endif       
      wr_push_clqueue_cl(g,rec);
    } else if (wg_rec_is_fact_clause(db,rec)) {
#ifdef DEBUG      
      wg_print_fact_clause_otter(db, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clqueue_cl(g,rec);
    }               
    //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
    rec = wg_get_next_raw_record(db,rec);    
  }
}

static int init_active_passive_lists_factactive(glb* g) {
  void *rec;
  void* db=(g->db);
  //int i;
  
  (g->proof_found)=0;
  //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
#ifdef DEBUG       
      wg_print_rule_clause_otter(db, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clqueue_cl(g,rec);      
    } else if (wg_rec_is_fact_clause(db,rec)) {
#ifdef DEBUG       
      wg_print_fact_clause_otter(db, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clactive_cl(g,rec);
    }               
    //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
    rec = wg_get_next_raw_record(db,rec);    
  }
}


static int init_active_passive_lists_ruleactive(glb* g) {
  void *rec;
  void* db=(g->db);
  //int i;
  
  (g->proof_found)=0;
  //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
#ifdef DEBUG       
      wg_print_rule_clause_otter(db, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clactive_cl(g,rec);      
    } else if (wg_rec_is_fact_clause(db,rec)) {
#ifdef DEBUG       
      wg_print_fact_clause_otter(db, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clqueue_cl(g,rec);
    }               
    //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
    rec = wg_get_next_raw_record(db,rec);    
  }
}



/* -----------------------------------------------

   Some funs for statistics 
   ----------------------------------------------- */

void wr_show_stats(glb* g) {
  
  printf("stat_given_used: %d\n",g->stat_given_used);
  printf("stat_given_candidates: %d\n",g->stat_given_candidates); 
  //printf("stat_derived_cl: %d\n",g->stat_derived_cl);
  printf("stat_binres_derived_cl: %d\n",g->stat_binres_derived_cl);
  printf("stat_factor_derived_cl: %d\n",g->stat_factor_derived_cl);
  printf("stat_kept_cl: %d\n",g->stat_kept_cl);
  printf("stat_weight_discarded_building: %d\n",g->stat_weight_discarded_building);
  printf("stat_weight_discarded_cl: %d\n",g->stat_weight_discarded_cl);
  printf("stat_internlimit_discarded_cl: %d\n",g->stat_internlimit_discarded_cl);
  printf("stat_simplified:  %d derived %d given\n",
         g->stat_simplified_derived, g->stat_simplified_given);  
  printf("stat_backward_subsumed: %d\n",g->stat_backward_subsumed);
  printf("stat_built_cl: %d\n",g->stat_built_cl);
#ifdef SHOW_SUBSUME_STATS  
  printf("stat_clsubs_attempted:           %20d\n",g->stat_clsubs_attempted);  
  printf("stat_clsubs_meta_attempted:      %20d\n",g->stat_clsubs_meta_attempted);
  printf("stat_clsubs_predsymbs_attempted: %20d\n",g->stat_clsubs_predsymbs_attempted);
  printf("stat_clsubs_unit_attempted:      %20d\n",g->stat_clsubs_unit_attempted);
  printf("stat_clsubs_full_attempted:      %20d\n",g->stat_clsubs_full_attempted);
#endif 
#ifdef SHOW_HASH_CUT_STATS  
  printf("stat_lit_hash_computed:     %20d\n",g->stat_lit_hash_computed);
  printf("stat_lit_hash_match_found:  %20d\n",g->stat_lit_hash_match_found);
  printf("stat_lit_hash_match_miss:   %20d\n",g->stat_lit_hash_match_miss); 
  printf("stat_lit_hash_cut_ok:       %20d\n",g->stat_lit_hash_cut_ok); 
  printf("stat_lit_hash_subsume_ok:   %20d\n",g->stat_lit_hash_subsume_ok); 
#endif
}  


#ifdef __cplusplus
}
#endif
