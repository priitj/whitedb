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
#include "../Parser/dbparse.h" 

  
/* ====== Private headers and defs ======== */

static void wr_set_no_printout(glb* g);
static void wr_set_normal_printout(glb* g);
static void wr_set_medium_printout(glb* g);
static void wr_set_detailed_printout(glb* g);  
  
#define SHOW_SUBSUME_STATS
#define SHOW_MEM_STATS
  
/* ======= Private protos ================ */



/* ====== Functions ============== */

  

  
int wg_run_reasoner(void *db, int argc, char **argv) {
  glb* g;
  int res=1;
  int default_print_level=30;

  dprintf("wg_run_reasoner starts\n"); 
  g=wg_init_reasoner(db,argc,argv);
  if (g==NULL) {
    //printf("Error: cannot allocate enough memory during reasoner initialization\n");
    // printout in wg_init_reasoner
    return -1;
  }  
  if (!(g->print_flag)) (g->print_level_flag)=0;
  if ((g->print_level_flag)<0) (g->print_level_flag)=default_print_level;
  if ((g->print_level_flag)==0) wr_set_no_printout(g);
  else if ((g->print_level_flag)<=10) wr_set_normal_printout(g);
  else if ((g->print_level_flag)<=20) wr_set_medium_printout(g);
  else if ((g->print_level_flag)<=30) wr_set_detailed_printout(g);
  else wr_set_normal_printout(g);  
  res=wr_genloop(g);
  if (g->print_flag) { 
    if (res==0) {
      printf("** PROOF FOUND\n"); 
    } else if (res==1) {
      printf("** SEARCH FINISHED WITHOUT PROOF, RESULT CODE %d\n",res);      
    } else if (res==-1) {
      printf("** SEARCH CANCELLED: MEMORY OVERFLOW %d\n",res);
    } else if (res<0) {
      printf("** SEARCH CANCELLED, ERROR CODE %d\n",res);
    }      
    wr_show_stats(g);
  }  
  wr_glb_free(g);       
  return res;  
}  

int wg_import_otter_file(void *db, char* filename) {  
  glb* g;
  int res;

  dprintf("wg_import_otterfile starts\n"); 
  g=wr_glb_new_simple(db); // no complex values given to glb elements 
  if (g==NULL) return 1; 
  (g->parser_print_level)=0;
  (g->print_initial_parser_result)=0;
  (g->print_generic_parser_result)=0;
  res=wr_import_otter_file(g,filename,NULL,NULL);
  sys_free(g); // no complex values given to glb elements
  dprintf("wg_import_otterfile ends with res\n",res); 
  return res;  
}

int wg_import_prolog_file(void *db, char* filename) {  
  glb* g;
  int res;

  dprintf("wg_import_prologfile starts\n"); 
  g=wr_glb_new_simple(db); // no complex values given to glb elements 
  if (g==NULL) return 1; 
  res=wr_import_prolog_file(g,filename,NULL,NULL);
  sys_free(g); // no complex values given to glb elements
  dprintf("wg_import_prologfile ends with res\n",res); 
  return res;  
}

glb* wg_init_reasoner(void *db, int argc, char **argv) {
  glb* g;
    
  dprintf("init starts\n");
  g=wr_glb_new_full(db);
  if (g==NULL) {
    printf("Error: cannot allocate enough memory during reasoner initialization\n");
    return NULL;
  }
  dprintf("glb made\n");
  dprintf("cycling over clauses to make active passive lists\n"); 
  wr_init_active_passive_lists_std(g);
  //wr_init_active_passive_lists_factactive(g);   
  //wr_init_active_passive_lists_ruleactive(g);
  dprintf("active passive lists made\n");  
  return g;  
}  


int wr_init_active_passive_lists_std(glb* g) {
  void *rec;
  void* db=(g->db);
  //int i;
  
  (g->proof_found)=0;
  //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
#ifdef DEBUG      
      wr_print_rule_clause_otter(g, (gint *) rec,(g->print_clause_detaillevel));
      printf("\n"); 
#endif       
      wr_push_clqueue_cl(g,rec);
    } else if (wg_rec_is_fact_clause(db,rec)) {
#ifdef DEBUG      
      wr_print_fact_clause_otter(g, (gint *) rec,(g->print_clause_detaillevel));
      printf("\n"); 
#endif      
      wr_push_clqueue_cl(g,rec);
    }               
    //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
    rec = wg_get_next_raw_record(db,rec);    
  }
  return 0;
}

/*
int wr_init_active_passive_lists_factactive(glb* g) {
  void *rec;
  void* db=(g->db);
  //int i;
  
  (g->proof_found)=0;
  //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
#ifdef DEBUG       
      wr_print_rule_clause_otter(g, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clqueue_cl(g,rec);      
    } else if (wg_rec_is_fact_clause(db,rec)) {
#ifdef DEBUG       
      wr_print_fact_clause_otter(g, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clactive_cl(g,rec);
    }               
    //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
    rec = wg_get_next_raw_record(db,rec);    
  }
  return 0;
}
*/

/*
int wr_init_active_passive_lists_ruleactive(glb* g) {
  void *rec;
  void* db=(g->db);
  //int i;
  
  (g->proof_found)=0;
  //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
#ifdef DEBUG       
      wr_print_rule_clause_otter(g, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clactive_cl(g,rec);      
    } else if (wg_rec_is_fact_clause(db,rec)) {
#ifdef DEBUG       
      wr_print_fact_clause_otter(g, (gint *) rec);
      printf("\n"); 
#endif      
      wr_push_clqueue_cl(g,rec);
    }               
    //for(i=0;i<10;i++) printf(" %d ",(int)((rotp(g,g->clqueue))[i])); printf("\n");
    rec = wg_get_next_raw_record(db,rec);    
  }
  return 0;
}
*/


/* ------------------------

  Printout settings 
  
 -------------------------- */

static void wr_set_no_printout(glb* g) {
  (g->print_flag)=0;
  
  (g->parser_print_level)=0;
  (g->print_initial_parser_result)=0;
  (g->print_generic_parser_result)=0;
  
  (g->print_initial_active_list)=0;
  (g->print_initial_passive_list)=0;
  
  (g->print_initial_given_cl)=0;
  (g->print_final_given_cl)=0;
  (g->print_active_cl)=0;
  (g->print_partial_derived_cl)=0;
  (g->print_derived_cl)=0;
  
  (g->print_clause_detaillevel)=0;
  (g->print_stats)=0;
  
}


static void wr_set_normal_printout(glb* g) {
  (g->print_flag)=1;
  
  (g->parser_print_level)=0;
  (g->print_initial_parser_result)=0;
  (g->print_generic_parser_result)=0;
  
  (g->print_initial_active_list)=0;
  (g->print_initial_passive_list)=0;
  
  (g->print_initial_given_cl)=0;
  (g->print_final_given_cl)=1;
  (g->print_active_cl)=0;
  (g->print_partial_derived_cl)=0;
  (g->print_derived_cl)=0;
  
  (g->print_clause_detaillevel)=1;
  (g->print_stats)=1;
  
}  

static void wr_set_medium_printout(glb* g) {
  (g->print_flag)=1;
  
  (g->parser_print_level)=0;
  (g->print_initial_parser_result)=0;
  (g->print_generic_parser_result)=0;
  
  (g->print_initial_active_list)=1;
  (g->print_initial_passive_list)=1;
  
  (g->print_initial_given_cl)=1;
  (g->print_final_given_cl)=1;
  (g->print_active_cl)=1;
  (g->print_partial_derived_cl)=1;
  (g->print_derived_cl)=1;
  
  (g->print_clause_detaillevel)=1;
  (g->print_stats)=1;
  
}

static void wr_set_detailed_printout(glb* g) {
  (g->print_flag)=1;
  
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
  
}

/* -----------------------------------------------

   Some funs for statistics 
   ----------------------------------------------- */

void wr_show_stats(glb* g) {
  
  if (!(g->print_stats)) return;
  
  printf("statistics:\n");
  printf("----------------------------------\n");
  printf("stat_given_used: %d\n",g->stat_given_used);
  printf("stat_given_candidates: %d\n",g->stat_given_candidates); 
  //printf("stat_derived_cl: %d\n",g->stat_derived_cl);
  printf("stat_binres_derived_cl: %d\n",g->stat_binres_derived_cl);
  printf("stat_factor_derived_cl: %d\n",g->stat_factor_derived_cl);
  printf("stat_kept_cl: %d\n",g->stat_kept_cl);
  printf("stat_hyperres_partial_cl: %d\n",g->stat_hyperres_partial_cl);
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
#ifdef SHOW_MEM_STATS
  
  //if ((g->clbuilt)!=(gint)NULL) 
  //  printf("clbuilt els %d used %d\n",
  //         (rotp(g,g->clbuilt))[0],(rotp(g,g->clbuilt))[1]-1);
  if ((g->clqueue)!=(gint)NULL) 
    printf("clqueue els %d used %d\n",
           (rotp(g,g->clqueue))[0],(rotp(g,g->clqueue))[1]-1);
  if ((g->clactive)!=(gint)NULL) 
    printf("clactive els %d used %d\n",
           (rotp(g,g->clactive))[0],(rotp(g,g->clactive))[1]-1);
  //if ((g->clweightqueue)!=(gint)NULL) 
  //  printf("clweightqueue els %d used %d\n",((gptr)(g->clweightqueue))[0],((gptr)(g->clweightqueue))[1]-1);
  
  if ((g->queue_termbuf)!=(gint)NULL) 
    printf("queue_termbuf els %d used %d\n",(g->queue_termbuf)[0],(g->queue_termbuf)[1]-1);
  if ((g->active_termbuf)!=(gint)NULL) 
    printf("active_termbuf els %d used %d\n",(g->active_termbuf)[0],(g->active_termbuf)[1]-1);
  if ((g->varstack)!=(gint)NULL) 
    printf("varstack els %d last used %d\n",(g->varstack)[0],(g->varstack)[1]-1);
  if ((g->given_termbuf)!=(gint)NULL) 
    printf("given_termbuf els %d last used %d\n",(g->given_termbuf)[0],(g->given_termbuf)[1]-1);
  if ((g->derived_termbuf)!=(gint)NULL) 
    printf("derived_termbuf els %d last used %d\n",(g->derived_termbuf)[0],(g->derived_termbuf)[1]-1);

  printf("wr_mallocs: %d\n",(g->stat_wr_mallocs));
  printf("wr_reallocs: %d\n",(g->stat_wr_reallocs));
  printf("wr_frees: %d\n",(g->stat_wr_frees));
  printf("wr_malloc_bytes: %d\n",(g->stat_wr_malloc_bytes));
  printf("wr_realloc_bytes: %d\n",(g->stat_wr_realloc_bytes));
#endif  
  printf("----------------------------------\n");
}  


#ifdef __cplusplus
}
#endif
