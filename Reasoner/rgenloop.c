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

 /** @file wr_genloop.c
 *  Procedures for reasoner top level search loops: given-clause, usable, sos etc.
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
#include "../Db/dbhash.h"
#include "../Db/dblog.h"
#include "../Db/dbindex.h"
#include "../Db/dbcompare.h"
  
#include "rincludes.h"
 
  
/* ====== Private headers and defs ======== */

//#define DEBUG
#undef DEBUG
#undef QUIET
#define SHOWACTIVE 

#define USE_RES_TERMS // loop over active clauses in wr_resolve_binary_all_active

/* ======= Private protos ================ */
  
static void wr_process_given_cl_setupsubst(glb* g, gptr buf, gint banknr);  
static void wr_process_given_cl_cleanupsubst(glb* g);

/* ====== Functions ============== */



int wr_genloop(glb* g) {
  int i;
  gptr picked_given_cl_cand;
  gptr given_cl_cand; 
  gptr given_cl;  
  gint ipassive;
  gint iactive;
  gptr activecl;
  int given_kept_flag=0;  
  
#ifndef QUIET    
  printf("========= rwr_genloop starting ========= \n");   
#endif  
  
  //clear_active_cl_list(); // ???
  wr_clear_all_varbanks(g); 
    
#ifdef DEBUG  
  printf("-- initial passive list starts --  \n");
  //printf("len %d next %d \n",CVEC_LEN(rotp(g,g->clqueue)),CVEC_NEXT(rotp(g,g->clqueue)));
  i=CVEC_START;
  for(; i<CVEC_NEXT(rotp(g,g->clqueue)) ; ++i) {
    wr_print_clause(g,(rotp(g,g->clqueue))[i]);    
  }  
  printf("-- initial passive list ends -- \n");     
  printf("-- initial active list starts --  \n");
  //printf("len %d next %d \n",CVEC_LEN(rotp(g,g->clactive)),CVEC_NEXT(rotp(g,g->clactive)));
  i=CVEC_START;
  for(; i<CVEC_NEXT(rotp(g,g->clactive)) ; ++i) {
    wr_print_clause(g,(rotp(g,g->clactive))[i]);        
  }  
  printf("-- initial active list ends -- \n");   
  printf ("****************************************\n");
#endif   
  
  // loop until no more passive clauses available

  g->proof_found=0;
  g->clqueue_given=CVEC_START;
  given_kept_flag=1;    
  
  for(;;) {   
#ifndef QUIET      
    printf("\n======= outer wr_genloop cycle (given) starts ========\n"); 
#endif      
    picked_given_cl_cand=wr_pick_given_cl(g,given_kept_flag);             
#ifdef DEBUG       
    printf("picked_given_cl_cand: ");
    if (picked_given_cl_cand==NULL) printf("NULL\n");
    else printf("real clause\n"); //data_print(picked_given_cl_cand);
    printf("\n");    
#endif        
    if (picked_given_cl_cand==NULL) {
      printf("no more clauses available to be taken as given\n");
      return 1;
    }
    given_kept_flag=0;
    (g->stat_given_candidates)++; //stats    
    given_cl_cand=wr_activate_passive_cl(g,picked_given_cl_cand);  
    given_cl_cand=picked_given_cl_cand;
    //if (given_cl_cand==GNULL) printf("activated given_cl_cand==GNULL\n");    
    if (given_cl_cand==NULL) continue;    
#ifndef QUIET    
    printf("(g->stat_given_used): %d\n",(g->stat_given_used));
    printf("given cl candidate with nr %d : ",(g->stat_given_used));
    wr_print_clause(g,given_cl_cand);
    printf("\n");      
#endif    
    given_cl=wr_process_given_cl(g,given_cl_cand); 
    if (given_cl==NULL) continue;  
    // do all resolutions with the given clause
#ifdef USE_RES_TERMS
    // normal case: active loop is done inside the wr_resolve_binary_all_active    
    wr_resolve_binary_all_active(g,given_cl);    
    if (g->proof_found) {      	      
      return 0;
    }	
#else    
    // testing/experimenting case: loop explicitly over active clauses
    iactive=CVEC_START;
    for(; iactive<CVEC_NEXT(rotp(g,g->clactive)); iactive++) {
#ifndef QUIET      
    printf("\n----- inner wr_genloop cycle (active) starts ----------\n"); 
#endif       
      activecl=(gptr)((rotp(g,g->clactive))[iactive]);      
#ifdef SHOWACTIVE      
      printf("active cl nr %d: ",iactive);
      wr_print_clause(g,activecl);       
      //printf("\n");
#endif      
      //resolve_binary(g,given_cl,activecl);
      if ((g->proof_found)) {
        return 0;
      }	      
    }
#endif // USE_RES_TERMS
  } 
}  


gptr wr_pick_given_cl(glb* g, int given_kept_flag) {
  gptr cl;

  //printf("wr_pick_given_cl called with clqueue_given %d and given_kept_flag %d\n",(g->clqueue_given),given_kept_flag);
  //printf(" CVEC_NEXT(rotp(g,g->clqueue)) %d \n",CVEC_NEXT(rotp(g,g->clqueue)));
  printf("picking cl nr %d as given\n",g->clqueue_given);
  //if (g->clqueue_given>=4) exit(0);
  if (CVEC_NEXT(rotp(g,g->clqueue))>(g->clqueue_given)) {
    cl=(gptr)((rotp(g,g->clqueue))[g->clqueue_given]);           
    ++(g->clqueue_given); 
  } else {
    return NULL;
  }    
  
  //printf("wr_pick_given_cl exiting\n");
  
  return cl;
}


gptr wr_activate_passive_cl(glb* g, gptr picked_given_cl_cand) {
  
  return  picked_given_cl_cand;
} 

gptr wr_process_given_cl(glb* g, gptr given_cl_cand) {
  void* db=g->db;
  gptr given_cl; 
  gptr active_cl;

#ifdef DEBUG
  printf("wr_process_given_cl called with \n");
  printf("int %d type %d\n",given_cl_cand,wg_get_encoded_type(db,given_cl_cand));
  wg_print_record(db,given_cl_cand);
  wr_print_clause(g,given_cl_cand);  
#endif    
  wr_process_given_cl_setupsubst(g,g->given_termbuf,1);
  given_cl=wr_build_calc_cl(g,given_cl_cand);
  wr_process_given_cl_cleanupsubst(g);
  //wr_print_varbank(g,g->varbanks);
#ifdef DEBUG
  printf("rebuilt as \n");
  wg_print_record(db,given_cl);
  wr_print_clause(g,given_cl);  
#endif  
  if (1) {         
    wr_process_given_cl_setupsubst(g,g->active_termbuf,2);    
    active_cl=wr_build_calc_cl(g,given_cl_cand);
    wr_process_given_cl_cleanupsubst(g);
    //wr_print_varbank(g,g->varbanks);
    wr_push_clactive_cl(g,active_cl);              
    (g->stat_given_used)++;  // stats 
  }
  return given_cl;
} 


static void wr_process_given_cl_setupsubst(glb* g, gptr buf, gint banknr) {
  g->build_subst=0;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  (g->given_termbuf)[1]=2; // reuse given_termbuf
  g->build_buffer=buf;
  g->build_rename=1;   // do var renaming
  g->build_rename_maxseenvnr=-1; // tmp var for var renaming
  g->build_rename_vc=0;    // tmp var for var renaming 
  g->build_rename_banknr=banknr; // nr of bank of created vars
  // points to bank of created vars
  g->build_rename_bank=(g->varbanks)+((g->build_rename_banknr)*NROF_VARSINBANK);  
}

static void wr_process_given_cl_cleanupsubst(glb* g) {
  int i;
  
  for(i=0;i<g->build_rename_vc;i++) {
    (g->build_rename_bank)[i]=UNASSIGNED;
  }  
}  




void wr_resolve_binary_all_active(glb* g, gptr cl) { 
  void* db=g->db;
  int i;
  int len;      
  int ruleflag; // 0 if not rule
  int poscount=0; // used only for pos/neg pref
  int negcount=0; // used only for pos/neg pref
  int posok=1;  // default allow
  int negok=1;  // default allow
  gint parent;
  gint meta;
  int negflag; // 1 if negative
  int termflag; // 1 if complex atom  
  gint hash;
  int addflag=0;
  int negadded=0;
  int posadded=0;
  vec hashvec;
  int tmp;
  int hlen;
  gint node;
  gint xatom;
  gint yatom;
  gptr xcl;
  gptr ycl;
  int ures;
  
  //char buf[1000];
  //int buflen=800;
  //for(i=0;i<buflen;i++) buf[i]=0;

#ifdef DEBUG
  printf("wr_resolve_binary_all_active called for clause ");
  wr_print_clause(g,cl);
  printf("\n");
#endif  
  // get clause data for input clause
       
  ruleflag=wg_rec_is_rule_clause(db,cl);
  //printf("ruleflag %d\n",ruleflag);
  if (ruleflag) len = wg_count_clause_atoms(db, cl);
  else len=1;
  
  // for negpref check out if negative literals present
    
  if (1) {
    // prohibit pos or neg    
    if ((g->negpref_strat) || (g->pospref_strat)) {
      if (!ruleflag) {
        poscount=1;
        negcount=0;
      } else {         
        poscount=0;
        negcount=0;        
        for(i=0; i<len; i++) {          
          meta=wg_get_rule_clause_atom_meta(db,cl,i);
          if (wg_atom_meta_is_neg(db,meta)) negcount++;
          else poscount++; 
        }  
        // set neg/pospref!    
        if (g->negpref_strat) {
          if (poscount>0 && negcount>0) posok=0;
        }      
        if (g->pospref_strat) {
          if (poscount>0 && negcount>0) negok=0;
        }
      }
    }
  }
  xcl=cl; 
#ifdef DEBUG
  printf("ruleflag %d len %d poscount %d negcount %d posok %d negok %d\n",
          ruleflag,len,poscount,negcount,posok,negok);
#endif  
  // loop over literals
#ifdef USE_CHILD_DB
  if (ruleflag) parent = wg_get_rec_base_offset(db,xcl);
#endif
  for(i=0; i<len; i++) {  
    negflag=0;
    termflag=0;
    addflag=0;
    if (!ruleflag) {
      xatom=wg_encode_record(db,xcl);
      hash=wr_atom_funhash(g,xatom);
      addflag=1;
    } else {       
      meta=wg_get_rule_clause_atom_meta(db,xcl,i);
      if (wg_atom_meta_is_neg(db,meta)) negflag=1;
      if (!((g->negpref_strat) || (g->pospref_strat)) ||
          (negok && negflag && !negadded) || 
          (posok && !negflag)) {            
        if (negflag) negadded++; 
        else posadded++; 
        xatom=wg_get_rule_clause_atom(db,xcl,0);             
#ifdef DEBUG            
        printf("atom nr %d from record \n",i);
        wg_print_record(db,xcl);           
        wg_print_record(db,wg_decode_record(db,xatom));                   
#endif            
        if (wg_get_encoded_type(db,xatom)==WG_RECORDTYPE) {
          termflag=1;
#ifdef USE_CHILD_DB
          if(parent) xatom=wg_encode_parent_data(parent, xatom);
#endif    
          //xatom=wg_decode_record(db,enc);                     
        } else {
          printf("\ncp2 enc %d\n",xatom);
        }                  
        hash=wr_atom_funhash(g,xatom);
        addflag=1;
      }      
    }
    // xcl: active clause
    // xatom: active atom
    if (addflag) {
      // now loop over hash vectors for all active unification candidates
      // ycl: cand clause
      // yatom: cand atom
#ifndef QUIET      
      printf("\n----- inner wr_genloop cycle (active hash list) starts ----------\n"); 
#endif       
      if (negflag) hashvec=otp(db,g->hash_pos_atoms);
      else hashvec=otp(db,g->hash_neg_atoms);
      hlen=wr_clterm_hashlist_len(g,hashvec,hash);
      if (hlen==0) {
        dprintf("no matching atoms in hash\n");
        continue;
      }  
      node=wr_clterm_hashlist_start(g,hashvec,hash);
      if (node<0)  {
        wr_sys_exiterr(g,"apparently wrong hash given to wr_clterm_hashlist_start");
        return;
      }  
      while(node!=0) {       
        yatom=(otp(db,node))[CLTERM_HASHNODE_TERM_POS];
        ycl=otp(db,(otp(db,node))[CLTERM_HASHNODE_CL_POS]);
#ifdef DEBUG        
        printf("\nxatom ");
        wr_print_term(g,xatom);
        printf(" in xcl ");
        wr_print_clause(g,xcl);
        printf("yatom ");
        wr_print_term(g,yatom);
        printf(" in ycl ");
        wr_print_clause(g,ycl);
        //wg_print_record(db,ycl);
        //printf("calling equality check\n");
        wr_print_vardata(g);
#endif        
        ures=wr_unify_term(g,xatom,yatom,1); // uniquestrflag=1
#ifdef DEBUG        
        printf("unification check res: %d\n",ures);
#endif        
        //wr_print_vardata(g);
        //wr_print_vardata(g);
        //wr_clear_varstack(g,g->varstack);
        //wr_print_vardata(g);
        if (ures) {
          // build and process the new clause
          wr_process_resolve_result(g,xatom,xcl,yatom,ycl);        
          if (g->proof_found) {
            return;
          }  
        }
        wr_clear_varstack(g,g->varstack);        
        //wr_print_vardata(g);
        // get next node;
        node=wr_clterm_hashlist_next(g,hashvec,node);       
      }        
    }  
  }     
  dprintf("wr_resolve_binary_all_active finished\n");      
  return;
}



#ifdef __cplusplus
}
#endif
