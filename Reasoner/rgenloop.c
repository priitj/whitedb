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

#define DEBUG
//#undef DEBUG
//#define QUIET

#define USE_RES_TERMS // loop over active clauses in wr_resolve_binary_all_active

/* ======= Private protos ================ */

static void wr_process_given_cl_setupsubst(glb* g, gptr buf, gint banknr, int reuseflag);
static void wr_process_given_cl_cleanupsubst(glb* g);

/* ====== Functions ============== */



int wr_genloop(glb* g) {

  gptr picked_given_cl_cand;
  gptr given_cl_cand; 
  gptr given_cl;  
  int i;
  int given_kept_flag; 
  gptr tmp;  
    
#ifndef USE_RES_TERMS  
  gint ipassive;
  gint iactive;
  gptr activecl;
#endif  
  
#ifndef QUIET    
  printf("========= rwr_genloop starting ========= \n");   
#endif  
  //clear_active_cl_list(); // ???
  wr_clear_all_varbanks(g); 
     
  if ((g->print_initial_passive_list)==1) {
    printf("-- initial passive list starts --  \n");
    //printf("len %d next %d \n",CVEC_LEN(rotp(g,g->clqueue)),CVEC_NEXT(rotp(g,g->clqueue)));
    i=CVEC_START;
    for(; i<CVEC_NEXT(rotp(g,g->clqueue)) ; ++i) {
      wr_print_clause(g,(gptr)((rotp(g,g->clqueue))[i]));    
    }  
    printf("-- initial passive list ends -- \n");     
  }  
  if ((g->print_initial_active_list)==1) {
    printf("-- initial active list starts --  \n");
    //printf("len %d next %d \n",CVEC_LEN(rotp(g,g->clactive)),CVEC_NEXT(rotp(g,g->clactive)));
    i=CVEC_START;
    for(; i<CVEC_NEXT(rotp(g,g->clactive)) ; ++i) {
      wr_print_clause(g,(gptr)((rotp(g,g->clactive))[i]));        
    }  
    printf("-- initial active list ends -- \n");   
  }
  // loop until no more passive clauses available

  g->proof_found=0;
  g->clqueue_given=CVEC_START;
  given_kept_flag=1;    
  
  for(;;) {       
    if (g->alloc_err) {
      printf("Unhandled alloc_err detected in the main wr_genloop\n");
      return -1;
    }      
    given_kept_flag=1; // will be overwritten
    picked_given_cl_cand=wr_pick_given_cl(g,&given_kept_flag);             
    // given_kept_flag will now indicate whether to add to active list or not
    if (g->print_initial_given_cl) {
      printf("*** given candidate %d: ",(g->stat_given_candidates));
      wr_print_clause(g,picked_given_cl_cand);      
      //CP0
      //wr_print_vardata(g);
      //printf("\n");    
    }         
    if (picked_given_cl_cand==NULL) {
      return 1;
    }
    (g->stat_given_candidates)++; //stats    
    given_cl_cand=wr_activate_passive_cl(g,picked_given_cl_cand);  
    if (given_cl_cand==NULL) {
      if (g->alloc_err) return -1;
      continue; 
    } 
    //given_cl_cand=picked_given_cl_cand;
    //if (given_cl_cand==GNULL) printf("activated given_cl_cand==GNULL\n");    
    if (given_cl_cand==NULL) continue;      
    if (wr_given_cl_subsumed(g,given_cl_cand)) {
#ifdef DEBUG
      printf("given cl is subsumed\n");
#endif    
      continue;
    }  
    //CP1
    //wr_print_vardata(g);
    given_cl=wr_process_given_cl(g,given_cl_cand); 
    //CP2
    //wr_print_vardata(g);
    //wr_clear_all_varbanks(g);
    if (given_cl==NULL) {
      if (g->alloc_err) return -1;
      continue; 
    }  
    if (g->print_final_given_cl) {
      printf("*** given %d: ",(g->stat_given_used));
      wr_print_clause(g,given_cl);
      //printf("\n");
      //wr_print_vardata(g);
      // printf("built %d kept %d \n",(g->stat_built_cl),(g->stat_kept_cl));      
      //printf("\n");    
    }
    //if ((g->stat_given_used)>233) return; //223
    if (given_kept_flag) {
      tmp=wr_add_given_cl_active_list(g,given_cl);
      if (tmp==NULL) {
        if (g->alloc_err) return -1;
        continue; 
      }
    }      
    // do all resolutions with the given clause
#ifdef USE_RES_TERMS
    // normal case: active loop is done inside the wr_resolve_binary_all_active    
    wr_resolve_binary_all_active(g,given_cl);    
    if (g->proof_found) return 0;
    if (g->alloc_err) return -1;      
#else    
    // testing/experimenting case: loop explicitly over active clauses
    iactive=CVEC_START;
    for(; iactive<CVEC_NEXT(rotp(g,g->clactive)); iactive++) {
#ifndef QUIET      
    printf("\n----- inner wr_genloop cycle (active) starts ----------\n"); 
#endif       
      activecl=(gptr)((rotp(g,g->clactive))[iactive]);          
      //resolve_binary(g,given_cl,activecl);
      if ((g->proof_found)) {
        return 0;
      }	      
    }
#endif // USE_RES_TERMS
  } 
}  


gptr wr_pick_given_cl(glb* g, int* given_kept_flag) {
  gptr cl;
  int next;

  //printf("wr_pick_given_cl called with clqueue_given %d and given_kept_flag %d\n",(g->clqueue_given),given_kept_flag);
  //printf(" CVEC_NEXT(rotp(g,g->clqueue)) %d \n",CVEC_NEXT(rotp(g,g->clqueue)));
#ifdef DEBUG  
  printf("picking cl nr %d as given\n",g->clqueue_given);
#endif  
  //if (g->clqueue_given>=4) exit(0);
  // first try stack
  next=CVEC_NEXT(rotp(g,g->clpickstack));
  if (next>CVEC_START) {
    cl=(gptr)((rotp(g,g->clpickstack))[next-1]);
    --(CVEC_NEXT(rotp(g,g->clpickstack)));
    // do not put cl to active list
    *given_kept_flag=0;
    if (cl!=NULL) return cl;    
  }  
  // then try queue
  next=CVEC_NEXT(rotp(g,g->clqueue));
  if (next>(g->clqueue_given)) {
    cl=(gptr)((rotp(g,g->clqueue))[g->clqueue_given]);           
    ++(g->clqueue_given); 
    // do not put cl to active list
    *given_kept_flag=1;
    return cl;
  }
  // no candidates for given found 
  return NULL;
}


gptr wr_activate_passive_cl(glb* g, gptr picked_given_cl_cand) {
  
  return  picked_given_cl_cand;
} 

gptr wr_process_given_cl(glb* g, gptr given_cl_cand) {  
  gptr given_cl; 

#ifdef DEBUG
  void* db=g->db;
  printf("wr_process_given_cl called with \n");
  printf("int %d type %d\n",given_cl_cand,wg_get_encoded_type(db,given_cl_cand));
  wr_print_record(g,given_cl_cand);
  wr_print_clause(g,given_cl_cand);  
#endif    
  wr_process_given_cl_setupsubst(g,g->given_termbuf,1,1);
  given_cl=wr_build_calc_cl(g,given_cl_cand);
  wr_process_given_cl_cleanupsubst(g);
  if (given_cl==NULL) return NULL; // could be memory err  
  //wr_print_varbank(g,g->varbanks);
#ifdef DEBUG
  printf("rebuilt as \n");
  wr_print_record(g,given_cl);
  wr_print_clause(g,given_cl);  
#endif  
  return given_cl;
} 

gptr wr_add_given_cl_active_list(glb* g, gptr given_cl) {  
  gptr active_cl;

#ifdef DEBUG
  void* db=g->db;
  printf("wr_add_given_cl_active_list called with \n");
  printf("int %d type %d\n",given_cl,wg_get_encoded_type(db,given_cl));
  wr_print_record(g,given_cl);
  wr_print_clause(g,given_cl);  
#endif          
  wr_process_given_cl_setupsubst(g,g->active_termbuf,2,0);    
  active_cl=wr_build_calc_cl(g,given_cl);
  wr_process_given_cl_cleanupsubst(g); 
  if (active_cl==NULL) return NULL; // could be memory err
#ifdef DEBUG
  printf("wr_add_given_cl_active_list generated for storage \n");
  printf("int %d type %d\n",given_cl,wg_get_encoded_type(db,active_cl));
  wr_print_record(g,active_cl);
  wr_print_clause(g,active_cl);
#endif    
  //wr_print_varbank(g,g->varbanks);
  wr_push_clactive_cl(g,active_cl);              
  (g->stat_given_used)++;  // stats 
  
  return active_cl;
} 


static void wr_process_given_cl_setupsubst(glb* g, gptr buf, gint banknr, int reuseflag) {
  g->build_subst=0;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  if (reuseflag) buf[1]=2; // reuse given_termbuf
  g->build_buffer=buf;
  g->build_rename=1;   // do var renaming
  g->build_rename_maxseenvnr=-1; // tmp var for var renaming
  g->build_rename_vc=0;    // tmp var for var renaming 
  g->build_rename_banknr=banknr; // nr of bank of created vars
  // points to bank of created vars
  g->build_rename_bank=(g->varbanks)+((g->build_rename_banknr)*NROF_VARSINBANK);  
  g->tmp_unify_vc=((gptr)(g->varstack))+1;
}

static void wr_process_given_cl_cleanupsubst(glb* g) {
  int i;
  
  wr_clear_varstack(g,g->varstack);
  //for(i=0;i<g->build_rename_vc;i++) {
  //  (g->build_rename_bank)[i]=UNASSIGNED;
  //}  
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
  //gint parent;
  gint meta;
  int negflag; // 1 if negative
  int termflag; // 1 if complex atom  
  gint hash;
  int addflag=0;
  int negadded=0;
  int posadded=0;
  vec hashvec;
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
  wr_print_vardata(g);
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
#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
  if (ruleflag) parent = wg_get_rec_base_offset(db,xcl);
#endif
#endif
  for(i=0; i<len; i++) {  
    negflag=0;
    termflag=0;
    addflag=0;
    //printf("\nruleflag %d, addflag %d, len %d, i: %d\n",ruleflag,addflag,len,i);
    if (!ruleflag) {
      xatom=encode_record(db,xcl);
      printf("!ruleflag atom with i %d: \n",i);
      wr_print_record(g,wg_decode_record(db,xatom));
      hash=wr_atom_funhash(g,xatom);
      printf("hash %d: \n",hash);
      addflag=1;
    } else {       
      meta=wg_get_rule_clause_atom_meta(db,xcl,i);
      if (wg_atom_meta_is_neg(db,meta)) negflag=1;
      if (!((g->negpref_strat) || (g->pospref_strat)) ||
          (negok && negflag && !negadded) || 
          (posok && !negflag)) {            
        if (negflag) negadded++; 
        else posadded++; 
        xatom=wg_get_rule_clause_atom(db,xcl,i);             
#ifdef DEBUG            
        printf("atom nr %d from record \n",i);
        wr_print_record(g,xcl);
        printf("\natom\n");              
        wr_print_record(g,wg_decode_record(db,xatom));
        printf("negflag %d\n",negflag);             
#endif            
        if (wg_get_encoded_type(db,xatom)==WG_RECORDTYPE) {
          termflag=1;
#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
          if(parent) xatom=wg_encode_parent_data(parent, xatom);
#endif    
#endif
          //xatom=wg_decode_record(db,enc);                     
        } else {
          //printf("\ncp2 enc %d\n",xatom);
        }                  
        hash=wr_atom_funhash(g,xatom);
        //printf("hash %d\n",hash);
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
      if (negflag) hashvec=rotp(g,g->hash_pos_atoms);
      else hashvec=rotp(g,g->hash_neg_atoms); 

      //wr_clterm_hashlist_print(g,hashvec); 
      
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
        
        printf("after while(node!=0): \n");
        printf("ycl: \n");
        wr_print_clause(g,ycl); 
        /*
        printf("xcl: \n");
        wr_print_clause(g,xcl);
        printf("xatom: \n");
        wr_print_clause(g,xatom);
        */
        if (g->print_active_cl) {
          printf("* active: ");
          wr_print_clause(g,ycl); 
          //printf("\n");
        }  
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
        //printf("!!!!!!!!!!!!!!!!!!!!! before unification\n");
        //wr_print_vardata(g); 
        //printf("CLEAR\n");
        //wr_clear_varstack(g,g->varstack);           
        //wr_print_vardata(g); 
        //printf("START UNIFICATION\n");
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
          printf("\nin wr_resolve_binary_all_active to call wr_process_resolve_result\n");
          wr_process_resolve_result(g,xatom,xcl,yatom,ycl);  
          printf("\nin wr_resolve_binary_all_active after  wr_process_resolve_result\n");
          printf("\nxatom\n");
          wr_print_term(g,xatom);
          if (g->proof_found || g->alloc_err) {
            wr_clear_varstack(g,g->varstack);          
            return;          
          }  
        }
        wr_clear_varstack(g,g->varstack);                
        //wr_print_vardata(g);
        // get next node;
        node=wr_clterm_hashlist_next(g,hashvec,node);       
      }
      printf("\nexiting node loop\n");      
    }  
  }     
  dprintf("wr_resolve_binary_all_active finished\n");      
  return;
}



#ifdef __cplusplus
}
#endif
