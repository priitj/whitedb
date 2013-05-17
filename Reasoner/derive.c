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

 /** @file derive.c
 * Clause derivation functions.  
 */

/* ====== Includes =============== */


#ifdef __cplusplus
extern "C" {
#endif

#include "rincludes.h"  


/* ====== Private defs =========== */


#define DEBUG
//#undef DEBUG


/* ====== Private headers ======== */

/* ====== Functions ============== */



void wr_process_resolve_result(glb* g, gint xatom, gptr xcl, gint yatom, gptr ycl) {
  void* db=g->db;
  int xisrule,yisrule,xatomnr,yatomnr;
  int rlen;
  int i,tmp;
  gptr rptr;
  int rpos;
  gptr res;
  gint meta;
  gint blt;
  int ruleflag,datalen;
  //int clstackflag;
  int partialresflag;
  gint given_termbuf_storednext;
  
#ifdef DEBUG
  printf("wr_process_resolve_result called\n");
  wr_print_clause(g,xcl); printf(" : ");wr_print_term(g,xatom);
  printf("\n");
  wr_print_clause(g,ycl);  printf(" : ");wr_print_term(g,yatom);
  printf("\n");
  wr_print_vardata(g);
#endif  
  ++(g->stat_derived_cl);
  ++(g->stat_binres_derived_cl);
  // get basic info about clauses
  xisrule=wg_rec_is_rule_clause(db,xcl);
  yisrule=wg_rec_is_rule_clause(db,ycl);
  if (xisrule) xatomnr=wg_count_clause_atoms(db,xcl);
  else xatomnr=1;
  if (yisrule) yatomnr=wg_count_clause_atoms(db,ycl);
  else yatomnr=1;
  // reserve sufficient space in derived_termbuf for simple sequential store of atoms:
  // no top-level meta kept
  rlen=(xatomnr+yatomnr-2)*LIT_WIDTH;
  if (rlen==0) {
    g->proof_found=1;
    return;
  }  
  (g->derived_termbuf)[1]=2; // init termbuf
  rptr=wr_alloc_from_cvec(g,g->derived_termbuf,rlen);
  if (rptr==NULL) {
    ++(g->stat_internlimit_discarded_cl);
    wr_alloc_err(g,"could not alloc first buffer in wr_process_resolve_result ");
    return; // could not alloc memory, could not store clause
  }  
  //printf("xisrule %d yisrule %d xatomnr %d yatomnr %d rlen %d\n",
  //        xisrule,yisrule,xatomnr,yatomnr,rlen);
  // set up var rename params
  wr_process_resolve_result_setupsubst(g);  
  // store all ready-built atoms sequentially, excluding duplicates
  // and looking for tautology: only for rule clauses needed    
  rpos=0;
  if (xatomnr>1) {
    tmp=wr_process_resolve_result_aux(g,xcl,xatom,xatomnr,rptr,&rpos);
    if (!tmp) {
      wr_process_resolve_result_cleanupsubst(g);
      return; 
    }  
  }
  if (yatomnr>1) {  
    tmp=wr_process_resolve_result_aux(g,ycl,yatom,yatomnr,rptr,&rpos);
    if (!tmp) {
      wr_process_resolve_result_cleanupsubst(g);
      return;
    }  
  }
  wr_process_resolve_result_cleanupsubst(g);
  if (rpos==0) {
    g->proof_found=1;
    return;
  }  
  // now we have stored all subst-into and renamed metas/atoms into rptr: build clause  
  //printf("filled meta/atom new vec, rpos %d\n",rpos);          
  // check whether should be stored as a ruleclause or not
  ruleflag=wr_process_resolve_result_isrulecl(g,rptr,rpos);  
  // create new record
  
  if ((g->hyperres_strat) &&  !wr_hyperres_satellite_tmpres(g,rptr,rpos)){
    partialresflag=1;   
    //wr_process_resolve_result_setupclpickstackcopy(g); 
    wr_process_resolve_result_setupgivencopy(g);    
    // store buffer pos to be restored later
    given_termbuf_storednext=g->build_buffer;
    //g->build_buffer=malloc(1024);
    //given_termbuf_storednext=CVEC_NEXT(g->given_termbuf); 
    g->build_buffer=wr_cvec_new(g,1000);  
  } else {
    partialresflag=0;
    wr_process_resolve_result_setupquecopy(g);
  }  
  if (ruleflag) {       
    meta=RECORD_META_RULE_CLAUSE;
    datalen=rpos*LIT_WIDTH;
    //printf("meta %d headerlen %d datalen %d\n",meta,headerlen,datalen);
    res=wr_create_raw_record(g,CLAUSE_EXTRAHEADERLEN+datalen,meta,g->build_buffer);
    if (res==NULL) {
      ++(g->stat_internlimit_discarded_cl);
      wr_alloc_err(g,"could not alloc raw record in wr_process_resolve_result ");
      return;
    }  
    for(i=RECORD_HEADER_GINTS;i<(RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN);i++) {
      res[i]=0;     
    }         
    for(i=0;i<rpos;i++) {
      tmp=i*LIT_WIDTH;
      res[tmp+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN+LIT_META_POS]=rptr[tmp+LIT_META_POS];
      //blt=wr_build_calc_term(g,rptr[tmp+LIT_ATOM_POS]);
      //else blt=rptr[tmp+LIT_ATOM_POS];
      blt=wr_build_calc_term(g,rptr[tmp+LIT_ATOM_POS]);
      if (blt==WG_ILLEGAL) {
        ++(g->stat_internlimit_discarded_cl);       
        wr_alloc_err(g,"could not build new atom blt in wr_process_resolve_result ");
        return;
      }
      res[tmp+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN+LIT_ATOM_POS]=blt;                
    }
    ++(g->stat_built_cl);    
  } else {
    meta=RECORD_META_FACT_CLAUSE;
    //if (partialresflag) blt=wr_build_calc_term(g,rptr[LIT_ATOM_POS]);
    //else blt=rptr[LIT_ATOM_POS];
    blt=wr_build_calc_term(g,rptr[LIT_ATOM_POS]);
    if (blt==WG_ILLEGAL) {
      ++(g->stat_internlimit_discarded_cl);
      wr_alloc_err(g,"could not build new atom blt in wr_process_resolve_result ");
      return;
    }
    res=otp(db,blt);
    res[RECORD_META_POS]=meta;
     ++(g->stat_built_cl);
  }   
#ifdef DEBUG  
  printf("\nwr_process_resolve_result generated a clause \n");
  wg_print_record(db,res);
  printf("\n");
#endif    
  // now the resulting clause is fully built
  if ((g->hyperres_strat) &&  !wr_hyperres_satellite_cl(g,res)) {  
    ++(g->stat_hyperres_partial_cl);
    if (g->print_partial_derived_cl) {
      printf("+ partial derived: ");
      wr_print_clause(g,res);
    }  
    //wr_push_clpickstack_cl(g,res);
    wr_clear_varstack(g,g->varstack);
    //wr_clear_all_varbanks(g);
    //wr_print_vardata(g);
    wr_resolve_binary_all_active(g,res);
    // restore buffer pos to situation before building the current clause
    wr_vec_free(g,g->build_buffer);
    g->build_buffer=given_termbuf_storednext;
    //CVEC_NEXT(g->given_termbuf)=given_termbuf_storednext;
  } else {       
    ++(g->stat_kept_cl);
    if (g->print_derived_cl) {
      printf("+ derived: ");
      wr_print_clause(g,res);            
    }    
    // push built clause into suitable list
    wr_push_clqueue_cl(g,res);
  }  
}  


  
int wr_process_resolve_result_isrulecl(glb* g, gptr rptr, int rpos) {
  void* db;
  int stopflag,ruleflag, len, i;
  gint meta, atom, term;
  gptr atomptr;
  
  if (rpos!=1) {
    return 1;
  } else {  
    // only clauses of len 1 check further
    db=g->db;
    stopflag=0;
    ruleflag=1;
    meta=rptr[LIT_META_POS];
    atom=rptr[LIT_ATOM_POS];     
    if (isdatarec(atom) && !wg_atom_meta_is_neg(db,meta)) {
      atomptr=decode_record(db,atom);
      len=get_record_len(atomptr);
      for(i=(g->unify_firstuseterm); i<len; i++) {
        term = get_field(atomptr,i);
        if (isdatarec(term) || isvar(term)) {
          stopflag=1;
          break;
        }        
      }
      if (!stopflag) {
        // really factclause 
        ruleflag=0;
      }  
    }
    return ruleflag;
  } 
}  

void wr_process_resolve_result_setupsubst(glb* g) {
  g->build_subst=1;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  (g->given_termbuf)[1]=2; // reuse given_termbuf
  g->build_buffer=g->derived_termbuf;
  //g->build_buffer=g->queue_termbuf;
  g->build_rename=1;   // do var renaming
  g->build_rename_maxseenvnr=-1; // tmp var for var renaming
  g->build_rename_vc=0;    // tmp var for var renaming 
  g->build_rename_banknr=3; // nr of bank of created vars
  // points to bank of created vars
  g->build_rename_bank=(g->varbanks)+((g->build_rename_banknr)*NROF_VARSINBANK);  
  g->use_comp_funs=g->use_comp_funs_strat;
}

void wr_process_resolve_result_cleanupsubst(glb* g) {
  int i;
  
  for(i=0;i<g->build_rename_vc;i++) {
    (g->build_rename_bank)[i]=UNASSIGNED;
  }  
}  

void wr_process_resolve_result_setupgivencopy(glb* g) {
  g->build_subst=0;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  //(g->given_termbuf)[1]=2; // reuse given_termbuf
  //g->build_buffer=g->given_termbuf;
  //g->build_buffer=g->given_termbuf;
  //g->build_buffer=NULL;  // PROBLEM WAS HERE: given_termbuf not ok here  
  g->build_rename=0;   // do var renaming   
  g->use_comp_funs=0;
}


void wr_process_resolve_result_setupquecopy(glb* g) {
  g->build_subst=0;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  //(g->given_termbuf)[1]=2; // reuse given_termbuf
  g->build_buffer=g->queue_termbuf;
  g->build_rename=0;   // do var renaming   
  g->use_comp_funs=0;
}

void wr_process_resolve_result_setupclpickstackcopy(glb* g) {
  g->build_subst=0;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  //(g->given_termbuf)[1]=2; // reuse given_termbuf
  g->build_buffer=g->queue_termbuf;
  g->build_rename=0;   // do var renaming   
  g->use_comp_funs=0;  
}  


int wr_process_resolve_result_aux
      (glb* g, gptr cl, gint cutatom, int atomnr, gptr rptr, int* rpos){
  //void *db=g->db;
  int i,j;
  int posfoundflag;
  gint meta,atom,newatom,rmeta;
        
#ifdef DEBUG
  printf("\nwr_process_resolve_result_aux called on atomnr %d\n",atomnr);
  wr_print_term(g,cutatom);          
#endif        
        
  for(i=0;i<atomnr;i++) {
    meta=wg_get_rule_clause_atom_meta(db,cl,i);
    atom=wg_get_rule_clause_atom(db,cl,i);
    if (atom==cutatom) continue; // drop cut atoms
    // subst into xatom      
    newatom=wr_build_calc_term(g,atom);
#ifdef DEBUG
    printf("\nwr_process_resolve_result_aux loop i %d built term\n",i);    
    wr_print_term(g,newatom);          
#endif
    if (newatom==WG_ILLEGAL) {
      ++(g->stat_internlimit_discarded_cl);
      wr_alloc_err(g,"could not build subst newatom in wr_process_resolve_result ");
      return 0; // could not alloc memory, could not store clause
    }  
    if (newatom==ACONST_TRUE) {
      if (wg_atom_meta_is_neg(db,meta)) continue;
      else return 0;
    } 
    if (newatom==ACONST_FALSE) {
      if (wg_atom_meta_is_neg(db,meta)) return 0;
      else continue;      
    }      
    posfoundflag=0;
    // check if xatom present somewhere earlier      
    for(j=0;j < *rpos;j++){
      if (wr_equal_term(g,newatom,rptr[(j*LIT_WIDTH)+LIT_ATOM_POS],1)) {        
        rmeta=rptr[(j*LIT_WIDTH)+LIT_META_POS];
        if (!litmeta_negpolarities(meta,rmeta)) {
          //same sign, drop lit
          posfoundflag=1;
          printf("\nequals found:\n");
          wr_print_term(g,newatom);
          printf("\n");
          wr_print_term(g,rptr[(j*LIT_WIDTH)+LIT_ATOM_POS]);
          printf("\n");
          break;                                
        } else {
          printf("\nin wr_process_resolve_result_aux return 0\n");
          // negative sign, tautology, drop clause          
          return 0;
        }            
      }         
    }
    if (!posfoundflag) {      
      // store lit
      rptr[((*rpos)*LIT_WIDTH)+LIT_META_POS]=meta;
      rptr[((*rpos)*LIT_WIDTH)+LIT_ATOM_POS]=newatom;
      ++(*rpos);      
    }       
  }     
  printf("\nwr_process_resolve_result_aux gen clause:\n");
  wr_print_clause(g,rptr);  
  return 1; // 1 means clause is still ok. 0 return means: drop clause 
}  
  
/**
  satellite is a fully-built hypperesolution result, not temporary result
  
*/

int wr_hyperres_satellite_cl(glb* g,gptr cl) {
  int len;
  int i;
  gint meta;
  
  if (cl==NULL) return 0;
  if (!wg_rec_is_rule_clause(g->db,cl)) {
    // fact clause (hence always positive)
    if (g->negpref_strat) return 1;
    else return 0;    
  } else {
    // rule clause: check if contains only non-preferred
    len=wg_count_clause_atoms(g->db,cl);
    for(i=0;i<len;i++) {
      meta=wg_get_rule_clause_atom_meta(g->db,cl,i);
      if (wg_atom_meta_is_neg(g->db,meta)) {
        if (g->negpref_strat) return 0;
      } else {
        if (g->pospref_strat) return 0;  
      }        
    }
    return 1;    
  }      
} 

/**
  satellite is a fully-built hypperesolution result, not temporary result
  
*/

int wr_hyperres_satellite_tmpres(glb* g,gptr tmpres, int respos) {
  int i;
  gint tmeta;
    
  for(i=0;i<respos;i++) {
    tmeta=tmpres[(i*LIT_WIDTH)+LIT_META_POS];
    if (wg_atom_meta_is_neg(g->db,tmeta)) {
      if (g->negpref_strat) return 0;
    } else {
      if (g->pospref_strat) return 0;  
    }        
  }
  return 1;    
} 

#ifdef __cplusplus
}
#endif
