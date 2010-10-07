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

 /** @file derive.c
 * Clause derivation functions.  
 */

/* ====== Includes =============== */


#ifdef __cplusplus
extern "C" {
#endif

#include "rincludes.h"  


/* ====== Private defs =========== */

#define USEDECO
//#undef USEDECO
#define USE_SIMPLIFICATION
//#undef USE_SIMPLIFICATION
#define DERIVED_SIMPLIFICATION_MATCH_LENGTHLIMIT 5
#define DERIVED_SIMPLIFICATION_UNIFY_LENGTHLIMIT 10
#define HASH_CUT_FLAG 0 // see also HASH_STORE_FLAG in clstore.c



#undef DEBUG
#undef SHOWACTIVE
#undef SHOWDERIVED
//#undef SHOWFACTORSTORING
//#undef SHOWSIMPLIFICATION
//#undef DEBUG_GC
//#undef DEBUG_HASH_CUT


//#define DEBUG
//#define SHOWACTIVE
//#define SHOWDERIVED
//#define SHOWFACTORSTORING
//#define SHOWSIMPLIFICATION
//#define DEBUG_GC
//#define DEBUG_HASH_CUT


/* ====== Private headers ======== */

/* ====== Functions ============== */



void wr_process_resolve_result(glb* g, gint xatom, gptr xcl, gint yatom, gptr ycl) {
  void* db=g->db;
  int xisrule,yisrule,xatomnr,yatomnr;
  int rlen,xlen;
  int i,tmp;
  gptr rptr, yptr;
  int rpos;
  gint xmeta;
  gptr atomptr,res;
  gint meta,atom,term;
  int len,headerlen;
  int ruleflag,stopflag,datalen;
  
#ifdef DEBUG  
  printf("wr_process_resolve_result called\n");
#endif  
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
  // now we have stored all subst-into and renamed metas/atoms into rptr: build clause
  yptr=wr_alloc_from_cvec(g,g->build_buffer,(RECORD_HEADER_GINTS+xlen)); 
  if (yptr==NULL) {
    ++(g->stat_internlimit_discarded_cl);
    wr_alloc_err(g,"could not alloc yptr buffer in wr_process_resolve_result ");
    return; // could not alloc memory, could not store clause
  } 
  //printf("filled meta/atom new vec, rpos %d\n",rpos);          
  // check whether should be stored as a ruleclause or not
  ruleflag=wr_process_resolve_result_isrulecl(g,rptr,rpos);  
  // create new record
  wr_process_resolve_result_setupquecopy(g);
  if (ruleflag) {    
    meta=RECORD_META_RULE_CLAUSE;
    datalen=rpos*LIT_WIDTH;
    //printf("meta %d headerlen %d datalen %d\n",meta,headerlen,datalen);
    res=wr_create_raw_record(g,CLAUSE_EXTRAHEADERLEN+datalen,meta,g->build_buffer);
    for(i=RECORD_HEADER_GINTS;i<(RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN);i++) {
      res[i]=0;     
    }         
    for(i=0;i<rpos;i++) {
      tmp=i*LIT_WIDTH;
      res[tmp+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN+LIT_META_POS]=rptr[tmp+LIT_META_POS];
      res[tmp+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN+LIT_ATOM_POS]=
                wr_build_calc_term(g,rptr[tmp+LIT_ATOM_POS]);         
    } 
  } else {
    meta=RECORD_META_FACT_CLAUSE;
    res=otp(db,wr_build_calc_term(g,rptr[LIT_ATOM_POS]));
    res[RECORD_META_POS]=meta;
  }   
  printf("\nwr_process_resolve_result generated a clause \n");
  //wg_print_record(db,res);
  //printf("\n");
  wr_print_clause(g,res);  
  // push built clause into suitable list
  wr_push_clqueue_cl(g,res);
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
      atomptr=wg_decode_record(db,atom);
      len=wg_get_record_len(db,atomptr);
      for(i=TERM_EXTRAHEADERLEN; i<len; i++) {
        term = wg_get_field(db,atomptr,i);
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
  g->build_rename=1;   // do var renaming
  g->build_rename_maxseenvnr=-1; // tmp var for var renaming
  g->build_rename_vc=0;    // tmp var for var renaming 
  g->build_rename_banknr=3; // nr of bank of created vars
  // points to bank of created vars
  g->build_rename_bank=(g->varbanks)+((g->build_rename_banknr)*NROF_VARSINBANK);  
}

void wr_process_resolve_result_cleanupsubst(glb* g) {
  int i;
  
  for(i=0;i<g->build_rename_vc;i++) {
    (g->build_rename_bank)[i]=UNASSIGNED;
  }  
}  

void wr_process_resolve_result_setupquecopy(glb* g) {
  g->build_subst=0;     // subst var values into vars
  g->build_calc=0;      // do fun and pred calculations
  g->build_dcopy=0;     // copy nonimmediate data (vs return ptr)
  //g->build_buffer=NULL; // build everything into tmp buffer (vs main area)
  //(g->given_termbuf)[1]=2; // reuse given_termbuf
  g->build_buffer=g->queue_termbuf;
  g->build_rename=0;   // do var renaming   
}


int wr_process_resolve_result_aux
      (glb* g, gptr cl, gint cutatom, int atomnr, gptr rptr, int* rpos){
  void *db=g->db;
  int i,j;
  int posfoundflag,negfoundflag;
  gint meta,atom,newatom,rmeta;

  for(i=0;i<atomnr;i++) {
    meta=wg_get_rule_clause_atom_meta(db,cl,i);
    atom=wg_get_rule_clause_atom(db,cl,i);
    if (atom==cutatom) continue; // drop cut atoms
    // subst into xatom      
    newatom=wr_build_calc_term(g,atom);
    if (newatom==WG_ILLEGAL) {
      ++(g->stat_internlimit_discarded_cl);
      wr_alloc_err(g,"could not build subst newatom in wr_process_resolve_result ");
      return 0; // could not alloc memory, could not store clause
    }  
    posfoundflag=0;
    negfoundflag=0;
    // check if xatom present somewhere earlier      
    for(j=0;j < *rpos;j++){
      if (wr_equal_term(g,newatom,rptr[(j*LIT_WIDTH)+LIT_ATOM_POS],1)) {        
        rmeta=rptr[(j*LIT_WIDTH)+LIT_META_POS];
        if ((wg_atom_meta_is_neg(db,meta) && wg_atom_meta_is_neg(db,rmeta)) ||
            (!wg_atom_meta_is_neg(db,meta) && !wg_atom_meta_is_neg(db,rmeta)) ) {
          //same sign, drop lit
          posfoundflag=1;
          break;                                
        } else {
          // negative sign, tautology, drop clause
          negfoundflag=1;
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
  return 1; // 1 means clause is still ok. 0 return means: drop clause 
}  
  


#ifdef __cplusplus
}
#endif
