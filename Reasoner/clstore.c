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

 /** @file clstore.c
 * Clause storage functions. 
 */

/* ====== Includes =============== */


#ifdef __cplusplus
extern "C" {
#endif

#include "rincludes.h"  
 

/* ====== Private defs =========== */

//#define DEBUG
#undef DEBUG  
#undef DEBUGHASH


/* ====== Private headers ======== */

static gint conststrhash(char* str);

static int int_hash(int x);
static int double_hash(double x);
static int str_hash(char* x); 
static int str_dual_hash(char* x, char* y);
  
  
/* ====== Functions ============== */


/*

store a clause in a passive stack

*/

void wr_push_clpickstack_cl(glb* g, gptr cl) {

#ifdef DEBUG  
  printf("pushing to clpickstack pos %d\n",(rotp(g,g->clpickstack))[1]);  
#endif  
  (g->clpickstack)=rpto(g,wr_cvec_push(g,rotp(g,(g->clpickstack)),(gint)cl));
  //wr_show_clpickstack(g);
}


void wr_show_clpickstack(glb* g) {
  int i;
  
  for(i=2;i<(rotp(g,g->clpickstack))[1];i++) {
    printf("\nclpickstack nr %d :",i);
    wr_print_record(g,(gptr)((rotp(g,g->clpickstack))[i]));    
  } 
}


/*

store a clause in a passive queue

*/

void wr_push_clqueue_cl(glb* g, gptr cl) {

#ifdef DEBUG  
  printf("pushing to clqueue pos %d\n",(rotp(g,g->clqueue))[1]);  
#endif  
  (g->clqueue)=rpto(g,wr_cvec_push(g,rotp(g,(g->clqueue)),(gint)cl));
  //wr_show_clqueue(g);
}


void wr_show_clqueue(glb* g) {
  int i;
  
  for(i=2;i<(rotp(g,g->clqueue))[1];i++) {
    printf("\nclqueue nr %d :",i);
    wr_print_record(g,(gptr)((rotp(g,g->clqueue))[i]));    
  } 
}

/*

make a clause active

*/

void wr_push_clactive_cl(glb* g, gptr cl) {
  
#ifdef DEBUG  
  printf("pushing to clactive pos %d\n",(rotp(g,g->clactive))[1]);
#endif  
  (g->clactive)=rpto(g,wr_cvec_push(g,rotp(g,(g->clactive)),(gint)cl));  
  wr_cl_store_res_terms(g,cl);
}  


void wr_show_clactive(glb* g) {
  int i;
  
  for(i=2;i<(rotp(g,g->clactive))[1];i++) {
    printf("\nclactive nr %d :",i);
    wr_print_record(g,(gptr)((rotp(g,g->clactive))[i]));    
  } 
}

/*

store resolvable literals/terms of a clause to fast resolvable-lit list
returns 0 iff ok

hash adding:
 
  - separately for pos and neg literals (g->hash_pos_atoms_bits and g->hash_neg_atoms_bits)
    - g->hash_pos_atoms_bits is a cvec with required hash pos combinations, 
      each as a gint of bits correponding to positions
    - g->hash_pos_atoms_vecs is a cvec with els pointing to hashvec-s of corresponding 
      bit/pos values

  - two hash systems: 
    
    - bit/pos for top-level-ground atoms
    - non-var top-level prefix for non-top-level-ground
  
  - unifiable atoms of each active clause are entered to hash for all given bit/pos values
    where all corresp subterms are non-var
  
  - usage of stored hash for unification candidates:

    - pick active, say -p(a,X) | -p(X,b) | r(a,b)
    - search the p(a hash (bits 11) for all matches and use them 
      like p(a,c), p(a,f(X)) ...
    - then search the p( hash (bits 1) for all matches and use them
      like p(X,c), p(X,X), ...
    - then search the univ list for all matches and use them
      like X(Y,Y), ...
      
    - easy for finding unifiable active ground atoms: 
      pick the hash with most bits nonvar: this is the
      best option and covers all unifiable ground atoms
      like for p(a,X) pick p(a, for p(a,c) pick this
      
    - suppose we search unifiable atoms for p(a,X)
     
      - we have p(a,Z): need p(a
      - we have p(Y,b): need p(
    
      - p(a,Z) comes up twice: as p(a and as p(
        - how to skip p( case? p( would be
          needed for finding p(Y,b) where we have
          var at hash bit/pos          
        - we could also mark handled cases

    - normally we search unifiable atoms for ground atoms?
    
  - idea: N-nonvar subcases

    0 nonvar: full list
    1 nonvar: hash over all nonvars
    2 nonvar: hash over all nonvars

    search:

    p(X,Y):

    use 1 nonvar hash
    use 0 nonvar hash

    p(a,Y)
    
    use 2 nonvar hash to find p(a,Z) and p(Z,a)
    use 1 nonvar hash to find p(Z,U)
    use 0 nonvar hash to find W(U,V)

    - 0-var: ground case, full bit/pos hash storage
       - search: just look for max bit/pos combo
    - 1-var: store in possible bit/pos hashes
       - search: 
    - ...
    - N/all-var: list 
    
    
    
    
    
    
    
  - idea: N-len ground prefixes
  
      suppose we search unifiable atoms for p(a,X)
  
      - we have p(a,Z): need p(a
      - we have p(Y,b): need p( 
      
      suppose we search unifiable atoms for p(U,V)
  
      - we have p(a,Z): need p(
      - we have p(Y,b): need p( 
      
       suppose we search unifiable atoms for W(U,V)
  
      - we have p(a,Z): need full list
      - we have p(Y,b): need full list
      
      
      NB! no overlap in ground prefix lists: each
          atom in exactly one      

      p(a,X) would be in 2-pref
      p(Y,X) would be in 1-pref
      U(Y,X) would be in 0-pref
      
      search unifiers for p(a,U):
        - search all hashes from 2-pref to lower
        should find 
        p(a,V) in 2-pref
        p(Y,b) in 1-pref
        X(Y,c) in 0-pref
        
      search unifiers for p(X,Y):
        should find 
        p(a,V) in 2-pref?? no, need to put p(a,V) to 1-pref as well
      
      --- two lists: pred hash and full ---      
      
      full contains everything
      full is used only by X(a,b) cases with var pred
      
      predhash contains all with pred
      predhash is used by all p(X,Y) cases?
      how to find X(U,V) then?
      
      --- just pred hash list
      
      search unifiers for p(X,Y): normal
      
      search unifiers for U(X,Y): scan all active clauses
            
      
*/


int wr_cl_store_res_terms(glb* g, gptr cl) {    
  void* db=g->db;
  int i;
  int len;      
  int ruleflag; // 0 if not rule
  int poscount=0; // used only for pos/neg pref
  int negcount=0; // used only for pos/neg pref
  int posok=1;  // default allow
  int negok=1;  // default allow
  gint meta;
  gint atom;  
  int negflag; // 1 if negative
  int termflag; // 1 if complex atom  
  gint hash;
  int addflag=0;
  int negadded=0;
  int posadded=0;
  vec hashvec;
  int tmp;
  
#ifdef DEBUG
  printf("cl_store_res_terms called on cl: "); 
  wr_print_clause(g,cl);
#endif  

  // get clause data for input clause
       
  ruleflag=wg_rec_is_rule_clause(db,cl);
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

#ifdef DEBUG
  printf("ruleflag %d len %d poscount %d negcount %d posok %d negok %d\n",
          ruleflag,len,poscount,negcount,posok,negok);
#endif  
  // loop over literals
#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
  if (ruleflag) parent = wg_get_rec_base_offset(db,cl);
#endif
#endif
  for(i=0; i<len; i++) {  
    negflag=0;
    termflag=0;
    addflag=0;
    if (!ruleflag) {
      atom=encode_record(db,cl);      
      addflag=1;
    } else {       
      meta=wg_get_rule_clause_atom_meta(db,cl,i);
      if (wg_atom_meta_is_neg(db,meta)) negflag=1;
      if (!((g->negpref_strat) || (g->pospref_strat)) ||
          (negflag && (g->hyperres_strat)) ||
          (negok && negflag && !negadded) || 
          (posok && !negflag)) {            
        if (negflag) negadded++; 
        else posadded++;          
        atom=wg_get_rule_clause_atom(db,cl,i);
        if (wg_get_encoded_type(db,atom)==WG_RECORDTYPE) {
          termflag=1;
#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
          if(parent) atom=wg_encode_parent_data(parent,atom);
#endif             
#endif
        }               
        addflag=1;
      }      
    }
    if (addflag) {
      hash=wr_atom_funhash(g,atom);
#ifdef DEBUG 
      printf("before adding to hash negflag: %d\n",negflag);
#endif      
      if (negflag) hashvec=rotp(g,g->hash_neg_atoms);
      else hashvec=rotp(g,g->hash_pos_atoms);
      tmp=wr_clterm_add_hashlist(g,hashvec,hash,atom,cl);
      if (tmp) {
        wr_sys_exiterr2int(g,"adding term to hashlist in cl_store_res_terms, code ",tmp);
        return 1;        
      }  
#ifdef DEBUGHASH      
      printf("\nhash table after adding:");      
      wr_clterm_hashlist_print(g,hashvec);     
      printf("\npos hash table after adding:");      
      wr_clterm_hashlist_print(g,rotp(g,g->hash_pos_atoms));
      printf("\nneg hash table after adding:");      
      wr_clterm_hashlist_print(g,rotp(g,g->hash_neg_atoms));  
#endif      
    }  
  }     
#ifdef DEBUG
  printf("cl_store_res_terms finished\n"); 
#endif      
  return 0;
}

int wr_cl_store_res_terms_new (glb* g, gptr cl) {    
  void* db=g->db;
  int i;
  int len;      
  int ruleflag; // 0 if not rule
  int poscount=0; // used only for pos/neg pref
  int negcount=0; // used only for pos/neg pref
  int posok=1;  // default allow
  int negok=1;  // default allow
  gint meta;
  gint atom;  
  int negflag; // 1 if negative
  int termflag; // 1 if complex atom  
  //gint hash;
  int addflag=0;
  int negadded=0;
  int posadded=0;
  vec hashvec;
  //void* hashdata;
  int tmp;
  //int hashposbits;
  
#ifdef DEBUG
  printf("cl_store_res_terms called on cl: "); 
  wr_print_clause(g,cl);
#endif  

  // get clause data for input clause
       
  ruleflag=wg_rec_is_rule_clause(db,cl);
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

#ifdef DEBUG
  printf("ruleflag %d len %d poscount %d negcount %d posok %d negok %d\n",
          ruleflag,len,poscount,negcount,posok,negok);
#endif  
  // loop over literals
#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
  if (ruleflag) parent = wg_get_rec_base_offset(db,cl);
#endif
#endif
  for(i=0; i<len; i++) {  
    negflag=0;
    termflag=0;
    addflag=0;
    if (!ruleflag) {
      atom=encode_record(db,cl);      
      addflag=1;
    } else {       
      meta=wg_get_rule_clause_atom_meta(db,cl,i);
      if (wg_atom_meta_is_neg(db,meta)) negflag=1;
      if (!((g->negpref_strat) || (g->pospref_strat)) ||
          (negflag && (g->hyperres_strat)) ||
          (negok && negflag && !negadded) || 
          (posok && !negflag)) {            
        if (negflag) negadded++; 
        else posadded++;          
        atom=wg_get_rule_clause_atom(db,cl,i);
        if (wg_get_encoded_type(db,atom)==WG_RECORDTYPE) {
          termflag=1;
#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
          if(parent) atom=wg_encode_parent_data(parent,atom);
#endif             
#endif
        }               
        addflag=1;
      }      
    }
    if (addflag) {      
#ifdef DEBUG 
      printf("before adding to hash negflag: %d\n",negflag);
#endif           
      if (negflag) hashvec=rotp(g,g->hash_neg_atoms);
      else hashvec=rotp(g,g->hash_pos_atoms);
      tmp=wr_term_hashstore(g,hashvec,atom,cl);    
      if (tmp) {
        wr_sys_exiterr2int(g,"adding term to hashlist in cl_store_res_terms, code ",tmp);
        return 1;        
      }  
#ifdef DEBUGHASH      
      printf("\nhash table after adding:");      
      wr_clterm_hashlist_print(g,hashvec);     
      printf("\npos hash table after adding:");      
      wr_clterm_hashdata_print(g,rotp(g,g->hash_pos_atoms));
      printf("\nneg hash table after adding:");      
      wr_clterm_hashdata_print(g,rotp(g,g->hash_neg_atoms));  
#endif      
    }  
  }     
#ifdef DEBUG
  printf("cl_store_res_terms finished\n"); 
#endif      
  return 0;
}


/* =====================================================

  top level hash storage funs for atom/clause 

====================================================== */


int wr_term_hashstore(glb* g, void* hashdata, gint term, gptr cl) {
  void* db=g->db;
  int pos;
  int bits;
  unsigned int hash=0;
  gptr tptr;
  int tlen;
  int uselen;
  int spos;
  int epos;
  int preflen;
  int prefpos;
  int nonvarflag;
  gint el;
  int thash;
  int tmp;
  int i;
  gint hashposbits=3; // bits 11
  //gptr nonvarhashbitset;
  //int nonvarhashbitsetsize;
  int nonvarhashbitsetsize=2;
  gint nonvarhashbitset[2];
  
  int maxhashpos=MAXHASHPOS;
  gint hasharr[MAXHASHPOS];
  
  
  nonvarhashbitset[0]=1;
  nonvarhashbitset[1]=3;
  
  // find the basic props of atom for hashing
  tptr=decode_record(db,term);  
  tlen=get_record_len(tptr);
  uselen=tlen;
  if (g->unify_maxuseterms) {
    if (((g->unify_maxuseterms)+(g->unify_firstuseterm))<uselen) 
      uselen=(g->unify_firstuseterm)+(g->unify_maxuseterms);
  }   
  spos=RECORD_HEADER_GINTS+(g->unify_firstuseterm);
  epos=RECORD_HEADER_GINTS+uselen;
  // loop over atom
  preflen=0;
  nonvarflag=1;
  for(pos=spos, preflen=0; pos<epos; ++pos, ++preflen) {    
    el=*(tptr+pos);
    if (isvar(el)) break;    
  } 
  // now we have basic data for atom top-level
  if (pos>=epos) {
    // fully non-var top level
    // precalc hash vals for positions
    for(pos=spos, prefpos=0; pos<epos && prefpos<maxhashpos; ++pos, ++prefpos) {    
      el=*(tptr+pos);
      thash=wr_term_basehash(g,el);
      hasharr[prefpos]=thash;      
    }     
    /*
    // compute complexhash and store to hashvecs
    for(bits=hashposbits, pos=spos; bits>0 && pos<epos; bits=bits>>1, ++pos) {
      if (bits & 1) {
        el=*(tptr+pos);
        thash=wr_term_basehash(g,el);
        //hash=hash+thash;      
        hash = thash + (hash << 6) + (hash << 16) - hash;
      }      
    }
    */    
    // loop over  hashbitset, compute complex hash and store            
    for(i=0;i<nonvarhashbitsetsize;i++) { 
      hash=0;      
      for(bits=nonvarhashbitset[i], pos=spos, preflen=0; 
         bits>0 && pos<epos; 
         bits=bits>>1, ++pos, ++preflen) {
        if (bits & 1) {
          thash=hasharr[preflen];
          //hash=hash+thash;      
          hash = thash + (hash << 6) + (hash << 16) - hash;
        }      
      }  
      if (hash<0) hash=0-hash; 
      hash=(1+(hash%(NROF_CLTERM_HASHVEC_ELS-2)));      
      // here store!
      tmp=1; // dummy
      //tmp=wr_clterm_add_hashlist(g,hashvec,hash,atom,cl); 
      if (tmp) {
        //wr_sys_exiterr2int(g,"adding toplevel-nonvar term to hashlist in wr_term_hashstore, code ",tmp);
        return 1;        
      }        
    }  
    return 0;
  }  
  // now we have vars in top level: use predicate hashtables
  if (1) {    
    el=get_field(tptr,(g->unify_funpos));  
    thash=wr_term_basehash(g,el);
    // here store!
    //tmp=1; // dummy    
    tmp=wr_clterm_add_hashlist(g,(vec)hashdata,thash,term,cl); 
    if (tmp) {
      //wr_sys_exiterr2int(g,"adding toplevel-nonvar term to hashlist in wr_term_hashstore, code ",tmp);
      return 1;        
    }        
  }    
  // everything ok
  return 0;
}  


gint wr_term_complexhash(glb* g, gint* hasharr, gint hashposbits, gint term) {
  int pos;
  int bits;
  unsigned int hash=0;
  gptr tptr;
  int tlen;
  int uselen;
  int spos;
  int epos;
  gint el;
  int thash;

#ifdef DEBUG
  printf("wr_term_complexhash called with term %d bits %d \n",term,hashposbits);
#endif    
  tptr=decode_record(g->db,term);  
  tlen=get_record_len(tptr);
  uselen=tlen;
  if (g->unify_maxuseterms) {
    if (((g->unify_maxuseterms)+(g->unify_firstuseterm))<uselen) 
      uselen=(g->unify_firstuseterm)+(g->unify_maxuseterms);
  }   
  spos=RECORD_HEADER_GINTS+(g->unify_firstuseterm);
  epos=RECORD_HEADER_GINTS+uselen;
  // first check if hashable (ie non-vars at hash positions)
  for(bits=hashposbits, pos=spos; bits>0 && pos<epos; bits=bits>>1, ++pos) {
    if (bits & 1) {
      el=*(tptr+pos);
      if (isvar(el)) return 0;
    }      
  } 
  // we know term is hashable: compute hash
  for(bits=hashposbits, pos=spos; bits>0 && pos<epos; bits=bits>>1, ++pos) {
    if (bits & 1) {
      el=*(tptr+pos);
      thash=wr_term_basehash(g,el);
      //hash=hash+thash;      
      hash = thash + (hash << 6) + (hash << 16) - hash;
    }      
  }  
  if (hash<0) hash=0-hash;  
#ifdef DEBUG
  printf("wr_term_complexhash computed hash %d using NROF_CLTERM_HASHVEC_ELS-2 %d gives final res %d \n",
         hash,NROF_CLTERM_HASHVEC_ELS-2,1+(hash%(NROF_CLTERM_HASHVEC_ELS-2)));
#endif    
  return (gint)(1+(hash%(NROF_CLTERM_HASHVEC_ELS-2)));
}  



gint wr_atom_funhash(glb* g, gint atom) {
  void* db=g->db;
  gint fun;
  gint chash;

#if 0
/* XXX: FIXME */
#ifdef USE_CHILD_DB
  gint parent; 
  parent=wg_get_rec_base_offset(db,cl);
  if(parent) enc=wg_encode_parent_data(parent, enc);
#endif    
#endif
  
  fun=get_field(decode_record(db,atom),(g->unify_funpos));  
  chash=wr_term_basehash(g,fun);
  return chash;
}    

/* =====================================================

  proper hash funs for atoms and terms

====================================================== */


gint wr_term_basehash(glb* g, gint enc) {
  void* db=g->db;
  int hash;
  int intdata;
  char *strdata, *exdata;
  double doubledata;
  
#ifdef DEBUG
  printf("wr_termhash called with enc %d visually ", enc);
  wr_print_simpleterm_otter(g,enc,(g->print_clause_detaillevel));  
  printf("\n");
#endif   
  switch(wg_get_encoded_type(db, enc)) {    
    case WG_NULLTYPE:
      hash=0;
      break;       
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      hash=int_hash(intdata);   
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      hash=double_hash(doubledata);      
      break;
    case WG_STRTYPE:
      strdata = wg_decode_unistr(db,enc,WG_STRTYPE);
      hash=str_hash(strdata);
      break;
    case WG_URITYPE:
      strdata = wg_decode_unistr(db, enc,WG_URITYPE);
      exdata =  wg_decode_unistr_lang(db, enc,WG_URITYPE);
      hash=str_dual_hash(strdata,exdata);      
      break;
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_unistr(db,enc,WG_XMLLITERALTYPE);
      exdata = wg_decode_unistr_lang(db,enc,WG_XMLLITERALTYPE);
      hash=str_dual_hash(strdata,exdata);
      break;
    case WG_CHARTYPE:      
      hash=int_hash(enc);
      break;
    case WG_DATETYPE:     
      hash=int_hash(enc);
      break;
    case WG_TIMETYPE:            
      hash=int_hash(enc);
      break;
    case WG_VARTYPE:
      hash=int_hash(0);
      break;
    case WG_ANONCONSTTYPE:     
      hash=int_hash(enc);
      break; 
    case WG_RECORDTYPE:
      //  ptrdata = (gint) wg_decode_record(db, enc);
      //  wg_print_subrecord_otter(db,(gint*)ptrdata);
      hash=1;
      break;
    default:
      hash=2;
      break;
  }
  if (hash<0) hash=0-hash;
#ifdef DEBUG
  printf("wr_termhash computed hash %d using NROF_CLTERM_HASHVEC_ELS-2 %d gives final res %d \n",
         hash,NROF_CLTERM_HASHVEC_ELS-2,0+(hash%(NROF_CLTERM_HASHVEC_ELS-2)));
#endif    
  return (gint)(0+(hash%(NROF_CLTERM_HASHVEC_ELS-2)));
}


static int int_hash(int x) {
  unsigned int a;
  
  if (x>=0 && x<NROF_CLTERM_HASHVEC_ELS-4) return x+3;
  a=(unsigned int)x;
  a -= (a<<6);
  a ^= (a>>17);
  a -= (a<<9);
  a ^= (a<<4);
  a -= (a<<3);
  a ^= (a<<10);
  a ^= (a>>15);
  return (int)a;  
}  

static int double_hash(double x) {
  if (x==(double)0) return 20;
  return int_hash((int)(x*1000));  
} 

static int str_hash(char* x) {
  unsigned long hash = 0;
  int c;  
    
  if (x!=NULL) {
    while(1) {
      c = (int)(*x);
      if (!c) break;
      hash = c + (hash << 6) + (hash << 16) - hash;
      x++;
    }
  }      
  return (int)hash;  
} 


static int str_dual_hash(char* x, char* y) {
  unsigned long hash = 0;
  int c;  
    
  if (x!=NULL) {
    while(1) {
      c = (int)(*x);
      if (!c) break;
      hash = c + (hash << 6) + (hash << 16) - hash;
      x++;
    }
  }    
  if (y!=NULL) {
    while(1) {
      c = (int)(*y);
      if (!c) break;
      hash = c + (hash << 6) + (hash << 16) - hash;
      y++;
    }
  }   
  return (int)hash;  
  
}


/* =====================================================

  storage to hashdata 

====================================================== */



int wr_clterm_add_hashlist(glb* g, vec hashvec, gint hash, gint term, gptr cl) {
  void* db=g->db;
  gint vlen;
  gint cell;
  gptr node;
  gptr prevnode;
  gint nextnode;
  
  vlen=VEC_LEN(hashvec);
  if (hash>=vlen || hash<1) return 1; // err case
  cell=hashvec[hash];
  if (cell==0) {
    // no hash chain yet: add first len-containing node
    prevnode=wr_clterm_alloc_hashnode(g);
    if (prevnode==NULL) {
      wr_sys_exiterr(g,"could not allocate node for hashlist in cl_store_res_terms");
      return 1;
    }  
    hashvec[hash]=pto(db,prevnode);
    prevnode[CLTERM_HASHNODE_LEN_POS]=1;            
    nextnode=0;
  } else {
    // hash chain exists: first node contains counter to increase
    // then take next ptr for node to handle
    prevnode=otp(db,cell);
    prevnode[CLTERM_HASHNODE_LEN_POS]++;
    nextnode=prevnode[CLTERM_HASHNODE_NEXT_POS];
  } 
  // make new node and add to chain
  node=wr_clterm_alloc_hashnode(g);
  if (node==NULL) {
    wr_sys_exiterr(g,"could not allocate node for hashlist in cl_store_res_terms");
    return 1;
  } 
  node[CLTERM_HASHNODE_TERM_POS]=term;    
  node[CLTERM_HASHNODE_CL_POS]=pto(db,cl);
  node[CLTERM_HASHNODE_NEXT_POS]=nextnode;  
  prevnode[CLTERM_HASHNODE_NEXT_POS]=pto(db,node);
  return 0;
}  


int wr_clterm_add_hashlist_new (glb* g, vec hashvec, gint hash, gint term, gptr cl) {
  void* db=g->db;
  gint vlen;
  gint cell;
  gptr node;
  gptr prevnode;
  gint nextnode;
  
  vlen=VEC_LEN(hashvec);
  if (hash>=vlen || hash<1) return 1; // err case
  cell=hashvec[hash];
  if (cell==0) {
    // no hash chain yet: add first len-containing node
    prevnode=wr_clterm_alloc_hashnode(g);
    if (prevnode==NULL) {
      wr_sys_exiterr(g,"could not allocate node for hashlist in cl_store_res_terms");
      return 1;
    }  
    hashvec[hash]=pto(db,prevnode);
    prevnode[CLTERM_HASHNODE_LEN_POS]=1;            
    nextnode=0;
  } else {
    // hash chain exists: first node contains counter to increase
    // then take next ptr for node to handle
    prevnode=otp(db,cell);
    prevnode[CLTERM_HASHNODE_LEN_POS]++;
    nextnode=prevnode[CLTERM_HASHNODE_NEXT_POS];
  } 
  // make new node and add to chain
  node=wr_clterm_alloc_hashnode(g);
  if (node==NULL) {
    wr_sys_exiterr(g,"could not allocate node for hashlist in cl_store_res_terms");
    return 1;
  } 
  node[CLTERM_HASHNODE_TERM_POS]=term;    
  node[CLTERM_HASHNODE_CL_POS]=pto(db,cl);
  node[CLTERM_HASHNODE_NEXT_POS]=nextnode;  
  prevnode[CLTERM_HASHNODE_NEXT_POS]=pto(db,node);
  return 0;
}  

int wr_clterm_hashlist_len(glb* g, vec hashvec, gint hash) {
  gint vlen;
  gint cell;
  
  vlen=VEC_LEN(hashvec);
  if (hash>=vlen || hash<1) return -1; // err case
  cell=hashvec[hash];
  if (cell==0) return 0;
  return (int)((rotp(g,cell))[CLTERM_HASHNODE_LEN_POS]);
}  

gint wr_clterm_hashlist_start(glb* g, vec hashvec, gint hash) {
  gint vlen;
  gint cell;
  
  vlen=VEC_LEN(hashvec);
  if (hash>=vlen || hash<1) return -1; // err case
  cell=hashvec[hash];
  if (cell==0) return 0; // empty case
  return (rotp(g,cell))[CLTERM_HASHNODE_NEXT_POS];
}  

int wr_clterm_hashlist_next(glb* g, vec hashvec, gint lastel) {
  return (rotp(g,lastel))[CLTERM_HASHNODE_NEXT_POS];
}  

gptr wr_clterm_alloc_hashnode(glb* g) {
  return sys_malloc(sizeof(gint)*CLTERM_HASHNODE_GINT_NR);
}  

void wr_clterm_free_hashnode(glb* g,gptr node) {
  sys_free(node);
} 

void wr_clterm_hashlist_free(glb* g, vec hashvec) {
  void* db=g->db;
  gint vlen;
  gint node;
  gint nextnode;
  int i;
  
  vlen=VEC_LEN(hashvec); 
  //printf("\nhashvec len %d and els:\n",vlen);  
  for(i=VEC_START;i<vlen;i++) {
    if (hashvec[i]!=0) {
      //printf("chain %d len %d:\n",i,(otp(db,hashvec[i]))[CLTERM_HASHNODE_LEN_POS]);
      node=(otp(db,hashvec[i]))[CLTERM_HASHNODE_NEXT_POS];
      while(node!=0) {
        //printf("term \n");
        //wr_print_term(g,(otp(db,node))[CLTERM_HASHNODE_TERM_POS]);
        //printf(" in cl \n");
        //wr_print_clause(g,otp(db,((otp(db,node))[CLTERM_HASHNODE_CL_POS])));
        //printf("cp\n");
        nextnode=(otp(db,node))[CLTERM_HASHNODE_NEXT_POS];
        wr_clterm_free_hashnode(g,(otp(db,node)));
        node=nextnode;
      }        
    }  
  } 
  wr_vec_free(g,hashvec);
}  

void wr_clterm_hashlist_print(glb* g, vec hashvec) {
  gint vlen;
  gint node;
  int i;
  
  vlen=VEC_LEN(hashvec); 
  printf("\nhashvec len %d and els:\n",vlen);  
  for(i=VEC_START;i<vlen;i++) {
    if (hashvec[i]!=0) {
      printf("i %d len %d:\n",i,(rotp(g,hashvec[i]))[CLTERM_HASHNODE_LEN_POS]);
      node=(rotp(g,hashvec[i]))[CLTERM_HASHNODE_NEXT_POS];
      while(node!=0) {
        printf("term ");
        wr_print_term(g,(rotp(g,node))[CLTERM_HASHNODE_TERM_POS]);
        printf(" in cl ");
        wr_print_clause(g,rotp(g,(rotp(g,node))[CLTERM_HASHNODE_CL_POS]));
        node=(rotp(g,node))[CLTERM_HASHNODE_NEXT_POS];
        //printf(" node %d \n",node);
      }        
    }  
  } 
  printf("hashvec printed\n");  
}  

#ifdef __cplusplus
}
#endif


