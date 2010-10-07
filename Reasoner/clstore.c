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
//#undefine DEBUG  

#define HASH_STORE_FLAG 0 // see also HASH_CUT_FLAG in derive.c 


/* ====== Private headers ======== */

static gint conststrhash(char* str);

/* ====== Functions ============== */


/*

store a clause in a passive queue

*/

void wr_push_clqueue_cl(glb* g, gptr cl) {
  int i;
  
  printf("pushing to clqueue pos %d\n",(rotp(g,g->clqueue))[1]);  
  (g->clqueue)=rpto(g,wr_cvec_push(g,rotp(g,(g->clqueue)),(gint)cl));
  //wr_show_clqueue(g);
}


void wr_show_clqueue(glb* g) {
  int i;
  
  for(i=2;i<(rotp(g,g->clqueue))[1];i++) {
    printf("\nclqueue nr %d :",i);
    wg_print_record(g->db,((rotp(g,g->clqueue))[i]));    
  } 
}

/*

make a clause active

*/

void wr_push_clactive_cl(glb* g, gptr cl) {
  
  printf("pushing to clactive pos %d\n",(rotp(g,g->clactive))[1]);
  (g->clactive)=rpto(g,wr_cvec_push(g,rotp(g,(g->clactive)),(gint)cl));  
  wr_cl_store_res_terms(g,cl);
}  


void wr_show_clactive(glb* g) {
  int i;
  
  for(i=2;i<(rotp(g,g->clactive))[1];i++) {
    printf("\nclactive nr %d :",i);
    wg_print_record(g->db,((rotp(g,g->clactive))[i]));    
  } 
}

/*

store resolvable literals/terms of a clause to fast resolvable-lit list
returns 0 iff ok
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
  gint parent;
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
#ifdef USE_CHILD_DB
  if (ruleflag) parent = wg_get_rec_base_offset(db,cl);
#endif
  for(i=0; i<len; i++) {  
    negflag=0;
    termflag=0;
    addflag=0;
    if (!ruleflag) {
      atom=wg_encode_record(db,cl);      
      addflag=1;
    } else {       
      meta=wg_get_rule_clause_atom_meta(db,cl,i);
      if (wg_atom_meta_is_neg(db,meta)) negflag=1;
      if (!((g->negpref_strat) || (g->pospref_strat)) ||
          (negok && negflag && !negadded) || 
          (posok && !negflag)) {            
        if (negflag) negadded++; 
        else posadded++;          
        atom=wg_get_rule_clause_atom(db,cl,i);
        if (wg_get_encoded_type(db,atom)==WG_RECORDTYPE) {
          termflag=1;
#ifdef USE_CHILD_DB
          if(parent) atom=wg_encode_parent_data(parent,atom);
#endif             
        }               
        addflag=1;
      }      
    }
    if (addflag) {
      hash=wr_atom_funhash(g,atom);
      if (negflag) hashvec=rotp(g,g->hash_neg_atoms);
      else hashvec=rotp(g,g->hash_pos_atoms);
      tmp=wr_clterm_add_hashlist(g,hashvec,hash,atom,cl);
      if (tmp) {
        wr_sys_exiterr2int(g,"adding term to hashlist in cl_store_res_terms, code ",tmp);
        return 1;        
      }  
#ifdef DEBUG      
      wr_clterm_hashlist_print(g,hashvec);
#endif      
    }  
  }     
#ifdef DEBUG
  printf("cl_store_res_terms finished\n"); 
#endif      
  return 0;
}



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
  gptr cell;
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
  gptr cell;
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



gint wr_atom_funhash(glb* g, gint atom) {
  void* db=g->db;
  gint fun;
  gint chash;
  gint hash;
  gptr atomptr;
  gint parent;  


#ifdef USE_CHILD_DB
  parent=wg_get_rec_base_offset(db,cl);
  if(parent) enc=wg_encode_parent_data(parent, enc);
#endif    
  
  fun=wg_get_field(db,wg_decode_record(db,atom),TERM_EXTRAHEADERLEN);  
  chash=consthash(g,fun);
  return chash;
}    

gint consthash(glb* g, gint enc) {
  void* db=g->db;
  gint hash;
  int intdata;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[80];
  
#ifdef DEBUG
  printf("consthash called with enc %d visually ", enc);
  wg_print_simpleterm_otter(db,enc);  
  printf("\n");
#endif   
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      hash=0;
      break;
    //case WG_RECORDTYPE:
    //  ptrdata = (gint) wg_decode_record(db, enc);
    //  wg_print_subrecord_otter(db,(gint*)ptrdata);
    //  break;    
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      hash=intdata;   
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      hash=(int)doubledata;      
      break;
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      hash=conststrhash(strdata);
      break;
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      hash=conststrhash(strdata)+conststrhash(exdata);      
      break;
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      hash=conststrhash(strdata)+conststrhash(exdata);
      break;
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      hash=intdata;
      break;
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      hash=conststrhash(strbuf);
      break;
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);      
      hash=intdata;
      break;
    case WG_VARTYPE:
      hash=0;
      break;
    case WG_ANONCONSTTYPE:
      strdata = wg_decode_anonconst(db, enc);
      hash=conststrhash(strdata);
      break; 
    default:
      hash=0;
      break;
  }
#ifdef DEBUG
  printf("consthash computed hash %d using NROF_CLTERM_HASHVEC_ELS-1 %d gives final res %d \n",
         hash,NROF_CLTERM_HASHVEC_ELS-2,1+(hash%(NROF_CLTERM_HASHVEC_ELS-2)));
#endif    
  return 1+(hash%(NROF_CLTERM_HASHVEC_ELS-2));
}
  

gint conststrhash(char* str) {
  gint res=0;
  
  if (str==NULL) return 0;
  while(*str) {
    res=res+(gint)(*str);  
    str++;
  }
  return res;
}  

#ifdef __cplusplus
}
#endif


