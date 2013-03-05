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

 /** @file clterm.c
 *  Procedures for building clauses/terms and fetching parts.
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
  
  
/* ====== Functions ============== */

/* ---------------- wr functions --------------------- */    




gptr wr_create_raw_record(glb* g, gint length, gint meta, gptr buffer) {
  gptr rec;
  
  if (buffer==NULL) {
    rec=wg_create_raw_record(g->db,length); 
    if (rec==NULL) return NULL;
    rec[RECORD_META_POS]=meta;    
  } else {
    rec=wr_alloc_from_cvec(g,buffer,length+RECORD_HEADER_GINTS);
    if (rec==NULL) return NULL;
    rec[0]=(length+RECORD_HEADER_GINTS)*sizeof(gint);
    rec[RECORD_BACKLINKS_POS]=0; 
    rec[RECORD_META_POS]=meta;    
    //printf("wr_create_raw_record created: \n");
    //wg_print_record(g->db,rec);
  }    
  return rec;
}  

  
  
/* ---------------- wg functions --------------------- */  
  
void* wr_create_rule_clause(glb* g, int litnr) {
  void* res;
  
  res=wg_create_raw_record(g->db,CLAUSE_EXTRAHEADERLEN+(LIT_WIDTH*litnr));
  //printf("meta %d",*((gint*)res+RECORD_META_POS));
  *((gint*)res+RECORD_META_POS)=(RECORD_META_NOTDATA | RECORD_META_RULE_CLAUSE);
  return res;  
} 

void* wr_create_fact_clause(glb* g, int litnr) {
  void* res;
  
  res=wg_create_raw_record(g->db,(g->unify_firstuseterm)+litnr+(g->unify_footerlen));
  *((gint*)res+RECORD_META_POS)=RECORD_META_FACT_CLAUSE;
  return res;  
} 

void* wr_create_atom(glb* g, int termnr) {
  void* res;
  
  res=wg_create_raw_record(g->db,(g->unify_firstuseterm)+termnr+(g->unify_footerlen));
  *((gint*)res+RECORD_META_POS)=(RECORD_META_NOTDATA | RECORD_META_ATOM);
  return res;  
} 


void* wr_create_term(glb* g, int termnr) {
  void* res;
  
  res=wg_create_raw_record(g->db,(g->unify_firstuseterm)+termnr+(g->unify_footerlen));
  *((gint*)res+RECORD_META_POS)=(RECORD_META_NOTDATA | RECORD_META_TERM);
  return res;  
} 



void* wr_convert_atom_fact_clause(glb* g, void* atom, int isneg) {
  void* res;
  
  res=atom;
  *((gint*)res+RECORD_META_POS)=(RECORD_META_ATOM | RECORD_META_FACT_CLAUSE);
  return res;  
} 

int wr_set_rule_clause_atom(glb* g, void* clause, int litnr, gint atom) {
  
  //wg_set_new_field(db,clause,CLAUSE_EXTRAHEADERLEN+(LIT_WIDTH*litnr)+1,atom);
  *((gint*)clause+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN+(LIT_WIDTH*litnr)+1)=atom;
  return 0;
} 

int wr_set_rule_clause_atom_meta(glb* g, void* clause, int litnr, gint meta) {
  
  //wg_set_new_field(db,clause,CLAUSE_EXTRAHEADERLEN+(LIT_WIDTH*litnr),meta);
  *((gint*)clause+RECORD_HEADER_GINTS+CLAUSE_EXTRAHEADERLEN+(LIT_WIDTH*litnr))=meta;
  return 0;
} 

int wr_set_atom_subterm(glb* g, void* atom, int termnr, gint subterm) {
  
  wg_set_new_field(g->db,atom,(g->unify_firstuseterm)+termnr,subterm);  
  return 0;
} 


int wr_set_term_subterm(glb* g, void* term, int termnr, gint subterm) {
  
  wg_set_new_field(g->db,term,(g->unify_firstuseterm)+termnr,subterm);  
  return 0;
}



#ifdef __cplusplus
}
#endif
