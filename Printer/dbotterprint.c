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

 /** @file dbotterprint.c
 *  Top level procedures for otterprinter
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "../Db/dbdata.h"
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
#include "../Db/dbmpool.h"


#include "../Reasoner/clterm.h"

#include "dbotterprint.h"

               
/* ====== Private headers and defs ======== */


/* ======== Data ========================= */

/* ====== Private protos ======== */

/*
static gint show_print_error(void* db, char* errmsg);
static gint show_print_error_nr(void* db, char* errmsg, gint nr);
static gint show_print_error_str(void* db, char* errmsg, char* str);
*/

/* ====== Functions ============== */


/* ========== prover functions taking glb as arg ======== */

void wr_print_clause(glb* g, gptr rec) {

  wg_print_clause_otter((g->db),rec);
}  

void wr_print_term(glb* g, gint rec) {

  wg_print_term_otter((g->db),rec);
}  


/* ========== wg functions taking db as arg ============ */

/** Print whole db
 *
 */
 
void wg_print_db_otter(void* db) { 
  void *rec;
  
  rec = wg_get_first_raw_record(db);
  while(rec) {
    if (wg_rec_is_rule_clause(db,rec)) {
      wg_print_rule_clause_otter(db, (gint *) rec);
      printf("\n"); 
    } else if (wg_rec_is_fact_clause(db,rec)) {
      wg_print_fact_clause_otter(db, (gint *) rec);
      printf("\n"); 
    }            
    rec = wg_get_next_raw_record(db,rec);    
  }
}


/** Print single clause (rule/fact record)
 *
 */

void wg_print_clause_otter(void *db, gint* rec) {
  //printf("wg_print_clause_otter called with rec ");
  //wg_print_record(db,rec);
  if (wg_rec_is_rule_clause(db,rec)) {
      //printf("ruleclause\n");
      wg_print_rule_clause_otter(db, (gint *) rec);
      printf("\n"); 
  } else if (wg_rec_is_fact_clause(db,rec)) {
      //printf("factclause\n");
      wg_print_fact_clause_otter(db, (gint *) rec);
      printf("\n"); 
  }     
  //printf("wg_print_clause_otter exiting\n");
}

/** Print single rule record
 *
 */

void wg_print_rule_clause_otter(void *db, gint* rec) {
  gint meta, enc;
  int i, len;
  //char strbuf[256];
#ifdef USE_CHILD_DB
  gint parent;
#endif

  if (rec==NULL) {
    printf("<null rec pointer>\n");
    return;
  }  

#ifdef USE_CHILD_DB
  parent = wg_get_rec_base_offset(db, rec);
#endif
  //len = wg_get_record_len(db, rec);
  len = wg_count_clause_atoms(db, rec);
  //printf("[%d ",len);
  for(i=0; i<len; i++) {
    //printf(" #%d-%d ",i,((i-CLAUSE_EXTRAHEADERLEN)%2));
    //if (i<CLAUSE_EXTRAHEADERLEN) continue;
    //if (((i-CLAUSE_EXTRAHEADERLEN)%2)==0) continue;
    //if(i>(CLAUSE_EXTRAHEADERLEN+2)) printf(" | ");
    //if (i>1 && i+1<len) printf(" | ");
    //enc = wg_get_field(db, rec, i);
    
    if (i>0 && i<len) printf(" | ");
    meta=wg_get_rule_clause_atom_meta(db,rec,i);
    enc=wg_get_rule_clause_atom(db,rec,i);
    //printf("[i %d meta %d enc %d]",i,meta,enc);
    //enc=wg_get_field(db,rec,i);
#ifdef USE_CHILD_DB
    if(parent)
      enc = wg_encode_parent_data(parent, enc);
#endif
    if (wg_atom_meta_is_neg(db,meta)) printf("-");
    if (wg_get_encoded_type(db, enc)==WG_RECORDTYPE) {   
      wg_print_atom_otter(db,enc);
    } else {  
      wg_print_simpleterm_otter(db, enc);
    }      
  }
  printf(".");
}

/** Print single fact record
 *
 */

void wg_print_fact_clause_otter(void *db, gint* rec) {
  wg_print_atom_otter(db,wg_encode_record(db,rec));
  printf(".");
}


void wg_print_atom_otter(void *db, gint rec) {
  gptr recptr;
  gint len, enc;
  int i;
  
  if (wg_get_encoded_type(db,rec)!=WG_RECORDTYPE) {
    wg_print_simpleterm_otter(db,rec);
    return;
  }
#ifdef USE_CHILD_DB
  gint parent;
  parent = wg_get_rec_base_offset(db, rec);
#endif
  recptr=wg_decode_record(db, rec);
  len = wg_get_record_len(db, recptr);
  //printf("[");
  for(i=0; i<len; i++) {
    if (i<TERM_EXTRAHEADERLEN) continue;
    if(i>(TERM_EXTRAHEADERLEN+1)) printf(",");
    enc = wg_get_field(db, recptr, i);
#ifdef USE_CHILD_DB
    if(parent)
      enc = wg_encode_parent_data(parent, enc);
#endif
    if (wg_get_encoded_type(db, enc)==WG_RECORDTYPE) {
      wg_print_term_otter(db,enc);
    } else {  
      wg_print_simpleterm_otter(db, enc);
    }       
    if (i==TERM_EXTRAHEADERLEN) printf("(");
  }
  printf(")");
}


void wg_print_term_otter(void *db, gint rec) {
  gptr recptr;
  gint len, enc;
  int i;
  
  if (wg_get_encoded_type(db,rec)!=WG_RECORDTYPE) {
    wg_print_simpleterm_otter(db,rec);
    return;
  }
#ifdef USE_CHILD_DB
  gint parent;
  parent = wg_get_rec_base_offset(db, rec);
#endif
  recptr=wg_decode_record(db, rec);
  len = wg_get_record_len(db, recptr);
  for(i=0; i<len; i++) {
    if (i<TERM_EXTRAHEADERLEN) continue;
    if(i>(TERM_EXTRAHEADERLEN+1)) printf(",");
    enc = wg_get_field(db, recptr, i);
#ifdef USE_CHILD_DB
    if(parent)
      enc = wg_encode_parent_data(parent, enc);
#endif
    if (wg_get_encoded_type(db, enc)==WG_RECORDTYPE) {
      wg_print_term_otter(db,enc);
    } else {  
      wg_print_simpleterm_otter(db, enc);
    }       
    if (i==TERM_EXTRAHEADERLEN) printf("(");
  }
  printf(")");
}


/** Print a single, encoded value or a subrecord
 *  
 */
void wg_print_simpleterm_otter(void *db, gint enc) {
  int intdata;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[80];
 
  //printf("simpleterm called with enc %d and type %d \n",(int)enc,wg_get_encoded_type(db,enc)); 
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      printf("NULL");
      break;
    //case WG_RECORDTYPE:
    //  ptrdata = (gint) wg_decode_record(db, enc);
    //  wg_print_subrecord_otter(db,(gint*)ptrdata);
    //  break;    
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      printf("%d", intdata);
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      printf("%f", doubledata);
      break;
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      printf("\"%s\"", strdata);
      break;
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      if (exdata==NULL)
        printf("%s", strdata);
      else
        printf("%s:%s", exdata, strdata);
      break;      
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      printf("\"<xsdtype %s>%s\"", exdata, strdata);
      break;
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      printf("%c", (char) intdata);
      break;
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      printf("<raw date %d>%s", intdata,strbuf);
      break;
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);        
      printf("<raw time %d>%s",intdata,strbuf+11);
      break;
    case WG_VARTYPE:
      intdata = wg_decode_var(db, enc);
      printf("?%d", intdata);
      break;
    case WG_ANONCONSTTYPE:
      strdata = wg_decode_anonconst(db, enc);
      printf("!%s", strdata);
      break; 
    default:
      printf("<unsupported type>");
      break;
  }
}





/* ------------ errors ---------------- */
/*

static gint show_print_error(void* db, char* errmsg) {
  printf("wg otterprint error: %s\n",errmsg);
  return -1;
}


static gint show_print_error_nr(void* db, char* errmsg, gint nr) {
  printf("wg parser error: %s %d\n", errmsg, (int) nr);
  return -1;
}

static gint show_print_error_str(void* db, char* errmsg, char* str) {
  printf("wg parser error: %s %s\n",errmsg,str);
  return -1;
}
*/
