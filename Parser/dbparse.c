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

 /** @file dbparse.c
 *  Top level procedures for parsers
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>


#include "../Db/dbdata.h"
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
#include "../Db/dbmpool.h"
#include "../Printer/dbotterprint.h"
#include "../Reasoner/clterm.h"
#include "dbparse.h"
#include "dbgenparse.h"
#include "dbotterparse.h"
#include "dbprologparse.h"


               
/* ====== Private headers and defs ======== */


#define MAX_URI_SCHEME 10
#define VARDATALEN 1000

#undef DEBUG

#ifdef DEBUG
#define DPRINTF(...) { printf(__VA_ARGS__); }
#else
#define DPRINTF(...) ;
#endif


//static void otter_escaped_str(void *db, char *iptr, char *buf, int buflen);


static int show_parse_error(void* db, char* format, ...);
static int show_parse_warning(void* db, char* format, ...);


/* ======== Data ========================= */

/** Recognized URI schemes (used when parsing input data)
 * when adding new schemes, check that MAX_URI_SCHEME is enough to
 * store the entire scheme + '\0'
 */

struct uri_scheme_info {
  char *prefix;
  int length;
} uri_scheme_table_otter[] = {
  { "urn:", 4 },
  { "file:", 5 },
  { "http://", 7 },
  { "https://", 8 },
  { "mailto:", 7 },
  { NULL, 0 }
};


/* ====== Private protos ======== */

/* ====== Functions ============== */

int wg_import_otter_file(void* db, char* filename) {
  parse_parm  pp;
  char* fnamestr;  
  FILE* fp;    
  //char* buf; 
  int pres=1;
  void* pres2=NULL;
  void *mpool;
  //void* ptr;

  DPRINTF("wg_import_otter_file called\n"); 
  fnamestr=filename;   
  fp=freopen(fnamestr, "r", stdin);
  pp.db=db;
  pp.filename=fnamestr;
  pp.foo="abcba";
  mpool=wg_create_mpool(db,1000000); 
  pp.mpool=mpool;  
    
  wg_yyotterlex_init(&pp.yyscanner);
  wg_yyotterset_extra(&pp, pp.yyscanner);
  pres=wg_yyotterparse(&pp, pp.yyscanner);      
  wg_yyotterlex_destroy(pp.yyscanner);   
  DPRINTF("result: %d\n",pres); 
  if (!pres) { 
    printf("\nOtter parser result:\n");    
    wg_mpool_print(db,pp.result);
    pres2=wg_parse_clauselist(db,mpool,pp.result);
  }     
  DPRINTF("\notterparse quitting with pres2 %d .\n",(int)pres2);   
  if (pres2==NULL) {  
    DPRINTF("\npres2 is null.\n");        
  } else { 
    //wg_mpool_print(db,pres2); 
    printf("\nGeneric parser result:\n");
    wg_print_db_otter(db);
  }  
  wg_free_mpool(db,mpool);
  if (pres || pres2==NULL) return 1;
  else return 0;  
}


/*
void wg_yyottererror (parse_parm* parm, void* scanner, char* msg) {
  printf("\n yyerror called with msg %s\n",msg);
  printf("\ input error at line %d token %s \n", yylineno,yytext);
  return;  
}  
*/

/*
void wg_yyottererror (const char *s) {
   char* errbuf;
   char* tmp;
  
   errbuf=malloc(1000);
   //(g->parser_errbuf)=errbuf;
   //snprintf(errbuf,1000,"input error at line %d: %s", wg_yyotterlineno, s);  
   sprintf(errbuf,1000,"input error at line %d: %s", wg_yyotterlineno, s);
   //tmp=xml_encode_str(errbuf);
   tmp=errbuf;
   //rqlgandalferr(-1,tmp);
   printf ("otterparse error at line %d: %s\n", wg_yyotterlineno, s);
   exit(0); 
   //printf ("parse error at line %d: %s\n", wg_yyotterlineno, s);
}

*/


int wg_import_prolog_file(void* db, char* filename) {
  parse_parm  pp;
  char* fnamestr;  
  FILE* fp;    
  //char* buf; 
  
  DPRINTF("Hello from dbprologparse!\n"); 
  /*
  buf=malloc(1000);
  strcpy(buf,"23*(10+)");
  pp.buf = buf;
  pp.length = strlen(buf);
  pp.pos = 0;
  */
  fnamestr=filename;   
  fp=freopen(fnamestr, "r", stdin);
  pp.db=db;
  pp.filename=fnamestr;
  pp.foo="abcba";
  
  wg_yyprologlex_init(&pp.yyscanner);
  wg_yyprologset_extra(&pp, pp.yyscanner);
  wg_yyprologparse(&pp, pp.yyscanner);
         
  wg_yyprologlex_destroy(pp.yyscanner);
  DPRINTF("\nprologparse quitting.\n");
  
  return 0;  
}



/* ---- convert parser-returned list to db records --------- */

void* wg_parse_clauselist(void *db,void* mpool,void* clauselist) {
  void* lpart;
  void* cl;
  void* clpart;
  void* lit;
  void* atom;
  int clnr=0;
  int litnr=0;
  void* fun;
  int isneg=0;
  void* tmpptr;
  void* atomres;
  gint ameta;
  gint tmpres2;
  gint setres;
  gint setres2;
  void* record=NULL;
  int issimple;
  void* termpart; 
  void* subterm;
  void* resultlist=NULL;
  char** vardata;
  int i;
  
#ifdef DEBUG  
  DPRINTF("wg_parse_rulelist starting with clauselist\n");  
  wg_mpool_print(db,clauselist); 
  DPRINTF("\n");
#endif

  // create vardata block by malloc or inside mpool
  
  //vardata=(char**)(wg_alloc_mpool(db,mpool,sizeof(char*)*VARDATALEN));
  vardata=(char**)(malloc(sizeof(char*)*VARDATALEN));
  for(i=0;i<VARDATALEN;i++) vardata[i]=NULL; // pointers to varstrings
  
  // loop over clauses
  
  for(lpart=clauselist,clnr=0;wg_ispair(db,lpart);lpart=wg_rest(db,lpart),clnr++) {
    cl=wg_first(db,lpart);

#ifdef DEBUG    
    DPRINTF("\nclause nr  %d:",clnr);    
    wg_mpool_print(db,cl); 
    printf("\n");
#endif    
    if (!wg_ispair(db,cl)) {
      show_parse_warning(db,"clause nr %d is atomic and hence ignored: ",clnr); 
#ifdef DEBUG      
      wg_mpool_print(db,cl); 
      printf("\n");
#endif      
      continue;
    }  
    
    // examine clause: find clause length and check if simple clause    

    issimple=1;
    litnr=0;
    for(clpart=cl;wg_ispair(db,clpart);clpart=wg_rest(db,clpart),litnr++) {
      lit=wg_first(db,clpart); 
#ifdef DEBUG      
      DPRINTF("lit: ");
      wg_mpool_print(db,lit); 
      printf("\n");      
#endif      
      if (!wg_ispair(db,lit)) { issimple=0; continue; }
      fun=wg_first(db,lit);
      if (wg_atomtype(db,fun)==WG_ANONCONSTTYPE && !strcmp(wg_atomstr1(db,fun),"not")) { issimple=0; continue; }
      for(termpart=lit;wg_ispair(db,termpart);termpart=wg_rest(db,termpart)) {
        subterm=wg_first(db,termpart);
#ifdef DEBUG        
        DPRINTF("subterm: ");
        wg_mpool_print(db,subterm); 
        printf("\n");
#endif        
        if (subterm!=NULL && wg_ispair(db,subterm)) { issimple=0; break; }     
        if (wg_atomtype(db,subterm)==WG_VARTYPE) { issimple=0; break; }     
      }      
    }        
    if (litnr>1) issimple=0;
    DPRINTF("\nclause issimple res %d length %d\n",issimple,litnr);    
    
    // create record for a rule clause
    
    if (!issimple) {
      record=wg_create_rule_clause(db,litnr);   
      if (((int)record)==0) {
        free(vardata);
        return NULL;
      }
      resultlist=wg_mkpair(db,mpool,record,resultlist);       
    }  
    
    // clear vardata block for the next clause
    
    for(i=0;i<VARDATALEN;i++) {
      if (vardata[i]==NULL) break;
      vardata[i]=NULL;       
    }      
    
    // process one clause
    
    for(clpart=cl,litnr=0;wg_ispair(db,clpart);clpart=wg_rest(db,clpart),litnr++) {
      lit=wg_first(db,clpart);
#ifdef DEBUG    
      DPRINTF("\nlit nr  %d:",litnr);    
      wg_mpool_print(db,lit); 
      printf("\n");
#endif      
      if (!wg_ispair(db,lit)) {
        show_parse_warning(db,"lit nr %d in clause nr %d is atomic and hence ignored: ",litnr,clnr); 
#ifdef DEBUG        
        wg_mpool_print(db,lit); 
        printf("\n");
#endif        
        continue;
      }              
      
      fun=wg_first(db,lit);
      if (!wg_isatom(db,fun)) {
        show_parse_warning(db,"lit nr %d in clause nr %d has nonatomic leadfun and hence ignored: ",litnr,clnr); 
#ifdef DEBUG        
        wg_mpool_print(db,fun); 
        printf("\n");
#endif        
        continue;
      } 
      
      isneg=0;
      if (wg_atomtype(db,fun)==WG_ANONCONSTTYPE && 
          !strcmp(wg_atomstr1(db,fun),"not") &&
          wg_atomstr2(db,fun)==NULL) {
        DPRINTF("detected negation");
        isneg=1;            
        tmpptr=wg_rest(db,lit);    
        if (!wg_ispair(db,tmpptr)) {
          show_parse_warning(db,"lit nr %d in clause nr %d does not contain proper atom after negation and hence ignored: ",litnr,clnr); 
#ifdef DEBUG          
          wg_mpool_print(db,lit); 
          printf("\n");
#endif          
          continue;
        }
        atom=wg_first(db,tmpptr);
        if (!wg_ispair(db,atom)) {
          show_parse_warning(db,"lit nr %d in clause nr %d is atomic (after negation) and hence ignored: ",litnr,clnr); 
#ifdef DEBUG          
          wg_mpool_print(db,lit); 
          printf("\n");
#endif          
          continue;
        }
        fun=wg_first(db,atom);
        if (!wg_isatom(db,fun)) {
          show_parse_warning(db,"lit nr %d in clause nr %d has nonatomic leadfun and hence ignored: ",litnr,clnr); 
#ifdef DEBUG          
          wg_mpool_print(db,fun); 
          printf("\n");
#endif          
          continue;
        }         
      } else {
        atom=lit;
      }      
#ifdef DEBUG      
      DPRINTF("atom isneg %d: ",isneg);
      wg_mpool_print(db,atom);
#endif      
      
      // parse an atom in the clause
      
      atomres=wg_parse_atom(db,mpool,atom,isneg,issimple,vardata); 
      if (atomres==NULL) {
        show_parse_error(db,"problem converting an atom to record");
        free(vardata);
        return NULL;        
      }
      if (issimple) {
        wg_convert_atom_fact_clause(db,atomres,isneg);
        resultlist=wg_mkpair(db,mpool,atomres,resultlist); 
        break;        
      } else {     
        ameta=0;        
        if (isneg) ameta=(ameta | ATOM_META_NEG);      
        setres=wg_set_rule_clause_atom_meta(db,record,litnr,ameta);                
        tmpres2=wg_encode_record(db,atomres);
        setres2=wg_set_rule_clause_atom(db,record,litnr,tmpres2);
        if (setres!=0 || setres2!=0) {
          wg_delete_record(db,atomres);
          free(vardata);
          return NULL; 
        }   
      }        
            
    } // end one clause processing loop
  } // end clause list loop 
    
  DPRINTF("\nwg_parse_rulelist ending\n");
  free(vardata);
  return resultlist;
}  


void* wg_parse_atom(void *db,void* mpool,void* term, int isneg, int issimple, char** vardata) {
  void* termpart; 
  void* ret;
  void* subterm;
  int termnr=0;
  int deeptcount=0;
  int vartcount=0;
  void* tmpres=NULL;
  gint tmpres2;
  gint setres;
  void* record;
  
  DPRINTF("\nwg_parse_atom starting with isneg %d atom\n",isneg);
#ifdef DEBUG  
  wg_mpool_print(db,term); 
  printf("\n");
#endif  
  // examine term
  
  termnr=0;
  deeptcount=0;
  vartcount=0;
  for(termpart=term;wg_ispair(db,termpart);termpart=wg_rest(db,termpart),termnr++) {
    subterm=wg_first(db,termpart);
    if (subterm!=NULL && wg_ispair(db,subterm)) deeptcount++;      
    else if (wg_atomtype(db,subterm)==WG_VARTYPE) vartcount++;
  }  
  
  // create data record
 
  record=wg_create_atom(db,termnr);   
  if (((int)record)==0) {
    return NULL;
  }  
  // fill data record and do recursive calls for subterms
  
  for(termpart=term,termnr=0;wg_ispair(db,termpart);termpart=wg_rest(db,termpart),termnr++) {
    term=wg_first(db,termpart);
    
#ifdef DEBUG    
    DPRINTF("\nterm nr  %d:",termnr);    
    wg_mpool_print(db,term); 
    printf("\n");
#endif    
    if (!wg_ispair(db,term)) {
      DPRINTF("term nr %d is primitive \n",termnr); 
      tmpres2=wg_parse_primitive(db,mpool,term,vardata);    
      if (tmpres2==WG_ILLEGAL) {
        wg_delete_record(db,record);
        return NULL;
      }  
    } else {
      DPRINTF("term nr %d is nonprimitive \n",termnr);
      tmpres=wg_parse_term(db,mpool,term,vardata);
      if (tmpres==NULL) {
        wg_delete_record(db,record);
        return NULL;
      } 
      tmpres2=wg_encode_record(db,tmpres);       
    }     
    if (tmpres2==WG_ILLEGAL) return NULL;
    setres=wg_set_atom_subterm(db,record,termnr,tmpres2);
    if (setres!=0) {
      wg_delete_record(db,record);
      return NULL; 
    }  
  }    
  ret=record;
  DPRINTF("\nwg_parse_atom ending\n");
  if (ret==NULL) DPRINTF("\nwg_parse_atom returns NULL\n");
  return ret;
}



void* wg_parse_term(void *db,void* mpool,void* term, char** vardata) {
  void* termpart; 
  void* ret;
  void* subterm;
  int termnr=0;
  int deeptcount=0;
  int vartcount=0;
  void* tmpres=NULL;
  gint tmpres2;
  gint setres;
  void* record;

#ifdef DEBUG  
  DPRINTF("\nwg_parse_term starting with ");
  wg_mpool_print(db,term); 
  printf("\n");
#endif
  
  // examine term
  
  termnr=0;
  deeptcount=0;
  vartcount=0;
  for(termpart=term;wg_ispair(db,termpart);termpart=wg_rest(db,termpart),termnr++) {
    subterm=wg_first(db,termpart);
    if (subterm!=NULL && wg_ispair(db,subterm)) deeptcount++;      
    else if (wg_atomtype(db,subterm)==WG_VARTYPE) vartcount++;
  }  
  
  // create data record
  record=wg_create_term(db,termnr);   
  if (((int)record)==0) {
    return NULL;
  } 
  //DPRINTF("\nwg_parse_term termnr %d \n",termnr);

  // fill data record and do recursive calls for subterms
  
  for(termpart=term,termnr=0;wg_ispair(db,termpart);termpart=wg_rest(db,termpart),termnr++) {
    term=wg_first(db,termpart);
#ifdef DEBUG    
    //DPRINTF("\nterm nr  %d:",termnr);    
    //wg_mpool_print(db,term); 
    //printf("\n");
#endif    
    if (!wg_ispair(db,term)) {
      DPRINTF("term nr %d is primitive \n",termnr); 
      tmpres2=wg_parse_primitive(db,mpool,term,vardata);    
      if (tmpres2==WG_ILLEGAL) { 
        wg_delete_record(db,record);
        return NULL;
      }  
    } else {     
      DPRINTF("term nr %d is nonprimitive \n",termnr);
      tmpres=wg_parse_term(db,mpool,term,vardata);
      if (tmpres==NULL) {
        wg_delete_record(db,record);
        return NULL;
      } 
      tmpres2=wg_encode_record(db,tmpres);  
    }       
    if (tmpres2==WG_ILLEGAL) return NULL;
    setres=wg_set_term_subterm(db,record,termnr,tmpres2);
    if (setres!=0) {
      wg_delete_record(db,record);
      return NULL; 
    }  
  }
    
  ret=record;
  DPRINTF("\nwg_parse_term ending \n");  
  if (ret==NULL) DPRINTF("\nwg_parse_term returns NULL\n");
  return ret;  
}

gint wg_parse_primitive(void *db,void* mpool,void* atomptr, char** vardata) {
  gint ret; 
  int type;
  char* str1;
  char* str2;
  int intdata;
  double doubledata;
  int i;

#ifdef DEBUG  
  DPRINTF("\nwg_parse_primitive starting with ");
  wg_mpool_print(db,atomptr); 
  printf("\n");  
#endif  
  
  if (atomptr==NULL) {
    ret=wg_encode_null(db,NULL);
  } else {    
    type=wg_atomtype(db,atomptr);
    str1=wg_atomstr1(db,atomptr);
    str2=wg_atomstr2(db,atomptr);
    switch (type) {
      case 0: ret=wg_encode_null(db,NULL); break; 
      case WG_NULLTYPE: ret=wg_encode_null(db,NULL); break;      
      case WG_INTTYPE: 
        intdata = atol(str1);
        if(errno!=ERANGE && errno!=EINVAL) {
          ret = wg_encode_int(db, intdata);
        } else {
          errno=0;
          ret=WG_ILLEGAL;         
        }
        break;         
      case WG_DOUBLETYPE: 
        doubledata = atof(str1);
        if(errno!=ERANGE && errno!=EINVAL) {
          ret = wg_encode_double(db, doubledata);
        } else {
          errno=0;
          ret=WG_ILLEGAL;         
        }
        break; 
      case WG_STRTYPE: 
        ret=wg_encode_str(db,str1,str2); 
        break; 
      case WG_XMLLITERALTYPE: 
        ret=wg_encode_xmlliteral(db,str1,str2); 
        break; 
      case WG_URITYPE: 
        ret=wg_encode_uri(db,str1,str2); 
        break; 
      //case WG_BLOBTYPE: 
      //  ret=wg_encode_blob(db,str1,str2); 
      //  break; 
      case WG_CHARTYPE: 
        ret=wg_encode_char(db,*str1); 
        break; 
      case WG_FIXPOINTTYPE:
        doubledata = atof(str1);
        if(errno!=ERANGE && errno!=EINVAL) {
          ret = wg_encode_fixpoint(db, doubledata);
        } else {
          errno=0;
          ret=WG_ILLEGAL;         
        }        
        break; 
      case WG_DATETYPE: 
        intdata=wg_strp_iso_date(db,str1);
        ret=wg_encode_date(db,intdata); 
        break; 
      case WG_TIMETYPE: 
        intdata=wg_strp_iso_time(db,str1); 
        ret=wg_encode_time(db,intdata);
        break; 
      case WG_ANONCONSTTYPE: 
        ret=wg_encode_anonconst(db,str1); 
        break; 
      case WG_VARTYPE:
        intdata=0;
        DPRINTF("starting WG_VARTYPE block\n");
        for(i=0;i<VARDATALEN;i++) {
          if (vardata[i]==NULL) {
            DPRINTF("no more vars to check\n");
            vardata[i]=str1;
            intdata=i;
            break;
          }  
          if (strcmp(vardata[i],str1)==0) {
            DPRINTF("found matching var\n");
            intdata=i;
            break;
          }            
        }  
        if (i>=VARDATALEN) {
          show_parse_warning(db,"too many variables in a clause: ignoring the clause");        
          errno=0;
          ret=WG_ILLEGAL;
          break;          
        }                   
        ret=wg_encode_var(db,intdata);
        DPRINTF("var %d encoded ok\n",intdata);       
        break; 
      default: 
        ret=wg_encode_null(db,NULL);
    }      
  }      
  DPRINTF("\nwg_parse_term ending with %d\n",ret);
  return ret;
}

/* -------------- parsing utilities ----------------------- */





/** Parse value from string, encode it for Wgandalf
 *  returns WG_ILLEGAL if value could not be parsed or
 *  encoded.
 *  Supports following data types:
 *  NULL - empty string
 *  variable - ?x where x is a numeric character
 *  int - plain integer
 *  double - floating point number in fixed decimal notation
 *  date - ISO8601 date
 *  time - ISO8601 time+fractions of second.
 *  uri - string starting with an URI prefix
 *  string - other strings
 *  Since leading whitespace generally makes type guesses fail,
 *  it invariably causes the data to be parsed as string.
 */
 
gint wg_parse_and_encode_otter_prim(void *db, char *buf) {
  int intdata;
  double doubledata;
  gint encoded = WG_ILLEGAL, res;
  char c = buf[0];

  if(c == 0) {
    /* empty fields become NULL-s */
    encoded = 0;
  }
  else if(c == '?' && buf[1] >= '0' && buf[1] <= '9') {
    /* try a variable */
    intdata = atol(buf+1);
    if(errno!=ERANGE && errno!=EINVAL) {
      encoded = wg_encode_var(db, intdata);
    } else {
      errno = 0;
    }
  }
  else if(c >= '0' && c <= '9') {
    /* This could be one of int, double, date or time */
    if((res = wg_strp_iso_date(db, buf)) >= 0) {
      encoded = wg_encode_date(db, res);
    } else if((res = wg_strp_iso_time(db, buf)) >= 0) {
      encoded = wg_encode_time(db, res);
    } else {
      /* Examine the field contents to distinguish between float
       * and int, then convert using atol()/atof(). sscanf() tends to
       * be too optimistic about the conversion, especially under Win32.
       */
      char *ptr = buf, *decptr = NULL;
      int decsep = 0;
      while(*ptr) {
        if(*ptr == OTTER_DECIMAL_SEPARATOR) {
          decsep++;
          decptr = ptr;
        }
        else if(*ptr < '0' || *ptr > '9') {
          /* Non-numeric. Mark this as an invalid number
           * by abusing the decimal separator count.
           */
          decsep = 2;
          break;
        }
        ptr++;
      }

      if(decsep==1) {
        char tmp = *decptr;
        *decptr = '.'; /* ignore locale, force conversion by plain atof() */
        doubledata = atof(buf);
        if(errno!=ERANGE && errno!=EINVAL) {
          encoded = wg_encode_double(db, doubledata);
        } else {
          errno = 0; /* Under Win32, successful calls don't do this? */
        }
        *decptr = tmp; /* conversion might have failed, restore string */
      } else if(!decsep) {
        intdata = atol(buf);
        if(errno!=ERANGE && errno!=EINVAL) {
          encoded = wg_encode_int(db, intdata);
        } else {
          errno = 0;
        }
      }
    }
  }
  else {
    /* Check for uri scheme */
    encoded = wg_parse_and_encode_otter_uri(db, buf);
  }
  
  if(encoded == WG_ILLEGAL) {
    /* All else failed. Try regular string. */
    encoded = wg_encode_str(db, buf, NULL);
  }
  return encoded;
}



/** Try parsing an URI from a string.
 *  Returns encoded WG_URITYPE field when successful
 *  Returns WG_ILLEGAL on error
 *
 *  XXX: this is a very naive implementation. Something more robust
 *  is needed.
 */
gint wg_parse_and_encode_otter_uri(void *db, char *buf) {
  gint encoded = WG_ILLEGAL;
  struct uri_scheme_info *next = uri_scheme_table_otter;

  /* Try matching to a known scheme */
  while(next->prefix) {
    if(!strncmp(buf, next->prefix, next->length)) {
      /* We have a matching URI scheme.
       * XXX: check this code for correct handling of prefix. */
      int urilen = strlen(buf);
      char *prefix = malloc(urilen + 1);
      char *dataptr;

      if(!prefix)
        break;
      strncpy(prefix, buf, urilen);

      dataptr = prefix + urilen;
      while(--dataptr >= prefix) {
        switch(*dataptr) {
          case ':':
          case '/':
          case '#':
            *(dataptr+1) = '\0';
            goto prefix_marked;
          default:
            break;
        }
      }
prefix_marked:
      encoded = wg_encode_uri(db, buf+((int)dataptr-(int)prefix+1), prefix);
      free(prefix);
      break;
    }
    next++;
  }
  return encoded;
}


/* ------------ errors ---------------- */


static int show_parse_error(void* db, char* format, ...) {
  va_list args;
  va_start (args, format);
  printf("*** Parser error: ");
  vprintf (format, args);
  va_end (args);
  return -1;
}

static int show_parse_warning(void* db, char* format, ...) {
  va_list args;
  va_start (args, format);
  printf("*** Parser warning: ");
  vprintf (format, args);
  va_end (args);
  return -1;
}



