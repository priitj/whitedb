/*

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol under a web server. Results are given in the json format.

You can also call it from the command line, passing a cgi-format
query string as a single argument, for example

dserve 'op=query&from=0&count=5'

dserve does not require additional libraries except wgdb. Compile by doing
gcc dserve.c -o dserve -O2 -lwgdb

Use and modify the code for creating your own data servers for WhiteDB.

See http://whitedb.org/tools.html for a detailed manual.

Copyright (c) 2013, Tanel Tammet

This software is under MIT licence:

Permission is hereby granted, free of charge, to any person obtaining 
a copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and/or sell copies of the Software, and to permit persons to whom the 
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included 
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE 
OR OTHER DEALINGS IN THE SOFTWARE.

NB! Observe that the current file is under a different licence than the
WhiteDB library: the latter is by default under GPLv3.

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h> // for alarm and termination signal handling
#include <unistd.h> // for alarm
#include <ctype.h> // for isxdigit and isdigit

/* =============== configuration macros =================== */

#define TIMEOUT_SECONDS 2

#define MAXQUERYLEN 1000 // query string length limit
#define MAXPARAMS 100 // max number of cgi params in query
#define MAXCOUNT 100 // max number of result records
#define INITIAL_MALLOC 10000 // initially malloced result size
#define MAX_MALLOC 100000000 // max malloced result size
#define DEFAULT_DATABASE "1000" 

#define TIMEOUT_ERR "timeout"
#define INTERNAL_ERR "internal error"
#define NOQUERY_ERR "no query"
#define LONGQUERY_ERR "too long query"
#define MALFQUERY_ERR "malformed query"

#define NO_OP_ERR "no op given: use op=opname for opname in search"
#define UNKNOWN_OP_ERR "urecognized op: use op=search"
#define NO_FIELD_ERR "no field given"
#define NO_VALUE_ERR "no value given"
#define DB_ATTACH_ERR "no database found: use db=name for a concrete database"
#define FIELD_ERR "unrecognized field: use an integer starting from 0"
#define COND_ERR "unrecognized compare: use equal, not_equal, lessthan, greater, ltequal or gtequal"
#define INTYPE_ERR "unrecognized type: use null, int, double, str, char or record "
#define INVALUE_ERR "did not find a value to use for comparison"
#define INVALUE_TYPE_ERR "value does not match type"
#define LOCK_ERR "database locked"
#define LOCK_RELEASE_ERR "releasing read lock failed: database may be in deadlock"
#define MALLOC_ERR "cannot allocate enough memory for result string"
#define QUERY_ERR "query creation failed"
#define DECODE_ERR "field data decoding failed"

#define JS_NULL "[]"
#define JS_CHAR_COEFF 6
#define JS_ALLOC_ERR "\"<failed_to_allocate>\""
#define JS_TYPE_ERR "\"<not_supported_type>\""
#define JS_DEEP_ERR "\"<rec_id %d>\""

#define CONTENT_TYPE "content-type: text/plain\n\n"

/* =============== protos =================== */

void timeout_handler(int signal);
void termination_handler(int signal);
char* search(char* database, char* inparams[], char* invalues[], int count);
int parse_query(char* query, int ql, char* params[], char* values[]);
char* urldecode(char *indst, char *src);

int isint(char* s);
int isdbl(char* s);

char* print_js_value(void *db, wg_int enc, char *buf, int buflen, int depth, int strenc); 
static void print_string(char* buf, int buflen, char* strdata, int strenc);
static void print_append(char** buf, char* str, int l);

void err_clear_detach_halt(void* db, wg_int lock_id, char* errstr);
void errhalt(char* str);


/* =============== globals =================== */

// global vars are used only for enabling signal/error handlers
// to free the lock and detach from the database:
// set/cleared after taking/releasing lock, attaching/detaching database

void* global_db=NULL; // NULL iff not attached
wg_int global_lock_id=0; // 0 iff not locked


/* =============== main =================== */


int main(int argc, char **argv) {
  int i=0;
  char* inquery=NULL;
  char query[MAXQUERYLEN];  
  int pcount=0;
  int ql,found;  
  char* res;
  char* database=DEFAULT_DATABASE;
  char* op=NULL;
  char* params[MAXPARAMS];
  char* values[MAXPARAMS];
  
  // Set up timeout signal and abnormal termination handlers:
  // the termination handler clears the read lock and detaches database
  signal(SIGSEGV,termination_handler);
  signal(SIGINT,termination_handler);
  signal(SIGFPE,termination_handler);
  signal(SIGABRT,termination_handler);
  signal(SIGTERM,termination_handler);
  signal(SIGALRM,timeout_handler);
  alarm(TIMEOUT_SECONDS);
  
  // for debugging print content-type immediately
  printf("content-type: text/plain\n\n");
  
  // get the cgi query: passed by server or given on the command line
  inquery=getenv("QUERY_STRING");
  if (inquery==NULL && argc>1) inquery=argv[1];
  // or use your own query string for testing
  // inquery="db=1000&op=search&field=1&value=23640&compare=equal&type=record&from=0&count=3";
  
  // parse the query 
  
  if (inquery==NULL || inquery[0]=='\0') errhalt(NOQUERY_ERR);
  ql=strlen(inquery);  
  if (ql>MAXQUERYLEN) errhalt(LONGQUERY_ERR); 
  strcpy((char*)query,inquery);
  printf("query: %s\n",query);
    
  pcount=parse_query(query,ql,params,values);    
  if (pcount<=0) errhalt(MALFQUERY_ERR);
  
  for(i=0;i<pcount;i++) {
    printf("param %s val %s\n",params[i],values[i]);
  }  
  
  // query is now successfully parsed: find the database
  
  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],"db",MAXQUERYLEN)==0) {
      if ((values[i]!=NULL) && (values[i][0]!='\0')) {
        database=values[i];
        break; 
      } 
    }  
  }  
    
  //find the operation and dispatch
  
  found=0;
  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],"op",MAXQUERYLEN)==0) {
      if (strncmp(values[i],"search",MAXQUERYLEN)==0) {
        found=1;
        op=values[i]; // for debugging later
        res=search(database,params,values,pcount);
        break;
      } else {
        errhalt(UNKNOWN_OP_ERR);
      }        
    }  
  }
  if (!found) errhalt(NO_OP_ERR);
  // print the final result
  printf("%s",CONTENT_TYPE);
  printf("dbase %s op %s\n",database,op); // debug the query
  printf("%s\n",res);
  return 0;  
}


/* ============== query processing ===================== */


/* search from the database */  
  
char* search(char* database, char* inparams[], char* invalues[], int incount) {
  int i,rcount,gcount,itmp;
  wg_int cmp=0,type=0,val=0;
  char* fields[MAXPARAMS];
  char* values[MAXPARAMS];
  char* compares[MAXPARAMS];
  char* types[MAXPARAMS];
  int fcount=0, vcount=0, ccount=0, tcount=0;
  int from=0;
  int count=MAXCOUNT;
  void* db=NULL;
  void* rec;
  char* res;
  int res_size=INITIAL_MALLOC;
  wg_query *wgquery;  
  wg_query_arg wgargs[MAXPARAMS];
  wg_int lock_id=0;
  int nosearch=0;
  
  // -------check and parse cgi parameters, attach database ------------
  
  // set params to defaults
  for(i=0;i<MAXPARAMS;i++) {
    fields[i]=NULL; values[i]=NULL; compares[i]=NULL; types[i]=NULL;
  }
  // find search parameters
  for(i=0;i<incount;i++) {
    if (strncmp(inparams[i],"field",MAXQUERYLEN)==0) {
      fields[fcount++]=invalues[i];       
    } else if (strncmp(inparams[i],"value",MAXQUERYLEN)==0) {
      values[vcount++]=invalues[i];
    } else if (strncmp(inparams[i],"compare",MAXQUERYLEN)==0) {
      compares[ccount++]=invalues[i];
    } else if (strncmp(inparams[i],"type",MAXQUERYLEN)==0) {
      types[tcount++]=invalues[i];
    } else if (strncmp(inparams[i],"from",MAXQUERYLEN)==0) {      
      from=atoi(invalues[i]);
    } else if (strncmp(inparams[i],"count",MAXQUERYLEN)==0) {      
      count=atoi(invalues[i]);
    }
  }
  // check search parameters
  if (!fcount) {
    if (vcount || ccount || tcount) errhalt(NO_FIELD_ERR);
    else nosearch=1;
  }    
  // attach to database
  db = wg_attach_existing_database(database);
  if (!db) errhalt(DB_ATTACH_ERR);
  res=malloc(res_size);
  if (!res) { 
    err_clear_detach_halt(db,0,MALLOC_ERR);
  } 
  // database attached OK
  
  if (nosearch) {
    // ------- special case without search: just output records ---    
    gcount=0;
    rcount=0;
    lock_id = wg_start_read(db); // get read lock
    global_lock_id=lock_id; // only for handling errors
    if (!lock_id) err_clear_detach_halt(db,0,LOCK_ERR);
    rec=wg_get_first_record(db);
    do {    
      if (rcount>=from) {
        gcount++;
        wg_print_record(db, rec);
        printf("\n"); 
      }
      if (gcount>=count) break;
      rec=wg_get_next_record(db,rec);
      rcount++;
    } while(rec!=NULL);       
    if (!wg_end_read(db, lock_id)) {  // release read lock
      err_clear_detach_halt(db,lock_id,LOCK_RELEASE_ERR);
    }
    global_lock_id=0; // only for handling errors
    wg_detach_database(db); 
    global_db=NULL; // only for handling errors
    printf("rcount %d gcount %d\n", rcount, gcount);
    return "NOSEARCH OK";
  }  
  
  // ------------ normal search case: ---------
  
  // create a query list datastructure
  
  for(i=0;i<fcount;i++) {   
    // field num    
    printf("i: %d fields[i]: %s %d\n",i,values[i],atoi(fields[i]));
    if (!isint(fields[i])) err_clear_detach_halt(db,0,NO_FIELD_ERR);
    itmp=atoi(fields[i]);
    printf("i: %d fields[i]: %s %d\n",i,values[i],itmp);
    wgargs[i].column = itmp;
    
    // comparison op: default equal
    if (compares[i]==NULL || compares[i]=='\0') cmp=WG_COND_EQUAL;
    else if (!strcmp(compares[i],"equal"))  cmp=WG_COND_EQUAL; 
    else if (!strcmp(compares[i],"not_equal"))  cmp=WG_COND_NOT_EQUAL; 
    else if (!strcmp(compares[i],"lessthan"))  cmp=WG_COND_LESSTHAN; 
    else if (!strcmp(compares[i],"greater"))  cmp=WG_COND_GREATER; 
    else if (!strcmp(compares[i],"ltequal"))  cmp=WG_COND_LTEQUAL;   
    else if (!strcmp(compares[i],"gtequal"))  cmp=WG_COND_GTEQUAL; 
    else err_clear_detach_halt(db,0,COND_ERR);    
    wgargs[i].cond = cmp;  
    
    // valuetype: default guess from value later
    if (types[i]==NULL || types[i]=='\0') type=0;   
    else if (!strcmp(types[i],"null"))  type=WG_NULLTYPE; 
    else if (!strcmp(types[i],"int"))  type=WG_INTTYPE; 
    else if (!strcmp(types[i],"record"))  type=WG_RECORDTYPE;
    else if (!strcmp(types[i],"double"))  type=WG_DOUBLETYPE; 
    else if (!strcmp(types[i],"str"))  type=WG_STRTYPE; 
    else if (!strcmp(types[i],"char"))  type=WG_CHARTYPE;   
    else err_clear_detach_halt(db,0,INTYPE_ERR);
    
    // encode value to compare with    
    if (values[i]==NULL) err_clear_detach_halt(db,0,INVALUE_ERR);
    if (type==WG_NULLTYPE) val=wg_encode_query_param_null(db,NULL);
    else if (type==WG_INTTYPE) {
      if (!isint(values[i])) err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);      
      val=wg_encode_query_param_int(db,atoi(values[i]));
    } else if (type==WG_RECORDTYPE) {
      if (!isint(values[i])) err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);      
      val=(wg_int)atoi(values[i]);
    } else if (type==WG_DOUBLETYPE) {
      if (!isdbl(values[i])) err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);
      val=wg_encode_query_param_double(db,strtod(values[i],NULL));
    } else if (type==WG_STRTYPE) {
      val=wg_encode_query_param_str(db,values[i],NULL);
    } else if (type==WG_CHARTYPE) {
      val=wg_encode_query_param_char(db,values[i][0]);
    } else if (type==0 && isint(values[i])) {
      val=wg_encode_query_param_int(db,atoi(values[i]));
    } else if (type==0 && isdbl(values[i])) {
      val=wg_encode_query_param_double(db,strtod(values[i],NULL));
    } else if (type==0) {
      val=wg_encode_query_param_str(db,values[i],NULL);
    } else {
      err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);
    }
    wgargs[i].value = val;
  }   
  
  // make the query structure: read-lock the database before!
  
  lock_id = wg_start_read(db); // get read lock
  global_lock_id=lock_id; // only for handling errors
  if (!lock_id) err_clear_detach_halt(db,lock_id,LOCK_ERR);
  wgquery = wg_make_query(db, NULL, 0, wgargs, i);
  if (!wgquery) err_clear_detach_halt(db,lock_id,QUERY_ERR);
  
  // actually perform the query
  
  rcount=0;
  gcount=0;
  while((rec = wg_fetch(db, wgquery))) {
    if (rcount>=from) {
      gcount++;
      wg_print_record(db, rec);
      printf("\n");      
    }  
    rcount++;
    if (gcount>=count) break;    
  }  
  
  // free query datastructure, release lock, detach
  
  for(i=0;i<fcount;i++) wg_free_query_param(db, wgargs[i].value);
  wg_free_query(db,wgquery); 
  if (!wg_end_read(db, lock_id)) {  // release read lock
    err_clear_detach_halt(db,lock_id,LOCK_RELEASE_ERR);
  }
  global_lock_id=0; // only for handling errors
  wg_detach_database(db); 
  global_db=NULL; // only for handling errors
  printf("rcount %d gcount %d\n", rcount, gcount);
  return "OK";
}



/* *******************  cgi query parsing  ******************** */


/* query parser: split by & and =, urldecode param and value
   return param count or -1 for error 
*/

int parse_query(char* query, int ql, char* params[], char* values[]) {
  int count=0;
  int i,pi,vi;
  
  for(i=0;i<ql;i++) {
    pi=i;
    for(;i<ql;i++) {
      if (query[i]=='=') { query[i]=0; break; }      
    }
    i++;
    if (i>=ql) return -1;
    vi=i;    
    for(;i<ql;i++) {
      if (query[i]=='&') { query[i]=0; break; }  
    }
    if (count>=MAXPARAMS) return -1;    
    params[count]=urldecode(query+pi,query+pi);
    values[count]=urldecode(query+vi,query+vi);
    count++;
  }
  return count;
}    

/* urldecode used by query parser 
*/

char* urldecode(char *indst, char *src) {
  char a, b;
  char* endptr;
  char* dst;
  dst=indst;
  if (src==NULL || src[0]=='\0') return indst;
  endptr=src+strlen(src);
  while (*src) {
    if ((*src == '%') && (src+2<endptr) &&
        ((a = src[1]) && (b = src[2])) &&
        (isxdigit(a) && isxdigit(b))) {
      if (a >= 'a') a -= 'A'-'a';
      if (a >= 'A') a -= ('A' - 10);
      else a -= '0';
      if (b >= 'a') b -= 'A'-'a';
      if (b >= 'A') b -= ('A' - 10);
      else b -= '0';
      *dst++ = 16*a+b;
      src+=3;
    } else {
      *dst++ = *src++;
    }
  }
  *dst++ = '\0';
  return indst;
}


/* **************** guess string datatype ***************** */

/* return 1 iff s contains numerals only
*/

int isint(char* s) {
  if (s==NULL) return 0;
  while(*s!='\0') {
    if (!isdigit(*s)) return 0;
    s++;
  }
  return 1;
}  
  
/* return 1 iff s contains numerals plus single optional period only
*/


int isdbl(char* s) {
  int c=0;
  if (s==NULL) return 0;
  while(*s!='\0') {
    if (!isdigit(*s)) {
      if (*s=='.') c++;
      else return 0;
      if (c>1) return 0;
    }
    s++;
  }
  return 1;
}   

/* ****************  json printing **************** */


/** Print a record into a buffer, handling records recursively
 *  expects buflen to be at least 2.
 */



void print_js_record(void *db, wg_int* rec, char *buf, int buflen, int depth, int strenc) {
  char *strbuf, *valbuf;
  int i,strbufsize,vallen;
  wg_int enc, len;
#ifdef USE_CHILD_DB
  void *parent;
#endif
  strbufsize=100; // initial size to locally allocate here
  if (rec==NULL) {
    snprintf(buf, buflen, "[]\n");
    return;
  }
  if (buflen<2) return;
  *buf++ = '[';
  buflen--; // remaining part of buf
#ifdef USE_CHILD_DB
  parent = wg_get_rec_owner(db, rec);
#endif
  strbuf = malloc(strbufsize); // local chunk for this record elems
  if (strbuf) {
    len = wg_get_record_len(db, rec);
    for(i=0; i<len; i++) {
      // Use a fresh buffer for the value. This way we can
      //easily count how many bytes printing the value added.
      enc = wg_get_field(db, rec, i);
#ifdef USE_CHILD_DB
      if(parent != db)
        enc = wg_translate_hdroffset(db, parent, enc);
#endif
      valbuf=print_js_value(db, enc, strbuf, strbufsize, depth, strenc);
      if (valbuf==NULL) { 
        // no new mallocs done in print_js_value
        vallen = strlen(strbuf);
      } else {  
        // new malloc done and returned in print_js_value
        vallen = strlen(valbuf);
      }
      //Check if the value fits comfortably, including
      //leading comma and trailing \0
      if (buflen<vallen+2)  break;
      if (i) {
        *buf++ = ',';
        buflen--;
      }
      if (valbuf==NULL) {
        strncpy(buf, strbuf, buflen);
      } else {  
        
      }
      buflen -= vallen;
      buf += vallen;
      if (buflen<2) break;
    }
    free(strbuf);
  }
  if (buflen>1) *buf++ = ']';
  *buf = '\0';
}


/** Print a single encoded value:
  The value is written into a character buffer.

  depth: limit on records nested via record pointers

  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe

*/


char* print_js_value(void *db, wg_int enc, char *buf, int buflen, int depth, int strenc) {
  wg_int ptrdata;
  int intdata,len,strl;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[80]; // tmp area for dates

  buflen--; /* snprintf adds '\0' */
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      snprintf(buf, buflen, JS_NULL);
      break;
    case WG_RECORDTYPE:
      ptrdata = (wg_int) wg_decode_record(db, enc);
      snprintf(buf, buflen, "<rec_id %d>", (int)enc);
      //len = strlen(buf);
      //if(buflen - len > 1)
      //  snprint_record(db, (wg_int*)ptrdata, buf+len, buflen-len);
      //break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      snprintf(buf, buflen, "%d", intdata);
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_FIXPOINTTYPE:
      doubledata = wg_decode_fixpoint(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      strl=strlen(strdata);
      if (JS_CHAR_COEFF*strl>=buflen) buf=malloc(JS_CHAR_COEFF*strl+10);
      if (!buf) snprintf(buf, buflen, JS_ALLOC_ERR);
      print_string(buf,buflen,strdata,strenc);
      //else snprintf(buf, buflen, "\"%s\"", strdata);        
      break;
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      if (exdata==NULL)
        snprintf(buf, buflen, "\"%s\"", strdata);
      else
        snprintf(buf, buflen, "\"%s:%s\"", exdata, strdata);
      break;
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      snprintf(buf, buflen, "\"%s:%s\"", exdata, strdata);
      break;
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      snprintf(buf, buflen, "\"%c\"", (char) intdata);
      break;
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      snprintf(buf, buflen, "\"%s\"",strbuf);
      break;
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);        
      snprintf(buf, buflen, "\"%s\"",strbuf+11);
      break;
    case WG_VARTYPE:
      intdata = wg_decode_var(db, enc);
      snprintf(buf, buflen, "\"?%d\"", intdata);
      break;  
    case WG_ANONCONSTTYPE:
      strdata = wg_decode_anonconst(db, enc);
      snprintf(buf, buflen, "\"!%s\"",strdata);
      break;
    default:
      snprintf(buf, buflen, JS_TYPE_ERR);
      break;
  }
  return buf;
}


/* Print string with several encoding/escaping options:
  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe

For standard json tools see:

json rfc http://www.ietf.org/rfc/rfc4627.txt
ccan json tool http://git.ozlabs.org/?p=ccan;a=tree;f=ccan/json
Jansson json tool https://jansson.readthedocs.org/en/latest/

*/

static void print_string(char* buf, int buflen, char* strdata, int strenc) {
  unsigned char c;
  char *bptr,*sptr;
  char *hex_chars="0123456789abcdef";
  int i;
  bptr=buf;
  sptr=strdata;
  *bptr++='"';
  if (sptr==NULL) {
    *bptr++='"';
    *bptr='\0'; 
    return;  
  }
  if (!strenc) {
    // nothing is escaped at all
    for(i=0;i<buflen-1;i++) {
      c=*sptr++;
      if (c=='\0') { 
        *bptr++='"';
        *bptr='\0';  
        return; 
      } else {
        *bptr++=c;
      } 
    }  
  } else if (strenc==1) {
    // non-ascii chars and % and " urlencoded
    for(i=0;i<buflen-1;i++) {
      c=*sptr++;
      if (c=='\0') { 
        *bptr++='"';
        *bptr='\0';  
        return; 
      } else if (c < ' ' || c=='%' || c=='"' || (int)c>126) {
        *bptr++='%';
        *bptr++=hex_chars[c >> 4];
        *bptr++=hex_chars[c & 0xf];
      } else {
        *bptr++=c;
      } 
    }    
  } else {
    // json encoding; chars before ' ' are are escaped with \u00
    sptr=strdata;
    bptr=buf;
    for(i=0;i<buflen-1;i++) {
      c=*sptr++;
      switch(c) {
      case '\0':
        *bptr++='"';
        *bptr='\0'; 
        return;  
      case '\b':
      case '\n':
      case '\r':
      case '\t':
      case '\f':
      case '"':
      case '\\':
      case '/':
        if(c == '\b') print_append(&bptr, "\\b", 2);
        else if(c == '\n') print_append(&bptr, "\\n", 2);
        else if(c == '\r') print_append(&bptr, "\\r", 2);
        else if(c == '\t') print_append(&bptr, "\\t", 2);
        else if(c == '\f') print_append(&bptr, "\\f", 2);
        else if(c == '"') print_append(&bptr, "\\\"", 2);
        else if(c == '\\') print_append(&bptr, "\\\\", 2);
        else if(c == '/') print_append(&bptr, "\\/", 2);
        break;      
      default:
        if(c < ' ') {
          print_append(&bptr, "\\u00", 4);
          *bptr++=hex_chars[c >> 4];
          *bptr++=hex_chars[c & 0xf];
        } else {
          *bptr++=c;
        } 
      }  
    }
  } 
}

static void print_append(char** buf, char* str, int l) {
  int i;
  
  for(i=0;i<l;i++) *(*buf)++=*str++;
}


/*

void json_print_str(void *db, wg_int encstr, char *buf, int buflen, int style) {
  char* decstr;
  char* ptr;
  
  decstr=wg_decode_str(db,encstr);
  ptr=decstr;
  *buf--='\"';  
  while(*ptr!='\0') {
    if ((int)*ptr<128) *buf--=*ptr++;
    else if (style==0) {
      *buf--='\\';
      *buf--='u';
      *buf--='0';
      snprintf(buf, buflen--, "\"");
    }
  }
  snprintf(buf, buflen--, "\"");
  *buf--='\"';
  return buf;
}  

const char *json_number_chars = "0123456789.+-eE";
const char *json_hex_chars = "0123456789abcdefABCDEF";

int json_escape_str(struct printbuf *pb, char *str, int len) {
  int pos = 0, start_offset = 0;
  unsigned char c;
  while (len--) {
    c = str[pos];
    switch(c) {
    case '\b':
    case '\n':
    case '\r':
    case '\t':
    case '\f':
    case '"':
    case '\\':
    case '/':
      if(pos - start_offset > 0)
        printbuf_memappend(pb, str + start_offset, pos - start_offset);
      if(c == '\b') printbuf_memappend(pb, "\\b", 2);
      else if(c == '\n') printbuf_memappend(pb, "\\n", 2);
      else if(c == '\r') printbuf_memappend(pb, "\\r", 2);
      else if(c == '\t') printbuf_memappend(pb, "\\t", 2);
      else if(c == '\f') printbuf_memappend(pb, "\\f", 2);
      else if(c == '"') printbuf_memappend(pb, "\\\"", 2);
      else if(c == '\\') printbuf_memappend(pb, "\\\\", 2);
      else if(c == '/') printbuf_memappend(pb, "\\/", 2);
      start_offset = ++pos;
      break;
    default:
      if(c < ' ') {
        if(pos - start_offset > 0)
         printbuf_memappend(pb, str + start_offset, pos - start_offset);
        sprintbuf(pb, "\\u00%c%c",
                 json_hex_chars[c >> 4],
                 json_hex_chars[c & 0xf]);
        start_offset = ++pos;
      } else pos++;
    }
  }
  if(pos - start_offset > 0)
    printbuf_memappend(pb, str + start_offset, pos - start_offset);
  return 0;
}

*/

/* *******************  errors  ******************** */

/* called in case of internal errors by the signal catcher:
   it is crucial that the locks are released and db detached */

void termination_handler(int signal) {
  err_clear_detach_halt(global_db,global_lock_id,INTERNAL_ERR);
}

/* called in case of timeout by the signal catcher:
   it is crucial that the locks are released and db detached */

void timeout_handler(int signal) {
  err_clear_detach_halt(global_db,global_lock_id,TIMEOUT_ERR);
}

/* normal termination call: free locks, detach, call errprint and halt */

void err_clear_detach_halt(void* db, wg_int lock_id, char* errstr) {      
  if (lock_id) {
    wg_end_read(db, lock_id);
    global_lock_id=0; // only for handling errors
  }  
  if (db!=NULL) wg_detach_database(db);
  global_db=NULL; // only for handling errors
  errhalt(errstr);
}  

/* error output and immediate halt
*/

void errhalt(char* str) {
  printf("%s\n",str);
  exit(0);
}

