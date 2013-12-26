/*

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http. Results are given in the json format.

You can also call it from the command line, passing a cgi-format,
urlencoded query string as a single argument, for example

dserve 'op=search&from=0&count=5'

dserve does not require additional libraries except wgdb. Compile by doing
gcc dserve.c -o dserve -O2 -lwgdb

dserve uses readlocks, does not use writelocks.

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
#include <ctype.h> //
#if _MSC_VER   // no alarm on windows
#else
#include <unistd.h> // for alarm
#endif

/* =============== configuration macros =================== */

#define DEFAULT_DATABASE "1000"

#define TIMEOUT_SECONDS 2

#define JSON_CONTENT_TYPE "content-type: application/json\r\n\r\n"
#define CSV_CONTENT_TYPE "content-type: text/csv\r\n\r\n"
#define CONTENT_LENGTH "content-length: %d\r\n"

#define MAXQUERYLEN 2000 // query string length limit
#define MAXPARAMS 100 // max number of cgi params in query
#define MAXCOUNT 10000 // max number of result records
#define MAXIDS 1000 // max number of rec id-s in recids query

#define INITIAL_MALLOC 1000 // initially malloced result size
#define MAX_MALLOC 100000000 // max malloced result size
#define MIN_STRLEN 100 // fixed-len obj strlen, add this to strlen for print-space need
#define STRLEN_FACTOR 6 // might need 6*strlen for json encoding
#define DOUBLE_FORMAT "%g" // snprintf format for printing double
#define JS_NULL "[]"
#define CSV_SEPARATOR ',' // must be a single char
#define MAX_DEPTH_DEFAULT 100 // can be increased
#define MAX_DEPTH_HARD 10000 // too deep rec nesting will cause stack overflow in the printer

#define TIMEOUT_ERR "timeout"
#define INTERNAL_ERR "internal error"
#define NOQUERY_ERR "no query"
#define LONGQUERY_ERR "too long query"
#define MALFQUERY_ERR "malformed query"

#define UNKNOWN_PARAM_ERR "unrecognized parameter: %s"
#define UNKNOWN_PARAM_VALUE_ERR "unrecognized value %s for parameter %s"
#define NO_OP_ERR "no op given: use op=opname for opname in search"
#define UNKNOWN_OP_ERR "unrecognized op: use op=search or op=recids"
#define NO_FIELD_ERR "no field given"
#define NO_VALUE_ERR "no value given"
#define DB_PARAM_ERR "use db=name with a numeric name for a concrete database"
#define DB_ATTACH_ERR "no database found: use db=name with a numeric name for a concrete database"
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

#define JS_TYPE_ERR "\"\""  // currently this will be shown also for empty string

#if _MSC_VER   // microsoft compatibility
#define snprintf _snprintf
#endif

/* =============== protos =================== */

void timeout_handler(int signal);
void termination_handler(int signal);

void print_final(char* str,int format);

char* search(char* database, char* inparams[], char* invalues[], int count, int* hformat);
char* recids(char* database, char* inparams[], char* invalues[], int incount, int* hformat);

static wg_int encode_incomp(void* db, char* incomp);
static wg_int encode_intype(void* db, char* intype);
static wg_int encode_invalue(void* db, char* invalue, wg_int type);
static int isint(char* s);
static int isdbl(char* s);
static int parse_query(char* query, int ql, char* params[], char* values[]);
static char* urldecode(char *indst, char *src);

int sprint_record(void *db, wg_int *rec, char **buf, int *bufsize, char **bptr,
                   int format, int showid, int depth,  int maxdepth, int strenc);
char* sprint_value(void *db, wg_int enc, char **buf, int *bufsize, char **bptr,
                   int format, int showid, int depth, int maxdepth, int strenc);
int sprint_string(char* bptr, int limit, char* strdata, int strenc);
int sprint_blob(char* bptr, int limit, char* strdata, int strenc);
int sprint_append(char** buf, char* str, int l);

static char* str_new(int len);
static int str_guarantee_space(char** stradr, int* strlenadr, char** ptr, int needed);

void err_clear_detach_halt(void* db, wg_int lock_id, char* errstr);
void errhalt(char* str);


/* =============== globals =================== */

// global vars are used only for enabling signal/error handlers
// to free the lock and detach from the database:
// set/cleared after taking/releasing lock, attaching/detaching database
// global_format used in error handler to select content-type header

void* global_db=NULL; // NULL iff not attached
wg_int global_lock_id=0; // 0 iff not locked
int global_format=1; // 1 json, 0 csv

/* =============== main =================== */


int main(int argc, char **argv) {
  int i=0;
  char* inquery=NULL;
  char query[MAXQUERYLEN];
  int pcount=0;
  int ql,found;
  char* res=NULL;
  char* database=DEFAULT_DATABASE;
  char* params[MAXPARAMS];
  char* values[MAXPARAMS];
  int hformat=1; // for header 0: csv, 1: json: reset later after reading params

  // Set up timeout signal and abnormal termination handlers:
  // the termination handler clears the read lock and detaches database.
  // This may fail, however, for some lock strategies and in case
  // nontrivial operations are taken in the handler.
#if _MSC_VER  // no signals on windows
#else
  signal(SIGSEGV,termination_handler);
  signal(SIGINT,termination_handler);
  signal(SIGFPE,termination_handler);
  signal(SIGABRT,termination_handler);
  signal(SIGTERM,termination_handler);
  signal(SIGALRM,timeout_handler);
  alarm(TIMEOUT_SECONDS);
#endif

  // for debugging print the plain content-type immediately
  // printf("content-type: text/plain\r\n");

  // get the cgi query: passed by server or given on the command line
  inquery=getenv("QUERY_STRING");
  if (inquery==NULL && argc>1) inquery=argv[1];
  // or use your own query string for testing a la
  // inquery="db=1000&op=search&field=1&value=2&compare=equal&type=record&from=0&count=3";

  // parse the query

  if (inquery==NULL || inquery[0]=='\0') errhalt(NOQUERY_ERR);
  ql=strlen(inquery);
  if (ql>MAXQUERYLEN) errhalt(LONGQUERY_ERR);
  strcpy((char*)query,inquery);
  //printf("query: %s\n",query);

  pcount=parse_query(query,ql,params,values);
  if (pcount<=0) errhalt(MALFQUERY_ERR);

  //for(i=0;i<pcount;i++) {
  //  printf("param %s val %s\n",params[i],values[i]);
  //}

  // query is now successfully parsed: find the database

  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],"db",MAXQUERYLEN)==0) {
      if ((values[i]!=NULL) && (values[i][0]!='\0')) {
        if (atoi(values[i])==0 && !(values[i][0]=='0' && values[i][1]=='\0')) {
          errhalt(DB_PARAM_ERR);
        }
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
        res=search(database,params,values,pcount,&hformat);
        // here the locks should be freed and database detached
        break;
      } else if (strncmp(values[i],"recids",MAXQUERYLEN)==0) {
        found=1;
        res=recids(database,params,values,pcount,&hformat);
        // here the locks should be freed and database detached
        break;
      } else {
        errhalt(UNKNOWN_OP_ERR);
      }
    }
  }
  if (!found) errhalt(NO_OP_ERR);
  print_final(res,hformat);
  if (res!=NULL) free(res); // not really necessary and wastes time: process exits
  return 0;
}

void print_final(char* str,int format) {
  if (str!=NULL) {
    printf(CONTENT_LENGTH,strlen(str)+1); //1 added for puts newline
    if (format) printf(JSON_CONTENT_TYPE);
    else printf(CSV_CONTENT_TYPE);
    puts(str);
  } else {
    if (format) printf(JSON_CONTENT_TYPE);
    else printf(CSV_CONTENT_TYPE);
  }
}

/* ============== query processing ===================== */


/* first possible query operation: search from the database */

char* search(char* database, char* inparams[], char* invalues[], int incount, int* hformat) {
  int i,rcount,gcount,itmp;
  wg_int type=0;
  char* fields[MAXPARAMS];
  char* values[MAXPARAMS];
  char* compares[MAXPARAMS];
  char* types[MAXPARAMS];
  int fcount=0, vcount=0, ccount=0, tcount=0;
  int from=0;
  int count=MAXCOUNT;
  void* db=NULL; // actual database pointer
  void* rec;
  char* res;
  int res_size=INITIAL_MALLOC;
  wg_query *wgquery;  // query datastructure built later
  wg_query_arg wgargs[MAXPARAMS];
  wg_int lock_id=0;  // non-0 iff lock set
  int nosearch=0; // 1 iff no search parameters given, use scan
  int maxdepth=MAX_DEPTH_DEFAULT; // rec depth limit for printer
  int showid=0; // add record id as first extra elem: 0: no, 1: yes
  int format=1; // 0: csv, 1:json
  int escape=2; // string special chars escaping:  0: just ", 1: urlencode, 2: json, 3: csv
  char errbuf[200]; // used for building variable-content input param error strings only
  char* strbuffer; // main result string buffer start (malloced later)
  int strbufferlen; // main result string buffer length
  char* strbufferptr; // current output location ptr in the main result string buffer

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
    } else if (strncmp(inparams[i],"depth",MAXQUERYLEN)==0) {
      maxdepth=atoi(invalues[i]);
    } else if (strncmp(inparams[i],"showid",MAXQUERYLEN)==0) {
      if (strncmp(invalues[i],"yes",MAXQUERYLEN)==0) showid=1;
      else if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) showid=0;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        errhalt(errbuf);
      }
    } else if (strncmp(inparams[i],"format",MAXQUERYLEN)==0) {
      if (strncmp(invalues[i],"csv",MAXQUERYLEN)==0) format=0;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) format=1;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        errhalt(errbuf);
      }
    } else if (strncmp(inparams[i],"escape",MAXQUERYLEN)==0) {
      if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) escape=0;
      else if (strncmp(invalues[i],"url",MAXQUERYLEN)==0) escape=1;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) escape=2;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        errhalt(errbuf);
      }
    } else if (strncmp(inparams[i],"db",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else if (strncmp(inparams[i],"op",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else {
      // incorrect/unrecognized parameter
      snprintf(errbuf,100,UNKNOWN_PARAM_ERR,inparams[i]);
      errhalt(errbuf);
    }
  }
  // all parameters and values were understood
  if (format==0) {
    // csv
    maxdepth=0; // record structure not printed for csv
    escape=3; // only " replaced with ""
    *hformat=0; // store to caller for content-type header
    global_format=0; // for error handler only
  }
  // check search parameters
  if (!fcount) {
    if (vcount || ccount || tcount) errhalt(NO_FIELD_ERR);
    else nosearch=1;
  }
  // attach to database
  db = wg_attach_existing_database(database);
  global_db=db;
  if (!db) errhalt(DB_ATTACH_ERR);
  res=malloc(res_size);
  if (!res) {
    err_clear_detach_halt(db,0,MALLOC_ERR);
  }
  // database attached OK
  // create output string buffer (may be reallocated later)

  strbuffer=str_new(INITIAL_MALLOC);
  strbufferlen=INITIAL_MALLOC;
  strbufferptr=strbuffer;
  if (nosearch) {
    // ------- special case without real search: just output records ---
    gcount=0;
    rcount=0;
    lock_id = wg_start_read(db); // get read lock
    global_lock_id=lock_id; // only for handling errors
    if (!lock_id) err_clear_detach_halt(db,0,LOCK_ERR);
    str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
    if (format!=0) {
      // json
      snprintf(strbufferptr,MIN_STRLEN,"[\n");
      strbufferptr+=2;
    }
    if (maxdepth>MAX_DEPTH_HARD) maxdepth=MAX_DEPTH_HARD;
    rec=wg_get_first_record(db);
    do {
      if (rcount>=from) {
        gcount++;
        if (gcount>count) break;
        str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
        if (gcount>1 && format!=0) {
          // json and not first row
          snprintf(strbufferptr,MIN_STRLEN,",\n");
          strbufferptr+=2;
        }
        sprint_record(db,rec,&strbuffer,&strbufferlen,&strbufferptr,format,showid,0,maxdepth,escape);
        if (format==0) {
          // csv
          str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
          snprintf(strbufferptr,MIN_STRLEN,"\r\n");
          strbufferptr+=2;
        }
      }
      rec=wg_get_next_record(db,rec);
      rcount++;
    } while(rec!=NULL);
    if (!wg_end_read(db, lock_id)) {  // release read lock
      err_clear_detach_halt(db,lock_id,LOCK_RELEASE_ERR);
    }
    global_lock_id=0; // only for handling errors
    wg_detach_database(db);
    global_db=NULL; // only for handling errors
    str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
    if (format!=0) {
      // json
      snprintf(strbufferptr,MIN_STRLEN,"\n]");
      strbufferptr+=3;
    }
    return strbuffer;
  }

  // ------------ normal search case: ---------

  // create a query list datastructure

  for(i=0;i<fcount;i++) {
    // field num
    if (!isint(fields[i])) err_clear_detach_halt(db,0,NO_FIELD_ERR);
    itmp=atoi(fields[i]);
    // column to compare
    wgargs[i].column = itmp;
    // comparison op: default equal
    wgargs[i].cond = encode_incomp(db,compares[i]);
    // valuetype: default guess from value later
    type=encode_intype(db,types[i]);
    // encode value to compare with
    wgargs[i].value =  encode_invalue(db,values[i],type);
  }

  // make the query structure and read-lock the database before!

  lock_id = wg_start_read(db); // get read lock
  global_lock_id=lock_id; // only for handling errors
  if (!lock_id) err_clear_detach_halt(db,lock_id,LOCK_ERR);
  wgquery = wg_make_query(db, NULL, 0, wgargs, i);
  if (!wgquery) err_clear_detach_halt(db,lock_id,QUERY_ERR);

  // actually perform the query

  rcount=0;
  gcount=0;
  str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
  if (format!=0) {
    // json
    snprintf(strbufferptr,MIN_STRLEN,"[\n");
    strbufferptr+=2;
  }
  if (maxdepth>MAX_DEPTH_HARD) maxdepth=MAX_DEPTH_HARD;
  while((rec = wg_fetch(db, wgquery))) {
    if (rcount>=from) {
      gcount++;
      str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
      if (gcount>1 && format!=0) {
        // json and not first row
        snprintf(strbufferptr,MIN_STRLEN,",\n");
        strbufferptr+=2;
      }
      sprint_record(db,rec,&strbuffer,&strbufferlen,&strbufferptr,format,showid,0,maxdepth,escape);
      if (format==0) {
        // csv
        str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
        snprintf(strbufferptr,MIN_STRLEN,"\r\n");
        strbufferptr+=2;
      }
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
  str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
  if (format!=0) {
    // json
    snprintf(strbufferptr,MIN_STRLEN,"\n]");
    strbufferptr+=3;
  }
  return strbuffer;
}


/* second possible query operation: get concrete records by ids from the database */

char* recids(char* database, char* inparams[], char* invalues[], int incount, int* hformat) {
  int i,j,x,gcount;
  char* cids;
  wg_int ids[MAXIDS];
  int count=MAXCOUNT;
  void* db=NULL; // actual database pointer
  void* rec;
  char* res;
  int res_size=INITIAL_MALLOC;
  wg_int lock_id=0;  // non-0 iff lock set
  int maxdepth=MAX_DEPTH_DEFAULT; // rec depth limit for printer
  int showid=0; // add record id as first extra elem: 0: no, 1: yes
  int format=1; // 0: csv, 1:json
  int escape=2; // string special chars escaping:  0: just ", 1: urlencode, 2: json, 3: csv
  char errbuf[200]; // used for building variable-content input param error strings only
  char* strbuffer; // main result string buffer start (malloced later)
  int strbufferlen; // main result string buffer length
  char* strbufferptr; // current output location ptr in the main result string buffer

  // -------check and parse cgi parameters, attach database ------------

  // set ids to defaults
  for(i=0;i<MAXIDS;i++) {
    ids[i]=0;
  }
  // find ids and display format parameters
  for(i=0;i<incount;i++) {
    if (strncmp(inparams[i],"recids",MAXQUERYLEN)==0) {
      cids=invalues[i];
      x=0;
      // split csv int list to ids int array
      for(j=0;j<strlen(cids);j++) {
        if (atoi(cids+j) && atoi(cids+j)>0) ids[x++]=atoi(cids+j);
        if (x>=MAXIDS) break;
        for(;j<strlen(cids) && cids[j]!=','; j++) {};
      }
    } else if (strncmp(inparams[i],"depth",MAXQUERYLEN)==0) {
      maxdepth=atoi(invalues[i]);
    } else if (strncmp(inparams[i],"showid",MAXQUERYLEN)==0) {
      if (strncmp(invalues[i],"yes",MAXQUERYLEN)==0) showid=1;
      else if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) showid=0;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        errhalt(errbuf);
      }
    } else if (strncmp(inparams[i],"format",MAXQUERYLEN)==0) {
      if (strncmp(invalues[i],"csv",MAXQUERYLEN)==0) format=0;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) format=1;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        errhalt(errbuf);
      }
    } else if (strncmp(inparams[i],"escape",MAXQUERYLEN)==0) {
      if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) escape=0;
      else if (strncmp(invalues[i],"url",MAXQUERYLEN)==0) escape=1;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) escape=2;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        errhalt(errbuf);
      }
    } else if (strncmp(inparams[i],"db",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else if (strncmp(inparams[i],"op",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else {
      // incorrect/unrecognized parameter
      snprintf(errbuf,100,UNKNOWN_PARAM_ERR,inparams[i]);
      errhalt(errbuf);
    }
  }
  // all parameters and values were understood
  if (format==0) {
    // csv
    maxdepth=0; // record structure not printed for csv
    escape=3; // only " replaced with ""
    *hformat=0; // store to caller for content-type header
    global_format=0; // for error handler only
  }
  // attach to database
  db = wg_attach_existing_database(database);
  global_db=db;
  if (!db) errhalt(DB_ATTACH_ERR);
  res=malloc(res_size);
  if (!res) {
    err_clear_detach_halt(db,0,MALLOC_ERR);
  }
  // database attached OK
  // create output string buffer (may be reallocated later)

  strbuffer=str_new(INITIAL_MALLOC);
  strbufferlen=INITIAL_MALLOC;
  strbufferptr=strbuffer;

  // take a read lock and loop over ids
  gcount=0;
  lock_id = wg_start_read(db); // get read lock
  global_lock_id=lock_id; // only for handling errors
  if (!lock_id) err_clear_detach_halt(db,0,LOCK_ERR);
  str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
  if (format!=0) {
    // json
    snprintf(strbufferptr,MIN_STRLEN,"[\n");
    strbufferptr+=2;
  }
  if (maxdepth>MAX_DEPTH_HARD) maxdepth=MAX_DEPTH_HARD;
  for(j=0; ids[j]!=0 && j<MAXIDS; j++) {

    x=wg_get_encoded_type(db,ids[j]);
    if (x!=WG_RECORDTYPE) continue;
    rec=wg_decode_record(db,ids[j]);
    if (rec==NULL) continue;
    x=wg_get_record_len(db,rec);
    if (x<=0) continue;

    gcount++;
    if (gcount>count) break;
    str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
    if (gcount>1 && format!=0) {
      // json and not first row
      snprintf(strbufferptr,MIN_STRLEN,",\n");
      strbufferptr+=2;
    }
    sprint_record(db,rec,&strbuffer,&strbufferlen,&strbufferptr,format,showid,0,maxdepth,escape);
    if (format==0) {
      // csv
      str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
      snprintf(strbufferptr,MIN_STRLEN,"\r\n");
      strbufferptr+=2;
    }
    rec=wg_get_next_record(db,rec);
  }
  if (!wg_end_read(db, lock_id)) {  // release read lock
    err_clear_detach_halt(db,lock_id,LOCK_RELEASE_ERR);
  }
  global_lock_id=0; // only for handling errors
  wg_detach_database(db);
  global_db=NULL; // only for handling errors
  str_guarantee_space(&strbuffer,&strbufferlen,&strbufferptr,MIN_STRLEN);
  if (format!=0) {
    // json
    snprintf(strbufferptr,MIN_STRLEN,"\n]");
    strbufferptr+=3;
  }
  return strbuffer;
}


/* ***************  encode cgi params as query vals  ******************** */


static wg_int encode_incomp(void* db, char* incomp) {
  if (incomp==NULL || incomp=='\0') return WG_COND_EQUAL;
  else if (!strcmp(incomp,"equal"))  return WG_COND_EQUAL;
  else if (!strcmp(incomp,"not_equal"))  return WG_COND_NOT_EQUAL;
  else if (!strcmp(incomp,"lessthan"))  return WG_COND_LESSTHAN;
  else if (!strcmp(incomp,"greater"))  return WG_COND_GREATER;
  else if (!strcmp(incomp,"ltequal"))  return WG_COND_LTEQUAL;
  else if (!strcmp(incomp,"gtequal"))  return WG_COND_GTEQUAL;
  else err_clear_detach_halt(db,0,COND_ERR);
  return WG_COND_EQUAL; // this return never happens
}

static wg_int encode_intype(void* db, char* intype) {
  if (intype==NULL || intype=='\0') return 0;
  else if (!strcmp(intype,"null"))  return WG_NULLTYPE;
  else if (!strcmp(intype,"int"))  return WG_INTTYPE;
  else if (!strcmp(intype,"record"))  return WG_RECORDTYPE;
  else if (!strcmp(intype,"double"))  return WG_DOUBLETYPE;
  else if (!strcmp(intype,"str"))  return WG_STRTYPE;
  else if (!strcmp(intype,"char"))  return WG_CHARTYPE;
  else err_clear_detach_halt(db,0,INTYPE_ERR);
  return 0; // this return never happens
}

static wg_int encode_invalue(void* db, char* invalue, wg_int type) {
  if (invalue==NULL) {
    err_clear_detach_halt(db,0,INVALUE_ERR);
    return 0; // this return never happens
  }
  if (type==WG_NULLTYPE) return wg_encode_query_param_null(db,NULL);
  else if (type==WG_INTTYPE) {
    if (!isint(invalue)) err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);
    return wg_encode_query_param_int(db,atoi(invalue));
  } else if (type==WG_RECORDTYPE) {
    if (!isint(invalue)) err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);
    return (wg_int)atoi(invalue);
  } else if (type==WG_DOUBLETYPE) {
    if (!isdbl(invalue)) err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);
    return wg_encode_query_param_double(db,strtod(invalue,NULL));
  } else if (type==WG_STRTYPE) {
    return wg_encode_query_param_str(db,invalue,NULL);
  } else if (type==WG_CHARTYPE) {
    return wg_encode_query_param_char(db,invalue[0]);
  } else if (type==0 && isint(invalue)) {
    return wg_encode_query_param_int(db,atoi(invalue));
  } else if (type==0 && isdbl(invalue)) {
    return wg_encode_query_param_double(db,strtod(invalue,NULL));
  } else if (type==0) {
    return wg_encode_query_param_str(db,invalue,NULL);
  } else {
    err_clear_detach_halt(db,0,INVALUE_TYPE_ERR);
    return 0; // this return never happens
  }
}

/* *******************  cgi query parsing  ******************** */


/* query parser: split by & and =, urldecode param and value
   return param count or -1 for error
*/

static int parse_query(char* query, int ql, char* params[], char* values[]) {
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

static char* urldecode(char *indst, char *src) {
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

static int isint(char* s) {
  if (s==NULL) return 0;
  while(*s!='\0') {
    if (!isdigit(*s)) return 0;
    s++;
  }
  return 1;
}

/* return 1 iff s contains numerals plus single optional period only
*/


static int isdbl(char* s) {
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


/** Print a record, handling records recursively

  The value is written into a character buffer.

  db: database pointer
  rec: rec to be printed

  buf: address of the whole string buffer start (not the start itself)
  bufsize: address of the actual pointer to start printing at in buffer
  bptr: address of the whole string buffer

  format: 0 csv, 1 json

  showid: print record id for record: 0 no show, 1 first (extra) elem of record

  depth: current depth in a nested record tree (increases from initial 0)
  maxdepth: limit on printing records nested via record pointers (0: no nesting)

  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe


*/



int sprint_record(void *db, wg_int *rec, char **buf, int *bufsize, char **bptr,
                   int format, int showid, int depth,  int maxdepth, int strenc) {
  int i,limit;
  wg_int enc, len;
#ifdef USE_CHILD_DB
  void *parent;
#endif
  limit=MIN_STRLEN;
  str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
  if (rec==NULL) {
    snprintf(*bptr, limit, JS_NULL);
    (*bptr)+=strlen(JS_NULL);
    return 1;
  }
  if (format!=0) {
    // json
    **bptr= '[';
    (*bptr)++;
  }
#ifdef USE_CHILD_DB
  parent = wg_get_rec_owner(db, rec);
#endif
  if (1) {
    len = wg_get_record_len(db, rec);
    if (showid) {
      // add record id (offset) as the first extra elem of record
      snprintf(*bptr, limit-1, "%d",wg_encode_record(db,rec));
      *bptr=*bptr+strlen(*bptr);
    }
    for(i=0; i<len; i++) {
      enc = wg_get_field(db, rec, i);
#ifdef USE_CHILD_DB
      if(parent != db)
        enc = wg_translate_hdroffset(db, parent, enc);
#endif
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      if (i || showid) {
        if (format!=0) **bptr = ',';
        else **bptr = CSV_SEPARATOR;
        (*bptr)++;
      }
      *bptr=sprint_value(db, enc, buf, bufsize, bptr, format, showid, depth, maxdepth, strenc);
    }
  }
  if (format!=0) {
    // json
    str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
    **bptr = ']';
    (*bptr)++;
  }
  return 1;
}


/** Print a single encoded value (may recursively contain record(s)).

  The value is written into a character buffer.

  db: database pointer
  enc: encoded value to be printed

  buf: address of the whole string buffer start (not the start itself)
  bufsize: address of the actual pointer to start printing at in buffer
  bptr: address of the whole string buffer

  format: 0 csv, 1 json

  showid: print record id for record: 0 no show, 1 first (extra) elem of record

  depth: limit on records nested via record pointers (0: no nesting)
  maxdepth: limit on printing records nested via record pointers (0: no nesting)

  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe
  strenc==3: csv encoding, only " replaced for ""

  returns nr of bytes printed

*/


char* sprint_value(void *db, wg_int enc, char **buf, int *bufsize, char **bptr,
                 int format, int showid, int depth, int maxdepth, int strenc) {
  wg_int *ptrdata;
  int intdata,strl,strl1,strl2;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[80]; // tmp area for dates
  int limit=MIN_STRLEN;

  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      if (format!=0) {
        // json
        snprintf(*bptr, limit, JS_NULL);
        return *bptr+strlen(*bptr);
      }
      return *bptr;
    case WG_RECORDTYPE:
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      if (!format || depth>=maxdepth) {
        snprintf(*bptr, limit,"%d", (int)enc); // record offset (i.e. id)
        return *bptr+strlen(*bptr);
      } else {
        // recursive print
        ptrdata = wg_decode_record(db, enc);
        sprint_record(db,ptrdata,buf,bufsize,bptr,format,showid,depth+1,maxdepth,strenc);
        **bptr='\0';
        return *bptr;
      }
      break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, "%d", intdata);
      return *bptr+strlen(*bptr);
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, DOUBLE_FORMAT, doubledata);
      return *bptr+strlen(*bptr);
    case WG_FIXPOINTTYPE:
      doubledata = wg_decode_fixpoint(db, enc);
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, DOUBLE_FORMAT, doubledata);
      return *bptr+strlen(*bptr);
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      exdata = wg_decode_str_lang(db,enc);
      if (strdata!=NULL) strl1=strlen(strdata);
      else strl1=0;
      if (exdata!=NULL) strl2=strlen(exdata);
      else strl2=0;
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN+STRLEN_FACTOR*(strl1+strl2));
      sprint_string(*bptr,(strl1+strl2),strdata,strenc);
      if (exdata!=NULL) {
        snprintf(*bptr+strl1+1,limit,"@%s\"", exdata);
      }
      return *bptr+strlen(*bptr);
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      if (strdata!=NULL) strl1=strlen(strdata);
      else strl1=0;
      if (exdata!=NULL) strl2=strlen(exdata);
      else strl2=0;
      limit=MIN_STRLEN+STRLEN_FACTOR*(strl1+strl2);
      str_guarantee_space(buf, bufsize, bptr, limit);
      if (exdata==NULL)
        snprintf(*bptr, limit, "\"%s\"", strdata);
      else
        snprintf(*bptr, limit, "\"%s:%s\"", exdata, strdata);
      return *bptr+strlen(*bptr);
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      if (strdata!=NULL) strl1=strlen(strdata);
      else strl1=0;
      if (exdata!=NULL) strl2=strlen(exdata);
      else strl2=0;
      limit=MIN_STRLEN+STRLEN_FACTOR*(strl1+strl2);
      str_guarantee_space(buf, bufsize, bptr, limit);
      snprintf(*bptr, limit, "\"%s:%s\"", exdata, strdata);
      return *bptr+strlen(*bptr);
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, "\"%c\"", (char) intdata);
      return *bptr+strlen(*bptr);
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, "\"%s\"",strbuf);
      return *bptr+strlen(*bptr);
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, "\"%s\"",strbuf+11);
      return *bptr+strlen(*bptr);
    case WG_VARTYPE:
      intdata = wg_decode_var(db, enc);
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, "\"?%d\"", intdata);
      return *bptr+strlen(*bptr);
    case WG_BLOBTYPE:
      strdata = wg_decode_blob(db, enc);
      strl=wg_decode_blob_len(db, enc);
      limit=MIN_STRLEN+STRLEN_FACTOR*strlen(strdata);
      str_guarantee_space(buf, bufsize, bptr, limit);
      sprint_blob(*bptr,strl,strdata,strenc);
      return *bptr+strlen(*bptr);
    default:
      str_guarantee_space(buf, bufsize, bptr, MIN_STRLEN);
      snprintf(*bptr, limit, JS_TYPE_ERR);
      return *bptr+strlen(*bptr);
  }
}


/* Print string with several encoding/escaping options.

  It must be guaranteed beforehand that there is enough room in the buffer.

  bptr: direct pointer to location in buffer where to start writing
  limit: max nr of chars traversed (NOT limiting output len)
  strdata: pointer to printed string

  strenc==0: nothing is escaped at all
  strenc==1: non-ascii chars and % and " urlencoded
  strenc==2: json utf-8 encoding, not ascii-safe
  strenc==3: csv encoding, only " replaced for ""

  For proper json tools see:

  json rfc http://www.ietf.org/rfc/rfc4627.txt
  ccan json tool http://git.ozlabs.org/?p=ccan;a=tree;f=ccan/json
  Jansson json tool https://jansson.readthedocs.org/en/latest/
  Parser http://linuxprograms.wordpress.com/category/json-c/

*/

int sprint_string(char* bptr, int limit, char* strdata, int strenc) {
  unsigned char c;
  char *sptr;
  char *hex_chars="0123456789abcdef";
  int i;
  sptr=strdata;
  *bptr++='"';
  if (sptr==NULL) {
    *bptr++='"';
    *bptr='\0';
    return 1;
  }
  if (!strenc) {
    // nothing is escaped at all
    for(i=0;i<limit;i++) {
      c=*sptr++;
      if (c=='\0') {
        *bptr++='"';
        *bptr='\0';
        return 1;
      } else {
        *bptr++=c;
      }
    }
  } else if (strenc==3) {
    // csv " replaced for "", no other escapes
    for(i=0;i<limit;i++) {
      c=*sptr++;
      if (c=='\0') {
        *bptr++='"';
        *bptr='\0';
        return 1;
      } else if (c=='"') {
        *bptr++=c;
        *bptr++=c;
      } else {
        *bptr++=c;
      }
    }
  } else if (strenc==1) {
    // non-ascii chars and % and " urlencoded
    for(i=0;i<limit;i++) {
      c=*sptr++;
      if (c=='\0') {
        *bptr++='"';
        *bptr='\0';
        return 1;
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
    for(i=0;i<limit;i++) {
      c=*sptr++;
      switch(c) {
      case '\0':
        *bptr++='"';
        *bptr='\0';
        return 1;
      case '\b':
      case '\n':
      case '\r':
      case '\t':
      case '\f':
      case '"':
      case '\\':
      case '/':
        if(c == '\b') sprint_append(&bptr, "\\b", 2);
        else if(c == '\n') sprint_append(&bptr, "\\n", 2);
        else if(c == '\r') sprint_append(&bptr, "\\r", 2);
        else if(c == '\t') sprint_append(&bptr, "\\t", 2);
        else if(c == '\f') sprint_append(&bptr, "\\f", 2);
        else if(c == '"') sprint_append(&bptr, "\\\"", 2);
        else if(c == '\\') sprint_append(&bptr, "\\\\", 2);
        else if(c == '/') sprint_append(&bptr, "\\/", 2);
        break;
      default:
        if(c < ' ') {
          sprint_append(&bptr, "\\u00", 4);
          *bptr++=hex_chars[c >> 4];
          *bptr++=hex_chars[c & 0xf];
        } else {
          *bptr++=c;
        }
      }
    }
  }
  *bptr++='"';
  *bptr='\0';
  return 1;
}


int sprint_blob(char* bptr, int limit, char* strdata, int strenc) {
  unsigned char c;
  char *sptr;
  char *hex_chars="0123456789abcdef";
  int i;
  sptr=strdata;
  *bptr++='"';
  if (sptr==NULL) {
    *bptr++='"';
    *bptr='\0';
    return 1;
  }
  // non-ascii chars and % and " urlencoded
  for(i=0;i<limit;i++) {
    c=*sptr++;
    if (c=='\0') {
      *bptr++='"';
      *bptr='\0';
      return 1;
    } else if (c < ' ' || c=='%' || c=='"' || (int)c>126) {
      *bptr++='%';
      *bptr++=hex_chars[c >> 4];
      *bptr++=hex_chars[c & 0xf];
    } else {
      *bptr++=c;
    }
  }
  *bptr++='"';
  *bptr='\0';
  return 1;
}

int sprint_append(char** bptr, char* str, int l) {
  int i;

  for(i=0;i<l;i++) *(*bptr)++=*str++;
  return 1;
}


/* *********** Functions for string buffer ******** */


/** Allocate a new string with length len, set last element to 0
*
*/

static char* str_new(int len) {
  char* res;

  res = (char*) malloc(len*sizeof(char));
  if (res==NULL) {
    err_clear_detach_halt(global_db,global_lock_id,MALLOC_ERR);
    return NULL; // never returns
  }
  res[len-1]='\0';
  return res;
}


/** Guarantee string space: realloc if necessary, change ptrs, set last byte to 0
*
*/

static int str_guarantee_space(char** stradr, int* strlenadr, char** ptr, int needed) {
  char* tmp;
  int newlen,used;

  if (needed>(*strlenadr-(int)((*ptr)-(*stradr)))) {
    used=(int)((*ptr)-(*stradr));
    newlen=(*strlenadr)*2;
    if (newlen<needed) newlen=needed;
    if (newlen>MAX_MALLOC) {
      if (*stradr!=NULL) free(*stradr);
      err_clear_detach_halt(global_db,global_lock_id,MALLOC_ERR);
      return 0; // never returns
    }
    //printf("needed %d oldlen %d used %d newlen %d \n",needed,*strlenadr,used,newlen);
    tmp=realloc(*stradr,newlen);
    if (tmp==NULL) {
      if (*stradr!=NULL) free(*stradr);
      err_clear_detach_halt(global_db,global_lock_id,MALLOC_ERR);
      return 0; // never returns
    }
    tmp[newlen-1]=0;   // set last byte to 0
    //printf("oldstradr %d newstradr %d oldptr %d newptr %d \n",(int)*stradr,(int)tmp,(int)*ptr,(int)tmp+used);
    *stradr=tmp;
    *strlenadr=newlen;
    *ptr=tmp+used;
    return 1;
  }
  return 1;
}

/* *******************  errors  ******************** */

/* called in case of internal errors by the signal catcher:
   it is crucial that the locks are released and db detached */

void termination_handler(int xsignal) {
  err_clear_detach_halt(global_db,global_lock_id,INTERNAL_ERR);
}


/* called in case of timeout by the signal catcher:
   it is crucial that the locks are released and db detached */

void timeout_handler(int signal) {
  err_clear_detach_halt(global_db,global_lock_id,TIMEOUT_ERR);
}

/* normal termination call: free locks, detach, call errprint and halt */

void err_clear_detach_halt(void* db, wg_int lock_id, char* errstr) {
  int r;
  if (lock_id) {
    r=wg_end_read(db, lock_id);
    global_lock_id=0; // only for handling errors
  }
  if (db!=NULL) wg_detach_database(db);
  global_db=NULL; // only for handling errors
  errhalt(errstr);
}

/* error output and immediate halt
*/

void errhalt(char* str) {
  char buf[1000];
  snprintf(buf,1000,"[\"%s\"]",str);
  print_final(buf,global_format);
  exit(0);
}

