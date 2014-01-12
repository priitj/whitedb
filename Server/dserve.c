/*

dserve.c contains the main functionality of dserve:

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http(s). Results are given in the json or csv format.

Run dserver in one of three ways:

* a cgi program under a web server, connecting like
  http://myhost.com/dserve?op=search&from=0&count=5  
* as a standalone http(s) server, passing a port number as a single argument, like
  dserve 8080
  and connecting like
  http://localhost:8080/dserve?op=search&from=0&count=5
  or, for dservehttps compiled with USE_OPENSSL
  dservehttps 8081 conf.txt
  https://localhost:8081/dserve?op=search&from=0&count=5
* from the command line, passing a cgi-format, urlencoded query string
  as a single argument, like
  dserve 'op=search&from=0&count=5'
  
Use the provided Makefile or compile.bat for ompiling dserve or
compile directly as:

gcc dserve.c dserve_util.c dserve_net.c  -o dserve -O2 -lwgdb -lpthread
gcc -DUSE_OPENSSL dserve.c dserve_util.c dserve_net.c  -o dservehttps 
  -O2 -lwgdb -lpthread -lssl -lcrypto

dserve can be also compiled to work as a cgi or command line tool only
without using pthreads by:
- removing #define SERVEROPTION from dserve.h
- compiling by gcc dserve.c dserve_util.c -o dserve -O2 -lwgdb 

Compiling under windows:
copy the files dbapi.h and wgdb.lib into the same folder where you compile, then build
the server version:
cl /Ox /I"." dserve.c dserve_util.c dserve_net.c wgdb.lib
or a non-server version
cl /Ox /I"." dserve.c dserve_util.c wgdb.lib

Use and modify the code for creating your own data servers for WhiteDB.

See http://whitedb.org/tools.html for a detailed manual.

Copyright (c) 2013, Tanel Tammet

This software is under MIT licence:
--------
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
------

NB! Observe that the current file is under a different licence than the
WhiteDB library: the latter is by default under GPLv3. Thus the
linked dserve is under GPLv3 unless a free commercial licence is used
(see whitedb.org for details).

It is OK to use the MIT licence when using this code or parts of it in
other projects without linking to the whitedb library.

*/

#include "dserve.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h> // for alarm and termination signal handling
#include <limits.h> // LONG_MAX
#if _MSC_VER   // no alarm on windows
#else
#include <unistd.h> // for alarm
#endif

//#include "../json/yajl_all.h"


/* =============== local protos =================== */

static char* get_cgi_query(thread_data_p tdata, char* inmethod);
static void setup_globals(void);

static char* search(thread_data_p tdata, char* inparams[], char* invalues[], 
  int count, int opcode);
static char* insert(thread_data_p tdata, char* inparams[], char* invalues[], 
  int incount);
static char* create(thread_data_p tdata, char* inparams[], char* invalues[], 
  int incount);
static char* drop(thread_data_p tdata, char* inparams[], char* invalues[], 
  int incount);

static int op_print_record(thread_data_p tdata,void* rec,int gcount);
static int op_delete_record(thread_data_p tdata,void* rec);

static char* handle_generic_param(thread_data_p tdata,char* key,char* value,
                                  char** token,char* errbuf);
static char* handle_fld_param(thread_data_p tdata,char* key,char* value,
              char** sfields, char** svalues, char** stypes, int sfcount, char* errbuf);                                  
static int op_print_data_start(thread_data_p tdata, int listflag);
static int op_print_data_end(thread_data_p tdata, int listflag);
static int op_update_record(thread_data_p tdata,void* db, void* rec, wg_int fld, wg_int value);
static void* op_create_database(thread_data_p tdata,char* database,long size);


/* =============== globals =================== */

// globalptr points to a struct containing a pointer to conf,
// the nr of threads and 
// an array of separate data blocks for each thread: global ptr
// guarantees access from the signal-called termination_handler.
// Except in main and termination handlers this is not used directly:
// access through thread_data->global instead.

dserve_global_p globalptr;

/* yajl */
/*
static int reformat_null(void * ctx)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_null(g);  
}  
  
static int reformat_boolean(void * ctx, int boolean)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_bool(g, boolean);  
}  
  
static int reformat_number(void * ctx, const char * s, size_t l)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_number(g, s, l);  
}  
  
static int reformat_string(void * ctx, const unsigned char * stringVal,  
                           size_t stringLen)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_string(g, stringVal, stringLen);  
}  
  
static int reformat_map_key(void * ctx, const unsigned char * stringVal,  
                            size_t stringLen)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_string(g, stringVal, stringLen);  
}  
  
static int reformat_start_map(void * ctx)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_map_open(g);  
}  
  
  
static int reformat_end_map(void * ctx)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    return yajl_gen_status_ok == yajl_gen_map_close(g);  
}  
  
static int reformat_start_array(void * ctx)  
{  
    yajl_gen g = (yajl_gen) ctx;
    //printf("array start depth %d\n",g.depth); 
    return yajl_gen_status_ok == yajl_gen_array_open(g);  
}  
  
static int reformat_end_array(void * ctx)  
{  
    yajl_gen g = (yajl_gen) ctx;  
    //printf("array end depth %d\n",g.depth); 
    return yajl_gen_status_ok == yajl_gen_array_close(g);  
}  
  
static yajl_callbacks callbacks = {  
    reformat_null,  
    reformat_boolean,  
    NULL,  
    NULL,  
    reformat_number,  
    reformat_string,  
    reformat_start_map,  
    reformat_map_key,  
    reformat_end_map,  
    reformat_start_array,  
    reformat_end_array  
};  
  
*/
/* =============== main =================== */

int main(int argc, char **argv) {
  char *inquery=NULL, *inip=NULL;
  char *inmethod=NULL, *inpath=NULL;  
  int port=0, cgi=0;  
  thread_data_p tdata;

  setup_globals(); // set globalptr and its components
  // Set up abnormal termination handler to clear locks 
#ifdef CATCH_SIGNALS  
  signal(SIGSEGV,termination_handler);
  signal(SIGFPE,termination_handler);
  signal(SIGABRT,termination_handler);
  signal(SIGTERM,termination_handler);
  signal(SIGINT,termination_handler);
  signal(SIGILL,termination_handler);
#endif  
#if _MSC_VER // some signals not used in windows
#else
  signal(SIGPIPE,SIG_IGN); // important for TCP/IP handling 
#endif
  // detect calling parameters
  // process environment and args
  inmethod=getenv("REQUEST_METHOD");
  if (inmethod!=NULL) {
    // assume cgi call
    cgi=1;    
    inip=getenv("REMOTE_ADDR");   
#ifdef CONF_FILE
    inpath=CONF_FILE;
#endif    
  } else {      
#ifdef SERVEROPTION
    if (argc<=1) {
      // no params      
#ifdef DEFAULT_PORT // use as server by default   
      port=DEFAULT_PORT;
#else
      print_help(); 
      exit(0);
#endif    
    } else if (argc>1) {
      // command line param given      
      inquery=argv[1];
      if (!strcmp(inquery,HELP_PARAM)) { print_help(); exit(0); }
      port=atoi(inquery); // 0 port means no server 
      if (argc>2) {
        // conf file given
        inpath=argv[2];
      }
    }
    // run either as a server or a command line/cgi program
    if (port) {
#ifdef CONF_FILE
      if (inpath==NULL) inpath=CONF_FILE;
#endif    
      if (inpath!=NULL) {
        // process conf file
        load_configuration(inpath,globalptr->conf);
        //print_conf(globalptr->conf);
      }
      run_server(port,globalptr);
      return 0;
    }
#else
    if (argc>1) {
      // command line param given
      inquery=argv[1];
      if (argc>2) {
        // conf file given
        inpath=argv[2];
      } else {
#ifdef CONF_FILE
        inpath=CONF_FILE;
#endif
      }
    } else {
      // no params given
      print_help(); 
      exit(0);
    }
#endif
  }
  if (!port) {
    // run as command line or cgi
#if _MSC_VER  // no alarm on windows
#else
    // a timeout for cgi/command line
    signal(SIGALRM,timeout_handler);
    alarm(TIMEOUT_SECONDS);
#endif
    if (inpath!=NULL) {
      // process conf file
      load_configuration(inpath,globalptr->conf);
      //print_conf(globalptr->conf);
    }
    // setup a single tdata block
    globalptr->maxthreads=1;
    tdata=&(globalptr->threads_data[0]);
    tdata->isserver=0;
    tdata->iscgi=cgi;
    tdata->ip=inip;
    tdata->port=0;
    tdata->method=0;
    tdata->realthread=0;
    tdata->format=1;
    tdata->global=globalptr;
    tdata->inbuf=NULL;
    tdata->intype=0;  
    tdata->common=NULL;     
    if (cgi) inquery=get_cgi_query(tdata,inmethod);             
    // actual processing
    process_query(inquery,tdata);
    return 0;
  }    
  return 0;
}  

/* used in cgi case only: get input data, data type and
   set crucial tdata fields.

   Returns inquery str if successful and NULL otherwise.
*/

static char* get_cgi_query(thread_data_p tdata, char* inmethod) {
  char *inquery=NULL, *inlen=NULL, *intype=NULL; 
  int len=0, type=0, n;
  if (inmethod!=NULL && !strcmp(inmethod,"GET")) {
    tdata->method=GET_METHOD_CODE;
    inquery=getenv("QUERY_STRING");
    return inquery;
  } else if (inmethod!=NULL && !strcmp(inmethod,"POST")) {
    tdata->method=POST_METHOD_CODE;
    inlen=getenv("CONTENT_LENGTH");
    if (inlen==NULL) return NULL;
    len=atoi(inlen);
    if (len<=0) return NULL;
    if (len>=MAX_MALLOC) return NULL;
    inquery=malloc(len+10);
    if (!inquery) return NULL;
    tdata->inbuf=inquery;
    intype=getenv("CONTENT_TYPE");
    if (intype!=NULL) {
      if (strstr(intype,"application/x-www-form-urlencoded")!=NULL) 
        type=CONTENT_TYPE_URLENCODED;
      else if (strstr(intype,"application/json")!=NULL) 
        type=CONTENT_TYPE_JSON;
      tdata->intype=type;
    }
    n=fread(inquery,1,len,stdin);
    if (n<=0) { return NULL; }
    if (n<len) {
      // read less than content-length
    }
    return inquery; 
  } else {
    return NULL;
  }
}
  
static void setup_globals(void) {
  int i;
  
  // set up global data
  globalptr=malloc(sizeof(struct dserve_global));
  if (globalptr==NULL) {errprint(CANNOT_ALLOC_ERR,NULL); exit(-1);}
  globalptr->conf=malloc(sizeof(struct dserve_conf));
  if (globalptr->conf==NULL) {errprint(CANNOT_ALLOC_ERR,NULL); exit(-1);}
  globalptr->maxthreads=MAX_THREADS;
  for(i=0;i<globalptr->maxthreads;i++) {
    globalptr->threads_data[i].db=NULL;
    globalptr->threads_data[i].inuse=0;
  }
  globalptr->conf->default_dbase.size=0;
  globalptr->conf->default_dbase_size.size=0;
  globalptr->conf->max_dbase_size.size=0;
  globalptr->conf->dbases.size=0;
  globalptr->conf->admin_ips.size=0;
  globalptr->conf->write_ips.size=0;
  globalptr->conf->read_ips.size=0;
  globalptr->conf->admin_tokens.size=0;
  globalptr->conf->write_tokens.size=0;
  globalptr->conf->read_tokens.size=0;
  globalptr->conf->default_dbase.used=0;
  globalptr->conf->default_dbase_size.used=0;
  globalptr->conf->max_dbase_size.size=0;
  globalptr->conf->dbases.used=0;
  globalptr->conf->admin_ips.used=0;
  globalptr->conf->write_ips.used=0;
  globalptr->conf->read_ips.used=0;
  globalptr->conf->admin_tokens.used=0;
  globalptr->conf->write_tokens.used=0;
  globalptr->conf->read_tokens.used=0;  
}
  
char* process_query(char* inquery, thread_data_p tdata) {  
  int i=0;
  char *query;
  char querybuf[MAXQUERYLEN];  
  int pcount=0;
  int ql,found;  
  char* res=NULL;
  char* database=DEFAULT_DATABASE;
  char* params[MAXPARAMS];
  char* values[MAXPARAMS];
 
  
  // first NULL values potentially left from earlier thread calls
#if _MSC_VER
#else  
  tdata->db=NULL;
#endif  
  tdata->database=NULL;
  tdata->lock_id=0;
  tdata->intype=0;
  tdata->jsonp=NULL;
  tdata->format=1;
  tdata->showid=0;
  tdata->depth=MAX_DEPTH_DEFAULT;
  tdata->maxdepth=MAX_DEPTH_DEFAULT;
  tdata->strenc=2;
  tdata->buf=NULL;
  tdata->bufptr=NULL;
  tdata->bufsize=0;      
  // or use your own query string for testing a la
  // inquery="db=1000&op=search&field=1&value=2&compare=equal&type=record&from=0&count=3";
  // parse the query   
  if (inquery==NULL || inquery[0]=='\0') {
    if (tdata->iscgi && tdata->method==POST_METHOD_CODE)
      return errhalt(CGI_QUERY_ERR,tdata);
    else return errhalt(NOQUERY_ERR,tdata);
  }   
  ql=strlen(inquery);  
  if (ql>MAXQUERYLEN) return errhalt(LONGQUERY_ERR,tdata); 
  if (tdata->isserver) query=inquery;
  else {
    strcpy((char*)querybuf,inquery);
    query=(char*)querybuf;    
  }  
  //fprintf(stderr, "query: %s\n", query);  
  pcount=parse_query(query,ql,params,values);    
  if (pcount<=0) return errhalt(MALFQUERY_ERR,tdata);
  
  //for(i=0;i<pcount;i++) {
  //  printf("param %s val %s\n",params[i],values[i]);
  //}  

  // query is now successfully parsed: 
  // find the database
#ifdef DEFAULT_DBASE
  database=DEFAULT_DBASE;
#endif  
  if ((tdata->global)->conf->default_dbase.used>0) 
    database=(tdata->global)->conf->default_dbase.vals[0];
  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],"db",MAXQUERYLEN)==0) {
      if ((values[i]!=NULL) && (values[i][0]!='\0')) {
        if (atoi(values[i])==0 && !(values[i][0]=='0' && values[i][1]=='\0')) {
          return errhalt(DB_PARAM_ERR,tdata);
        }        
        database=values[i];        
        break; 
      } 
    }  
  }  
  tdata->database=database;  
  // try to find jsonp  
  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],JSONP_PARAM,MAXQUERYLEN)==0) {      
      tdata->jsonp=values[i];
      break;
    }  
  }
  //find the operation and dispatch  
  found=0;
  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],"op",MAXQUERYLEN)==0) {
      if (!strncmp(values[i],"count",MAXQUERYLEN)){
        found=1;
        res=search(tdata,params,values,pcount,COUNT_CODE);
        break;
      } else if (!strncmp(values[i],"search",MAXQUERYLEN) || 
                 !strncmp(values[i],"select",MAXQUERYLEN)) {
        found=1;
        res=search(tdata,params,values,pcount,SEARCH_CODE);
        break;                 
      } else if (!strncmp(values[i],"insert",MAXQUERYLEN)) {
        found=1;
        res=insert(tdata,params,values,pcount);
        break; 
      } else if (!strncmp(values[i],"update",MAXQUERYLEN)) {
        found=1;
        res=search(tdata,params,values,pcount,UPDATE_CODE);
        break;
      } else if (!strncmp(values[i],"delete",MAXQUERYLEN)){
        found=1;
        res=search(tdata,params,values,pcount,DELETE_CODE);
        break;       
      } else if (!strncmp(values[i],"create",MAXQUERYLEN)) {
        found=1;
        res=create(tdata,params,values,pcount);
        break; 
      } else if (!strncmp(values[i],"drop",MAXQUERYLEN)) {
        found=1;
        res=drop(tdata,params,values,pcount);
        break;       
      } else {
        return errhalt(UNKNOWN_OP_ERR,tdata);
      }        
    } 
  }
  if (!found) return errhalt(NO_OP_ERR,tdata);
  if (tdata->isserver) {
    if (tdata->inbuf!=NULL) { free(tdata->inbuf); tdata->inbuf=NULL; }
    return res;
  } else {
    print_final(res,tdata);
    // freeing here is not really necessary and wastes time: process exits anyway
    if (tdata->inbuf!=NULL) free(tdata->inbuf);
    if (res!=NULL) free(res); 
    return NULL;  
  }  
}

void print_final(char* str, thread_data_p tdata) {
  if (str!=NULL) {
    if (tdata->isserver || tdata->iscgi) {
      printf(CONTENT_LENGTH,strlen(str)+1); //1 added for puts newline
      if (tdata->format) printf(JSON_CONTENT_TYPE); 
      else printf(CSV_CONTENT_TYPE);  
    }  
    puts(str);
  } else {    
    if (tdata->isserver || tdata->iscgi) {
      if (tdata->format) printf(JSON_CONTENT_TYPE); 
      else printf(CSV_CONTENT_TYPE); 
    }        
  }  
}

/* ============== operations: query parsing and handling ===================== */


/* search from the database, combined with update and delete */  
  
static char* search(thread_data_p tdata, char* inparams[], char* invalues[], 
             int incount, int opcode) {
  char* database=tdata->database;             
  char *token=NULL;             
  int i,j,x,itmp;
  wg_int type=0;
  char* fields[MAXPARAMS]; // search fields
  char* values[MAXPARAMS]; // search values
  char* compares[MAXPARAMS]; // search comparisons
  char* types[MAXPARAMS]; // search value types
  char* cids=NULL;             
  wg_int ids[MAXIDS];  // select these ids only         
  int fcount=0, vcount=0, ccount=0, tcount=0; // array el counters for above
  char* sfields[MAXPARAMS]; // set / selected fields
  char* svalues[MAXPARAMS]; // set field values
  char* stypes[MAXPARAMS];  // set field types  
  int sfcount; // array el counters for above              
  int from=0;             
  unsigned long count,rcount,gcount,handlecount;
  void* db=NULL; // actual database pointer
  void *rec, *oldrec; 
  char* res;
  wg_query *wgquery;  // query datastructure built later
  wg_query_arg wgargs[MAXPARAMS]; 
  wg_int lock_id=0;  // non-0 iff lock set
  int searchtype=0; // 0: full scan, 1: record ids, 2: by fields             
  char errbuf[ERRBUF_LEN]; // used for building variable-content input param error strings only               
  
  // default max nr of rows shown/handled
  if (opcode==COUNT_CODE) count=LONG_MAX;  
  else count=MAXCOUNT;
  // -------check and parse cgi parameters, attach database ------------
  // set params to defaults
  for(i=0;i<MAXPARAMS;i++) {
    fields[i]=NULL; values[i]=NULL; compares[i]=NULL; types[i]=NULL;
    sfields[i]=NULL; svalues[i]=NULL; stypes[i]=NULL;
  }
  // set printing params to defaults
  tdata->format=1; // 1: json
  tdata->maxdepth=MAX_DEPTH_DEFAULT; // rec depth limit for printer
  tdata->showid=0; // add record id as first extra elem: 0: no, 1: yes
  tdata->strenc=2; // string special chars escaping:  0: just ", 1: urlencode, 2: json, 3: csv
  // find search parameters
  for(i=0;i<incount;i++) {
    if (strncmp(inparams[i],"recids",MAXQUERYLEN)==0) {
      cids=invalues[i];         
      x=0;     
      // set ids to defaults
      for(j=0;j<MAXIDS;j++) ids[j]=0;
      // split csv int list to ids int array      
      for(j=0;j<strlen(cids);j++) {
        if (atoi(cids+j) && atoi(cids+j)>0) ids[x++]=atoi(cids+j);        
        if (x>=MAXIDS) break;
        for(;j<strlen(cids) && cids[j]!=','; j++) {};
      }             
    } else if (strncmp(inparams[i],"fld",MAXQUERYLEN)==0) {
      res=handle_fld_param(tdata,inparams[i],invalues[i],
                           &sfields[sfcount],&svalues[sfcount],&stypes[sfcount],sfcount,errbuf);
      if (res!=NULL) return res; // return error string
      sfcount++;     
    } else if (strncmp(inparams[i],"field",MAXQUERYLEN)==0) {
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
    } else {  
      // handle generic parameters for all queries: at end of param check
      res=handle_generic_param(tdata,inparams[i],invalues[i],&token,errbuf);      
      if (res!=NULL) return res;  // return error string
    }      
  }
  // authorization
  if (opcode==DELETE_CODE || opcode==UPDATE_CODE) {
    if (!authorize(WRITE_LEVEL,tdata,database,token))
      return errhalt(NOT_AUTHORIZED_ERR,tdata); 
  } else {  
    if (!authorize(READ_LEVEL,tdata,database,token))
      return errhalt(NOT_AUTHORIZED_ERR,tdata);
  }  
  // all parameters and values were understood 
  if (tdata->format==0) {
    // csv     
    tdata->maxdepth=0; // record structure not printed for csv
    tdata->strenc=3; // only " replaced with ""
  }  
  // check search parameters
  if (cids!=NULL) {
    // query by record ids
    if (fcount) return errhalt(RECIDS_COMBINED_ERR,tdata);
    searchtype=1;
  } else if (!fcount) {
    // no search fields given
    if (vcount || ccount || tcount) return errhalt(NO_FIELD_ERR,tdata);
    else searchtype=0; // scan everything
  } else {
    // search by fields
    searchtype=2;
  }    
  // attach to database
  db=op_attach_database(tdata,database,READ_LEVEL);
  if (!db) return errhalt(DB_ATTACH_ERR,tdata);   
  // database attached OK
  // create output string buffer (may be reallocated later)  
  tdata->buf=str_new(INITIAL_MALLOC);
  if (tdata->buf==NULL) return errhalt(MALLOC_ERR,tdata);
  tdata->bufsize=INITIAL_MALLOC;
  tdata->bufptr=tdata->buf; 
  // check printing depth
  if (tdata->maxdepth>MAX_DEPTH_HARD) tdata->maxdepth=MAX_DEPTH_HARD;  
  // initial print
  if(!op_print_data_start(tdata,opcode==SEARCH_CODE))
  return err_clear_detach_halt(MALLOC_ERR,tdata);
  // zero counters
  rcount=0;
  gcount=0;  
  handlecount=0; // actual nr of records handled
  // get lock
  if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only
  lock_id = wg_start_read(db); // get read lock
  tdata->lock_id=lock_id;
  tdata->lock_type=READ_LOCK_TYPE;
  if (!lock_id) return err_clear_detach_halt(LOCK_ERR,tdata);
  // handle one of the cases
  if (searchtype==0) {
    // ------- full scan case  ---     
    rec=wg_get_first_record(db);
    while (rec!=NULL) {    
      if (rcount>=from) {
        gcount++;
        if (gcount>count) break;  
        if (opcode==COUNT_CODE) { 
          handlecount++; 
        } else if (opcode==SEARCH_CODE) {
          itmp=op_print_record(tdata,rec,gcount);
          if (!itmp) return err_clear_detach_halt(MALLOC_ERR,tdata);
        } else if (opcode==UPDATE_CODE) {
          itmp=op_update_record(tdata,db,rec,0,0);
          if (!itmp) handlecount++;
        }
      }
      oldrec=rec;
      rec=wg_get_next_record(db,rec);
      if (opcode==DELETE_CODE) {
        x=wg_get_record_len(db,oldrec);
        if (x>0) {
          itmp=op_delete_record(tdata,oldrec);
          if (!itmp) handlecount++;
          //else err_clear_detach_halt(DELETE_ERR,tdata);
        }  
      } 
      rcount++;
    }   
  } else if (searchtype==1) {
    // ------------ search by record ids: ------------               
    for(j=0; ids[j]!=0 && j<MAXIDS; j++) {    
      x=wg_get_encoded_type(db,ids[j]);
      if (x!=WG_RECORDTYPE) continue;
      rec=wg_decode_record(db,ids[j]);    
      if (rec==NULL) continue;
      x=wg_get_record_len(db,rec);
      if (x<=0) continue;      
      gcount++;
      if (gcount>count) break; 
      if (opcode==COUNT_CODE) handlecount++;
      else if (opcode==SEARCH_CODE) {
        itmp=op_print_record(tdata,rec,gcount);
        if (!itmp) return err_clear_detach_halt(MALLOC_ERR,tdata);
      } else if (opcode==UPDATE_CODE) {
          itmp=op_update_record(tdata,db,rec,0,0);
          if (!itmp) handlecount++;         
      } else if (opcode==DELETE_CODE) {
        // test that db is not null, otherwise we may corrupt the database
        oldrec=wg_get_first_record(db);
        if (oldrec!=NULL) {
          //wg_int objecthead=dbfetch((void*)db,(void*)rec);
          //printf("isfreeobject %d\n",isfreeobject((int)objecthead));
          wg_print_record(db,rec);
          itmp=op_delete_record(tdata,rec);
          printf("deletion result %d\n",itmp);
          if (!itmp) handlecount++;
          //else return err_clear_detach_halt(DELETE_ERR,tdata);        
        }  
      }      
    }           
  } else if (searchtype==2) {
    // ------------by field search case: ---------

    // create a query list datastructure    
    for(i=0;i<fcount;i++) {   
      // field num    
      if (!isint(fields[i])) return err_clear_detach_halt(NO_FIELD_ERR,tdata);
      itmp=atoi(fields[i]);
      if(itmp<0) return err_clear_detach_halt(NO_FIELD_ERR,tdata);
      // column to compare
      wgargs[i].column = itmp;    
      // comparison op: default equal
      wgargs[i].cond = encode_incomp(db,compares[i]);
      if (wgargs[i].cond==BAD_WG_VALUE) return err_clear_detach_halt(COND_ERR,tdata);    
      // valuetype: default guess from value later
      type=encode_intype(db,types[i]); 
      if (type==BAD_WG_VALUE) return err_clear_detach_halt(INTYPE_ERR,tdata);
      // encode value to compare with   
      wgargs[i].value =  encode_invalue(db,values[i],type);        
      if (wgargs[i].value==WG_ILLEGAL) return err_clear_detach_halt(INTYPE_ERR,tdata);
    }   
    
    // make the query structure       
    wgquery = wg_make_query(db, NULL, 0, wgargs, i);
    if (!wgquery) return err_clear_detach_halt(QUERY_ERR,tdata);
    
    // actually perform the query           
    if (tdata->maxdepth>MAX_DEPTH_HARD) tdata->maxdepth=MAX_DEPTH_HARD;
    while((rec = wg_fetch(db, wgquery))) {
      if (rcount>=from) {
        gcount++;                           
        if (opcode==COUNT_CODE) handlecount++;
        else if (opcode==SEARCH_CODE) {
          itmp=op_print_record(tdata,rec,gcount);
          if (!itmp) return err_clear_detach_halt(MALLOC_ERR,tdata);
        } else if (opcode==UPDATE_CODE) {
          itmp=op_update_record(tdata,db,rec,0,0);
          if (!itmp) handlecount++;          
        } else if (opcode==DELETE_CODE) {
          itmp=op_delete_record(tdata,rec);
          if (!itmp) handlecount++;
          //else return err_clear_detach_halt(DELETE_ERR,tdata);  
        }
      }  
      rcount++;
      if (gcount>=count) break;    
    }   
    // free query datastructure, 
    for(i=0;i<fcount;i++) wg_free_query_param(db, wgargs[i].value);
    wg_free_query(db,wgquery); 
  }
  // ----- cases  handled  ------
  // print a single number for count and delete
  if (opcode==COUNT_CODE || opcode==DELETE_CODE) {
    if(!str_guarantee_space(tdata,MIN_STRLEN)) 
      return err_clear_detach_halt(MALLOC_ERR,tdata);
    itmp=snprintf(tdata->bufptr,MIN_STRLEN,"%lu",handlecount);    
    tdata->bufptr+=itmp;
  }
  // release locks and detach
  if (!wg_end_read(db, lock_id)) {  // release read lock
    return err_clear_detach_halt(LOCK_RELEASE_ERR,tdata);
  }
  tdata->lock_id=0;
  op_detach_database(tdata,db);
  if(!op_print_data_end(tdata,opcode==SEARCH_CODE))
    return err_clear_detach_halt(MALLOC_ERR,tdata);
  return tdata->buf;
}


// insert into the database */  
  
static char* insert(thread_data_p tdata, char* inparams[], char* invalues[], int incount) {
  char* database=tdata->database;
  char *token=NULL;
  int i,tmp;
  //int j,x;
  char* json=NULL;
  //int count=MAXCOUNT;
  void* db=NULL; // actual database pointer
  //void* rec; 
  char* res;
  wg_int lock_id=0;  // non-0 iff lock set  
  char errbuf[ERRBUF_LEN]; // used for building variable-content input param error strings only
  
  // yajl
  /*
  yajl_handle hand;
  yajl_gen g;  
  yajl_status stat;  
  size_t rd;  
  int retval = 0;  
  int a = 1;  
  */
  
  // -------check and parse cgi parameters, attach database ------------
  
  // set printing params to defaults
  tdata->format=1; // 1: json
  tdata->maxdepth=MAX_DEPTH_DEFAULT; // rec depth limit for printer
  tdata->showid=0; // add record id as first extra elem: 0: no, 1: yes
  tdata->strenc=2; // string special chars escaping:  0: just ", 1: urlencode, 2: json, 3: csv
  // find ids and display format parameters
  for(i=0;i<incount;i++) {
    if (strncmp(inparams[i],"rec",MAXQUERYLEN)==0) {
      json=invalues[i];   
    } else {  
      // handle generic parameters for all queries: at end of param check
      res=handle_generic_param(tdata,inparams[i],invalues[i],&token,errbuf);      
      if (res!=NULL) return res;  // return error string
    }    
  }
  if (json==NULL || strlen(json)==0) {
    return errhalt(MISSING_JSON_ERR,tdata);
  }
  // authorization
  if (!authorize(WRITE_LEVEL,tdata,database,token)) {
    return errhalt(NOT_AUTHORIZED_ERR,tdata);
  }  
  // all parameters and values were understood
  if (tdata->format==0) {
    // csv     
    tdata->maxdepth=0; // record structure not printed for csv
    tdata->strenc=3; // only " replaced with ""
  }  
  // attach to database
  db=op_attach_database(tdata,database,READ_LEVEL);
  if (!db) {
    if (!authorize(ADMIN_LEVEL,tdata,database,token)) {
      return errhalt(NOT_AUTHORIZED_INSERT_CREATE_ERR,tdata);
    } else { 
      if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only     
      db=op_create_database(tdata,database,0);
      if (!db) return err_clear_detach_halt(DB_CREATE_ERR,tdata);   
    }  
  }  
  // database attached OK
  // create output string buffer (may be reallocated later)
  tdata->buf=str_new(INITIAL_MALLOC);
  if (tdata->buf==NULL) return errhalt(MALLOC_ERR,tdata);
  tdata->bufsize=INITIAL_MALLOC;
  tdata->bufptr=tdata->buf;
  op_print_data_start(tdata,1);  
  
  // initialize yajl
  /*
  g = yajl_gen_alloc(NULL);  
  yajl_gen_config(g, yajl_gen_beautify, 0);  
  yajl_gen_config(g, yajl_gen_validate_utf8, 1);   
  hand = yajl_alloc(&callbacks, NULL, (void *) g);  
  yajl_config(hand, yajl_allow_comments, 1);  
  */
  
  // take a write lock
  if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only
  lock_id = wg_start_write(db); // get write lock
  tdata->lock_id=lock_id;
  tdata->lock_type=WRITE_LOCK_TYPE;
  if (!lock_id) return err_clear_detach_halt(LOCK_ERR,tdata);  
  
  // start parsing
  /*
  rd=strlen(json);  
  stat = yajl_parse(hand, json, rd);  
  if (stat==yajl_status_ok) stat=yajl_complete_parse(hand);    
  if (stat != yajl_status_ok) {  
    unsigned char * str = yajl_get_error(hand, 1, json, rd);  
    fprintf(stderr, "%s", (const char *) str);  
    yajl_free_error(hand, str);  
    retval = 1;  
  } else {
    // parse succeeded
    const unsigned char * buf;  
    size_t len;  
    yajl_gen_get_buf(g, &buf, &len);  
    fwrite(buf, 1, len, stdout);  
    yajl_gen_clear(g); 
  }    
  yajl_gen_free(g);  
  yajl_free(hand);
  */
  // parsing ended

  // parse json and insert
  tmp=wg_parse_json_document(db,json);
  if(tmp==-1) {
    return err_clear_detach_halt(JSON_ERR,tdata);
  } else if(tmp==-2) {
    return err_clear_detach_halt(INCONSISTENT_ERR,tdata);
  }
  if(!str_guarantee_space(tdata,MIN_STRLEN)) 
      return err_clear_detach_halt(MALLOC_ERR,tdata);
  strcpy(tdata->bufptr,"1");
  tdata->bufptr+=strlen("1");
  // end activity
  if (!wg_end_write(db, lock_id)) {  // release write lock
    return err_clear_detach_halt(LOCK_RELEASE_ERR,tdata);
  }
  tdata->lock_id=0;
  op_detach_database(tdata,db); 
  if(!op_print_data_end(tdata,1))
    return err_clear_detach_halt(MALLOC_ERR,tdata);    
  return tdata->buf;
}



// create a new database  
  
static char* create(thread_data_p tdata, char* inparams[], char* invalues[], int incount) {
  char* database=NULL;
  char *token=NULL;
  int i;
  void* db=NULL; // actual database pointer
  long size=0;
  long max_size=0;
  char *tmps,*res;
  char errbuf[ERRBUF_LEN];  
  
  // find and check parameters
  for(i=0;i<incount;i++) {
    if (strncmp(inparams[i],"db",MAXQUERYLEN)==0) {
      database=invalues[i];
      if(database==NULL || strlen(database)<1 || atoi(database)<=0) {
        return errhalt(DB_NAME_ERR,tdata);
      }
    } else if (strncmp(inparams[i],"size",MAXQUERYLEN)==0) {
      tmps=invalues[i];   
      size=atoi(tmps);
      if(tmps<=0) return errhalt(DB_NO_SIZE_ERR,tdata); 
#ifdef MAX_DATABASE_SIZE
      max_size=MAX_DATABASE_SIZE;
#else
      max_size=LONG_MAX;  
#endif
      if ((tdata->global)->conf->max_dbase_size.used>0) {
        tmps=(tdata->global)->conf->max_dbase_size.vals[0];
        max_size=atol(tmps);
      }  
      if(size>max_size) return errhalt(DB_BIG_SIZE_ERR,tdata);       
    }
    else {  
      // handle generic parameters for all queries: at end of param check
      res=handle_generic_param(tdata,inparams[i],invalues[i],&token,errbuf);      
      if (res!=NULL) return res;  // return error string
    }    
  }  
  // authorization
  if (!authorize(ADMIN_LEVEL,tdata,database,token)) {
    return errhalt(NOT_AUTHORIZED_ERR,tdata);
  }  
  // all parameters and values were understood
  // create output string buffer (may be reallocated later)
  tdata->buf=str_new(INITIAL_MALLOC);
  if (tdata->buf==NULL) return errhalt(MALLOC_ERR,tdata);
  tdata->bufsize=INITIAL_MALLOC;
  tdata->bufptr=tdata->buf;
  op_print_data_start(tdata,0);      
  // indicate no lock
  if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only
  tdata->lock_id=0;
  // check and create database
  //db=wg_attach_existing_database(database);
  //if (db!=NULL) return errhalt(DB_EXISTS_ALREADY_ERR,tdata);      
  db=op_create_database(tdata,database,size);
  if (db==NULL) return errhalt(DB_CREATE_ERR,tdata); 
  tdata->db=db;
  // created successfully 
  if(!str_guarantee_space(tdata,MIN_STRLEN)) 
      return err_clear_detach_halt(MALLOC_ERR,tdata);
  strcpy(tdata->bufptr,"1");
  tdata->bufptr+=strlen("1");
  // end activity
  op_detach_database(tdata,db);
  if(!op_print_data_end(tdata,0))
    return err_clear_detach_halt(MALLOC_ERR,tdata);    
  return tdata->buf;
}

// drop a database  
  
static char* drop(thread_data_p tdata, char* inparams[], char* invalues[], int incount) {
  char* database=tdata->database;
  char *token=NULL;
  int i,tmp;
  void* db=NULL; // actual database pointer
  char *res;
  int lock_id=0;
  int found=0;
  char errbuf[ERRBUF_LEN];  
  
  // find and check parameters
  for(i=0;i<incount;i++) {
    if (strncmp(inparams[i],"db",MAXQUERYLEN)==0) {
      database=invalues[i];
      if(database==NULL || strlen(database)<1 || atoi(database)<=0) {
        return errhalt(DB_NAME_ERR,tdata);
      }
    } else {  
      // handle generic parameters for all queries: at end of param check
      res=handle_generic_param(tdata,inparams[i],invalues[i],&token,errbuf);      
      if (res!=NULL) return res;  // return error string
    }    
  }  
  // authorization
  if (!authorize(ADMIN_LEVEL,tdata,database,token)) {
    return errhalt(NOT_AUTHORIZED_ERR,tdata);
  }  
  // all parameters and values were understood
  // create output string buffer (may be reallocated later)
  tdata->buf=str_new(INITIAL_MALLOC);
  if (tdata->buf==NULL) return errhalt(MALLOC_ERR,tdata);
  tdata->bufsize=INITIAL_MALLOC;
  tdata->bufptr=tdata->buf;
  op_print_data_start(tdata,0);
  // check if access allowed in the conf file
  if (database!=NULL && (tdata->global)->conf->dbases.used>0) {
    for(i=0;i<(tdata->global)->conf->dbases.used;i++) {
      if (!strcmp(database,(tdata->global)->conf->dbases.vals[i])) {
        found=1;
        break;
      }
    }
    if (!found) return errhalt(DB_AUTHORIZE_ERR,tdata);
  }
  // first try to attach to an existing database 
  db=op_attach_database(tdata,database,ADMIN_LEVEL);
  if (db==NULL) {
    return errhalt(DB_NOT_EXISTS_ERR,tdata);
  } else {  
    // database exists, take lock
    if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only
    tdata->db=db;
    lock_id = wg_start_write(db); // get write lock
    tdata->lock_id=lock_id;
    tdata->lock_type=WRITE_LOCK_TYPE;
    if (!lock_id) return err_clear_detach_halt(LOCK_ERR,tdata); 
    tmp=wg_detach_database(db); // detaches a database: returns 0 if OK
    if (tmp) return err_clear_detach_halt(DB_DROP_ERR,tdata);
    tmp=wg_delete_database(database);    
    if (tmp) return errhalt(DB_DROP_ERR,tdata);
  }  
  // deleted successfully 
  tdata->db=NULL;
  tdata->lock_id=0;
  if(!str_guarantee_space(tdata,MIN_STRLEN)) 
      return err_clear_detach_halt(MALLOC_ERR,tdata);
  strcpy(tdata->bufptr,"1");
  tdata->bufptr+=strlen("1");
  // end activity
  if(!op_print_data_end(tdata,0))
    return errhalt(MALLOC_ERR,tdata);    
  return tdata->buf;
}

/* ***** print, delete, update utilities ****** */

// print a single record to output string buffer of tdata
// return 1 if ok, 0 if fails

static int op_print_record(thread_data_p tdata,void* rec,int gcount) {  
  int res;
  
  if (!str_guarantee_space(tdata,MIN_STRLEN)) return 0;
  if (gcount>1 && tdata->format!=0) {
    // json and not first row
    snprintf(tdata->bufptr,MIN_STRLEN,",\n");
    tdata->bufptr+=2;           
  }                    
  res=sprint_record(tdata->db,rec,tdata);
  if (!res) return 0;
  if (tdata->format==0) {
    // csv
    if(!str_guarantee_space(tdata,MIN_STRLEN)) return 0;
    snprintf(tdata->bufptr,MIN_STRLEN,"\r\n");
    tdata->bufptr+=2;
  }
  return 1;
}  

// delete a record
// return 0 if ok, 1 if fails (contrary to print above)

static int op_delete_record(thread_data_p tdata,void* rec) {
  return wg_delete_record(tdata->db,rec);
}

/* ******** query preparation and ending utilities ******** */


// return NULL if parsing ok, errstr otherwise

static char* handle_generic_param(thread_data_p tdata,char* key,char* value,
                                  char** token,char* errbuf) {

  if (key==NULL || value==NULL) { 
    return NULL; 
  } else if (strncmp(key,"depth",MAXQUERYLEN)==0) {      
    tdata->maxdepth=atoi(value);
  } else if (strncmp(key,"showid",MAXQUERYLEN)==0) {      
    if (strncmp(value,"yes",MAXQUERYLEN)==0) tdata->showid=1;            
    else if (strncmp(value,"no",MAXQUERYLEN)==0) tdata->showid=0;
    else {
      snprintf(errbuf,ERRBUF_LEN,UNKNOWN_PARAM_VALUE_ERR,value,key);
      return errhalt(errbuf,tdata);
    }                                                                        
  } else if (strncmp(key,"format",MAXQUERYLEN)==0) {      
    if (strncmp(value,"csv",MAXQUERYLEN)==0) tdata->format=0;
    else if (strncmp(value,"json",MAXQUERYLEN)==0) tdata->format=1;
    else {
      snprintf(errbuf,ERRBUF_LEN,UNKNOWN_PARAM_VALUE_ERR,value,key);
      return errhalt(errbuf,tdata);
    }
  } else if (strncmp(key,"escape",MAXQUERYLEN)==0) {      
    if (strncmp(value,"no",MAXQUERYLEN)==0) tdata->strenc=0;
    else if (strncmp(value,"url",MAXQUERYLEN)==0) tdata->strenc=1;
    else if (strncmp(value,"json",MAXQUERYLEN)==0) tdata->strenc=2;
    else {
      snprintf(errbuf,ERRBUF_LEN,UNKNOWN_PARAM_VALUE_ERR,value,key);
      return errhalt(errbuf,tdata);
    }            
  } else if (strncmp(key,"token",MAXQUERYLEN)==0) {
    *token=value;   
  } else if (strncmp(key,JSONP_PARAM,MAXQUERYLEN)==0) {
    //tdata->jsonp=value;
  } else if (strncmp(key,NOACTION_PARAM,MAXQUERYLEN)==0) {
    // correct parameter, no action here       
  } else if (strncmp(key,"db",MAXQUERYLEN)==0) {
    // correct parameter, no action here     
  } else if (strncmp(key,"op",MAXQUERYLEN)==0) {
    // correct parameter, no action here  
  } else {
    // incorrect/unrecognized parameter
#ifdef ALLOW_UNKNOWN_PARAMS      
#else          
    snprintf(errbuf,ERRBUF_LEN,UNKNOWN_PARAM_ERR,key);
    return errhalt(errbuf,tdata);
#endif      
  }
  return NULL;
}


static char* handle_fld_param(thread_data_p tdata,char* key,char* value,
              char** sfields, char** svalues, char** stypes, int sfcount, char* errbuf) {
                
  if (key==NULL || value==NULL) {
    return NULL;
  }
  *sfields=NULL;
  *svalues=NULL;
  *stypes=NULL;
  return NULL;
}

// call to print output start
// return 1 if successful, 0 if fails

static int op_print_data_start(thread_data_p tdata, int listflag) {
  int itmp;
  
  if(!str_guarantee_space(tdata,MIN_STRLEN)) return 0;
  if (tdata->format!=0) {
    // json
    if (tdata->jsonp!=NULL) {
      if (listflag) itmp=snprintf(tdata->bufptr,MIN_STRLEN,"%s([\n",tdata->jsonp);
      else itmp=snprintf(tdata->bufptr,MIN_STRLEN,"%s(",tdata->jsonp);
      tdata->bufptr+=itmp;
    } else {
      if (listflag) { 
        itmp=snprintf(tdata->bufptr,MIN_STRLEN,"[\n");      
        tdata->bufptr+=itmp;
      }  
    }  
  } 
  return 1;  
}

// call just before finishing an operation to finish output
// return 1 if successful, 0 if fails

static int op_print_data_end(thread_data_p tdata, int listflag) {
  int itmp;

  if(!str_guarantee_space(tdata,MIN_STRLEN)) return 0; 
  if (tdata->format!=0) {
    // json
    if (tdata->jsonp!=NULL) {
      if (listflag) itmp=snprintf(tdata->bufptr,MIN_STRLEN,"\n]);");
      else itmp=snprintf(tdata->bufptr,MIN_STRLEN,");");
      tdata->bufptr+=itmp;
    } else {
      if (listflag) {
        itmp=snprintf(tdata->bufptr,MIN_STRLEN,"\n]");
        tdata->bufptr+=itmp;
      }  
    }     
  } 
  return 1;  
} 


// update a record

static int op_update_record(thread_data_p tdata,void* db, void* rec, wg_int fld, wg_int value) {

  wg_set_int_field(db,rec,fld,value);
  return 0;
}  


// create a new database 

static void* op_create_database(thread_data_p tdata,char* database,long size) {
  void* db;
  char* sizestr;
  long max_size=0;
  int i;
  int found=0;

  //printf("op_create_database called\n"); 
  if (database==NULL) {
#ifdef DEFAULT_DATABASE
    database=DEFAULT_DATABASE;
#endif
    if ((tdata->global)->conf->default_dbase.used>0)
      database=(tdata->global)->conf->default_dbase.vals[0];
  }
  if (size<=0) {
#ifdef DEFAULT_DATABASE_SIZE
    size=DEFAULT_DATABASE_SIZE;
#endif
    if ((tdata->global)->conf->default_dbase_size.used>0) {
      sizestr=(tdata->global)->conf->default_dbase_size.vals[0];
      size=atol(sizestr);
    }
  }
#ifdef MAX_DATABASE_SIZE
  max_size=MAX_DATABASE_SIZE;
#else
  max_size=LONG_MAX;  
#endif
  if ((tdata->global)->conf->max_dbase_size.used>0) {
    sizestr=(tdata->global)->conf->max_dbase_size.vals[0];
    max_size=atol(sizestr);
  }
  if (database==NULL) return NULL;
  // check if access allowed in the conf file
  if ((tdata->global)->conf->dbases.used>0) {
    for(i=0;i<(tdata->global)->conf->dbases.used;i++) {
      if (!strcmp(database,(tdata->global)->conf->dbases.vals[i])) {
        found=1;
        break;
      }
    }
    if (!found) return NULL;
  }    
  if (size<=0) return NULL;
  if (size>max_size) return NULL;
  db = wg_attach_database(database,size);
  return db;
}

// attach to database

void* op_attach_database(thread_data_p tdata,char* database,int accesslevel) {
  void* db;
  int i;
  int found=0;
  
  if (database==NULL) {
    //use default
#ifdef DEFAULT_DATABASE    
    database=DEFAULT_DATABASE;
#endif    
    if ((tdata->global)->conf->default_dbase.used>0) {
      database=(tdata->global)->conf->default_dbase.vals[0];      
    }
    if (database==NULL) return NULL;
  } 
  // check if access allowed in the conf file
  if ((tdata->global)->conf->dbases.used>0) {
    for(i=0;i<(tdata->global)->conf->dbases.used;i++) {
      if (!strcmp(database,(tdata->global)->conf->dbases.vals[i])) {
        found=1;
        break;
      }
    }
    if (!found) return NULL;
  }

#if _MSC_VER
  db = tdata->db;
#else
  db = wg_attach_existing_database(database);
  //db = wg_attach_database(database,100000000);
  tdata->db=db;
#endif
  return db;
}

// detach database

int op_detach_database(thread_data_p tdata, void* db) {
  
#if _MSC_VER
#else
  if (db!=NULL) wg_detach_database(db);
  tdata->db=NULL;
#endif
  return 0;
}
