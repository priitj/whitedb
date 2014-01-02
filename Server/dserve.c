/*

dserve.c contains the main functionality of dserve:

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http. Results are given in the json format.

Run dserver in one of three ways:

* a cgi program under a web server, connecting like
  http://myhost.com/dserve?op=search&from=0&count=5  
* as a standalone http(s) server, passing a port number as a single argument, like
  dserve 8080
  and connecting like
  http://localhost:8080/dserve?op=search&from=0&count=5
  or, for dservehttps compiled with USE_OPENSSL
  dservehttps 8080
  https://localhost:8080/dserve?op=search&from=0&count=5
* from the command line, passing a cgi-format, urlencoded query string
  as a single argument, like
  dserve 'op=search&from=0&count=5'
  
dserve does not require additional libraries except wgdb and if compiled
for the server mode, also pthreads:

gcc dserve.c dserve_util.c dserve_net.c  -o dserve -O2 -lwgdb -lpthread
gcc -DUSE_OPENSSL dserve.c dserve_util.c dserve_net.c  -o dservehttps -O2 -lwgdb -lpthread -lssl -lcrypto
or, after removing #define SERVEROPTION from dserve.h:
gcc dserve.c dserve_util.c -o dserve -O2 -lwgdb

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

#include "dserve.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h> // for alarm and termination signal handling
#if _MSC_VER   // no alarm on windows
#else
#include <unistd.h> // for alarm
#endif

/* =============== local protos =================== */

void setup_globals(void);
char* search(struct thread_data * tdata, char* inparams[], char* invalues[], int count, int* hformat);
char* recids(struct thread_data * tdata, char* inparams[], char* invalues[], int incount, int* hformat);

/* =============== globals =================== */

// dsdata points to a struct containing the nr of threads and 
// an array of separate data blocks for each thread: global ptr
// guarantees access from the signal-called termination_handler

struct dserve_global * dsdata;

/* =============== main =================== */

int main(int argc, char **argv) {
  char *inquery=NULL, *inip=NULL, *inpath=NULL;  
  int port=0, cgi=0;  
  struct thread_data * tdata;

  setup_globals(); // set dsdata and its components
  // Set up abnormal termination handler to clear locks  
  signal(SIGSEGV,termination_handler);
  signal(SIGFPE,termination_handler);
  signal(SIGABRT,termination_handler);
  signal(SIGTERM,termination_handler);  
  signal(SIGINT,termination_handler);
  signal(SIGILL,termination_handler);  
#if _MSC_VER // some signals not used in windows
#else     
  signal(SIGPIPE,SIG_IGN); // important for TCP/IP handling 
#endif    
  // detect calling parameters 
  // process environment and args
  inquery=getenv("QUERY_STRING");
  if (inquery!=NULL) {
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
        load_configuration(inpath,dsdata->conf);
        //print_conf(dsdata->conf);
      }
      run_server(port);
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
      load_configuration(inpath,dsdata->conf);
      //print_conf(dsdata->conf);
    }  
    // setup a single tdata block
    dsdata->maxthreads=1;
    tdata=&(dsdata->threads_data[0]);
    tdata->isserver=0;
    tdata->iscgi=cgi;
    tdata->ip=inip;
    tdata->realthread=0;
    tdata->format=1;
    process_query(inquery,tdata);
    return 0;
  }    
  return 0;
}  
  
void setup_globals(void) {
  int i;
  
  // set up global data
  dsdata=malloc(sizeof(struct dserve_global));
  if (dsdata==NULL) {errprint(CANNOT_ALLOC_ERR,NULL); exit(-1);}
  dsdata->conf=malloc(sizeof(struct dserve_conf));
  if (dsdata->conf==NULL) {errprint(CANNOT_ALLOC_ERR,NULL); exit(-1);}
  dsdata->maxthreads=MAX_THREADS;
  for(i=0;i<dsdata->maxthreads;i++) {
    dsdata->threads_data[i].db=NULL;
    dsdata->threads_data[i].inuse=0;
  }
  dsdata->conf->default_dbase.size=0;
  dsdata->conf->dbases.size=0;
  dsdata->conf->admin_ips.size=0;
  dsdata->conf->write_ips.size=0;
  dsdata->conf->read_ips.size=0;
  dsdata->conf->admin_tokens.size=0;
  dsdata->conf->write_tokens.size=0;
  dsdata->conf->read_tokens.size=0;
  dsdata->conf->default_dbase.used=0;
  dsdata->conf->dbases.used=0;
  dsdata->conf->admin_ips.used=0;
  dsdata->conf->write_ips.used=0;
  dsdata->conf->read_ips.used=0;
  dsdata->conf->admin_tokens.used=0;
  dsdata->conf->write_tokens.used=0;
  dsdata->conf->read_tokens.used=0;  
}
  
char* process_query(char* inquery, struct thread_data * tdata) {  
  int i=0;
  char *query;
  char querybuf[MAXQUERYLEN];  
  int pcount=0;
  int ql,found;  
  char* res=NULL;
  char* database=DEFAULT_DATABASE;
  char* params[MAXPARAMS];
  char* values[MAXPARAMS];
  int hformat=1; // for header 0: csv, 1: json: reset later after reading params
  
  // or use your own query string for testing a la
  // inquery="db=1000&op=search&field=1&value=2&compare=equal&type=record&from=0&count=3";
  // parse the query 
  
  if (inquery==NULL || inquery[0]=='\0') return errhalt(NOQUERY_ERR,tdata);
  ql=strlen(inquery);  
  if (ql>MAXQUERYLEN) return errhalt(LONGQUERY_ERR,tdata); 
  if (tdata->isserver) query=inquery;
  else {
    strcpy((char*)querybuf,inquery);
    query=(char*)querybuf;    
  }  
  //printf("query: %s\n",query);
    
  pcount=parse_query(query,ql,params,values);    
  if (pcount<=0) return errhalt(MALFQUERY_ERR,tdata);
  
  //for(i=0;i<pcount;i++) {
  //  printf("param %s val %s\n",params[i],values[i]);
  //}  
  
  // query is now successfully parsed: find the database
  
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
  //find the operation and dispatch  
  found=0;
  for(i=0;i<pcount;i++) {
    if (strncmp(params[i],"op",MAXQUERYLEN)==0) {
      if (strncmp(values[i],"search",MAXQUERYLEN)==0) {
        found=1;
        res=search(tdata,params,values,pcount,&hformat);
        // here the locks should be freed and database detached
        break;
      } else if (strncmp(values[i],"recids",MAXQUERYLEN)==0) {
        found=1;
        res=recids(tdata,params,values,pcount,&hformat);
        // here the locks should be freed and database detached
        break;  
      } else {
        return errhalt(UNKNOWN_OP_ERR,tdata);
      }        
    }  
  }
  if (!found) errhalt(NO_OP_ERR,tdata);
  if (tdata->isserver) {
    return res;
  } else {
    print_final(res,tdata);
    if (res!=NULL) free(res); // not really necessary and wastes time: process exits 
    return NULL;  
  }  
}

void print_final(char* str, struct thread_data * tdata) {
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

/* ============== query parsing and handling ===================== */


/* first possible query operation: search from the database */  
  
char* search(struct thread_data * tdata, char* inparams[], char* invalues[], 
             int incount, int* hformat) {
  char* database=tdata->database;             
  char* token=NULL;             
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
  char errbuf[200]; // used for building variable-content input param error strings only               
    
  // -------check and parse cgi parameters, attach database ------------
  // set params to defaults
  for(i=0;i<MAXPARAMS;i++) {
    fields[i]=NULL; values[i]=NULL; compares[i]=NULL; types[i]=NULL;
  }
  // set printing params to defaults
  tdata->format=1; // 1: json
  tdata->maxdepth=MAX_DEPTH_DEFAULT; // rec depth limit for printer
  tdata->showid=0; // add record id as first extra elem: 0: no, 1: yes
  tdata->strenc=2; // string special chars escaping:  0: just ", 1: urlencode, 2: json, 3: csv
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
      tdata->maxdepth=atoi(invalues[i]);
    } else if (strncmp(inparams[i],"showid",MAXQUERYLEN)==0) {      
      if (strncmp(invalues[i],"yes",MAXQUERYLEN)==0) tdata->showid=1;            
      else if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) tdata->showid=0;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        return errhalt(errbuf,tdata);
      }
    } else if (strncmp(inparams[i],"format",MAXQUERYLEN)==0) {      
      if (strncmp(invalues[i],"csv",MAXQUERYLEN)==0) tdata->format=0;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) tdata->format=1;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        return errhalt(errbuf,tdata);
      }
    } else if (strncmp(inparams[i],"escape",MAXQUERYLEN)==0) {      
      if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) tdata->strenc=0;
      else if (strncmp(invalues[i],"url",MAXQUERYLEN)==0) tdata->strenc=1;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) tdata->strenc=2;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        return errhalt(errbuf,tdata);
      }      
    } else if (strncmp(inparams[i],"token",MAXQUERYLEN)==0) {
      token=invalues[i];  
    } else if (strncmp(inparams[i],"db",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else if (strncmp(inparams[i],"op",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else {
      // incorrect/unrecognized parameter
      snprintf(errbuf,100,UNKNOWN_PARAM_ERR,inparams[i]);
      return errhalt(errbuf,tdata);
    }
  }
  // authorization
  if (!authorize(READ_LEVEL,dsdata->conf,tdata,token)) {
    return errhalt(NOT_AUTHORIZED_ERR,tdata);
  }  
  // all parameters and values were understood 
  if (tdata->format==0) {
    // csv     
    tdata->maxdepth=0; // record structure not printed for csv
    tdata->strenc=3; // only " replaced with ""
    *hformat=0; // store to caller for content-type header
  }  
  // check search parameters
  if (!fcount) {
    if (vcount || ccount || tcount) return errhalt(NO_FIELD_ERR,tdata);
    else nosearch=1;
  }    
  // attach to database
  //printf("trying to attach database %s\n",database);
#if _MSC_VER
  db = tdata->db;
#else    
  db = wg_attach_existing_database(database); 
  //db = wg_attach_database(database,100000000);  
  tdata->db=db;
#endif     
  if (!db) return errhalt(DB_ATTACH_ERR,tdata);
  res=malloc(res_size);
  if (!res) { 
    err_clear_detach_halt(MALLOC_ERR);
  } 
  // database attached OK
  // create output string buffer (may be reallocated later)
  
  tdata->buf=str_new(INITIAL_MALLOC);
  tdata->bufsize=INITIAL_MALLOC;
  tdata->bufptr=tdata->buf;
  if (nosearch) {
    // ------- special case without real search: just output records ---    
    gcount=0;
    rcount=0;
    if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only
    lock_id = wg_start_read(db); // get read lock
    tdata->lock_id=lock_id;
    tdata->lock_type=READ_LOCK_TYPE;
    if (!lock_id) err_clear_detach_halt(LOCK_ERR);
    str_guarantee_space(tdata,MIN_STRLEN);
    if (tdata->format!=0) {
      // json
      snprintf(tdata->bufptr,MIN_STRLEN,"[\n");
      tdata->bufptr+=2;
    }  
    if (tdata->maxdepth>MAX_DEPTH_HARD) tdata->maxdepth=MAX_DEPTH_HARD;
    rec=wg_get_first_record(db);
    
    //printf("lock %d\n",tdata->lock_id);
    //gcount=2/rcount;
    //raise(SIGINT );
    //wait(1000);
    
    do {    
      if (rcount>=from) {
        gcount++;
        if (gcount>count) break;
        str_guarantee_space(tdata,MIN_STRLEN); 
        if (gcount>1 && tdata->format!=0) {
          // json and not first row
          snprintf(tdata->bufptr,MIN_STRLEN,",\n");
          tdata->bufptr+=2;           
        }                    
        sprint_record(db,rec,tdata);
        if (tdata->format==0) {
          // csv
          str_guarantee_space(tdata,MIN_STRLEN);
          snprintf(tdata->bufptr,MIN_STRLEN,"\r\n");
          tdata->bufptr+=2;
        } 
      }
      rec=wg_get_next_record(db,rec);
      rcount++;
    } while(rec!=NULL);       
    if (!wg_end_read(db, lock_id)) {  // release read lock
      err_clear_detach_halt(LOCK_RELEASE_ERR);
    }
    tdata->lock_id=0;
#if _MSC_VER
#else    
    itmp=wg_detach_database(db); 
#endif    
    tdata->db=NULL;
    str_guarantee_space(tdata,MIN_STRLEN); 
    if (tdata->format!=0) {
      // json
      snprintf(tdata->bufptr,MIN_STRLEN,"\n]");
      tdata->bufptr+=3;
    }  
    return tdata->buf;
  }  
  
  // ------------ normal search case: ---------
  
  // create a query list datastructure
  
  for(i=0;i<fcount;i++) {   
    // field num    
    if (!isint(fields[i])) err_clear_detach_halt(NO_FIELD_ERR);
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
  tdata->lock_id=lock_id;
  tdata->lock_type=READ_LOCK_TYPE;
  if (!lock_id) err_clear_detach_halt(LOCK_ERR);
  wgquery = wg_make_query(db, NULL, 0, wgargs, i);
  if (!wgquery) err_clear_detach_halt(QUERY_ERR);
  
  // actually perform the query
  
  rcount=0;
  gcount=0;
  str_guarantee_space(tdata,MIN_STRLEN);
  if (tdata->format!=0) {
    // json
    snprintf(tdata->bufptr,MIN_STRLEN,"[\n");
    tdata->bufptr+=2;
  }  
  if (tdata->maxdepth>MAX_DEPTH_HARD) tdata->maxdepth=MAX_DEPTH_HARD;
  printf("cp0\n");
  while((rec = wg_fetch(db, wgquery))) {
    if (rcount>=from) {
      gcount++;
      str_guarantee_space(tdata,MIN_STRLEN); 
      if (gcount>1 && tdata->format!=0) {
        // json and not first row
        snprintf(tdata->bufptr,MIN_STRLEN,",\n");
        tdata->bufptr+=2;           
      }                    
      sprint_record(db,rec,tdata);
      if (tdata->format==0) {
        // csv
        str_guarantee_space(tdata,MIN_STRLEN);
        snprintf(tdata->bufptr,MIN_STRLEN,"\r\n");
        tdata->bufptr+=2;
      }      
    }  
    rcount++;
    if (gcount>=count) break;    
  }   
  // free query datastructure, release lock, detach
  printf("cp1\n");
  for(i=0;i<fcount;i++) wg_free_query_param(db, wgargs[i].value);
  wg_free_query(db,wgquery); 
  if (!wg_end_read(db, lock_id)) {  // release read lock
    err_clear_detach_halt(LOCK_RELEASE_ERR);
  }
  tdata->lock_id=0;
#if _MSC_VER
#else    
  itmp=wg_detach_database(db); 
#endif   
  tdata->db=NULL;
  str_guarantee_space(tdata,MIN_STRLEN); 
  if (tdata->format!=0) {
    // json
    snprintf(tdata->bufptr,MIN_STRLEN,"\n]");
    tdata->bufptr+=3;
  }  
  return tdata->buf;
}


/* second possible query operation: get concrete records by ids from the database */  
  
char* recids(struct thread_data * tdata, char* inparams[], char* invalues[], int incount, int* hformat) {
  char* database=tdata->database;
  char* token;
  int i,j,x,gcount;
  char* cids;
  wg_int ids[MAXIDS];
  int count=MAXCOUNT;
  void* db=NULL; // actual database pointer
  void* rec; 
  char* res;
  int res_size=INITIAL_MALLOC;
  wg_int lock_id=0;  // non-0 iff lock set  
  char errbuf[200]; // used for building variable-content input param error strings only
  
  // -------check and parse cgi parameters, attach database ------------
  
  // set ids to defaults
  for(i=0;i<MAXIDS;i++) {
    ids[i]=0; 
  }
  // set printing params to defaults
  tdata->format=1; // 1: json
  tdata->maxdepth=MAX_DEPTH_DEFAULT; // rec depth limit for printer
  tdata->showid=0; // add record id as first extra elem: 0: no, 1: yes
  tdata->strenc=2; // string special chars escaping:  0: just ", 1: urlencode, 2: json, 3: csv
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
      tdata->maxdepth=atoi(invalues[i]);
    } else if (strncmp(inparams[i],"showid",MAXQUERYLEN)==0) {      
      if (strncmp(invalues[i],"yes",MAXQUERYLEN)==0) tdata->showid=1;            
      else if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) tdata->showid=0;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        return errhalt(errbuf,tdata);
      }
    } else if (strncmp(inparams[i],"format",MAXQUERYLEN)==0) {      
      if (strncmp(invalues[i],"csv",MAXQUERYLEN)==0) tdata->format=0;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) tdata->format=1;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        return errhalt(errbuf,tdata);
      }
    } else if (strncmp(inparams[i],"escape",MAXQUERYLEN)==0) {      
      if (strncmp(invalues[i],"no",MAXQUERYLEN)==0) tdata->strenc=0;
      else if (strncmp(invalues[i],"url",MAXQUERYLEN)==0) tdata->strenc=1;
      else if (strncmp(invalues[i],"json",MAXQUERYLEN)==0) tdata->strenc=2;
      else {
        snprintf(errbuf,100,UNKNOWN_PARAM_VALUE_ERR,invalues[i],inparams[i]);
        return errhalt(errbuf,tdata);
      }      
    } else if (strncmp(inparams[i],"token",MAXQUERYLEN)==0) {
      token=invalues[i];    
    } else if (strncmp(inparams[i],"db",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else if (strncmp(inparams[i],"op",MAXQUERYLEN)==0) {
      // correct parameter, no action here
    } else {
      // incorrect/unrecognized parameter
      snprintf(errbuf,100,UNKNOWN_PARAM_ERR,inparams[i]);
      return errhalt(errbuf,tdata);
    }
  }
  // authorization
  if (!authorize(READ_LEVEL,dsdata->conf,tdata,token)) {
    return errhalt(NOT_AUTHORIZED_ERR,tdata);
  }  
  // all parameters and values were understood
  if (tdata->format==0) {
    // csv     
    tdata->maxdepth=0; // record structure not printed for csv
    tdata->strenc=3; // only " replaced with ""
    *hformat=0; // store to caller for content-type header
  }  
  // attach to database
  db = wg_attach_existing_database(database);
  tdata->db=db;
  if (!db) return errhalt(DB_ATTACH_ERR,tdata);
  res=malloc(res_size);
  if (!res) { 
    err_clear_detach_halt(MALLOC_ERR);
  } 
  // database attached OK
  // create output string buffer (may be reallocated later)
  
  tdata->buf=str_new(INITIAL_MALLOC);
  tdata->bufsize=INITIAL_MALLOC;
  tdata->bufptr=tdata->buf;

  // take a read lock and loop over ids  
  gcount=0;
  if (tdata->realthread && tdata->common->shutdown) return NULL; // for multithreading only
  lock_id = wg_start_read(db); // get read lock
  tdata->lock_id=lock_id;
  tdata->lock_type=READ_LOCK_TYPE;
  if (!lock_id) err_clear_detach_halt(LOCK_ERR);
  str_guarantee_space(tdata,MIN_STRLEN);
  if (tdata->format!=0) {
    // json
    snprintf(tdata->bufptr,MIN_STRLEN,"[\n");
    tdata->bufptr+=2;
  }  
  if (tdata->maxdepth>MAX_DEPTH_HARD) tdata->maxdepth=MAX_DEPTH_HARD;
  for(j=0; ids[j]!=0 && j<MAXIDS; j++) {
    
    x=wg_get_encoded_type(db,ids[j]);
    if (x!=WG_RECORDTYPE) continue;
    rec=wg_decode_record(db,ids[j]);    
    if (rec==NULL) continue;
    x=wg_get_record_len(db,rec);
    if (x<=0) continue;
    
    gcount++;
    if (gcount>count) break;
    str_guarantee_space(tdata,MIN_STRLEN); 
    if (gcount>1 && tdata->format!=0) {
      // json and not first row
      snprintf(tdata->bufptr,MIN_STRLEN,",\n");
      tdata->bufptr+=2;           
    }                    
    sprint_record(db,rec,tdata);
    if (tdata->format==0) {
      // csv
      str_guarantee_space(tdata,MIN_STRLEN);
      snprintf(tdata->bufptr,MIN_STRLEN,"\r\n");
      tdata->bufptr+=2;
    }
    rec=wg_get_next_record(db,rec);
  }      
  if (!wg_end_read(db, lock_id)) {  // release read lock
    err_clear_detach_halt(LOCK_RELEASE_ERR);
  }
  tdata->lock_id=0;
  wg_detach_database(db);     
  tdata->db=NULL;
  str_guarantee_space(tdata,MIN_STRLEN); 
  if (tdata->format!=0) {
    // json
    snprintf(tdata->bufptr,MIN_STRLEN,"\n]");
    tdata->bufptr+=3;
  }  
  return tdata->buf;
}


