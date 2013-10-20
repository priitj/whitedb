#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define MAXQUERYLEN 1000 // query string length limit
#define MAXPARAMS 100 // max number of cgi params in query
#define MAXCOUNT 100 // max number of result records
#define INITIAL_MALLOC 10000 // initially malloced result size
#define MAX_MALLOC 100000000 // max malloced result size
#define DEFAULT_DATABASE "1000" 

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

#define CONTENT_TYPE "content-type: text/plain\n\n"

int isxdigit(int a);
int isdigit(int a);

int isint(char* s);
int isdbl(char* s);

char* search(char* database, char* inparams[], char* invalues[], int count);
int parse_query(char* query, int ql, char* params[], char* values[]);
char* urldecode(char *indst, char *src);
void errhalt(char* str);

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

/* search from the database */  
  
char* search(char* database, char* inparams[], char* invalues[], int incount) {
  int i,rcount,gcount,itmp;
  wg_int cmp,type,val;
  char* fields[MAXPARAMS];
  char* values[MAXPARAMS];
  char* compares[MAXPARAMS];
  char* types[MAXPARAMS];
  int fcount=0, vcount=0, ccount=0, tcount=0;
  int from=0;
  int count=MAXCOUNT;
  void* db;
  void* rec;
  char* res;
  int res_size=INITIAL_MALLOC;
  wg_query *wgquery;  
  wg_query_arg wgargs[MAXPARAMS];
  wg_int lock_id;
  int nosearch=0;
  
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
    wg_detach_database(db);
    errhalt(NO_FIELD_ERR); 
  } 
  // database attached OK
  if (nosearch) {
    // special case without search: just output records     
    gcount=0;
    rcount=0;
    lock_id = wg_start_read(db); // get read lock
    if (!lock_id) {
      wg_detach_database(db); 
      errhalt(LOCK_ERR);
    }
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
      wg_detach_database(db);
      errhalt(LOCK_RELEASE_ERR);
    }
    wg_detach_database(db); 
    printf("rcount %d gcount %d\n", rcount, gcount);
    return "NOSEARCH OK";
  }  
  // normal search case:
  // create a query list datastructure
  for(i=0;i<fcount;i++) {   
    // field num    
    printf("i: %d fields[i]: %s %d\n",i,values[i],atoi(fields[i]));
    if (!isint(fields[i])) {
      wg_detach_database(db);
      errhalt(FIELD_ERR); 
    }    
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
    else {
      wg_detach_database(db);
      errhalt(COND_ERR); 
    }
    wgargs[i].cond = cmp;  
    // valuetype: default guess from value later
    if (types[i]==NULL || types[i]=='\0') type=0;   
    else if (!strcmp(types[i],"null"))  type=WG_NULLTYPE; 
    else if (!strcmp(types[i],"int"))  type=WG_INTTYPE; 
    else if (!strcmp(types[i],"record"))  type=WG_RECORDTYPE;
    else if (!strcmp(types[i],"double"))  type=WG_DOUBLETYPE; 
    else if (!strcmp(types[i],"str"))  type=WG_STRTYPE; 
    else if (!strcmp(types[i],"char"))  type=WG_CHARTYPE;   
    else {
      wg_detach_database(db);
      errhalt(INTYPE_ERR); 
    }
    // encode value to compare with    
    if (values[i]==NULL) {
      wg_detach_database(db);
      errhalt(INVALUE_ERR); 
    }
    lock_id = wg_start_read(db); // get read lock
    if (!lock_id) {
      wg_detach_database(db); 
      errhalt(LOCK_ERR);
    }
    if (type==WG_NULLTYPE) val=wg_encode_query_param_null(db,NULL);
    else if (type==WG_INTTYPE) {
      if (!isint(values[i])) {
        wg_end_read(db, lock_id);
        wg_detach_database(db);
        errhalt(INVALUE_TYPE_ERR); 
      }
      val=wg_encode_query_param_int(db,atoi(values[i]));
    } else if (type==WG_RECORDTYPE) {
      if (!isint(values[i])) {
        wg_end_read(db, lock_id);
        wg_detach_database(db);
        errhalt(INVALUE_TYPE_ERR); 
      }
      val=(wg_int)atoi(values[i]);
    } else if (type==WG_DOUBLETYPE) {
      if (!isdbl(values[i])) {
        wg_end_read(db, lock_id);
        wg_detach_database(db);
        errhalt(INVALUE_TYPE_ERR); 
      }
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
      wg_end_read(db, lock_id);
      wg_detach_database(db);
      errhalt(INVALUE_TYPE_ERR); 
    }
    wgargs[i].value = val;
  }   
  wgquery = wg_make_query(db, NULL, 0, wgargs, i);
  if(!wgquery) { 
    wg_end_read(db, lock_id); // release lock
    wg_detach_database(db);        
    errhalt(QUERY_ERR);  
  } 
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
  wg_free_query(db,wgquery); 
  if (!wg_end_read(db, lock_id)) {  // release read lock
    wg_detach_database(db);
    errhalt(LOCK_RELEASE_ERR);
  }
  wg_detach_database(db); 
  printf("rcount %d gcount %d\n", rcount, gcount);
  return "OK";
}
  

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

int isint(char* s) {
  if (s==NULL) return 0;
  while(*s!='\0') {
    if (!isdigit(*s)) return 0;
    s++;
  }
  return 1;
}  
  
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

/* error output and immediate halt
*/

void errhalt(char* str) {
  printf("%s\n",str);
  exit(0);
}



    