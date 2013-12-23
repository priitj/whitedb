/*

dserve.h is a common header for dserve

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http. Results are given in the json format.

See http://whitedb.org/tools.html for a detailed manual.

Copyright (c) 2013, Tanel Tammet

This software is under MIT licence.
*/

#include <whitedb/dbapi.h> // set this to "../Db/dbapi.h" if whitedb is not installed

/* =============== configuration macros =================== */

#define SERVEROPTION // remove this for cgi/command line only: no need for pthreads in this case

#define DEFAULT_DATABASE "1000" // used if none explicitly given

// server/connection configuration

#define DEFAULT_PORT 8080 // if used as a server without a port given on a command line
#define MULTI_THREAD // removing this creates a simple iterative server
#define MAX_THREADS 8 // size of threadpool and max nr of threads in an always-new-thread model
#define QUEUE_SIZE 100 // task queue size for threadpool
#define THREADPOOL 1 // set to 0 for no threadpool (instead, new thread for each connection)
#define CLOSE_CHECK_THRESHOLD 10000 // close immediately after shutdown for msg len less than this
#define TIMEOUT_SECONDS 2 // used for cgi and command line only

// header row templates

#define JSON_CONTENT_TYPE "Content-Type: application/json\r\n\r\n"
#define CSV_CONTENT_TYPE "Content-Type: text/csv\r\n\r\n"
#define CONTENT_LENGTH "Content-Length: %d\r\n"
#define HEADER_TEMPLATE "HTTP/1.0 200 OK\r\nServer: dserve\r\nContent-Length: XXXXXXXXXX \r\nContent-Type: text/plain\r\n\r\n"

// limits

#define MAXQUERYLEN 2000 // query string length limit
#define MAXPARAMS 100 // max number of cgi params in query
#define MAXCOUNT 100000 // max number of result records
#define MAXIDS 1000 // max number of rec id-s in recids query
#define MAXLINE 10000 // server query input buffer

// result output/print settings

#define INITIAL_MALLOC 1000 // initially malloced result size
#define MAX_MALLOC 100000000 // max malloced result size
#define MIN_STRLEN 100 // fixed-len obj strlen, add this to strlen for print-space need
#define STRLEN_FACTOR 6 // might need 6*strlen for json encoding
#define DOUBLE_FORMAT "%g" // snprintf format for printing double
#define JS_NULL "[]" 
#define CSV_SEPARATOR ',' // must be a single char
#define MAX_DEPTH_DEFAULT 100 // can be increased
#define MAX_DEPTH_HARD 10000 // too deep rec nesting will cause stack overflow in the printer
#define HTTP_LISTENQ  1024 // server only: second arg to listen
#define HTTP_HEADER_SIZE 1000 // server only: buffer size for header
#define HTTP_ERR_BUFSIZE 1000 // server only: buffer size for errstr

// error strings

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
#define HTTP_METHOD_ERR "method given in http not implemented: use GET"
#define HTTP_REQUEST_ERR "incorrect http request"
#define HTTP_NOQUERY_ERR "no query found"
#define JS_TYPE_ERR "\"\""  // currently this will be shown also for empty string

// internal values

#define READ_LOCK_TYPE 1
#define WRITE_LOCK_TYPE 2

#if _MSC_VER   // microsoft compatibility
#define snprintf _snprintf
#endif

/*   ========== global structures =============  */

// each thread (or a single cgi/command line) has its own thread_data block

struct thread_data{  
  int    isserver; // 1 if run as a server, 0 if not
  int    iscgi; // 1 if run as a cgi program, 0 if not
  int    realthread; // 1 if thread, 0 if not
  int    thread_id; // 0,1,..
  struct common_data *common; // common is shared by all threads
  void*  db; // NULL iff not attached
  char*  database; //database name
  wg_int lock_id; // 0 iff not locked
  int    lock_type; // 1 read, 2 write
  int    format;  // 1 json, 0 csv
  int    inuse; // 1 if in use, 0 if not (free to reuse)
  int    conn; //
  char*  ip; // ip to open
  int    port;  // port to open
  char*  urlpart;  // urlpart to open like /dserve?op=search
  char*  verify; // string to look for
  int    res;    // stored by thread
};


// a single dserve_global is created as a global var dsglobal

struct dserve_global{
  int          maxthreads;
  struct thread_data threads_data[MAX_THREADS];
};


/* =============== global protos =================== */

// in dserve.c:

char* process_query(char* inquery, struct thread_data * tdata); 
void print_final(char* str,int format);

// in dserve_net.c:

int run_server(int port);
char* make_http_errstr(char* str);

// in dserve_util.c:

wg_int encode_incomp(void* db, char* incomp);
wg_int encode_intype(void* db, char* intype);
wg_int encode_invalue(void* db, char* invalue, wg_int type);
int isint(char* s);
int isdbl(char* s);
int parse_query(char* query, int ql, char* params[], char* values[]);
char* urldecode(char *indst, char *src);

int sprint_record(void *db, wg_int *rec, char **buf, int *bufsize, char **bptr, 
                   int format, int showid, int depth,  int maxdepth, int strenc); 
char* sprint_value(void *db, wg_int enc, char **buf, int *bufsize, char **bptr, 
                   int format, int showid, int depth, int maxdepth, int strenc);
int sprint_string(char* bptr, int limit, char* strdata, int strenc);
int sprint_blob(char* bptr, int limit, char* strdata, int strenc);
int sprint_append(char** buf, char* str, int l);

char* str_new(int len);
int str_guarantee_space(char** stradr, int* strlenadr, char** ptr, int needed);

void termination_handler(int signal);
void clear_detach(int signal);
void timeout_handler(int signal);
void err_clear_detach_halt(char* errstr);
char* errhalt(char* str, struct thread_data * tdata);
char* make_http_errstr(char* str);

