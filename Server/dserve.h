/*

dserve.h is a common header for dserve

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http(s). Results are given in the json or csv format.

See http://whitedb.org/tools.html for a detailed manual.

Copyright (c) 2013, Tanel Tammet

This software is under MIT licence: see dserve.c for details.
*/

/* ====== select windows/linux dependent includes and options ======= */

#define SERVEROPTION // remove this for cgi/command line only: no need for threads/sockets in this case

#if _MSC_VER
#include <dbapi.h> // set this to "../Db/dbapi.h" if whitedb is not installed
#define snprintf _snprintf
#else
#include <whitedb/dbapi.h> 
#endif

#ifdef SERVEROPTION
#if _MSC_VER
// windows, see http://msdn.microsoft.com/en-us/library/windows/desktop/ms738566%28v=vs.85%29.aspx
//#define WIN32_LEAN_AND_MEAN
//#include <windows.h> // windows.h only with lean_and_mean before it
#include <winsock2.h>
#include <ws2tcpip.h>
#include <tchar.h>
#include <strsafe.h>
#pragma comment (lib, "ws2_32.lib")
#pragma comment (lib, "User32.lib") // required by win_err_handle only
#define THREADPOOL 0 // threadpools are not implemented for windows
#define CLOSE_CHECK_THRESHOLD 0 // always set this to 0 for windows
#else
// linux
#include <pthread.h>
#define THREADPOOL 1 // set to 0 for no threadpool (instead, new thread for each connection)
#define CLOSE_CHECK_THRESHOLD 10000 // close immediately after shutdown for msg len less than this
#endif
#endif

#ifdef USE_OPENSSL
#include <openssl/opensslconf.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
// create by: openssl req -out CSR.csr -new -newkey rsa:2048 -nodes -keyout privateKey.key
//#define KEY_FILE "/home/tanel/whitedb/Server/privateKey.key"
// create by: openssl rsa -in privateKey.key -out newPrivateKey.pem
#define KEY_FILE "/home/tanel/whitedb/Server/newPrivateKey.pem"
// create by: 
#define CERT_FILE "/home/tanel/whitedb/Server/certificate.crt"
#endif

/* =============== configuration macros =================== */

//#define CONF_FILE "/home/tanel/whitedb/Server/conf.txt"

#define DEFAULT_DATABASE "1000" // used if none explicitly given

// print level

#define INFOPRINT // if set, neutral info printed to stderr, no info otherwise
#define WARNPRINT // if set, warnings printed to stderr, no warnprint otherwise
#define ERRPRINT // if set, errors printed to stderr, no errprint otherwise

// server/connection configuration

//#define DEFAULT_PORT 8080 // define this to run as a server on that port if no params given
//#define USE_OPENSSL // define this to build a https server
#define MULTI_THREAD // removing this creates a simple iterative server
#define MAX_THREADS 8 // size of threadpool and max nr of threads in an always-new-thread model
#define QUEUE_SIZE 100 // task queue size for threadpool
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
#define CONF_BUF_SIZE 1000 // initial conf buf size, incremented as necessary
#define MAX_CONF_BUF_SIZE 10000000 // max conf file size
#define CONF_VALS_SIZE 2 // initial size of conf value array
#define MAX_CONF_VALS_SIZE 1000000 // max size of conf value array

// result output/print settings

#define HELP_PARAM "--help" // for dserve --help
#define INITIAL_MALLOC 1000 // initially malloced result size
#define MAX_MALLOC 100000000 // max malloced result size
#define MIN_STRLEN 100 // fixed-len obj strlen, add this to strlen for print-space need
#define STRLEN_FACTOR 6 // might need 6*strlen for json encoding
#define DOUBLE_FORMAT "%g" // snprintf format for printing double
#define JS_NULL "[]" 
#define CSV_SEPARATOR ',' // must be a single char
#define MAX_DEPTH_DEFAULT 100 // can be increased
#define MAX_DEPTH_HARD 10000 // too deep rec nesting will cause stack overflow in the printer
#define HTTP_LISTENQ  1024 // server only: second arg to listen: listening queue length
   // may want to use SOMAXCONN instead of 1024 in windows
#define HTTP_HEADER_SIZE 1000 // server only: buffer size for header
#define HTTP_ERR_BUFSIZE 1000 // server only: buffer size for errstr

// normal nonterminating error strings

#define NOQUERY_ERR "no query"
#define LONGQUERY_ERR "too long query"
#define MALFQUERY_ERR "malformed query"

#define UNKNOWN_PARAM_ERR "unrecognized parameter: %s"
#define UNKNOWN_PARAM_VALUE_ERR "unrecognized value %s for parameter %s"
#define NO_OP_ERR "no op given: use op=opname for opname in search"
#define UNKNOWN_OP_ERR "unrecognized op: use op=search or op=recids"
#define NO_FIELD_ERR "no field given"
#define NO_VALUE_ERR "no value given"
#define CANNOT_ALLOC_ERR "cannot allocate global data\n"
#define DB_PARAM_ERR "use db=name with a numeric name for a concrete database"
#define DB_ATTACH_ERR "no database found: use db=name with a numeric name for a concrete database"
#define FIELD_ERR "unrecognized field: use an integer starting from 0"
#define COND_ERR "unrecognized compare: use equal, not_equal, lessthan, greater, ltequal or gtequal"
#define INTYPE_ERR "unrecognized type: use null, int, double, str, char or record "
#define INVALUE_ERR "did not find a value to use for comparison"
#define INVALUE_TYPE_ERR "value does not match type"
#define DECODE_ERR "field data decoding failed"

#define HTTP_METHOD_ERR "method given in http not implemented: use GET"
#define HTTP_REQUEST_ERR "incorrect http request"
#define HTTP_NOQUERY_ERR "no query found"
#define WRITEN_ERROR "writen error\n"

// formatting normal err messages 

#define JS_TYPE_ERR "\"\""  // currently this will be shown also for empty string
#define NORMAL_ERR_FORMAT "[\"%s\"]" // normal non-terminate error string is put in here

// terminating error strings

#define TIMEOUT_ERR "timeout"
#define INTERNAL_ERR "internal error"
#define LOCK_ERR "database locked"
#define LOCK_RELEASE_ERR "releasing read lock failed: database may be in deadlock"
#define MALLOC_ERR "cannot allocate enough memory for result string"
#define QUERY_ERR "query creation failed"
#define NOT_AUTHORIZED_ERR "query not authorized"

#define WSASTART_ERR "WSAStartup failed\n"
#define MUTEX_ERROR "Error initializing pthread mutex, cond or attr\n"
#define THREAD_CREATE_ERR "Cannot create a thread: %s\n"
#define PORT_LISTEN_ERR "Cannot open port for listening: %s\n"
#define SETSOCKOPT_READT_ERR "Setsockopt for read timeout failed\n"
#define SETSOCKOPT_WRITET_ERR "Setsockopt for write timeout failed\n"
#define THREADPOOL_UNLOCK_ERR "Threadpool unlock failure\n"
#define COND_WAIT_FAIL_ERR "pthread_cond_wait failure\n"
#define THREADPOOL_LOCK_ERR "Threadpool lock failure \n"
#define TERMINATE_ERR "dserve terminating\n"
#define TERMINATE_NOGLOB_ERR "dserve terminating: no global data found\n"

#define CONF_OPEN_ERR "Cannot open configuration file %s \n"
#define CONF_MALLOC_ERR "Cannot malloc for configuration file reading\n"
#define CONF_READ_ERR "Cannot read from configuration file %s\n"
#define CONF_VAL_ERR "Unknown key %s in configuration file\n"
#define CONF_SIZE_ERR "MAX_CONF_BUF_SIZE too small to read the the configuration file %s\n"
#define CONF_VALNR_ERR "MAX_CONF_VALS_SIZE too small for the list of conf values\n"
#define NO_KEY_FILE_ERR "key_file not given in configuration for https\n"
#define NO_CERT_FILE_ERR "cert_file not given in configuration for https\n"


// warnings and info

#define CONN_ACCEPT_WARN "Cannot accept connection: %s\n"
#define SHUTDOWN_WARN "Shutting down\n"
#define SHUTDOWN_THREAD_WARN "Shutting down thread\n"
#define COND_SIGNAL_FAIL_WARN "pthread_cond_signal failure\n"

#define THREADPOOL_INFO "Running multithreaded with a threadpool\n"
#define MULTITHREAD_INFO "Running multithreaded without threadpool\n"

// internal values

#define READ "r"
#define READ_LOCK_TYPE 1
#define WRITE_LOCK_TYPE 2

#define ADMIN_LEVEL 0
#define WRITE_LEVEL 1
#define READ_LEVEL  2

#define CONF_DEFAULT_DBASE "default_dbase"
#define CONF_DBASES "dbases"
#define CONF_ADMIN_IPS "admin_ips"
#define CONF_WRITE_IPS "write_ips"
#define CONF_READ_IPS "read_ips"
#define CONF_ADMIN_TOKENS "admin_tokens"
#define CONF_WRITE_TOKENS "write_tokens"
#define CONF_READ_TOKENS "read_tokens"
#define CONF_KEY_FILE "key_file"
#define CONF_CERT_FILE "cert_file"

/*   ========== global structures =============  */

// each thread (or a single cgi/command line) has its own thread_data block

struct thread_data{  
  // thread type, database, locks
  int    isserver; // 1 if run as a server, 0 if not
  int    iscgi; // 1 if run as a cgi program, 0 if not
  int    realthread; // 1 if thread, 0 if not
  int    thread_id; // 0,1,..
  struct common_data *common; // common is shared by all threads
  void*  db; // NULL iff not attached
  char*  database; //database name
  wg_int lock_id; // 0 iff not locked
  int    lock_type; // 1 read, 2 write  
  int    inuse; // 1 if in use, 0 if not (free to reuse)
  // task details   
  int    conn; // actual socket id
#ifdef USE_OPENSSL    
  SSL    *ssl;  
#endif   
  char*  ip; // request ip
  int    port;  // request port
  //char*  urlpart;  // urlpart to open like /dserve?op=search
  //char*  verify; // string to look for
  int    res;    // stored by thread
  // printing
  int    format;  // 1 json, 0 csv    
  int    showid; // print record id for record: 0 no show, 1 first (extra) elem of record
  int    depth; // limit on records nested via record pointers (0: no nesting)
  int    maxdepth; // limit on printing records nested via record pointers (0: no nesting)
  int    strenc; /*
                strenc==0: nothing is escaped at all
                strenc==1: non-ascii chars and % and " urlencoded
                strenc==2: json utf-8 encoding, not ascii-safe
                strenc==3: csv encoding, only " replaced for ""
              */
  char   *buf; // address of the whole string buffer start (not the start itself)
  char   *bufptr;  // address of the next place in buf to write into
  int    bufsize; // buffer length
};

// a single dserve_global is created as a global var dsglobal

struct dserve_global{
  int                maxthreads;
  struct dserve_conf *conf;
  struct thread_data threads_data[MAX_THREADS];  
};

// configuration data read from file, kept as sized_strlst for each kind
// with each sized_strlst containing char* array of conf vals

struct sized_strlst{
  int size; // vals array size: not all have to be used
  int used; // nr of used els in vals
  char** vals; // actual array of char* to vals
};

struct dserve_conf{
  struct sized_strlst default_dbase;
  struct sized_strlst dbases;
  struct sized_strlst admin_ips;
  struct sized_strlst write_ips;
  struct sized_strlst read_ips;
  struct sized_strlst admin_tokens;
  struct sized_strlst write_tokens;
  struct sized_strlst read_tokens;
  struct sized_strlst key_file;
  struct sized_strlst cert_file;
};

#ifdef SERVEROPTION

#if _MSC_VER
// windows

#define ssize_t int
#define socklen_t int

// task queue elements

typedef struct {
  int  conn; 
} common_task_t;

// common information pointed to from each thread data block: lock, queue, etc

struct common_data{
  void*           *threads;
  void*           mutex;    
  void*           cond;  
  common_task_t   *queue;
  int             thread_count;
  int             queue_size;
  int             head;
  int             tail;
  int             count;
  int             started;
  int             shutdown;  
};

#else
// linux
// task queue elements

typedef struct {
  int  conn; 
#ifdef USE_OPENSSL
  SSL    *ssl; 
#endif  
} common_task_t;

// common information pointed to from each thread data block: lock, queue, etc

struct common_data{
  pthread_t       *threads;
  pthread_mutex_t mutex;    
  pthread_cond_t  cond;  
  common_task_t   *queue;
  int             thread_count;
  int             queue_size;
  int             head;
  int             tail;
  int             count;
  int             started;
  int             shutdown;  
};
#endif // win or linux server
#else
// no serveroption
typedef struct {
  int  conn; 
} common_task_t;

struct common_data{
  void*           *threads;
  void*           mutex;    
  void*           cond;  
  common_task_t   *queue;
  int             thread_count;
  int             queue_size;
  int             head;
  int             tail;
  int             count;
  int             started;
  int             shutdown;  
};
#endif // serveroption or no serveroption

/* =============== global protos =================== */

// in dserve.c:

char* process_query(char* inquery, struct thread_data * tdata); 
void print_final(char* str, struct thread_data * tdata);

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

int sprint_record(void *db, wg_int *rec, struct thread_data * tdata);                   
char* sprint_value(void *db, wg_int enc, struct thread_data * tdata);
int sprint_string(char* bptr, int limit, char* strdata, int strenc);
int sprint_blob(char* bptr, int limit, char* strdata, int strenc);
int sprint_append(char** buf, char* str, int l);

char* str_new(int len);
int str_guarantee_space(struct thread_data *tdata, int needed);

int load_configuration(char* path, struct dserve_conf *conf);
int add_conf_key_val(struct dserve_conf *conf, char* key, char* val);
int add_slval(struct sized_strlst *lst, char* val);
void print_conf(struct dserve_conf *conf);
void print_conf_slval(struct sized_strlst *lst, char* key);
int authorize(int level, struct dserve_conf *conf, struct thread_data * tdata, char* token);

void infoprint(char* fmt, char* param);
void warnprint(char* fmt, char* param);
void errprint(char* fmt, char* param);
void termination_handler(int signal);
void clear_detach(int signal);
void timeout_handler(int signal);
void err_clear_detach_halt(char* errstr);
char* errhalt(char* str, struct thread_data * tdata);
char* make_http_errstr(char* str);

#if _MSC_VER
void usleep(__int64 usec);
void win_err_handler(LPTSTR lpszFunction);
#endif
