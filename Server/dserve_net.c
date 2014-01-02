/*

dserve_net.c contains networking functions for dserve.c

dserve is a tool for performing REST queries from WhiteDB using a cgi
protocol over http. Results are given in the json format.

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
#include <ctype.h> 
#include <errno.h>
#include <time.h> // linux nanosleep 

#if _MSC_VER   
#else
#include <netinet/in.h>
#include <arpa/inet.h> // inet_ntop
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h> // for alarm
#ifdef MULTI_THREAD
#include <pthread.h>
#endif
#endif

/* ============= local protos ============= */

int open_listener(int port);
void write_header(char* buf);
void write_header_clen(char* buf, int clen);
int parse_uri(char *uri, char *filename, char *cgiargs);
ssize_t readlineb(int fd, void *usrbuf, size_t maxlen, void* sslp);
ssize_t readn(int fd, void *usrbuf, size_t n, void* sslp);     
ssize_t writen(int fd, void *usrbuf, size_t n, void* sslp);
#if _MSC_VER
DWORD WINAPI handle_http(LPVOID targ);
#else   
void *handle_http(void *targ);
#endif
#ifdef USE_OPENSSL
SSL_CTX *init_openssl();
void ShowCerts(SSL* ssl);
#endif

/*   ========== structures =============  */

/* ========== globals =========================== */

extern struct dserve_global * dsdata;

/* =============== functions =================== */

int run_server(int port) {
  struct sockaddr_in clientaddr;
  int rc, sd, connsd, next; 
  struct thread_data *tdata; 
  struct common_data *common;
  long tid, maxtid, tcount, i;   
  size_t clientlen;
  //struct timeval timeout;
#ifdef MULTI_THREAD   
#if _MSC_VER
  HANDLE thandle;
  HANDLE thandlearray[MAX_THREADS];
  DWORD threads[MAX_THREADS];
#else  
  pthread_t threads[MAX_THREADS];   
  pthread_attr_t attr;
  struct timespec tim, tim2;    
#endif
#ifdef USE_OPENSSL    
  SSL_CTX *ctx; 
  SSL *ssl;  
#endif 
#endif
#if _MSC_VER
  void* db=NULL; // actual database pointer
  WSADATA wsaData;
   
  if (WSAStartup(MAKEWORD(2, 0),&wsaData) != 0) {
    errprint(WSASTART_ERR,NULL);
    exit(1);
  }       
  db = wg_attach_existing_database("1000");
  //db = wg_attach_database(database,100000000);
  if (!db) { errhalt(DB_ATTACH_ERR,NULL); return -1;}
#else 
  signal(SIGPIPE,SIG_IGN); // important for linux TCP/IP handling   
#endif  
  tdata=&(dsdata->threads_data[0]); 
#ifdef MULTI_THREAD    
#if _MSC_VER
#else
  if (THREADPOOL) {
    // -------- run as server with threadpool --------
    infoprint(THREADPOOL_INFO,NULL);
    // setup nanosleep for 100 microsec
    tim.tv_sec = 0;
    tim.tv_nsec = 100000;   
#ifdef USE_OPENSSL    
    // prepare openssl
    ctx=init_openssl();
#endif    
    // prepare threads
    common=(struct common_data *)malloc(sizeof(struct common_data));
    tid=0;
    tcount=0;
    maxtid=0;
    if (pthread_mutex_init(&(common->mutex),NULL) !=0 ||
        pthread_cond_init(&(common->cond),NULL) != 0 ||
        pthread_attr_init(&attr) !=0) {
      errprint(MUTEX_ERROR,NULL);    
      exit(1);
    }        
    common->threads = threads;
    common->queue = (common_task_t *)malloc(sizeof(common_task_t) * QUEUE_SIZE);
    common->thread_count = 0;
    common->queue_size = QUEUE_SIZE;
    common->head = common->tail = common->count = 0;
    common->shutdown = common->started = 0;
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE); //PTHREAD_CREATE_DETACHED);     
    // create threads
    for(tid=0;tid<MAX_THREADS;tid++) {
      // init thread data block 
      tdata[tid].isserver=1;      
      tdata[tid].thread_id=tid;
      tdata[tid].realthread=2;
      tdata[tid].format=1;
      tdata[tid].common=common;
      tdata[tid].inuse=0;
      tdata[tid].conn=0;
      tdata[tid].ip=NULL;
      tdata[tid].port=0;
      tdata[tid].urlpart=NULL;
      tdata[tid].verify=NULL;
      tdata[tid].res=0;        
      //fprintf(stderr,"creating thread %d tcount %d \n",(int)tid,(int)tcount); 
      rc=pthread_create(&threads[tid], &attr, handle_http, (void *) &tdata[tid]);
      if (rc) {
        errprint(THREAD_CREATE_ERR,strerror(errno));
        exit(-1);
      }      
      tcount++;
    }
    //
    sd=open_listener(port);
    if (sd<0) {
      errprint(PORT_LISTEN_ERR, strerror(errno));
      return -1;
    }         
    /*
    timeout.tv_sec = 1;
    timeout.tv_usec = 510;    
    if (setsockopt (sd,SOL_SOCKET,SO_RCVTIMEO,(char *)&timeout,sizeof(timeout)) < 0) {
      errprint(SETSOCKOPT_READT_ERR,NULL);
      return -1;
    } 
     
    if (setsockopt (sd,SOL_SOCKET,SO_SNDTIMEO,(char *)&timeout,sizeof(timeout)) < 0) {
      errprint(SETSOCKOPT_WRITET_ERR,NULL);        
      return -1;
    }     
    
    int deferer = 1;
    int ys = setsockopt(sd, SOL_TCP,TCP_DEFER_ACCEPT,(char *) &deferer, sizeof(int)); 
    */
    
    // socket options for modifying tcp: faster for longer messages
    /*
    state = 1;
    setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, &state, sizeof(state));
    setsockopt(sd, IPPROTO_TCP, TCP_CORK, &state, sizeof(state));    
    */
    
    clientlen = sizeof(clientaddr);
    // loop forever, servicing requests
    while (1) {   
      connsd=accept(sd,(struct sockaddr *)&clientaddr, &clientlen);
      if (connsd<0) {
        warnprint(CONN_ACCEPT_WARN, strerror(errno));
        continue;
      }  
      if(pthread_mutex_lock(&(common->mutex)) != 0) {
        errprint(THREADPOOL_LOCK_ERR,NULL);
        exit(1);
      }  
#ifdef USE_OPENSSL      
      ssl = SSL_new(ctx);  // get new SSL state with context
      SSL_set_fd(ssl,connsd);	
#endif      
      // now we have a connection: add to queue
      next=common->tail+1;
      next=(next==common->queue_size) ? 0 : next;
      
      /*
      if (setsockopt (connsd,SOL_SOCKET,SO_RCVTIMEO,(char *)&timeout,sizeof(timeout)) < 0) {
        errprint(SETSOCKOPT_READT_ERR,NULL);
        return -1;
      }       
      if (setsockopt (connsd,SOL_SOCKET,SO_SNDTIMEO,(char *)&timeout,sizeof(timeout)) < 0) {
        errprint(SETSOCKOPT_WRITET_ERR,NULL);        
        return -1;
      } 
      */
      do {      
        if(common->count==common->queue_size) { // full?
          //fprintf(stderr, "queue full\n");
          nanosleep(&tim , &tim2);
          break; //continue;
        }
        if(common->shutdown) { 
          warnprint(SHUTDOWN_WARN,NULL);
          break;
        }
        // add to task queue                
        common->queue[common->tail].conn=connsd;
#ifdef USE_OPENSSL      
        common->queue[common->tail].ssl=ssl; 
#endif          
        common->tail=next;
        common->count+=1;
        //printf("next %d\n",next);
        // broadcast
        if(pthread_cond_signal(&(common->cond)) != 0) {
          warnprint(COND_SIGNAL_FAIL_WARN,NULL);
          break;
        }
      } while(0);
      //fprintf(stderr,"starting to unlock \n");
      if(pthread_mutex_unlock(&(common->mutex)) != 0) {
        errprint(THREADPOOL_UNLOCK_ERR,NULL);
        exit(-1);
      }   
    }  
    return 0; // never come to this
    
  } else 
#endif // threadpool not implemented on windows version: using a non-threadpool version
         {
    // ---------- run as server without threadpool ---------
    infoprint(MULTITHREAD_INFO,NULL);
    // setup nanosleep for 100 microsec
#if _MSC_VER
#else    
    tim.tv_sec = 0;
    tim.tv_nsec = 100000;
#endif    
    // prepare common block
    common=(struct common_data *)malloc(sizeof(struct common_data));     
    common->shutdown=0;           
    // mark thread data blocks free
    for(i=0;i<MAX_THREADS;i++) {
      tdata[i].inuse=0;      
      tdata[i].common=common;
    }
    // prepare threads
    tid=0;
    tcount=0;
    maxtid=0;    
#if _MSC_VER
#else    
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED); //PTHREAD_CREATE_JOINABLE); 
#endif    
    sd=open_listener(port);
    if (sd<0) {
      errprint(PORT_LISTEN_ERR, strerror(errno));
      return -1;
    }
    clientlen = sizeof(clientaddr);
    while (1) {             
      connsd=accept(sd,(struct sockaddr *)&clientaddr, &clientlen);
      if (common->shutdown==1) break; 
      if (connsd<0) {     
        warnprint(CONN_ACCEPT_WARN, strerror(errno));       
        continue;
      }           
      tid=-1;       
      // find first free thread data block
      // loop until we get a free one
      while(tid<0) {        
        for(i=0;i<MAX_THREADS;i++) {
          if (!tdata[i].inuse) {
            tid=i;
            break;
          }
        }
        if (tid>=0) break;
#if _MSC_VER
        usleep(1);
#else        
        nanosleep(&tim , &tim2);
#endif        
      }  
      if (tid>maxtid) maxtid=tid;
      tcount++;
      // init thread data block
      tdata[tid].isserver=1;      
      tdata[tid].thread_id=tid;
      tdata[tid].realthread=1;
      tdata[tid].format=1;
      tdata[tid].inuse=1;
      tdata[tid].conn=connsd;
      tdata[tid].ip=NULL;
      tdata[tid].port=0;
      tdata[tid].urlpart=NULL;
      tdata[tid].verify=NULL;
      tdata[tid].res=0;       
#if _MSC_VER
      tdata[tid].db=db;
      thandle=CreateThread(NULL, 0, handle_http, (void *) &tdata[tid], 0, &threads[tid]);     
      if (thandle==NULL) {
        win_err_handler(TEXT("CreateThread"));
        ExitProcess(3);
      } else {
        thandlearray[tid]=thandle;
      }       
#else      
      rc=pthread_create(&threads[tid], &attr, handle_http, (void *) &tdata[tid]);                         
#endif      
    }
    return 0; // never come to this
  } 
#else  
  // --------- run as an iterative server ----------
  sd=open_listener(port);
  if (sd<0) {
    errprint(PORT_LISTEN_ERR, strerror(errno));
    return -1;
  }
  clientlen = sizeof(clientaddr);
  while (1) {      
    connsd=accept(sd,(struct sockaddr *)&clientaddr, &clientlen);
    if (connsd<0) {
      warnprint(CONN_ACCEPT_WARN, strerror(errno));
      continue;
    }           
    tid=0;  
    tdata[tid].isserver=1;
    tdata[tid].thread_id=tid;
    tdata[tid].realthread=0;
    tdata[tid].format=1;
    tdata[tid].conn=connsd;
    tdata[tid].ip=NULL;
    tdata[tid].port=0;
    tdata[tid].urlpart=NULL;
    tdata[tid].verify=NULL;
    tdata[tid].res=0;  
#if _MSC_VER
    tdata[tid].db=db;
#endif     
    handle_http((void *) &tdata[tid]);           
  }
  return 0; // never come to this
#endif  
}  

// handle one http request 

#if _MSC_VER
DWORD WINAPI handle_http(LPVOID targ) {
#else   
void *handle_http(void *targ) {
#endif
  int connsd,i,len,tid,itmp;    
  char *method=NULL, *uri=NULL, *version=NULL, *query=NULL;  
  char *bp=NULL, *res=NULL;
  char buf[MAXLINE];
  char header[HTTP_HEADER_SIZE];
  struct thread_data *tdata;  
  struct common_data *common;    
  socklen_t alen;  
  struct sockaddr_storage addr;
  struct sockaddr_in *s4;
  struct sockaddr_in6 *s6;   
  char ipstr[INET6_ADDRSTRLEN];
  int port;
#ifdef USE_OPENSSL
  SSL* ssl;
#else
  void* ssl;  
#endif  
  //int error;
  //socklen_t slen;
  //  
#if _MSC_VER    
#else  
  struct timespec tim, tim2;
  
  tim.tv_sec = 0;
  tim.tv_nsec = 1000;
#endif   

  tdata=(struct thread_data *) targ;   
  tid=tdata->thread_id;
  common=tdata->common; 
  while(1) {
    // infinite loop for threadpool, just once for non-threadpool
    if ((tdata->realthread)!=2) {
      // once-run thread
      connsd=tdata->conn;
    } else {  
      // threadpool thread
#ifdef MULTI_THREAD 
#if THREADPOOL      
      pthread_mutex_lock(&(common->mutex)); 
      while ((common->count==0) && (common->shutdown==0)) {      
        itmp=pthread_cond_wait(&(common->cond),&(common->mutex)); // wait
        if (itmp) {
          errprint(COND_WAIT_FAIL_ERR,NULL);
          exit(1);
        }
      }  
      if (common->shutdown) {
        pthread_mutex_unlock(&(common->mutex)); // ?
        warnprint(SHUTDOWN_THREAD_WARN,NULL);
        tdata->inuse=0;
        pthread_exit((void*) tid);     
        return NULL; 
      }
#endif      
#endif      
      connsd=common->queue[common->head].conn; 
#ifdef USE_OPENSSL
      ssl=common->queue[common->head].ssl;
      if (SSL_accept(ssl)==-1) {
        SSL_free(ssl);
        ssl=NULL;
        //fprintf(stderr,"ssl accept error\n");
        //ERR_print_errors_fp(stderr);        
      }  
      //ShowCerts(ssl);      
#else  
      ssl=NULL;      
#endif         
      common->head+=1;
      common->head=(common->head == common->queue_size) ? 0 : common->head;
      common->count-=1;       
#ifdef MULTI_THREAD
#if THREADPOOL
      pthread_mutex_unlock(&(common->mutex)); 
#endif      
#else
      // break;
#endif      
    }   
    // who is calling?
    alen = sizeof addr;
    getpeername(connsd, (struct sockaddr*)&addr, &alen);
    if (addr.ss_family == AF_INET) {
      s4 = (struct sockaddr_in *)&addr;
      port = ntohs(s4->sin_port);
#if _MSC_VER      
      InetNtop(AF_INET, &s4->sin_addr, ipstr, sizeof ipstr);
    } else { // AF_INET6  
      s6 = (struct sockaddr_in6 *)&addr;
      port = ntohs(s6->sin6_port);
      InetNtop(AF_INET6, &s6->sin6_addr, ipstr, sizeof ipstr);     
    }     
#else          
      inet_ntop(AF_INET, &s4->sin_addr, ipstr, sizeof ipstr);
    } else { // AF_INET6  
      s6 = (struct sockaddr_in6 *)&addr;
      port = ntohs(s6->sin6_port);
      inet_ntop(AF_INET6, &s6->sin6_addr, ipstr, sizeof ipstr);
    }  
#endif      
    printf("Peer IP address: %s\n", ipstr);
    printf("Peer port      : %d\n", port);
#ifdef USE_OPENSSL
    if (ssl!=NULL) {
#else
    if (1) {
#endif    
      // accepted connection: read and process
      // read and parse request line      
      readlineb(connsd,buf,MAXLINE,ssl);
      method=buf;
      for(i=0,bp=buf; *bp!='\0' && i<2; bp++) {
        if (*bp==' ') {
          *bp='\0';
          if (!i) uri=bp+1; else version=bp+1;
          ++i;
        }
      }  
      if (strcmp(method, "GET")) { 
        //return;
        res=make_http_errstr(HTTP_METHOD_ERR);
      } else if (uri==NULL || version==NULL) { 
        //return;
        res=make_http_errstr(HTTP_REQUEST_ERR);
      } else {      
        for(bp=uri; *bp!='\0'; bp++) {
          if (*bp=='?') { 
            *bp='\0';
            query=bp+1; 
            break; 
          }
        }  
        if (query==NULL || *query=='\0') { 
          res=make_http_errstr(HTTP_NOQUERY_ERR);
        } else {      
          // compute result       
  #ifdef MULTI_THREAD     
          if (!(common->shutdown)) {
            res=process_query(query,tdata);
            //printf("res: %s\n",res);
          } else {
            tdata->inuse=0;
  #if _MSC_VER        
            ExitThread(1);
            return 0;
  #else        
            pthread_exit((void*) tid);
            return NULL;
  #endif                  
          }  
  #else               
          res=process_query(query,tdata);
  #endif        
        }
      }  
      //printf("res: %s\n",res);
      // make header
      if (res==NULL) len=0;
      else len=strlen(res);
      write_header(header);
      write_header_clen(header,len); 
      // send result       
      i=writen(connsd,header,strlen(header),ssl);  
      if (res!=NULL) {
        i=writen(connsd,res,len,ssl);
        free(res);
      }    
  #ifdef USE_OPENSSL    
      if (ssl!=NULL) SSL_free(ssl);    
  #endif    
    }  
    // next part is run also for non-accepted connections   
  #if _MSC_VER    
    //Sleep(1);
    //if (len>=CLOSE_CHECK_THRESHOLD) Sleep(10);
    //else usleep(10);
    if (shutdown(connsd,SD_SEND)<0) { // SHUT_BOTH    
#else
    if (shutdown(connsd,SHUT_WR)<0) { // SHUT_RDWR 
#endif    
      // shutdown fails
      //fprintf(stderr, "Cannot shutdown connection: %s\n", strerror(errno));        

#if _MSC_VER      
      if (closesocket(connsd) < 0) {
#else      
      if (close(connsd) < 0) { 
#endif      
        warnprint("Cannot close connection after failed shutdown: %s\n", strerror(errno));          
      }          
    } else {    
      // normal shutdown               
      if (len>=CLOSE_CHECK_THRESHOLD) { 
        for(;;) {
          itmp=recv(connsd, buf, MAXLINE, 0);
          if(itmp<0) {
            fprintf(stderr,"error %d reading after shutdown in thread %d\n",itmp,tid);
            break;   
          }
          if(!itmp)  break;
#if _MSC_VER          
          usleep(1);
#else
          nanosleep(&tim , &tim2);
#endif          
        }                       
        /* // alternative check
        i=-1;
        for(;;) {
          ioctl(connsd, SIOCOUTQ, &itmp);
          //if(itmp != i) printf("Outstanding: %d\n", itmp);
          i = itmp;
          if(!itmp) break;
          //usleep(1);
          nanosleep(&tim , &tim2);
        }
        */
      }    
#if _MSC_VER      
      if (closesocket(connsd) < 0) {
#else      
      if (close(connsd) < 0) { 
#endif         
        warnprint("Cannot close connection: %s\n", strerror(errno));        
      }               
    }
    // connection is now down 
    tdata->inuse=0;
    if ((tdata->realthread)==1) {
      // non-threadpool thread
      //fprintf(stderr,"exiting thread %d\n",tid);
#ifdef MULTI_THREAD    
#if _MSC_VER
      ExitThread(1); 
      return 0;      
#else      
      pthread_exit((void*) tid);
      return NULL;
#endif      
#endif      
    } else if ((tdata->realthread)==2) {
      // threadpool thread
      //fprintf(stderr,"thread %d loop ended\n",tid);
    } else {
      // not a thread at all
#if _MSC_VER
      return 0;
#else      
      return NULL;
#endif      
    }      
  }  
}


void write_header(char* buf) {
  char *h1;
  
  h1=HEADER_TEMPLATE;
  strcpy(buf,h1);    
}

void write_header_clen(char* buf, int clen) {
  char* p;
  int n;
  
  p=strchr(buf,'X');
  n=sprintf(p,"%d",clen);
  *(p+n)=' ';
  for(p=p+n+1;*p=='X';p++) *p=' ';  
}


int open_listener(int port) {
  int sd, opt=1;
  struct sockaddr_in saddr;
  // create socket descriptor sd
  if ((sd=socket(AF_INET, SOCK_STREAM, 0)) < 0) return -1; 
  // eliminate addr in use error
  if (setsockopt(sd,SOL_SOCKET,SO_REUSEADDR,(const void *)&opt,sizeof(int))<0) return -1;
  // all requests to port for this host will be given to sd
  //bzero((char *) &saddr, sizeof(saddr)); // use memset instead
  memset((char *) &saddr,0,sizeof(saddr));
  saddr.sin_family=AF_INET; 
  saddr.sin_addr.s_addr=htonl(INADDR_ANY); 
  saddr.sin_port=htons((unsigned short)port); 
  if (bind(sd,(struct sockaddr *)&saddr, sizeof(saddr)) < 0) return -1;
  // make a listening socket to accept requests
  if (listen(sd,HTTP_LISTENQ)<0) return -1;
  return sd;
}

ssize_t readlineb(int fd, void *usrbuf, size_t maxlen, void* sslp) {
  int n, rc;
  char c, *bufp = usrbuf;
#ifdef USE_OPENSSL  
  SSL* ssl=(SSL*)sslp;
#endif  
  for (n = 1; n < maxlen; n++) { 
#if _MSC_VER    
  if ((rc =  recv(fd, &c, 1, 0)) == 1) {
#else    
#ifdef USE_OPENSSL  
  if ((rc = readn(fd, &c, 1, sslp)) == 1) {
#else    
  if ((rc = readn(fd, &c, 1, NULL)) == 1) {
#endif    
#endif      
	    *bufp++ = c;
	    if (c == '\n') break;
    } else if (rc == 0) {
	    if (n == 1) return 0; // EOF, no data read 
	    else break;    // EOF, some data was read 
    } else {
      return -1;	  // error 
    }  
  }
  *bufp = 0;
  return n;
}

ssize_t readn(int fd, void *usrbuf, size_t n, void* sslp)  {
  size_t nleft = n;
  ssize_t nread;
  char *bufp = usrbuf;
#ifdef USE_OPENSSL  
  SSL* ssl=(SSL*)sslp;
#endif 
  
  while (nleft>0) { 
#ifdef USE_OPENSSL  
    if ((nread=SSL_read(ssl, bufp, nleft)) < 0) {
#else    
    if ((nread=read(fd, bufp, nleft)) < 0) {
#endif    
        if (errno==EINTR) nread=0;/* interrupted by sig handler return */
            /* and call read() again */
        else return -1;      /* errno set by read() */ 
    } else if (nread==0) break;              /* EOF */
    nleft -= nread;
    bufp += nread;
  }
  return (n-nleft);         /* return >= 0 */
}


ssize_t writen(int fd, void *usrbuf, size_t n, void* sslp) {
  size_t nleft = n;
  ssize_t nwritten;
  char *bufp = usrbuf;
#ifdef USE_OPENSSL  
  SSL* ssl=(SSL*)sslp;
#endif 
  
  while (nleft > 0) {
#if _MSC_VER    
    if ((nwritten = send(fd, bufp, nleft, 0)) <= 0) {
#else    
#ifdef USE_OPENSSL  
    if ((nwritten = SSL_write(ssl, bufp, nleft)) < 0) {
#else    
    if ((nwritten = write(fd, bufp, nleft)) <= 0) {
#endif         
#endif      
      if (errno == EINTR) {
        nwritten = 0;   /* interrupted by sig handler return */
		    /* and call write() again */
      } else {
        warnprint(WRITEN_ERROR,NULL); // EPIPE 32 is likely here
        return -1;       /* errorno set by write() */
      }  
    }
    nleft -= nwritten;
    bufp += nwritten;
  }
  return n;
}

#ifdef USE_OPENSSL
SSL_CTX *init_openssl() {
  char* CertFile;
  char* KeyFile;
  const SSL_METHOD *method;
  SSL_CTX *ctx;  
  
  printf("initializing openssl\n");
  CertFile=CERT_FILE;
  KeyFile=KEY_FILE;
  SSL_load_error_strings();
  SSL_library_init();
  //OpenSSL_add_all_algorithms();
  method = SSLv3_server_method();
  ctx = SSL_CTX_new(method);
  if (ctx==NULL) {
    fprintf(stderr,"ssl initialization error:\n");
    ERR_print_errors_fp(stderr);    
    exit(-1);
  }
  /* set the local certificate from CertFile */
  SSL_CTX_use_certificate_file(ctx, CertFile, SSL_FILETYPE_PEM);
  if (SSL_CTX_use_certificate_file(ctx, CertFile, SSL_FILETYPE_PEM) <= 0){
    fprintf(stderr,"ssl certificate file error:\n");
    ERR_print_errors_fp(stderr);    
    exit(-1);
  }
  /* set the private key from KeyFile */
  if (SSL_CTX_use_PrivateKey_file(ctx, KeyFile, SSL_FILETYPE_PEM)<=0) {
    fprintf(stderr,"ssl private key file error:\n");
    ERR_print_errors_fp(stderr);
    exit(-1);
  }
  /* verify private key */
  if ( !SSL_CTX_check_private_key(ctx) ) {
    fprintf(stderr,"ssl error checking key\n");
    ERR_print_errors_fp(stderr);
    exit(-1);
  }  
  printf("openssl initialized\n");
  return ctx;
}  

void ShowCerts(SSL* ssl){  // this function is not really needed
  X509 *cert;
  char *line;

  cert = SSL_get_peer_certificate(ssl);	/* Get certificates (if available) */
  if ( cert != NULL ) {
      printf("Server certificates:\n");
      line = X509_NAME_oneline(X509_get_subject_name(cert), 0, 0);
      printf("Subject: %s\n", line);
      free(line);
      line = X509_NAME_oneline(X509_get_issuer_name(cert), 0, 0);
      printf("Issuer: %s\n", line);
      free(line);
      X509_free(cert);
  } else {
    printf("No certificates.\n");
  }  
}
#endif
