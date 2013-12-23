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
#include <netinet/in.h>
#include <arpa/inet.h> // inet_ntop
#include <sys/socket.h>
#include <errno.h>
#include <pthread.h>
#include <time.h> // nanosleep
// #include <linux/sockios.h> // SIOCOUTQ
// #include <netinet/tcp.h>
// #include <sys/ioctl.h>
// #include <Winsock2.h> // todo later: windows version 

#if _MSC_VER   
#include <Ws2tcpip.h>
#else
#include <unistd.h> // for alarm
#endif


#include <netinet/tcp.h>



/* ============= local protos ============= */

int open_listener(int port);
void *handle_http(void *targ);
void write_header(char* buf);
void write_header_clen(char* buf, int clen);
int parse_uri(char *uri, char *filename, char *cgiargs);
ssize_t readlineb(int fd, void *usrbuf, size_t maxlen);
ssize_t readn(int fd, void *usrbuf, size_t n);     
ssize_t writen(int fd, void *usrbuf, size_t n);

/*   ========== structures =============  */



/* ========== globals =========================== */

extern struct dserve_global * dsdata;

/* =============== functions =================== */

 
int run_server(int port) {
  struct sockaddr_in clientaddr;
  int rc, sd, connsd, next;
  pthread_t threads[MAX_THREADS];   
  struct thread_data *tdata; 
  struct common_data *common;
  pthread_attr_t attr;
  long tid, maxtid, tcount, i;  
  struct timespec tim, tim2;
  size_t clientlen;
  //struct timeval timeout; 
  
 
  signal(SIGPIPE,SIG_IGN); // important for TCP/IP handling
  tdata=&(dsdata->threads_data[0]);  
#ifdef MULTI_THREAD    
  if (THREADPOOL) {
    // -------- run as server with threadpool --------
    infoprint(THREADPOOL_INFO,NULL);
    // setup nanosleep for 100 microsec
    tim.tv_sec = 0;
    tim.tv_nsec = 100000;   
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
    
  } else  {
    // ---------- run as server without threadpool ---------
    infoprint(MULTITHREAD_INFO,NULL);
    // setup nanosleep for 100 microsec
    tim.tv_sec = 0;
    tim.tv_nsec = 100000;
    // mark thread data blocks free
    for(i=0;i<MAX_THREADS;i++) tdata[i].inuse=0;      
    // prepare threads
    tid=0;
    tcount=0;
    maxtid=0;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED); //PTHREAD_CREATE_JOINABLE); 
    
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
        nanosleep(&tim , &tim2);
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
      rc=pthread_create(&threads[tid], &attr, handle_http, (void *) &tdata[tid]);                         
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
    handle_http((void *) &tdata[tid]);           
  }
  return 0; // never come to this
#endif  
}  


// handle one http request 

void *handle_http(void *targ) {
  int connsd,i,len,tid,itmp;    
  char *method=NULL, *uri=NULL, *version=NULL, *query=NULL;  
  char *bp=NULL, *res=NULL;
  char buf[MAXLINE];
  char header[HTTP_HEADER_SIZE];
  struct thread_data *tdata; 
  struct common_data *common;
  struct timespec tim, tim2;  
  socklen_t alen;
  struct sockaddr_storage addr;
  struct sockaddr_in *s4;
  struct sockaddr_in6 *s6;
  char ipstr[INET6_ADDRSTRLEN];
  //int error;
  //socklen_t slen;
  //int port;
  
  tdata=(struct thread_data *) targ;   
  tid=tdata->thread_id;
  common=tdata->common;
  tim.tv_sec = 0;
  tim.tv_nsec = 1000;  
  while(1) {
    // infinite loop for threadpool, just once for non-threadpool
    if ((tdata->realthread)!=2) {
      // once-run thread
      connsd=tdata->conn;
    } else {  
      // threadpool thread
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
      connsd=common->queue[common->head].conn; 
      common->head+=1;
      common->head=(common->head == common->queue_size) ? 0 : common->head;
      common->count-=1;    
      pthread_mutex_unlock(&(common->mutex));      
    }   
    // who is calling?
    alen = sizeof addr;
    getpeername(connsd, (struct sockaddr*)&addr, &alen);
    if (addr.ss_family == AF_INET) {
      s4 = (struct sockaddr_in *)&addr;
      //port = ntohs(s4->sin_port);
      inet_ntop(AF_INET, &s4->sin_addr, ipstr, sizeof ipstr);
    } else { // AF_INET6
      s6 = (struct sockaddr_in6 *)&addr;
      //port = ntohs(s6->sin6_port);
      inet_ntop(AF_INET6, &s6->sin6_addr, ipstr, sizeof ipstr);
    }
    //printf("Peer IP address: %s\n", ipstr);
    //printf("Peer port      : %d\n", port);
        
    // read and parse request line         
    readlineb(connsd,buf,MAXLINE);
    method=buf;
    for(i=0,bp=buf; *bp!='\0' && i<2; bp++) {
      if (*bp==' ') {
        *bp='\0';
        if (!i) uri=bp+1; else version=bp+1;
        ++i;
      }
    }  
    if (strcasecmp(method, "GET")) { 
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
        if (!(common->shutdown)) {
          res=process_query(query,tdata);
        } else {
          tdata->inuse=0;
          pthread_exit((void*) tid);
          return NULL; 
        }          
      }
    }  
    // make header
    if (res==NULL) len=0;
    else len=strlen(res);
    write_header(header);
    write_header_clen(header,len); 
    // send result      
    i=writen(connsd,header,strlen(header));  
    if (res!=NULL) {
      i=writen(connsd,res,len);
      free(res);
    }      
    if (shutdown(connsd,SHUT_WR)<0) { // SHUT_RDWR
      // shutdown fails
      //fprintf(stderr, "Cannot shutdown connection: %s\n", strerror(errno));        
      if (close(connsd) < 0) { 
        warnprint("Cannot close connection after failed shutdown: %s\n", strerror(errno));          
      }          
    } else {    
      // normal shutdown
      if (len>=CLOSE_CHECK_THRESHOLD) {
        // for messages longer than CLOSE_CHECK_THRESHOLD check before closing
        for(;;) {
          itmp=read(connsd,buf,MAXLINE);
          if(itmp<0) {
            fprintf(stderr,"error %d reading after shutdown in thread %d\n",itmp,tid);
            break;            
          }
          if(!itmp)  break;
          nanosleep(&tim , &tim2);
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
      if (close(connsd) < 0) { 
        warnprint("Cannot close connection: %s\n", strerror(errno));        
      }               
    }
    // connection is now down 
    tdata->inuse=0;
    if ((tdata->realthread)==1) {
      // non-threadpool thread
      //fprintf(stderr,"exiting thread %d\n",tid);
      pthread_exit((void*) tid);
    } else if ((tdata->realthread)==2) {
      // threadpool thread
      //fprintf(stderr,"thread %d loop ended\n",tid);
    } else {
      // not a thread at all
      return NULL;
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
  bzero((char *) &saddr, sizeof(saddr));
  saddr.sin_family=AF_INET; 
  saddr.sin_addr.s_addr=htonl(INADDR_ANY); 
  saddr.sin_port=htons((unsigned short)port); 
  if (bind(sd,(struct sockaddr *)&saddr, sizeof(saddr)) < 0) return -1;
  // make a listening socket to accept requests
  if (listen(sd,HTTP_LISTENQ)<0) return -1;
  return sd;
}

ssize_t readlineb(int fd, void *usrbuf, size_t maxlen) {
  int n, rc;
  char c, *bufp = usrbuf;

  for (n = 1; n < maxlen; n++) { 
    if ((rc = readn(fd, &c, 1)) == 1) {
	    *bufp++ = c;
	    if (c == '\n') break;
    } else if (rc == 0) {
	    if (n == 1) return 0; // EOF, no data read 
	    else break;    // EOF, some data was read 
    } else return -1;	  // error 
  }
  *bufp = 0;
  return n;
}

ssize_t readn(int fd, void *usrbuf, size_t n)  {
  size_t nleft = n;
  ssize_t nread;
  char *bufp = usrbuf;
  
  while (nleft>0) {        
    if ((nread=read(fd, bufp, nleft)) < 0) {
        if (errno==EINTR) nread=0;/* interrupted by sig handler return */
            /* and call read() again */
        else return -1;      /* errno set by read() */ 
    } else if (nread==0) break;              /* EOF */
    nleft -= nread;
    bufp += nread;
  }
  return (n-nleft);         /* return >= 0 */
}


ssize_t writen(int fd, void *usrbuf, size_t n) {
  size_t nleft = n;
  ssize_t nwritten;
  char *bufp = usrbuf;
  
  while (nleft > 0) {
    if ((nwritten = write(fd, bufp, nleft)) <= 0) {
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

/*

    struct timeval timeout;      
    timeout.tv_sec = 10;
    timeout.tv_usec = 0;

    if (setsockopt (sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,
                sizeof(timeout)) < 0)
        error("setsockopt failed\n");

    if (setsockopt (sockfd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout,
                sizeof(timeout)) < 0)
        error("setsockopt failed\n");

Edit: from the setsockopt man page:

SO_SNDTIMEO is an option to set a timeout value for output operations. It accepts a struct timeval parameter with the number of seconds and microseconds used to limit waits for output operations to complete. If a send operation has blocked for this much time, it returns with a partial count or with the error EWOULDBLOCK if no data were sent. In the current implementation, this timer is restarted each time additional data are delivered to the protocol, implying that the limit applies to output por- tions ranging in size from the low-water mark to the high-water mark for output.

SO_RCVTIMEO is an option to set a timeout value for input operations. It accepts a struct timeval parameter with the number of seconds and microseconds used to limit waits for input operations to complete. In the current implementation, this timer is restarted each time additional data are received by the protocol, and thus the limit is in effect an inactivity timer. If a receive operation has been blocked for this much time without receiving additional data, it returns with a short count or with the error EWOULDBLOCK if no data were received. The struct timeval parameter must represent a positive time interval; otherwise, setsockopt() returns with the error EDOM.

up vote 2 down vote
	

am not sure if I fully understand the issue, but guess it's related to the one I had, am using Qt with TCP socket communication, all non-blocking, both Windows and Linux..

wanted to get a quick notification when an already connected client failed or completely disappeared, and not waiting the default 900+ seconds until the disconnect signal got raised. The trick to get this working was to set the TCP_USER_TIMEOUT socket option of the SOL_TCP layer to the required value, given in milliseconds.

this is a comparably new option, pls see http://tools.ietf.org/html/rfc5482, but apparently it's working fine, tried it with WinXP, Win7/x64 and Kubuntu 12.04/x64, my choice of 10 s turned out to be a bit longer, but much better than anything else I've tried before ;-)

the only issue I came across was to find the proper includes, as apparently this isn't added to the standard socket includes (yet..), so finally I defined them myself as follows:

#ifdef WIN32
    #include <winsock2.h>
#else
    #include <sys/socket.h>
#endif

#ifndef SOL_TCP
    #define SOL_TCP 6  // socket options TCP level
#endif
#ifndef TCP_USER_TIMEOUT
    #define TCP_USER_TIMEOUT 18  // how long for loss retry before timeout [ms]
#endif

setting this socket option only works when the client is already connected, the lines of code look like:

int timeout = 10000;  // user timeout in milliseconds [ms]
setsockopt (fd, SOL_TCP, TCP_USER_TIMEOUT, (char*) &timeout, sizeof (timeout));

and the failure of an initial connect is caught by a timer started when calling connect(), as there will be no signal of Qt for this, the connect signal will no be raised, as there will be no connection, and the disconnect signal will also not be raised, as there hasn't been a connection yet..


*/