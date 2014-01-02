/*

testing the speed of answering queries via net

compile by
linux: gcc nsmeasure.c -o nsmeasure -O2 -lpthread
windows: cl /Ox /I"." Server\nsmeasure.c wgdb.lib

run like
nsmeasure 'http://127.0.0.1:8080/dserve?op=search' 4 1000 '5689'

where:
- url must use http
- url must contain a numeric IP, not a domain name
- url must contain the port number
- 4 indicates the number of parallel threads (use 1,2,... etc)
- 1000 indicates the number each thread opens the connection
- '5689' is a string searched for from each result (thus testing result is ok)
  you may skip this last string param altogether  

nsmeasure exits immediately when it sees an error or any result
does not contain the string searched for.


*/

#if _MSC_VER
// see http://msdn.microsoft.com/en-us/library/windows/desktop/ms738566%28v=vs.85%29.aspx
#define WIN32_LEAN_AND_MEAN
#include <windows.h> // windows.h only with lean_and_mean before it
#include <winsock2.h>
//#include <ws2tcpip.h>
#include <tchar.h>
#include <strsafe.h>
#pragma comment (lib, "ws2_32.lib")
#pragma comment (lib, "User32.lib")
#endif

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>
#include <errno.h>
#include <time.h> // nanosleep for linux

#if _MSC_VER
#else
#include <resolv.h>
#include <sys/socket.h>
#include <pthread.h>
#endif

#define MAX_THREADS 5000
#define INITIALBUFS  1000
#define MAXHEADERS  500
#define MAXBUFS  100000000
#define TRACE 


#if _MSC_VER
#define ssize_t int
DWORD WINAPI process(LPVOID targ);
void usleep(__int64 usec);
void win_err_handler(LPTSTR lpszFunction);
#else   
void *process(void *targ);
#endif 
void errhalt(char *msg);
void err_exit(int sockfd, char *buffer, int tid);
static ssize_t readn(int fd, void *usrbuf, size_t n);


struct thread_data{
  int    thread_id; // 0,1,..
  int    maxiter; // how many iterations to run
  char*  ip; // ip to open
  int    port;  // port to open
  char*  urlpart;  // urlpart to open like /dserve?op=search
  char*  verify; // string to look for
  int    res;    // stored by thread
};

int main(int argc, char **argv) {
  int i,tmax,iter,maxiter,rc;  
  char ip[100], urlpart[2000];
  int portnr=0;  
#if _MSC_VER
  WSADATA wsaData;
  HANDLE thandle;
  HANDLE thandlearray[MAX_THREADS];
  DWORD threads[MAX_THREADS];
#else  
  pthread_t threads[MAX_THREADS];
  pthread_attr_t attr;
#endif  
  struct thread_data tdata[MAX_THREADS];
  long tid;
  char* verify=NULL;
  
  if (argc<4) 
    errhalt("three obligatory args - url, threads, iter - like: \
 'http://127.0.0.1:8080/dserve?op=search' 2 3  - plus one optional: string to look for in results");
  if (sscanf(argv[1], "http://%99[^:]:%i/%999[^\n]", ip, &portnr, urlpart) != 3) { 
    errhalt("could not parse the url");
  }
  if (!portnr) errhalt("nonnumeric or zero port given");
  tmax=atoi(argv[2]);
  if (!tmax) errhalt("nonnumeric or zero thread count given");
  maxiter=atoi(argv[3]);
  if (!maxiter) errhalt("nonnumeric or zero iteration count given");
  if (tmax>=MAX_THREADS) errhalt("too many threads to be created"); 
  if (argc==5) {
    verify=argv[4];    
    printf ("first line must contain 200 and the body must contain %s\n",verify);
  } else {
    verify=NULL;
    printf("no result verification to be done except 200 check in the first line  \n");
  }
   
  printf("starting %d threads to run %d iterations each\n",tmax,maxiter); 
  // prepare and create threads
#if _MSC_VER
  if (WSAStartup(MAKEWORD(2, 0),&wsaData) != 0) {
    printf("WSAStartup failed\n");
    exit(1);
  }  
#else  
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);    
#endif  
  for(iter=0; iter<1; iter++) {
    // run just once    
    for(tid=0;tid<tmax;tid++) {   
      tdata[tid].thread_id=tid;
      tdata[tid].maxiter=maxiter;
      tdata[tid].ip=ip;
      tdata[tid].port=portnr;
      tdata[tid].urlpart=urlpart;
      tdata[tid].verify=verify;
      tdata[tid].res=0;        
#if _MSC_VER
      thandle=CreateThread(NULL, 0, process, (void *) &tdata[tid], 0, &threads[tid]);
      /*
            NULL,                   // default security attributes
            0,                      // use default stack size  
            process,       // thread function name
            &tdata[tid],          // argument to thread function 
            0,                      // use default creation flags 
            &threads[tid];   // returns the thread identifier 
      */      
      if (thandle==NULL) {
        win_err_handler(TEXT("CreateThread"));
        ExitProcess(3);
      } else {
        thandlearray[tid]=thandle;
      }       
#else      
      rc=pthread_create(&threads[tid], &attr, process, (void *) &tdata[tid]);               
#endif      
      //tmax=0;
      //process((void *) &tdata[tid]);
    }    
    
#if _MSC_VER
    WaitForMultipleObjects(tmax, thandlearray, TRUE, INFINITE);
    for(tid=0; tid<tmax; tid++) {
        if (thandlearray[tid]!=NULL) CloseHandle(thandlearray[tid]);
       /*
        if(pDataArray[i] != NULL) {
            HeapFree(GetProcessHeap(), 0, pDataArray[i]);
            pDataArray[i] = NULL;    // Ensure address is not reused.
        }
       */
    }
#else              
    // wait for all threads to finish
    for(tid=0;tid<tmax;tid++) {      
      pthread_join(threads[tid],NULL);      
      //printf("thread %d finished with res %d\n",
      //        tdata[tid].thread_id,tdata[tid].res);
    }  
    // printf("iteration %d finished\n",iter);
#endif    
  }  
  printf("\nall iterations finished\n");  
#if _MSC_VER
  WSACleanup();
#else    
  pthread_exit(NULL);
#endif  
  return 0;
}

#if _MSC_VER
DWORD WINAPI process(LPVOID targ) {
#else   
void *process(void *targ) {
#endif  
  struct thread_data *tdata; 
  int i,j,k,tid,iter,maxiter,len,clenheader_len,toread,nread;
  int sockfd, bytes_read, ok;
  struct sockaddr_in dest;
  char *buffer, *bufp, *tp;
  char *loc,*clenheader;
#if _MSC_VER
#else  
  // setup nanosleep for 100 microsec
  struct timespec tim, tim2;
  
  tim.tv_sec = 0;
  tim.tv_nsec = 100000;
#endif
  
  tdata=(struct thread_data *) targ;    
  tid=tdata->thread_id;
  printf("thread %d starts, ip %s portnr %d url %s \n",tid,tdata->ip,tdata->port,tdata->urlpart);
  buffer=malloc(INITIALBUFS);
  
  //pthread_exit((void*) tid);
  maxiter=tdata->maxiter;
  clenheader="Content-Length:";
  clenheader_len=strlen(clenheader); 
  for(iter=0; iter<maxiter; iter++) { 
    i=0;    
    if ( (sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
      errhalt("Socket"); 
    }      
    // Initialize server address/port struct
    memset(&dest,0,sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(tdata->port); 
#if _MSC_VER
    dest.sin_addr.s_addr=inet_addr(tdata->ip);
    if (dest.sin_addr.s_addr==0 || dest.sin_addr.s_addr==INADDR_NONE )
#else
    if (inet_addr(tdata->ip, &dest.sin_addr.s_addr)==0)
#endif        
        errhalt("inet_addr problem");

    // Connect to server
    ok=0;
    for(j=0;j<100;j++) { 
      if ( connect(sockfd, (struct sockaddr*)&dest, sizeof(dest)) != 0 ) {
        // problem here
#if _MSC_VER
        usleep(100);
#else         
        nanosleep(&tim , &tim2);
#endif        
      } else {
        ok=1;
        break;
      }                    
    }  
    if (!ok) { errhalt("connect problem"); }    
    
    //printf("thread %d connecting iteration %d\n",tid,iter);
    sprintf(buffer, "GET /%s HTTP/1.0\n\n", tdata->urlpart);
    send(sockfd, buffer, strlen(buffer),0);

    len=0;
    if (tdata->verify==NULL) {
      // read first line plus a bit more
      bufp=buffer;
      //memset(buffer,0,22);
      while (1) {
        if ((nread=read(sockfd, bufp, 20)) < 0) {
          if (errno==EINTR) nread=0;/* interrupted by sig handler return */
          else break;
        } else {
          break;
        }        
      }    
      *(bufp+nread)='\0';
      if (strstr(buffer,"200")==NULL) { 
        printf("thread %d received a non-200 http result at iter %d, exiting\n",tid,iter);
        err_exit(sockfd,buffer,tid);       
      }        
    } else {
      bufp=buffer;
      // first read the main part of the header
      
      toread=INITIALBUFS-10;  
      for(k=0;k<10;k++) {
        nread=readn(sockfd, bufp, toread);
        if (nread>0) break;
        usleep(k*100);
        printf("thread %d read try %d at iter %d\n",tid,k,iter);
      }      
      /*
      printf("nread: %d toread: %d\n",nread,toread); 
      bufp+=nread;
      *bufp='\0';      
      printf("%s\n",buffer);
      exit(0);
      */
      if (nread<0) {       
        printf("thread %d did not succeed to read at all at iter %d, exiting\n",tid,iter);
        err_exit(sockfd,buffer,tid);
      } else if (nread<toread) {
        // ok, read all we had to
        bufp+=nread;
        *bufp='\0';
      } else {        
        // should read more
        bufp+=nread;
        *bufp='\0';
        // get the content-length
        loc=strstr(buffer,"Content-Length: ");
        if (loc==NULL) loc=strstr(buffer,"content-length: ");
        if (loc==NULL) {
          printf("thread %d did not find Content-Length at iter %d, exiting\n",tid,iter);
          err_exit(sockfd,buffer,tid);   
        }
        tp=loc+strlen("Content-length: ");
        len=atoi(tp);
        printf("len %d\n",len);
        if (len<=0) {
          printf("thread %d got a zero or less Content-Length at iter %d, exiting\n",tid,iter);
          err_exit(sockfd,buffer,tid);  
        }
        if (len+MAXHEADERS>MAXBUFS) {
          printf("thread %d got a too big Content-Length at iter %d, exiting\n",tid,iter);
          err_exit(sockfd,buffer,tid); 
        }
        if (len>(INITIALBUFS-MAXHEADERS)) {
          j=bufp-buffer;
          buffer=realloc(buffer,len+MAXHEADERS);
          if (buffer==NULL) {
            printf("thread %d failed to alloc enough memory at iter %d, exiting\n",tid,iter);
            err_exit(sockfd,buffer,tid);
          }
          bufp=buffer+j;
        }
        //printf("nr %d |%s|\n",len,tp);
        //printf("buffer:\n----\n%s\n----\n",buffer); 
        loc=strstr(buffer,"\r\n\r\n");
        if (loc==NULL) {
          printf("thread %d did not find empty line at iter %d, exiting\n",tid,iter);
          err_exit(sockfd,buffer,tid);
        }
        // have to read content_length+header_length-read_already
        toread=len+((loc-buffer)+4)-nread;
        //printf("toread: %d \n",toread);        
        nread=readn(sockfd, bufp, toread); 
        //printf("nread: %d\n",nread);
        *(bufp+nread)='\0';
      }             
      //printf("j %d buffer:\n----\n%s\n----\n",j,buffer); 
      if (strstr(buffer,"200")==NULL) { 
        printf("thread %d received a non-200 http result at iter %d, exiting\n",tid,iter);
        printf("j %d buffer:\n----\n%s\n----\n",j,buffer); 
        err_exit(sockfd,buffer,tid);
      }
      if (strstr(buffer,tdata->verify)==NULL) { 
        printf("thread %d received a non-verified http result at iter %d, exiting\n",tid,iter);
        err_exit(sockfd,buffer,tid);
      }      
      
    }            
    // Clean up
#if _MSC_VER    
    shutdown(sockfd,SD_RECEIVE);
    closesocket(sockfd);
#else    
    shutdown(sockfd,SHUT_RDWR);
    close(sockfd);
#endif        
    //shutdown(sockfd,3);
  }
  // end thread
  tdata->res=i; 
  //printf ("thread %d finishing with res %d \n",tid,i);
  free(buffer);
#if _MSC_VER
  ExitThread(0);
  return 0;
#else     
  pthread_exit((void*) tid);
  return NULL;
#endif  
}

void err_exit(int sockfd, char *buffer, int tid) {    
#if _MSC_VER
  shutdown(sockfd,SD_RECEIVE);
  closesocket(sockfd);
  if (buffer!=NULL) free(buffer);
  ExitThread(1);
#else     
  shutdown(sockfd,SHUT_RDWR);
  close(sockfd);
  if (buffer!=NULL) free(buffer);
  pthread_exit((void*) tid);
#endif  
}  

static ssize_t readn(int fd, void *usrbuf, size_t n)  {
  size_t nleft = n;
  ssize_t nread;
  char *bufp = usrbuf;

  while (nleft>0) {
    if ((nread=recv(fd, bufp, nleft, 0)) < 0) {
        if (errno==EINTR) nread=0;/* interrupted by sig handler return */
            /* and call recv() again */
        else {
          //fprintf(stderr,"read %d err %d \n",nread,errno);
          return -1;      /* errno set by read() */ 
        }  
    } else if (nread==0) break;              /* EOF */
    nleft -= nread;
    bufp += nread;
  }
  //fprintf(stderr,"readn terminates with %d\n",n-nleft);
  return (n-nleft);         /* return >= 0 */
}

void errhalt(char *str) {
  printf("Error: %s\n",str);
#if _MSC_VER
  WSACleanup();
#endif  
  exit(-1);
}



#if _MSC_VER
void usleep(__int64 usec) { 
  HANDLE timer; 
  LARGE_INTEGER ft; 

  ft.QuadPart = -(10*usec); // Convert to 100 nanosecond interval, negative value indicates relative time

  timer = CreateWaitableTimer(NULL, TRUE, NULL); 
  SetWaitableTimer(timer, &ft, 0, NULL, NULL, 0); 
  WaitForSingleObject(timer, INFINITE); 
  CloseHandle(timer); 
}
#endif

#if _MSC_VER
void win_err_handler(LPTSTR lpszFunction)  { 
  // Retrieve the system error message for the last-error code.

  LPVOID lpMsgBuf;
  LPVOID lpDisplayBuf;
  DWORD dw = GetLastError(); 

  FormatMessage(
    FORMAT_MESSAGE_ALLOCATE_BUFFER | 
    FORMAT_MESSAGE_FROM_SYSTEM |
    FORMAT_MESSAGE_IGNORE_INSERTS,
    NULL,
    dw,
    MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
    (LPTSTR) &lpMsgBuf,
    0, NULL );

  // Display the error message.

  lpDisplayBuf = (LPVOID)LocalAlloc(LMEM_ZEROINIT, 
    (lstrlen((LPCTSTR) lpMsgBuf) + lstrlen((LPCTSTR) lpszFunction) + 40) * sizeof(TCHAR)); 
  StringCchPrintf((LPTSTR)lpDisplayBuf, 
    LocalSize(lpDisplayBuf) / sizeof(TCHAR),
    TEXT("%s failed with error %d: %s"), 
    lpszFunction, dw, lpMsgBuf); 
  MessageBox(NULL, (LPCTSTR) lpDisplayBuf, TEXT("Error"), MB_OK); 

  // Free error-handling buffer allocations.

  LocalFree(lpMsgBuf);
  LocalFree(lpDisplayBuf);
}
#endif
/*

Timings on an i7 laptop:

parallel mode
------------

measuring wdb query on localhost:

query is dserve?op=search giving all dbase rows

a) small: db size is 3 rows of 3 cols of ints, output ca 120 bytes
b) medium: db size is 1000 rows of 3 cols of ints, output ca 17 kilobytes
c) large: db size is 100.000 rows of 3 cols of ints, output ca 2.3 megabytes

running N threads querying over http in parallel, each M iterations

1 thread 10.000 iterations:
- - - - - - - - - - - - - -

iterative server:

small:
real	0m0.840s
user	0m0.026s
sys	0m0.427s
medium:
real	0m6.793s
user	0m0.144s
sys	0m0.773s

server with 8 threads:

small:
real	0m1.416s
user	0m0.019s
sys	0m0.495s
medium:
real	0m6.777s
user	0m0.156s
sys	0m0.720s

threadpool with 8 threads

small without close check
real	0m0.840s
user	0m0.020s
sys	0m0.444s
small with close check
real	0m1.464s
user	0m0.035s
sys	0m0.502s

2 threads 10.000 iterations:
- - - - - - - - - - - - - -

iterative server:

small:
real	0m0.951s
user	0m0.038s
sys	0m0.863s
medium:
real	0m10.622s
user	0m0.264s
sys	0m1.200s

server with 8 threads:

small:
real	0m2.376s
user	0m0.053s
sys	0m1.330s
medium:
real	0m6.698s
user	0m0.252s
sys	0m1.558s

threadpool with 8 threads

small without close check:
real	0m0.985s
user	0m0.053s
sys	0m0.990s
small with close check:
real	0m2.964s
user	0m0.053s
sys	0m1.116s

3 threads 10.000 iterations:
- - - - - - - - - - - - - -

iterative server:

small:
real	0m1.378s
user	0m0.050s
sys	0m1.303s
medium:
real	0m16.007s
user	0m0.411s
sys	0m1.778s

server with 8 threads:

small:
real	0m3.389s
user	0m0.100s
sys	0m2.213s
medium:
real	0m7.148s
user	0m0.394s
sys	0m2.381s

threadpool with 8 threads

real	0m4.439s
user	0m0.088s
sys	0m1.795s


4 threads 10.000 iterations:
- - - - - - - - - - - - - -

iterative server:

small:
real	0m1.822s
user	0m0.077s
sys	0m1.723s
medium:
real	0m21.363s
user	0m0.520s
sys	0m2.392s

server with 8 threads:

small:
real	0m4.589s
user	0m0.140s
sys	0m3.112s
medium:
real	0m9.372s
user	0m0.582s
sys	0m3.065s

threadpool with 8 threads 


small without closing check
real	0m1.398s
user	0m0.130s
sys	0m2.534s
small with closing check
real	0m5.913s
user	0m0.117s
sys	0m2.571s
medium:
real	0m9.139s
user	0m0.472s
sys	0m2.933s

8 threads 10.000 iterations:
- - - - - - - - - - - - - -

iterative server:

small:
real	0m3.695s
user	0m0.138s
sys	0m3.514s
medium:
real	0m42.736s
user	0m1.232s
sys	0m4.712s

server with 8 threads:

small:
real	0m9.324s
user	0m0.272s
sys	0m6.227s
medium:
connection problems occur after a while, server refuses to answer for ca 
10 secs, then automagically starts working ok again
survives OK with 4 server threads, though:
real	0m28.006s
user	0m0.309s

threadpool with 8 threads

small without close check:
real	0m3.324s
user	0m0.268s
sys	0m5.139s
small with close check:
sys	0m6.928s
real	0m11.897s
user	0m0.222s
sys	0m5.187s
medium:
real	0m18.915s
user	0m0.309s
sys	0m6.068s


burst mode for 
-----------

creating 100.000 empty threads (no connections done) sequentially

real	0m1.454s
user	0m0.461s
sys	0m1.156s

creating 10 empty thread batches 10.000 times:

real	0m1.358s
user	0m0.774s
sys	0m1.999s

creating 100 empty thread batches 1000 times:

real	0m1.544s
user	0m0.808s
sys	0m2.190s


Windows
========

1 thread 1000 calls with a small dataset:

threaded server: 0.5 sec
iterative server: 1 sec

8 threads 1000 calls each with a small dataset:

threaded server: 2 sec
iterative server: 8 sec

8 threads 10 calls each with a large dataset (100K rows, 2.34MB)

threaded server: 2 sec
iterative server: 7.4 sec

*/
