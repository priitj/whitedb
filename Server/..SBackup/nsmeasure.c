/*

testing the speed of answering queries via net

compile by
gcc nsmeasure.c -o nsmeasure -O2 -lpthread

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

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <resolv.h>
#include <errno.h>
#include <time.h> // nanosleep

#define MAX_THREADS 5000
#define INITIALBUFS  1000
#define MAXHEADERS  500
#define MAXBUFS  100000000
#define TRACE 


void errhalt(char *msg);
void *process(void *targ);
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
  pthread_t threads[MAX_THREADS];   
  struct thread_data tdata[MAX_THREADS];
  pthread_attr_t attr;
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
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);    
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
      rc=pthread_create(&threads[tid], &attr, process, (void *) &tdata[tid]);               
      //tmax=0;
      //process((void *) &tdata[tid]);
    }    
    // wait for all threads to finish
    for(tid=0;tid<tmax;tid++) {
      pthread_join(threads[tid],NULL);
      //printf("thread %d finished with res %d\n",
      //        tdata[tid].thread_id,tdata[tid].res);
    }  
    // printf("iteration %d finished\n",iter);
  }  
  printf("\nall iterations finished\n");  
  pthread_exit(NULL);
  return 0;
}

void *process(void *targ) {
  struct thread_data *tdata; 
  int i,j,tid,iter,maxiter,len,clenheader_len,toread,nread;
  int sockfd, bytes_read, ok;
  struct sockaddr_in dest;
  char *buffer, *bufp, *tp;
  char *loc,*clenheader;
  struct timespec tim, tim2;
  
  tdata=(struct thread_data *) targ;    
  tid=tdata->thread_id;
  printf("thread %d starts, ip %s portnr %d url %s \n",tid,tdata->ip,tdata->port,tdata->urlpart);
  buffer=malloc(INITIALBUFS);
  
  //pthread_exit((void*) tid);
  maxiter=tdata->maxiter;
  clenheader="Content-Length:";
  clenheader_len=strlen(clenheader);
  // setup nanosleep for 100 microsec
  tim.tv_sec = 0;
  tim.tv_nsec = 100000;
  for(iter=0; iter<maxiter; iter++) { 
    i=0;    
    if ( (sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
      errhalt("Socket"); 
    }      
    // Initialize server address/port struct
    bzero(&dest, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(tdata->port); 
    if ( inet_addr(tdata->ip, &dest.sin_addr.s_addr) == 0 )
        errhalt("inet_addr problem");

    // Connect to server
    ok=0;
    for(j=0;j<100;j++) { 
      if ( connect(sockfd, (struct sockaddr*)&dest, sizeof(dest)) != 0 ) {
        // problem here
        nanosleep(&tim , &tim2);
      } else {
        ok=1;
        break;
      }                    
    }  
    if (!ok) { errhalt("connect problem"); }    
    
    //printf("thread %d connecting iteration %d\n",tid,iter);
    sprintf(buffer, "GET /%s HTTP/1.0\n\n", tdata->urlpart);
    write(sockfd, buffer, strlen(buffer));

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
        close(sockfd);
        free(buffer);
        printf("thread %d received a non-200 http result at iter %d, exiting\n",tid,iter);
        pthread_exit((void*) tid);
      }        
    } else {
      bufp=buffer;
      // first read the main part of the header
      
      toread=INITIALBUFS-10;
      nread=readn(sockfd, bufp, toread);       
      /*
      printf("nread: %d toread: %d\n",nread,toread); 
      bufp+=nread;
      *bufp='\0';      
      printf("%s\n",buffer);
      exit(0);
      */
      if (nread<0) {
        close(sockfd);
        free(buffer);
        printf("thread %d did not succeed to read at all at iter %d, exiting\n",tid,iter);
        pthread_exit((void*) tid);
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
          close(sockfd);
          free(buffer);
          printf("thread %d did not find Content-Length at iter %d, exiting\n",tid,iter);
          pthread_exit((void*) tid);
        }
        tp=loc+strlen("Content-length: ");
        len=atoi(tp);
        //printf("len %d\n",len);
        if (len<=0) {
          close(sockfd);
          free(buffer);
          printf("thread %d got a zero or less Content-Length at iter %d, exiting\n",tid,iter);
          pthread_exit((void*) tid);
        }
        if (len+MAXHEADERS>MAXBUFS) {
          close(sockfd);
          free(buffer);
          printf("thread %d got a too big Content-Length at iter %d, exiting\n",tid,iter);
          pthread_exit((void*) tid);
        }
        if (len>(INITIALBUFS-MAXHEADERS)) {
          j=bufp-buffer;
          buffer=realloc(buffer,len+MAXHEADERS);
          if (buffer==NULL) {
            close(sockfd);
            printf("thread %d failed to alloc enough memory at iter %d, exiting\n",tid,iter);
            pthread_exit((void*) tid);
          }
          bufp=buffer+j;
        }
        //printf("nr %d |%s|\n",len,tp);
        //printf("buffer:\n----\n%s\n----\n",buffer); 
        loc=strstr(buffer,"\r\n\r\n");
        if (loc==NULL) {
          close(sockfd);
          free(buffer);
          printf("thread %d did not find empty line at iter %d, exiting\n",tid,iter);
          pthread_exit((void*) tid);
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
        close(sockfd);
        free(buffer);
        printf("thread %d received a non-200 http result at iter %d, exiting\n",tid,iter);
        pthread_exit((void*) tid);
      }
      if (strstr(buffer,tdata->verify)==NULL) { 
        close(sockfd);
        free(buffer);
        printf("thread %d received a non-verified http result at iter %d, exiting\n",tid,iter);
        pthread_exit((void*) tid);
      }      
      
    }            
    // Clean up
    shutdown(sockfd,SHUT_RDWR);
    close(sockfd);
    //shutdown(sockfd,3);
  }
  // end thread
  tdata->res=i; 
  //printf ("thread %d finishing with res %d \n",tid,i);
  free(buffer);
  pthread_exit((void*) tid);
}



static ssize_t readn(int fd, void *usrbuf, size_t n)  {
  size_t nleft = n;
  ssize_t nread;
  char *bufp = usrbuf;

  while (nleft>0) {
    if ((nread=read(fd, bufp, nleft)) < 0) {
        if (errno==EINTR) nread=0;/* interrupted by sig handler return */
            /* and call read() again */
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
  exit(-1);
}

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

*/
