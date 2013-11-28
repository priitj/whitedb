/*

traversing a flat pre-built (in speed20.c) pointer list of 10 million records
of 5 fields in a 1 GB database using a ctrl record to scan 
different segments of the list in parallel.

Nr of segments is built by speed20: control the number there.

Compile with 

gcc speed21.c -o speed21 -O2 -lwgdb -lpthread

Ctrl record (see speed20.c) fields:
 0: type (unused)
 1: ptr field (3 here)
 2: start pointer
 3: midpointer0
 4: midpointer1
 ...
 N: ptr to last record
 N+1: NULL

Database was created earlier by speed20 and is not freed here.

one thread:

real	0m0.110s
user	0m0.064s
sys	0m0.045s

two threads:

real	0m0.064s
user	0m0.071s
sys	0m0.048s

three threads:

real	0m0.047s
user	0m0.073s
sys	0m0.052s

four threads:

real	0m0.041s
user	0m0.072s
sys	0m0.057s

eight threads:

real	0m0.032s
user	0m0.097s
sys	0m0.062s

sixteen threads

real	0m0.033s
user	0m0.088s
sys	0m0.076s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#define RECORD_HEADER_GINTS 3
#define wg_field_addr(db,record,fieldnr) (((wg_int*)record)+RECORD_HEADER_GINTS+fieldnr)

#define MAX_THREADS 100
#define DB_NAME "20"

void *process(void *targ);

struct thread_data{
  int    thread_id; // 0,1,..
  void*  db;     // db handler
  wg_int fptr;   // first pointer
  wg_int lptr;   // last pointer
  int    ptrfld; // pointer field in rec
  int    res;    // stored by thread
};

int main(int argc, char **argv) {
  void *db, *ctrl, *rec;
  char *name=DB_NAME;
  int i,ptrfld,ptrs,rc;  
  wg_int encptr,tmp,first,next,last;
  pthread_t threads[MAX_THREADS];   
  struct thread_data tdata[MAX_THREADS];
  pthread_attr_t attr;
  long tid;
  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  
  // get values from cntrl record
  ctrl=wg_get_first_record(db);  
  ptrfld=wg_decode_int(db,wg_get_field(db,ctrl,1));
  first=wg_get_field(db,ctrl,2);
  for(ptrs=0;ptrs<10000;ptrs++) {
    tmp=wg_get_field(db,ctrl,3+ptrs);
    if ((int)tmp==0) break;          
    last=tmp;
  }
  printf("\nsegments found: %d \n",ptrs);
  if (ptrs>=MAX_THREADS) {
    printf("List segment nr larger than MAX_THREADS, exiting\n");
    wg_detach_database(db);  
    pthread_exit(NULL);
    return 0;
  }
  // prepare and create threads
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  
  for(tid=0;tid<ptrs;tid++) {   
    first=wg_get_field(db,ctrl,2+tid);
    next=wg_get_field(db,ctrl,2+tid+1);
    tdata[tid].thread_id=tid;
    tdata[tid].db=db;
    tdata[tid].fptr=first;
    tdata[tid].lptr=next;
    tdata[tid].ptrfld=ptrfld;
    tdata[tid].res=0;    
    rc=pthread_create(&threads[tid], &attr, process, (void *) &tdata[tid]);    
  }  

  // wait for all threads to finish
  for(tid=0;tid<ptrs;tid++) {
    pthread_join(threads[tid],NULL);
    //printf("thread %d finished with res %d\n",
    //        tdata[tid].thread_id,tdata[tid].res);
  }  
  printf("\nall threads finished\n"); 
  wg_detach_database(db);  
  pthread_exit(NULL);
  return 0;
}

void *process(void *targ) {
  struct thread_data *tdata; 
  void *db,*rec,*lptr;
  wg_int encptr;
  int i,tid,ptrfld;
     
  tdata=(struct thread_data *) targ;    
  tid=tdata->thread_id;
  db=tdata->db;
  ptrfld=tdata->ptrfld;  
  rec=wg_decode_record(db,tdata->fptr);
  lptr=wg_decode_record(db,tdata->lptr);
  
  printf("thread %d starts, first el value %d \n",tid,
         wg_decode_int(db,wg_get_field(db,rec,1)));
  i=1;
  while(1) {
    encptr=*(wg_field_addr(db,rec,ptrfld)); // encptr is not yet decoded   
    rec=wg_decode_record(db,encptr); // get a pointer to the previous record
    if (rec==lptr) break;
    i++;    
  } 
  tdata->res=i; 
  printf ("thread %d finishing with res %d \n",tid,i);
  pthread_exit((void*) tid);
}
