/*

call with 
speed20 N
where N is the number of segments (N >= 1), default 1

compile with 
gcc speed21.c -o speed21 -O2 -lwgdb -lpthread

creating a flat pointer list of 10 million records of 5 fields in
a 1 GB database: 
store a pointer to the previous record to field 3 of each record,
thus first list elems are actually created last.

Store lstlen-i to field 1. 

Additionally create a ctrl record with pointers to the middle of the list,
to be used for parallel multicore scanning of the list later.
Ctrl record fields:
 0: type (unused)
 1: ptr field (3 here)
 2: start pointer
 3: midpointer0
 4: midpointer1
 ...
 N: ptr to last record
 N+1: NULL

Observe that we use standard C int 0 for the NULL pointer,
this is also what wg_encode_null(db,0) always gives.

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

#define DB_NAME "20"

int main(int argc, char **argv) {
  void *db, *rec, *ctrlrec, *firstrec, *lastrec;
  char *name=DB_NAME;
  int i;  
  int lstlen=10000000; // total nr of elems in list
  int ptrfld=3; // field where a pointer is stored  
  int segnr=1; // total number of segments
  int midcount=0; // middle ptr count  
  int midlasti=0; // last i where midpoint stored
  int midseglen; // mid segment length
  wg_int tmp;
  
  if (argc>=2) {
    segnr=atoi(argv[1]);
  }
  printf("creating a list with %d segments \n",segnr);
  midseglen=lstlen/segnr; // mid segment length
  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  ctrlrec = wg_create_record(db, 1000); // this will contain info about the list
  // build the list
  firstrec=wg_create_raw_record(db, 5);
  lastrec=firstrec;
  // next ptr from the last record is an encoded NULL pointer
  wg_set_new_field(db,firstrec,ptrfld,wg_encode_null(db,0));  
  wg_set_new_field(db,firstrec,1,wg_encode_int(db,lstlen));
  for(i=1;i<lstlen;i++) {   
    rec = wg_create_raw_record(db, 5);
    if (!rec) { printf("record creation failed \n"); exit(0); }   
    // store a pointer to the previously built record
    wg_set_new_field(db,rec,ptrfld,wg_encode_record(db,lastrec));
    wg_set_new_field(db,rec,1,wg_encode_int(db,lstlen-i));
    lastrec=rec;
    // check if the pointer should be stored as as a midptr
    // observe the last segment may be slightly longer
    // example: lstlen=11, midnr=1: midseglen=5, midpoint0=5, at i=10 11-10<5
    // example: lstlen=10, midnr=2: midseglen=3, midpoint0=3, midpoint1=6 at i=9 10-9<3
    if (i-midlasti==midseglen && lstlen-i>=midseglen) {
      // this lst is built from end to beginning
      wg_set_field(db,ctrlrec,2+(segnr-1)-midcount,wg_encode_record(db,rec));
      printf("\nmidpoint %d at i %d to field %d val %d",midcount,i,2+(segnr-1)-midcount, 
        (int)(db,wg_get_field(db,ctrlrec,2+(segnr-1)-midcount)));
      midlasti=i;
      midcount++;
    }          
  }  
  // set ctrlrec fields: type,ptr field,first pointer,midpointer1,midpointer2,...
  wg_set_field(db,ctrlrec,0,wg_encode_int(db,1)); // type not used
  wg_set_field(db,ctrlrec,1,wg_encode_int(db,ptrfld)); // ptrs at field 3
  wg_set_field(db,ctrlrec,2,wg_encode_record(db,lastrec)); // lst starts here
  wg_set_field(db,ctrlrec,2+segnr,wg_encode_record(db,firstrec)); // last record in a list
  printf("\nfinal i %d\n", i);
  printf("\nctrl rec ptr fld val %d \n",wg_decode_int(db,wg_get_field(db,ctrlrec,1)));
  for(i=0;i<1000;i++) {
    tmp=wg_get_field(db,ctrlrec,2+i);
    if (!(int)tmp) break;
    printf("ptr %d value %d content %d\n",i,(int)tmp,
       wg_decode_int(db,wg_get_field(db,wg_decode_record(db,tmp),1)) ); 
  }    
  wg_detach_database(db);
  return 0;
}
