/*

creating a flat pointer list of 10 million records of 5 fields in
a 1 GB database: 
store a pointer to the previous record to field 3 of each record,
store a pointer to the last record to field 2 of first record.

Observe that we use standard C int 0 for the NULL pointer,
this is also what wg_encode_null(db,0) always gives.

real	0m0.666s
user	0m0.516s
sys	0m0.150s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec, *firstrec, *lastrec;
  char *name="15";
  int i;  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  rec = wg_create_raw_record(db, 5);
  firstrec=rec; // store for use in the end
  lastrec=rec; 
  for(i=1;i<10000000;i++) {   
    rec = wg_create_raw_record(db, 5);
    if (!rec) { printf("record creation failed \n"); exit(0); }   
    // store a pointer to the previously built record
    wg_set_new_field(db,rec,3,wg_encode_record(db,lastrec));   
    lastrec=rec;
  }  
  // field 3 of the first record will be an encoded NULL pointer
  // which is always just (wg_int)0
  wg_set_new_field(db,firstrec,3,wg_encode_null(db,0));
  // field 2 of the first record will be a pointer to the last record
  wg_set_new_field(db,firstrec,2,wg_encode_record(db,lastrec));
  printf("i %d\n", i);
  wg_detach_database(db);
  return 0;
}
