/*

traversing a flat pre-built (in speed15.c) pointer list of 10 million records
of 5 fields in a 1 GB database: 
a pointer to the previous record is stored in field 3 of each record,
a pointer to the last record is stored in field 2 of first record.

Observe that we use standard C int 0 for the NULL pointer,
this is also what wg_encode_null(db,0) always gives.

Database was created earlier by speed15.
Do not forget to delete database later, a la: wgdb 15 free

real	0m0.153s
user	0m0.110s
sys	0m0.043s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="15";
  int i;  
  wg_int encptr;
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  rec=wg_get_first_record(db);  
  // get a pointer to the last record
  rec=wg_decode_record(db,wg_get_field(db,rec,2)); 
  i=1;
  while(1) {
    encptr=wg_get_field(db,rec,3); // encptr is not yet decoded
    if (encptr==(wg_int)0) break; // encoded null is always standard 0
    rec=wg_decode_record(db,encptr); // get a pointer to the previous record
    i++;
  } 
  printf("i %d\n", i);
  wg_detach_database(db);
  return 0;
}