/*

creating and immediately filling with data 10 million
records of 5 fields in a 1 GIG database

real	0m1.163s
user	0m1.042s
sys	0m0.120s

adding code to read back the value and add it to a counter:

real	0m1.768s
user	0m1.643s
sys	0m0.124s

doing record filling with 1000 records of length 50 thousand:

real	0m0.941s
user	0m0.863s
sys	0m0.077s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="51";
  int i,j,count=0;
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<10000000;i++) {
    rec = wg_create_raw_record(db, 5);
    if (!rec) { printf("record creation failed \n"); exit(0); }      
    for (j=0;j<5;j++) {
      wg_set_new_field(db,rec,j,wg_encode_int(db,i+j));
      //count+=wg_decode_int(db,wg_get_field(db, rec, j));
    }
  }  
  printf("i %d count %d\n", i,count);
  wg_detach_database(db);
  wg_delete_database(name);
  return 0;
}
