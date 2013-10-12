/* 

Scan through 10 million records in a pre-built database, 
counting all these records which have integer 123 as the value of the third field.

Database was created earlier by speed10. Do not forget to delete database later, 
a la: wgdb 10 free

real	0m0.201s
user	0m0.157s
sys	0m0.044s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="10";
  int i=0;  
  int count=0;
  wg_int encval;
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db attaching failed \n"); exit(0); } 
  encval=wg_encode_int(db,123); // encode for faster comparison in the loop
  rec=wg_get_first_record(db);
  do {
    //if (wg_decode_int(db,wg_get_field(db,rec,3))==123) count++; // a bit slower alternative
    if (wg_get_field(db,rec,3)==encval) count++;
    rec=wg_get_next_record(db,rec);
    i++;
  } while(rec!=NULL);       
  wg_free_encoded(db,encval); // have to free encval since we did not store it to db
  printf("i %d, count %d\n", i,count);
  return 0;
}
