/* 
creating and immediately filling with integer data 
10 million records of 5 fields in a 1 GIG database.

Field values are computed as the last five digits
of the record number, thus storing each number
between 0...100000 to 100 different records.

Database created will be later used by speed11 for
scanning.

real	0m0.483s
user	0m0.381s
sys	0m0.101s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="10";
  int i,j;  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<10000000;i++) {   
    rec = wg_create_raw_record(db, 5);    
    if (!rec) { printf("record creation failed \n"); exit(0); }          
    wg_set_new_field(db,rec,3,wg_encode_int(db,i%100000));
  }  
  printf("i %d\n", i);
  return 0;
}
