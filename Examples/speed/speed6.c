/*
creating and immediately filling with string data 
1 million records of 5 fields in a 1 GIG database:
the string is 30 bytes long and is encoded each time
using up 32 bytes

real	0m0.403s
user	0m0.283s
sys	0m0.119s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="69";
  int i,j;
  char* content="01234567890";
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<1000000;i++) {   
    rec = wg_create_raw_record(db, 5);
    if (!rec) { printf("record creation failed \n"); exit(0); }      
    for (j=0;j<5;j++) {
      wg_set_new_field(db,rec,j,wg_encode_str(db,content,NULL));      
    }
  }  
  printf("i %d \n", i);
  wg_detach_database(db);
  wg_delete_database(name);
  return 0;
}
