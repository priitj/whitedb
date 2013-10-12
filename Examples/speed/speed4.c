/*
creating and immediately deleting 10 million records of 5 fields in a 1 GIG database

real	0m1.160s
user	0m1.149s
sys	0m0.009s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="4";
  int i;
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<10000000;i++) {   
    rec = wg_create_record(db, 5);
    if (!rec) { printf("record creation failed \n"); exit(0); }
    wg_delete_record(db, rec);      
  }  
  printf("i %d\n", i);
  wg_detach_database(db);
  wg_delete_database(name);
  return 0;
}
