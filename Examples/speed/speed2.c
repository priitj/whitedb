/*
creating 10 million records of 5 fields in a 1 GB database

real	0m0.586s
user	0m0.473s
sys	0m0.113s

creating 10 million records of 9 fields in a 1 GB database

real	0m0.812s
user	0m0.645s
sys	0m0.166s


*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="2";
  int i;
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<10000000;i++) {   
    rec = wg_create_record(db, 9);
    if (!rec) { printf("record creation failed \n"); exit(0); }   
  }  
  printf("i %d\n", i);
  wg_detach_database(db);
  wg_delete_database(name);
  return 0;
}
