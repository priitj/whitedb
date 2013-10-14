/*
creating 10 million raw records of 5 fields in a 1 GB database

real	0m0.349s
user	0m0.217s
sys	0m0.131s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="3";
  int i;
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<10000000;i++) {   
    rec = wg_create_raw_record(db, 5);
    if (!rec) { printf("record creation failed \n"); exit(0); }   
  }  
  printf("i %d\n", i);
  wg_detach_database(db);
  wg_delete_database(name);
  return 0;
}
