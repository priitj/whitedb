#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="9";
  int i,j;  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  for(i=0;i<100000;i++) {   
    rec = wg_create_raw_record(db, 100);
    if (!rec) { printf("record creation failed \n"); exit(0); }      
    for (j=0;j<100;j++) {
      wg_set_new_field(db,rec,j,wg_encode_double(db,3.14159265359));      
    }
  }  
  printf("i %d \n", i);
  wg_detach_database(db);
  wg_delete_database(name);
  return 0;
}
/*
creating and immediately filling with double data 
100 000 records of 100 fields in a 1 GIG database

real	0m0.314s
user	0m0.241s
sys	0m0.072s

*/