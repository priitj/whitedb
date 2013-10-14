/*
creating and deleting a 1 GB database 1000 times:

real	0m9.694s
user	0m2.044s
sys	0m7.642s

creating and deleting a 10 MB database 100000 times:

real	0m12.800s
user	0m2.622s
sys	0m10.137s

*/

#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db;
  char *name="1";
  int i;
  for(i=0;i<1000;i++) { // 100000
    db = wg_attach_database(name,1000000000);  // 10000000
    if (!db) { printf("failed at try %d\n", i);  exit(0); }
    wg_detach_database(db);
    wg_delete_database(name);
  }  
  printf("i %d\n", i);
  return 0;
}
