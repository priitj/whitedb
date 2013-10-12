/* 
create an index for the field 3 in the previously built
database of 10 million records.

real	0m6.540s
user	0m6.436s
sys	0m0.098s

*/

#include <whitedb/dbapi.h>
#include <whitedb/indexapi.h> // must additionally include indexapi.h
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="10";
  int tmp;  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db creation failed \n"); exit(0); }
  tmp=wg_create_index(db,3,WG_INDEX_TYPE_TTREE,NULL,0);
  if (tmp) printf("Index creation failed\n");
  else printf("Index creation succeeded\n");  
  return 0;
}
