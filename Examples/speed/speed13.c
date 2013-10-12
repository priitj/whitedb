/* 

Outer loop: run the inner loop million times to obtain sensible timings.

Inner loop: prepare query and search from the index on 10 million records, counting all these which have 
integer 123 as the value of the third field, using query on the indexed field. There are 100 of 
such values.

Database was created earlier by speed11 and indexed by speed 12. 
Do not forget to delete database later, a la: wgdb 10 free

Outer loop time (i.e. 1 million identical query building / performing / deallocating operations) altogether:

real	0m3.256s
user	0m3.252s
sys	0m0.001s

*/


#include <whitedb/dbapi.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char **argv) {
  void *db, *rec;
  char *name="10";
  int i;  
  int count=0;
  wg_query *query;  
  wg_query_arg arglist[5];
  
  db = wg_attach_database(name, 1000000000);
  if (!db) { printf("db attaching failed \n"); exit(0); } 
  // outer loop is just for sensible timing: do the same thing 1000 times
  for(i=0;i<1000000;i++) { 
    arglist[0].column = 3;
    arglist[0].cond = WG_COND_EQUAL;
    arglist[0].value = wg_encode_query_param_int(db,123);
    query = wg_make_query(db, NULL, 0, arglist, 1);
    if(!query) { printf("query creation failed \n"); exit(0); } 
    while((rec = wg_fetch(db, query))) {
      count++;
      //wg_print_record(db, rec); printf("\n");
    }  
    wg_free_query(db,query);
  }  
  printf("count altogether for i %d runs: %d\n", i, count);
  return 0;
}
