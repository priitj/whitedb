#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <conio.h> // for _getch
#endif

#include <raptor.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
//#include "../Db/dbapi.h"
#include "../Db/dbtest.h"
#include "../Db/dbdump.h"
#include "../Db/dblog.h"


/* rdfprint.c: print triples from parsing RDF/XML */

/* 

gcc -o rdfprint rdfprint.c `raptor-config --cflags` `raptor-config --libs`

gcc -o raptortry raptortry.c `raptor-config --cflags` `raptor-config --libs`

$ ./rdfprint raptor.rdf
_:genid1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://usefulinc.com/ns/doap#Project> .
_:genid1 <http://usefulinc.com/ns/doap#name> "Raptor" .
_:genid1 <http://usefulinc.com/ns/doap#homepage> <http://librdf.org/raptor/> .
...


*/

int tcount=0;

void handle_triple(void* user_data, const raptor_statement* triple) {
  int print=1;
  void* rec;
  void* db=user_data;
  gint enc;
  gint tmp;
  
  rec=wg_create_record(db,3);
  if (!rec) { 
    printf("\n cannot create a new record, tcount %d\n",tcount);
    exit(0);    
  }  
  
  if (print) {
    raptor_print_statement_as_ntriples(triple, stdout);
    fputc('\n', stdout);
  
    printf("s: %s\n",(char*)(triple->subject));
    printf("p: %s\n",(char*)(triple->predicate));
    printf("o: %s\n",(char*)(triple->object)); 
  }
  
  enc=wg_encode_str(db,(char*)(triple->subject),NULL);
  tmp=wg_set_field(db,rec,0,enc);
  enc=wg_encode_str(db,(char*)(triple->predicate),NULL);
  tmp=wg_set_field(db,rec,1,enc);
  
  
  if ((triple->object_type)==RAPTOR_IDENTIFIER_TYPE_RESOURCE) {
    if (print) printf("t: resource\n");   
    enc=wg_encode_str(db,(char*)(triple->subject),NULL);
    tmp=wg_set_field(db,rec,2,enc);    
  } else if ((triple->object_type)==RAPTOR_IDENTIFIER_TYPE_ANONYMOUS)  {
    if (print) printf("t: anonymous\n");
    enc=wg_encode_str(db,(char*)(triple->subject),NULL);
    tmp=wg_set_field(db,rec,2,enc); 
  } else if ((triple->object_type)==RAPTOR_IDENTIFIER_TYPE_LITERAL)  {  
    if (print) printf("t: literal\n");
    if (print) printf("d: %s\n",(char*)(triple->object_literal_datatype));
    if (print) printf("l: %s\n",(char*)(triple->object_literal_language));
    if ((triple->object_literal_datatype)==NULL) {
      enc=wg_encode_str(db,(char*)(triple->subject),(char*)(triple->object_literal_language));
      tmp=wg_set_field(db,rec,2,enc); 
    } else {
      enc=wg_encode_str(db,(char*)(triple->subject),(char*)(triple->object_literal_datatype));
      tmp=wg_set_field(db,rec,2,enc);
    }          
  } else {
    printf("ERROR! Unknown triple object type.\n"); 
    exit(0);
  }         
  tcount++;
}

int
main(int argc, char *argv[])
{
  raptor_parser* rdf_parser=NULL;
  unsigned char *uri_string;
  raptor_uri *uri, *base_uri;

  char* shmname=NULL;
  char* shmptr; 
  
  if (!strcmp(argv[1],"load")) {
    shmptr=wg_attach_database(shmname,0); 
    
    raptor_init();

    //rdf_parser=raptor_new_parser("rdfxml");
    rdf_parser=raptor_new_parser("turtle");

    raptor_set_statement_handler(rdf_parser, shmptr, handle_triple);

    uri_string=raptor_uri_filename_to_uri_string(argv[2]);
    uri=raptor_new_uri(uri_string);
    base_uri=raptor_uri_copy(uri);

    raptor_parse_file(rdf_parser, uri, base_uri);

    raptor_free_parser(rdf_parser);

    raptor_free_uri(base_uri);
    raptor_free_uri(uri);
    raptor_free_memory(uri_string);

    raptor_finish();
    printf("tcount %d \n",tcount);
  } else if (!strcmp(argv[1],"run1")) {
    
  } 
  return 0;  
}

