#include <stdio.h>
#include <stdlib.h>
#include <whitedb/dbapi.h>

int main(int argc, char **argv) {
  void *db, *rec, *lastrec;
  wg_int enc;
  int i;

  db = wg_attach_database("1000", 1000000); /* 1MB should fill up fast */
  if(!db) {
    printf("ERR: Could not attach to database.\n");
    exit(1);
  }

  lastrec = NULL;
  for(i=0;;i++) {
    char buf[20];
    rec = wg_create_record(db, 1);
    if(!rec) {
      printf("ERR: Failed to create a record (made %d so far)\n", i);
      break;
    }
    lastrec = rec;
    sprintf(buf, "%d", i); /* better to use snprintf() in real applications */
    enc = wg_encode_str(db, buf, NULL);
    if(enc == WG_ILLEGAL) {
      printf("ERR: Failed to encode a string (%d records currently)\n", i+1);
      break;
    }
    if(wg_set_field(db, rec, 0, enc)) {
      printf("ERR: This error is less likely, but wg_set_field() failed.\n");
      break;
    }
  }

  /* For educational purposes, let's pretend we're interested in what's
   * stored in the last record.
   */
  if(lastrec) {
    char *str = wg_decode_str(db, wg_get_field(db, lastrec, 0));
    if(!str) {
      printf("ERR: Decoding the string field failed.\n");
      if(wg_get_field_type(db, lastrec, 0) != WG_STRTYPE) {
        printf("ERR: The field type is not string - "
          "should have checked that first!\n");
      }
    }
  }

  wg_detach_database(db);
  return 0;
}
