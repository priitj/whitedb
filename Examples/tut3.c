#include <stdio.h>
#include <whitedb/dbapi.h>

int main(int argc, char **argv) {
  void *db, *rec;
  wg_int enc;

  db = wg_attach_database("1000", 2000000);

  /* create some records for testing */
  rec = wg_create_record(db, 10);
  enc = wg_encode_int(db, 443); /* will match */
  wg_set_field(db, rec, 7, enc);

  rec = wg_create_record(db, 10);
  enc = wg_encode_int(db, 442);
  wg_set_field(db, rec, 7, enc); /* will not match */

  /* now find the records that match our condition
   * "field 7 equals 443"
   */
  rec = wg_find_record_int(db, 7, WG_COND_EQUAL, 443, NULL);
  while(rec) {
    printf("Found a record where field 7 is 443\n");
    rec = wg_find_record_int(db, 7, WG_COND_EQUAL, 443, rec);
  }

  return 0;
}

