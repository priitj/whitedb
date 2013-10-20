#include <stdlib.h>
#include <whitedb/dbapi.h>

int main(int argc, char **argv) {
  void *db, *rec, *rec2, *rec3;
  wg_int enc;

  if(!(db = wg_attach_database("1000", 2000000)))
    exit(1); /* failed to attach */

  rec = wg_create_record(db, 2); /* this is some record */
  rec2 = wg_create_record(db, 3); /* this is another record */
  rec3 = wg_create_record(db, 4); /* this is a third record */

  if(!rec || !rec2 || !rec3)
    exit(2);

  /* Add some content */
  wg_set_field(db, rec, 1, wg_encode_str(db, "hello", NULL));
  wg_set_field(db, rec2, 0, wg_encode_str(db,
    "I'm pointing to other records", NULL));
  wg_set_field(db, rec3, 0, wg_encode_str(db,
    "I'm linked from two records", NULL));

  /* link the records to each other */
  enc = wg_encode_record(db, rec);
  wg_set_field(db, rec2, 2, enc); /* rec2[2] points to rec */
  enc = wg_encode_record(db, rec3);
  wg_set_field(db, rec2, 1, enc); /* rec2[1] points to rec3 */
  wg_set_field(db, rec, 0, enc); /* rec[0] points to rec3 */

  wg_detach_database(db);
  return 0;
}

