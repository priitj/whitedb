#include <whitedb/dbapi.h>

int main(int argc, char **argv) {
  void *db, *rec, *rec2;
  wg_int enc, enc2;

  db = wg_attach_database("1000", 2000000);
  rec = wg_create_record(db, 10);
  rec2 = wg_create_record(db, 2);

  enc = wg_encode_int(db, 443);
  enc2 = wg_encode_str(db, "this is my string", NULL);

  wg_set_field(db, rec, 7, enc);
  wg_set_field(db, rec2, 0, enc2);

  return 0;
}

