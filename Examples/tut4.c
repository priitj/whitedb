#include <stdio.h>
#include <whitedb/dbapi.h>

int main(int argc, char **argv) {
  void *db, *rec;
  wg_int enc;
  wg_query_arg arglist[2]; /* holds the arguments to the query */
  wg_query *query;         /* used to fetch the query results */

  db = wg_attach_database("1000", 2000000);

  /* just in case, create some records for testing */
  rec = wg_create_record(db, 10);
  enc = wg_encode_int(db, 443); /* will match */
  wg_set_field(db, rec, 7, enc);

  rec = wg_create_record(db, 10);
  enc = wg_encode_int(db, 442);
  wg_set_field(db, rec, 7, enc); /* will not match */

  /* now find the records that match the condition
   * "field 7 equals 443 and field 6 equals NULL". The
   * second part is a bit redundant but we're adding it
   * to show the use of the argument list.
   */
  arglist[0].column = 7;
  arglist[0].cond = WG_COND_EQUAL;
  arglist[0].value = wg_encode_query_param_int(db, 443);

  arglist[1].column = 6;
  arglist[1].cond = WG_COND_EQUAL;
  arglist[1].value = wg_encode_query_param_null(db, NULL);

  query = wg_make_query(db, NULL, 0, arglist, 2);

  while((rec = wg_fetch(db, query))) {
    printf("Found a record where field 7 is 443 and field 6 is NULL\n");
  }

  /* Free the memory allocated for the query */
  wg_free_query(db, query);
  wg_free_query_param(db, arglist[0].value);
  wg_free_query_param(db, arglist[1].value);
  return 0;
}

