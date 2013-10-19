#include <stdio.h>
#include <stdlib.h>
#include <whitedb/dbapi.h>

#define NUM_INCREMENTS 100000

void die(void *db, int err) {
  wg_detach_database(db);
  exit(err);
}

int main(int argc, char **argv) {
  void *db, *rec;
  wg_int lock_id;
  int i, val;

  if(!(db = wg_attach_database("1000", 1000000))) {
    exit(1); /* failed to attach */
  }

  /* First we need to make sure both counting programs start at the
   * same time (otherwise the example would be boring).
   */
  lock_id = wg_start_read(db);
  rec = wg_get_first_record(db); /* our database only contains one record,
                                  * so we don't need to make a query.
                                  */
  wg_end_read(db, lock_id);

  if(!rec) {
    /* There is no record yet, we're the first to run and have
     * to set up the counter.
     */
    lock_id = wg_start_write(db);
    if(!lock_id) die(db, 2);
    rec = wg_create_record(db, 1);
    wg_end_write(db, lock_id);

    if(!rec) die(db, 3);
    printf("Press a key when all the counter programs have been started.");
    fgetc(stdin);

    /* Setting the counter to 0 lets each counting program know it can
     * start counting now.
     */
    lock_id = wg_start_write(db);
    if(!lock_id) die(db, 2);
    wg_set_field(db, rec, 0, wg_encode_int(db, 0));
    wg_end_write(db, lock_id);
  } else {
    /* Some other program has started first, we wait until the counter
     * is ready.
     */
    int ready = 0;

    while(!ready) {
      lock_id = wg_start_read(db);
      if(!lock_id) die(db, 2);
      if(wg_get_field_type(db, rec, 0) == WG_INTTYPE)
        ready = 1;
      wg_end_read(db, lock_id);
    }
  }

  /* Now start the actual counting. */
  for(i=0; i<NUM_INCREMENTS; i++) {
    lock_id = wg_start_write(db);
    if(!lock_id) die(db, 2);

    /* This is the "critical section" for the counter. we read the value,
     * increment it and write it back.
     */
    val = wg_decode_int(db, wg_get_field(db, rec, 0));
    wg_set_field(db, rec, 0, wg_encode_int(db, ++val));

    wg_end_write(db, lock_id);
  }

  printf("\nCounting done. My last value was %d\n", val);
  return 0;
}

