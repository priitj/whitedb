/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
*
* Minor mods by Tanel Tammet
*
* This file is part of WhiteDB
*
* WhiteDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* WhiteDB is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with WhiteDB.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file demo.c
 *  Demonstration of WhiteDB low-level API usage
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/* Include dbapi.h for WhiteDB API functions */
#include "../Db/dbapi.h"


#ifdef __cplusplus
extern "C" {
#endif

/* ====== Private defs =========== */


/* ======= Private protos ================ */

void run_demo(void *db);


/* ====== Functions ============== */

/** Init database, run demo, drop database
 * Command line arguments are ignored.
 */

int main(int argc, char **argv) {

  void* shmptr;

  /* Create a database with custom key and 2M size */
  shmptr=wg_attach_database("9273", 2000000);

  /* Using default key and size:
  shmptr=wg_attach_database(NULL, 0);
  */

  if(!shmptr) {
    fprintf(stderr, "Failed to attach to database.\n");
    exit(1);
  }

  /* We have successfully attached, run the demo code */
  run_demo(shmptr);

  /* Clean up. The shared memory area is released. This is
   * useful for the purposes of this demo, but might best be
   * avoided for more persistent databases. */
  wg_delete_database("9273");
  /* Database with default key:
  wg_delete_database(NULL);
  */
  exit(0);
}


/** Run demo code.
 *  Uses various database API functions.
 */

void run_demo(void* db) {
  void *rec = NULL, *firstrec = NULL, *nextrec = NULL;
                                /* Pointers to a database record */
  wg_int enc; /* Encoded data */
  wg_int lock_id; /* Id of an acquired lock (for releasing it later) */
  wg_int len;
  int i;
  int intdata, datedata, timedata;
  char strbuf[80];

  printf("********* Starting demo ************\n");

  /* Begin by creating a simple record of 3 fields and fill it
   * with integer data.
   */

  printf("Creating first record.\n");

  rec=wg_create_record(db, 3);
  if (rec==NULL) {
    printf("rec creation error.\n");
    return;
  }

  /* Encode a field, checking for errors */
  enc = wg_encode_int(db, 44);
  if(enc==WG_ILLEGAL) {
    printf("failed to encode an integer.\n");
    return;
  }

  /* Negative return value shows that an error occurred */
  if(wg_set_field(db, rec, 0, enc) < 0) {
    printf("failed to store a field.\n");
    return;
  }

  /* Skip error checking for the sake of brevity for the rest of fields */
  enc = wg_encode_int(db, -199999);
  wg_set_field(db, rec, 1, enc);
  wg_set_field(db, rec, 2, wg_encode_int(db, 0));

  /* Now examine the record we have created. Get record length,
   * encoded value of each field, data type and decoded value.
   */

  /* Negative return value shows an error. */
  len = wg_get_record_len(db, rec);
  if(len < 0) {
    printf("failed to get record length.\n");
    return;
  }
  printf("Size of created record at %p was: %d\n", rec, (int) len);

  for(i=0; i<len; i++) {
    printf("Reading field %d:", i);
    enc = wg_get_field(db, rec, i);
    if(wg_get_encoded_type(db, enc) != WG_INTTYPE) {
      printf("data was of unexpected type.\n");
      return;
    }
    intdata = wg_decode_int(db, enc);
    /* No error checking here. All integers are valid. */
    printf(" %d\n", intdata);
  }

  /* Fields can be erased by setting their value to 0 which always stands for NULL value. */
  printf("Clearing field 1.\n");

  wg_set_field(db, rec, 1, 0);

  if(wg_get_field(db, rec, 1)==0) {
    printf("Re-reading field 1 returned a 0 (NULL) field.\n");
  } else {
    printf("unexpected value \n");
    return;
  }

  /* Fields can be updated with data of any type (the type is not fixed). */
  printf("Updating field 0 to a floating-point number.\n");

  enc = wg_encode_double(db, 56.9988);
  wg_set_field(db, rec, 0, enc);

  enc = wg_get_field(db, rec, 0);
  if(wg_get_encoded_type(db, enc) == WG_DOUBLETYPE) {
    printf("Re-reading field 0 returned %f.\n", wg_decode_double(db, enc));
  } else {
    printf("data was of unexpected type.\n");
    return;
  }

  /* Create a next record. Let's assume we're in an environment where
   * the database is used concurrently, so there's a need to use locking.
   */

  printf("Creating second record.\n");

  /* Lock id of 0 means that the operation failed */
  lock_id = wg_start_write(db);
  if(!lock_id) {
    printf("failed to acquire lock.\n");
    return;
  }

  /* Do the write operation we acquired the lock for. */
  rec=wg_create_record(db, 6);

  /* Failing to release the lock would be fatal to database operation. */
  if(!wg_end_write(db, lock_id)) {
    printf("failed to release lock.\n");
    return;
  }

  if (!rec) {
    printf("rec creation error.\n");
    return;
  }

  /* Reading also requires locking./ */
  lock_id = wg_start_read(db);
  if(!lock_id) {
    printf("failed to acquire lock.\n");
    return;
  }

  /* Do our read operation... */
  len = wg_get_record_len(db, rec);

  /* ... and unlock immediately */
  if(!wg_end_read(db, lock_id)) {
    printf("failed to release lock.\n");
    return;
  }

  if(len < 0) {
    printf("failed to get record length.\n");
    return;
  }
  printf("Size of created record at %p was: %d\n", rec, (int) len);

  /* Let's find the first record in the database */
  lock_id = wg_start_read(db);
  firstrec = wg_get_first_record(db);
  wg_end_read(db, lock_id);
  if(!firstrec) {
    printf("Failed to find first record.\n");
    return;
  }

  printf("First record of database had address %p.\n", firstrec);

  /* Let's check what the next record is to demonstrate scanning records. */
  nextrec = firstrec;
  lock_id = wg_start_read(db);
  do {

    nextrec = wg_get_next_record(db, nextrec);
    if(nextrec)
      printf("Next record had address %p.\n", nextrec);
  } while(nextrec);
  printf("Finished scanning database records.\n");
  wg_end_read(db, lock_id);

  /* Set fields to various values. Field 0 is not touched at all (un-
   * initialized). Field 1 is set to point to another record.
   */

  printf("Populating second record with data.\n");

  /* Let's use the first record we found to demonstrate storing
   * a link to a record in a field inside another record. */
  lock_id = wg_start_write(db);
  enc = wg_encode_record(db, firstrec);
  wg_set_field(db, rec, 1, enc);
  wg_end_write(db, lock_id);

  /* Now set other fields to various data types. To keep the example shorter,
   * the locking and unlocking operations are omitted (in real applications,
   * this would be incorrect usage if concurrent access is expected).
   */

  wg_set_field(db, rec, 2, wg_encode_str(db, "This is a char array", NULL));
  wg_set_field(db, rec, 3, wg_encode_char(db, 'a'));

  /* For time and date, we use current time in local timezone */
  enc = wg_encode_date(db, wg_current_localdate(db));
  if(enc==WG_ILLEGAL) {
    printf("failed to encode date.\n");
    return;
  }
  wg_set_field(db, rec, 4, enc);

  enc = wg_encode_time(db, wg_current_localtime(db));
  if(enc==WG_ILLEGAL) {
    printf("failed to encode time.\n");
    return;
  }
  wg_set_field(db, rec, 5, enc);

  /* Now read and print all the fields. */

  wg_print_record(db, (wg_int *) rec);
  printf("\n");

  /* Date and time can be handled together as a datetime object. */
  datedata = wg_decode_date(db, wg_get_field(db, rec, 4));
  timedata = wg_decode_time(db, wg_get_field(db, rec, 5));
  wg_strf_iso_datetime(db, datedata, timedata, strbuf);
  printf("Reading datetime: %s.\n", strbuf);

  printf("Setting date and time to 2010-03-31, 12:59\n");

  /* Update date and time to arbitrary values using wg_strp_iso_date/time */
  wg_set_field(db, rec, 4,
    wg_encode_date(db, wg_strp_iso_date(db, "2010-03-31")));
  wg_set_field(db, rec, 5,
    wg_encode_time(db, wg_strp_iso_time(db, "12:59:00.33")));

  printf("Dumping the contents of the database:\n");
  wg_print_db(db);

  printf("********* Demo ended ************\n");
}

#ifdef __cplusplus
}
#endif
