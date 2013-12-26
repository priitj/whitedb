/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010,2011,2013
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

 /** @file query.c
 *  Demonstration of various queries to WhiteDB database.
 *  This program also uses locking to show how to handle queries
 *  in a parallel environment.
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../Db/dbapi.h"
#include "../Db/indexapi.h"

/* Extra protos for demo data (not in dbapi.h) */
int wg_genintdata_mix(void *db, int databasesize, int recordsize);

/* ====== Private defs =========== */

/* ======= Private protos ================ */

void run_querydemo(void *db);
void fetchall(void *db, wg_query *q);


/* ====== Functions ============== */

/** Init database, run demo, drop database
 * Command line arguments are ignored.
 */

int main(int argc, char **argv) {

  char* shmptr;

  /* Create a database with custom key and size */
  shmptr=wg_attach_database("9722", 2000000);

  if(!shmptr) {
    fprintf(stderr, "Failed to attach to database.\n");
    exit(1);
  }

  /* Database was created, run demo and clean up. */
  run_querydemo(shmptr);
  wg_delete_database("9722");
  exit(0);
}


/** Run demo queries.
 *
 */

void run_querydemo(void* db) {
  void *rec = NULL, *firstrec = NULL;
  wg_int lock_id;
  wg_query_arg arglist[5]; /* holds query parameters */
  wg_query *query;         /* query object */
  wg_int matchrec[10];     /* match record query parameter */
  int i;

  printf("********* Starting query demo ************\n");

  /* Create indexes on the database */
  if(wg_create_index(db, 0, WG_INDEX_TYPE_TTREE, NULL, 0)) {
    fprintf(stderr, "index creation failed, aborting.\n");
    return;
  }
  if(wg_create_index(db, 2, WG_INDEX_TYPE_TTREE, NULL, 0)) {
    fprintf(stderr, "index creation failed, aborting.\n");
    return;
  }
  if(wg_create_index(db, 3, WG_INDEX_TYPE_TTREE, NULL, 0)) {
    fprintf(stderr, "index creation failed, aborting.\n");
    return;
  }

  /* Take a write lock until we're done writing to the database.
   */
  lock_id = wg_start_write(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get write lock, aborting.\n");
    return; /* lock timed out */
  }

  /* Generate test data */
  wg_genintdata_mix(db, 20, 4);

  /* Add some non-unique values */
  firstrec = rec = wg_get_first_record(db);
  i = 0;
  while(rec) {
    wg_set_field(db, rec, 0, wg_encode_int(db, i++ % 3));
    if(i<=6)
      wg_set_field(db, rec, 3, wg_encode_int(db, 6));
    rec = wg_get_next_record(db, rec);
  }

  printf("Database test data contents\n");
  wg_print_db(db);

  /* Release the write lock. We could have released it before wg_print_db()
   * and acquired a separate read lock instead. That would have been correct,
   * but unnecessary for this demo.
   */
  wg_end_write(db, lock_id);


  /* Encode query arguments. We will use the wg_encode_query_param*()
   * family of functions which do not write to the shared memory
   * area, therefore locking is not required at this point.
   *
   * Basic query 1: column 2 less than 30
   */
  arglist[0].column = 2;
  arglist[0].cond = WG_COND_LESSTHAN;
  arglist[0].value = wg_encode_query_param_int(db, 30);

  /* Take read lock. No alterations should be allowed
   * during the building of the query.
   */
  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, NULL, 0, arglist, 1);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  /* We keep the lock before using the results for best possible
   * isolation. In some cases this is not necessary.
   */
  printf("Printing results for query 1: column 2 less than 30\n");
  fetchall(db, query);

  /* Release the read lock */
  wg_end_read(db, lock_id);

  wg_free_query(db, query); /* free the memory */

  /* Basic query 2: col 2 > 21 and col 2 <= 111 */
  arglist[0].column = 2;
  arglist[0].cond = WG_COND_GREATER;
  arglist[0].value = wg_encode_query_param_int(db, 21);
  arglist[1].column = 2;
  arglist[1].cond = WG_COND_LTEQUAL;
  arglist[1].value = wg_encode_query_param_int(db, 111);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, NULL, 0, arglist, 2);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing results for query 2: col 2 > 21 and col 2 <= 111\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  /* Basic query 3: match all records [ 0, ...]. Fields that
   * are beyond the size of matchrec implicitly become wildcards.
   */
  matchrec[0] = wg_encode_query_param_int(db, 0);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, matchrec, 1, NULL, 0);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing results for query 3: all records that match [ 0, ... ]\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  /* Combine the parameters of queries 2 and 3 (it is allowed to
   * mix both types of arguments).
   */

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, matchrec, 1, arglist, 2);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing results for combined queries 2 and 3\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  /* Add an extra condition */
  arglist[2].column = 3;
  arglist[2].cond = WG_COND_EQUAL;
  arglist[2].value = wg_encode_query_param_int(db, 112);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, matchrec, 1, arglist, 3);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Adding extra condtion to previous queries: col 3 = 112\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  /* Non-indexed columns may be used too. This will produce
   * a "full scan" query with non-ordered results.
   */
  arglist[0].column = 1;
  arglist[0].cond = WG_COND_GREATER;
  arglist[0].value = wg_encode_query_param_int(db, 20);
  arglist[1].column = 1;
  arglist[1].cond = WG_COND_LTEQUAL;
  arglist[1].value = wg_encode_query_param_int(db, 110);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, NULL, 0, arglist, 2);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing results for non-indexed column: col 1 > 20 and col 1 <= 110\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  /* More complete match record. Use variable field type
   * for wildcards. The identifier used for the variable is not
   * important currently.
   */
  matchrec[0] = wg_encode_query_param_int(db, 1);
  matchrec[1] = wg_encode_query_param_var(db, 0);
  matchrec[2] = wg_encode_query_param_var(db, 0);
  matchrec[3] = wg_encode_query_param_int(db, 6);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  query = wg_make_query(db, matchrec, 4, NULL, 0);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing results for match query: records like [ 1, *, *, 6 ]\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  /* Arguments may be omitted. This causes the query to return
   * all the rows in the database.
   */
  query = wg_make_query(db, NULL, 0, NULL, 0);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing results for a query with no arguments\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "failed to get read lock, aborting.\n");
    return; /* lock timed out */
  }

  /* Finally, try matching to a database record. Depending on the
   * test data, this sould return at least one record. Note that
   * reclen 0 indicates that the record should be taken from database.
   */
  query = wg_make_query(db, firstrec, 0, NULL, 0);
  if(!query) {
    fprintf(stderr, "failed to build query, aborting.\n");
    return;
  }

  printf("Printing records matching the first record in database\n");
  fetchall(db, query);
  wg_end_read(db, lock_id);
  wg_free_query(db, query);

  printf("********* Demo ended ************\n");
}

/** Fetch the results of a single query
 *
 */

void fetchall(void *db, wg_query *q) {
  void *rec = wg_fetch(db, q);
  while(rec) {
    wg_print_record(db, rec);
    printf("\n");
    rec = wg_fetch(db, q);
  }
  printf("---- end of data ----\n");
}
