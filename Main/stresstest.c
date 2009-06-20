/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009
*
* This file is part of wgandalf
*
* Wgandalf is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
* 
* Wgandalf is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with Wgandalf.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file stresstest.c
 *  generate load with writer and reader threads
 */

/* ====== Includes =============== */

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#ifdef _WIN32
#include <conio.h> // for _getch
#endif
#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif

#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
#include "../Db/dbtest.h"
#include "../Db/dblock.h"


/* ====== Private defs =========== */

#define DBSIZE 4000000
#define WORKLOAD 100000
#define REC_SIZE 5
#define CHATTY_THREADS 1

typedef struct {
  int threadid;
  void *db;
#ifdef HAVE_PTHREAD
  pthread_t pth;
#endif
} pt_data;


/* ======= Private protos ================ */

void run_workers(void *db, int rcnt, int wcnt);
void * writer_thread(void * threadarg);
void * reader_thread(void * threadarg);


/* ====== Functions ============== */



int main(int argc, char **argv) {
 
  char* shmname = NULL;
  char* shmptr;
  int rcnt = -1, wcnt = -1;
  
  if(argc==4) {
    shmname = argv[1];
    rcnt = atol(argv[2]);
    wcnt = atol(argv[3]);
  }

  if(rcnt<0 || wcnt<0) {
    fprintf(stderr, "usage: %s <shmname> <readers> <writers>\n", argv[0]);
    exit(1);
  }

  shmptr=wg_attach_database(shmname,DBSIZE);
  if (shmptr==NULL)
    exit(2);
  
  /* XXX: add timing here */
  run_workers(shmptr, rcnt, wcnt);
  /* XXX: add timing here */

  wg_delete_database(shmname);
  
#ifdef _WIN32  
  _getch();  
#endif  
  exit(0);
}


/** Run requested number of worker threads
 *  Waits until all workers complete.
 */

void run_workers(void *db, int rcnt, int wcnt) {
  pt_data *pt_table;
  int i, tcnt;

#if !defined(HAVE_PTHREAD)
  fprintf(stderr, "No thread support: skipping tests.\n");
  return;
#else
  pthread_attr_t attr;

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  tcnt = rcnt + wcnt;
  pt_table = (pt_data *) malloc(tcnt * sizeof(pt_data));
  if(!pt_table) {
    fprintf(stderr, "Failed to allocate thread table: skipping tests.\n");
    return;
  }

  /* Precreate data for workers */
  for (i=0; i<WORKLOAD; i++) {
    if(wg_create_record(db, REC_SIZE) == NULL) { 
      fprintf(stderr, "Failed to create data record #%d: skipping tests.\n", i);
      goto workers_done;
    }
  } 

  /* Spawn the threads */
  for(i=0; i<tcnt; i++) {
    int err;

    pt_table[i].db = db;
    pt_table[i].threadid = i;

    /* First wcnt threads are writers, the remainder readers */
    if(i<wcnt) {
      err = pthread_create(&pt_table[i].pth, &attr, writer_thread, \
        (void *) &pt_table[i]);
    } else {
      err = pthread_create(&pt_table[i].pth, &attr, reader_thread, \
        (void *) &pt_table[i]);
    }

    if(err) {
      fprintf(stderr, "Error code from pthread_create: %d.\n", err);
      goto workers_done;
    }
  }

  /* Join the workers (wait for them to complete) */
  for(i=0; i<tcnt; i++) {
    int err;
    void *status;

    err = pthread_join(pt_table[i].pth, &status);
    if(err) {
      fprintf(stderr, "Error code from pthread_join: %d.\n", err);
      break;
    }
  }

workers_done:
  pthread_attr_destroy(&attr);
  free(pt_table);
#endif
}

/** Writer thread
 *  Runs preconfigured number of basic write transactions
 */

void * writer_thread(void * threadarg) {
  void * db;
  int threadid, i, j;
  void *rec = NULL;

  db = ((pt_data *) threadarg)->db;
  threadid = ((pt_data *) threadarg)->threadid;

#ifdef CHATTY_THREADS
  fprintf(stdout, "Writer thread %d started.\n", threadid);
#endif

  for(i=0; i<WORKLOAD; i++) {
    wg_int c=-1;

    /* Start transaction */
    if(!wg_start_write(db)) {
      fprintf(stderr, "Writer thread %d: wg_start_write failed.\n", threadid);
      goto writer_done;
    }
    
    /* Fetch record from database */
    if(i) rec = wg_get_next_record(db, rec);
    else rec = wg_get_first_record(db);
    if(!rec) {
      fprintf(stderr, "Writer thread %d: wg_get_next_record failed.\n", threadid);
      wg_end_write(db);
      goto writer_done;
    }

    /* Modify record */
    for(j=0; j<REC_SIZE; j++) {
      if (wg_set_int_field(db, rec, j, c--) != 0) { 
        fprintf(stderr, "Writer thread %d: int storage error.\n", threadid);
        wg_end_write(db);
        goto writer_done;
      }
    } 

    /* End transaction */
    if(!wg_end_write(db)) {
      fprintf(stderr, "Writer thread %d: wg_end_write failed.\n", threadid);
      goto writer_done;
    }
  }

#ifdef CHATTY_THREADS
  fprintf(stdout, "Writer thread %d ended.\n", threadid);
#endif

writer_done:
#ifdef HAVE_PTHREAD
  pthread_exit(NULL);
#endif
}

/** Reader thread
 *  Runs preconfigured number of read transactions
 */

void * reader_thread(void * threadarg) {
  void * db;
  int threadid, i, j;
  void *rec = NULL;

  db = ((pt_data *) threadarg)->db;
  threadid = ((pt_data *) threadarg)->threadid;

#ifdef CHATTY_THREADS
  fprintf(stdout, "Reader thread %d started.\n", threadid);
#endif

  for(i=0; i<WORKLOAD; i++) {
    wg_int reclen;

    /* Start transaction */
    if(!wg_start_read(db)) {
      fprintf(stderr, "Reader thread %d: wg_start_read failed.\n", threadid);
      goto reader_done;
    }
    
    /* Fetch record from database */
    if(i) rec = wg_get_next_record(db, rec);
    else rec = wg_get_first_record(db);
    if(!rec) {
      fprintf(stderr, "Reader thread %d: wg_get_next_record failed.\n", threadid);
      wg_end_read(db);
      goto reader_done;
    }

    /* Parse the record. We can safely ignore errors here,
     * except for the record length, which is supposed to be REC_SIZE
     */
    reclen = wg_get_record_len(db, rec);
    if (reclen < 0) { 
      fprintf(stderr, "Reader thread %d: invalid record length.\n", threadid);
      wg_end_read(db);
      goto reader_done;
    }

    for(j=0; j<reclen; j++) {
      wg_get_field(db, rec, j);
      wg_get_field_type(db, rec, j);
    }

    /* End transaction */
    if(!wg_end_read(db)) {
      fprintf(stderr, "Reader thread %d: wg_end_read failed.\n", threadid);
      goto reader_done;
    }
  }

#ifdef CHATTY_THREADS
  fprintf(stdout, "Reader thread %d ended.\n", threadid);
#endif

reader_done:
#ifdef HAVE_PTHREAD
  pthread_exit(NULL);
#endif
}
