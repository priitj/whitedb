/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009
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

 /** @file stresstest.c
 *  generate load with writer and reader threads
 *  Currently supports two thread API-s: libpthread and Win32
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <conio.h>
#include <windows.h>
#else
#include <sys/time.h>
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include "../Db/dballoc.h"
#include "../Db/dbmem.h"
#include "../Db/dbdata.h"
#include "../Db/dblock.h"


/* ====== Private defs =========== */

#define DBSIZE 10000000
#define WORKLOAD 100000
#define REC_SIZE 5
#define CHATTY_THREADS 1
#define SYNC_THREADS 1

/* Use libpthread rwlock to create a reference
 * benchmark for measuring the performance of
 * dblock.c spinlocks against.
 */
/* #define BENCHMARK 1 */

typedef struct {
  int threadid;
  void *db;
#ifdef HAVE_PTHREAD
  pthread_t pth;
#elif defined(_WIN32)
  HANDLE hThread;
#endif
} pt_data;

#if defined(_WIN32)
typedef DWORD worker_t;
#else /* compatible with libpthread */
typedef void * worker_t;
#endif

/* ======= Private protos ================ */

int prepare_data(void *db);
void check_data(void *db, int wcnt);
void run_workers(void *db, int rcnt, int wcnt);
worker_t writer_thread(void * threadarg);
worker_t reader_thread(void * threadarg);


/* ====== Global vars ======== */

#ifdef SYNC_THREADS
#if defined(HAVE_PTHREAD)
pthread_mutex_t twait_mutex;
pthread_cond_t twait_cv;
#elif defined(_WIN32)
HANDLE twait_ev;
#endif
volatile int twait_cnt; /* count of workers in wait state */
#endif

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
pthread_rwlock_t rwlock;
#endif

/* ====== Functions ============== */



int main(int argc, char **argv) {

  char* shmname = NULL;
  void* shmptr;
  int rcnt = -1, wcnt = -1;
#ifndef _WIN32
  struct timeval tv;
#endif
  unsigned long long start_ms, end_ms;

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

  if(prepare_data(shmptr)) {
    wg_delete_database(shmname);
    exit(3);
  }

#ifdef _WIN32
  start_ms = (unsigned long long) GetTickCount();
#else
  gettimeofday(&tv, NULL);
  start_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif

  run_workers(shmptr, rcnt, wcnt);

#ifdef _WIN32
  end_ms = (unsigned long long) GetTickCount();
#else
  gettimeofday(&tv, NULL);
  end_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif

  check_data(shmptr, wcnt);

  fprintf(stdout, "elapsed: %d ms\n", (int) (end_ms - start_ms));

  wg_delete_database(shmname);

  exit(0);
}

/**
 * Precreate data for workers
 */
int prepare_data(void *db) {
  int i;
  for (i=0; i<WORKLOAD; i++) {
    int j;
    void *rec = wg_create_record(db, REC_SIZE);
    if(rec == NULL) {
      fprintf(stderr, "Failed to create data record #%d: skipping tests.\n", i);
      return -1;
    }
    for(j=0; j<REC_SIZE; j++) {
      if(wg_set_int_field(db, rec, j, 0) != 0) {
        fprintf(stderr,
          "Failed to create data record #%d: skipping tests.\n", i);
        return -1;
      }
    }
  }
  return 0;
}

/**
 * Check the database state
 */
void check_data(void *db, int wcnt) {
  void *rec = wg_get_first_record(db);
  int cksum;
  if(rec == NULL) {
    fprintf(stderr, "Database check failed: first record not found.\n");
    return;
  }
  cksum = wg_decode_int(db, wg_get_field(db, rec, 0));
  if(cksum != wcnt * WORKLOAD) {
    fprintf(stderr, "Database check failed: bad checksum (%d != %d).\n",
      cksum, wcnt * WORKLOAD);
    return;
  }
}

/** Run requested number of worker threads. If SYNC_THREADS is
 *  defined, the created threads sleep until signaled by the
 *  initial thread to start work simultaneously.
 *  Waits until all workers complete.
 */

void run_workers(void *db, int rcnt, int wcnt) {
  pt_data *pt_table;
  int i, tcnt;
#ifdef HAVE_PTHREAD
  int err;
  void *status;
  pthread_attr_t attr;
#endif

#if !defined(HAVE_PTHREAD) && !defined(_WIN32)
  fprintf(stderr, "No thread support: skipping tests.\n");
  return;
#endif

#ifdef HAVE_PTHREAD
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
#endif

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
  pthread_rwlock_init(&rwlock, NULL);
#endif

#ifdef SYNC_THREADS
#if defined(HAVE_PTHREAD)
  pthread_mutex_init(&twait_mutex, NULL);
  pthread_cond_init(&twait_cv, NULL);
#elif defined(_WIN32)
  /* Manual reset event, initial state nonsignaled. */
  twait_ev = CreateEvent(NULL, TRUE, FALSE, NULL);
#endif
#endif

  tcnt = rcnt + wcnt;
  pt_table = (pt_data *) malloc(tcnt * sizeof(pt_data));
  if(!pt_table) {
    fprintf(stderr, "Failed to allocate thread table: skipping tests.\n");
    return;
  }

  /* Spawn the threads */
#ifdef SYNC_THREADS
  twait_cnt = 0;
#endif
  for(i=0; i<tcnt; i++) {
    pt_table[i].db = db;
    pt_table[i].threadid = i;

    /* First wcnt threads are writers, the remainder readers */
    if(i<wcnt) {
#if defined(HAVE_PTHREAD)
      err = pthread_create(&pt_table[i].pth, &attr, writer_thread, \
        (void *) &pt_table[i]);
#elif defined(_WIN32)
      pt_table[i].hThread = CreateThread(NULL, 0,
        (LPTHREAD_START_ROUTINE) writer_thread,
        (LPVOID) &pt_table[i], 0, NULL);
#endif
    } else {
#if defined(HAVE_PTHREAD)
      err = pthread_create(&pt_table[i].pth, &attr, reader_thread, \
        (void *) &pt_table[i]);
#elif defined(_WIN32)
      pt_table[i].hThread = CreateThread(NULL, 0,
        (LPTHREAD_START_ROUTINE) reader_thread,
        (LPVOID) &pt_table[i], 0, NULL);
#endif
    }

#if defined(HAVE_PTHREAD)
    if(err) {
      fprintf(stderr, "Error code from pthread_create: %d.\n", err);
#elif defined(_WIN32)
    if(!pt_table[i].hThread) {
      /* XXX: GetLastError() gives the error code if needed */
      fprintf(stderr, "CreateThread failed.\n");
#endif
      goto workers_done;
    }
  }

#ifdef SYNC_THREADS
  /* Check that all workers have entered wait state */
  for(;;) {
    /* While reading a word from memory is atomic, we
     * still use the mutex because we want to guarantee
     * that the last thread has called pthread_cond_wait().
     * With Win32 API, condition variables with similar
     * functionality are available starting from Windows Vista,
     * so this implementation uses a simple synchronization
     * event instead. This causes small, probably non-relevant
     * loss in sync accuracy.
     */
#ifdef HAVE_PTHREAD
    pthread_mutex_lock(&twait_mutex);
#endif
    if(twait_cnt >= tcnt) break;
#ifdef HAVE_PTHREAD
    pthread_mutex_unlock(&twait_mutex);
#endif
  }

  /* Now wake up all threads */
#if defined(HAVE_PTHREAD)
  pthread_cond_broadcast(&twait_cv);
  pthread_mutex_unlock(&twait_mutex);
#elif defined(_WIN32)
  SetEvent(twait_ev);
#endif
#endif /* SYNC_THREADS */

  /* Join the workers (wait for them to complete) */
  for(i=0; i<tcnt; i++) {
#if defined(HAVE_PTHREAD)
    err = pthread_join(pt_table[i].pth, &status);
    if(err) {
      fprintf(stderr, "Error code from pthread_join: %d.\n", err);
      break;
    }
#elif defined(_WIN32)
    WaitForSingleObject(pt_table[i].hThread, INFINITE);
    CloseHandle(pt_table[i].hThread);
#endif
  }

workers_done:
#ifdef HAVE_PTHREAD
  pthread_attr_destroy(&attr);
#endif

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
  pthread_rwlock_destroy(&rwlock);
#endif

#ifdef SYNC_THREADS
#if defined(HAVE_PTHREAD)
  pthread_mutex_destroy(&twait_mutex);
  pthread_cond_destroy(&twait_cv);
#elif defined(_WIN32)
  CloseHandle(twait_ev);
#endif
#endif
  free(pt_table);
}

/** Writer thread
 *  Runs preconfigured number of basic write transactions
 */

worker_t writer_thread(void * threadarg) {
  void * db;
  int threadid, i, j, cksum;
  void *rec = NULL, *frec = NULL;

  db = ((pt_data *) threadarg)->db;
  threadid = ((pt_data *) threadarg)->threadid;

#ifdef CHATTY_THREADS
  fprintf(stdout, "Writer thread %d started.\n", threadid);
#endif

#ifdef SYNC_THREADS
  /* Increment the thread counter to inform the caller
   * that we are entering wait state.
   */
#ifdef HAVE_PTHREAD
  pthread_mutex_lock(&twait_mutex);
#endif
  twait_cnt++;
#if defined(HAVE_PTHREAD)
  pthread_cond_wait(&twait_cv, &twait_mutex);
  pthread_mutex_unlock(&twait_mutex);
#elif defined(_WIN32)
  WaitForSingleObject(twait_ev, INFINITE);
#endif
#endif /* SYNC_THREADS */

  frec = wg_get_first_record(db);
  for(i=0; i<WORKLOAD; i++) {
    wg_int c=-1, lock_id;

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
    pthread_rwlock_wrlock(&rwlock);
#else
    /* Start transaction */
    if(!(lock_id = wg_start_write(db))) {
      fprintf(stderr, "Writer thread %d: wg_start_write failed.\n", threadid);
      goto writer_done;
    }
#endif

    /* Fetch checksum */
    cksum = wg_decode_int(db, wg_get_field(db, frec, 0));

    /* Fetch record from database */
    if(i) rec = wg_get_next_record(db, rec);
    else rec = frec;
    if(!rec) {
      fprintf(stderr, "Writer thread %d: wg_get_next_record failed.\n", threadid);
#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
      pthread_rwlock_unlock(&rwlock);
#else
      wg_end_write(db, lock_id);
#endif
      goto writer_done;
    }

    /* Modify record */
    if(i) j = 0;
    else j = 1;
    for(; j<REC_SIZE; j++) {
      if (wg_set_int_field(db, rec, j, c--) != 0) {
        fprintf(stderr, "Writer thread %d: int storage error.\n", threadid);
#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
        pthread_rwlock_unlock(&rwlock);
#else
        wg_end_write(db, lock_id);
#endif
        goto writer_done;
      }
    }

    /* Update checksum */
    wg_set_int_field(db, frec, 0, ++cksum);

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
    pthread_rwlock_unlock(&rwlock);
#else
    /* End transaction */
    if(!wg_end_write(db, lock_id)) {
      fprintf(stderr, "Writer thread %d: wg_end_write failed.\n", threadid);
      goto writer_done;
    }
#endif
  }

#ifdef CHATTY_THREADS
  fprintf(stdout, "Writer thread %d ended.\n", threadid);
#endif

writer_done:
#if defined(HAVE_PTHREAD)
  pthread_exit(NULL);
#elif defined(_WIN32)
  return 0;
#endif
}

/** Reader thread
 *  Runs preconfigured number of read transactions
 */

worker_t reader_thread(void * threadarg) {
  void * db;
  int threadid, i, j;
  void *rec = NULL;

  db = ((pt_data *) threadarg)->db;
  threadid = ((pt_data *) threadarg)->threadid;

#ifdef CHATTY_THREADS
  fprintf(stdout, "Reader thread %d started.\n", threadid);
#endif

#ifdef SYNC_THREADS
  /* Enter wait state */
#ifdef HAVE_PTHREAD
  pthread_mutex_lock(&twait_mutex);
#endif
  twait_cnt++;
#if defined(HAVE_PTHREAD)
  pthread_cond_wait(&twait_cv, &twait_mutex);
  pthread_mutex_unlock(&twait_mutex);
#elif defined(_WIN32)
  WaitForSingleObject(twait_ev, INFINITE);
#endif
#endif /* SYNC_THREADS */

  for(i=0; i<WORKLOAD; i++) {
    wg_int reclen, lock_id;

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
    pthread_rwlock_rdlock(&rwlock);
#else
    /* Start transaction */
    if(!(lock_id = wg_start_read(db))) {
      fprintf(stderr, "Reader thread %d: wg_start_read failed.\n", threadid);
      goto reader_done;
    }
#endif

    /* Fetch record from database */
    if(i) rec = wg_get_next_record(db, rec);
    else rec = wg_get_first_record(db);
    if(!rec) {
      fprintf(stderr, "Reader thread %d: wg_get_next_record failed.\n", threadid);
#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
      pthread_rwlock_unlock(&rwlock);
#else
      wg_end_read(db, lock_id);
#endif
      goto reader_done;
    }

    /* Parse the record. We can safely ignore errors here,
     * except for the record length, which is supposed to be REC_SIZE
     */
    reclen = wg_get_record_len(db, rec);
    if (reclen < 0) {
      fprintf(stderr, "Reader thread %d: invalid record length.\n", threadid);
#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
      pthread_rwlock_unlock(&rwlock);
#else
      wg_end_read(db, lock_id);
#endif
      goto reader_done;
    }

    for(j=0; j<reclen; j++) {
      wg_get_field(db, rec, j);
      wg_get_field_type(db, rec, j);
    }

#if defined(BENCHMARK) && defined(HAVE_PTHREAD)
    pthread_rwlock_unlock(&rwlock);
#else
    /* End transaction */
    if(!wg_end_read(db, lock_id)) {
      fprintf(stderr, "Reader thread %d: wg_end_read failed.\n", threadid);
      goto reader_done;
    }
#endif
  }

#ifdef CHATTY_THREADS
  fprintf(stdout, "Reader thread %d ended.\n", threadid);
#endif

reader_done:
#if defined(HAVE_PTHREAD)
  pthread_exit(NULL);
#elif defined(_WIN32)
  return 0;
#endif
}

#ifdef __cplusplus
}
#endif
