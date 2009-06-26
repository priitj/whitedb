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

 /** @file dblock.c
 *  Concurrent access support for wgandalf memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <time.h>
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbdata.h" /* for CHECK */

/* ====== Private headers and defs ======== */

#include "dblock.h"

#define WAFLAG 0x1  /* writer active flag */
#define RC_INCR 0x2  /* increment step for reader count */

#define ASM32 1 /* XXX: handle using autotools etc */

#ifdef _WIN32
#define SPIN_COUNT 100000 /* break spin after this many cycles */
#define SLEEP_MSEC 1 /* minimum resolution is 1 millisecond */
#else
#define SPIN_COUNT 500 /* shorter spins perform better with Linux */
#define SLEEP_NSEC 500000 /* 500 microseconds */
#endif

/* ======= Private protos ================ */




/* ====== Functions ============== */


/* ----------- read and write transaction support ----------- */

/*
 * The following functions implement giant shared/exclusive
 * lock on the database. The rest of the db API is (currently)
 * implemented independently - therefore use of the locking routines
 * does not automatically guarantee isolation.
 *
 * Algorithm used for locking is simple reader-preference lock
 * using a single global sync variable (described by Mellor-Crummey
 * & Scott '92).
 */

/** Start write transaction
 *   Current implementation: acquire database level exclusive lock
 *   Blocks until lock is acquired.
 */

gint wg_start_write(void * db) {

  volatile gint *gl;
  int i;
#ifdef ASM32
  gint cond = 0;
#endif
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif

#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_start_write.\n");
    return 0;
  }
#endif  
  
  gl = offsettoptr(db,
    ((db_memsegment_header *) db)->locks.global_lock);

  /* First attempt at getting the lock without spinning */
#if defined(__GNUC__) && defined (ASM32)
  __asm__ __volatile__(
    "movl $0, %%eax;\n\t"
    "lock cmpxchgl %1, %2;\n\t"
    "setzb %0\n"
    : "=m" (cond)
    : "q" (WAFLAG), "m" (*gl)
    : "eax", "memory" );
  if(cond)
    return 1;
#elif defined(__GNUC__)
  if(__sync_bool_compare_and_swap(gl, 0, WAFLAG))
    return 1;
#elif defined(_WIN32)
  if(_InterlockedCompareExchange(gl, WAFLAG, 0) == 0)
    return 1;
#else
#error Atomic operations not implemented for this compiler
#endif

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
#if defined(__GNUC__) && defined (ASM32)
      __asm__ __volatile__(
        "pause;\n\t"
        "cmpl $0, %2;\n\t"
        "jne l1;\n\t"
        "movl $0, %%eax;\n\t"
        "lock cmpxchgl %1, %2;\n"
        "l1: setzb %0\n"
        : "=m" (cond)
        : "q" (WAFLAG), "m" (*gl)
        : "eax", "memory");
      if(cond)
        return 1;
#elif defined(__GNUC__)
      if(!(*gl) && __sync_bool_compare_and_swap(gl, 0, WAFLAG))
        return 1;
#elif defined(_WIN32)
      if(!(*gl) && _InterlockedCompareExchange(gl, WAFLAG, 0) == 0)
        return 1;
#else
#error Atomic operations not implemented for this compiler
#endif
    }
    
    /* Give up the CPU so the lock holder(s) can continue */
#ifdef _WIN32
    Sleep(ts);
    ts += SLEEP_MSEC;
#else
    nanosleep(&ts, NULL);
    ts.tv_nsec += SLEEP_NSEC;
#endif
  }

  return 0; /* dummy */
}

/** End write transaction
 *   Current implementation: release database level exclusive lock
 */

gint wg_end_write(void * db) {

  volatile gint *gl;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_end_write.\n");
    return 0;
  }
#endif  
  
  gl = offsettoptr(db,
    ((db_memsegment_header *) db)->locks.global_lock);

  /* Clear the writer active flag */
#if defined(__GNUC__)
  __sync_fetch_and_and(gl, ~(WAFLAG));
#elif defined(_WIN32)
  _InterlockedAnd(gl, ~(WAFLAG));
#else
#error Atomic operations not implemented for this compiler
#endif

  return 1;
}

/** Start read transaction
 *   Current implementation: acquire database level shared lock
 *   Increments reader count, blocks until there are no active
 *   writers.
 */

gint wg_start_read(void * db) {

  volatile gint *gl;
  int i;
#ifdef ASM32
  gint cond = 0;
#endif
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_start_read.\n");
    return 0;
  }
#endif  
  
  gl = offsettoptr(db,
    ((db_memsegment_header *) db)->locks.global_lock);

  /* Increment reader count atomically */
#if defined(__GNUC__)
  __sync_fetch_and_add(gl, RC_INCR);
#elif defined(_WIN32)
  _InterlockedExchangeAdd(gl, RC_INCR);
#else
#error Atomic operations not implemented for this compiler
#endif

  /* Try getting the lock without pause */
#if defined(__GNUC__) && defined (ASM32)
  __asm__(
    "movl %2, %%eax;\n\t"
    "andl %1, %%eax;\n\t"
    "setzb %0\n"
    : "=m" (cond)
    : "i" (WAFLAG), "m" (*gl)
    : "eax");
  if(cond)
    return 1;
#else
  if(!((*gl) & WAFLAG)) return 1;
#endif

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
#if defined(__GNUC__) && defined (ASM32)
      __asm__ __volatile__(
        "pause;\n\t"
        "movl %2, %%eax;\n\t"
        "andl %1, %%eax;\n\t"
        "setzb %0\n"
        : "=m" (cond)
        : "i" (WAFLAG), "m" (*gl)
        : "eax");
      if(cond)
        return 1;
#else
      if(!((*gl) & WAFLAG)) return 1;
#endif
    }

#ifdef _WIN32
    Sleep(ts);
    ts += SLEEP_MSEC;
#else
    nanosleep(&ts, NULL);
    ts.tv_nsec += SLEEP_NSEC;
#endif
  }

  return 0; /* dummy */
}

/** End read transaction
 *   Current implementation: release database level shared lock
 */

gint wg_end_read(void * db) {

  volatile gint *gl;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_end_read.\n");
    return 0;
  }
#endif  
  
  gl = offsettoptr(db,
    ((db_memsegment_header *) db)->locks.global_lock);

  /* Decrement reader count */
#if defined(__GNUC__)
  __sync_fetch_and_add(gl, -RC_INCR);
#elif defined(_WIN32)
  _InterlockedExchangeAdd(gl, -RC_INCR);
#else
#error Atomic operations not implemented for this compiler
#endif

  return 1;
}
