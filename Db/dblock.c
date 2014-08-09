/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009, 2010, 2011, 2013, 2014
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

 /** @file dblock.c
 *  Concurrent access support for WhiteDB memory database
 *
 *  Note: this file contains compiler and target-specific code.
 *  For compiling on plaforms that do not have support for
 *  specific opcodes needed for atomic operations and spinlocks,
 *  locking may be disabled by ./configure --disable-locking
 *  or by editing the appropriate config-xxx.h file. This will
 *  allow the code to compile, but concurrent access will NOT
 *  work.
 */

/* ====== Includes =============== */

#include <stdio.h>
#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <time.h>
#include <limits.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dblock.h"

#if (LOCK_PROTO==TFQUEUE)
#ifdef __linux__
#include <linux/futex.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/errno.h>
#endif
#endif

/* ====== Private headers and defs ======== */

#define compare_and_swap wg_compare_and_swap // wg_ prefix used in dblock.h, non-wg below

#ifndef LOCK_PROTO
#define DUMMY_ATOMIC_OPS /* allow compilation on unsupported platforms */
#endif

#if (LOCK_PROTO==RPSPIN) || (LOCK_PROTO==WPSPIN)
#define WAFLAG 0x1  /* writer active flag */
#define RC_INCR 0x2  /* increment step for reader count */
#else
/* classes of locks. */
#define LOCKQ_READ 0x02
#define LOCKQ_WRITE 0x04
#endif

/* Macro to emit Pentium 4 "pause" instruction. */
#if !defined(LOCK_PROTO)
#define MM_PAUSE
#elif defined(__GNUC__)
#if defined(__SSE2__)
#define MM_PAUSE {\
  __asm__ __volatile__("pause;\n");\
}
#else
#define MM_PAUSE
#endif
#elif defined(_WIN32)
#include <emmintrin.h>
#define MM_PAUSE { _mm_pause(); }
#endif

/* Helper function for implementing atomic operations
 * with gcc 4.3 / ARM EABI by Julian Brown.
 * This works on Linux ONLY.
 */
#if defined(__ARM_EABI__) && defined(__linux__)
typedef int (kernel_cmpxchg_t) (int oldval, int newval, int *ptr);
#define kernel_cmpxchg (*(kernel_cmpxchg_t *) 0xffff0fc0)
#endif

/* For easier testing of GCC version */
#ifdef __GNUC__
#define GCC_VERSION (__GNUC__ * 10000 \
                   + __GNUC_MINOR__ * 100 \
                   + __GNUC_PATCHLEVEL__)
#endif

/* Spinlock timings
 * SPIN_COUNT: how many cycles until CPU is yielded
 * SLEEP_MSEC and SLEEP_NSEC: increment of wait time after each cycle
 */
#ifdef _WIN32
#define SPIN_COUNT 100000 /* Windows scheduling seems to force this */
#define SLEEP_MSEC 1 /* minimum resolution is 1 millisecond */
#else
#define SPIN_COUNT 500 /* shorter spins perform better with Linux */
#define SLEEP_NSEC 500000 /* 500 microseconds */
#endif

#ifdef _WIN32
#define INIT_SPIN_TIMEOUT(t)
#else /* timings are in nsec */
#define INIT_SPIN_TIMEOUT(t) \
  if(t > INT_MAX/1000000) /* hack: primitive overflow protection */ \
    t = INT_MAX; \
  else \
    t *= 1000000;
#endif

#ifdef _WIN32
#define UPDATE_SPIN_TIMEOUT(t, ts) t -= ts;
#else
#define UPDATE_SPIN_TIMEOUT(t, ts) t -= ts.tv_nsec;
#endif

#define INIT_QLOCK_TIMEOUT(t, ts) \
  ts.tv_sec = t / 1000; \
  ts.tv_nsec = t % 1000;

#define ALLOC_LOCK(d, l) \
  l = alloc_lock(d); \
  if(!l) { \
    unlock_queue(d); \
    show_lock_error(d, "Failed to allocate lock"); \
    return 0; \
  }

#define DEQUEUE_LOCK(d, dbh, l, lp) \
  if(lp->prev) { \
    lock_queue_node *pp = offsettoptr(d, lp->prev); \
    pp->next = lp->next; \
  } \
  if(lp->next) { \
    lock_queue_node *np = offsettoptr(d, lp->next); \
    np->prev = lp->prev; \
  } else if(dbh->locks.tail == l) { \
    dbh->locks.tail = lp->prev; \
  }

/* ======= Private protos ================ */


#if (LOCK_PROTO==WPSPIN)
static void atomic_increment(volatile gint *ptr, gint incr);
#endif
#if (LOCK_PROTO==WPSPIN) || (LOCK_PROTO==RPSPIN)
static void atomic_and(volatile gint *ptr, gint val);
#endif
#if (LOCK_PROTO==RPSPIN)
static gint fetch_and_add(volatile gint *ptr, gint incr);
#endif
#if 0 /* unused */
static gint fetch_and_store(volatile gint *ptr, gint val);
#endif
// static gint compare_and_swap(volatile gint *ptr, gint oldv, gint newv);

#if (LOCK_PROTO==TFQUEUE)
static gint alloc_lock(void * db);
static void free_lock(void * db, gint node);
/*static gint deref_link(void *db, volatile gint *link);*/
#ifdef __linux__
#ifndef USE_LOCK_TIMEOUT
static void futex_wait(volatile gint *addr1, int val1);
#endif
static int futex_trywait(volatile gint *addr1, int val1,
  struct timespec *timeout);
static void futex_wake(volatile gint *addr1, int val1);
#endif
#endif

static gint show_lock_error(void *db, char *errmsg);


/* ====== Functions ============== */


/* -------------- helper functions -------------- */

/*
 * System- and platform-dependent atomic operations
 */

/** Atomic increment. On x86 platform, this is internally
 *  the same as fetch_and_add().
 */

#if (LOCK_PROTO==WPSPIN)
static void atomic_increment(volatile gint *ptr, gint incr) {
#if defined(DUMMY_ATOMIC_OPS)
  *ptr += incr;
#elif defined(__GNUC__)
#if defined(_MIPS_ARCH)
  gint tmp1, tmp2;  /* XXX: any way to get rid of these? */
  __asm__ __volatile__(
    ".set	noreorder\n\t"
    "1: ll	%0,%4\n\t"    /* load old */
    "add	%1,%0,%3\n\t" /* compute tmp2=tmp1+incr */
    "sc		%1,%2\n\t"    /* store new */
    "beqz	%1,1b\n\t"    /* SC failed, retry */
    "sync\n\t"
    ".set	reorder\n\t"
    : "=&r" (tmp1), "=&r" (tmp2), "=m" (*ptr)
    : "r" (incr), "m" (*ptr)
    : "memory");
#elif (GCC_VERSION < 40400) && defined(__ARM_EABI__) && defined(__linux__)
  gint failure, tmp;
  do {
    tmp = *ptr;
    failure = kernel_cmpxchg(tmp, tmp + incr, (int *) ptr);
  } while (failure != 0);
#else /* try gcc intrinsic */
  __sync_fetch_and_add(ptr, incr);
#endif
#elif defined(_WIN32)
  _InterlockedExchangeAdd(ptr, incr);
#else
#error Atomic operations not implemented for this compiler
#endif
}
#endif

/** Atomic AND operation.
 */

#if (LOCK_PROTO==WPSPIN) || (LOCK_PROTO==RPSPIN)
static void atomic_and(volatile gint *ptr, gint val) {
#if defined(DUMMY_ATOMIC_OPS)
  *ptr &= val;
#elif defined(__GNUC__)
#if defined(_MIPS_ARCH)
  gint tmp1, tmp2;
  __asm__ __volatile__(
    ".set	noreorder\n\t"
    "1: ll	%0,%4\n\t"      /* load old */
    "and	%1,%0,%3\n\t"   /* compute tmp2=tmp1 & val; */
    "sc		%1,%2\n\t"      /* store new */
    "beqz	%1,1b\n\t"      /* SC failed, retry */
    "sync\n\t"
    ".set	reorder\n\t"
    : "=&r" (tmp1), "=&r" (tmp2), "=m" (*ptr)
    : "r" (val), "m" (*ptr)
    : "memory");
#elif (GCC_VERSION < 40400) && defined(__ARM_EABI__) && defined(__linux__)
  gint failure, tmp;
  do {
    tmp = *ptr;
    failure = kernel_cmpxchg(tmp, tmp & val, (int *) ptr);
  } while (failure != 0);
#else /* try gcc intrinsic */
  __sync_fetch_and_and(ptr, val);
#endif
#elif defined(_WIN32)
  _InterlockedAnd(ptr, val);
#else
#error Atomic operations not implemented for this compiler
#endif
}
#endif

/** Atomic OR operation.
 */

#if 0 /* unused */
static void atomic_or(volatile gint *ptr, gint val) {
#if defined(DUMMY_ATOMIC_OPS)
  *ptr |= val;
#elif defined(__GNUC__)
#if defined(_MIPS_ARCH)
  gint tmp1, tmp2;
  __asm__ __volatile__(
    ".set	noreorder\n\t"
    "1: ll	%0,%4\n\t"      /* load old */
    "or		%1,%0,%3\n\t"   /* compute tmp2=tmp1 | val; */
    "sc		%1,%2\n\t"      /* store new */
    "beqz	%1,1b\n\t"      /* SC failed, retry */
    "sync\n\t"
    ".set	reorder\n\t"
    : "=&r" (tmp1), "=&r" (tmp2), "=m" (*ptr)
    : "r" (val), "m" (*ptr)
    : "memory");
#elif (GCC_VERSION < 40400) && defined(__ARM_EABI__) && defined(__linux__)
  gint failure, tmp;
  do {
    tmp = *ptr;
    failure = kernel_cmpxchg(tmp, tmp | val, (int *) ptr);
  } while (failure != 0);
#else /* try gcc intrinsic */
  __sync_fetch_and_or(ptr, val);
#endif
#elif defined(_WIN32)
  _InterlockedOr(ptr, val);
#else
#error Atomic operations not implemented for this compiler
#endif
}
#endif

/** Fetch and (dec|inc)rement. Returns value before modification.
 */

#if (LOCK_PROTO==RPSPIN)
static gint fetch_and_add(volatile gint *ptr, gint incr) {
#if defined(DUMMY_ATOMIC_OPS)
  gint tmp = *ptr;
  *ptr += incr;
  return tmp;
#elif defined(__GNUC__)
#if defined(_MIPS_ARCH)
  gint ret, tmp;
  __asm__ __volatile__(
    ".set	noreorder\n\t"
    "1: ll	%0,%4\n\t"      /* load old */
    "add	%1,%0,%3\n\t"   /* compute tmp=ret+incr */
    "sc		%1,%2\n\t"      /* store new */
    "beqz	%1,1b\n\t"      /* SC failed, retry */
    "sync\n\t"
    ".set	reorder\n\t"
    : "=&r" (ret), "=&r" (tmp), "=m" (*ptr)
    : "r" (incr), "m" (*ptr)
    : "memory");
  return ret;
#elif (GCC_VERSION < 40400) && defined(__ARM_EABI__) && defined(__linux__)
  gint failure, tmp;
  do {
    tmp = *ptr;
    failure = kernel_cmpxchg(tmp, tmp + incr, (int *) ptr);
  } while (failure != 0);
  return tmp;
#else /* try gcc intrinsic */
  return __sync_fetch_and_add(ptr, incr);
#endif
#elif defined(_WIN32)
  return _InterlockedExchangeAdd(ptr, incr);
#else
#error Atomic operations not implemented for this compiler
#endif
}
#endif

/** Atomic fetch and store. Swaps two values.
 */

#if 0 /* unused */
static gint fetch_and_store(volatile gint *ptr, gint val) {
  /* Despite the name, the GCC builtin should just
   * issue XCHG operation. There is no testing of
   * anything, just lock the bus and swap the values,
   * as per Intel's opcode reference.
   *
   * XXX: not available on all compiler targets :-(
   */
#if defined(DUMMY_ATOMIC_OPS)
  gint tmp = *ptr;
  *ptr = val;
  return tmp;
#elif defined(__GNUC__)
#if defined(_MIPS_ARCH)
  gint ret, tmp;
  __asm__ __volatile__(
    ".set	noreorder\n\t"
    "1: ll	%0,%4\n\t"  /* load old */
    "move	%1,%3\n\t"
    "sc		%1,%2\n\t"  /* store new */
    "beqz	%1,1b\n\t"  /* SC failed, retry */
    "sync\n\t"
    ".set	reorder\n\t"
    : "=&r" (ret), "=&r" (tmp), "=m" (*ptr)
    : "r" (val), "m" (*ptr)
    : "memory");
  return ret;
#elif (GCC_VERSION < 40400) && defined(__ARM_EABI__) && defined(__linux__)
  gint failure, oldval;
  do {
    oldval = *ptr;
    failure = kernel_cmpxchg(oldval, val, (int *) ptr);
  } while (failure != 0);
  return oldval;
#else /* try gcc intrinsic */
  return __sync_lock_test_and_set(ptr, val);
#endif
#elif defined(_WIN32)
  return _InterlockedExchange(ptr, val);
#else
#error Atomic operations not implemented for this compiler
#endif
}
#endif

/** Compare and swap. If value at ptr equals old, set it to
 *  new and return 1. Otherwise the function returns 0.
 */

gint wg_compare_and_swap(volatile gint *ptr, gint oldv, gint newv) {
#if defined(DUMMY_ATOMIC_OPS)
  if(*ptr == oldv) {
    *ptr = newv;
    return 1;
  }
  return 0;
#elif defined(__GNUC__)
#if defined(_MIPS_ARCH)
  gint ret;
  __asm__ __volatile__(
    ".set	noreorder\n\t"
    "1: ll	%0,%4\n\t"
    "bne	%0,%2,2f\n\t"   /* *ptr!=oldv, return *ptr */
    "move	%0,%3\n\t"
    "sc		%0,%1\n\t"
    "beqz	%0,1b\n\t"      /* SC failed, retry */
    "move	%0,%2\n\t"      /* return oldv (*ptr==newv now) */
    "2: sync\n\t"
    ".set	reorder\n\t"
    : "=&r" (ret), "=m" (*ptr)
    : "r" (oldv), "r" (newv), "m" (*ptr)
    : "memory");
  return ret == oldv;
#elif (GCC_VERSION < 40400) && defined(__ARM_EABI__) && defined(__linux__)
  gint failure = kernel_cmpxchg(oldv, newv, (int *) ptr);
  return (failure == 0);
#else /* try gcc intrinsic */
  return __sync_bool_compare_and_swap(ptr, oldv, newv);
#endif
#elif defined(_WIN32)
  return (_InterlockedCompareExchange(ptr, newv, oldv) == oldv);
#else
#error Atomic operations not implemented for this compiler
#endif
}

/* ----------- read and write transaction support ----------- */

/*
 * Read and write transactions are currently realized using database
 * level locking. The rest of the db API is implemented independently -
 * therefore use of the locking routines does not automatically guarantee
 * isolation, rather, all of the concurrently accessing clients are expected
 * to follow the same protocol.
 */

/** Start write transaction
 *   Current implementation: acquire database level exclusive lock
 */

gint wg_start_write(void * db) {
  return db_wlock(db, DEFAULT_LOCK_TIMEOUT);
}

/** End write transaction
 *   Current implementation: release database level exclusive lock
 */

gint wg_end_write(void * db, gint lock) {
  return db_wulock(db, lock);
}

/** Start read transaction
 *   Current implementation: acquire database level shared lock
 */

gint wg_start_read(void * db) {
  return db_rlock(db, DEFAULT_LOCK_TIMEOUT);
}

/** End read transaction
 *   Current implementation: release database level shared lock
 */

gint wg_end_read(void * db, gint lock) {
  return db_rulock(db, lock);
}

/*
 * The following functions implement a giant shared/exclusive
 * lock on the database.
 *
 * Algorithms used for locking:
 *
 * 1. Simple reader-preference lock using a single global sync
 *    variable (described by Mellor-Crummey & Scott '92).
 * 2. A writer-preference spinlock based on the above.
 * 3. A task-fair lock implemented using a queue. Similar to
 *    the queue-based MCS rwlock, but uses futexes to synchronize
 *    the waiting processes.
 */

#if (LOCK_PROTO==RPSPIN)

/** Acquire database level exclusive lock (reader-preference spinlock)
 *   Blocks until lock is acquired.
 *   If USE_LOCK_TIMEOUT is defined, may return without locking
 */

#ifdef USE_LOCK_TIMEOUT
gint db_rpspin_wlock(void * db, gint timeout) {
#else
gint db_rpspin_wlock(void * db) {
#endif
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  volatile gint *gl;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_wlock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* First attempt at getting the lock without spinning */
  if(compare_and_swap(gl, 0, WAFLAG))
    return 1;

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#endif

#ifdef USE_LOCK_TIMEOUT
  INIT_SPIN_TIMEOUT(timeout)
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
      MM_PAUSE
      if(!(*gl) && compare_and_swap(gl, 0, WAFLAG))
        return 1;
    }

    /* Check if we would time out during next sleep. Note that
     * this is not a real time measurement.
     */
#ifdef USE_LOCK_TIMEOUT
    UPDATE_SPIN_TIMEOUT(timeout, ts)
    if(timeout < 0)
      return 0;
#endif

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

/** Release database level exclusive lock (reader-preference spinlock)
 */

gint db_rpspin_wulock(void * db) {

  volatile gint *gl;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_wulock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* Clear the writer active flag */
  atomic_and(gl, ~(WAFLAG));

  return 1;
}

/** Acquire database level shared lock (reader-preference spinlock)
 *   Increments reader count, blocks until there are no active
 *   writers.
 *   If USE_LOCK_TIMEOUT is defined, may return without locking.
 */

#ifdef USE_LOCK_TIMEOUT
gint db_rpspin_rlock(void * db, gint timeout) {
#else
gint db_rpspin_rlock(void * db) {
#endif
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  volatile gint *gl;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_rlock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* Increment reader count atomically */
  fetch_and_add(gl, RC_INCR);

  /* Try getting the lock without pause */
  if(!((*gl) & WAFLAG)) return 1;

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#endif

#ifdef USE_LOCK_TIMEOUT
  INIT_SPIN_TIMEOUT(timeout)
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
      MM_PAUSE
      if(!((*gl) & WAFLAG)) return 1;
    }

    /* Check for timeout. */
#ifdef USE_LOCK_TIMEOUT
    UPDATE_SPIN_TIMEOUT(timeout, ts)
    if(timeout < 0) {
      /* We're no longer waiting, restore the counter */
      fetch_and_add(gl, -RC_INCR);
      return 0;
    }
#endif

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

/** Release database level shared lock (reader-preference spinlock)
 */

gint db_rpspin_rulock(void * db) {

  volatile gint *gl;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_rulock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* Decrement reader count */
  fetch_and_add(gl, -RC_INCR);

  return 1;
}

#elif (LOCK_PROTO==WPSPIN)

/** Acquire database level exclusive lock (writer-preference spinlock)
 *   Blocks until lock is acquired.
 */

#ifdef USE_LOCK_TIMEOUT
gint db_wpspin_wlock(void * db, gint timeout) {
#else
gint db_wpspin_wlock(void * db) {
#endif
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  volatile gint *gl, *w;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_wlock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);
  w = (gint *) offsettoptr(db, dbmemsegh(db)->locks.writers);

  /* Let the readers know a writer is present */
  atomic_increment(w, 1);

  /* First attempt at getting the lock without spinning */
  if(compare_and_swap(gl, 0, WAFLAG))
    return 1;

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#endif

#ifdef USE_LOCK_TIMEOUT
  INIT_SPIN_TIMEOUT(timeout)
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
      MM_PAUSE
      if(!(*gl) && compare_and_swap(gl, 0, WAFLAG))
        return 1;
    }

    /* Check for timeout. */
#ifdef USE_LOCK_TIMEOUT
    UPDATE_SPIN_TIMEOUT(timeout, ts)
    if(timeout < 0) {
      /* Restore the previous writer count */
      atomic_increment(w, -1);
      return 0;
    }
#endif

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

/** Release database level exclusive lock (writer-preference spinlock)
 */

gint db_wpspin_wulock(void * db) {

  volatile gint *gl, *w;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_wulock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);
  w = (gint *) offsettoptr(db, dbmemsegh(db)->locks.writers);

  /* Clear the writer active flag */
  atomic_and(gl, ~(WAFLAG));

  /* writers-- */
  atomic_increment(w, -1);

  return 1;
}

/** Acquire database level shared lock (writer-preference spinlock)
 *   Blocks until there are no active or waiting writers, then increments
 *   reader count atomically.
 */

#ifdef USE_LOCK_TIMEOUT
gint db_wpspin_rlock(void * db, gint timeout) {
#else
gint db_wpspin_rlock(void * db) {
#endif
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  volatile gint *gl, *w;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_rlock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);
  w = (gint *) offsettoptr(db, dbmemsegh(db)->locks.writers);

  /* Try locking without spinning */
  if(!(*w)) {
    gint readers = (*gl) & ~WAFLAG;
    if(compare_and_swap(gl, readers, readers + RC_INCR))
      return 1;
  }

#ifdef USE_LOCK_TIMEOUT
  INIT_SPIN_TIMEOUT(timeout)
#endif

  for(;;) {
#ifdef _WIN32
    ts = SLEEP_MSEC;
#else
    ts.tv_sec = 0;
    ts.tv_nsec = SLEEP_NSEC;
#endif

    /* Spin-wait until writers disappear */
    while(*w) {
      for(i=0; i<SPIN_COUNT; i++) {
        MM_PAUSE
        if(!(*w)) goto no_writers;
      }

#ifdef USE_LOCK_TIMEOUT
      UPDATE_SPIN_TIMEOUT(timeout, ts)
      if(timeout < 0)
        return 0;
#endif

#ifdef _WIN32
      Sleep(ts);
      ts += SLEEP_MSEC;
#else
      nanosleep(&ts, NULL);
      ts.tv_nsec += SLEEP_NSEC;
#endif
    }
no_writers:

    do {
      gint readers = (*gl) & ~WAFLAG;
      /* Atomically increment the reader count. If a writer has activated,
       * this fails and the do loop will also exit. If another reader modifies
       * the value, we retry.
       *
       * XXX: maybe MM_PAUSE and non-atomic checking can affect the
       * performance here, like in spin loops (this is more like a
       * retry loop though, not clear how many times it will typically
       * repeat).
       */
      if(compare_and_swap(gl, readers, readers + RC_INCR))
        return 1;
    } while(!(*w));
  }

  return 0; /* dummy */
}

/** Release database level shared lock (writer-preference spinlock)
 */

gint db_wpspin_rulock(void * db) {

  volatile gint *gl;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_rulock");
    return 0;
  }
#endif

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* Decrement reader count */
  atomic_increment(gl, -RC_INCR);

  return 1;
}

#elif (LOCK_PROTO==TFQUEUE)

/** Acquire the queue mutex.
 */
static void lock_queue(void * db) {
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  volatile gint *gl;

  /* skip the database pointer check, this function is not called directly */
  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.queue_lock);

  /* First attempt at getting the lock without spinning */
  if(compare_and_swap(gl, 0, 1))
    return;

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
      MM_PAUSE
      if(!(*gl) && compare_and_swap(gl, 0, 1))
        return;
    }

    /* Backoff */
#ifdef _WIN32
    Sleep(ts);
    ts += SLEEP_MSEC;
#else
    nanosleep(&ts, NULL);
    ts.tv_nsec += SLEEP_NSEC;
#endif
  }
}

/** Release the queue mutex
 */
static void unlock_queue(void * db) {
  volatile gint *gl;

  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.queue_lock);

  *gl = 0;
}

/** Acquire database level exclusive lock (task-fair queued lock)
 *   Blocks until lock is acquired.
 *   If USE_LOCK_TIMEOUT is defined, may return without locking
 */

#ifdef USE_LOCK_TIMEOUT
gint db_tfqueue_wlock(void * db, gint timeout) {
#else
gint db_tfqueue_wlock(void * db) {
#endif
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  gint lock, prev;
  lock_queue_node *lockp;
  db_memsegment_header* dbh;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_wlock");
    return 0;
  }
#endif

  dbh = dbmemsegh(db);

  lock_queue(db);
  ALLOC_LOCK(db, lock)

  prev = dbh->locks.tail;
  dbh->locks.tail = lock;

  lockp = (lock_queue_node *) offsettoptr(db, lock);
  lockp->class = LOCKQ_WRITE;
  lockp->prev = prev;
  lockp->next = 0;

  if(prev) {
    lock_queue_node *prevp = offsettoptr(db, prev);
    prevp->next = lock;
    lockp->waiting = 1;
  } else {
    lockp->waiting = 0;
  }

  unlock_queue(db);

  if(lockp->waiting) {
#ifdef __linux__
#ifdef USE_LOCK_TIMEOUT
    INIT_QLOCK_TIMEOUT(timeout, ts)
    if(futex_trywait(&lockp->waiting, 1, &ts) == ETIMEDOUT) {
      lock_queue(db);
      DEQUEUE_LOCK(db, dbh, lock, lockp)
      free_lock(db, lock);
      unlock_queue(db);
      return 0;
    }
#else
    futex_wait(&lockp->waiting, 1);
#endif
#else
/* XXX: add support for other platforms */
#error This code needs Linux SYS_futex service to function
#endif
  }

  return lock;
}

/** Release database level exclusive lock (task-fair queued lock)
 */

gint db_tfqueue_wulock(void * db, gint lock) {
  lock_queue_node *lockp;
  db_memsegment_header* dbh;
  volatile gint *syn_addr = NULL;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_wulock");
    return 0;
  }
#endif

  dbh = dbmemsegh(db);
  lockp = (lock_queue_node *) offsettoptr(db, lock);

  lock_queue(db);
  if(lockp->next) {
    lock_queue_node *nextp = offsettoptr(db, lockp->next);
    nextp->waiting = 0;
    nextp->prev = 0; /* we're a writer lock, head of the queue */
    syn_addr = &nextp->waiting;
  } else if(dbh->locks.tail == lock) {
    dbh->locks.tail = 0;
  }
  free_lock(db, lock);
  unlock_queue(db);
  if(syn_addr) {
#ifdef __linux__
    futex_wake(syn_addr, 1);
#else
/* XXX: add support for other platforms */
#error This code needs Linux SYS_futex service to function
#endif
  }

  return 1;
}

/** Acquire database level shared lock (task-fair queued lock)
 *   If USE_LOCK_TIMEOUT is defined, may return without locking.
 */

#ifdef USE_LOCK_TIMEOUT
gint db_tfqueue_rlock(void * db, gint timeout) {
#else
gint db_tfqueue_rlock(void * db) {
#endif
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif
  gint lock, prev;
  lock_queue_node *lockp;
  db_memsegment_header* dbh;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_rlock");
    return 0;
  }
#endif

  dbh = dbmemsegh(db);

  lock_queue(db);
  ALLOC_LOCK(db, lock)

  prev = dbh->locks.tail;
  dbh->locks.tail = lock;

  lockp = (lock_queue_node *) offsettoptr(db, lock);
  lockp->class = LOCKQ_READ;
  lockp->prev = prev;
  lockp->next = 0;

  if(prev) {
    lock_queue_node *prevp = (lock_queue_node *) offsettoptr(db, prev);
    prevp->next = lock;

    if(prevp->class == LOCKQ_READ && prevp->waiting == 0) {
      lockp->waiting = 0;
    } else {
      lockp->waiting = 1;
    }
  } else {
    lockp->waiting = 0;
  }
  unlock_queue(db);

  if(lockp->waiting) {
    volatile gint *syn_addr = NULL;
#ifdef __linux__
#ifdef USE_LOCK_TIMEOUT
    INIT_QLOCK_TIMEOUT(timeout, ts)
    if(futex_trywait(&lockp->waiting, 1, &ts) == ETIMEDOUT) {
      lock_queue(db);
      DEQUEUE_LOCK(db, dbh, lock, lockp)
      free_lock(db, lock);
      unlock_queue(db);
      return 0;
    }
#else
    futex_wait(&lockp->waiting, 1);
#endif
#else
/* XXX: add support for other platforms */
#error This code needs Linux SYS_futex service to function
#endif
    lock_queue(db);
    if(lockp->next) {
      lock_queue_node *nextp = offsettoptr(db, lockp->next);
      if(nextp->class == LOCKQ_READ && nextp->waiting) {
        nextp->waiting = 0;
        syn_addr = &nextp->waiting;
      }
    }
    unlock_queue(db);
    if(syn_addr) {
#ifdef __linux__
      futex_wake(syn_addr, 1);
#else
/* XXX: add support for other platforms */
#error This code needs Linux SYS_futex service to function
#endif
    }
  }

  return lock;
}

/** Release database level shared lock (task-fair queued lock)
 */

gint db_tfqueue_rulock(void * db, gint lock) {
  lock_queue_node *lockp;
  db_memsegment_header* dbh;
  volatile gint *syn_addr = NULL;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in db_rulock");
    return 0;
  }
#endif

  dbh = dbmemsegh(db);
  lockp = (lock_queue_node *) offsettoptr(db, lock);

  lock_queue(db);
  if(lockp->prev) {
    lock_queue_node *prevp = offsettoptr(db, lockp->prev);
    prevp->next = lockp->next;
  }
  if(lockp->next) {
    lock_queue_node *nextp = offsettoptr(db, lockp->next);
    nextp->prev = lockp->prev;
    if(nextp->waiting && (!lockp->prev || nextp->class == LOCKQ_READ)) {
      nextp->waiting = 0;
      syn_addr = &nextp->waiting;
    }
  } else if(dbh->locks.tail == lock) {
    dbh->locks.tail = lockp->prev;
  }
  free_lock(db, lock);
  unlock_queue(db);
  if(syn_addr) {
#ifdef __linux__
    futex_wake(syn_addr, 1);
#else
/* XXX: add support for other platforms */
#error This code needs Linux SYS_futex service to function
#endif
  }

  return 1;
}

#endif /* LOCK_PROTO */

/** Initialize locking subsystem.
 *   Not parallel-safe, so should be run during database init.
 *
 * Note that this function is called even if locking is disabled.
 */
gint wg_init_locks(void * db) {
#if (LOCK_PROTO==TFQUEUE)
  gint i, chunk_wall;
  lock_queue_node *tmp = NULL;
#endif
  db_memsegment_header* dbh;

#ifdef CHECK
  if (!dbcheck(db) && !dbcheckinit(db)) {
    show_lock_error(db, "Invalid database pointer in wg_init_locks");
    return -1;
  }
#endif
  dbh = dbmemsegh(db);

#if (LOCK_PROTO==TFQUEUE)
  chunk_wall = dbh->locks.storage + dbh->locks.max_nodes*SYN_VAR_PADDING;

  for(i=dbh->locks.storage; i<chunk_wall; ) {
    tmp = (lock_queue_node *) offsettoptr(db, i);
    i+=SYN_VAR_PADDING;
    tmp->next_cell = i; /* offset of next cell */
  }
  tmp->next_cell=0; /* last node */

  /* top of the stack points to first cell in chunk */
  dbh->locks.freelist = dbh->locks.storage;

  /* reset the state */
  dbh->locks.tail = 0; /* 0 is considered invalid offset==>no value */
  dbstore(db, dbh->locks.queue_lock, 0);
#else
  dbstore(db, dbh->locks.global_lock, 0);
  dbstore(db, dbh->locks.writers, 0);
#endif
  return 0;
}

#if (LOCK_PROTO==TFQUEUE)

/* ---------- memory management for queued locks ---------- */

/*
 * Queued locks algorithm assumes allocating memory cells
 * for each lock. These cells need to be memory-aligned to
 * allow spinlocks run locally, but more importantly, allocation
 * and freeing of the cells has to be implemented in a lock-free
 * manner.
 *
 * The method used in the initial implementation is freelist
 * with reference counts (generally described by Valois '95,
 * actual code is based on examples from
 * http://www.non-blocking.com/Eng/services-technologies_non-blocking-lock-free.htm)
 *
 * XXX: code untested currently
 * XXX: Mellor-Crummey & Scott algorithm possibly does not need
 *      refcounts. If so, they should be #ifdef-ed out, but
 *      kept for possible future expansion.
 */

/** Allocate memory cell for a lock.
 *   Used internally only, so we assume the passed db pointer
 *   is already validated.
 *
 *   Returns offset to allocated cell.
 */

#if 0
static gint alloc_lock(void * db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  lock_queue_node *tmp;

  for(;;) {
    gint t = dbh->locks.freelist;
    if(!t)
      return 0; /* end of chain :-( */
    tmp = (lock_queue_node *) offsettoptr(db, t);

    fetch_and_add(&(tmp->refcount), 2);

    if(compare_and_swap(&(dbh->locks.freelist), t, tmp->next_cell)) {
      fetch_and_add(&(tmp->refcount), -1); /* clear lsb */
      return t;
    }

    free_lock(db, t);
  }

  return 0; /* dummy */
}

/** Release memory cell for a lock.
 *   Used internally only.
 */

static void free_lock(void * db, gint node) {
  db_memsegment_header* dbh = dbmemsegh(db);
  lock_queue_node *tmp;
  volatile gint t;

  tmp = (lock_queue_node *) offsettoptr(db, node);

  /* Clear reference */
  fetch_and_add(&(tmp->refcount), -2);

  /* Try to set lsb */
  if(compare_and_swap(&(tmp->refcount), 0, 1)) {

/* XXX:
    if(tmp->next_cell) free_lock(db, tmp->next_cell);
*/
    do {
      t = dbh->locks.freelist;
      tmp->next_cell = t;
    } while (!compare_and_swap(&(dbh->locks.freelist), t, node));
  }
}

/** De-reference (release pointer to) a link.
 *   Used internally only.
 */

static gint deref_link(void *db, volatile gint *link) {
  lock_queue_node *tmp;
  volatile gint t;

  for(;;) {
    t = *link;
    if(t == 0) return 0;

    tmp = (lock_queue_node *) offsettoptr(db, t);

    fetch_and_add(&(tmp->refcount), 2);
    if(t == *link) return t;
    free_lock(db, t);
  }
}

#else
/* Simple lock memory allocation (non lock-free) */

static gint alloc_lock(void * db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint t = dbh->locks.freelist;
  lock_queue_node *tmp;

  if(!t)
    return 0; /* end of chain :-( */
  tmp = (lock_queue_node *) offsettoptr(db, t);

  dbh->locks.freelist = tmp->next_cell;
  return t;
}

static void free_lock(void * db, gint node) {
  db_memsegment_header* dbh = dbmemsegh(db);
  lock_queue_node *tmp = (lock_queue_node *) offsettoptr(db, node);
  tmp->next_cell = dbh->locks.freelist;
  dbh->locks.freelist = node;
}

#endif

#ifdef __linux__
/* Futex operations */

#ifndef USE_LOCK_TIMEOUT
static void futex_wait(volatile gint *addr1, int val1)
{
  syscall(SYS_futex, (void *) addr1, FUTEX_WAIT, val1, NULL);
}
#endif

static int futex_trywait(volatile gint *addr1, int val1,
  struct timespec *timeout)
{
  if(syscall(SYS_futex, (void *) addr1, FUTEX_WAIT, val1, timeout) == -1)
    return errno; /* On Linux, this is thread-safe. Caution needed however */
  else
    return 0;
}

static void futex_wake(volatile gint *addr1, int val1)
{
  syscall(SYS_futex, (void *) addr1, FUTEX_WAKE, val1);
}
#endif

#endif /* LOCK_PROTO==TFQUEUE */


/* ------------ error handling ---------------- */

static gint show_lock_error(void *db, char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg locking error: %s.\n", errmsg);
#endif
  return -1;
}

#ifdef __cplusplus
}
#endif
