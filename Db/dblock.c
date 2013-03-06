/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009, 2010, 2011
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
 *  Note: this file contains compiler and target-specific code.
 *  For compiling on plaforms that do not have support for
 *  specific opcodes needed for atomic operations and spinlocks,
 *  DUMMY_LOCKS may be defined via ./configure --enable-dummy-locks
 *  or by editing the appropriate config-xxx.h file. This will
 *  allow the code to compile correctly, but concurrent access will NOT
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

/* ====== Private headers and defs ======== */

#include "dblock.h"

#ifndef QUEUED_LOCKS
#define WAFLAG 0x1  /* writer active flag */
#define RC_INCR 0x2  /* increment step for reader count */
#else
/* classes of locks. Class "none" is also possible, but
 * this is defined as 0x0 to simplify some atomic operations */
#define LOCKQ_READ 0x02
#define LOCKQ_WRITE 0x04
#endif

/* Macro to emit Pentium 4 "pause" instruction. */
#if defined(DUMMY_LOCKS)
#define _MM_PAUSE
#elif defined(__GNUC__)
#if defined(__i686__) || defined(__amd64__)  /* assume SSE2 support */
#define _MM_PAUSE {\
  __asm__ __volatile__("pause;\n");\
}
#else
#define _MM_PAUSE
#endif
#elif defined(_WIN32)
#define _MM_PAUSE {\
  __asm {_emit 0xf3}; __asm{_emit 0x90};\
}
#endif

/* Helper function for implementing atomic operations
 * with gcc 4.3 / ARM EABI by Julian Brown.
 * This works on Linux ONLY.
 */
#if defined(__ARM_EABI__) && defined(__linux__)
typedef int (__kernel_cmpxchg_t) (int oldval, int newval, int *ptr);
#define __kernel_cmpxchg (*(__kernel_cmpxchg_t *) 0xffff0fc0)
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
#ifndef QUEUED_LOCKS
#ifdef _WIN32
#define SPIN_COUNT 100000 /* Windows scheduling seems to force this */
#define SLEEP_MSEC 1 /* minimum resolution is 1 millisecond */
#else
#define SPIN_COUNT 500 /* shorter spins perform better with Linux */
#define SLEEP_NSEC 500000 /* 500 microseconds */
#endif
#else  /* QUEUED_LOCKS */
#define SPIN_COUNT 500
#ifdef _WIN32
#define SLEEP_MSEC 0 /* The only way this will even remotely work is
                      * is if we simply yield the CPU using Sleep(0).
                      * However, this is a bad practice since processes
                      * with elevated priority can block us out forever. */
#else
#define SLEEP_NSEC 1 /* just deschedule thread */
#endif
#endif /* QUEUED_LOCKS */

#ifdef _WIN32
/* XXX: quick hack for MSVC. Should probably find a cleaner solution */
#define inline __inline
#endif

/* XXX: update QUEUED_LOCKS code and remove this */
#if defined(QUEUED_LOCKS) && defined(USE_LOCK_TIMEOUT)
#error USE_LOCK_TIMEOUT cannot be used with queued locks.
#endif

/* ======= Private protos ================ */


static inline void atomic_increment(volatile gint *ptr, gint incr);
static inline void atomic_and(volatile gint *ptr, gint val);
static inline gint fetch_and_add(volatile gint *ptr, gint incr);
static inline gint fetch_and_store(volatile gint *ptr, gint val);
static inline gint compare_and_swap(volatile gint *ptr, gint oldv, gint newv);

#ifdef QUEUED_LOCKS
static gint alloc_lock(void * db);
static void free_lock(void * db, gint node);
static gint deref_link(void *db, volatile gint *link);
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

static inline void atomic_increment(volatile gint *ptr, gint incr) {
#if defined(DUMMY_LOCKS)
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
    failure = __kernel_cmpxchg(tmp, tmp + incr, (int *) ptr);
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

/** Atomic AND operation.
 */

static inline void atomic_and(volatile gint *ptr, gint val) {
#if defined(DUMMY_LOCKS)
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
    failure = __kernel_cmpxchg(tmp, tmp & val, (int *) ptr);
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

/** Atomic OR operation.
 */

static inline void atomic_or(volatile gint *ptr, gint val) {
#if defined(DUMMY_LOCKS)
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
    failure = __kernel_cmpxchg(tmp, tmp | val, (int *) ptr);
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

/** Fetch and (dec|inc)rement. Returns value before modification.
 */

static inline gint fetch_and_add(volatile gint *ptr, gint incr) {
#if defined(DUMMY_LOCKS)
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
    failure = __kernel_cmpxchg(tmp, tmp + incr, (int *) ptr);
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

/** Atomic fetch and store. Swaps two values.
 */

static inline gint fetch_and_store(volatile gint *ptr, gint val) {
  /* Despite the name, the GCC builtin should just
   * issue XCHG operation. There is no testing of
   * anything, just lock the bus and swap the values,
   * as per Intel's opcode reference.
   *
   * XXX: not available on all compiler targets :-(
   */
#if defined(DUMMY_LOCKS)
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
    failure = __kernel_cmpxchg(oldval, val, (int *) ptr);
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

/** Compare and swap. If value at ptr equals old, set it to
 *  new and return 1. Otherwise the function returns 0.
 */

static inline gint compare_and_swap(volatile gint *ptr, gint oldv, gint newv) {
#if defined(DUMMY_LOCKS)
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
  gint failure = __kernel_cmpxchg(oldv, newv, (int *) ptr);
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
#ifdef USE_LOCK_TIMEOUT
  return wg_db_wlock(db, DEFAULT_LOCK_TIMEOUT);
#else
  return wg_db_wlock(db);
#endif
}

/** End write transaction
 *   Current implementation: release database level exclusive lock
 */

gint wg_end_write(void * db, gint lock) {
  return wg_db_wulock(db, lock);
}

/** Start read transaction
 *   Current implementation: acquire database level shared lock
 */

gint wg_start_read(void * db) {
#ifdef USE_LOCK_TIMEOUT
  return wg_db_rlock(db, DEFAULT_LOCK_TIMEOUT);
#else
  return wg_db_rlock(db);
#endif
}

/** End read transaction
 *   Current implementation: release database level shared lock
 */

gint wg_end_read(void * db, gint lock) {
  return wg_db_rulock(db, lock);
}

/*
 * The following functions implement a giant shared/exclusive
 * lock on the database.
 *
 * Algorithms used for locking:
 *
 * 1. Simple reader-preference lock using a single global sync
 *    variable (described by Mellor-Crummey & Scott '92).
 * 2. Locally spinning queued locks (Mellor-Crummey & Scott '92). This
 *    algorithm is enabled by defining QUEUED_LOCKS.
 */

/** Acquire database level exclusive lock
 *   Blocks until lock is acquired.
 *   If USE_LOCK_TIMEOUT is defined, may return without locking
 */

#ifdef USE_LOCK_TIMEOUT
gint wg_db_wlock(void * db, gint timeout) {
#else
gint wg_db_wlock(void * db) {
#endif
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif

#ifndef QUEUED_LOCKS
  volatile gint *gl;
#else
  gint lock, prev;
  lock_queue_node *lockp;
  db_memsegment_header* dbh;
#endif

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in wg_db_wlock");
    return 0;
  }
#endif  
  
#ifndef QUEUED_LOCKS
  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* First attempt at getting the lock without spinning */
  if(compare_and_swap(gl, 0, WAFLAG))
    return 1;

#ifdef _WIN32
  ts = SLEEP_MSEC;
#else
  ts.tv_sec = 0;
  ts.tv_nsec = SLEEP_NSEC;
#ifdef USE_LOCK_TIMEOUT
  if(timeout > INT_MAX/1000000) /* hack: primitive overflow protection */
    timeout = INT_MAX;
  else
    timeout *= 1000000;
#endif
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
      _MM_PAUSE
      if(!(*gl) && compare_and_swap(gl, 0, WAFLAG))
        return 1;
    }
    
    /* Check if we would time out during next sleep. Note that
     * this is not a real time measurement.
     */
#ifdef USE_LOCK_TIMEOUT
#ifdef _WIN32
    timeout -= ts;
#else
    timeout -= ts.tv_nsec;
#endif
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

#else /* QUEUED_LOCKS */
  lock = alloc_lock(db);
  if(!lock) {
    show_lock_error(db, "Failed to allocate lock");
    return 0;
  }

  dbh = dbmemsegh(db);
  lockp = (lock_queue_node *) offsettoptr(db, lock);

  lockp->class = LOCKQ_WRITE;
  lockp->next = 0;
  lockp->state = 1; /* blocked, no successor */

  /* Put ourselves at the end of queue and check
   * if there is a predecessor node.
   */
  prev = fetch_and_store(&(dbh->locks.tail), lock);

  if(!prev) {
    /* No other locks in queue (note that this does not
     * explicitly mean there are no active readers. For
     * that we examine reader_count).
     */
    dbh->locks.next_writer = lock;
    if(!dbh->locks.reader_count &&\
      fetch_and_store(&(dbh->locks.next_writer), 0) == lock) {
      /* No readers, we're still the next writer */
      /* lockp->state &= ~1; */
      atomic_and(&(lockp->state), ~1); /* not blocked */
    }
  }
  else {
    lock_queue_node *prevp = (lock_queue_node *) offsettoptr(db, prev);

    /* There is something ahead of us in the queue, by
     * definition we must wait until all predecessors complete.
     * The unblocking will be done by either a lone writer
     * directly before us, or a random reader that manages to decrement
     * the reader count to 0 upon completion.
     */
      /* prevp->state |= LOCKQ_WRITE; */
     atomic_or(&(prevp->state), LOCKQ_WRITE);
     prevp->next = lock;
  }

  if(lockp->state & 1) {
    /* Spin-wait */
#ifdef _WIN32
    ts = SLEEP_MSEC;
#else
    ts.tv_sec = 0;
    ts.tv_nsec = SLEEP_NSEC;
#endif

    for(;;) {
      for(i=0; i<SPIN_COUNT; i++) {
        _MM_PAUSE
        if(!(lockp->state & 1)) return lock;
      }

#ifdef _WIN32
      Sleep(ts);
      ts += SLEEP_MSEC;
#else
      nanosleep(&ts, NULL);
      ts.tv_nsec += SLEEP_NSEC;
#endif
    }
  }

  return lock;
#endif /* QUEUED_LOCKS */
  return 0; /* dummy */
}

/** Release database level exclusive lock
 */

gint wg_db_wulock(void * db, gint lock) {

#ifndef QUEUED_LOCKS
  volatile gint *gl;
#else
  lock_queue_node *lockp;
  db_memsegment_header* dbh;
#endif
  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in wg_db_wulock");
    return 0;
  }
#endif  
  
#ifndef QUEUED_LOCKS
  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* Clear the writer active flag */
  atomic_and(gl, ~(WAFLAG));

#else /* QUEUED_LOCKS */
  dbh = dbmemsegh(db);
  lockp = (lock_queue_node *) offsettoptr(db, lock);

  /* Check for the successor. If we're the last node, reset
   * the queue completely (see comments in wg_db_rulock() for
   * a more detailed explanation of why this can be done).
   */
  if(lockp->next || !compare_and_swap(&(dbh->locks.tail), lock, 0)) {
    lock_queue_node *nextp;
    while(!lockp->next); /* Wait until the successor has updated
                          * this record. */
    nextp = (lock_queue_node *) offsettoptr(db, lockp->next);
    if(nextp->class & LOCKQ_READ)
      atomic_increment(&(dbh->locks.reader_count), 1);

    /* nextp->state &= ~1; */
    atomic_and(&(nextp->state), ~1); /* unblock successor */
  }

  free_lock(db, lock);
#endif /* QUEUED_LOCKS */

  return 1;
}

/** Acquire database level shared lock
 *   Increments reader count, blocks until there are no active
 *   writers.
 *   If USE_LOCK_TIMEOUT is defined, may return without locking.
 */

#ifdef USE_LOCK_TIMEOUT
gint wg_db_rlock(void * db, gint timeout) {
#else
gint wg_db_rlock(void * db) {
#endif
  int i;
#ifdef _WIN32
  int ts;
#else
  struct timespec ts;
#endif

#ifndef QUEUED_LOCKS
  volatile gint *gl;
#else
  gint lock, prev;
  lock_queue_node *lockp;
  db_memsegment_header* dbh;
#endif

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in wg_db_rlock");
    return 0;
  }
#endif  
  
#ifndef QUEUED_LOCKS
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
#ifdef USE_LOCK_TIMEOUT
  if(timeout > INT_MAX/1000000)
    timeout = INT_MAX;
  else
    timeout *= 1000000;
#endif
#endif

  /* Spin loop */
  for(;;) {
    for(i=0; i<SPIN_COUNT; i++) {
      _MM_PAUSE
      if(!((*gl) & WAFLAG)) return 1;
    }

    /* Check for timeout. */
#ifdef USE_LOCK_TIMEOUT
#ifdef _WIN32
    timeout -= ts;
#else
    timeout -= ts.tv_nsec;
#endif
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

#else /* QUEUED_LOCKS */
  lock = alloc_lock(db);
  if(!lock) {
    show_lock_error(db, "Failed to allocate lock");
    return 0;
  }

  dbh = dbmemsegh(db);
  lockp = (lock_queue_node *) offsettoptr(db, lock);

  lockp->class = LOCKQ_READ;
  lockp->next = 0;
  lockp->state = 1; /* blocked, no successor */

  /* Put ourselves at the end of queue and check
   * if there is a predecessor node.
   */
  prev = fetch_and_store(&(dbh->locks.tail), lock);

  if(!prev) {
    /* No other locks, increment reader count and return */
    atomic_increment(&(dbh->locks.reader_count), 1);
    /* lockp->state &= ~1; */
    atomic_and(&(lockp->state), ~1); /* not blocked */
  }
  else {
    lock_queue_node *prevp = (lock_queue_node *) offsettoptr(db, prev);

    /* There is a previous lock. Depending on it's type
     * and state we may need to spin-wait (this happens if
     * there is an active writer somewhere).
     */
    if(prevp->class & LOCKQ_WRITE ||\
      compare_and_swap(&(prevp->state), 1, 1|(LOCKQ_READ))) {

      /* Predecessor is a writer or a blocked reader. Spin-wait;
       * the predecessor will unblock us and increment the reader count */
      prevp->next = lock;
      if(lockp->state & 1) {
        /* Spin-wait */
#ifdef _WIN32
        ts = SLEEP_MSEC;
#else
        ts.tv_sec = 0;
        ts.tv_nsec = SLEEP_NSEC;
#endif

        for(;;) {
          for(i=0; i<SPIN_COUNT; i++) {
            _MM_PAUSE
            if(!(lockp->state & 1)) goto rd_lock_cont;
          }

#ifdef _WIN32
          Sleep(ts);
          ts += SLEEP_MSEC;
#else
          nanosleep(&ts, NULL);
          ts.tv_nsec += SLEEP_NSEC;
#endif
        }
      }
    }
    else {
      /* Predecessor is a reader, we can continue */
      atomic_increment(&(dbh->locks.reader_count), 1);
      prevp->next = lock;
      /* lockp->state &= ~1; */
      atomic_and(&(lockp->state), ~1); /* not blocked */
    }
  }

rd_lock_cont:
  /* Now check if this lock has a successor. If it's a reader
   * we know it's currently blocked since this lock was
   * blocked too up to now. So we need to unblock the successor.
   */
  if(lockp->state & LOCKQ_READ) {
    lock_queue_node *nextp;

    while(!lockp->next); /* wait until structure is updated */
    atomic_increment(&(dbh->locks.reader_count), 1);
    nextp = (lock_queue_node *) offsettoptr(db, lockp->next);
    /* nextp->state &= ~1; */
    atomic_and(&(nextp->state), ~1); /* unblock successor */
  }
  
  return lock;
#endif /* QUEUED_LOCKS */
  return 0; /* dummy */
}

/** Release database level shared lock
 */

gint wg_db_rulock(void * db, gint lock) {

#ifndef QUEUED_LOCKS
  volatile gint *gl;
#else
  lock_queue_node *lockp;
  db_memsegment_header* dbh;
#endif
  
#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in wg_db_rulock");
    return 0;
  }
#endif  
  
#ifndef QUEUED_LOCKS
  gl = (gint *) offsettoptr(db, dbmemsegh(db)->locks.global_lock);

  /* Decrement reader count */
  fetch_and_add(gl, -RC_INCR);

#else /* QUEUED_LOCKS */
  dbh = dbmemsegh(db);
  lockp = (lock_queue_node *) offsettoptr(db, lock);

  /* Check if the successor is a waiting writer (predecessors
   * cannot be waiting readers with fair queueing).
   *
   * If there are active readers, their presence is also
   * known via reader_count. This is why we can set the value
   * of tail to none (0) if our reader is the last one in queue.
   * This is important from memory management point of view - 
   * basically the contents of the rest of the reader locks is
   * now irrelevant for future locks and we can "cut" the queue.
   *
   * The other important point is that we are interested in cases where
   * the CAS operation *fails*, indicating that a successor has appeared.
   */
  if(lockp->next || !compare_and_swap(&(dbh->locks.tail), lock, 0)) {

    while(!lockp->next); /* Wait until the successor has updated
                          * this record, meaning no further locks
                          * are interested in reading our state. This
                          * record can now be freed without checking
                          * reference count.
                          */
    if(lockp->state & LOCKQ_WRITE)
      dbh->locks.next_writer = lockp->next;
  }
  if(fetch_and_add(&(dbh->locks.reader_count), -1) == 1) {

    /* No more readers. If there is a writer in line, unblock it */
    gint w = fetch_and_store(&(dbh->locks.next_writer), 0);
    if(w) {
        lock_queue_node *wp = (lock_queue_node *) offsettoptr(db, w);
        /* wp->state &= ~1; */
        atomic_and(&(wp->state), ~1); /* unblock writer */
    }
  }
  free_lock(db, lock);
#endif /* QUEUED_LOCKS */

  return 1;
}

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

/** Initialize locking subsystem.
 *   Not parallel-safe, so should be run during database init.
 */
gint wg_init_locks(void * db) {
#ifdef QUEUED_LOCKS
  gint i, chunk_wall;
  lock_queue_node *tmp = NULL;
#endif
  db_memsegment_header* dbh;

#ifdef CHECK
  if (!dbcheck(db)) {
    show_lock_error(db, "Invalid database pointer in wg_init_locks");
    return -1;
  }
#endif  
  dbh = dbmemsegh(db);

#ifdef QUEUED_LOCKS
  chunk_wall = dbh->locks.storage + dbh->locks.max_nodes*SYN_VAR_PADDING;

  for(i=dbh->locks.storage; i<chunk_wall; ) {
    tmp = (lock_queue_node *) offsettoptr(db, i);
    tmp->refcount = 1;
    i+=SYN_VAR_PADDING;
    tmp->next_cell = i; /* offset of next cell */
  }
  tmp->next_cell=0; /* last node */

  /* top of the stack points to first cell in chunk */
  dbh->locks.freelist = dbh->locks.storage;

  /* reset the state */
  dbh->locks.tail = 0; /* 0 is considered invalid offset==>no value */
  dbh->locks.reader_count = 0;
  dbh->locks.next_writer = 0; /* 0==>no value */
#else
  dbstore(db, dbh->locks.global_lock, 0);
#endif
  return 0;
}

#ifdef QUEUED_LOCKS

/** Allocate memory cell for a lock.
 *   Used internally only, so we assume the passed db pointer
 *   is already validated.
 *
 *   Returns offset to allocated cell.
 */

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

#endif /* QUEUED_LOCKS */


/* ------------ error handling ---------------- */

static gint show_lock_error(void *db, char *errmsg) {
  fprintf(stderr,"wg locking error: %s.\n", errmsg);
  return -1;
}

#ifdef __cplusplus
}
#endif
