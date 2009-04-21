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

#include "../config.h"
#include "dballoc.h"

/* ====== Private headers and defs ======== */

#include "dblock.h"

#define WAFLAG 0x1  /* writer active flag */
#define RC_INCR 0x2  /* increment step for reader count */

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

  gint *gl;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_start_write.\n");
    return 0;
  }
#endif  
  
  gl = ((db_memsegment_header *) db)->locks.global_lock;

  /* First attempt at getting the lock without spinning */
#if defined(__GNUC__)
  if(__sync_bool_compare_and_swap(gl, 0, WAFLAG))
#else
#error Atomic operations not implemented for this compiler
#endif
    return 1;

  /* Spin loop */
  for(;;) {
#if defined(__GNUC__)
    if(!(*gl) && __sync_bool_compare_and_swap(gl, 0, WAFLAG))
#else
#error Atomic operations not implemented for this compiler
#endif
      return 1;
    
    /* XXX: add Pentium 4  "pause" instruction */
    /* XXX: add sleeping to deschedule thread */
  }

  return 0; /* dummy */
}

/** End write transaction
 *   Current implementation: release database level exclusive lock
 */

gint wg_end_write(void * db) {

  gint *gl;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_end_write.\n");
    return 0;
  }
#endif  
  
  gl = ((db_memsegment_header *) db)->locks.global_lock;

  /* Clear the writer active flag */
#if defined(__GNUC__)
  __sync_fetch_and_and(gl, ~(WAFLAG));
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

  gint *gl;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_start_read.\n");
    return 0;
  }
#endif  
  
  gl = ((db_memsegment_header *) db)->locks.global_lock;

  /* Increment reader count atomically */
#if defined(__GNUC__)
  __sync_fetch_and_add(gl, RC_INCR);
#else
#error Atomic operations not implemented for this compiler
#endif

  /* Spin loop */
  for(;;) {
    if(!((*gl) & WAFLAG)) return 1;
    
    /* XXX: add Pentium 4 "pause" instruction */
    /* XXX: add sleeping to deschedule thread */
  }

  return 0; /* dummy */
}

/** End read transaction
 *   Current implementation: release database level shared lock
 */

gint wg_end_read(void * db) {

  gint *gl;
  
#ifdef CHECK
  if (!dbcheck(db)) {
    fprintf(stderr,"Invalid database pointer in wg_end_read.\n");
    return 0;
  }
#endif  
  
  gl = ((db_memsegment_header *) db)->locks.global_lock;

  /* Decrement reader count */
#if defined(__GNUC__)
  __sync_fetch_and_add(gl, -RC_INCR);
#else
#error Atomic operations not implemented for this compiler
#endif

  return 1;
}
