/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2009, 2013, 2014
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

 /** @file dblock.h
 * Public headers for concurrent access routines.
 */

#ifndef DEFINED_DBLOCK_H
#define DEFINED_DBLOCK_H

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* ==== Public macros ==== */

/* XXX: move to configure.in / config-xxx.h */
#define USE_LOCK_TIMEOUT 1
#define DEFAULT_LOCK_TIMEOUT 2000 /* in ms */

/* Lock protocol */
#define RPSPIN 1
#define WPSPIN 2
#define TFQUEUE 3

/* ====== data structures ======== */

#if (LOCK_PROTO==TFQUEUE)

/* Queue nodes are stored locally in allocated cells.
 * The size of this structure can never exceed SYN_VAR_PADDING
 * defined in dballoc.h.
 */
typedef struct {
  /* XXX: do we need separate links for stack? Or even, does
   * it break correctness? */
  gint next_cell; /* freelist chain (db offset) */

  gint class; /* LOCKQ_READ, LOCKQ_WRITE */
  volatile gint waiting;  /* sync variable */
  volatile gint next; /* queue chain (db offset) */
  volatile gint prev; /* queue chain */
} lock_queue_node;

#endif

/* ==== Protos ==== */

/* API functions (copied in dbapi.h) */

gint wg_start_write(void * dbase);          /* start write transaction */
gint wg_end_write(void * dbase, gint lock); /* end write transaction */
gint wg_start_read(void * dbase);           /* start read transaction */
gint wg_end_read(void * dbase, gint lock);  /* end read transaction */

/* WhiteDB internal functions */

gint wg_compare_and_swap(volatile gint *ptr, gint oldv, gint newv);
gint wg_init_locks(void * db); /* (re-) initialize locking subsystem */

#if (LOCK_PROTO==RPSPIN)

#ifdef USE_LOCK_TIMEOUT
gint db_rpspin_wlock(void * dbase, gint timeout);
#define db_wlock(d, t) db_rpspin_wlock(d, t)
#else
gint db_rpspin_wlock(void * dbase);             /* get DB level X lock */
#define db_wlock(d, t) db_rpspin_wlock(d)
#endif
gint db_rpspin_wulock(void * dbase);            /* release DB level X lock */
#define db_wulock(d, l) db_rpspin_wulock(d)
#ifdef USE_LOCK_TIMEOUT
gint db_rpspin_rlock(void * dbase, gint timeout);
#define db_rlock(d, t) db_rpspin_rlock(d, t)
#else
gint db_rpspin_rlock(void * dbase);             /* get DB level S lock */
#define db_rlock(d, t) db_rpspin_rlock(d)
#endif
gint db_rpspin_rulock(void * dbase);            /* release DB level S lock */
#define db_rulock(d, l) db_rpspin_rulock(d)

#elif (LOCK_PROTO==WPSPIN)

#ifdef USE_LOCK_TIMEOUT
gint db_wpspin_wlock(void * dbase, gint timeout);
#define db_wlock(d, t) db_wpspin_wlock(d, t)
#else
gint db_wpspin_wlock(void * dbase);             /* get DB level X lock */
#define db_wlock(d, t) db_wpspin_wlock(d)
#endif
gint db_wpspin_wulock(void * dbase);            /* release DB level X lock */
#define db_wulock(d, l) db_wpspin_wulock(d)
#ifdef USE_LOCK_TIMEOUT
gint db_wpspin_rlock(void * dbase, gint timeout);
#define db_rlock(d, t) db_wpspin_rlock(d, t)
#else
gint db_wpspin_rlock(void * dbase);             /* get DB level S lock */
#define db_rlock(d, t) db_wpspin_rlock(d)
#endif
gint db_wpspin_rulock(void * dbase);            /* release DB level S lock */
#define db_rulock(d, l) db_wpspin_rulock(d)

#elif (LOCK_PROTO==TFQUEUE)

#ifdef USE_LOCK_TIMEOUT
gint db_tfqueue_wlock(void * dbase, gint timeout);
#define db_wlock(d, t) db_tfqueue_wlock(d, t)
#else
gint db_tfqueue_wlock(void * dbase);             /* get DB level X lock */
#define db_wlock(d, t) db_tfqueue_wlock(d)
#endif
gint db_tfqueue_wulock(void * dbase, gint lock); /* release DB level X lock */
#define db_wulock(d, l) db_tfqueue_wulock(d, l)
#ifdef USE_LOCK_TIMEOUT
gint db_tfqueue_rlock(void * dbase, gint timeout);
#define db_rlock(d, t) db_tfqueue_rlock(d, t)
#else
gint db_tfqueue_rlock(void * dbase);             /* get DB level S lock */
#define db_rlock(d, t) db_tfqueue_rlock(d)
#endif
gint db_tfqueue_rulock(void * dbase, gint lock); /* release DB level S lock */
#define db_rulock(d, l) db_tfqueue_rulock(d, l)

#else /* undefined or invalid value, disable locking */

#define db_wlock(d, t) (1)
#define db_wulock(d, l) (1)
#define db_rlock(d, t) (1)
#define db_rulock(d, l) (1)

#endif /* LOCK_PROTO */

#endif /* DEFINED_DBLOCK_H */
