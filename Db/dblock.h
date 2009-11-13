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

 /** @file dblock.h
 * Public headers for concurrent access routines.
 */

#ifndef __defined_dblock_h
#define __defined_dblock_h

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* ====== data structures ======== */

#ifdef QUEUED_LOCKS


/* Queue nodes are stored locally in allocated cells.
 * The size of this structure can never exceed SYN_VAR_PADDING
 * defined in dballoc.h.
 */
struct __lock_queue_node {
  volatile gint refcount;
  /* XXX: do we need separate links for stack? Or even, does
   * it break correctness? */
  gint next_cell; /* freelist chain (db offset) */

  gint class; /* LOCKQ_READ, LOCKQ_WRITE */
  gint next; /* queue chain (db offset) */
  volatile gint state; /* lsb - blocked, remainder of the
                    bits define the class of successor */
};

typedef struct __lock_queue_node lock_queue_node;

#endif

/* ==== Protos ==== */

gint wg_start_write(void * dbase);          /* start write transaction */
gint wg_end_write(void * dbase, gint lock); /* end write transaction */
gint wg_start_read(void * dbase);           /* start read transaction */
gint wg_end_read(void * dbase, gint lock);  /* end read transaction */

#ifdef QUEUED_LOCKS
gint init_lock_queue(void * db); /* initialize lock queue */
#endif

#endif /* __defined_dblock_h */
