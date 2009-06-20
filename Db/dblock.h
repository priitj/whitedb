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


/* ==== Protos ==== */

gint wg_start_write(void * dbase);  /* start write transaction */
gint wg_end_write(void * dbase);    /* end write transaction */
gint wg_start_read(void * dbase);   /* start read transaction */
gint wg_end_read(void * dbase); /* end read transaction */


#endif /* __defined_dblock_h */
