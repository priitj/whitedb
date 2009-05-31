/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
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

 /** @file dbdump.h
 * Public headers for memory dumping to the disk.
 */

#ifndef __defined_dbdump_h
#define __defined_dbdump_h
#include "../config.h"

/* ====== data structures ======== */


/* ==== Protos ==== */

gint wg_dump(void * db,char fileName[]); /* dump shared memory database to the disk */
gint wg_import_dump(void * db,char fileName[]); /* import database from the disk */

#endif /* __defined_dbdump_h */