/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
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

 /** @file dbdump.h
 * Public headers for memory dumping to the disk.
 */

#ifndef DEFINED_DBDUMP_H
#define DEFINED_DBDUMP_H
#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* ====== data structures ======== */


/* ==== Protos ==== */

gint wg_dump(void * db,char fileName[]); /* dump shared memory database to the disk */
gint wg_dump_internal(void * db,char fileName[], int locking); /* handle the dump */
gint wg_import_dump(void * db,char fileName[]); /* import database from the disk */
gint wg_check_dump(void *db, char fileName[],
  gint *mixsize, gint *maxsize); /* check the dump file and get the db size */

#endif /* DEFINED_DBDUMP_H */
