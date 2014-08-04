/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
*
* Contact: tanel.tammet@gmail.com
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

 /** @file dbmem.h
 * Public headers for database memory handling.
 */

#ifndef DEFINED_DBMEM_H
#define DEFINED_DBMEM_H

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#define DEFAULT_MEMDBASE_KEY 1000
//#define DEFAULT_MEMDBASE_SIZE 1000000  // 1 meg
#define DEFAULT_MEMDBASE_SIZE 10000000  // 10 meg
//#define DEFAULT_MEMDBASE_SIZE 800000000  // 800 meg
//#define DEFAULT_MEMDBASE_SIZE 2000000000

#define MAX_FILENAME_SIZE 100

/* ====== data structures ======== */


/* ==== Protos ==== */

void* wg_attach_database(char* dbasename, gint size); // returns a pointer to the database, NULL if failure
void* wg_attach_existing_database(char* dbasename); // like wg_attach_database, but does not create a new base
void* wg_attach_logged_database(char* dbasename, gint size); // like wg_attach_database, but activates journal logging on creation
void* wg_attach_database_mode(char* dbasename, gint size, int mode);  // like wg_attach_database, set shared segment permissions to "mode"
void* wg_attach_logged_database_mode(char* dbasename, gint size, int mode); // like above, activate journal logging

void* wg_attach_memsegment(char* dbasename, gint minsize,
                            gint size, int create, int logging, int mode); // same as wg_attach_database, does not check contents
int wg_detach_database(void* dbase); // detaches a database: returns 0 if OK
int wg_delete_database(char* dbasename); // deletes a database: returns 0 if OK
int wg_check_header_compat(db_memsegment_header *dbh); // check memory image compatibility
void wg_print_code_version(void);  // show libwgdb version info
void wg_print_header_version(db_memsegment_header *dbh, int verbose); // show version info from header

void* wg_attach_local_database(gint size);
void wg_delete_local_database(void* dbase);

int wg_memmode(void *db);
int wg_memowner(void *db);
int wg_memgroup(void *db);

#endif /* DEFINED_DBMEM_H */
