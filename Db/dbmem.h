/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
*
* Contact: tanel.tammet@gmail.com                 
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

 /** @file dbmem.h
 * Public headers for database memory handling.
 */

#ifndef __defined_dbmem_h
#define __defined_dbmem_h

#include "../config.h"

#define DEFAULT_MEMDBASE_KEY 1000
//#define DEFAULT_MEMDBASE_SIZE 1000000  // 1 meg
//#define DEFAULT_MEMDBASE_SIZE 800000000  // 800 meg
#define DEFAULT_MEMDBASE_SIZE 2000000000

#define MAX_FILENAME_SIZE 100

/* ====== data structures ======== */


/* ==== Protos ==== */

void* wg_attach_database(char* dbasename, int size); // returns a pointer to the database, NULL if failure
int wg_detach_database(void* dbase); // detaches a database: returns 0 if OK
int wg_delete_database(char* dbasename); // deletes a database: returns 0 if OK


void* link_shared_memory(int key);
void* create_shared_memory(int key,int size);
int free_shared_memory(int key);

int detach_shared_memory(void* shmptr);


#endif
