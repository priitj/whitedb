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

 /** @file dbmpool.h
 * Public headers for memory pool utilities.
 */

#ifndef DEFINED_DBMPOOL_H
#define DEFINED_DBMPOOL_H

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif


/* ====== data structures ======== */


/* ==== Protos ==== */

void* wg_create_mpool(void* db, int bytes);             // call this to init pool with initial size bytes
void* wg_alloc_mpool(void* db, void* mpool, int bytes); // call each time you want to "malloc":
                                                        // automatically extends pool if no space left
void wg_free_mpool(void* db, void* mpool);              // remove the whole pool

int wg_ispair(void* db, void* ptr);
void* wg_mkpair(void* db, void* mpool, void* x, void* y);
void* wg_first(void* db, void* ptr);
void* wg_rest(void* db, void *ptr);

int wg_listtreecount(void* db, void *ptr);

int wg_isatom(void* db, void* ptr);
void* wg_mkatom(void* db, void* mpool, int type, char* str1, char* str2);
int wg_atomtype(void* db, void* ptr);
char* wg_atomstr1(void* db, void* ptr);
char* wg_atomstr2(void* db, void* ptr);

void wg_mpool_print(void* db, void* ptr);


#endif /* DEFINED_DBMPOOL_H */
