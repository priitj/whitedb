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

 /** @file dbtest.h
 * Public headers for database testing procedures.
 */

#ifndef __defined_dbtest_h
#define __defined_dbtest_h

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* ====== general typedefs and macros ======= */


/* ==== Protos ==== */

int wg_run_tests(void* db, int printlevel);
gint wg_check_db(void* db);
gint wg_check_datatype_writeread(void* db, gint printlevel);
gint wg_check_strhash(void* db, gint printlevel);

void wg_show_db_memsegment_header(void* db);
void wg_show_db_area_header(void* db, void* area_header);
void wg_show_bucket_freeobjects(void* db, gint freelist);
void wg_show_strhash(void* db);

gint wg_count_freelist(void* db, gint freelist); 



/* ------- testing ------------ */

#endif
