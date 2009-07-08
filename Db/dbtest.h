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

void show_db_memsegment_header(void* db);
void show_db_area_header(void* db, void* area_header);
void show_bucket_freeobjects(void* db, gint freelist);
gint count_freelist(void* db, gint freelist); 

gint check_datatype_writeread(void* db);

gint check_db(void* db);
gint check_varlen_area(void* db, void* area_header);
gint check_varlen_area_freelist(void* db, void* area_header);
gint check_bucket_freeobjects(void* db, void* area_header, gint bucketindex);
gint check_varlen_area_markers(void* db, void* area_header);
gint check_varlen_area_dv(void* db, void* area_header);
gint check_object_in_areabounds(void*db,void* area_header,gint offset,gint size);
gint check_varlen_area_scan(void* db, void* area_header);
gint check_varlen_object_infreelist(void* db, void* area_header, gint offset, gint isfree);

/* ------- testing ------------ */

#endif
