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

#define WG_TEST_COMMON  0x01
#define WG_TEST_INDEX   0x02
#define WG_TEST_QUERY   0x04
#define WG_TEST_LOG     0x08
#define WG_TEST_QUICK   (WG_TEST_COMMON|WG_TEST_LOG)
#define WG_TEST_FULL    (WG_TEST_QUICK|WG_TEST_INDEX|WG_TEST_QUERY)

/* ==== Protos ==== */

int wg_run_tests(int tests, int printlevel);
gint wg_check_db(void* db);
gint wg_check_datatype_writeread(void* db, int printlevel);
gint wg_check_backlinking(void* db, int printlevel);
gint wg_check_parse_encode(void* db, int printlevel);
gint wg_check_compare(void* db, int printlevel);
gint wg_check_query_param(void* db, int printlevel);
gint wg_check_strhash(void* db, int printlevel);
gint wg_test_index1(void *db, int magnitude, int printlevel);
gint wg_test_index2(void *db, int printlevel);
gint wg_check_childdb(void* db, int printlevel);
gint wg_test_query(void *db, int magnitude, int printlevel);
gint wg_check_log(void* db, int printlevel);

void wg_show_db_memsegment_header(void* db);
void wg_show_db_area_header(void* db, void* area_header);
void wg_show_bucket_freeobjects(void* db, gint freelist);
void wg_show_strhash(void* db);

gint wg_count_freelist(void* db, gint freelist); 

int wg_genintdata_asc(void *db, int databasesize, int recordsize);
int wg_genintdata_desc(void *db, int databasesize, int recordsize);
int wg_genintdata_mix(void *db, int databasesize, int recordsize);

void wg_debug_print_value(void *db, gint data);
void wg_show_strhash(void* db);

/* ------- testing ------------ */

#endif
