/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
* Copyright (c) Priit Järv 2009,2010,2012,2013,2014
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

#ifndef DEFINED_DBTEST_H
#define DEFINED_DBTEST_H

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
void wg_show_db_memsegment_header(void* db);

int wg_genintdata_asc(void *db, int databasesize, int recordsize);
int wg_genintdata_desc(void *db, int databasesize, int recordsize);
int wg_genintdata_mix(void *db, int databasesize, int recordsize);

void wg_debug_print_value(void *db, gint data);

/* ------- testing ------------ */

#endif /* DEFINED_DBTEST_H */
