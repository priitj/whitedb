/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
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

 /** @file dbutil.h
 * Public headers for miscellaneous functions.
 */

#ifndef __defined_dbutil_h
#define __defined_dbutil_h
#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* ==== Protos ==== */

/* API functions (copied in dbapi.h) */
void wg_print_db(void *db);
void wg_print_record(void *db, gint* rec);
void wg_snprint_value(void *db, gint enc, char *buf, int buflen);

#endif /* __defined_dbutil_h */
