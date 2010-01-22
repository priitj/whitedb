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

 /** @file dbcompare.h
 * Public headers for data comparison functions.
 */

#ifndef __defined_dbcompare_h
#define __defined_dbcompare_h
#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* For gint data type */
#include "dbdata.h"

/* ==== Public macros ==== */

#define WG_EQUAL 0
#define WG_GREATER 1 
#define WG_LESSTHAN -1

/* wrapper macro for compare(), if encoded values are
 * equal they will also decode to an equal value and so
 * we can avoid calling the function.
 */
#define WG_COMPARE(d,a,b) (a==b ? WG_EQUAL : wg_compare(d,a,b))

/* ==== Protos ==== */

gint wg_compare(void *db, gint a, gint b);

#endif /* __defined_dbcompare_h */
