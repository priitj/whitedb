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

 /** @file dbhash.h
 * Public headers for hash-related procedures.
 */

#ifndef __defined_dbhash_h
#define __defined_dbhash_h

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"

int wg_hash_typedstr(void* db, char* data, char* extrastr, gint type, gint length);
gint wg_find_strhash_bucket(void* db, char* data, char* extrastr, gint type, gint size, gint hashchain);
static int wg_right_strhash_bucket
            (void* db, gint longstr, char* cstr, char* cextrastr, gint ctype, gint cstrsize);

gint show_consistency_error(void* db, char* errmsg);
gint show_consistency_error_nr(void* db, char* errmsg, gint nr) ;
gint show_consistency_error_double(void* db, char* errmsg, double nr);
gint show_consistency_error_str(void* db, char* errmsg, char* str);

#endif
