/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
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

 /** @file dbparse.h
 *  Top level/generic headers and defs for parsers
 *
 */

#ifndef __defined_dbparse_h
#define __defined_dbparse_h

#include "../Db/dballoc.h"


#define OTTER_DECIMAL_SEPARATOR '.'


int wg_import_otter_file(void* db, char* filename);
int wg_import_prolog_file(void* db, char* filename);

void* wg_parse_clauselist(void *db,void* mpool,void* clauselist); 
void* wg_parse_atom(void *db,void* mpool,void* term, int isneg, int issimple, char** vardata);
void* wg_parse_term(void *db,void* mpool,void* term, char** vardata);
gint wg_parse_primitive(void *db,void* mpool,void* term, char** vardata);

gint wg_parse_and_encode_otter_prim(void *db, char *buf);
gint wg_parse_and_encode_otter_uri(void *db, char *buf);

gint wg_print_parseres(void* db, gint x);

#endif
