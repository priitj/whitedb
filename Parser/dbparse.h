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
#include "../Reasoner/mem.h"
#include "../Reasoner/glb.h" 

#define OTTER_DECIMAL_SEPARATOR '.'

int wr_import_otter_file(glb* g, char* filename, char* strasfile, cvec clvec);
//int wg_import_otter_file(void* db, char* filename, int printlevel);
int wr_import_prolog_file(glb* g, char* filename, char* strasfile, cvec clvec);

void* wr_parse_clauselist(glb* g,void* mpool,cvec clvec,void* clauselist);
void* wr_parse_atom(glb* g,void* mpool,void* term, int isneg, int issimple, char** vardata);
void* wr_parse_term(glb* g,void* mpool,void* term, char** vardata);
gint wr_parse_primitive(glb* g,void* mpool,void* term, char** vardata);

gint wr_parse_and_encode_otter_prim(glb* g, char *buf);
gint wr_parse_and_encode_otter_uri(glb* g, char *buf);

gint wr_print_parseres(glb* g, gint x);


#endif
