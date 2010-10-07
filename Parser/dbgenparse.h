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

 /** @file dbgenparse.h
 *  Top level/generic headers and defs for parsers
 *
 */

#ifndef __defined_dbgenparse_h
#define __defined_dbgenparse_h

#include "../Db/dbdata.h"
#include "../Db/dbmpool.h"
#include "dbparse.h"

#define parseprintf(...) 

#define MKWGPAIR(pp,x,y) (wg_mkpair(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,x,y))

#define MKWGINT(pp,x)   (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_INTTYPE,x,NULL))
#define MKWGFLOAT(pp,x) (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_DOUBLETYPE,x,NULL))
#define MKWGDATE(pp,x)  (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_DATETYPE,x,NULL))
#define MKWGTIME(pp,x)  (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_TIMETYPE,x,NULL))

#define MKWGSTRING(pp,x)  (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_STRTYPE,x,NULL))
#define MKWGID(pp,x)      (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_URITYPE,x,NULL))
#define MKWGCONST(pp,x)   (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_ANONCONSTTYPE,x,NULL))
#define MKWGVAR(pp,x)     (wg_mkatom(((parse_parm*)pp)->db,((parse_parm*)pp)->mpool,WG_VARTYPE,x,NULL))
#define MKWGNIL NULL


// ---- reeentrant ----

typedef struct parse_parm_s {
  void  *yyscanner; // has to be present
  char  *buf;       // for parse from str case
  int   pos;        // for parse from str case
  int   length;     // for parse from str case
  char* filename;   // for err handling
  void* result;     // parser result  
  void* db;         // database pointer
  void* mpool;      // mpool pointer
  char* foo;        // test
} parse_parm;

#define YYSTYPE         char*
#define YY_EXTRA_TYPE   parse_parm *



#endif
