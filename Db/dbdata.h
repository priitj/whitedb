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

 /** @file dbdata.h
 * Datatype encoding defs and public headers for actual data handling procedures.
 */

#ifndef __defined_dbdata_h
#define __defined_dbdata_h

#include "../config.h"
#include "dballoc.h"

#define CHECK
//#undef CHECK
#define RECORD_HEADER_GINTS 1

// recognising gint types as gb types: bits, shifts, masks

/*
special value null (unassigned)         integer 0

Pointers to word-len ints end with            ?01  = not eq
Pointers to data records end with             000  = not eq
Pointers to long string records end with      100  = eq
Pointers to doubleword-len doubles end with   010  = not eq
Pointers to 32byte string records end with    110  = not eq


Immediate integers end with                   011  = is eq

(Other immediates                             111 (continued below))
Immediate vars end with                      0111  
Immediate short floats                  ???0 1111  = is eq
Immediate chars                         0001 1111  = is eq
Immediate dates                         0011 1111  = is eq
Immediate times                         0101 1111  = is eq
Immediate strings                       0111 1111  = is eq
Immediate anon constants                1001 1111  = is eq
*/

#define SMALLINTBITS  0x2       ///< int ends with       010
#define SMALLINTSHFT  3
#define SMALLINTMASK  0x7

#define fits_smallint(i)   ((((i)<<SMALLINTSHFT)>>SMALLINTSHFT)==i)
#define encode_smallint(i) (((i)<<SMALLINTSHFT)|SMALLINTBITS)
#define decode_smallint(i) ((i)>>SMALLINTSHFT)

#define FULLINTBITS  0x1      ///< full int ptr ends with       01
#define FULLINTMASK  0x3

#define encode_fullint_offset(i) ((i)|FULLINTBITS)
#define decode_fullint_offset(i) ((i) & ~FULLINTMASK)

#define FULLDOUBLEBITS  0x2      ///< full double ptr ends with       010
#define FULLDOUBLEMASK  0x7

#define encode_fulldouble_offset(i) ((i)|FULLDOUBLEBITS)
#define decode_fulldouble_offset(i) ((i) & ~FULLDOUBLEMASK)

#define TINYSTRBITS  0x7f       ///< tiny str ends with 0111 1111

#define SHORTSTRBITS  0x6      ///< short str ptr ends with  110
#define SHORTSTRMASK  0x7

#define encode_shortstr_offset(i) ((i)|SHORTSTRBITS)
#define decode_shortstr_offset(i) ((i) & ~SHORTSTRMASK)




/*
#define GBPTRBITS  0x0       ///< pointer ends with   ?000
#define GBPTRSHFT  0
#define GBPTRMASK  0x7
#define GBINTBITS  0x2       ///< int ends with       0010
#define GBINTSHFT  4
#define GBINTMASK  0xF
#define GBVARBITS  0x6       ///< var ends with       0110
#define GBVARSHFT  4
#define GBVARMASK  0xF     
#define GBVARNRSHFT    8 
#define GBVARDECOMASK  0xF0
#define GBVARDECOSHFT  4
#define GBTABBITS  0xA       ///< tab ends with       1010
#define GBTABSHFT  4
#define GBTABMASK  0xF 
#define GBOTHBITS  0xE       ///< others end with     1110
#define GBOTHSHFT  4
#define GBOTHMASK  0xF
*/


/* --------- error handling ------------ */

#define recordcheck(db,record,fieldnr,opname) { \
  if (!dbcheck(db)) {\
    show_data_error_str(db,"wrong database pointer given to ",opname);\
    return -1;\
  }\
  if (fieldnr<0 ||\
     (dbfetch(db,ptrtooffset(db,record)))<=(((gint)fieldnr+RECORD_HEADER_GINTS)*sizeof(gint)) ) {\
    show_data_error_str(db,"wrong field number given to ",opname);\
    return -2;\
  }\
}

/* ==== Protos ==== */

void* wg_create_record(void* db, int length);
void* wg_get_first_record(void* db);
void* wg_get_next_record(void* db, void* record);

int wg_set_int_field(void* db, void* record, int fieldnr, int data);
int wg_set_double_field(void* db, void* record, int fieldnr, double data);
int wg_set_str_field(void* db, void* record, int fieldnr, char* str);

gint show_data_error(void* db, char* errmsg);
gint show_data_error_nr(void* db, char* errmsg, gint nr);
gint show_data_error_double(void* db, char* errmsg, double nr);
gint show_data_error_str(void* db, char* errmsg, char* str);

#endif
