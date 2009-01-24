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
Immediate short floats                  0000 1111  = is eq
Immediate chars                         0001 1111  = is eq
Immediate dates                         0010 1111  = is eq
Immediate times                         0011 1111  = is eq
Immediate tiny strings                  0100 1111  = is eq
Immediate anon constants                0101 1111  = is eq
*/


/* --- encoding and decoding basic data ---- */

#define SMALLINTBITS    0x2       ///< int ends with       010
#define SMALLINTSHFT  3
#define SMALLINTMASK  0x7

#define fits_smallint(i)   ((((i)<<SMALLINTSHFT)>>SMALLINTSHFT)==i)
#define encode_smallint(i) (((i)<<SMALLINTSHFT)|SMALLINTBITS)
#define decode_smallint(i) ((i)>>SMALLINTSHFT)

#define FULLINTBITS  0x1      ///< full int ptr ends with       01
#define FULLINTBITSV0  0x1    ///< full int type as 3-bit nr version 0:  001
#define FULLINTBITSV1  0x5    ///< full int type as 3-bit nr version 1:  101
#define FULLINTMASK  0x3

#define encode_fullint_offset(i) ((i)|FULLINTBITS)
#define decode_fullint_offset(i) ((i) & ~FULLINTMASK)

#define DATARECBITS  0x0      ///< datarec ptr ends with       000
#define DATARECMASK  0x7

#define encode_datarec_offset(i) (i)
#define decode_datarec_offset(i) (i)

#define LONGSTRBITS  0x4      ///< longstr ptr ends with       100
#define LONGSTRMASK  0x7

#define encode_longstr_offset(i) ((i)|LONGSTRBITS)
#define decode_longstr_offset(i) ((i) & ~LONGSTRMASK)

#define FULLDOUBLEBITS  0x2      ///< full double ptr ends with       010
#define FULLDOUBLEMASK  0x7

#define encode_fulldouble_offset(i) ((i)|FULLDOUBLEBITS)
#define decode_fulldouble_offset(i) ((i) & ~FULLDOUBLEMASK)

#define SHORTSTRBITS  0x6      ///< short str ptr ends with  110
#define SHORTSTRMASK  0x7

#define encode_shortstr_offset(i) ((i)|SHORTSTRBITS)
#define decode_shortstr_offset(i) ((i) & ~SHORTSTRMASK)


/* --- encoding and decoding other data ---- */

#define VARMASK  0xf
#define VARSHFT  4
#define VARBITS  0x7       ///< var ends with 0111

#define encode_var(i) (((i)<<VARSHFT)|VARBITS)
#define decode_var(i) ((i)>>VARSHFT)

#define CHARMASK  0xff
#define CHARSHFT  8
#define CHARBITS  0x1f       ///< char ends with 0001 1111

#define encode_char(i) (((i)<<CHARSHFT)|CHARBITS)
#define decode_char(i) ((i)>>CHARSHFT)

#define DATEMASK  0xff
#define DATESHFT  8
#define DATEBITS  0x2f       ///< date ends with 0010 1111

#define encode_date(i) (((i)<<DATESHFT)|DATEBITS)
#define decode_date(i) ((i)>>DATESHFT)

#define TIMEMASK  0xff
#define TIMESHFT  8
#define TIMEBITS  0x3f       ///< time ends with 0011 1111

#define encode_time(i) (((i)<<TIMESHFT)|TIMEBITS)
#define decode_time(i) ((i)>>TIMESHFT)

#define TINYSTRMASK  0xff
#define TINYSTRSHFT  8
#define TINYSTRBITS  0x4f       ///< tiny str ends with 0100 1111

#define ANONCONSTMASK  0xff
#define ANONCONSTSHFT  8
#define ANONCONSTBITS  0x5f       ///< anon const ends with 0101 1111

#define encode_anonconst(i) (((i)<<ANONCONSTSHFT)|ANONCONSTBITS)
#define decode_anonconst(i) ((i)>>ANONCONSTSHFT)

/* --- recognizing data ---- */

#define NORMALPTRMASK 0x7  ///< all pointers except fullint
#define NONPTRBITS 0x3
#define LASTFOURBITSMASK 0xf
#define PRELASTFOURBITSMASK 0xf0
#define LASTBYTEMASK 0xff

#define isptr(i)        ((i) && (((i)&NONPTRBITS)!=NONPTRBITS))

#define isdatarec(i)    (((i)&DATARECMASK)==DATARECBITS)
#define isfullint(i)    (((i)&FULLINTMASK)==FULLINTBITS)
#define isfulldouble(i) (((i)&FULLDOUBLEMASK)==FULLDOUBLEBITS)
#define isshortstr(i)   (((i)&SHORTSTRMASK)==SHORTSTRBITS)

#define issmallint(i)   (((i)&SMALLINTMASK)==SMALLINTBITS)

#define isvar(i)   (((i)&VARMASK)==VARBITS)
#define ischar(i)   (((i)&CHARMASK)==CHARBITS)
#define isdate(i)   (((i)&DATEMASK)==DATEBITS)
#define istime(i)   (((i)&TIMEMASK)==TIMEBITS)
#define istinystr(i)   (((i)&TINYSTRMASK)==TINYSTRBITS)
#define isanonconst(i)   (((i)&ANONCONSTMASK)==ANONCONSTBITS)

/* ------ metainfo and special data items --------- */

#define datarec_size_bytes(i) (getusedobjectwantedbytes(i))
#define datarec_end_ptr(i) 


/* --------- record and longstr data object structure ---------- */


/* record data object

gint usage from start:

0:  encodes length in bytes. length is aligned to sizeof gint
1:  pointer to next sibling
2:  pointer to prev sibling or parent 
3:  data gints
...



---- conventional database rec ----------

car1:
id: 10
model: ford
licenceplate: 123LGH
owner: 20 (we will have ptr to rec 20)

car2:
id: 11
model: opel
licenceplate: 456RMH
owner: 20 (we will have ptr to rec 20)

person1:
parents: list of pointers to person1?
id: 20
fname: John
lname: Brown


---- xml node -------

<person fname="john" lname="brown">
  <owns>
    <car model="ford">
  </owns>
  <owns>  
    <car model="opel">
  </owns>
</person>

xml-corresponding rdf triplets

_10 model ford
_10 licenceplate 123LGH

_11 model opel
_11 licenceplate 456RMH

_20 fname john
_20 lname brown
_20 owns _10
_20 owns _11


(?x fname john) & (?x lname brown) & (?x owns ?y) & (?y model ford) => answer(?y)

solution: 

- locate from value index brown
- instantiate ?x with _20 
- scan _20 value occurrences with pred lname to find john
- scan _20 subject occurrences with pred owns to find _10
- scan _10 subject occurrences with pred model to find ford

----normal rdf triplets -----

_10 model ford
_10 licenceplate 123LGH
_10 owner _20

_11 model opel
_11 licenceplate 456RMH
_11 owner _20

_20 fname john
_20 lname brown


(?x fname john) & (?x lname brown) & (?y owner ?x) & (?y model ford) => answer(?y)

solution: 

- locate from value index brown
- instantiate ?x with _20 
- scan _20 value occurrences with pred lname to find john
- scan _20 value occurrences with pred owner to find _10
- scan _10 subject occurrences with pred model to find ford

--------- fromptr structure -------


fld 1 pts to either directly (single) occurrence or rec of occurrences:

single occ case:

- last bit zero indicates direct ptr to rec
- two previous bits indicate position in rec (0-3)

multiple (or far pos) case:

- last bit 1 indicates special pos list array ptr:

pos array:

recsize
position fld nr,
ptr to (single) rec or to corresp list of recs
position fld nr,
ptr to (single) rec or to corresp list o recs
...

where corresp list is made of pairs (list cells):

ptr to rec
ptr to next list cell

alternative:

ptr to rec
ptr to rec
ptr to rec
ptr to rec
ptr to next block


*/


/* longstr data object

gint usage from start:

0:  encodes data obj length in bytes. length is aligned to sizeof gint
1:  refcount / actual length subtraction
    - last 3 bits have to be subtracted from data obj length to get real length, incl trailing 0 i present
    - previous bits store refcount
2:  pointer to next longstr in the hash bucket, 0 if no following    
3:  xsd type str (offset) or lang str (offset), if 0 xsd:string / no lang
4:  namespace str (offset), if 0 no namespace
5:  actual bytes ....
...


*/

#define LONGSTR_REFCOUNT_POS 1


/* --------- error handling ------------ */

#define recordcheck(db,record,fieldnr,opname) { \
  if (!dbcheck(db)) {\
    show_data_error_str(db,"wrong database pointer given to ",opname);\
    return -1;\
  }\
  if (fieldnr<0 || getusedobjectwantedgintsnr(*((gint*)record))<=fieldnr+RECORD_HEADER_GINTS) {\
    show_data_error_str(db,"wrong field number given to ",opname);\
    return -2;\
  }\
}

/* ==== Protos ==== */

void free_field_data(void* db,gint fielddata);

gint show_data_error(void* db, char* errmsg);
gint show_data_error_nr(void* db, char* errmsg, gint nr);
gint show_data_error_double(void* db, char* errmsg, double nr);
gint show_data_error_str(void* db, char* errmsg, char* str);

#endif
