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

 /** @file dbapi.h
 *
 * Wg database api for public use.
 *
 */

#ifndef __defined_dbapi_h
#define __defined_dbapi_h

/* ---  built-in data type numbers ----- */

/* the built-in data types are primarily for api purposes.
   internally, some of these types like int, str etc have several 
   different ways to encode along with different bit masks 
*/   


#define WG_NULLTYPE 1
#define WG_RECORDTYPE 2
#define WG_INTTYPE 3
#define WG_DOUBLETYPE 4
#define WG_STRTYPE 5
#define WG_XMLLITERALTYPE 6
#define WG_URITYPE 7
#define WG_BLOBTYPE 8
#define WG_CHARTYPE 9
#define WG_DATETYPE 10
#define WG_TIMETYPE 11
#define WG_ANONCONSTTYPE 12
#define WG_VARTYPE 13

/* prototypes of wg database api functions 

*/

typedef int wg_int;

/* ------- attaching and detaching a database ----- */

void* wg_attach_database(char* dbasename, int size); // returns a pointer to the database, NULL if failure
int wg_detach_database(void* dbase); // detaches a database: returns 0 if OK
int wg_delete_database(char* dbasename); // deletes a database: returns 0 if OK

/* -------- creating and scanning records --------- */

void* wg_create_record(void* db, wg_int length); ///< returns NULL when error, ptr to rec otherwise
void* wg_delete_record(void* db, wg_int);  ///< returns NULL when error, any other otherwise

void* wg_get_first_record(void* db);              ///< returns NULL when error or no recs
void* wg_get_next_record(void* db, void* record); ///< returns NULL when error or no more recs

/* -------- setting and fetching record field values --------- */

wg_int wg_get_record_len(void* db, void* record); ///< returns negative int when error
wg_int* wg_get_record_dataarray(void* db, void* record); ///< pointer to record data array start

// following field setting functions return negative int when err, 0 when ok
wg_int wg_set_field(void* db, void* record, wg_int fieldnr, wg_int data); 

wg_int wg_set_int_field(void* db, void* record, wg_int fieldnr, wg_int data); 
wg_int wg_set_double_field(void* db, void* record, wg_int fieldnr, double data);
wg_int wg_set_str_field(void* db, void* record, wg_int fieldnr, char* data);

wg_int wg_get_field(void* db, void* record, wg_int fieldnr);      // returns 0 when error
wg_int wg_get_field_type(void* db, void* record, wg_int fieldnr); // returns 0 when error


/* ---------- general operations on encoded data -------- */

wg_int wg_get_encoded_type(void* db, wg_int data);
wg_int wg_free_encoded(void* db, wg_int data);

/* -------- encoding and decoding data: records contain encoded data only ---------- */

// int

wg_int wg_encode_int(void* db, wg_int data);
wg_int wg_decode_int(void* db, wg_int data);

// double

wg_int wg_encode_double(void* db, double data);
double wg_decode_double(void* db, wg_int data);

// str (standard C string: zero-terminated array of chars)
// along with optional attached language indicator str

wg_int wg_encode_str(void* db, char* str, char* lang); ///< let lang==NULL if not used

char* wg_decode_str(void* db, wg_int data);
char* wg_decode_str_lang(void* db, wg_int data);

wg_int wg_decode_str_len(void* db, wg_int data); 
wg_int wg_decode_str_lang_len(void* db, wg_int data); 
wg_int wg_decode_str_copy(void* db, wg_int data, char* strbuf, wg_int buflen);
wg_int wg_decode_str_lang_copy(void* db, wg_int data, char* langbuf, wg_int buflen);                         

// xmlliteral (standard C string: zero-terminated array of chars)
// along with obligatory attached xsd:type str

wg_int wg_encode_xmlliteral(void* db, char* str, char* xsdtype);
char* wg_decode_xmlliteral_copy(void* db, wg_int data);   
char* wg_decode_xmlliteral_xsdtype_copy(void* db, wg_int data); 

wg_int wg_decode_xmlliteral_len(void* db, wg_int data);
wg_int wg_decode_xmlliteral_xsdtype_len(void* db, wg_int data);
wg_int wg_decode_xmlliteral(void* db, wg_int data, char* strbuf, wg_int buflen);                           
wg_int wg_decode_xmlliteral_xsdtype(void* db, wg_int data, char* strbuf, wg_int buflen);                                                 

// uri (standard C string: zero-terminated array of chars)
// along with an optional namespace str

wg_int wg_encode_uri(void* db, char* str, char* nspace); ///< let nspace==NULL if not used
char* wg_decode_uri_copy(void* db, wg_int data);
char* wg_decode_uri_namespace_copy(void* db, wg_int data);

wg_int wg_decode_uri_len(void* db, wg_int data); 
wg_int wg_decode_uri_namespace_len(void* db, wg_int data); 
wg_int wg_decode_uri(void* db, wg_int data, char* strbuf, wg_int buflen);
wg_int wg_decode_uri_namespace(void* db, wg_int data, char* nspacebuf, wg_int buflen);   

// blob (binary large object, i.e. any kind of data)
// along with an obligatory length in bytes
                                
wg_int wg_encode_blob(void* db, char* blob, wg_int bloblen);
wg_int wg_decode_blob_len(void* db, wg_int data);
wg_int wg_decode_blob(void* db, wg_int data, char* blobbuf, wg_int buflen);                                
char* wg_decode_blob_copy(void* db, wg_int data);

/// ptr to record

wg_int wg_encode_record(void* db, void* data);
void* wg_decode_record(void* db, wg_int data);

/// char

wg_int wg_encode_char(void* db, char data);
char wg_decode_char(void* db, wg_int data); 

/* ---------- concurrency support  ---------- */

wg_int wg_start_write(void * dbase);          /* start write transaction */
wg_int wg_end_write(void * dbase, wg_int lock); /* end write transaction */
wg_int wg_start_read(void * dbase);           /* start read transaction */
wg_int wg_end_read(void * dbase, wg_int lock);  /* end read transaction */

#endif
