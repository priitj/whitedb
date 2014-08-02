/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
* Copyright (c) Priit Järv 2009,2010,2011,2013,2014
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

 /** @file dbapi.h
 *
 * Wg database api for public use.
 *
 */

#ifndef DEFINED_DBAPI_H
#define DEFINED_DBAPI_H

/* For gint/wg_int types */
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

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
#define WG_FIXPOINTTYPE 10
#define WG_DATETYPE 11
#define WG_TIMETYPE 12
#define WG_ANONCONSTTYPE 13
#define WG_VARTYPE 14

/* Illegal encoded data indicator */
#define WG_ILLEGAL 0xff

/* Query "arglist" parameters */
#define WG_COND_EQUAL       0x0001      /** = */
#define WG_COND_NOT_EQUAL   0x0002      /** != */
#define WG_COND_LESSTHAN    0x0004      /** < */
#define WG_COND_GREATER     0x0008      /** > */
#define WG_COND_LTEQUAL     0x0010      /** <= */
#define WG_COND_GTEQUAL     0x0020      /** >= */

/* Query types. Python extension module uses the API and needs these. */
#define WG_QTYPE_TTREE      0x01
#define WG_QTYPE_HASH       0x02
#define WG_QTYPE_SCAN       0x04
#define WG_QTYPE_PREFETCH   0x80

/* Direct access to field */
#define RECORD_HEADER_GINTS 3
#define wg_field_addr(db,record,fieldnr) (((wg_int*)(record))+RECORD_HEADER_GINTS+(fieldnr))

/* WhiteDB data types */

typedef ptrdiff_t wg_int;
typedef size_t wg_uint;

/** Query argument list object */
typedef struct {
  wg_int column;      /** column (field) number this argument applies to */
  wg_int cond;        /** condition (equal, less than, etc) */
  wg_int value;       /** encoded value */
} wg_query_arg;

/** Query object */
typedef struct {
  wg_int qtype;         /** Query type (T-tree, hash, full scan, prefetch) */
  /* Argument list based query is the only one supported at the moment. */
  wg_query_arg *arglist;    /** check each row in result set against these */
  wg_int argc;              /** number of elements in arglist */
  wg_int column;            /** index on this column used */
  /* Fields for T-tree query (XXX: some may be re-usable for
   * other types as well) */
  wg_int curr_offset;
  wg_int end_offset;
  wg_int curr_slot;
  wg_int end_slot;
  wg_int direction;
  /* Fields for full scan */
  wg_int curr_record;       /** offset of the current record */
  /* Fields for prefetch; with/without mpool */
  void *mpool;              /** storage for row offsets */
  void *curr_page;          /** current page of results */
  wg_int curr_pidx;         /** current index on page */
  wg_uint res_count;        /** number of rows in results */
} wg_query;

/* prototypes of wg database api functions

*/

/* ------- attaching and detaching a database ----- */

void* wg_attach_database(char* dbasename, wg_int size); // returns a pointer to the database, NULL if failure
void* wg_attach_existing_database(char* dbasename); // like wg_attach_database, but does not create a new base
void* wg_attach_logged_database(char* dbasename, wg_int size); // like wg_attach_database, but activates journal logging on creation
void* wg_attach_database_mode(char* dbasename, wg_int size, int mode);  // like wg_attach_database, set shared segment permissions to "mode"
void* wg_attach_logged_database_mode(char* dbasename, wg_int size, int mode); // like above, activate journal logging
int wg_detach_database(void* dbase); // detaches a database: returns 0 if OK
int wg_delete_database(char* dbasename); // deletes a database: returns 0 if OK

/* ------- attaching and detaching a local db ----- */

void* wg_attach_local_database(wg_int size);
void wg_delete_local_database(void* dbase);

/* ------- functions to query database state ------ */

wg_int wg_database_freesize(void *db);
wg_int wg_database_size(void *db);

/* -------- creating and scanning records --------- */

void* wg_create_record(void* db, wg_int length); ///< returns NULL when error, ptr to rec otherwise
void* wg_create_raw_record(void* db, wg_int length); ///< returns NULL when error, ptr to rec otherwise
wg_int wg_delete_record(void* db, void *rec);  ///< returns 0 on success, non-0 on error

void* wg_get_first_record(void* db);              ///< returns NULL when error or no recs
void* wg_get_next_record(void* db, void* record); ///< returns NULL when error or no more recs

void *wg_get_first_parent(void* db, void *record);
void *wg_get_next_parent(void* db, void* record, void *parent);

/* -------- setting and fetching record field values --------- */

wg_int wg_get_record_len(void* db, void* record); ///< returns negative int when error
wg_int* wg_get_record_dataarray(void* db, void* record); ///< pointer to record data array start

// following field setting functions return negative int when err, 0 when ok
wg_int wg_set_field(void* db, void* record, wg_int fieldnr, wg_int data);
wg_int wg_set_new_field(void* db, void* record, wg_int fieldnr, wg_int data);

wg_int wg_set_int_field(void* db, void* record, wg_int fieldnr, wg_int data);
wg_int wg_set_double_field(void* db, void* record, wg_int fieldnr, double data);
wg_int wg_set_str_field(void* db, void* record, wg_int fieldnr, char* data);

wg_int wg_update_atomic_field(void* db, void* record, wg_int fieldnr, wg_int data, wg_int old_data);
wg_int wg_set_atomic_field(void* db, void* record, wg_int fieldnr, wg_int data);
wg_int wg_add_int_atomic_field(void* db, void* record, wg_int fieldnr, int data);

wg_int wg_get_field(void* db, void* record, wg_int fieldnr);      // returns 0 when error
wg_int wg_get_field_type(void* db, void* record, wg_int fieldnr); // returns 0 when error


/* ---------- general operations on encoded data -------- */

wg_int wg_get_encoded_type(void* db, wg_int data);
wg_int wg_free_encoded(void* db, wg_int data);

/* -------- encoding and decoding data: records contain encoded data only ---------- */

wg_int wg_encode_null(void* db, wg_int data);
wg_int wg_decode_null(void* db, wg_int data);

// int

wg_int wg_encode_int(void* db, wg_int data);
wg_int wg_decode_int(void* db, wg_int data);

// double

wg_int wg_encode_double(void* db, double data);
double wg_decode_double(void* db, wg_int data);

// fixpoint

wg_int wg_encode_fixpoint(void* db, double data);
double wg_decode_fixpoint(void* db, wg_int data);

// date and time

wg_int wg_encode_date(void* db, int data);
int wg_decode_date(void* db, wg_int data);

wg_int wg_encode_time(void* db, int data);
int wg_decode_time(void* db, wg_int data);

int wg_current_utcdate(void* db);
int wg_current_localdate(void* db);
int wg_current_utctime(void* db);
int wg_current_localtime(void* db);

int wg_strf_iso_datetime(void* db, int date, int time, char* buf);
int wg_strp_iso_date(void* db, char* buf);
int wg_strp_iso_time(void* db, char* inbuf);

int wg_ymd_to_date(void* db, int yr, int mo, int day);
int wg_hms_to_time(void* db, int hr, int min, int sec, int prt);
void wg_date_to_ymd(void* db, int date, int *yr, int *mo, int *day);
void wg_time_to_hms(void* db, int time, int *hr, int *min, int *sec, int *prt);

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

char* wg_decode_xmlliteral(void* db, wg_int data);
char* wg_decode_xmlliteral_xsdtype(void* db, wg_int data);

wg_int wg_decode_xmlliteral_len(void* db, wg_int data);
wg_int wg_decode_xmlliteral_xsdtype_len(void* db, wg_int data);
wg_int wg_decode_xmlliteral_copy(void* db, wg_int data, char* strbuf, wg_int buflen);
wg_int wg_decode_xmlliteral_xsdtype_copy(void* db, wg_int data, char* strbuf, wg_int buflen);

// uri (standard C string: zero-terminated array of chars)
// along with an optional namespace str

wg_int wg_encode_uri(void* db, char* str, char* nspace); ///< let nspace==NULL if not used

char* wg_decode_uri(void* db, wg_int data);
char* wg_decode_uri_prefix(void* db, wg_int data);

wg_int wg_decode_uri_len(void* db, wg_int data);
wg_int wg_decode_uri_prefix_len(void* db, wg_int data);
wg_int wg_decode_uri_copy(void* db, wg_int data, char* strbuf, wg_int buflen);
wg_int wg_decode_uri_prefix_copy(void* db, wg_int data, char* strbuf, wg_int buflen);


// blob (binary large object, i.e. any kind of data)
// along with an obligatory length in bytes


wg_int wg_encode_blob(void* db, char* str, char* type, wg_int len);

char* wg_decode_blob(void* db, wg_int data);
char* wg_decode_blob_type(void* db, wg_int data);

wg_int wg_decode_blob_len(void* db, wg_int data);
wg_int wg_decode_blob_copy(void* db, wg_int data, char* strbuf, wg_int buflen);
wg_int wg_decode_blob_type_len(void* db, wg_int data);
wg_int wg_decode_blob_type_copy(void* db, wg_int data, char* langbuf, wg_int buflen);

/// ptr to record

wg_int wg_encode_record(void* db, void* data);
void* wg_decode_record(void* db, wg_int data);

/// char

wg_int wg_encode_char(void* db, char data);
char wg_decode_char(void* db, wg_int data);

// anonconst

wg_int wg_encode_anonconst(void* db, char* str);
char* wg_decode_anonconst(void* db, wg_int data);

// var

wg_int wg_encode_var(void* db, wg_int varnr);
wg_int wg_decode_var(void* db, wg_int data);

/* --- dumping and restoring -------- */


wg_int wg_dump(void * db,char* fileName); // dump shared memory database to the disk
wg_int wg_import_dump(void * db,char* fileName); // import database from the disk
wg_int wg_start_logging(void *db); /* activate journal logging globally */
wg_int wg_stop_logging(void *db); /* deactivate journal logging */
wg_int wg_replay_log(void *db, char *filename); /* restore from journal */

/* ---------- concurrency support  ---------- */

wg_int wg_start_write(void * dbase);          /* start write transaction */
wg_int wg_end_write(void * dbase, wg_int lock); /* end write transaction */
wg_int wg_start_read(void * dbase);           /* start read transaction */
wg_int wg_end_read(void * dbase, wg_int lock);  /* end read transaction */

/* ------------- utilities ----------------- */

void wg_print_db(void *db);
void wg_print_record(void *db, wg_int* rec);
void wg_snprint_value(void *db, wg_int enc, char *buf, int buflen);
wg_int wg_parse_and_encode(void *db, char *buf);
wg_int wg_parse_and_encode_param(void *db, char *buf);
void wg_export_db_csv(void *db, char *filename);
wg_int wg_import_db_csv(void *db, char *filename);

/* ---------- query functions -------------- */

wg_query *wg_make_query(void *db, void *matchrec, wg_int reclen,
  wg_query_arg *arglist, wg_int argc);
#define wg_make_prefetch_query wg_make_query
wg_query *wg_make_query_rc(void *db, void *matchrec, wg_int reclen,
  wg_query_arg *arglist, wg_int argc, wg_uint rowlimit);
void *wg_fetch(void *db, wg_query *query);
void wg_free_query(void *db, wg_query *query);

wg_int wg_encode_query_param_null(void *db, char *data);
wg_int wg_encode_query_param_record(void *db, void *data);
wg_int wg_encode_query_param_char(void *db, char data);
wg_int wg_encode_query_param_fixpoint(void *db, double data);
wg_int wg_encode_query_param_date(void *db, int data);
wg_int wg_encode_query_param_time(void *db, int data);
wg_int wg_encode_query_param_var(void *db, wg_int data);
wg_int wg_encode_query_param_int(void *db, wg_int data);
wg_int wg_encode_query_param_double(void *db, double data);
wg_int wg_encode_query_param_str(void *db, char *data, char *lang);
wg_int wg_encode_query_param_xmlliteral(void *db, char *data, char *xsdtype);
wg_int wg_encode_query_param_uri(void *db, char *data, char *prefix);
wg_int wg_free_query_param(void* db, wg_int data);

void *wg_find_record(void *db, wg_int fieldnr, wg_int cond, wg_int data,
    void* lastrecord);
void *wg_find_record_null(void *db, wg_int fieldnr, wg_int cond, char *data,
    void* lastrecord);
void *wg_find_record_record(void *db, wg_int fieldnr, wg_int cond, void *data,
    void* lastrecord);
void *wg_find_record_char(void *db, wg_int fieldnr, wg_int cond, char data,
    void* lastrecord);
void *wg_find_record_fixpoint(void *db, wg_int fieldnr, wg_int cond,
    double data, void* lastrecord);
void *wg_find_record_date(void *db, wg_int fieldnr, wg_int cond, int data,
    void* lastrecord);
void *wg_find_record_time(void *db, wg_int fieldnr, wg_int cond, int data,
    void* lastrecord);
void *wg_find_record_var(void *db, wg_int fieldnr, wg_int cond, wg_int data,
    void* lastrecord);
void *wg_find_record_int(void *db, wg_int fieldnr, wg_int cond, int data,
    void* lastrecord);
void *wg_find_record_double(void *db, wg_int fieldnr, wg_int cond, double data,
    void* lastrecord);
void *wg_find_record_str(void *db, wg_int fieldnr, wg_int cond, char *data,
    void* lastrecord);
void *wg_find_record_xmlliteral(void *db, wg_int fieldnr, wg_int cond,
    char *data, char *xsdtype, void* lastrecord);
void *wg_find_record_uri(void *db, wg_int fieldnr, wg_int cond, char *data,
    char *prefix, void* lastrecord);

/* ---------- child database handling ------ */

wg_int wg_register_external_db(void *db, void *extdb);
wg_int wg_encode_external_data(void *db, void *extdb, wg_int encoded);

/* ---------- JSON document I/O ------------ */

wg_int wg_parse_json_file(void *db, char *filename);
wg_int wg_check_json(void *db, char *buf);
wg_int wg_parse_json_document(void *db, char *buf, void **document);
wg_int wg_parse_json_fragment(void *db, char *buf, void **document);


#ifdef __cplusplus
}
#endif

#endif /* DEFINED_DBAPI_H */
