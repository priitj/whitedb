/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010,2011,2012,2013
*
* Minor mods by Tanel Tammet. Triple handler for raptor and raptor
* rdf parsing originally written by Tanel Tammet.
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

 /** @file dbutil.c
 * Miscellaneous utility functions.
 */

/* ====== Includes =============== */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef HAVE_RAPTOR
#include <raptor.h>
#endif

/* ====== Private headers and defs ======== */

#ifdef __cplusplus
extern "C" {
#endif

#include "dbdata.h"
#include "dbutil.h"
#include "dbquery.h"

#ifdef _WIN32
#define snprintf(s, sz, f, ...) _snprintf_s(s, sz+1, sz, f, ## __VA_ARGS__)
#define strncpy(d, s, sz) strncpy_s(d, sz+1, s, sz)
#else
/* Use error-detecting versions for other C libs */
#define atof(s) strtod(s, NULL)
#define atol(s) strtol(s, NULL, 10)
#endif

#define CSV_FIELD_BUF 4096      /** max size of csv I/O field */
#define CSV_FIELD_SEPARATOR ',' /** field separator, comma or semicolon */
#define CSV_DECIMAL_SEPARATOR '.'   /** comma or dot */
#define CSV_ENCDATA_BUF 10      /** initial storage for encoded (gint) data */

#define MAX_URI_SCHEME 10

/* ======== Data ========================= */

/** Recognized URI schemes (used when parsing input data)
 * when adding new schemes, check that MAX_URI_SCHEME is enough to
 * store the entire scheme + '\0'
 */
struct uri_scheme_info {
  char *prefix;
  int length;
} uri_scheme_table[] = {
  { "urn:", 4 },
  { "file:", 5 },
  { "http://", 7 },
  { "https://", 8 },
  { "mailto:", 7 },
  { NULL, 0 }
};


/* ======= Private protos ================ */

static gint show_io_error(void *db, char *errmsg);
static gint show_io_error_str(void *db, char *errmsg, char *str);
static void snprint_record(void *db, wg_int* rec, char *buf, int buflen);
static void csv_escaped_str(void *db, char *iptr, char *buf, int buflen);
static void snprint_value_csv(void *db, gint enc, char *buf, int buflen);
#if 0
static gint parse_and_encode_uri(void *db, char *buf);
#endif
static gint parse_input_type(void *db, char *buf, gint *intdata,
                                        double *doubledata, gint *datetime);
static gint fread_csv(void *db, FILE *f);

#ifdef HAVE_RAPTOR
static gint import_raptor(void *db, gint pref_fields, gint suff_fields,
  gint (*callback) (void *, void *), char *filename, raptor_parser *rdf_parser);
static void handle_triple(void* user_data, const raptor_statement* triple);
static raptor_uri *dburi_to_raptoruri(void *db, gint enc);
static gint export_raptor(void *db, gint pref_fields, char *filename,
  raptor_serializer *rdf_serializer);
#endif

/* ====== Functions ============== */

/** Print contents of database.
 *
 */

void wg_print_db(void *db) {
  void *rec;

  rec = wg_get_first_record(db);
  while(rec) {
    wg_print_record(db, (gint *) rec);
    printf("\n");
    rec = wg_get_next_record(db,rec);
  }
}

/** Print single record
 *
 */
void wg_print_record(void *db, wg_int* rec) {

  wg_int len, enc;
  int i;
  char strbuf[256];
#ifdef USE_CHILD_DB
  void *parent;
#endif

  if (rec==NULL) {
    printf("<null rec pointer>\n");
    return;
  }

#ifdef USE_CHILD_DB
  parent = wg_get_rec_owner(db, rec);
#endif

  len = wg_get_record_len(db, rec);
  printf("[");
  for(i=0; i<len; i++) {
    if(i) printf(",");
    enc = wg_get_field(db, rec, i);
#ifdef USE_CHILD_DB
    if(parent != db)
      enc = wg_translate_hdroffset(db, parent, enc);
#endif
    wg_snprint_value(db, enc, strbuf, 255);
    printf("%s", strbuf);
  }
  printf("]");
}

/** Print a record into a stream (to handle records recursively)
 *  expects buflen to be at least 2.
 */
static void snprint_record(void *db, wg_int* rec, char *buf, int buflen) {

  char *strbuf;
#ifdef USE_CHILD_DB
  void *parent;
#endif

  if(rec==NULL) {
    snprintf(buf, buflen, "<null rec pointer>\n");
    return;
  }
  if(buflen < 2)
    return;

  *buf++ = '[';
  buflen--;

#ifdef USE_CHILD_DB
  parent = wg_get_rec_owner(db, rec);
#endif

  strbuf = malloc(buflen);
  if(strbuf) {
    int i, strbuflen;
    gint enc;
    gint len = wg_get_record_len(db, rec);
    for(i=0; i<len; i++) {
      /* Use a fresh buffer for the value. This way we can
       * easily count how many bytes printing the value added.
       */
      enc = wg_get_field(db, rec, i);
  #ifdef USE_CHILD_DB
      if(parent != db)
        enc = wg_translate_hdroffset(db, parent, enc);
  #endif
      wg_snprint_value(db, enc, strbuf, buflen);
      strbuflen = strlen(strbuf);

      /* Check if the value fits comfortably, including
       * leading comma and trailing \0
       */
      if(buflen < strbuflen + 2)
        break;
      if(i) {
        *buf++ = ',';
        buflen--;
      }
      strncpy(buf, strbuf, buflen);
      buflen -= strbuflen;
      buf += strbuflen;
      if(buflen < 2)
        break;
    }
    free(strbuf);
  }
  if(buflen > 1)
    *buf++ = ']';
  *buf = '\0';
}

/** Print a single, encoded value
 *  The value is written into a character buffer.
 */
void wg_snprint_value(void *db, gint enc, char *buf, int buflen) {
  gint ptrdata;
  int intdata, len;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[80];

  buflen--; /* snprintf adds '\0' */
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      snprintf(buf, buflen, "NULL");
      break;
    case WG_RECORDTYPE:
      ptrdata = (gint) wg_decode_record(db, enc);
      snprintf(buf, buflen, "<rec %x>", (int) ptrdata);
      len = strlen(buf);
      if(buflen - len > 1)
        snprint_record(db, (wg_int*)ptrdata, buf+len, buflen-len);
      break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      snprintf(buf, buflen, "%d", intdata);
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_FIXPOINTTYPE:
      doubledata = wg_decode_fixpoint(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      snprintf(buf, buflen, "\"%s\"", strdata);
      break;
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      if (exdata==NULL)
        snprintf(buf, buflen, "%s", strdata);
      else
        snprintf(buf, buflen, "%s:%s", exdata, strdata);
      break;
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      snprintf(buf, buflen, "\"<xsdtype %s>%s\"", exdata, strdata);
      break;
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      snprintf(buf, buflen, "%c", (char) intdata);
      break;
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      snprintf(buf, buflen, "<raw date %d>%s", intdata,strbuf);
      break;
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);
      snprintf(buf, buflen, "<raw time %d>%s",intdata,strbuf+11);
      break;
    case WG_VARTYPE:
      intdata = wg_decode_var(db, enc);
      snprintf(buf, buflen, "?%d", intdata);
      break;
    case WG_ANONCONSTTYPE:
      strdata = wg_decode_anonconst(db, enc);
      snprintf(buf, buflen, "!%s",strdata);
      break;
    default:
      snprintf(buf, buflen, "<unsupported type>");
      break;
  }
}


/** Create CSV-formatted quoted string
 *
 */
static void csv_escaped_str(void *db, char *iptr, char *buf, int buflen) {
  char *optr;

#ifdef CHECK
  if(buflen < 3) {
    show_io_error(db, "CSV field buffer too small");
    return;
  }
#endif
  optr = buf;
  *optr++ = '"';
  buflen--; /* space for terminating quote */
  while(*iptr) { /* \0 terminates */
    int nextsz = 1;
    if(*iptr == '"') nextsz++;

    /* Will our string fit? */
    if(((gint)optr + nextsz - (gint)buf) < buflen) {
      *optr++ = *iptr;
      if(*iptr++ == '"')
        *optr++ = '"'; /* quote -> double quote */
    } else
      break;
  }
  *optr++ = '"'; /* CSV string terminator */
  *optr = '\0'; /* C string terminator */
}


/** Print a single, encoded value, into a CSV-friendly format
 *  The value is written into a character buffer.
 */
static void snprint_value_csv(void *db, gint enc, char *buf, int buflen) {
  int intdata, ilen;
  double doubledata;
  char strbuf[80], *ibuf;

  buflen--; /* snprintf adds '\0' */
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      buf[0] = '\0'; /* output an empty field */
      break;
    case WG_RECORDTYPE:
      intdata = ptrtooffset(db, wg_decode_record(db, enc));
      snprintf(buf, buflen, "\"<record offset %d>\"", intdata);
      break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      snprintf(buf, buflen, "%d", intdata);
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_FIXPOINTTYPE:
      doubledata = wg_decode_fixpoint(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_STRTYPE:
      csv_escaped_str(db, wg_decode_str(db, enc), buf, buflen);
      break;
    case WG_XMLLITERALTYPE:
      csv_escaped_str(db, wg_decode_xmlliteral(db, enc), buf, buflen);
      break;
    case WG_URITYPE:
      /* More efficient solutions are possible, but here we simply allocate
       * enough storage to concatenate the URI before encoding it for CSV.
       */
      ilen = wg_decode_uri_len(db, enc);
      ilen += wg_decode_uri_prefix_len(db, enc);
      ibuf = (char *) malloc(ilen + 1);
      if(!ibuf) {
        show_io_error(db, "Failed to allocate memory");
        return;
      }
      snprintf(ibuf, ilen+1, "%s%s",
        wg_decode_uri_prefix(db, enc), wg_decode_uri(db, enc));
      csv_escaped_str(db, ibuf, buf, buflen);
      free(ibuf);
      break;
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      snprintf(buf, buflen, "%c", (char) intdata);
      break;
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      snprintf(buf, buflen, "%s", strbuf);
      break;
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);
      snprintf(buf, buflen, "%s", strbuf+11);
      break;
    default:
      snprintf(buf, buflen, "\"<unsupported type>\"");
      break;
  }
}


/** Try parsing an URI from a string.
 *  Returns encoded WG_URITYPE field when successful
 *  Returns WG_ILLEGAL on error
 *
 *  XXX: this is a very naive implementation. Something more robust
 *  is needed.
 *
 *  XXX: currently unused.
 */
#if 0
static gint parse_and_encode_uri(void *db, char *buf) {
  gint encoded = WG_ILLEGAL;
  struct uri_scheme_info *next = uri_scheme_table;

  /* Try matching to a known scheme */
  while(next->prefix) {
    if(!strncmp(buf, next->prefix, next->length)) {
      /* We have a matching URI scheme.
       * XXX: check this code for correct handling of prefix. */
      int urilen = strlen(buf);
      char *prefix = (char *) malloc(urilen + 1);
      char *dataptr;

      if(!prefix)
        break;
      strncpy(prefix, buf, urilen);

      dataptr = prefix + urilen;
      while(--dataptr >= prefix) {
        switch(*dataptr) {
          case ':':
          case '/':
          case '#':
            *(dataptr+1) = '\0';
            goto prefix_marked;
          default:
            break;
        }
      }
prefix_marked:
      encoded = wg_encode_uri(db, buf+((gint)dataptr-(gint)prefix+1), prefix);
      free(prefix);
      break;
    }
    next++;
  }
  return encoded;
}
#endif

/** Parse value from string, encode it for WhiteDB
 *  returns WG_ILLEGAL if value could not be parsed or
 *  encoded.
 *
 *  See the comment for parse_input_type() for the supported types.
 *  If other conversions fail, data will be encoded as string.
 */
gint wg_parse_and_encode(void *db, char *buf) {
  gint intdata = 0;
  double doubledata = 0;
  gint encoded = WG_ILLEGAL, res = 0;

  switch(parse_input_type(db, buf, &intdata, &doubledata, &res)) {
    case WG_NULLTYPE:
      encoded = 0;
      break;
    case WG_INTTYPE:
      encoded = wg_encode_int(db, intdata);
      break;
    case WG_DOUBLETYPE:
      encoded = wg_encode_double(db, doubledata);
      break;
    case WG_STRTYPE:
      encoded = wg_encode_str(db, buf, NULL);
      break;
    case WG_DATETYPE:
      encoded = wg_encode_date(db, res);
      break;
    case WG_TIMETYPE:
      encoded = wg_encode_time(db, res);
      break;
    default:
      break;
  }
  return encoded;
}

/** Parse value from string, encode it as a query parameter.
 *  returns WG_ILLEGAL if value could not be parsed or
 *  encoded.
 *
 *  Parameters encoded like this should be freed with
 *  wg_free_query_param() and cannot be used interchangeably
 *  with other encoded values.
 */
gint wg_parse_and_encode_param(void *db, char *buf) {
  gint intdata = 0;
  double doubledata = 0;
  gint encoded = WG_ILLEGAL, res = 0;

  switch(parse_input_type(db, buf, &intdata, &doubledata, &res)) {
    case WG_NULLTYPE:
      encoded = 0;
      break;
    case WG_INTTYPE:
      encoded = wg_encode_query_param_int(db, intdata);
      break;
    case WG_DOUBLETYPE:
      encoded = wg_encode_query_param_double(db, doubledata);
      break;
    case WG_STRTYPE:
      encoded = wg_encode_query_param_str(db, buf, NULL);
      break;
    case WG_DATETYPE:
      encoded = wg_encode_query_param_date(db, res);
      break;
    case WG_TIMETYPE:
      encoded = wg_encode_query_param_time(db, res);
      break;
    default:
      break;
  }
  return encoded;
}

/** Detect the type of input data in string format.
 *
 *  Supports following data types:
 *  NULL - empty string
 *  int - plain integer
 *  double - floating point number in fixed decimal notation
 *  date - ISO8601 date
 *  time - ISO8601 time+fractions of second.
 *  string - input data that does not match the above types
 *
 *  Does NOT support ambiguous types:
 *  fixpoint - floating point number in fixed decimal notation
 *  uri - string starting with an URI prefix
 *  char - single character
 *
 *  Does NOT support types which would require a special encoding
 *  scheme in string form:
 *  record, XML literal, blob, anon const, variables
 *
 *  Return values:
 *  0 - value type could not be parsed or detected
 *  WG_NULLTYPE - NULL
 *  WG_INTTYPE - int, *intdata contains value
 *  WG_DOUBLETYPE - double, *doubledata contains value
 *  WG_DATETYPE - date, *datetime contains internal representation
 *  WG_TIMETYPE - time, *datetime contains internal representation
 *  WG_STRTYPE - string, use entire buf
 *
 *  Since leading whitespace makes type guesses fail, it invariably
 *  causes WG_STRTYPE to be returned.
 */
static gint parse_input_type(void *db, char *buf, gint *intdata,
                                        double *doubledata, gint *datetime) {
  gint type = 0;
  char c = buf[0];

  if(c == 0) {
    /* empty fields become NULL-s */
    type = WG_NULLTYPE;
  }
  else if((c >= '0' && c <= '9') ||\
   (c == '-' && buf[1] >= '0' && buf[1] <= '9')) {
    /* This could be one of int, double, date or time */
    if(c != '-' && (*datetime = wg_strp_iso_date(db, buf)) >= 0) {
      type = WG_DATETYPE;
    } else if(c != '-' && (*datetime = wg_strp_iso_time(db, buf)) >= 0) {
      type = WG_TIMETYPE;
    } else {
      /* Examine the field contents to distinguish between float
       * and int, then convert using atol()/atof(). sscanf() tends to
       * be too optimistic about the conversion, especially under Win32.
       */
      char numbuf[80];
      char *ptr = buf, *wptr = numbuf, *decptr = NULL;
      int decsep = 0;
      while(*ptr) {
        if(*ptr == CSV_DECIMAL_SEPARATOR) {
          decsep++;
          decptr = wptr;
        }
        else if((*ptr < '0' || *ptr > '9') && ptr != buf) {
          /* Non-numeric. Mark this as an invalid number
           * by abusing the decimal separator count.
           */
          decsep = 2;
          break;
        }
        *(wptr++) = *(ptr++);
        if((int) (wptr - numbuf) >= 79)
          break;
      }
      *wptr = '\0';

      if(decsep==1) {
        char tmp = *decptr;
        *decptr = '.'; /* ignore locale, force conversion by plain atof() */
        *doubledata = atof(numbuf);
        if(errno!=ERANGE && errno!=EINVAL) {
          type = WG_DOUBLETYPE;
        } else {
          errno = 0; /* Under Win32, successful calls don't do this? */
        }
        *decptr = tmp; /* conversion might have failed, restore string */
      } else if(!decsep) {
        *intdata = atol(numbuf);
        if(errno!=ERANGE && errno!=EINVAL) {
          type = WG_INTTYPE;
        } else {
          errno = 0;
        }
      }
    }
  }

  if(type == 0) {
    /* Default type is string */
    type = WG_STRTYPE;
  }
  return type;
}

/** Write single record to stream in CSV format
 *
 */
void wg_fprint_record_csv(void *db, wg_int* rec, FILE *f) {

  wg_int len, enc;
  int i;
  char *strbuf;

  if(rec==NULL) {
    show_io_error(db, "null record pointer");
    return;
  }

  strbuf = (char *) malloc(CSV_FIELD_BUF);
  if(strbuf==NULL) {
    show_io_error(db, "Failed to allocate memory");
    return;
  }

  len = wg_get_record_len(db, rec);
  for(i=0; i<len; i++) {
    if(i) fprintf(f, "%c", CSV_FIELD_SEPARATOR);
    enc = wg_get_field(db, rec, i);
    snprint_value_csv(db, enc, strbuf, CSV_FIELD_BUF-1);
    fprintf(f, "%s", strbuf);
  }

  free(strbuf);
}

/** Export contents of database into a CSV file.
 *
 */

void wg_export_db_csv(void *db, char *filename) {
  void *rec;
  FILE *f;

#ifdef _WIN32
  if(fopen_s(&f, filename, "w")) {
#else
  if(!(f = fopen(filename, "w"))) {
#endif
    show_io_error_str(db, "failed to open file", filename);
    return;
  }

  rec = wg_get_first_record(db);
  while(rec) {
    wg_fprint_record_csv(db, (wg_int *) rec, f);
    fprintf(f, "\n");
    rec = wg_get_next_record(db, rec);
  };

  fclose(f);
}

/** Read CSV stream and convert it to database records.
 *  Returns 0 if there were no errors
 *  Returns -1 for non-fatal errors
 *  Returns -2 for database errors
 *  Returns -3 for other errors
 */
static gint fread_csv(void *db, FILE *f) {
  char *strbuf, *ptr;
  gint *encdata;
  gint err = 0;
  gint uq_field, quoted_field, esc_quote, eat_sep; /** State flags */
  gint commit_strbuf, commit_record;
  gint reclen;
  gint encdata_sz = CSV_ENCDATA_BUF;

  strbuf = (char *) malloc(CSV_FIELD_BUF);
  if(strbuf==NULL) {
    show_io_error(db, "Failed to allocate memory");
    return -1;
  }

  encdata = (gint *) malloc(sizeof(gint) * encdata_sz);
  if(strbuf==NULL) {
    free(strbuf);
    show_io_error(db, "Failed to allocate memory");
    return -1;
  }

  /* Init parser state */
  reclen = 0;
  uq_field = quoted_field = esc_quote = eat_sep = 0;
  commit_strbuf = commit_record = 0;
  ptr = strbuf;

  while(!feof(f)) {
    /* Parse cycle consists:
     * 1. read character from stream. This can either:
     *   - change the state of the parser
     *   - be appended to strbuf
     * 2. if the parser state changed, we need to do one
     *    of the following:
     *   - parse the field from strbuf
     *   - store the record in the database
     */

    char c = (char) fgetc(f);

    if(quoted_field) {
      /* We're parsing a quoted field. Anything we get is added to
       * strbuf unless it's a quote character.
       */
      if(!esc_quote && c == '"') {
        char nextc = (char) fgetc(f);
        ungetc((int) nextc, f);

        if(nextc!='"') {
          /* Field ends. Note that even EOF is acceptable here */
          quoted_field = 0;
          commit_strbuf = 1; /* set flag to commit buffer */
          eat_sep = 1; /* next separator can be ignored. */
        } else {
          esc_quote = 1; /* make a note that next quote is escaped */
        }
      } else {
        esc_quote = 0;
        /* read the character. It's simply ignored if the buffer is full */
        if(((gint) ptr - (gint) strbuf) < CSV_FIELD_BUF-1)
          *ptr++ = c;
      }
    } else if(uq_field) {
      /* In case of an unquoted field, terminator can be the field
       * separator or end of line. In the latter case we also need to
       * store the record.
       */
      if(c == CSV_FIELD_SEPARATOR) {
        uq_field = 0;
        commit_strbuf = 1;
      } else if(c == 13) { /* Ignore CR. */
        continue;
      } else if(c == 10) { /* LF is the last character for both DOS and UNIX */
        uq_field = 0;
        commit_strbuf = 1;
        commit_record = 1;
      } else {
        if(((gint) ptr - (gint) strbuf) < CSV_FIELD_BUF-1)
          *ptr++ = c;
      }
    } else {
      /* We are currently not parsing anything. Four things can happen:
       * - quoted field begins
       * - unquoted field begins
       * - we're on a field separator
       * - line ends
       */
      if(c == CSV_FIELD_SEPARATOR) {
        if(eat_sep) {
          /* A quoted field just ended, this separator can be skipped */
          eat_sep = 0;
          continue;
        } else {
          /* The other way to interpret this is that we have a NULL field.
           * Commit empty buffer. */
          commit_strbuf = 1;
        }
      } else if(c == 13) { /* CR is ignored, as we're expecting LF to follow */
        continue;
      } else if(c == 10) { /* LF is line terminator. */
        if(eat_sep) {
          eat_sep = 0; /* should reset this as well */
        } else if(reclen) {
          /* This state can occur when we've been preceded by a record
           * separator. The zero length field between ,\n counts as a NULL
           * field. XXX: this creates an inconsistent situation where
           * empty lines are discarded while a sincle comma (or semicolon)
           * generates a two-field record of NULL-s. The benefit is that
           * junk empty lines don't generate junk records. Check the
           * unofficial RFC to see if this should be changed.
           */
          commit_strbuf = 1;
        }
        commit_record = 1;
      } else {
        /* A new field begins */
        if(c == '"') {
          quoted_field = 1;
        }
        else {
          uq_field = 1;
          *ptr++ = c;
        }
      }
    }

    if(commit_strbuf) {
      gint enc;

      /* We were instructed to convert our string buffer to data. First
       * mark the end of string and reset strbuf state for next loop. */
      *ptr = (char) 0;
      commit_strbuf = 0;
      ptr = strbuf;

      /* Need more storage for encoded data? */
      if(reclen >= encdata_sz) {
        gint *tmp;
        encdata_sz += CSV_ENCDATA_BUF;
        tmp = (gint *) realloc(encdata, sizeof(gint) * encdata_sz);
        if(tmp==NULL) {
          err = -3;
          show_io_error(db, "Failed to allocate memory");
          break;
        } else
          encdata = tmp;
      }

      /* Do the actual parsing. This also allocates database-side
       * storage for the new data. */
      enc = wg_parse_and_encode(db, strbuf);
      if(enc == WG_ILLEGAL) {
        show_io_error_str(db, "Warning: failed to parse", strbuf);
        enc = 0; /* continue anyway */
      }
      encdata[reclen++] = enc;
    }

    if(commit_record) {
      /* Need to save the record to database. */
      int i;
      void *rec;

      commit_record = 0;
      if(!reclen)
        continue; /* Ignore empty rows */

      rec = wg_create_record(db, reclen);
      if(!rec) {
        err = -2;
        show_io_error(db, "Failed to create record");
        break;
      }
      for(i=0; i<reclen; i++) {
        if(wg_set_field(db, rec, i, encdata[i])) {
          err = -2;
          show_io_error(db, "Failed to save field data");
          break;
        }
      }

      /* Reset record data */
      reclen = 0;
    }
  }

  free(encdata);
  free(strbuf);
  return err;
}

/** Import data from a CSV file into database
 *  Data will be added to existing data.
 *  Returns 0 if there were no errors
 *  Returns -1 for file I/O errors
 *  Other error codes may be generated by fread_csv()
 */

gint wg_import_db_csv(void *db, char *filename) {
  FILE *f;
  gint err = 0;

#ifdef _WIN32
  if(fopen_s(&f, filename, "r")) {
#else
  if(!(f = fopen(filename, "r"))) {
#endif
    show_io_error_str(db, "failed to open file", filename);
    return -1;
  }

  err = fread_csv(db, f);
  fclose(f);
  return err;
}

#ifdef HAVE_RAPTOR

/** Import RDF data from file
 *  wrapper for import_raptor() that recognizes the content via filename
 */
gint wg_import_raptor_file(void *db, gint pref_fields, gint suff_fields,
  gint (*callback) (void *, void *), char *filename) {
  raptor_parser* rdf_parser=NULL;
  gint err = 0;

  raptor_init();
  rdf_parser = raptor_new_parser_for_content(NULL, NULL, NULL, 0,
    (unsigned char *) filename);
  if(!rdf_parser)
    return -1;

  err = import_raptor(db, pref_fields, suff_fields, (*callback),
    filename, rdf_parser);

  raptor_free_parser(rdf_parser);
  raptor_finish();
  return err;
}

/** Import RDF data from file, instructing raptor to use rdfxml parser
 *  Sample wrapper to demonstrate potential extensions to API
 */
gint wg_import_raptor_rdfxml_file(void *db, gint pref_fields, gint suff_fields,
  gint (*callback) (void *, void *), char *filename) {
  raptor_parser* rdf_parser=NULL;
  gint err = 0;

  raptor_init();
  rdf_parser=raptor_new_parser("rdfxml"); /* explicitly select the parser */
  if(!rdf_parser)
    return -1;

  err = import_raptor(db, pref_fields, suff_fields, (*callback),
    filename, rdf_parser);

  raptor_free_parser(rdf_parser);
  raptor_finish();
  return err;
}

/** File-based raptor import function
 *  Uses WhiteDB-specific API parameters of:
 *  pref_fields
 *  suff_fields
 *  callback
 *
 *  This function should be wrapped in a function that initializes
 *  raptor parser to the appropriate content type.
 */
static gint import_raptor(void *db, gint pref_fields, gint suff_fields,
  gint (*callback) (void *, void *), char *filename, raptor_parser *rdf_parser) {
  unsigned char *uri_string;
  raptor_uri *uri, *base_uri;
  struct wg_triple_handler_params user_data;
  int err;

  user_data.db = db;
  user_data.pref_fields = pref_fields;
  user_data.suff_fields = suff_fields;
  user_data.callback = (*callback);
  user_data.rdf_parser = rdf_parser;
  user_data.count = 0;
  user_data.error = 0;
  raptor_set_statement_handler(rdf_parser, &user_data, handle_triple);

  uri_string=raptor_uri_filename_to_uri_string(filename);
  uri=raptor_new_uri(uri_string);
  base_uri=raptor_uri_copy(uri);

  /* Parse the file. In some cases raptor returns an error but not
   * in all cases that interest us, we also consider feedback from
   * the triple handler.
   */
  err = raptor_parse_file(rdf_parser, uri, base_uri);
  if(err > 0)
    err = -1; /* XXX: not clear if fatal errors can occur here */
  if(!user_data.count && err > -1)
    err = -1; /* No rows read. File was total garbage? */
  if(err > user_data.error)
    err = user_data.error; /* More severe database error. */

  raptor_free_uri(base_uri);
  raptor_free_uri(uri);
  raptor_free_memory(uri_string);
  return (gint) err;
}

/** Triple handler for raptor
 *  Stores the triples parsed by raptor into database
 */
static void handle_triple(void* user_data, const raptor_statement* triple) {
  void* rec;
  struct wg_triple_handler_params *params = \
    (struct wg_triple_handler_params *) user_data;
  gint enc;

  rec=wg_create_record(params->db,
    params->pref_fields + 3 + params->suff_fields);
  if (!rec) {
    show_io_error(params->db, "cannot create a new record");
    params->error = -2;
    raptor_parse_abort(params->rdf_parser);
  }

  /* Field storage order: predicate, subject, object */
  enc = parse_and_encode_uri(params->db, (char*)(triple->predicate));
  if(enc==WG_ILLEGAL ||\
    wg_set_field(params->db, rec, params->pref_fields, enc)) {
    show_io_error(params->db, "failed to store field");
    params->error = -2;
    raptor_parse_abort(params->rdf_parser);
  }
  enc = parse_and_encode_uri(params->db, (char*)(triple->subject));
  if(enc==WG_ILLEGAL ||\
    wg_set_field(params->db, rec, params->pref_fields+1, enc)) {
    show_io_error(params->db, "failed to store field");
    params->error = -2;
    raptor_parse_abort(params->rdf_parser);
  }

  if ((triple->object_type)==RAPTOR_IDENTIFIER_TYPE_RESOURCE) {
    enc = parse_and_encode_uri(params->db, (char*)(triple->object));
  } else if ((triple->object_type)==RAPTOR_IDENTIFIER_TYPE_ANONYMOUS) {
    /* Fixed prefix urn:local: */
    enc=wg_encode_uri(params->db, (char*)(triple->object),
      "urn:local:");
  } else if ((triple->object_type)==RAPTOR_IDENTIFIER_TYPE_LITERAL) {
    if ((triple->object_literal_datatype)==NULL) {
      enc=wg_encode_str(params->db,(char*)(triple->object),
        (char*)(triple->object_literal_language));
    } else {
      enc=wg_encode_xmlliteral(params->db, (char*)(triple->object),
        (char*)(triple->object_literal_datatype));
    }
  } else {
    show_io_error(params->db, "Unknown triple object type");
    /* XXX: is this fatal? Maybe we should set error and continue here */
    params->error = -2;
    raptor_parse_abort(params->rdf_parser);
  }

  if(enc==WG_ILLEGAL ||\
    wg_set_field(params->db, rec, params->pref_fields+2, enc)) {
    show_io_error(params->db, "failed to store field");
    params->error = -2;
    raptor_parse_abort(params->rdf_parser);
  }

  /* After correctly storing the triple, call the designated callback */
  if(params->callback) {
    if((*(params->callback)) (params->db, rec)) {
      show_io_error(params->db, "record callback failed");
      params->error = -2;
      raptor_parse_abort(params->rdf_parser);
    }
  }

  params->count++;
}

/** WhiteDB RDF parsing callback
 *  This callback does nothing, but is always called when RDF files
 *  are imported using wgdb commandline tool. If import API is used from
 *  user application, alternative callback functions can be implemented
 *  in there.
 *
 *  Callback functions are expected to return 0 on success and
 *  <0 on errors that cause the database to go into an invalid state.
 */
gint wg_rdfparse_default_callback(void *db, void *rec) {
  return 0;
}

/** Export triple data to file
 *  wrapper for export_raptor(), allows user to specify serializer type.
 *
 *  raptor provides an API to enumerate serializers. This is not
 *  utilized here.
 */
gint wg_export_raptor_file(void *db, gint pref_fields, char *filename,
  char *serializer) {
  raptor_serializer *rdf_serializer=NULL;
  gint err = 0;

  raptor_init();
  rdf_serializer = raptor_new_serializer(serializer);
  if(!rdf_serializer)
    return -1;

  err = export_raptor(db, pref_fields, filename, rdf_serializer);

  raptor_free_serializer(rdf_serializer);
  raptor_finish();
  return err;
}

/** Export triple data to file, instructing raptor to use rdfxml serializer
 *
 */
gint wg_export_raptor_rdfxml_file(void *db, gint pref_fields, char *filename) {
  return wg_export_raptor_file(db, pref_fields, filename, "rdfxml");
}

/** Convert wgdb URI field to raptor URI
 *  Helper function. Caller is responsible for calling raptor_free_uri()
 *  when the returned value is no longer needed.
 */
static raptor_uri *dburi_to_raptoruri(void *db, gint enc) {
  raptor_uri *tmpuri = raptor_new_uri((unsigned char *)
    wg_decode_uri_prefix(db, enc));
  raptor_uri *uri = raptor_new_uri_from_uri_local_name(tmpuri,
    (unsigned char *) wg_decode_uri(db, enc));
  raptor_free_uri(tmpuri);
  return uri;
}

/** File-based raptor export function
 *  Uses WhiteDB-specific API parameters of:
 *  pref_fields
 *  suff_fields
 *
 *  Expects an initialized serializer as an argument.
 *  returns 0 on success.
 *  returns -1 on errors (no fatal errors that would corrupt
 *  the database are expected here).
 */
static gint export_raptor(void *db, gint pref_fields, char *filename,
  raptor_serializer *rdf_serializer) {
  int err, minsize;
  raptor_statement *triple;
  void *rec;

  err = raptor_serialize_start_to_filename(rdf_serializer, filename);
  if(err)
    return -1; /* initialization failed somehow */

  /* Start constructing triples and sending them to the serializer. */
  triple = (raptor_statement *) malloc(sizeof(raptor_statement));
  if(!triple) {
    show_io_error(db, "Failed to allocate memory");
    return -1;
  }
  memset(triple, 0, sizeof(raptor_statement));

  rec = wg_get_first_record(db);
  minsize = pref_fields + 3;
  while(rec) {
    if(wg_get_record_len(db, rec) >= minsize) {
      gint enc = wg_get_field(db, rec, pref_fields);

      if(wg_get_encoded_type(db, enc) == WG_URITYPE) {
        triple->predicate = dburi_to_raptoruri(db, enc);
      }
      else if(wg_get_encoded_type(db, enc) == WG_STRTYPE) {
        triple->predicate = (void *) raptor_new_uri(
          (unsigned char *) wg_decode_str(db, enc));
      }
      else {
        show_io_error(db, "Bad field type for predicate");
        err = -1;
        goto done;
      }
      triple->predicate_type = RAPTOR_IDENTIFIER_TYPE_RESOURCE;

      enc = wg_get_field(db, rec, pref_fields + 1);

      if(wg_get_encoded_type(db, enc) == WG_URITYPE) {
        triple->subject = dburi_to_raptoruri(db, enc);
      }
      else if(wg_get_encoded_type(db, enc) == WG_STRTYPE) {
        triple->subject = (void *) raptor_new_uri(
          (unsigned char *) wg_decode_str(db, enc));
      }
      else {
        show_io_error(db, "Bad field type for subject");
        err = -1;
        goto done;
      }
      triple->subject_type = RAPTOR_IDENTIFIER_TYPE_RESOURCE;

      enc = wg_get_field(db, rec, pref_fields + 2);

      triple->object_literal_language = NULL;
      triple->object_literal_datatype = NULL;
      if(wg_get_encoded_type(db, enc) == WG_URITYPE) {
        triple->object = dburi_to_raptoruri(db, enc);
        triple->object_type = RAPTOR_IDENTIFIER_TYPE_RESOURCE;
      }
      else if(wg_get_encoded_type(db, enc) == WG_XMLLITERALTYPE) {
        triple->object = (void *) raptor_new_uri(
          (unsigned char *) wg_decode_xmlliteral(db, enc));
        triple->object_literal_datatype = raptor_new_uri(
          (unsigned char *) wg_decode_xmlliteral_xsdtype(db, enc));
        triple->object_type = RAPTOR_IDENTIFIER_TYPE_LITERAL;
      }
      else if(wg_get_encoded_type(db, enc) == WG_STRTYPE) {
        triple->object = (void *) wg_decode_str(db, enc);
        triple->object_literal_language =\
          (unsigned char *) wg_decode_str_lang(db, enc);
        triple->object_type = RAPTOR_IDENTIFIER_TYPE_LITERAL;
      }
      else {
        show_io_error(db, "Bad field type for object");
        err = -1;
        goto done;
      }

      /* Write the triple */
      raptor_serialize_statement(rdf_serializer, triple);

      /* Cleanup current triple */
      raptor_free_uri((raptor_uri *) triple->subject);
      raptor_free_uri((raptor_uri *) triple->predicate);
      if(triple->object_type == RAPTOR_IDENTIFIER_TYPE_RESOURCE)
        raptor_free_uri((raptor_uri *) triple->object);
      else if(triple->object_literal_datatype)
        raptor_free_uri((raptor_uri *) triple->object_literal_datatype);
    }
    rec = wg_get_next_record(db, rec);
  }

done:
  raptor_serialize_end(rdf_serializer);
  free(triple);
  return (gint) err;
}


#endif /* HAVE_RAPTOR */

void wg_pretty_print_memsize(gint memsz, char *buf, size_t buflen) {
  if(memsz < 1000) {
    snprintf(buf, buflen-1, "%d bytes", (int) memsz);
  } else if(memsz < 1000000) {
    snprintf(buf, buflen-1, "%d kB", (int) (memsz/1000));
  } else if(memsz < 1000000000) {
    snprintf(buf, buflen-1, "%d MB", (int) (memsz/1000000));
  } else {
    snprintf(buf, buflen-1, "%d GB", (int) (memsz/1000000000));
  }
  buf[buflen-1] = '\0';
}

/* ------------ error handling ---------------- */

static gint show_io_error(void *db, char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"I/O error: %s.\n", errmsg);
#endif
  return -1;
}

static gint show_io_error_str(void *db, char *errmsg, char *str) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"I/O error: %s: %s.\n", errmsg, str);
#endif
  return -1;
}

#ifdef __cplusplus
}
#endif
