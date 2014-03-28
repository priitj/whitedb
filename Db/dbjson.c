/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2013, 2014
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

 /** @file dbjson.c
 * WhiteDB JSON input and output.
 */

/* ====== Includes =============== */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>


/* ====== Private headers and defs ======== */

#ifdef __cplusplus
extern "C" {
#endif

/*#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <malloc.h>
#endif*/

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "dbdata.h"
#include "dbcompare.h"
#include "dbschema.h"
#include "dbjson.h"
#include "dbutil.h"
#include "../json/yajl_api.h"

#ifdef _WIN32
#define strncpy(d, s, sz) strncpy_s(d, sz+1, s, sz)
#define strnlen strnlen_s
#endif

#ifdef USE_BACKLINKING
#if !defined(WG_COMPARE_REC_DEPTH) || (WG_COMPARE_REC_DEPTH < 2)
#error WG_COMPARE_REC_DEPTH not defined or too small
#else
#define MAX_DEPTH WG_COMPARE_REC_DEPTH
#endif
#else /* !USE_BACKLINKING */
#define MAX_DEPTH 99 /* no reason to limit */
#endif

/* Commenting this out allows parsing literal value in input, but
 * the current code lacks the capability of representing them
 * (what record should they be stored in?) so there would be
 * no obvious benefit.
 */
#define CHECK_TOPLEVEL_STRUCTURE

typedef enum { ARRAY, OBJECT } stack_entry_t;

struct __stack_entry_elem {
  gint enc;
  struct __stack_entry_elem *next;
};

typedef struct __stack_entry_elem stack_entry_elem;

typedef struct {
  stack_entry_t type;
  stack_entry_elem *head;
  stack_entry_elem *tail;
  char last_key[80];
  int size;
} stack_entry;

typedef struct {
  int state;
  stack_entry stack[MAX_DEPTH];
  int stack_ptr;
  void *db;
  int isparam;
  int isdocument;
  void **document;
} parser_context;

/* ======= Private protos ================ */

static int push(parser_context *ctx, stack_entry_t type);
static int pop(parser_context *ctx);
static int add_elem(parser_context *ctx, gint enc);
static int add_key(parser_context *ctx, char *key);
static int add_literal(parser_context *ctx, gint val);

static gint run_json_parser(void *db, char *buf,
  yajl_callbacks *cb, int isparam, int isdocument, void **document);
static int check_push_cb(void* cb_ctx);
static int check_pop_cb(void* cb_ctx);
static int array_begin_cb(void* cb_ctx);
static int array_end_cb(void* cb_ctx);
static int object_begin_cb(void* cb_ctx);
static int object_end_cb(void* cb_ctx);
static int elem_integer_cb(void* cb_ctx, long long intval);
static int elem_double_cb(void* cb_ctx, double doubleval);
static int object_key_cb(void* cb_ctx, const unsigned char * strval,
                           size_t strl);
static int elem_string_cb(void* cb_ctx, const unsigned char * strval,
                           size_t strl);
static void print_cb(void *cb_ctx, const char *str, size_t len);
static int pretty_print_json(void *db, yajl_gen *g, void *rec);
static int pretty_print_jsonval(void *db, yajl_gen *g, gint enc);

static gint show_json_error(void *db, char *errmsg);
static gint show_json_error_fn(void *db, char *errmsg, char *filename);
static gint show_json_error_byte(void *db, char *errmsg, int byte);

/* ======== Data ========================= */

yajl_callbacks validate_cb = {
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    check_push_cb,
    NULL,
    check_pop_cb,
    check_push_cb,
    check_pop_cb
};

yajl_callbacks input_cb = {
    NULL,
    NULL,
    elem_integer_cb,
    elem_double_cb,
    NULL,
    elem_string_cb,
    object_begin_cb,
    object_key_cb,
    object_end_cb,
    array_begin_cb,
    array_end_cb
};


/* ====== Functions ============== */

/**
 * Parse an input file. Does an initial pass to verify the syntax
 * of the input and passes it on to the document parser.
 * XXX: caches the data in memory, so this is very unsuitable
 * for large files. An alternative would be to feed bytes directly
 * to the document parser and roll the transaction back, if something fails;
 */
#define WG_JSON_INPUT_CHUNK 16384

gint wg_parse_json_file(void *db, char *filename) {
  char *buf = NULL;
  FILE *f = NULL;
  int count = 0, result = 0, bufsize = 0, depth = -1;
  yajl_handle hand = NULL;

  buf = malloc(WG_JSON_INPUT_CHUNK);
  if(!buf) {
    return show_json_error(db, "Failed to allocate memory");
  }
  bufsize = WG_JSON_INPUT_CHUNK;

  if(!filename) {
#ifdef _WIN32
    printf("reading JSON from stdin, press CTRL-Z and ENTER when done\n");
#else
    printf("reading JSON from stdin, press CTRL-D when done\n");
#endif
    fflush(stdout);
    f = stdin;
  } else {
#ifdef _WIN32
    if(fopen_s(&f, filename, "r")) {
#else
    if(!(f = fopen(filename, "r"))) {
#endif
      show_json_error_fn(db, "Failed to open input", filename);
      result = -1;
      goto done;
    }
  }

  /* setup parser */
  hand = yajl_alloc(&validate_cb, NULL, (void *) &depth);
  yajl_config(hand, yajl_allow_comments, 1);

  while(!feof(f)) {
    int rd = fread((void *) &buf[count], 1, WG_JSON_INPUT_CHUNK, f);
    if(rd == 0) {
      if(!feof(f)) {
        show_json_error_byte(db, "Read error", count);
        result = -1;
      }
      goto done;
    }
    if(yajl_parse(hand, (unsigned char *) &buf[count], rd) != yajl_status_ok) {
      unsigned char *errtxt = yajl_get_error(hand, 1,
        (unsigned char *) &buf[count], rd);
      show_json_error(db, (char *) errtxt);
      yajl_free_error(hand, errtxt);
      result = -1;
      goto done;
    }
    count += rd;
    if(count >= bufsize) {
      void *tmp = realloc(buf, bufsize + WG_JSON_INPUT_CHUNK);
      if(!tmp) {
        show_json_error(db, "Failed to allocate additional memory");
        result = -1;
        goto done;
      }
      buf = tmp;
      bufsize += WG_JSON_INPUT_CHUNK;
    }
  }
  if(yajl_complete_parse(hand) != yajl_status_ok) {
    show_json_error(db, "Syntax error (JSON not properly terminated?)");
    result = -1;
    goto done;
  }

#ifdef CHECK_TOPLEVEL_STRUCTURE
  if(depth == -1) {
    show_json_error(db, "Top-level array or object is required in JSON");
    result = -1;
    goto done;
  }
#endif


  buf[count] = '\0';
  result = wg_parse_json_document(db, buf, NULL);

done:
  if(buf) free(buf);
  if(filename && f) fclose(f);
  if(hand) yajl_free(hand);
  return result;
}

/* Validate JSON data in a string buffer.
 * Does not insert data into the database, so this may be used
 * as a first pass before calling the wg_parse_*() functions.
 *
 * returns 0 for success.
 * returns -1 in case of a syntax error.
 */
gint wg_check_json(void *db, char *buf) {
  int count = 0, result = 0, depth = -1;
  char *iptr = buf;
  yajl_handle hand = NULL;

#ifdef CHECK
  if(!buf)
    return show_json_error(db, "Invalid input buffer");
#endif

  /* setup parser */
  hand = yajl_alloc(&validate_cb, NULL, (void *) &depth);
  yajl_config(hand, yajl_allow_comments, 1);

  while((count = strnlen(iptr, WG_JSON_INPUT_CHUNK)) > 0) {
    if(yajl_parse(hand, (unsigned char *) iptr, count) != yajl_status_ok) {
      show_json_error(db, "JSON parsing failed");
      result = -1;
      goto done;
    }
    iptr += count;
  }

  if(yajl_complete_parse(hand) != yajl_status_ok) {
    show_json_error(db, "JSON parsing failed");
    result = -1;
  }
#ifdef CHECK_TOPLEVEL_STRUCTURE
  else if(depth == -1) {
    show_json_error(db, "Top-level array or object is required in JSON");
    result = -1;
  }
#endif

done:
  if(hand) yajl_free(hand);
  return result;
}

/* Parse a JSON buffer.
 * The data is inserted in database using the JSON schema.
 * If parsing is successful, the pointer referred to by
 * **document will point to the top-level record.
 * If **document is NULL, the pointer is discarded.
 *
 * returns 0 for success.
 * returns -1 on non-fatal error.
 * returns -2 if database is left non-consistent due to an error.
 */
gint wg_parse_json_document(void *db, char *buf, void **document) {
  void *rec = NULL;
  gint retv = run_json_parser(db, buf, &input_cb, 0, 1, &rec);
  if(document)
    *document = rec;
  return retv;
}

/* Parse a JSON buffer.
 * Like wg_parse_json_document, except the top-level object or
 * array is not marked as a document.
 *
 * returns 0 for success.
 * returns -1 on non-fatal error.
 * returns -2 if database is left non-consistent due to an error.
 */
gint wg_parse_json_fragment(void *db, char *buf, void **document) {
  void *rec = NULL;
  gint retv = run_json_parser(db, buf, &input_cb, 0, 0, &rec);
  if(document)
    *document = rec;
  return retv;
}

/* Parse a JSON parameter(s).
 * The data is inserted in database as "special" records.
 * It does not make sense to call this function with NULL as the
 * third parameter, as that would imply data input semantics but
 * the records generated here are speficially flagged *non-data*.
 *
 * returns 0 for success.
 * returns -1 on non-fatal error.
 * returns -2 if database is left non-consistent due to an error.
 */
gint wg_parse_json_param(void *db, char *buf, void **document) {
  if(!document) {
    return show_json_error(db, "wg_parse_json_param: arg 3 cannot be NULL");
  }
  return run_json_parser(db, buf, &input_cb, 1, 1, document);
}

/* Run JSON parser.
 * The data is inserted in the database. If there are any errors, the
 * database will currently remain in an inconsistent state, so beware.
 *
 * if isparam is specified, the data will not be indexed nor returned
 * by wg_get_*_record() calls.
 *
 * if isdocument is 0, the input will be treated as a fragment and
 * not as a full document.
 *
 * if the call is successful, *document contains a pointer to the
 * top-level record.
 *
 * returns 0 for success.
 * returns -1 on non-fatal error.
 * returns -2 if database is left non-consistent due to an error.
 */
static gint run_json_parser(void *db, char *buf,
  yajl_callbacks *cb, int isparam, int isdocument, void **document)
{
  int count = 0, result = 0;
  yajl_handle hand = NULL;
  char *iptr = buf;
  parser_context ctx;

  /* setup context */
  ctx.state = 0;
  ctx.stack_ptr = -1;
  ctx.db = db;
  ctx.isparam = isparam;
  ctx.isdocument = isdocument;
  ctx.document = document;

  /* setup parser */
  hand = yajl_alloc(cb, NULL, (void *) &ctx);
  yajl_config(hand, yajl_allow_comments, 1);

  while((count = strnlen(iptr, WG_JSON_INPUT_CHUNK)) > 0) {
    if(yajl_parse(hand, (unsigned char *) iptr, count) != yajl_status_ok) {
      show_json_error(db, "JSON parsing failed");
      result = -2; /* Fatal error */
      goto done;
    }
    iptr += count;
  }

  if(yajl_complete_parse(hand) != yajl_status_ok) {
    show_json_error(db, "JSON parsing failed");
    result = -2; /* Fatal error */
  }

done:
  if(hand) yajl_free(hand);
  return result;
}

static int check_push_cb(void* cb_ctx)
{
  int *depth = (int *) cb_ctx;
  if(*depth == -1) *depth = 0; /* hack: something was pushed */
  if(++(*depth) >= MAX_DEPTH) {
    return 0;
  }
  return 1;
}

static int check_pop_cb(void* cb_ctx)
{
  int *depth = (int *) cb_ctx;
  --(*depth);
  return 1;
}

/**
 * Push an object or an array on the stack.
 */
static int push(parser_context *ctx, stack_entry_t type)
{
  stack_entry *e;
  if(++ctx->stack_ptr >= MAX_DEPTH) /* paranoia, parser guards from this */
    return 0;
  e = &ctx->stack[ctx->stack_ptr];
  e->size = 0;
  e->type = type;
  e->head = NULL;
  e->tail = NULL;
  return 1;
}

/**
 * Pop an object or an array from the stack.
 * If this is not the top level in the document, the object is also added
 * as an element on the previous level.
 */
static int pop(parser_context *ctx)
{
  stack_entry *e;
  void *rec;
  int ret, istoplevel;

  if(ctx->stack_ptr < 0)
    return 0;
  e = &ctx->stack[ctx->stack_ptr--];

  /* is it a top level object? */
  if(ctx->stack_ptr < 0) {
    istoplevel = 1;
  } else {
    istoplevel = 0;
  }

  if(e->type == ARRAY) {
    rec = wg_create_array(ctx->db, e->size,
      (istoplevel && ctx->isdocument), ctx->isparam);
  } else {
    rec = wg_create_object(ctx->db, e->size,
      (istoplevel && ctx->isdocument), ctx->isparam);
  }

  /* add elements to the database */
  if(rec) {
    stack_entry_elem *curr = e->head;
    int i = 0;
    ret = 1;
    while(curr) {
      if(wg_set_field(ctx->db, rec, i++, curr->enc)) {
        ret = 0;
        break;
      }
      curr = curr->next;
    }
    if(istoplevel)
      *(ctx->document) = rec;
  } else {
    ret = 0;
  }

  /* free the elements */
  while(e->head) {
    stack_entry_elem *tmp = e->head;
    e->head = e->head->next;
    free(tmp);
  }
  e->tail = NULL;
  e->size = 0;

  /* is it an element of something? */
  if(!istoplevel && rec && ret) {
    gint enc = wg_encode_record(ctx->db, rec);
    ret = add_literal(ctx, enc);
  }
  return ret;
}

/**
 * Append an element to the current stack entry.
 */
static int add_elem(parser_context *ctx, gint enc)
{
  stack_entry *e;
  stack_entry_elem *tmp;

  if(ctx->stack_ptr < 0 || ctx->stack_ptr >= MAX_DEPTH)
    return 0; /* paranoia */

  e = &ctx->stack[ctx->stack_ptr];
  tmp = (stack_entry_elem *) malloc(sizeof(stack_entry_elem));
  if(!tmp)
    return 0;

  if(!e->tail) {
    e->head = tmp;
  } else {
    e->tail->next = tmp;
  }
  e->tail = tmp;
  e->size++;
  tmp->enc = enc;
  tmp->next = NULL;
  return 1;
}

/**
 * Store a key in the current stack entry.
 */
static int add_key(parser_context *ctx, char *key)
{
  stack_entry *e;

  if(ctx->stack_ptr < 0 || ctx->stack_ptr >= MAX_DEPTH)
    return 0; /* paranoia */

  e = &ctx->stack[ctx->stack_ptr];
  strncpy(e->last_key, key, 80);
  e->last_key[79] = '\0';
  return 1;
}

/**
 * Add a literal value. If it's inside an object, generate
 * a key-value pair using the last key. Otherwise insert
 * it directly.
 */
static int add_literal(parser_context *ctx, gint val)
{
  stack_entry *e;

  if(ctx->stack_ptr < 0 || ctx->stack_ptr >= MAX_DEPTH)
    return 0; /* paranoia */

  e = &ctx->stack[ctx->stack_ptr];
  if(e->type == ARRAY) {
    return add_elem(ctx, val);
  } else {
    void *rec;
    gint key = wg_encode_str(ctx->db, e->last_key, NULL);
    if(key == WG_ILLEGAL)
      return 0;
    rec = wg_create_kvpair(ctx->db, key, val, ctx->isparam);
    if(!rec)
      return 0;
    return add_elem(ctx, wg_encode_record(ctx->db, rec));
  }
}

#define OUT_INDENT(x,i,f) \
      for(i=0; i<x; i++) \
        fprintf(f, "  ");

static int array_begin_cb(void* cb_ctx)
{
/*  int i;*/
  parser_context *ctx = (parser_context *) cb_ctx;
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("BEGIN ARRAY\n");*/
  if(!push(ctx, ARRAY))
    return 0;
  return 1;
}

static int array_end_cb(void* cb_ctx)
{
/*  int i;*/
  parser_context *ctx = (parser_context *) cb_ctx;
  if(!pop(ctx))
    return 0;
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("END ARRAY\n");*/
  return 1;
}

static int object_begin_cb(void* cb_ctx)
{
/*  int i;*/
  parser_context *ctx = (parser_context *) cb_ctx;
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("BEGIN object\n");*/
  if(!push(ctx, OBJECT))
    return 0;
  return 1;
}

static int object_end_cb(void* cb_ctx)
{
/*  int i;*/
  parser_context *ctx = (parser_context *) cb_ctx;
  if(!pop(ctx))
    return 0;
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("END object\n");*/
  return 1;
}

static int elem_integer_cb(void* cb_ctx, long long intval)
{
/*  int i;*/
  gint val;
  parser_context *ctx = (parser_context *) cb_ctx;
  val = wg_encode_int(ctx->db, (gint) intval);
  if(val == WG_ILLEGAL)
    return 0;
  if(!add_literal(ctx, val))
    return 0;
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("INTEGER: %d\n", (int) intval);*/
  return 1;
}

static int elem_double_cb(void* cb_ctx, double doubleval)
{
/*  int i;*/
  gint val;
  parser_context *ctx = (parser_context *) cb_ctx;
  val = wg_encode_double(ctx->db, doubleval);
  if(val == WG_ILLEGAL)
    return 0;
  if(!add_literal(ctx, val))
    return 0;
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("FLOAT: %.6f\n", doubleval);*/
  return 1;
}

static int object_key_cb(void* cb_ctx, const unsigned char * strval,
                           size_t strl)
{
/*  int i;*/
  int res = 1;
  parser_context *ctx = (parser_context *) cb_ctx;
  char *buf = malloc(strl + 1);
  if(!buf)
    return 0;
  strncpy(buf, (char *) strval, strl);
  buf[strl] = '\0';

  if(!add_key(ctx, buf)) {
    res = 0;
  }
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("KEY: %s\n", buf);*/
  free(buf);
  return res;
}

static int elem_string_cb(void* cb_ctx, const unsigned char * strval,
                           size_t strl)
{
/*  int i;*/
  int res = 1;
  gint val;
  parser_context *ctx = (parser_context *) cb_ctx;
  char *buf = malloc(strl + 1);
  if(!buf)
    return 0;
  strncpy(buf, (char *) strval, strl);
  buf[strl] = '\0';

  val = wg_encode_str(ctx->db, buf, NULL);

  if(val == WG_ILLEGAL) {
    res = 0;
  } else if(!add_literal(ctx, val)) {
    res = 0;
  }
/*  OUT_INDENT(ctx->stack_ptr+1, i, stdout)
  printf("STRING: %s\n", buf);*/
  free(buf);
  return res;
}

static void print_cb(void *cb_ctx, const char *str, size_t len)
{
  FILE *f = (FILE *) cb_ctx;
  fwrite(str, len, 1, f);
}

/*
 * Print a JSON document. If a callback is given, it
 * should be of type (void) (void *, char *, size_t) where the first
 * pointer will be cast to FILE * stream. Otherwise the document will
 * be written to stdout.
 */
void wg_print_json_document(void *db, void *cb, void *cb_ctx, void *document) {
  yajl_gen g;
  if(!is_schema_document(document)) {
    /* Paranoia check. This increases the probability we're dealing
     * with records belonging to a proper schema. Omitting this check
     * would allow printing parts of documents as well.
     */
    show_json_error(db, "Given record is not a document");
    return;
  }
  g = yajl_gen_alloc(NULL);
  yajl_gen_config(g, yajl_gen_beautify, 1);
  if(cb) {
    yajl_gen_config(g, yajl_gen_print_callback, (yajl_print_t *) cb, cb_ctx);
  } else {
    yajl_gen_config(g, yajl_gen_print_callback, print_cb, (void *) stdout);
  }
  pretty_print_json(db, &g, document);
  yajl_gen_free(g);
}

/*
 * Recursively print JSON elements (using the JSON schema)
 * Returns 0 on success
 * Returns -1 on error.
 */
static int pretty_print_json(void *db, yajl_gen *g, void *rec)
{
  if(is_schema_object(rec)) {
    gint i, reclen;

    if(yajl_gen_map_open(*g) != yajl_gen_status_ok) {
      return show_json_error(db, "Formatter failure");
    }

    reclen = wg_get_record_len(db, rec);
    for(i=0; i<reclen; i++) {
      gint enc;
      enc = wg_get_field(db, rec, i);
      if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
        return show_json_error(db, "Object had an element of invalid type");
      }
      if(pretty_print_json(db, g, wg_decode_record(db, enc))) {
        return -1;
      }
    }

    if(yajl_gen_map_close(*g) != yajl_gen_status_ok) {
      return show_json_error(db, "Formatter failure");
    }
  }
  else if(is_schema_array(rec)) {
    gint i, reclen;

    if(yajl_gen_array_open(*g) != yajl_gen_status_ok) {
      return show_json_error(db, "Formatter failure");
    }

    reclen = wg_get_record_len(db, rec);
    for(i=0; i<reclen; i++) {
      gint enc;
      enc = wg_get_field(db, rec, i);
      if(pretty_print_jsonval(db, g, enc)) {
        return -1;
      }
    }

    if(yajl_gen_array_close(*g) != yajl_gen_status_ok) {
      return show_json_error(db, "Formatter failure");
    }
  }
  else {
    /* assume key-value pair */
    gint key, value;
    key = wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET);
    value = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);

    if(wg_get_encoded_type(db, key) != WG_STRTYPE) {
      return show_json_error(db, "Key is of invalid type");
    } else {
      int len = wg_decode_str_len(db, key);
      char *buf = wg_decode_str(db, key);
      if(buf) {
        if(yajl_gen_string(*g, (unsigned char *) buf,
          (size_t) len) != yajl_gen_status_ok) {
          return show_json_error(db, "Formatter failure");
        }
      }
    }
    if(pretty_print_jsonval(db, g, value)) {
      return -1;
    }
  }
  return 0;
}

/*
 * Print a JSON array element or object value.
 * May be an array or object itself.
 */
static int pretty_print_jsonval(void *db, yajl_gen *g, gint enc)
{
  gint type = wg_get_encoded_type(db, enc);
  if(type == WG_RECORDTYPE) {
    if(pretty_print_json(db, g, wg_decode_record(db, enc))) {
      return -1;
    }
  } else if(type == WG_STRTYPE) {
    int len = wg_decode_str_len(db, enc);
    char *buf = wg_decode_str(db, enc);
    if(buf) {
      if(yajl_gen_string(*g, (unsigned char *) buf,
        (size_t) len) != yajl_gen_status_ok) {
        return show_json_error(db, "Formatter failure");
      }
    }
  } else {
    /* other literal value */
    size_t len;
    char buf[80];
    wg_snprint_value(db, enc, buf, 79);
    len = strlen(buf);
    if(type == WG_INTTYPE || type == WG_DOUBLETYPE ||\
      type == WG_FIXPOINTTYPE) {
      if(yajl_gen_number(*g, buf, len) != yajl_gen_status_ok) {
        return show_json_error(db, "Formatter failure");
      }
    } else {
      if(yajl_gen_string(*g, (unsigned char *) buf,
        len) != yajl_gen_status_ok) {
        return show_json_error(db, "Formatter failure");
      }
    }
  }
  return 0;
}

/* ------------ error handling ---------------- */

static gint show_json_error(void *db, char *errmsg) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg json I/O error: %s.\n", errmsg);
#endif
  return -1;
}

static gint show_json_error_fn(void *db, char *errmsg, char *filename) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg json I/O error: %s (file=`%s`)\n", errmsg, filename);
#endif
  return -1;
}

static gint show_json_error_byte(void *db, char *errmsg, int byte) {
#ifdef WG_NO_ERRPRINT
#else
  fprintf(stderr,"wg json I/O error: %s (byte=%d)\n", errmsg, byte);
#endif
  return -1;
}

#ifdef __cplusplus
}
#endif
