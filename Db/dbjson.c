/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2013
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

#include "dbdata.h"
#include "dbschema.h"
#include "../json/JSON_parser.h"

#ifdef _WIN32
#define snprintf(s, sz, f, ...) _snprintf_s(s, sz+1, sz, f, ## __VA_ARGS__)
#define strncpy(d, s, sz) strncpy_s(d, sz+1, s, sz)
#endif

#define MAX_DEPTH 20 /* XXX: this is actually related to WG_COMPARE_REC_DEPTH */

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
  int arraylevel;
  int oblevel;
  stack_entry stack[MAX_DEPTH];
  int stack_ptr;
  void *db;
} parser_context;

/* ======== Data ========================= */

/* ======= Private protos ================ */

static int push(parser_context *ctx, stack_entry_t type);
static int pop(parser_context *ctx);
static int add_elem(parser_context *ctx, gint enc);
static int add_key(parser_context *ctx, char *key);
static int add_literal(parser_context *ctx, gint val);

static int parse_json_cb(void* ctx, int type, const JSON_value* value);

/* ====== Functions ============== */

/* Run JSON parser.
 * If parsing is successful, the data is inserted in the database.
 * returns 0 for success.
 * returns -1 on error.
 */
gint wg_parse_json_document(void *db, char *buf) {
  int next_char, count = 0, result = 0;
  JSON_config config;
  struct JSON_parser_struct* jc = NULL;
  char *iptr = buf;
  parser_context ctx;

  init_JSON_config(&config);

  /* setup context */
  ctx.state = 0;
  ctx.arraylevel = 0;
  ctx.oblevel = 0;
  ctx.stack_ptr = -1;
  ctx.db = db;

  /* setup parser */
  config.depth = MAX_DEPTH - 1;
  config.callback = &parse_json_cb;
  config.callback_ctx = &ctx;
  config.allow_comments = 1;
  config.handle_floats_manually = 0;

  jc = new_JSON_parser(&config);

  /* XXX: do an empty pass to check the syntax first */

  while((next_char = *iptr++)) {
    if (!JSON_parser_char(jc, next_char)) {
      fprintf(stderr, "JSON_parser_char: syntax error, byte %d\n", count);
      result = -1;
      goto done;
    }
    count++;
  }
  if (!JSON_parser_done(jc)) {
    fprintf(stderr, "JSON_parser_end: syntax error\n");
    result = -1;
    goto done;
  }

done:
  delete_JSON_parser(jc);
  return result;
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
    rec = wg_create_array(ctx->db, e->size, istoplevel);
  } else {
    rec = wg_create_object(ctx->db, e->size, istoplevel);
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
    ret = add_elem(ctx, enc);
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
    rec = wg_create_kvpair(ctx->db, key, val);
    if(!rec)
      return 0;
    return add_elem(ctx, wg_encode_record(ctx->db, rec));
  }
}

#define OUT_INDENT(x,i) \
      for(i=0; i<x; i++) \
        printf("  "); \

static int parse_json_cb(void* cb_ctx, int type, const JSON_value* value)
{
  int i;
  gint val;
  parser_context *ctx = (parser_context *) cb_ctx;

  switch(type) {
    case JSON_T_ARRAY_BEGIN:
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("BEGIN ARRAY\n");
      if(!push(ctx, ARRAY))
        return 0;
      break;
    case JSON_T_ARRAY_END:
      if(!pop(ctx))
        return 0;
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("END ARRAY\n");
      break;
    case JSON_T_OBJECT_BEGIN:
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("BEGIN object\n");
      if(!push(ctx, OBJECT))
        return 0;
      break;
    case JSON_T_OBJECT_END:
      if(!pop(ctx))
        return 0;
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("END object\n");
      break;
    case JSON_T_INTEGER:
      val = wg_encode_int(ctx->db, value->vu.integer_value);
      if(val == WG_ILLEGAL)
        return 0;
      if(!add_literal(ctx, val))
        return 0;
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("INTEGER: %d\n", value->vu.integer_value);
      break;
    case JSON_T_FLOAT:
      val = wg_encode_double(ctx->db, value->vu.float_value);
      if(val == WG_ILLEGAL)
        return 0;
      if(!add_literal(ctx, val))
        return 0;
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("FLOAT: %.6f\n", value->vu.float_value);
      break;
    case JSON_T_KEY:
      if(!add_key(ctx, (char *) value->vu.str.value))
        return 0;
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("KEY: %s\n", value->vu.str.value);
      break;
    case JSON_T_STRING:
      val = wg_encode_str(ctx->db, (char *) value->vu.str.value, NULL);
      if(val == WG_ILLEGAL)
        return 0;
      if(!add_literal(ctx, val))
        return 0;
      OUT_INDENT(ctx->stack_ptr+1, i)
      printf("STRING: %s\n", value->vu.str.value);
      break;
    default:
      break;
  }
  
  return 1;
}


#ifdef __cplusplus
}
#endif
