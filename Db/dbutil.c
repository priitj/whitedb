/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
*
* Minor mods by Tanel Tammet
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

 /** @file dbutil.c
 * Miscellaneous utility functions.
 */

/* ====== Includes =============== */

#include <stdio.h>
#include "dbdata.h"

/* ====== Private headers and defs ======== */

#include "dbutil.h"

#ifdef _WIN32
#define snprintf(s, sz, f, ...) _snprintf_s(s, sz+1, sz, f, ## __VA_ARGS__)
#endif

/* ====== Functions ============== */

/** Print contents of database.
 * 
 */

void wg_print_db(void *db) {
  void *rec;
  
  rec = wg_get_first_record(db);
  do{    
    wg_print_record(db,rec);
    printf("\n");   
    rec = wg_get_next_record(db,rec);    
  } while(rec);
}

/** Print single record
 *
 */
void wg_print_record(void *db, wg_int* rec) {

  wg_int len, enc;
  int i;
  char strbuf[256];
  
  if (rec==NULL) {
    printf("<null rec pointer>\n");
    return;
  }  
  len = wg_get_record_len(db, rec);
  printf("[");
  for(i=0; i<len; i++) {
    if(i) printf(",");
    enc = wg_get_field(db, rec, i);
    wg_snprint_value(db, enc, strbuf, 255);
    printf(strbuf);
  }
  printf("]");
}

/** Print a single, encoded value
 *  The value is written into a character buffer.
 */
void wg_snprint_value(void *db, gint enc, char *buf, int buflen) {
  int intdata;
  char *strdata;
  double doubledata;
  char strbuf[80];

  buflen--; /* snprintf adds '\0' */
  switch(wg_get_encoded_type(db, enc)) {
    case WG_NULLTYPE:
      snprintf(buf, buflen, "NULL");
      break;
    case WG_RECORDTYPE:
      intdata = (int) wg_decode_record(db, enc);
      snprintf(buf, buflen, "<record at %x>", intdata);
      wg_print_record(db,(wg_int*)intdata);
      break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      snprintf(buf, buflen, "%d", intdata);
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      snprintf(buf, buflen, "%f", doubledata);
      break;
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      snprintf(buf, buflen, "\"%s\"", strdata);
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
    default:
      snprintf(buf, buflen, "<unsupported type>");
      break;
  }
}
