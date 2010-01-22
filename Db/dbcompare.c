/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
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

 /** @file dbcompare.c
 * Data comparison functions.
 */

/* ====== Includes =============== */

#include <string.h>
#include "dbdata.h"

/* ====== Private headers and defs ======== */

#include "dbcompare.h"

/* ====== Functions ============== */

/** Compare two encoded values
 * a, b - encoded values
 * returns WG_GREATER, WG_EQUAL or WG_LESSTHAN
 * assumes that a and b themselves are not equal and so
 * their decoded values need to be examined (which could still
 * be equal for some data types).
 */
gint wg_compare(void *db, gint a, gint b) {
/* a very simplistic version of the function:
 * - we get the types of the variables
 * - if the types match, compare the decoded values
 * - otherwise compare the type codes (not really scientific,
 *   but will provide a means of ordering values).
 */
  /* XXX: might be able to save time here to mask and compare
   * the type bits instead */
  gint typea = wg_get_encoded_type(db, a);
  gint typeb = wg_get_encoded_type(db, b);

  /* assume types are >2 (NULLs are always equal) and
   * <13 (not implemented as of now)
   * XXX: all of this will fall apart if type codes
   * are somehow rearranged :-) */
  if(typeb==typea) {
    if(typea>WG_CHARTYPE) { /* > 9, not a string */
      if(typea>WG_FIXPOINTTYPE) {
        /* date or time. Compare decoded gints */
        gint deca, decb;
        if(typea==WG_DATETYPE) {
          deca = wg_decode_date(db, a);
          decb = wg_decode_date(db, b);
        } else {
          deca = wg_decode_time(db, a);
          decb = wg_decode_time(db, b);
        }
        return (deca>decb ? WG_GREATER : WG_LESSTHAN);
      } else {
        /* fixpoint, need to compare doubles */
        double deca, decb;
        deca = wg_decode_fixpoint(db, a);
        decb = wg_decode_fixpoint(db, b);
        return (deca>decb ? WG_GREATER : WG_LESSTHAN);
      }
    }
    else if(typea<WG_STRTYPE) { /* < 5, still not a string */
      if(typea==WG_RECORDTYPE) {
        void *deca, *decb;
        deca = wg_decode_record(db, a);
        decb = wg_decode_record(db, b);
        /* could also compare record length here or whatever */
        return ((int) deca> (int) decb ? WG_GREATER : WG_LESSTHAN);
      }
      else if(typea==WG_INTTYPE) {
        gint deca, decb;
        deca = wg_decode_int(db, a);
        decb = wg_decode_int(db, b);
        /* could also compare record length here or whatever */
        return (deca>decb ? WG_GREATER : WG_LESSTHAN);
      } else {
        /* WG_DOUBLETYPE */
        double deca, decb;
        deca = wg_decode_double(db, a);
        decb = wg_decode_double(db, b);
        return (deca>decb ? WG_GREATER : WG_LESSTHAN);
      }
    }
    else { /* string */
      /* need to compare the characters. */
      char *deca, *decb;
      gint lena, lenb;
      if(typea==WG_STRTYPE) {
        deca = wg_decode_str(db, a);
        decb = wg_decode_str(db, b);
        lena = wg_decode_str_len(db, a);
        lenb = wg_decode_str_len(db, b);
      }
      else if(typea==WG_URITYPE) {
        /* XXX: this is quite broken, as we're not looking at prefix */
        deca = wg_decode_uri(db, a);
        decb = wg_decode_uri(db, b);
        lena = wg_decode_uri_len(db, a);
        lenb = wg_decode_uri_len(db, b);
      }
      else if(typea==WG_XMLLITERALTYPE) {
        /* see comment for URI type */
        deca = wg_decode_xmlliteral(db, a);
        decb = wg_decode_xmlliteral(db, b);
        lena = wg_decode_xmlliteral_len(db, a);
        lenb = wg_decode_xmlliteral_len(db, b);
      }
      else { /* WG_BLOBTYPE */
        /* XXX: it's probably OK to ignore BLOB type */
        deca = wg_decode_blob(db, a);
        decb = wg_decode_blob(db, b);
        lena = wg_decode_blob_len(db, a);
        lenb = wg_decode_blob_len(db, b);
      }
      if(lena>lenb) {
        if(memcmp(deca, decb, lenb) < 0) return WG_LESSTHAN;
        else return WG_GREATER; /* if the compared area was
                               equal, a wins by being longer */
      }
      else if(lenb>lena) {
        if(memcmp(deca, decb, lena) > 0) return WG_GREATER;
        else return WG_LESSTHAN;
      }
      else { /* lenghts are equal, so all 3 outcomes are possible */
        gint res = memcmp(deca, decb, lena);
        if(res > 0) return WG_GREATER;
        else if(res < 0) return WG_LESSTHAN;
        else return WG_EQUAL;
      }
    }
  }
  else
    return (typea>typeb ? WG_GREATER : WG_LESSTHAN);
}
