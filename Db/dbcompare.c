/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010,2011,2012,2013
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

 /** @file dbcompare.c
 * Data comparison functions.
 */

/* ====== Includes =============== */

#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

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
 * depth - recursion depth for records
 */
gint wg_compare(void *db, gint a, gint b, int depth) {
/* a very simplistic version of the function:
 * - we get the types of the variables
 * - if the types match, compare the decoded values
 * - otherwise compare the type codes (not really scientific,
 *   but will provide a means of ordering values).
 *
 * One important point that should be observed here is
 * that the returned values should be consistent when
 * comparing A to B and then B to A. This applies to cases
 * where we have no reason to think one is greater than
 * the other from the *user's* point of view, but for use
 * in T-tree index and similar, values need to be consistently
 * ordered. Examples include unknown types and record pointers
 * (once recursion depth runs out).
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
        } else if(typea==WG_TIMETYPE) {
          deca = wg_decode_time(db, a);
          decb = wg_decode_time(db, b);
        } else if(typea==WG_VARTYPE) {
          deca = wg_decode_var(db, a);
          decb = wg_decode_var(db, b);
        } else {
          /* anon const or other new type, no idea how to compare */
          return (a>b ? WG_GREATER : WG_LESSTHAN);
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

        if(!depth) {
          /* No more recursion allowed and pointers aren't equal.
           * So while we're technically comparing the addresses here,
           * the main point is that the returned value != WG_EQUAL
           */
          return ((gint) deca> (gint) decb ? WG_GREATER : WG_LESSTHAN);
        }
        else {
          int i;
#ifdef USE_CHILD_DB
          void *parenta, *parentb;
#endif
          int lena = wg_get_record_len(db, deca);
          int lenb = wg_get_record_len(db, decb);

#ifdef USE_CHILD_DB
          /* Determine, if the records are inside the memory area beloning
           * to our current base address. If it is outside, the encoded
           * values inside the record contain offsets in relation to
           * a different base address and need to be translated.
           */
          parenta = wg_get_rec_owner(db, deca);
          parentb = wg_get_rec_owner(db, decb);
#endif

          /* XXX: Currently we're considering records of differing lengths
           * non-equal without comparing the elements
           */
          if(lena!=lenb)
            return (lena>lenb ? WG_GREATER : WG_LESSTHAN);

          /* Recursively check each element in the record. If they
           * are not equal, break and return with the obtained value
           */
          for(i=0; i<lena; i++) {
            gint elema = wg_get_field(db, deca, i);
            gint elemb = wg_get_field(db, decb, i);

#ifdef USE_CHILD_DB
            if(parenta != dbmemseg(db)) {
              elema = wg_translate_hdroffset(db, parenta, elema);
            }
            if(parentb != dbmemseg(db)) {
              elemb = wg_translate_hdroffset(db, parentb, elemb);
            }
#endif

            if(elema != elemb) {
              gint cr = wg_compare(db, elema, elemb, depth - 1);
              if(cr != WG_EQUAL)
                return cr;
            }
          }
          return WG_EQUAL; /* all elements matched */
        }
      }
      else if(typea==WG_INTTYPE) {
        gint deca, decb;
        deca = wg_decode_int(db, a);
        decb = wg_decode_int(db, b);
        if(deca==decb) return WG_EQUAL; /* large ints can be equal */
        return (deca>decb ? WG_GREATER : WG_LESSTHAN);
      } else {
        /* WG_DOUBLETYPE */
        double deca, decb;
        deca = wg_decode_double(db, a);
        decb = wg_decode_double(db, b);
        if(deca==decb) return WG_EQUAL; /* decoded doubles can be equal */
        return (deca>decb ? WG_GREATER : WG_LESSTHAN);
      }
    }
    else { /* string */
      /* Need to compare the characters. In case of 0-terminated
       * strings we use strcmp() directly, which in glibc is heavily
       * optimised. In case of blob type we need to query the length
       * and use memcmp().
       */
      char *deca, *decb, *exa=NULL, *exb=NULL;
      char buf[4];
      gint res;
      if(typea==WG_STRTYPE) {
        /* lang is ignored */
        deca = wg_decode_str(db, a);
        decb = wg_decode_str(db, b);
      }
      else if(typea==WG_URITYPE) {
        exa = wg_decode_uri_prefix(db, a);
        exb = wg_decode_uri_prefix(db, b);
        deca = wg_decode_uri(db, a);
        decb = wg_decode_uri(db, b);
      }
      else if(typea==WG_XMLLITERALTYPE) {
        exa = wg_decode_xmlliteral_xsdtype(db, a);
        exb = wg_decode_xmlliteral_xsdtype(db, b);
        deca = wg_decode_xmlliteral(db, a);
        decb = wg_decode_xmlliteral(db, b);
      }
      else if(typea==WG_CHARTYPE) {
        buf[0] = wg_decode_char(db, a);
        buf[1] = '\0';
        buf[2] = wg_decode_char(db, b);
        buf[3] = '\0';
        deca = buf;
        decb = &buf[2];
      }
      else { /* WG_BLOBTYPE */
        deca = wg_decode_blob(db, a);
        decb = wg_decode_blob(db, b);
      }

      if(exa || exb) {
        /* String type where extra information is significant
         * (we're ignoring this for plain strings and blobs).
         * If extra part is equal, normal comparison continues. If
         * one string is missing altogether, it is considered to be
         * smaller than the other string.
         */
        if(!exb) {
          if(exa[0])
            return WG_GREATER;
        } else if(!exa) {
          if(exb[0])
            return WG_LESSTHAN;
        } else {
          res = strcmp(exa, exb);
          if(res > 0) return WG_GREATER;
          else if(res < 0) return WG_LESSTHAN;
        }
      }

#if 0 /* paranoia check */
      if(!deca || !decb) {
        if(decb)
          if(decb[0])
            return WG_LESSTHAN;
        } else if(deca) {
          if(deca[0])
            return WG_GREATER;
        }
        return WG_EQUAL;
      }
#endif

      if(typea==WG_BLOBTYPE) {
        /* Blobs are not 0-terminated */
        int lena = wg_decode_blob_len(db, a);
        int lenb = wg_decode_blob_len(db, b);
        res = memcmp(deca, decb, (lena < lenb ? lena : lenb));
        if(!res) res = lena - lenb;
      } else {
        res = strcmp(deca, decb);
      }
      if(res > 0) return WG_GREATER;
      else if(res < 0) return WG_LESSTHAN;
      else return WG_EQUAL;
    }
  }
  else
    return (typea>typeb ? WG_GREATER : WG_LESSTHAN);
}

#ifdef __cplusplus
}
#endif
