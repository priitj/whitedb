/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
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

 /** @file unify.h
 *  Unification functions.
 *
 */
 


#ifndef __defined_unify_h
#define __defined_unify_h

#include "glb.h" 

#define UNASSIGNED WG_ILLEGAL // 0xff in dbata.h

#define VARVAL(x,vb) (wr_varval(x,vb))
#define VARVAL_F(x,vb) (tmp=vb[decode_var(x)], ((tmp==UNASSIGNED) ? x : (!isvar(tmp) ? tmp : wr_varval(tmp,vb)))) 
#define VARVAL_DIRECT(x,vb) (vb[decode_var(x)])
#define SETVAR(x,y,vb,vstk,vc) (vb[decode_var(x)]=y,vstk[*vc]=(gint)((gptr)vb+decode_var(x)),++(*vc))  

// WR_EQUAL_TERM is a faster version of the wr_equal_term function, doing exactly the same thing
// NB! gint eqenc unused variable must be present to call the following macro, as well as valued uniquestrflag

#define WR_EQUAL_TERM(g,x,y,uniquestrflag) \
  (((x)==(y)) ? 1 : \
    (eqencx=(x)&NORMALPTRMASK,\
     (((eqencx==LONGSTRBITS && uniquestrflag) || eqencx==SMALLINTBITS || eqencx==NORMALPTRMASK) ? 0 : \
      ((!isptr(x) || !isptr(y)) ? 0 : \
       ((((x)&NONPTRBITS)!=((y)&NONPTRBITS)) ? 0 : wr_equal_term_macroaux((g),(x),(y),(uniquestrflag)) )))))
         

gint wr_unify_term(glb* g, gint x, gint y, int uniquestrflag);
gint wr_unify_term_aux(glb* g, gint x, gint y, int uniquestrflag);

gint wr_match_term_aux(glb* g, gint x, gint y, int uniquestrflag);
gint wr_match_term(glb* g, gint x, gint y, int uniquestrflag);

gint wr_equal_term(glb* g, gint x, gint y, int uniquestrflag);
gint wr_equal_term_macroaux(glb* g, gint x, gint y, int uniquestrflag);
int wr_equal_ptr_primitives(glb* g, gint a, gint b, int uniquestrflag);

gint wr_varval(gint x, gptr vb);
void wr_setvar(gint x, gint y, gptr vb, gptr vstk, gint* vc);

void wr_clear_varstack(glb* g,vec vs);
void wr_clear_varstack_topslice(glb* g, vec vs, int y);
void wr_clear_all_varbanks(glb* g);

void wr_print_vardata(glb* g);
void wr_print_varbank(glb* g, gptr vb);
void wr_print_varstack(glb* g, gptr vs);
int wr_varbanks_are_clear(glb* g, gptr vb);

#endif
