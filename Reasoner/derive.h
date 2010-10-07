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

 /** @file derive.h
 * Headers for clause derivation functions. 
 */


#ifndef __defined_derive_h
#define __defined_derive_h

/* ==== Includes ==== */

#include "types.h"
#include "glb.h"

/* ==== Global defines ==== */

/* ==== Protos ==== */

void wr_process_resolve_result(glb* g, gint xatom, gptr xcl, gint yatom, gptr ycl);
int wr_process_resolve_result_isrulecl(glb* g, gptr rptr, int rpos);
void wr_process_resolve_result_setupsubst(glb* g);
void wr_process_resolve_result_setupquecopy(glb* g);
void wr_process_resolve_result_cleanupsubst(glb* g);
int wr_process_resolve_result_aux
      (glb* g, gptr cl, gint cutatom, int atomnr, gptr rptr, int* rpos);

// void resolve_binary_all_active(gptr cl1);

/*
void resolve_binary(gptr cl1, gptr cl2);
gptr factor_step(gptr incl);
int simplify_cl_destr(gptr cl, int given_flag);
int can_cut_lit(gptr litpt1, int unify_flag, int given_flag);

void proc_derived_cl(gptr incl);
void proc_derived_cl_binhist(gptr incl, gint clid1, gint clid2, gint litpos1, gint litpos2);
void proc_input_cl(gptr incl);
*/

#endif
