/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009,2010
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

 /** @file clstore.h
 * Headers for clause storage functions.
 */


#ifndef __defined_clstore_h
#define __defined_clstore_h

/* ==== Includes ==== */

#include "types.h"
#include "glb.h"

/* ==== Global defines ==== */

#define CLTERM_HASHNODE_GINT_NR 3
#define CLTERM_HASHNODE_LEN_POS 0
#define CLTERM_HASHNODE_TERM_POS 0
#define CLTERM_HASHNODE_CL_POS 1
#define CLTERM_HASHNODE_NEXT_POS 2

#define MAXHASHPOS 30

/* ==== Protos ==== */


void wr_push_clpickstack_cl(glb* g, gptr cl);
void wr_show_clpickstack(glb* g);
void wr_push_clqueue_cl(glb* g, gptr cl);
void wr_show_clqueue(glb* g);
void wr_push_clactive_cl(glb* g, gptr cl);
void wr_show_clactive(glb* g);

int wr_cl_store_res_terms(glb* g, gptr cl);

int wr_term_hashstore(glb* g, void* hashdata, gint atom, gptr cl);

gint wr_term_complexhash(glb* g, gint* hasharr, gint hashposbits, gint term);
gint wr_atom_funhash(glb* g, gint atom);
gint wr_term_basehash(glb* g, gint enc);

int wr_clterm_add_hashlist(glb* g, vec hashvec, gint hash, gint term, gptr cl);
int wr_clterm_hashlist_len(glb* g, vec hashvec, gint hash);
gint wr_clterm_hashlist_start(glb* g, vec hashvec, gint hash);
gint wr_clterm_hashlist_next(glb* g, vec hashvec, gint lastel);

gptr wr_clterm_alloc_hashnode(glb* g);
void wr_clterm_free_hashnode(glb* g, gptr node);
void wr_clterm_hashlist_free(glb* g, vec hashvec);
void wr_clterm_hashlist_print(glb* g, vec hashvec);



#endif
