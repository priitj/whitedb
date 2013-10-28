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

 /** @file clterm.h
 * Procedures for building clauses/terms and fetching parts.
 */

#ifndef DEFINED_CLTERM_H
#define DEFINED_CLTERM_H

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dballoc.h"

#include "types.h"
#include "glb.h"

/* ============= term and record header structure =============== */

/*

 // Record header structure. Position 0 is always reserved
 // for size.
 
#define RECORD_HEADER_GINTS 3
#define RECORD_META_POS 1           // metainfo, reserved for future use /
#define RECORD_BACKLINKS_POS 2      // backlinks structure offset /

#define LITTLEENDIAN 1  ///< (intel is little-endian) difference in encoding tinystr
//#define USETINYSTR 1    ///< undef to prohibit usage of tinystr

// Record meta bits. 
#define RECORD_META_NOTDATA 0x1 // Record is a "special" record (not data) 
#define RECORD_META_MATCH 0x2   // "match" record (needs NOTDATA as well) 

#define is_special_record(r) (*((gint *) r + RECORD_META_POS) &\
                            RECORD_META_NOTDATA)
*/

#define CLAUSE_EXTRAHEADERLEN 1
//#define TERM_EXTRAHEADERLEN 1 // nr of gints in datarec before terms start
// we use (g->unify_firstuseterm) instead with the same meaning

//#define RECORD_META_NOTDATA 0x1 // Record is a "special" record (not data) 
//#define RECORD_META_MATCH 0x2

#define RECORD_META_RULE_CLAUSE  (1<<3) // should be notdata as well
#define RECORD_META_FACT_CLAUSE  (1<<4) // should be notdata as well
#define RECORD_META_ATOM (1<<5)      // should be notdata as well
#define RECORD_META_TERM (1<<6)      // should be notdata as well

#define ATOM_META_NEG encode_smallint(1) 

/* ============= external funs defs ============ */

/* ==== macros ===== */

#define LIT_WIDTH 2 //meta gint plus atom gint is width 2
#define LIT_META_POS 0
#define LIT_ATOM_POS 1 

#define get_field(r,n) (*(((gint*)(r))+RECORD_HEADER_GINTS+(n)))
#define set_field(r,n,d)  (*(((gint*)record)+RECORD_HEADER_GINTS+fieldnr)=(d))
#define get_record_len(r) (((gint)(getusedobjectwantedgintsnr(*((gint*)(r)))))-RECORD_HEADER_GINTS)  
#define decode_record(db,d) ((void*)(offsettoptr(db,decode_datarec_offset((d)))))
#define encode_record(db,d) ((gint)(encode_datarec_offset(ptrtooffset((db),(d)))))

#define wg_rec_is_rule_clause(db,rec) (*((gint*)(rec)+RECORD_META_POS) & RECORD_META_RULE_CLAUSE)
#define wg_rec_is_fact_clause(db,rec) (*((gint*)(rec)+RECORD_META_POS) & RECORD_META_FACT_CLAUSE)
#define wg_rec_is_atom_rec(db,rec) (*((gint*)(rec)+RECORD_META_POS) & RECORD_META_ATOM)
#define wg_rec_is_term_rec(db,rec) (*((gint*)(rec)+RECORD_META_POS) & RECORD_META_TERM)

#define wg_get_rule_clause_atom_meta(db,rec,litnr) get_field((rec), (CLAUSE_EXTRAHEADERLEN+((litnr)*LIT_WIDTH)))
#define wg_get_rule_clause_atom(db,rec,litnr) get_field((rec), (CLAUSE_EXTRAHEADERLEN+((litnr)*LIT_WIDTH)+1))

#define wg_atom_meta_is_neg(db,meta) ((meta) & ATOM_META_NEG)
#define litmeta_negpolarities(meta1,meta2) (((meta1) & ATOM_META_NEG)!=((meta2) & ATOM_META_NEG))

#define wg_count_clause_atoms(db,clause) ((get_record_len((clause))-CLAUSE_EXTRAHEADERLEN)/LIT_WIDTH)
  

/* ==== Protos ==== */

gptr wr_create_raw_record(glb* g, gint length, gint meta, gptr buffer);

void* wr_create_rule_clause(glb* g, int litnr);
void* wr_create_fact_clause(glb* g, int litnr);
void* wr_create_atom(glb* g, int termnr);
void* wr_create_term(glb* g, int termnr);
void* wr_convert_atom_fact_clause(glb* g, void* atom, int isneg);
int wr_set_rule_clause_atom(glb* g, void* clause, int litnr, gint atom);
int wr_set_rule_clause_atom_meta(glb* g, void* clause, int litnr, gint meta);
int wr_set_atom_subterm(glb* g, void* atom, int termnr, gint subterm);
int wr_set_term_subterm(glb* g, void* term, int termnr, gint subterm);

#endif
