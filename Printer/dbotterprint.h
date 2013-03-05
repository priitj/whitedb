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

 /** @file dbotterprint.h
 *  Top level/generic headers and defs for otterprinter
 *
 */

#ifndef __defined_dbotterprint_h
#define __defined_dbotterprint_h

#include "../Db/dballoc.h"

#include "../Reasoner/types.h"
#include "../Reasoner/mem.h"
#include "../Reasoner/glb.h"

void wr_print_clause(glb* g, gptr rec);
void wr_print_term(glb* g, gint rec);
void wr_print_record(glb* g, gptr rec);

void wr_print_db_otter(glb* g,int printlevel);

void wr_print_clause_otter(glb* g, gint* rec,int printlevel);
void wr_print_rule_clause_otter(glb* g, gint* rec,int printlevel);
void wr_print_fact_clause_otter(glb* g, gint* rec,int printlevel);
void wr_print_atom_otter(glb* g, gint rec,int printlevel);
void wr_print_term_otter(glb* g, gint rec,int printlevel);
void wr_print_simpleterm_otter(glb* g, gint enc,int printlevel);

#endif
