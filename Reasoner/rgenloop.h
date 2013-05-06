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

 /** @file rgenloop.h
 *  Procedures for reasoner top level search loops: given-clause, usable, sos etc.
 *
 */

#ifndef __defined_rgenloop_h
#define __defined_rgenloop_h

#include "types.h"
#include "glb.h"

int wr_genloop(glb* g);
gptr wr_pick_given_cl(glb* g, int* given_kept_flag);
gptr wr_activate_passive_cl(glb* g, gptr picked_given_cl_cand);
gptr wr_add_given_cl_active_list(glb* g, gptr given_cl);
gptr wr_process_given_cl(glb* g, gptr given_cl_cand);
void wr_resolve_binary_all_active(glb* g, gptr cl);

#endif
