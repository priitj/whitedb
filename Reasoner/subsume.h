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

 /** @file subsume.h
 *  Public headers for subsumption functions.
 *
 */
 


#ifndef __defined_subsume_h
#define __defined_subsume_h

#include "glb.h" 

int wr_given_cl_subsumed(glb* g, gptr given_cl);

gint wr_subsume_cl(glb* g, gptr cl1, gptr cl2, int uniquestrflag);
gint wr_subsume_cl_aux(glb* g,gptr cl1vec, gptr cl2vec, 
	                  gptr litpt1, gptr litpt2, 
	                  int litind1, int litind2, 
                    int cllen1, int cllen2,
                    int uniquestrflag);

#endif
