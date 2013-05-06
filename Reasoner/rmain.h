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


 /** @file rmain.h
 *  Reasoner top level: initialisation and startup.
 *
 */


#ifndef __defined_rmain_h
#define __defined_rmain_h


#include "glb.h" 

int wg_run_reasoner(void *db, int argc, char **argv);
int wg_import_otter_file(void *db, char* filename);
int wg_import_prolog_file(void *db, char* filename);
glb* wg_init_reasoner(void *db, int argc, char **argv);
int wr_init_active_passive_lists_std(glb* g);
void wr_show_stats(glb* g);


#endif
