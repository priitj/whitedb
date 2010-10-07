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

 /** @file printerrutils.h
 *   Headers for printing and err handling utils.
 *
 */


#ifndef __defined_printerrutils_h
#define __defined_printerrutils_h

/* ==== Includes ==== */


/* ==== Global defines ==== */

#ifdef DPRINTF
#define dprintf(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define dprintf(fmt,...) ;
#endif

/* ==== Protos ==== */


void* wr_alloc_err(glb* g, char* errstr);
void* wr_alloc_err2(glb* g, char* errstr1, char* errstr2);
void* wr_alloc_err2int(glb* g, char* errstr, int n);
void wr_sys_exiterr(glb* g, char* errstr);
void wr_sys_exiterr2(glb* g, char* errstr1, char* errstr2);
void wr_sys_exiterr2int(glb* g, char* errstr, int n);
 

#endif
