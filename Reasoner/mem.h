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


 /** @file mem.h
 *  Specific memory allocation functions: vectors etc.
 *
 */


#ifndef __defined_mem_h
#define __defined_mem_h


#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"

#include "types.h"
#include "glb.h"

#define sys_malloc malloc ///< never use malloc: use sys_malloc as a last resort
#define sys_free free     ///< never use free: use sys_free as a last resort
#define sys_realloc realloc  ///< use sys_realloc instead of realloc

#define OVER_MALLOC_BYTES 8 ///<always add this to guarantee alignment
#define VEC_ALIGNMENT_BYTES 8 ///< guarantee vec start and wr_alloc_from_cvec alignment

/* ======= vec and cvec constants === */

#define VEC_START 1
#define CVEC_START 2
#define VEC_LEN(v) ((v)[0])
#define CVEC_LEN(v) ((v)[0])
#define CVEC_NEXT(v) ((v)[1])

#define GNULL ((gint)NULL)

/* ======= global defines === */

#define otp(db,offset) ((gptr)(offsettoptr((db),(offset)))) 
#define pto(db,realptr)((gint)(ptrtooffset((db),(realptr))))
#define rotp(g,offset) ((gptr)(otp(((g)->db),(offset))))
#define rpto(g,realptr)((gint)(pto(((g)->db),(realptr))))

/* ======= prototypes ===== */

void* wr_malloc(glb* g, int bytes);
void* wr_realloc(glb* g, void* p, int bytes);
void wr_free(glb* g, void* p);

vec wr_vec_new(glb* g, int len);
cvec wr_cvec_new(glb* g,int len);
void wr_vec_free(glb* g, vec v);
void wr_vecstr_free(glb* g, vec v);
void wr_vecvecstr_free(glb* g, vec v);
vec wr_vec_realloc(glb* g, vec v, int i);
vec wr_vec_store(glb* g, vec v, int i, gint e);
cvec wr_cvec_store(glb* g,vec v, int i, gint e);
cvec wr_cvec_push(glb* g,vec v, gint e);

gptr wr_alloc_from_cvec(glb* g, cvec buf, gint gints);

char* wr_str_new(glb* g, int len);
void wr_str_guarantee_space(glb* g, char** stradr, int* strlenadr, int needed);
void wr_str_free(glb* g, char* str);
void wr_str_freeref(glb* g, char** strref);


#endif
