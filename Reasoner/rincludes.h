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

 /** @file rincludes.h
 *   standard includes for reasoner c files
 */


#ifndef __defined_rincludes_h
#define __defined_rincludes_h

/* ==== Includes ==== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

#include "mem.h" 
#include "glb.h"
#include "clterm.h"
#include "unify.h"
#include "build.h"
#include "clstore.h"
#include "subsume.h"
#include "derive.h"
#include "rgenloop.h"
#include "rmain.h"
#include "printerrutils.h"
#include "../Db/dbutil.h"
#include "../Printer/dbotterprint.h"

/* ==== Global defines ==== */

#define CP0 printf("CP0\n");
#define CP1 printf("CP1\n");
#define CP2 printf("CP2\n");
#define CP3 printf("CP3\n");
#define CP4 printf("CP4\n");
#define CP5 printf("CP5\n");
#define CP6 printf("CP6\n");
#define CP7 printf("CP7\n");
#define CP8 printf("CP8\n");
#define CP9 printf("CP9\n");

#define PRINT_LIMITS

/* ==== Protos ==== */

#endif
