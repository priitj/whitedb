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


 /** @file types.h
 *  Various datatypes.
 *
 */


#ifndef DEFINED_DATATYPES_H
#define DEFINED_DATATYPES_H


#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"


typedef gint* gptr;
typedef gint* vec;  /**< array with length: 0 contains len of array*/
typedef gint* cvec; /**< array with length and freepos: 0 len of array, 1 first free pos */

typedef gint veco; /**< offset of vec */
typedef gint cveco; /**< offset of cvec */



#endif
