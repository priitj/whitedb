/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2013
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

 /** @file dbschema.h
 * Public headers for the strucured data functions.
 */

#ifndef __defined_dbschema_h
#define __defined_dbschema_h


/* ====== data structures ======== */


/* ==== Protos ==== */

void *wg_create_triple(void *db, gint subj, gint prop, gint ob);
#define wg_create_kvpair(db, key, val) wg_create_triple(db, 0, key, val)
void *wg_create_array(void *db, gint size, gint isdocument);
void *wg_create_object(void *db, gint size, gint isdocument);

#endif /* __defined_dbschema_h */
