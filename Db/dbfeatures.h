/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
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

 /** @file dbfeatures.h
 * Constructs bit vector of libwgdb compile-time features
 */

#ifndef __defined_dbfeatures_h
#define __defined_dbfeatures_h

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* Used to check for individual features */
#define feature_bits_64bit 0x1
#define feature_bits_queued_locks 0x2
#define feature_bits_ttree_chained 0x4
#define feature_bits_backlink 0x8
#define feature_bits_child_db 0x10
#define feature_bits_index_tmpl 0x20

/* Construct the bit vector */
#ifdef HAVE_64BIT_GINT
  #define __feature_bits_01__ feature_bits_64bit
#else
  #define __feature_bits_01__ 0x0
#endif

#if (LOCK_PROTO==3)
  #define __feature_bits_02__ feature_bits_queued_locks
#else
  #define __feature_bits_02__ 0x0
#endif

#ifdef TTREE_CHAINED_NODES
  #define __feature_bits_03__ feature_bits_ttree_chained
#else
  #define __feature_bits_03__ 0x0
#endif

#ifdef USE_BACKLINKING
  #define __feature_bits_04__ feature_bits_backlink
#else
  #define __feature_bits_04__ 0x0
#endif

#ifdef USE_CHILD_DB
  #define __feature_bits_05__ feature_bits_child_db
#else
  #define __feature_bits_05__ 0x0
#endif

#ifdef USE_INDEX_TEMPLATE
  #define __feature_bits_06__ feature_bits_index_tmpl
#else
  #define __feature_bits_06__ 0x0
#endif

#define MEMSEGMENT_FEATURES (__feature_bits_01__ |\
  __feature_bits_02__ |\
  __feature_bits_03__ |\
  __feature_bits_04__ |\
  __feature_bits_05__ |\
  __feature_bits_06__)

#endif /* __defined_dbfeatures_h */
