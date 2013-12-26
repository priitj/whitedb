/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2010
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

 /** @file dbfeatures.h
 * Constructs bit vector of libwgdb compile-time features
 */

#ifndef DEFINED_DBFEATURES_H
#define DEFINED_DBFEATURES_H

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif

/* Used to check for individual features */
#define FEATURE_BITS_64BIT 0x1
#define FEATURE_BITS_QUEUED_LOCKS 0x2
#define FEATURE_BITS_TTREE_CHAINED 0x4
#define FEATURE_BITS_BACKLINK 0x8
#define FEATURE_BITS_CHILD_DB 0x10
#define FEATURE_BITS_INDEX_TMPL 0x20

/* Construct the bit vector */
#ifdef HAVE_64BIT_GINT
  #define FEATURE_BITS_01 FEATURE_BITS_64BIT
#else
  #define FEATURE_BITS_01 0x0
#endif

#if (LOCK_PROTO==3)
  #define FEATURE_BITS_02 FEATURE_BITS_QUEUED_LOCKS
#else
  #define FEATURE_BITS_02 0x0
#endif

#ifdef TTREE_CHAINED_NODES
  #define FEATURE_BITS_03 FEATURE_BITS_TTREE_CHAINED
#else
  #define FEATURE_BITS_03 0x0
#endif

#ifdef USE_BACKLINKING
  #define FEATURE_BITS_04 FEATURE_BITS_BACKLINK
#else
  #define FEATURE_BITS_04 0x0
#endif

#ifdef USE_CHILD_DB
  #define FEATURE_BITS_05 FEATURE_BITS_CHILD_DB
#else
  #define FEATURE_BITS_05 0x0
#endif

#ifdef USE_INDEX_TEMPLATE
  #define FEATURE_BITS_06 FEATURE_BITS_INDEX_TMPL
#else
  #define FEATURE_BITS_06 0x0
#endif

#define MEMSEGMENT_FEATURES (FEATURE_BITS_01 |\
  FEATURE_BITS_02 |\
  FEATURE_BITS_03 |\
  FEATURE_BITS_04 |\
  FEATURE_BITS_05 |\
  FEATURE_BITS_06)

#endif /* DEFINED_DBFEATURES_H */
