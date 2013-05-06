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

 /** @file printerrutils.c
 *   printing and err handling utils.
 *
 */
 
 

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#include "rincludes.h"  
   
/* ====== Private headers and defs ======== */



/* ======= Private protos ================ */


/* ====== Functions ============== */

  
/* =========== debug printing  ========================= */

  
  



  
/* ======================= errors ================================= */

/** Allocation error not requiring immediate exit.
*
*/

void* wr_alloc_err(glb* g, char* errstr) {
  if (g->print_flag) printf("Cannot allocate memory: %s.\n",errstr);
  return NULL;
}  


/** Allocation error not requiring immediate exit.
*
*/

void* wr_alloc_err2(glb* g, char* errstr1, char* errstr2) {
  if (g->print_flag) printf("Cannot allocate memory: %s %s.\n",errstr1,errstr2);
  return NULL;
}  


/** Allocation error not requiring immediate exit.
*
*/

void* wr_alloc_err2int(glb* g, char* errstr, int n) {
  if (g->print_flag) printf("Cannot allocate memory: %s %d.\n",errstr,n);
  return NULL;
}  

/** Hard system error requiring immediate exit.
*
*/

void wr_sys_exiterr(glb* g, char* errstr) {
  printf("System error in wgdb reasoner, exiting: %s.\n",errstr);
  exit(1);
}  


/** Hard system error requiring immediate exit.
*
*/

void wr_sys_exiterr2(glb* g, char* errstr1, char* errstr2) {
  printf("System error in wgdb reasoner, exiting: %s %s.\n",errstr1,errstr2);
  exit(1);
}  


/** Hard system error requiring immediate exit.
*
*/

void wr_sys_exiterr2int(glb* g, char* errstr, int n) {
  printf("System error in wgdb reasoner, exiting: %s %d.\n",errstr,n);
  exit(1);
}  



#ifdef __cplusplus
}
#endif
