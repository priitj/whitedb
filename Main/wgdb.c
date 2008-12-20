/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
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

 /** @file wgdb.c
 *  wgandalf database tool: command line utility
 */

/* ====== Includes =============== */



#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <conio.h> // for _getch
#endif

#include "../config.h"
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"

/* ====== Private defs =========== */


/* ====== Global vars ======== */


/* ====== Private vars ======== */


/* ====== Functions ============== */


/*
how to set 500 meg of shared memory:

su
echo 500000000  > /proc/sys/kernel/shmmax 
*/

/** top level for the database command line tool
*
*
*/

int main(int argc, char **argv) {
 
  char* shmname;
  char* shmptr;
  
  printf("hello from wgdb, argc: %d \n",argc);
  // memdbase command? if yes, perform and exit.
  if (argc>1) shmname=argv[1];
  else shmname=NULL;
  
  if (argc>2 && !strcmp(argv[2],"free")) {
    // free shared memory and exit
    wg_delete_database(shmname);
    exit(0);
    
  } 
  shmptr=wg_attach_database(shmname,0); // 0 size causes default size to be used
    
  printf("wg_attach_database on %d gave ptr %x\n",DEFAULT_MEMDBASE_KEY,(int)shmptr);
  if (shmptr==NULL) return 0;
  
  show_db_memsegment_header(shmptr);
  
  wg_detach_database(shmptr); 
#ifdef _WIN32  
  _getch();  
#endif  
  return 0;  
}


