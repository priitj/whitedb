/*
* $Id:  $
* $Version: $
*
* Copyright (c) Priit Järv 2014
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

 /** @file gendata.c
 *  WhiteDB test tool: generate integer data
 */

/* ====== Includes =============== */



#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dballoc.h"
#include "../Db/dbmem.h"
#include "../Db/dblock.h"
#include "../Test/dbtest.h"


/* ====== Private defs =========== */

#define TESTREC_SIZE 3


/* Helper macros for database lock management */

#define WLOCK(d,i) i = wg_start_write(d); \
    if(!i) { \
        fprintf(stderr, "Failed to get database lock\n"); \
        break; \
    }

#define WULOCK(d,i)  if(i) { \
        wg_end_write(d,i); \
        i = 0; \
    }

/* ======= Private protos ================ */

/* ====== Functions ============== */


/** usage: display command line help.
*
*/

void usage(char *prog) {
  printf("usage: %s [shmname] <command> [command arguments]\n"\
    "Where:\n"\
    "  shmname - (numeric) shared memory name for database. May be omitted.\n"\
    "  command - required, one of:\n\n"\
    "    help (or \"-h\") - display this text.\n"\
    "    fill <nr of rows> [asc | desc | mix] - fill db with integer data.\n",
    prog);
}

/** Command line parser.
*
*
*/

int main(int argc, char **argv) {

  char *shmname = NULL;
  void *shmptr = NULL;
  int i, scan_to;
  gint shmsize;
  gint wlock = 0;

  /* look for commands in argv[1] or argv[2] */
  if(argc < 3) scan_to = argc;
  else scan_to = 3;
  shmsize = 0;

  /* loop passes are handled like in wgdb.c:
   * if the first loop doesn't find a command, the 
   * argument is assumed to be the shared memory key.
   */
  for(i=1; i<scan_to; i++) {
    if (!strcmp(argv[i],"help") || !strcmp(argv[i],"-h")) {
      usage(argv[0]);
      exit(0);
    }
    else if(argc>(i+1) && !strcmp(argv[i], "fill")) {
      int rows = atol(argv[i+1]);
      if(!rows) {
        fprintf(stderr, "Invalid number of rows.\n");
        exit(1);
      }

      shmptr=wg_attach_database(shmname, shmsize);
      if(!shmptr) {
        fprintf(stderr, "Failed to attach to database.\n");
        exit(1);
      }

      WLOCK(shmptr, wlock);
      if(argc > (i+2) && !strcmp(argv[i+2], "mix"))
        wg_genintdata_mix(shmptr, rows, TESTREC_SIZE);
      else if(argc > (i+2) && !strcmp(argv[i+2], "desc"))
        wg_genintdata_desc(shmptr, rows, TESTREC_SIZE);
      else
        wg_genintdata_asc(shmptr, rows, TESTREC_SIZE);
      WULOCK(shmptr, wlock);
      printf("Data inserted\n");
      break;
    }

    shmname = argv[1];
  }

  if(i==scan_to) {
    usage(argv[0]);
  }
  if(shmptr) {
    WULOCK(shmptr, wlock);
    wg_detach_database(shmptr);
  }
  exit(0);
}


#ifdef __cplusplus
}
#endif
