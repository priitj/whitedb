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

 /** @file selftest.c
 *  WhiteDB regression test utility
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
#include "../Test/dbtest.h"
#ifdef USE_REASONER
#include "../Test/rtest.h"
#include "../Parser/dbparse.h"
#endif


/* ====== Private defs =========== */

#define FLAGS_QUIET 0x1

/* ======= Private protos ================ */

gint parse_flag(char *arg);

/* ====== Functions ============== */


/** usage: display command line help.
*
*/

void usage(char *prog) {
  printf("usage: %s [-q] <command>\n"\
    "Where:\n"\
    "  command is one of:\n\n"\
    "    help (or \"-h\") - display this text.\n"\
    "    common - run quick tests of common functionality.\n"\
    "    query - run in-depth query tests.\n"\
    "    index - run in-depth index tests.\n"\
    "    log - run journal logging tests.\n", prog);
#ifdef USE_REASONER
    printf("    reasoner - test the reasoner.\n");
#endif
  printf("\nThe flag `-q' will disable most of the output from the tests.\n");
}

/** Handle a command-line flag
*
*/
gint parse_flag(char *arg) {
  while(arg[0] == '-')
    arg++;
  switch(arg[0]) {
    case 'q':
      return FLAGS_QUIET;
    default:
      fprintf(stderr, "Unrecognized option: `%c'\n", arg[0]);
      break;
  }
  return 0;
}

/** Parse the command line and execute at most one command. 
*
*
*/

int main(int argc, char **argv) {
  int i, flags = 0;
  int rc = 0;

  for(i=1; i<argc; i++) {
    if (!strcmp(argv[i],"help") || !strcmp(argv[i],"-h")) {
      usage(argv[0]);
      exit(0);
    }
    else if(argv[i][0] == '-') {
      flags |= parse_flag(argv[i]);
    }
    /* We don't attach to any database here. The test functions do their
     * own memory allocation.
     */
#ifdef USE_REASONER
    else if(!strcmp(argv[i],"reasoner")){
      rc = wg_test_reasoner(argc,argv);
      break;
    }
#endif
    else if(!strcmp(argv[i],"common")) {
      rc = wg_run_tests(WG_TEST_COMMON, (flags & FLAGS_QUIET ? 0 : 2));
      break;
    }
    else if(!strcmp(argv[i],"query")) {
      rc = wg_run_tests(WG_TEST_QUERY, (flags & FLAGS_QUIET ? 0 : 2));
      break;
    }
    else if(!strcmp(argv[i],"index")) {
      rc = wg_run_tests(WG_TEST_INDEX, (flags & FLAGS_QUIET ? 0 : 2));
      break;
    }
    else if(!strcmp(argv[i],"log")) {
      rc = wg_run_tests(WG_TEST_LOG, (flags & FLAGS_QUIET ? 0 : 2));
      break;
    }
  }

  if(i==argc) {
    /* loop completed normally ==> no commands found */
    usage(argv[0]);
  }
  exit(rc);
}


#ifdef __cplusplus
}
#endif
