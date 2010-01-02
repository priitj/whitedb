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

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dbmem.h"
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
//#include "../Db/dbapi.h"
#include "../Db/dbtest.h"
#include "../Db/dbdump.h"
#include "../Db/dblog.h"
#include "wgdb.h"



/* ====== Private defs =========== */


/* ======= Private protos ================ */


int db_write(void* db);
int db_read(void* db);


/* ====== Global vars ======== */


/* ====== Private vars ======== */


/* ====== Functions ============== */


/*
how to set 500 meg of shared memory:

su
echo 500000000  > /proc/sys/kernel/shmmax 
*/

/** usage: display command line help.
*
*/

void usage(char *prog) {
  printf("usage: %s [shmname] <command> [command arguments]\n"\
    "Where:\n"\
    "  shmname - shared memory name for database. May be omitted.\n"\
    "  command - required, one of:\n\n"\
    "    help (or \"-h\") - display this text.\n"\
    "    free - free shared memory.\n"\
    "    export <filename> - write memory dump to disk.\n"\
    "    import <filename> - read memory dump from disk. Overwrites previous "\
    "memory contents.\n"\
    "    log <filename> - deprecated. Removed in future versions.\n"\
    "    importlog <filename> - replay journal file from disk.\n"\
    "    test - run database tests.\n\n"\
    "Commands may have variable number of arguments. Command names may "\
    "not be used as shared memory name for the database.\n", prog);
}

/** top level for the database command line tool
*
*
*/

int main(int argc, char **argv) {
 
  char *shmname = NULL;
  char *shmptr;
  int i, scan_to, shmsize;
  
  /* look for commands in argv[1] or argv[2] */
  if(argc < 3) scan_to = argc;
  else scan_to = 3;
  shmsize = 0; /* 0 size causes default size to be used */
 
  /* 1st loop through, shmname is NULL for default. If
   * the first argument is not a recognizable command, it
   * is assumed to be the shmname and the next argument
   * is checked against known commands.
   */
  for(i=1; i<scan_to;) {
    if (!strcmp(argv[i],"help") || !strcmp(argv[i],"-h")) {
      usage(argv[0]);
      exit(0);
    }
    if (!strcmp(argv[i],"free")) {
      /* free shared memory */
      wg_delete_database(shmname);
      exit(0);
    }
    if(argc>(i+1) && !strcmp(argv[i],"import")){
      wg_int err;
      
      shmptr=wg_attach_database(shmname, shmsize);
      if(!shmptr) {
        fprintf(stderr, "Failed to attach to database.\n");
        exit(1);
      }

      err = wg_import_dump(shmptr,argv[i+1]);
      if(!err)
        db_read(shmptr); /* XXX: temporary test code */
      else if(err<-1)
        fprintf(stderr, "Fatal error in wg_import_dump, db may have"\
          " become corrupt\n");
      else
        fprintf(stderr, "Import failed.\n");
      break;
    }
    else if(argc>(i+1) && !strcmp(argv[i],"export")){
      wg_int err;

      shmptr=wg_attach_database(shmname, shmsize);
      if(!shmptr) {
        fprintf(stderr, "Failed to attach to database.\n");
        exit(1);
      }

      db_write(shmptr);  /* XXX: temporary test code */
      err = wg_dump(shmptr,argv[i+1]);
      if(err<-1)
        fprintf(stderr, "Fatal error in wg_dump, db may have"\
          " become corrupt\n");
      else if(err)
        fprintf(stderr, "Export failed.\n");
      break;
    }
    else if(argc>(i+1) && !strcmp(argv[i],"log")) {
      shmptr=wg_attach_database(shmname, shmsize);
      if(!shmptr) {
        fprintf(stderr, "Failed to attach to database.\n");
        exit(1);
      }
      db_write(shmptr);  /* XXX: temporary test code */
      wg_print_log(shmptr);
      wg_dump_log(shmptr,argv[i+1]);
      break;
    }
    else if(argc>(i+1) && !strcmp(argv[i],"importlog")) {    
      shmptr=wg_attach_database(shmname, shmsize);
      if(!shmptr) {
        fprintf(stderr, "Failed to attach to database.\n");
        exit(1);
      }
      wg_import_log(shmptr,argv[i+1]);
      break;
    }
    else if(!strcmp(argv[i],"test")) {
      shmptr=wg_attach_database(shmname, shmsize);
      if(!shmptr) {
        fprintf(stderr, "Failed to attach to database.\n");
        exit(1);
      }
      //printf("cp1\n");    
      check_datatype_writeread(shmptr,2);
      //show_strhash(shmptr);
      /* wg_delete_database(shmname); */
      break;
    }
    
    shmname = argv[1]; /* assuming two loops max */
    i++;
  }

  if(i==scan_to) {
    /* loop completed normally ==> no commands found */
    usage(argv[0]);
  }
#ifdef _WIN32  
  else {
    _getch();  
  }
#endif
  exit(0);
}


/*

db_example is for simple functionality testing

*/

int db_write(void* db) {
  void* rec=(char*)1;
  int i; 
  int j;
  int c;
  int flds;
  int records=3;
  int tmp=0;
  
  printf("********* db_example starts ************\n");
  flds=6;
  c=1;
  for (i=0;i<records;i++) {
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      printf("rec creation error");
      exit(0);    
    }      
    printf("wg_create_record(db) gave new adr %d offset %d\n",(int)rec,ptrtooffset(db,rec));      
    for(j=0;j<flds;j++) {
      tmp=wg_set_int_field(db,rec,j,c);
      //tmp=wg_set_field(db,rec,j,wg_encode_int(db,c));
      //fieldadr=((gint*)rec)+RECORD_HEADER_GINTS+j;
      //*fieldadr=wg_encode_int(db,c);
      //printf("computed fieldadr %d\n",fieldadr);
      //tmp2=wg_get_field(db,rec,j);
      //printf("wg_get_field gave raw %d\n",(int)tmp2);
      //printf("at fieldadr %d\n",(int)*fieldadr);
      c++;
      if (tmp!=0) { 
        printf("int storage error");
        exit(0);    
      }
    }           
  } 
  printf("gint: %d\n",sizeof(gint));
  printf("created %d records with %d fields, final c is %d\n",i,flds,c); 
  printf("first record adr %x offset %d\n",(int)rec,ptrtooffset(db,rec));
  printf("********* db_example ended ************\n");
  return c;
}

/*
    db read example
*/

int db_read(void* db) {
  void* rec=(char*)1;
  int i; 
  int c;
  int records=1;
  int tmp=0;
  
  printf("********* db_example starts ************\n");
  printf("logoffset: %d\n",wg_get_log_offset(db));
  c=0;
  for(i=0;i<records;i++) {
    rec=wg_get_first_record(db);
    printf("wg_get_first_record(db) gave adr %d offset %d\n",(int)rec,ptrtooffset(db,rec)); 
    tmp=wg_get_field(db,rec,0);
    printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
      tmp=wg_get_field(db,rec,1);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
        tmp=wg_get_field(db,rec,2);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    c++;
    while(rec!=NULL) {
      rec=wg_get_next_record(db,rec); 
      if (rec==NULL) break;
      c++;
      printf("wg_get_next_record(db) gave new adr %d offset %d\n",(int)rec,ptrtooffset(db,rec));
      tmp=wg_get_field(db,rec,0);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    tmp=wg_get_field(db,rec,1);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
        tmp=wg_get_field(db,rec,2);
      printf("wg_get_field gave raw %d decoded %d\n",(int)tmp,wg_decode_int(db,tmp));
    }
  }    
 // printf("c is %d\n",c);  
  
  printf("********* db_example ended ************\n");
  return c;
}
