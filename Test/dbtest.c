/*
* $Id:  $
* $Version: $
*
* Copyright (c) Tanel Tammet 2004,2005,2006,2007,2008,2009
* Copyright (c) Priit Järv 2010, 2011, 2012, 2013, 2014
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

 /** @file dbtest.c
 *  Database testing, checking and report procedures
 *
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifndef _WIN32
#include <unistd.h>
#else
#include <process.h>
#include <errno.h>
#include <io.h>
#include <share.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "../Db/dballoc.h"
#include "../Db/dbdata.h"
#include "../Db/dbhash.h"
#include "../Db/dbindex.h"
#include "../Db/dbmem.h"
#include "../Db/dbutil.h"
#include "../Db/dbquery.h"
#include "../Db/dbcompare.h"
#include "../Db/dblog.h"
#include "../Db/dbschema.h"
#include "../Db/dbjson.h"
#include "dbtest.h"

/* ====== Private headers and defs ======== */

#ifdef _WIN32
#define snprintf sprintf_s
#endif

#define OK_TO_CONTINUE(x) ((x)==0 || (x)==77) /* 77 - skipped test in
                                               * autotools framework */

/* ======= Private protos ================ */

static int do_check_parse_encode(void *db, gint enc, gint exptype, void *expval,
                                                        int printlevel);
static gint wg_check_db(void* db);
static gint wg_check_datatype_writeread(void* db, int printlevel);
static gint wg_check_backlinking(void* db, int printlevel);
static gint wg_check_parse_encode(void* db, int printlevel);
static gint wg_check_compare(void* db, int printlevel);
static gint wg_check_query_param(void* db, int printlevel);
static gint wg_check_strhash(void* db, int printlevel);
static gint wg_test_index1(void *db, int magnitude, int printlevel);
static gint wg_test_index2(void *db, int printlevel);
static gint wg_test_index3(void *db, int magnitude, int printlevel);
static gint wg_check_childdb(void* db, int printlevel);
static gint wg_check_schema(void* db, int printlevel);
static gint wg_check_json_parsing(void* db, int printlevel);
static gint wg_check_idxhash(void* db, int printlevel);
static gint wg_test_query(void *db, int magnitude, int printlevel);
static gint wg_check_log(void* db, int printlevel);

static void wg_show_db_area_header(void* db, void* area_header);
static void wg_show_bucket_freeobjects(void* db, gint freelist);
static void wg_show_strhash(void* db);
static gint wg_count_freelist(void* db, gint freelist);

static gint check_varlen_area(void* db, void* area_header);
static gint check_varlen_area_freelist(void* db, void* area_header);
static gint check_bucket_freeobjects(void* db, void* area_header, gint bucketindex);
static gint check_varlen_area_markers(void* db, void* area_header);
static gint check_varlen_area_dv(void* db, void* area_header);
static gint check_object_in_areabounds(void*db,void* area_header,gint offset,gint size);
static gint check_varlen_area_scan(void* db, void* area_header);
static gint check_varlen_object_infreelist(void* db, void* area_header, gint offset, gint isfree);

static int guarded_strlen(char* str);
static int guarded_strcmp(char* a, char* b);
static int bufguarded_strcmp(char* a, char* b);
static int validate_index(void *db, void *rec, int rows, int column,
  int printlevel);
static int validate_mc_index(void *db, void *rec, size_t rows, gint index_id,
  gint *columns, size_t col_count, int printlevel);
#ifdef USE_CHILD_DB
static int childdb_mkindex(void *db, int cnt);
static int childdb_ckindex(void *db, int cnt, int printlevel);
static int childdb_dropindex(void *db, int cnt);
#endif
static gint longstr_in_hash(void* db, char* data, char* extrastr, gint type, gint length);
static int is_offset_in_list(void *db, gint reclist_offset, gint offset);
static int check_matching_rows(void *db, int col, int cond,
 void *val, gint type, int expected, int printlevel);
static int check_db_rows(void *db, int expected, int printlevel);
static int check_sanity(void *db);

/* ====== Functions ============== */

/** Run database tests.
 * Allows each test to be run in separate locally allocated databases,
 * if necessary.
 *
 * returns 0 if no errors.
 * returns 77 if a test was skipped. NOTE: this onlt makes sense
 * if the function is called with a single test.
 *
 * otherwise returns error code.
 */
int wg_run_tests(int tests, int printlevel) {
  int tmp = 0;
  void *db = NULL;

  if(tests & WG_TEST_COMMON) {
    db = wg_attach_local_database(800000);
    wg_show_db_memsegment_header(db);
    tmp=check_sanity(db);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_db(db);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_datatype_writeread(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_parse_encode(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_backlinking(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_compare(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_query_param(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_db(db);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_strhash(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_test_index2(db,printlevel);
    if (OK_TO_CONTINUE(tmp)) tmp=wg_check_childdb(db,printlevel);
    wg_delete_local_database(db);

    if (OK_TO_CONTINUE(tmp)) {
      /* separate database for the schema */
      db = wg_attach_local_database(800000);
      tmp=wg_check_schema(db,printlevel); /* run this first */
      if (OK_TO_CONTINUE(tmp)) tmp=wg_check_json_parsing(db,printlevel);
      if (OK_TO_CONTINUE(tmp)) tmp=wg_check_idxhash(db,printlevel);
      wg_delete_local_database(db);
    }

    if (OK_TO_CONTINUE(tmp)) {
      printf("\n***** Quick tests passed ******\n");
    } else {
      printf("\n***** Quick test failed ******\n");
      return tmp;
    }
  }

  if(tests & WG_TEST_INDEX) {
    db = wg_attach_local_database(20000000);
    tmp = wg_test_index1(db, 50, printlevel);
    wg_delete_local_database(db);

    if(OK_TO_CONTINUE(tmp)) {
      db = wg_attach_local_database(20000000);
      tmp = wg_test_index3(db, 50, printlevel);
      wg_delete_local_database(db);
    }

    if (!OK_TO_CONTINUE(tmp)) {
      printf("\n***** Index test failed ******\n");
      return tmp;
    } else {
      printf("\n***** Index test succeeded ******\n");
    }
  }

  if(tests & WG_TEST_QUERY) {
    db = wg_attach_local_database(120000000);
    tmp = wg_test_query(db, 4, printlevel);
    wg_delete_local_database(db);

    if (!OK_TO_CONTINUE(tmp)) {
      printf("\n***** Query test failed ******\n");
      return tmp;
    } else {
      printf("\n***** Query test succeeded ******\n");
    }
  }

  if(tests & WG_TEST_LOG) {
    db = wg_attach_local_database(800000);
    tmp = wg_check_log(db, printlevel);
    wg_delete_local_database(db);

    if (!OK_TO_CONTINUE(tmp)) {
      printf("\n***** Log test failed ******\n");
      return tmp;
    } else {
      printf("\n***** Log test succeeded ******\n");
    }
  }

  /* Add other tests here */
  return tmp;
}

/* ---------------- overviews, statistics ---------------------- */

/** print an overview of full memsegment memory  usage and addresses
*
*
*/


void wg_show_db_memsegment_header(void* db) {
  db_memsegment_header* dbh = dbmemsegh(db);

  printf("\nShowing db segment information\n");
  printf("==============================\n");
  printf("mark %d\n", (int) dbh->mark);
#ifdef _WIN32
  printf("size %Id\n", dbh->size);
  printf("free %Id\n", dbh->free);
#else
  printf("size %td\n", dbh->size);
  printf("free %td\n", dbh->free);
#endif
  printf("initialadr %p\n", (void *) dbh->initialadr);
  printf("key  %d\n", (int) dbh->key);
  printf("segment header size %d\n", (int) sizeof(db_memsegment_header));
  printf("subarea  array size %d\n",SUBAREA_ARRAY_SIZE);

  printf("\ndatarec_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->datarec_area_header));
  printf("\nlongstr_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->longstr_area_header));
  printf("\nlistcell_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->listcell_area_header));
  printf("\nshortstr_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->shortstr_area_header));
  printf("\nword_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->word_area_header));
  printf("\ndoubleword_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->doubleword_area_header));
  printf("\ntnode_area\n");
  printf("-------------\n");
  wg_show_db_area_header(db,&(dbh->tnode_area_header));
}

/** print an overview of a single area memory usage and addresses
*
*
*/

static void wg_show_db_area_header(void* db, void* area_header) {
  db_area_header* areah;
  gint i;

  areah=(db_area_header*)area_header;
  if (areah->fixedlength) {
    printf("fixedlength with objlength %d bytes\n", (int) areah->objlength);
    printf("freelist %d\n", (int) areah->freelist);
    printf("freelist len %d\n", (int) wg_count_freelist(db,areah->freelist));
  } else {
    printf("varlength\n");
  }
  printf("last_subarea_index %d\n", (int) areah->last_subarea_index);
  for (i=0;i<=(areah->last_subarea_index);i++) {
    printf("subarea nr %d \n", (int) i);
    printf("  size     %d\n", (int) ((areah->subarea_array)[i]).size);
    printf("  offset        %d\n", (int) ((areah->subarea_array)[i]).offset);
    printf("  alignedsize   %d\n", (int) ((areah->subarea_array)[i]).alignedsize);
    printf("  alignedoffset %d\n", (int) ((areah->subarea_array)[i]).alignedoffset);
  }
  for (i=0;i<EXACTBUCKETS_NR+VARBUCKETS_NR;i++) {
    if ((areah->freebuckets)[i]!=0) {
      printf("bucket nr %d \n", (int) i);
      if (i<EXACTBUCKETS_NR) {
        printf(" is exactbucket at offset %d\n", (int) dbaddr(db,&(areah->freebuckets)[i]));
        wg_show_bucket_freeobjects(db,(areah->freebuckets)[i]);
      } else {
        printf(" is varbucket at offset %d \n", (int) dbaddr(db,&(areah->freebuckets)[i]));
        wg_show_bucket_freeobjects(db,(areah->freebuckets)[i]);
      }
    }
  }
  if ((areah->freebuckets)[DVBUCKET]!=0) {
    printf("bucket nr %d at offset %d \n contains dv at offset %d with size %d(%d) and end %d \n",
          DVBUCKET, (int) dbaddr(db,&(areah->freebuckets)[DVBUCKET]),
          (int) (areah->freebuckets)[DVBUCKET],
          (int) ((areah->freebuckets)[DVSIZEBUCKET]>0 ? dbfetch(db,(areah->freebuckets)[DVBUCKET]) : -1),
          (int) (areah->freebuckets)[DVSIZEBUCKET],
          (int) ((areah->freebuckets)[DVBUCKET]+(areah->freebuckets)[DVSIZEBUCKET]));
  }
}



/** show a list of free objects in a bucket
*
*/

static void wg_show_bucket_freeobjects(void* db, gint freelist) {
  gint size;
  gint freebits;
  gint nextptr;
  gint prevptr;

  while(freelist!=0) {
    size=getfreeobjectsize(dbfetch(db,freelist));
    freebits=dbfetch(db,freelist) & 3;
    nextptr=dbfetch(db,freelist+sizeof(gint));
    prevptr=dbfetch(db,freelist+2*sizeof(gint));
    printf("    object offset %d end %d freebits %d size %d nextptr %d prevptr %d \n",
            (int) freelist, (int) (freelist+size), (int) freebits,
            (int) size, (int) nextptr, (int) prevptr);
    freelist=nextptr;
  }
}




/** count elements in a freelist
*
*/

static gint wg_count_freelist(void* db, gint freelist) {
  gint i;
  //printf("freelist %d dbfetch(db,freelist) %d\n",freelist,dbfetch(db,freelist));

  for(i=0;freelist; freelist=dbfetch(db,freelist)) {
    //printf("i %d freelist %u\n",i,(uint)freelist);
    i++;
  }
  return i;
}


/* --------------- datatype conversion/writing/reading testing ------------------------------*/


/**
  printlevel: 0 no print, 1 err print, 2 full print

*/

static gint wg_check_datatype_writeread(void* db, int printlevel) {
  int p;
  int i;
  int j;
  int k,r,m;
  int tries;

  // encoded and decoded data
  gint enc;
  gint* rec;
  char* nulldec;
  int intdec;
  char chardec;
  double doubledec;
  char* strdec;
  int len;
  int tmplen;
  int tmp;
  int decbuflen=1000;
  char decbuf[1000];
  char encbuf[1000];

  // amount of tested data
  int nulldata_nr=1;
  int chardata_nr=2;
  int vardata_nr=2;
  int intdata_nr=4;
  int doubledata_nr=4;
  int fixpointdata_nr=5;
  int datedata_nr=4;
  int timedata_nr=4;
  int datevecdata_nr=4;
  int datevecbad_nr=2;
  int timevecdata_nr=4;
  int timevecbad_nr=4;
  int strdata_nr=5;
  int xmlliteraldata_nr=2;
  int uridata_nr=2;
  int blobdata_nr=3;
  int recdata_nr=10;

  // tested data buffers
  char* nulldata[10];
  char chardata[10];
  int vardata[10];
  int intdata[10];
  double doubledata[10];
  double fixpointdata[10];
  int timedata[10];
  int datedata[10];
  char* strdata[10];
  char* strextradata[10];
  char* xmlliteraldata[10];
  char* xmlliteralextradata[10];
  char* uridata[10];
  char* uriextradata[10];
  char* blobdata[10];
  char* blobextradata[10];
  int bloblendata[10];
  int recdata[10];
  int tmpvec[4];

  int datevecdata[][3] = {
    {1, 1, 1},
    {2010, 1, 1},
    {2010, 4, 30},
    {5997, 1, 6} };
  int datevecbad[][3] = {
    {1, -1, 2},
    {1990, 7, 32},
    {2010, 2, 29},
    {2010, 4, 31} };
  int timevecdata[][4] = {
    {0, 0, 0, 0},
    {0, 10, 20, 3},
    {24, 0, 0, 0},
    {13, 32, 0, 3} };
  int timevecbad[][4] = {
    {1, -1, 2, 99},
    {1, 1, 1, 101},
    {25, 2, 1, 0},
    {23, 12, 73, 0} };

  p=printlevel;
  tries=1;
  if (p>1) printf("********* check_datatype_writeread starts ************\n");

  // initialise tested data

  nulldata[0]=NULL;
  chardata[0]='p';
  chardata[1]=' ';
  intdata[0]=0;
  intdata[1]=100;
  intdata[2]=-50;
  intdata[3]=100200;
  doubledata[0]=0;
  doubledata[1]=1000;
  doubledata[2]=0.45678;
  doubledata[3]=-45.991;
  datedata[1]=733773; // 2010 m 1 d 1
  datedata[2]=733892; // 2010 m 4 d 30
  datedata[0]=1;
  datedata[3]=6000*365;
  timedata[0]=0;
  timedata[1]=10*(60*100)+20*100+3;
  timedata[2]=24*60*60*100;
  timedata[3]=14*60*58*100+3;

  fixpointdata[0]=0;
  fixpointdata[1]=1.23;
  fixpointdata[2]=790.3456;
  fixpointdata[3]=-799.7891;
  fixpointdata[4]=0.002345678;

  strdata[0]="abc";
  strdata[1]="abcdefghijklmnop";
  strdata[2]="1234567890123456789012345678901234567890";
  strdata[3]="";
  strdata[4]="";
  strextradata[0]=NULL;
  strextradata[1]=NULL;
  strextradata[2]="op12345";
  strextradata[3]="asdasdasdsd";
  strextradata[4]=NULL;
  xmlliteraldata[0]="ffoo";
  xmlliteraldata[1]="ffooASASASasaasweerrtttyyuuu";
  xmlliteralextradata[0]="bar:we";
  xmlliteralextradata[1]="bar:weasdasdasdasdasdasdasdasdasdasdasdasdasddas";
  uridata[0]="dasdasdasd";
  uridata[1]="dasdasdasd12345678901234567890";
  uriextradata[0]="";
  uriextradata[1]="fofofofof";

  blobdata[0]=(char*)malloc(10);
  for(i=0;i<10;i++) *(blobdata[0]+i)=i+65;
  blobextradata[0]="type1";
  bloblendata[0]=10;

  blobdata[1]=(char*)malloc(1000);
  for(i=0;i<1000;i++) *(blobdata[1]+i)=(i%10)+65;   //i%256;
  blobextradata[1]="type2";
  bloblendata[1]=200;

  blobdata[2]=(char*)malloc(10);
  for(i=0;i<10;i++) *(blobdata[2]+i)=i%256;
  blobextradata[2]=NULL;
  bloblendata[2]=10;

  recdata[0]=0;
  recdata[1]=1;
  recdata[2]=2;
  recdata[3]=3;
  recdata[4]=4;
  recdata[5]=5;
  recdata[6]=100;
  recdata[7]=101;
  recdata[8]=10000;
  recdata[9]=10001;

  vardata[0]=0;
  vardata[1]=999882;

  for (i=0;i<tries;i++) {

    // null test
    for (j=0;j<nulldata_nr;j++) {
      if (p>1) printf("checking null enc/dec\n");
      enc=wg_encode_null(db,nulldata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_NULLTYPE) {
        if (p) printf("check_datatype_writeread gave error: null enc not right type \n");
        return 1;
      }
      nulldec=wg_decode_null(db,enc);
      if (nulldata[j]!=nulldec) {
        if (p) printf("check_datatype_writeread gave error: null enc/dec \n");
        return 1;
      }
    }


    // char test

    for (j=0;j<chardata_nr;j++) {
      if (p>1) printf("checking char enc/dec for j %d, value '%c'\n",j,chardata[j]);
      enc=wg_encode_char(db,chardata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_CHARTYPE) {
        if (p) printf("check_datatype_writeread gave error: char enc not right type for j %d value '%c'\n",
                      j,chardata[j]);
        return 1;
      }
      chardec=wg_decode_char(db,enc);
      if (chardata[j]!=chardec) {
        if (p) printf("check_datatype_writeread gave error: char enc/dec for j %d enc value '%c' dec value '%c'\n",
                     j,chardata[j],chardec);
        return 1;
      }
    }

    // int test

    for (j=0;j<intdata_nr;j++) {
      if (p>1) printf("checking int enc/dec for j %d, value %d\n",j,intdata[j]);
      enc=wg_encode_int(db,intdata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_INTTYPE) {
        if (p) printf("check_datatype_writeread gave error: int enc not right type for j %d value %d\n",
                      j,intdata[j]);
        return 1;
      }
      intdec=wg_decode_int(db,enc);
      if (intdata[j]!=intdec) {
        if (p) printf("check_datatype_writeread gave error: int enc/dec for j %d enc value %d dec value %d\n",
                      j,intdata[j],intdec);
        return 1;
      }
    }

    // double test

    for (j=0;j<doubledata_nr;j++) {
      if (p>1) printf("checking double enc/dec for j %d, value %e\n",j,doubledata[j]);
      enc=wg_encode_double(db,doubledata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_DOUBLETYPE) {
        if (p) printf("check_datatype_writeread gave error: double enc not right type for j %d value %e\n",
                      j,doubledata[j]);
        return 1;
      }
      doubledec=wg_decode_double(db,enc);
      if (doubledata[j]!=doubledec) {
        if (p) printf("check_datatype_writeread gave error: double enc/dec for j %d enc value %e dec value %e\n",
                      j,doubledata[j],doubledec);
        return 1;
      }
    }

    // date test

    for (j=0;j<datedata_nr;j++) {
      if (p>1) printf("checking date enc/dec for j %d, value %d\n",j,datedata[j]);
      enc=wg_encode_date(db,datedata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_DATETYPE) {
        if (p) printf("check_datatype_writeread gave error: date enc not right type for j %d value %d\n",
                      j,intdata[j]);
        return 1;
      }
      intdec=wg_decode_date(db,enc);
      if (datedata[j]!=intdec) {
        if (p) printf("check_datatype_writeread gave error: date enc/dec for j %d enc value %d dec value %d\n",
                      j,datedata[j],intdec);
        return 1;
      }
    }

    for (j=0;j<datedata_nr && j<datevecdata_nr;j++) {
      if (p>1) printf("checking building dates from vectors for j %d, expected value %d\n",j,datedata[j]);
      tmp=wg_ymd_to_date(db, datevecdata[j][0], datevecdata[j][1], datevecdata[j][2]);
      if(tmp != datedata[j]) {
        if (p) printf("check_datatype_writeread gave error: scalar date returned was %d\n",tmp);
        return 1;
      }
      wg_date_to_ymd(db, tmp, &tmpvec[0], &tmpvec[1], &tmpvec[2]);
      if(tmpvec[0]!=datevecdata[j][0] || tmpvec[1]!=datevecdata[j][1] ||\
        tmpvec[2]!=datevecdata[j][2]) {
        if (p) printf("check_datatype_writeread gave error: scalar date reverse conversion failed for j %d\n",j);
        return 1;
      }
    }

    for (j=0;j<datevecbad_nr;j++) {
      if (p>1) printf("checking invalid date input for j %d\n",j);
      tmp=wg_ymd_to_date(db, datevecbad[j][0], datevecbad[j][1], datevecbad[j][2]);
      if(tmp != -1) {
        if (p) printf("check_datatype_writeread gave error: invalid date j %d did not cause an error\n", j);
        return 1;
      }
    }

    // time test

    for (j=0;j<timedata_nr;j++) {
      if (p>1) printf("checking time enc/dec for j %d, value %d\n",j,timedata[j]);
      enc=wg_encode_time(db,timedata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_TIMETYPE) {
        if (p) printf("check_datatype_writeread gave error: time enc not right type for j %d value %d\n",
                      j,timedata[j]);
        return 1;
      }
      intdec=wg_decode_time(db,enc);
      if (timedata[j]!=intdec) {
        if (p) printf("check_datatype_writeread gave error: time enc/dec for j %d enc value %d dec value %d\n",
                      j,timedata[j],intdec);
        return 1;
      }
    }

    for (j=0;j<timedata_nr && j<timevecdata_nr;j++) {
      if (p>1) printf("checking building times from vectors for j %d, expected value %d\n",j,timedata[j]);
      tmp=wg_hms_to_time(db, timevecdata[j][0], timevecdata[j][1], timevecdata[j][2], timevecdata[j][3]);
      if(tmp != timedata[j]) {
        if (p) printf("check_datatype_writeread gave error: scalar time returned was %d\n",tmp);
        return 1;
      }
      wg_time_to_hms(db, tmp, &tmpvec[0], &tmpvec[1], &tmpvec[2], &tmpvec[3]);
      if(tmpvec[0]!=timevecdata[j][0] || tmpvec[1]!=timevecdata[j][1] ||\
        tmpvec[2]!=timevecdata[j][2] || tmpvec[3]!=timevecdata[j][3]) {
        if (p) printf("check_datatype_writeread gave error: scalar time reverse conversion failed for j %d\n",j);
        return 1;
      }
    }

    for (j=0;j<timevecbad_nr;j++) {
      if (p>1) printf("checking invalid time input for j %d\n",j);
      tmp=wg_hms_to_time(db, timevecbad[j][0], timevecbad[j][1], timevecbad[j][2], timevecbad[j][3]);
      if(tmp != -1) {
        if (p) printf("check_datatype_writeread gave error: invalid time j %d did not cause an error\n", j);
        return 1;
      }
    }

    // datetime test

    for (j=0;j<datedata_nr;j++) {
      if (p>1) printf("checking strf iso datetime conv for j %d, date %d time %d\n",j,datedata[j],timedata[j]);
      for(k=0;k<1000;k++) decbuf[k]=0;
      for(k=0;k<1000;k++) encbuf[k]=0;
      wg_strf_iso_datetime(db,datedata[j],timedata[j],decbuf);
      if (p>1) printf("wg_strf_iso_datetime gives %s ",decbuf);
      k=wg_strp_iso_date(db,decbuf);
      r=wg_strp_iso_time(db,decbuf+11);
      //printf("k is %d r is %d\n",k,r);
      if (1) {
        if (k>=0 && r>=0) {
          wg_strf_iso_datetime(db,k,r,encbuf);
          if (strcmp(decbuf,encbuf)) {
            if(p) printf("check_datatype_writeread gave error: wg_strf_iso_datetime gives %s and rev op gives %s\n",
                         decbuf,encbuf);
            return 1;
          }
          if (p>1) printf("rev gives %s\n",decbuf);
        } else {
          if(p) printf("check_datatype_writeread gave error: wg_strp_iso_date gives %d and wg_strp_iso_time gives %d on %s\n",
                        k,r,decbuf);
          return 1;
        }
      }
    }

    // current date and time test

    for(k=0;k<1000;k++) encbuf[k]=0;
    m=wg_current_utcdate(db);
    k=wg_current_localdate(db);
    r=wg_current_utctime(db);
    j=wg_current_localtime(db);
    if (p>1) {
      wg_strf_iso_datetime(db,m,r,encbuf);
      printf("checking wg_current_utcdate/utctime: %s\n",encbuf);
      wg_strf_iso_datetime(db,k,j,encbuf);
      printf("checking wg_current_localdate/localtime: %s\n",encbuf);
    }

    // fixpoint test

     for (j=0;j<fixpointdata_nr;j++) {
      if (p>1) printf("checking fixpoint enc/dec for j %d, value %f\n",j,fixpointdata[j]);
      enc=wg_encode_fixpoint(db,fixpointdata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_FIXPOINTTYPE) {
        if (p) printf("check_datatype_writeread gave error: fixpoint enc not right type for j %d value %e\n",
                      j,doubledata[j]);
        return 1;
      }
      doubledec=wg_decode_fixpoint(db,enc);
      if (round(FIXPOINTDIVISOR*fixpointdata[j])!=round(FIXPOINTDIVISOR*doubledec)) {
         //(fixpointdata[j]!=doubledec) {
        if (p) printf("check_datatype_writeread gave error: fixpoint enc/dec for j %d enc value %f dec value %f\n",
                      j,fixpointdata[j],doubledec);
        return 1;
      }
    }

    // str test

    for (j=0;j<strdata_nr;j++) {
      if (p>1) printf("checking str enc/dec for j %d, value \"%s\", extra \"%s\"\n",
                      j,strdata[j],strextradata[j]);
      enc=wg_encode_str(db,strdata[j],strextradata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_STRTYPE) {
        if (p) printf("check_datatype_writeread gave error: str enc not right type for j %d value \"%s\", extra \"%s\"\n",
                      j,strdata[j],strextradata[j]);
        return 1;
      }
      len=wg_decode_str_len(db,enc);
      if (len!=guarded_strlen(strdata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n",
                      j,strdata[j],strextradata[j],guarded_strlen(strdata[j]),len);
        return 1;
      }
      strdec=wg_decode_str(db,enc);
      if (guarded_strcmp(strdata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str for j %d value \"%s\" extra \"%s\"\n",
                      j,strdata[j],strextradata[j]);
        return 1;
      }
      tmplen=wg_decode_str_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(strdata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_copy len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n\n",
                      j,strdata[j],strextradata[j],guarded_strlen(strdata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,strdata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_copy for j %d value \"%s\" extra \"%s\" dec main \"%s\"\n",
                      j,strdata[j],strextradata[j],decbuf);
        return 1;
      }
      len=wg_decode_str_lang_len(db,enc);
      if (len!=guarded_strlen(strextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_lang_len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n",
                      j,strdata[j],strextradata[j],guarded_strlen(strextradata[j]),len);
        return 1;
      }
      strdec=wg_decode_str_lang(db,enc);
      if (guarded_strcmp(strextradata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_lang for j %d value \"%s\" extra \"%s\"\n",
                      j,strdata[j],strextradata[j]);
        return 1;
      }
      tmplen=wg_decode_str_lang_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(strextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_lang_copy len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n\n",
                      j,strdata[j],strextradata[j],guarded_strlen(strextradata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,strextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_str_lang_copy for j %d value \"%s\" extra \"%s\" dec extra \"%s\"\n",
                      j,strdata[j],strextradata[j],decbuf);
        return 1;
      }
    }

    // xmllit test

    for (j=0;j<xmlliteraldata_nr;j++) {
      if (p>1) printf("checking xmlliteral enc/dec for j %d, value \"%s\", extra \"%s\"\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j]);
      enc=wg_encode_xmlliteral(db,xmlliteraldata[j],xmlliteralextradata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_XMLLITERALTYPE) {
        if (p) printf("check_datatype_writeread gave error: xmlliteral enc not right type for j %d value \"%s\", extra \"%s\"\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j]);
        return 1;
      }
      len=wg_decode_xmlliteral_len(db,enc);
      if (len!=guarded_strlen(xmlliteraldata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j],guarded_strlen(xmlliteraldata[j]),len);
        return 1;
      }
      strdec=wg_decode_xmlliteral(db,enc);
      if (guarded_strcmp(xmlliteraldata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral for j %d value \"%s\" extra \"%s\"\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j]);
        return 1;
      }
      tmplen=wg_decode_xmlliteral_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(xmlliteraldata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_copy len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j],guarded_strlen(xmlliteraldata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,xmlliteraldata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_copy for j %d value \"%s\" extra \"%s\" dec main \"%s\"\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j],decbuf);
        return 1;
      }
      len=wg_decode_xmlliteral_xsdtype_len(db,enc);
      if (len!=guarded_strlen(xmlliteralextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_xsdtype_len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j],guarded_strlen(xmlliteralextradata[j]),len);
        return 1;
      }
      strdec=wg_decode_xmlliteral_xsdtype(db,enc);
      if (guarded_strcmp(xmlliteralextradata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_xsdtype for j %d value \"%s\" extra \"%s\"\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j]);
        return 1;
      }
      tmplen=wg_decode_xmlliteral_xsdtype_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(xmlliteralextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_xsdtype_copy len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j],guarded_strlen(xmlliteralextradata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,xmlliteralextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_xmlliteral_xsdtype_copy for j %d value \"%s\" extra \"%s\" dec extra \"%s\"\n",
                      j,xmlliteraldata[j],xmlliteralextradata[j],decbuf);
        return 1;
      }
    }


    // uri test


    for (j=0;j<uridata_nr;j++) {
      if (p>1) printf("checking uri enc/dec for j %d, value \"%s\", extra \"%s\"\n",
                      j,uridata[j],uriextradata[j]);
      enc=wg_encode_uri(db,uridata[j],uriextradata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_URITYPE) {
        if (p) printf("check_datatype_writeread gave error: uri enc not right type for j %d value \"%s\", extra \"%s\"\n",
                      j,uridata[j],uriextradata[j]);
        return 1;
      }
      len=wg_decode_uri_len(db,enc);
      if (len!=guarded_strlen(uridata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n",
                      j,uridata[j],uriextradata[j],guarded_strlen(uridata[j]),len);
        return 1;
      }
      strdec=wg_decode_uri(db,enc);
      if (guarded_strcmp(uridata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri for j %d value \"%s\" extra \"%s\"\n",
                      j,uridata[j],uriextradata[j]);
        return 1;
      }
      tmplen=wg_decode_uri_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(uridata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_copy len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n\n",
                      j,uridata[j],uriextradata[j],guarded_strlen(uridata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,uridata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_copy for j %d value \"%s\" extra \"%s\" dec main \"%s\"\n",
                      j,uridata[j],uriextradata[j],decbuf);
        return 1;
      }
      len=wg_decode_uri_prefix_len(db,enc);
      if (len!=guarded_strlen(uriextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_prefix_len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n",
                      j,uridata[j],uriextradata[j],guarded_strlen(uriextradata[j]),len);
        return 1;
      }
      strdec=wg_decode_uri_prefix(db,enc);
      if (guarded_strcmp(uriextradata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_prefix for j %d value \"%s\" extra \"%s\"\n",
                      j,uridata[j],uriextradata[j]);
        return 1;
      }
      tmplen=wg_decode_uri_prefix_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(uriextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_prefix_copy len for j %d value \"%s\" extra \"%s\" enc len %d dec len %d\n\n",
                      j,uridata[j],uriextradata[j],guarded_strlen(uriextradata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,uriextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_uri_prefix_copy for j %d value \"%s\" extra \"%s\" dec extra \"%s\"\n",
                      j,uridata[j],uriextradata[j],decbuf);
        return 1;
      }
    }

    // blob test

    for (j=0;j<blobdata_nr;j++) {
      if (p>1) printf("checking blob enc/dec for j %d, len %d extra \"%s\"\n",
                      j,bloblendata[j],blobextradata[j]);
      enc=wg_encode_blob(db,blobdata[j],blobextradata[j],bloblendata[j]);
      if (!enc)  {
        if (p) printf("check_datatype_writeread gave error: cannot create a blob\n");
        return 1;
      }
      if (wg_get_encoded_type(db,enc)!=WG_BLOBTYPE) {
        if (p) printf("check_datatype_writeread gave error: blob enc not right type for j %d len %d, extra \"%s\"\n",
                      j,bloblendata[j],blobextradata[j]);
        return 1;
      }
      len=wg_decode_blob_len(db,enc);
      if (len!=bloblendata[j]) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_len for j %d len %d extra \"%s\" enc len %d dec len %d\n",
                      j,bloblendata[j],blobextradata[j],bloblendata[j],len);
        return 1;
      }
      strdec=wg_decode_blob(db,enc);
      if (memcmp(blobdata[j],strdec,bloblendata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob for j %d len %d extra \"%s\"\n",
                      j,bloblendata[j],blobextradata[j]);
        return 1;
      }
      tmplen=wg_decode_blob_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=bloblendata[j]) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_copy len for j %d len %d extra \"%s\" enc len %d dec len %d\n\n",
                      j,bloblendata[j],blobextradata[j],bloblendata[j],tmplen);
        return 1;
      }
      if (memcmp(decbuf,blobdata[j],bloblendata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_copy for j %d len %d extra \"%s\" dec len %d\n",
                      j,bloblendata[j],blobextradata[j],tmplen);
        return 1;
      }
      len=wg_decode_blob_type_len(db,enc);
      if (len!=guarded_strlen(blobextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_type_len for j %d len %d extra \"%s\" enc len %d dec len %d\n",
                      j,bloblendata[j],blobextradata[j],guarded_strlen(blobextradata[j]),len);
        return 1;
      }
      strdec=wg_decode_blob_type(db,enc);
      if (guarded_strcmp(blobextradata[j],strdec)) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_type for j %d len %d extra \"%s\"\n",
                      j,bloblendata[j],blobextradata[j]);
        return 1;
      }
      tmplen=wg_decode_blob_type_copy(db,enc,decbuf,decbuflen);
      if (tmplen!=guarded_strlen(blobextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_type_copy len for j %d len %d extra \"%s\" enc len %d dec len %d\n\n",
                      j,bloblendata[j],blobextradata[j],guarded_strlen(blobextradata[j]),tmplen);
        return 1;
      }
      if (bufguarded_strcmp(decbuf,blobextradata[j])) {
        if (p) printf("check_datatype_writeread gave error: wg_decode_blob_type_copy for j %d len %d extra \"%s\" dec extra \"%s\"\n",
                      j,bloblendata[j],blobextradata[j],decbuf);
        return 1;
      }
    }

    // rec test

    for (j=0;j<recdata_nr;j++) {
      if (p>1) printf("checking rec creation, content read/write for j %d, length %d\n",j,recdata[j]);
      rec=(gint *)wg_create_record(db,recdata[j]);
      if (rec==NULL) {
        if (p) printf("check_datatype_writeread gave error: creating record for j %d len %d failed\n",
                      j,recdata[j]);
        return 1;
      }
/* the following code can't be correct - rec is a pointer, not encoded value
     if (wg_get_encoded_type(db,(gint)rec)!=WG_RECORDTYPE) {
        if (p) printf("check_datatype_writeread gave error: created record not right type for j %d len %d\n",
                      j,recdata[j]);
        return 1;
      } */
      tmplen=wg_get_record_len(db,rec);
      if (tmplen!=recdata[j]) {
        if (p) printf("check_datatype_writeread gave error: wg_get_record_len gave %d for rec of len %d\n",
                       tmplen,recdata[j]);
        return 1;
      }
      for(k=0;k<recdata[j];k++) {
        enc=wg_encode_int(db,k*10);
        tmp=wg_set_field(db,rec,k,enc);
        if (tmp) {
          if (p) printf("check_datatype_writeread gave error: cannot store data to field %d, reclen %d, result is %d\n",
                       k,recdata[j],tmp);
          return 1;
        }
      }
      for(k=0;k<recdata[j];k++) {
        enc=wg_get_field(db,rec,k);
        if (wg_get_encoded_type(db,enc)!=WG_INTTYPE) {
          if (p) printf("check_datatype_writeread gave error: data read is not an int as stored for j %d field %d\n",
                        j,k);
          return 1;
        }
        intdec=wg_decode_int(db,enc);
        if (intdec!=10*k) {
          if (p) printf("check_datatype_writeread gave error: int %d read and decoded is not a stored int %d for j %d field %d\n",
                        intdec,10*k,j,k);
          return 1;
        }
      }
    }

    // var test

    for (j=0;j<vardata_nr;j++) {
      if (p>1) printf("checking var enc/dec for j %d, value %d\n",j,vardata[j]);
      enc=wg_encode_var(db,vardata[j]);
      if (wg_get_encoded_type(db,enc)!=WG_VARTYPE) {
        if (p) printf("check_datatype_writeread gave error: var enc not right type for j %d value %d\n",
                      j,vardata[j]);
        return 1;
      }
      intdec=wg_decode_var(db,enc);
      if (vardata[j]!=intdec) {
        if (p) printf("check_datatype_writeread gave error: var enc/dec for j %d enc value %d dec value %d\n",
                     j,vardata[j],intdec);
        return 1;
      }
    }

    /* Test string decode with insufficient buffer size */
    if (p>1) printf("checking decoding data that doesn't fit the decode buffer "\
      "(expecting some errors)\n");
    enc=wg_encode_str(db, "00000000001111111111", NULL); /* shortstr, len=20 */
    memset(decbuf, 0, decbuflen);
    if(wg_decode_str_copy(db, enc, decbuf, 10) > 0) {
      /* we expect this to fail, but if it succeeds, let's check if the
       * buffer size was honored */
      if(strlen(decbuf) != 10) {
        if(p)
          printf("check_datatype_writeread gave error: "\
            "buffer overflow when decoding a shortstr\n");
        return 1;
      }
    }

    enc=wg_encode_str(db, "0000000000111111111", "et"); /* longstr, len=19 */
    memset(decbuf, 0, decbuflen);
    if(wg_decode_str_copy(db, enc, decbuf, 11) > 0) {
      if(strlen(decbuf) != 11) {
        if(p)
          printf("check_datatype_writeread gave error: "\
            "buffer overflow when decoding a longstr\n");
        return 1;
      }
    }

    enc=wg_encode_blob(db, "000000000011111111", "blobtype", 18); /* blob */
    memset(decbuf, 0, decbuflen);
    if(wg_decode_blob_copy(db, enc, decbuf, 12) > 0) {
      if(strlen(decbuf) != 12) {
        if(p)
          printf("check_datatype_writeread gave error: "\
            "buffer overflow when decoding a blob\n");
        return 1;
      }
    }
  }

  if (p>1) printf("********* check_datatype_writeread ended without errors ************\n");
  return 0;
}

static int guarded_strlen(char* str) {
  if (str==NULL) return 0;
  else return strlen(str);
}

static int guarded_strcmp(char* a, char* b) {
  if (a==NULL && b!=NULL) return 1;
  if (a!=NULL && b==NULL) return -1;
  if (a==NULL && b==NULL) return 0;
  else return strcmp(a,b);
}

static int bufguarded_strcmp(char* a, char* b) {
  if (a==NULL && b==NULL) return 0;
  if (a==NULL && strlen(b)==0) return 0;
  if (b==NULL && strlen(a)==0) return 0;
  if (a==NULL && b!=NULL) return 1;
  if (a!=NULL && b==NULL) return -1;
  else return strcmp(a,b);
}


/* ------------------------ test record linking ------------------------------*/

static gint wg_check_backlinking(void* db, int printlevel) {
#ifdef USE_BACKLINKING
  int p;
  int tmp;
  gint *rec, *rec2, *rec3, *parent;

  p = printlevel;

  if (p>1)
    printf("********* checking record linking and deleting ************\n");
  rec=(gint *) wg_create_record(db,2);
  rec2=(gint *) wg_create_record(db,2);
  rec3=(gint *) wg_create_record(db,2);
  if (rec==NULL || rec2==NULL || rec3==NULL) {
    if (p) printf("unexpected error: rec creation failed\n");
    return 1;
  }

  wg_set_field(db, rec, 0, wg_encode_int(db, 10));
  wg_set_field(db, rec, 1, wg_encode_str(db, "hello", NULL));
  wg_set_field(db, rec2, 1, wg_encode_str(db, "hi", NULL));
  wg_set_field(db, rec3, 0, wg_encode_record(db, rec2));
  wg_set_field(db, rec3, 1, wg_encode_record(db, rec));
  wg_set_field(db, rec2, 0, wg_encode_record(db, rec));

  /* rec3 does not have parents */
  if(wg_get_first_parent(db, rec3) != NULL) {
    if (p) printf("check_backlinking: non-referenced record had a parent");
    return 1;
  }

  /* rec2 has one parent */
  parent = wg_get_first_parent(db, rec2);
  if(parent != rec3) {
    if (p) printf("check_backlinking: record had an invalid parent");
    return 1;
  }
  if(wg_get_next_parent(db, rec2, parent) != NULL) {
    if (p) printf("check_backlinking: record had too many parents");
    return 1;
  }

  /* rec has two parents */
  parent = wg_get_first_parent(db, rec);
  if(parent != rec3) {
    if (p) printf("check_backlinking: record had an invalid parent");
    return 1;
  }
  if((parent = wg_get_next_parent(db, rec, parent)) != rec2) {
    if (p) printf("check_backlinking: record had an invalid parent");
    return 1;
  }
  if(wg_get_next_parent(db, rec, parent) != NULL) {
    if (p) printf("check_backlinking: record had too many parents");
    return 1;
  }

  /* this should fail */
  tmp = wg_delete_record(db, rec);
  if(tmp != -1) {
    if (p) printf("check_backlinking: deleting referenced record, expected %d, received %d\n",
      -1, (int) tmp);
    return 1;
  }

  /* this should also fail */
  tmp = wg_delete_record(db, rec2);
  if(tmp != -1) {
    if (p) printf("check_backlinking: deleting referenced record, expected %d, received %d\n",
      -1, (int) tmp);
    return 1;
  }

  wg_set_field(db, rec3, 0, 0);

  /* rec2 no longer has parents */
  if(wg_get_first_parent(db, rec2) != NULL) {
    if (p) printf("check_backlinking: non-referenced record had a parent");
    return 1;
  }

  wg_set_field(db, rec3, 1, 0);

  /* rec now has one parent */
  parent = wg_get_first_parent(db, rec);
  if(parent != rec2) {
    if (p) printf("check_backlinking: record had an invalid parent");
    return 1;
  }
  if(wg_get_next_parent(db, rec, parent) != NULL) {
    if (p) printf("check_backlinking: record had too many parents");
    return 1;
  }

  /* this should now succeed */
  tmp = wg_delete_record(db, rec2);
  if(tmp != 0) {
    if (p) printf("check_backlinking: deleting no longer referenced record, expected %d, received %d\n",
      0, (int) tmp);
    return 1;
  }

  /* this should also succeed */
  tmp = wg_delete_record(db, rec);
  if(tmp != 0) {
    if (p) printf("check_backlinking: deleting child of deleted record, expected %d, received %d\n",
      0, (int) tmp);
    return 1;
  }

  /* and this should succeed */
  tmp = wg_delete_record(db, rec3);
  if(tmp != 0) {
    if (p) printf("check_backlinking: deleting record, expected %d, received %d\n",
      0, (int) tmp);
    return 1;
  }
  if (p>1) printf("********* check_backlinking: no errors ************\n");
#else
  printf("check_backlinking: disabled, skipping checks\n");
#endif
  return 0;
}

/* ------------------------ test string parsing ------------------------------*/

static int do_check_parse_encode(void *db, gint enc, gint exptype, void *expval,
                                                        int printlevel) {
  int i, p=printlevel, tmp;
  gint intdec;
  double doubledec, diff;
  char* strdec;
  int vecdec[4];
  gint enctype;

  enctype = wg_get_encoded_type(db, enc);
  if(enctype != exptype) {
    if(p)
      printf("check_parse_encode: expected type %s, got type %s\n",
        wg_get_type_name(db, exptype),
        wg_get_type_name(db, enctype));
    return 1;
  }
  switch(enctype) {
    case WG_NULLTYPE:
      if(wg_decode_null(db, enc) != NULL) {
        if(p)
          printf("check_parse_encode: expected value NULL, got %d (encoded)\n",
            (int) enc);
        return 1;
      }
      break;
    case WG_INTTYPE:
      intdec = wg_decode_int(db, enc);
      if(intdec != *((gint *) expval)) {
        if(p)
          printf("check_parse_encode: expected value %d, got %d\n",
            (int) *((gint *) expval), (int) intdec);
        return 1;
      }
      break;
    case WG_DOUBLETYPE:
      doubledec = wg_decode_double(db, enc);
      diff = doubledec - *((double *) expval);
      if(diff < -0.000001 || diff > 0.000001) {
        if(p)
          printf("check_parse_encode: expected value %f, got %f\n",
            *((double *) expval), doubledec);
        return 1;
      }
      break;
    case WG_STRTYPE:
      strdec = wg_decode_str(db, enc);
      if(bufguarded_strcmp(strdec, (char *) expval)) {
        if(p)
          printf("check_parse_encode: expected value \"%s\", got \"%s\"\n",
            (char *) expval, strdec);
        return 1;
      }
      break;
    case WG_DATETYPE:
      tmp = wg_decode_date(db, enc);
      wg_date_to_ymd(db, tmp, &vecdec[0], &vecdec[1], &vecdec[2]);
      for(i=0; i<3; i++) {
        if(vecdec[i] != ((int *) expval)[i]) {
          if(p)
            printf("check_parse_encode: "\
              "date vector pos %d expected value %d, got %d\n",
              i, ((int *) expval)[i], vecdec[i]);
          return 1;
        }
      }
      break;
    case WG_TIMETYPE:
      tmp = wg_decode_time(db, enc);
      wg_time_to_hms(db, tmp, &vecdec[0], &vecdec[1], &vecdec[2], &vecdec[3]);
      for(i=0; i<4; i++) {
        if(vecdec[i] != ((int *) expval)[i]) {
          if(p)
            printf("check_parse_encode: "\
              "time vector pos %d expected value %d, got %d\n",
              i, ((int *) expval)[i], vecdec[i]);
          return 1;
        }
      }
      break;
    default:
      printf("check_parse_encode: unexpected type %s\n",
        wg_get_type_name(db, enctype));
      return 1;
  }

  return 0;
}

static gint wg_check_parse_encode(void* db, int printlevel) {
  int p, i;

  const char *testinput[] = {
    "", /* empty string - NULL */
    " ", /* space - string */
    "\r\t \n\r\t  \b\xff", /* various whitespace and other junk */
    "üöäõõõü ÄÖÜÕ", /* ISO-8859-1 encoded string */
    "\xc3\xb5\xc3\xa4\xc3\xb6\xc3\xbc \xc3\x95\xc3\x84\xc3\x96\xc3\x9c", /* UTF-8 */
    "0", /* integer */
    "5435354534", /* a large integer, parsed as string if strtol() is 32-bit */
    "54312313214385290438390523442348932048234324348930243242342342389"\
      "4380148902432428904283323892374282394832423", /* a very large integer */
    "7.432432", /* floating point (CSV_DECIMAL_SEPARATOR in dbutil.c) */
    "-7899", /* negative integer */
    "-14324.432432", /* negative floating point number */
    "-tere", /* something that is not a negative number */
    "0.88872d", /* a number with garbage appended */
    " 995", /* a number that is parsed as a string */
    "1996-01-01", /* iso8601 date */
    "2038-12-12", /* same, in the future */
    "12:01:17", /* iso8601 time */
    "23:01:17.87", /* iso8601 time, with fractions */
    "09:01", /* time, no seconds */
    NULL /* terminator */
  };

  /* verification data */
  gint intval[] = {
    0,
    (sizeof(long) > 4 ? (gint) 5435354534L : 0),
    -7899
  };
  double doubleval[] = {
    7.432432,
    -14324.432432
  };
  int datevec[][3] = {
    {1996, 1, 1},
    {2038, 12, 12}
  };
  int timevec[][4] = {
    {12, 1, 17, 0},
    {23, 1, 17, 87}
  };

  /* should match testinput */
  gint testtype[] = {
    WG_NULLTYPE,
    WG_STRTYPE,
    WG_STRTYPE,
    WG_STRTYPE,
    WG_STRTYPE,
    WG_INTTYPE,
    (sizeof(long) > 4 ? WG_INTTYPE : WG_STRTYPE),
    WG_STRTYPE,
    WG_DOUBLETYPE,
    WG_INTTYPE,
    WG_DOUBLETYPE,
    WG_STRTYPE,
    WG_STRTYPE,
    WG_STRTYPE,
    WG_DATETYPE,
    WG_DATETYPE,
    WG_TIMETYPE,
    WG_TIMETYPE,
    WG_STRTYPE,
    -1, /* unused */
  };

  /* map to verification data, recast to correct type when used */
  void *testval[] = {
    NULL, /* unused */
    (void *) testinput[1],
    (void *) testinput[2],
    (void *) testinput[3],
    (void *) testinput[4],
    (void *) &intval[0],
    (sizeof(long) > 4 ? (void *) &intval[1] : (void *) testinput[6]),
    (void *) testinput[7],
    (void *) &doubleval[0],
    (void *) &intval[2],
    (void *) &doubleval[1],
    (void *) testinput[11],
    (void *) testinput[12],
    (void *) testinput[13],
    (void *) datevec[0],
    (void *) datevec[1],
    (void *) timevec[0],
    (void *) timevec[1],
    (void *) testinput[18],
    NULL /* unused */
  };

  p=printlevel;

  if (p>1) printf("********* testing string parsing ************\n");

  i=0;
  while(testinput[i]) {
    gint encv, encp;

    /* Announce */
    if(p>1) {
      printf("parsing string: \"%s\"\n", testinput[i]);
    }

    /* Parse and encode */
    encv = wg_parse_and_encode(db, (char *) testinput[i]);
    encp = wg_parse_and_encode_param(db, (char *) testinput[i]);

    /* Check */
    if(encv == WG_ILLEGAL) {
      if(p)
        printf("check_parse_encode: encode value failed, got WG_ILLEGAL\n");
      return 1;
    } else if(do_check_parse_encode(db, encv, testtype[i], testval[i], p)) {
      return 1;
    }
    if(encp == WG_ILLEGAL) {
      if(p)
        printf("check_parse_encode: encode param failed, got WG_ILLEGAL\n");
      return 1;
    } else if(do_check_parse_encode(db, encp, testtype[i], testval[i], p)) {
      return 1;
    }

    /* Free */
    wg_free_encoded(db, encv);
    wg_free_query_param(db, encp);
    i++;
  }

  if (p>1) printf("********* check_parse_encode: no errors ************\n");
  return 0;
}

/* ------------------------ test comparison ------------------------------*/

static gint wg_check_compare(void* db, int printlevel) {
  int i, j;
  gint testdata[28];
  void *rec1, *rec2, *rec3;

  testdata[0] = wg_encode_null(db, 0);

  testdata[4] = wg_encode_int(db, -321784);
  testdata[5] = wg_encode_int(db, 34531);

  testdata[6] = wg_encode_double(db, 0.000000001);
  testdata[7] = wg_encode_double(db, 0.00000001);

  testdata[8] = wg_encode_str(db, "", NULL);
  testdata[9] = wg_encode_str(db, "XX", NULL);
  testdata[10] = wg_encode_str(db, "this is a string", NULL);
  testdata[11] = wg_encode_str(db, "this is a string ", NULL);

  testdata[12] = wg_encode_xmlliteral(db, "this is a string ", "foo:bar");
  testdata[13] = wg_encode_xmlliteral(db, "this is a string ", "foo:bart");

  testdata[14] = wg_encode_uri(db, "www.amazon.com", "http://");
  testdata[15] = wg_encode_uri(db, "www.yahoo.com", "http://");

  testdata[16] = wg_encode_blob(db,
    "\0\0\045\120\104\106\055\061\0\056\065\012\045\045", "blob", 14);
  testdata[17] = wg_encode_blob(db,
    "\0\0\045\120\104\106\055\061\001\056\065\012\044", "blob", 13);

  testdata[18] = wg_encode_char(db, 'C');
  testdata[19] = wg_encode_char(db, 'c');

  testdata[20] = wg_encode_fixpoint(db, -7.25);
  testdata[21] = wg_encode_fixpoint(db, -7.2);

  testdata[22] = wg_encode_date(db, wg_ymd_to_date(db, 2010, 4, 1));
  testdata[23] = wg_encode_date(db, wg_ymd_to_date(db, 2010, 4, 30));

  testdata[24] = wg_encode_time(db, wg_hms_to_time(db, 13, 32, 0, 3));
  testdata[25] = wg_encode_time(db, wg_hms_to_time(db, 24, 0, 0, 0));

  testdata[26] = wg_encode_var(db, 7);
  testdata[27] = wg_encode_var(db, 10);

  /* create records in reverse order to catch offset comparison */
  rec3 = wg_create_raw_record(db, 3);
  wg_set_new_field(db, rec3, 0, testdata[4]);
  wg_set_new_field(db, rec3, 1, testdata[23]);
  wg_set_new_field(db, rec3, 2, testdata[9]);

  rec2 = wg_create_raw_record(db, 3);
  wg_set_new_field(db, rec2, 0, testdata[4]);
  wg_set_new_field(db, rec2, 1, testdata[14]);
  wg_set_new_field(db, rec2, 2, testdata[9]);

  rec1 = wg_create_raw_record(db, 2);

  testdata[1] = wg_encode_record(db, rec1);
  testdata[2] = wg_encode_record(db, rec2);
  testdata[3] = wg_encode_record(db, rec3);

  if(printlevel>1)
    printf("********* testing data comparison ************\n");

  for(i=0; i<26; i++) {
    for(j=i; j<26; j++) {
      if(i==j) {
        if(WG_COMPARE(db, testdata[i], testdata[j]) != WG_EQUAL) {
          if(printlevel) {
            printf("value1: ");
            wg_debug_print_value(db, testdata[i]);
            printf(" value2: ");
            wg_debug_print_value(db, testdata[j]);
            printf("\nvalue1 and value2 should have been equal\n");
          }
          return 1;
        }
      } else
#if WG_COMPARE_REC_DEPTH < 2
        if(wg_get_encoded_type(db, testdata[i]) != \
           wg_get_encoded_type(db, testdata[j]) || \
           wg_get_encoded_type(db, testdata[i]) != WG_RECORDTYPE)
#endif
      {
        if(WG_COMPARE(db, testdata[i], testdata[j]) != WG_LESSTHAN) {
          if(printlevel) {
            printf("value1: ");
            wg_debug_print_value(db, testdata[i]);
            printf(" value2: ");
            wg_debug_print_value(db, testdata[j]);
            printf("\nvalue1 should have been less than value2\n");
          }
          return 1;
        }

        if(WG_COMPARE(db, testdata[j], testdata[i]) != WG_GREATER) {
          if(printlevel) {
            printf("value1: ");
            wg_debug_print_value(db, testdata[i]);
            printf(" value2: ");
            wg_debug_print_value(db, testdata[j]);
            printf("\nvalue1 should have been greater than value2\n");
          }
          return 1;
        }
      }
    }
  }

  if(printlevel>1)
    printf("********* check_compare: no errors ************\n");
  return 0;
}

/* -------------------- test query parameter encoding --------------------*/

static gint wg_check_query_param(void* db, int printlevel) {
  gint encv, encp, tmp;
  int i;
  char *strdata[] = {
    "RjlTKUoxfhdqLiIz",
    "llWsdbuVGhoGqjs",
    "HRmUHyBkMKiqsu",
    "NcDoCfVjFPgWh",
    "ESGgFsyEcGLI",
    "PxPGipbFQgq",
    "UdDVsnFVKA",
    "JnhQcGTnC",
    "KxKPyzju",
    NULL
  };

  if(printlevel>1)
    printf("********* testing query parameter encoding ************\n");

  /* Data that does not require storage allocation */
  encv = wg_encode_null(db, 0);
  encp = wg_encode_query_param_null(db, 0);
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded NULL parameter (%d)"\
        "was not equal to encoded NULL value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  encv = wg_encode_char(db, 'X');
  encp = wg_encode_query_param_char(db, 'X');
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded char parameter (%d) "\
        "was not equal to encoded char value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  encv = wg_encode_fixpoint(db, 37.596);
  encp = wg_encode_query_param_fixpoint(db, 37.596);
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded fixpoint parameter (%d) "\
        "was not equal to encoded fixpoint value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  tmp = wg_ymd_to_date(db, 1859, 7, 13);
  encv = wg_encode_date(db, tmp);
  encp = wg_encode_query_param_date(db, tmp);
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded date parameter (%d) "\
        "was not equal to encoded date value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  tmp = wg_hms_to_time(db, 17, 15, 0, 0);
  encv = wg_encode_time(db, tmp);
  encp = wg_encode_query_param_time(db, tmp);
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded time parameter (%d) "\
        "was not equal to encoded time value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  encv = wg_encode_var(db, 2);
  encp = wg_encode_query_param_var(db, 2);
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded var parameter (%d) "\
        "was not equal to encoded var value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  /* Smallint */
  encv = wg_encode_int(db, 77);
  encp = wg_encode_query_param_int(db, 77);
  if(encv != encp) {
    if(printlevel) {
      printf("check_query_param: encoded int parameter (%d) "\
        "was not equal to encoded int value (%d)\n",
        (int) encp, (int) encv);
    }
    return 1;
  }

  /* Data that requires storage */
  if(sizeof(gint) > 4) {
    tmp = (gint) 3152921502073741877L;
  } else {
    tmp = 2073741877;
  }
  encp = wg_encode_query_param_int(db, tmp);
  if(!isfullint(encp)) {
    if(printlevel) {
      printf("check_query_param: encoded int parameter (%d) "\
        "had bad encoding (does not look like a full int)\n",
        (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  }
  if((gint) (dbfetch(db, decode_fullint_offset(encp))) != tmp) {
    if(printlevel) {
      printf("check_query_param: encoded int parameter (%d) "\
        "contained an invalid value\n", (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  }

  tmp = decode_fullint_offset(encp);
  if(tmp > 0 && tmp < dbmemsegh(db)->free) {
    if(printlevel) {
      printf("check_query_param: encoded int parameter (%d) "\
        "had an invalid offset\n", (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  }
  wg_free_query_param(db, encp);

  encp = wg_encode_query_param_double(db, 0.00000000000324445);
  if(!isfulldouble(encp)) {
    if(printlevel) {
      printf("check_query_param: encoded double parameter (%d) "\
        "had bad encoding (does not look like a double)\n",
        (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  } else {
    double val = wg_decode_double(db, encp);
    double diff = val - 0.00000000000324445;
    if(diff > 0.00000000000000001 || diff < -0.00000000000000001) {
      if(printlevel) {
        printf("check_query_param: encoded double parameter (%d) "\
          "contained an invalid value (delta: %f)\n",
            (int) encp, diff);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    tmp = decode_fulldouble_offset(encp);
    if(tmp > 0 && tmp < dbmemsegh(db)->free) {
      if(printlevel) {
        printf("check_query_param: encoded double parameter (%d) "\
          "had an invalid offset\n", (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
  }
  wg_free_query_param(db, encp);

  encp = wg_encode_query_param_str(db,
    "lalalalalalalalalalalalalalalalalalalala", NULL);
  if(!isshortstr(encp)) {
    if(printlevel) {
      printf("check_query_param: encoded longstr parameter (%d) "\
        "had bad encoding (should be encoded as shortstr)\n",
        (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  } else {
    char *val = wg_decode_str(db, encp);
    if(strcmp(val, "lalalalalalalalalalalalalalalalalalalala")) {
      if(printlevel) {
        printf("check_query_param: encoded longstr parameter (%d) "\
          "decoded to an invalid value \"%s\"\n",
          (int) encp, val);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(wg_decode_str_len(db, encp) != 40) {
      if(printlevel) {
        printf("check_query_param: encoded longstr parameter (%d) "\
          "had invalid length\n", (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    tmp = decode_shortstr_offset(encp);
    if(tmp > 0 && tmp < dbmemsegh(db)->free) {
      if(printlevel) {
        printf("check_query_param: encoded longstr parameter (%d) "\
          "had an invalid offset\n",
          (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
  }
  wg_free_query_param(db, encp);

  encp = wg_encode_query_param_str(db, "", NULL);
  if(wg_get_encoded_type(db, encp) != WG_STRTYPE) {
    if(printlevel) {
      printf("check_query_param: encoded empty string parameter (%d) "\
        "had bad type (should be WG_STRTYPE)\n",
        (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  } else {
    char *val = wg_decode_str(db, encp);
    if(strcmp(val, "")) {
      if(printlevel) {
        printf("check_query_param: encoded empty string parameter (%d) "\
          "decoded to an invalid value \"%s\"\n",
          (int) encp, val);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(wg_decode_str_len(db, encp) != 0) {
      if(printlevel) {
        printf("check_query_param: encoded empty string parameter (%d) "\
          "had invalid length\n", (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    tmp = decode_shortstr_offset(encp);
    if(tmp > 0 && tmp < dbmemsegh(db)->free) {
      if(printlevel) {
        printf("check_query_param: encoded empty string parameter (%d) "\
          "had an invalid offset\n",
          (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
  }
  wg_free_query_param(db, encp);

  i=0;
  while(strdata[i]) {
    encp = wg_encode_query_param_str(db, strdata[i], "et");
    if(!islongstr(encp)) {
      if(printlevel) {
        printf("check_query_param: encoded string parameter (%d) "\
          "had bad encoding (should be encoded as longstr)\n",
          (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    } else {
      char *val = wg_decode_str(db, encp);
      char cbuf[17];
      int strl = strlen(strdata[i]), encl;

      if(strcmp(val, strdata[i])) {
        if(printlevel) {
          printf("check_query_param: encoded string parameter (%d) "\
            "decoded to an invalid value \"%s\"\n",
            (int) encp, val);
        }
        wg_free_query_param(db, encp);
        return 1;
      }
      if((encl = wg_decode_str_len(db, encp)) != strl) {
        if(printlevel) {
          printf("check_query_param: encoded string parameter \"%s\" "\
            "had invalid length (%d != %d)\n", strdata[i], encl, strl);
        }
        wg_free_query_param(db, encp);
        return 1;
      }

      if(wg_decode_str_copy(db, encp, cbuf, 17) != strl) {
        if(printlevel) {
          printf("check_query_param: wg_decode_str_copy(): invalid length\n");
        }
        wg_free_query_param(db, encp);
        return 1;
      }
      if(strcmp(cbuf, strdata[i])) {
        if(printlevel) {
          printf("check_query_param: copy of encoded string parameter (%d) "\
            "is an invalid value \"%s\"\n",
            (int) encp, cbuf);
        }
        wg_free_query_param(db, encp);
        return 1;
      }

      val = wg_decode_str_lang(db, encp);
      if(strcmp(val, "et")) {
        if(printlevel) {
          printf("check_query_param: encoded string parameter (%d) "\
            "had invalid language \"%s\"\n",
            (int) encp, val);
        }
        wg_free_query_param(db, encp);
        return 1;
      }
      if(wg_decode_str_lang_len(db, encp) != 2) {
        if(printlevel) {
          printf("check_query_param: encoded string parameter (%d) language "\
            "had invalid length\n", (int) encp);
        }
        wg_free_query_param(db, encp);
        return 1;
      }

      if(wg_decode_str_lang_copy(db, encp, cbuf, 17) != 2) {
        if(printlevel) {
          printf("check_query_param: wg_decode_str_lang_copy(): "\
            "invalid length\n");
        }
        wg_free_query_param(db, encp);
        return 1;
      }
      if(strcmp(cbuf, "et")) {
        if(printlevel) {
          printf("check_query_param: copy of encoded string parameter's (%d) "\
            "language is an invalid value \"%s\"\n",
            (int) encp, cbuf);
        }
        wg_free_query_param(db, encp);
        return 1;
      }

      tmp = decode_longstr_offset(encp);
      if(tmp > 0 && tmp < dbmemsegh(db)->free) {
        if(printlevel) {
          printf("check_query_param: encoded string parameter (%d) "\
            "had an invalid offset\n",
            (int) encp);
        }
        wg_free_query_param(db, encp);
        return 1;
      }
    }
    wg_free_query_param(db, encp);
    i++;
  }

  encp = wg_encode_query_param_xmlliteral(db,
    "VwwEtCiQQLvcIoB", "ACWzCMGGFVcZBjk");
  if(!islongstr(encp)) {
    if(printlevel) {
      printf("check_query_param: encoded string parameter (%d) "\
        "had bad encoding (should be encoded as longstr)\n",
        (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  } else {
    char *val = wg_decode_xmlliteral(db, encp);
    char cbuf[16];
    int encl;

    if(strcmp(val, "VwwEtCiQQLvcIoB")) {
      if(printlevel) {
        printf("check_query_param: encoded XML literal param (%d) "\
          "decoded to an invalid value \"%s\"\n",
          (int) encp, val);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if((encl = wg_decode_xmlliteral_len(db, encp)) != 15) {
      if(printlevel) {
        printf("check_query_param: encoded XML literal param \"%s\" "\
          "had invalid length (%d != 15)\n", strdata[i], encl);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    if(wg_decode_xmlliteral_copy(db, encp, cbuf, 16) != 15) {
      if(printlevel) {
        printf("check_query_param: wg_decode_xmlliteral_copy(): "\
          "invalid length\n");
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(strcmp(cbuf, "VwwEtCiQQLvcIoB")) {
      if(printlevel) {
        printf("check_query_param: copy of encoded XML literal param (%d) "\
          "is an invalid value \"%s\"\n",
          (int) encp, cbuf);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    val = wg_decode_xmlliteral_xsdtype(db, encp);
    if(strcmp(val, "ACWzCMGGFVcZBjk")) {
      if(printlevel) {
        printf("check_query_param: encoded XML literal param (%d) "\
          "had invalid language \"%s\"\n",
          (int) encp, val);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(wg_decode_xmlliteral_xsdtype_len(db, encp) != 15) {
      if(printlevel) {
        printf("check_query_param: encoded XML literal param (%d) type "\
          "had invalid length\n", (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    if(wg_decode_xmlliteral_xsdtype_copy(db, encp, cbuf, 16) != 15) {
      if(printlevel) {
        printf("check_query_param: wg_decode_xmlliteral_xsdtype_copy(): "\
          "invalid length\n");
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(strcmp(cbuf, "ACWzCMGGFVcZBjk")) {
      if(printlevel) {
        printf("check_query_param: copy of encoded XML literal param's "\
          "(%d) type is an invalid value \"%s\"\n",
          (int) encp, cbuf);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    tmp = decode_longstr_offset(encp);
    if(tmp > 0 && tmp < dbmemsegh(db)->free) {
      if(printlevel) {
        printf("check_query_param: encoded XML literal param (%d) "\
          "had an invalid offset\n",
          (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
  }
  wg_free_query_param(db, encp);

  encp = wg_encode_query_param_uri(db,
    "GCwepgqnKqcxnTj", "WdszkaEjrhEjgNS");
  if(!islongstr(encp)) {
    if(printlevel) {
      printf("check_query_param: encoded string parameter (%d) "\
        "had bad encoding (should be encoded as longstr)\n",
        (int) encp);
    }
    wg_free_query_param(db, encp);
    return 1;
  } else {
    char *val = wg_decode_uri(db, encp);
    char cbuf[16];
    int encl;

    if(strcmp(val, "GCwepgqnKqcxnTj")) {
      if(printlevel) {
        printf("check_query_param: encoded URI parameter (%d) "\
          "decoded to an invalid value \"%s\"\n",
          (int) encp, val);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if((encl = wg_decode_uri_len(db, encp)) != 15) {
      if(printlevel) {
        printf("check_query_param: encoded URI parameter \"%s\" "\
          "had invalid length (%d != 15)\n", strdata[i], encl);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    if(wg_decode_uri_copy(db, encp, cbuf, 16) != 15) {
      if(printlevel) {
        printf("check_query_param: wg_decode_uri_copy(): "\
          "invalid length\n");
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(strcmp(cbuf, "GCwepgqnKqcxnTj")) {
      if(printlevel) {
        printf("check_query_param: copy of encoded URI parameter (%d) "\
          "is an invalid value \"%s\"\n",
          (int) encp, cbuf);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    val = wg_decode_uri_prefix(db, encp);
    if(strcmp(val, "WdszkaEjrhEjgNS")) {
      if(printlevel) {
        printf("check_query_param: encoded URI parameter (%d) "\
          "had invalid language \"%s\"\n",
          (int) encp, val);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(wg_decode_uri_prefix_len(db, encp) != 15) {
      if(printlevel) {
        printf("check_query_param: encoded URI parameter (%d) type "\
          "had invalid length\n", (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    if(wg_decode_uri_prefix_copy(db, encp, cbuf, 16) != 15) {
      if(printlevel) {
        printf("check_query_param: wg_decode_uri_prefix_copy(): "\
          "invalid length\n");
      }
      wg_free_query_param(db, encp);
      return 1;
    }
    if(strcmp(cbuf, "WdszkaEjrhEjgNS")) {
      if(printlevel) {
        printf("check_query_param: copy of encoded URI parameter's "\
          "(%d) type is an invalid value \"%s\"\n",
          (int) encp, cbuf);
      }
      wg_free_query_param(db, encp);
      return 1;
    }

    tmp = decode_longstr_offset(encp);
    if(tmp > 0 && tmp < dbmemsegh(db)->free) {
      if(printlevel) {
        printf("check_query_param: encoded URI parameter (%d) "\
          "had an invalid offset\n",
          (int) encp);
      }
      wg_free_query_param(db, encp);
      return 1;
    }
  }
  wg_free_query_param(db, encp);

  if(printlevel>1)
    printf("********* check_query_param: no errors ************\n");
  return 0;
}

/* --------------- allocation, storage, updafe and deallocation tests ---------- */
/*
gint wg_check_allocation_deallocation(void* db, int printlevel) {
  rec* records[1000];
  gint strs[1000];
  int count;
  int n;
  int i;
  int j;
  char tmpstr[1000];
  char str;

  count=2;
  n=3;

  for(i=0;i<count;i++) {

    records[i]=wg_create_record(db,n);
    for(j=0;j<n;j++) {
      sprintf(tmpstr,"test%d",j);
      strs[j]=wg_encode_str(tmpstr);
      wg_set_field(db,j,strs[j]);
    }
    for(j=0;j<n;j++) {
      sprintf(tmpstr,"test%d",j);
      str=wg_decode_str(wg_get_);
      wg_set_field(db,j,strs[j]);
    }

  }


}
*/


/* --------------- string hash reading and testing ------------------------------*/

static gint wg_check_strhash(void* db, int printlevel) {
  int p;
  int i,j;
  char* lang;
  /*gint strs[100];
  int strcount=0;*/
  char instrbuf[200];
  gint* recarr[10];
  int recarrcnt=0;
  gint enc;
  gint* rec;
  int records=3;
  int flds=2;

  p=printlevel;
  if (p>1) printf("********* testing strhash ********** \n");
  /*for(i=0;i<100;i++) strs[i]=0;*/

  if (p>1) printf("---------- initial hashtable -----------\n");
  if (p>1) wg_show_strhash(db);

  if (p>1) printf("---------- testing str creation --------- \n");
  for (i=0;i<records;i++) {
    rec=(gint *) wg_create_record(db,flds);
    if (rec==NULL) {
      if (p) printf("rec creation error in wg_check_strhash i %d\n",i);
      return 1;
    }
    recarr[recarrcnt]=rec;
    recarrcnt++;
    for(j=0;j<flds;j++) {
      snprintf(instrbuf,100,"h%da1234567890123456789012345678901234567890",i);
      if (i==1) lang=NULL;
      else lang="en";
      //lang="enasasASAS AASASAsASASASAS sASASASA ASASASAS ASASASAS aSASASAsASASASASAS ASASS 1231231231231212312312";
      //printf("-----------------------------\n");
      //printf("starting to encode %s %s \n",instrbuf,lang);
      enc=wg_encode_str(db,instrbuf,lang);
      //printf("encoding gave %d \n",enc);
      /*strs[strcount++]=enc;  */
      if (p>1) printf("wg_set_field rec %d fld %d str '%s' lang '%s' encoded %d\n",
                     (int)i,(int)j,instrbuf,lang,(int)enc);
      wg_set_field(db,rec,j,enc);
      if (!longstr_in_hash(db,instrbuf,lang,WG_STRTYPE,strlen(instrbuf)+1)) {
        if (p) printf("wg_check_strhash gave error: stored str not present in strhash: \"%s\" lang \"%s\" \n",instrbuf,lang);
        return 1;
      }
    }
  }

  if (p>1) printf("---------- hashtable after str adding -----------\n");
  if (p>1) wg_show_strhash(db);

  if (p>1) printf("---------- testing str removals by overwriting data --------- \n");
  for (i=0;i<recarrcnt;i++) {
    for(j=0;j<flds;j++) {
      enc=wg_encode_int(db,i*10+j);
      //printf("\nstoring %d to rec %d fld %d\n",i*10+j,i,j);
      wg_set_field(db,recarr[i],j,enc);
      //printf("removal result %d %d\n",i,j);
      //wg_show_strhash(db);
    }
  }
 /*
  for(i=strcount;i>=0;i--) {
    if (strs[i]!=0) {
      if (p>1) printf("removing str nr %d enc %d\n",i,strs[i]);
      j=wg_remove_from_strhash(db,strs[i]);
      if (p>1) printf("removal result %d\n",j);
      wg_show_strhash(db);
    }
  }
*/
  if (p>1) printf("---------- ending str removals, testing if strs removed from hash ----------\n");
  for (i=0;i<records;i++) {
    for(j=0;j<flds;j++) {
      snprintf(instrbuf,100,"h%da1234567890123456789012345678901234567890",i);
      if (i==1) lang=NULL;
      else lang="en";
      //lang="enasasASAS AASASAsASASASAS sASASASA ASASASAS ASASASAS aSASASAsASASASASAS ASASS 1231231231231212312312";
      //printf("-----------------------------\n");
      //printf("starting to encode %s %s \n",instrbuf,lang);
      if (longstr_in_hash(db,instrbuf,lang,WG_STRTYPE,strlen(instrbuf)+1)) {
        if (p) printf("wg_check_strhash gave error: created str still present in strhash: \"%s\" lang \"%s\" \n",instrbuf,lang);
        return 1;
      }
    }
  }

  if (p>1) printf("---------- hashtable after str removals -----------\n");
  if (p>1) wg_show_strhash(db);

  if (p>1)printf("********* strhash testing ended without errors ********** \n");
  return 0;
}



static gint longstr_in_hash(void* db, char* data, char* extrastr, gint type, gint length) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint old=0;
  int hash;
  gint hasharrel;

  if (0) {
  } else {
    // find hash, check if exists
    hash=wg_hash_typedstr(db,data,extrastr,type,length);
    //hasharrel=((gint*)(offsettoptr(db,((db->strhash_area_header).arraystart))))[hash];
    hasharrel=dbfetch(db,((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash));
    //printf("hash %d ((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash) %d hasharrel %d\n",
    //       hash,((dbh->strhash_area_header).arraystart)+(sizeof(gint)*hash), hasharrel);
    if (hasharrel) old=wg_find_strhash_bucket(db,data,extrastr,type,length,hasharrel);
    //printf("old %d \n",old);
    if (old) {
      //printf("str found in hash\n");
      return 1;
    }
    //printf("str not found in hash\n");
    return 0;
  }
}


static void wg_show_strhash(void* db) {
  db_memsegment_header* dbh = dbmemsegh(db);
  gint i;
  gint hashchain;
  /*gint lasthashchain;*/
  gint type;
  //gint offset;
  //gint refc;
  //int encoffset;

  printf("\nshowing strhash table and buckets\n");
  printf("-----------------------------------\n");
  printf("configured strhash size %d (%% of db size)\n",STRHASH_SIZE);
  printf("size %d\n", (int) (dbh->strhash_area_header).size);
  printf("offset %d\n", (int) (dbh->strhash_area_header).offset);
  printf("arraystart %d\n", (int) (dbh->strhash_area_header).arraystart);
  printf("arraylength %d\n", (int) (dbh->strhash_area_header).arraylength);
  printf("nonempty hash buckets:\n");
  for(i=0;i<(dbh->strhash_area_header).arraylength;i++) {
    hashchain=dbfetch(db,(dbh->strhash_area_header).arraystart+(sizeof(gint)*i));
    /*lasthashchain=hashchain;    */
    if (hashchain!=0) {
      printf("%d: contains %d encoded offset to chain\n",
        (int) i, (int) hashchain);
      for(;hashchain!=0;
          hashchain=dbfetch(db,decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint))) {
          //printf("hashchain %d decode_longstr_offset(hashchain) %d fulladr %d contents %d\n",
          //       hashchain,
          //       decode_longstr_offset(hashchain),
          //       (decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint)),
          //       dbfetch(db,decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint)));
          type=wg_get_encoded_type(db,hashchain);
          printf("  ");
          wg_debug_print_value(db,hashchain);
          printf("\n");
          //printf("  type %s",wg_get_type_name(db,type));
          if (type==WG_BLOBTYPE) {
            //printf(" len %d\n",wg_decode_str_len(db,hashchain));
          } else if (type==WG_STRTYPE || type==WG_XMLLITERALTYPE ||
                     type==WG_URITYPE || type== WG_ANONCONSTTYPE) {
          } else {
            printf("ERROR: wrong type in strhash bucket\n");
            exit(0);
          }
          /*lasthashchain=hashchain;*/
      }
    }
  }
}



/* --------------- allocation/memory checking and testing ------------------------------*/


/** check if varlen freelist is ok
*
* return 0 if ok, error nr if wrong
* in case of error an errmsg is printed and function returns immediately
*
*/


static gint wg_check_db(void* db) {
  gint res;
  db_memsegment_header* dbh = dbmemsegh(db);

  printf("\nchecking datarec area\n");
  printf("-----------------------\n");
  res=check_varlen_area(db,&(dbh->datarec_area_header));
  if (res) return res;
  printf("\narea test passed ok\n");
  printf("\nchecking longstr area\n");
  printf("-----------------------\n");
  res=check_varlen_area(db,&(dbh->longstr_area_header));
  if (res) return res;
  printf("\narea test passed ok\n");
  printf("\nwhole test passed ok\n");
  return 0;
}

static gint check_varlen_area(void* db, void* area_header) {
  gint res;

  res=check_varlen_area_markers(db,area_header);
  if (res) return res;
  res=check_varlen_area_dv(db,area_header);
  if (res) return res;
  res=check_varlen_area_freelist(db,area_header);
  if (res) return res;
  res=check_varlen_area_scan(db,area_header);
  if (res) return res;
  return 0;
}

static gint check_varlen_area_freelist(void* db, void* area_header) {
  db_area_header* areah;
  gint i;
  gint res;

  areah=(db_area_header*)area_header;
  for (i=0;i<EXACTBUCKETS_NR+VARBUCKETS_NR;i++) {
    if ((areah->freebuckets)[i]!=0) {
      //printf("checking bucket nr %d \n",i);
      if (i<EXACTBUCKETS_NR) {
        //printf(" is exactbucket at offset %d\n",dbaddr(db,&(areah->freebuckets)[i]));
        res=check_bucket_freeobjects(db,areah,i);
        if (res) return res;
      } else {
        //printf(" is varbucket at offset %d \n",dbaddr(db,&(areah->freebuckets)[i]));
        res=check_bucket_freeobjects(db,areah,i);
        if (res) return res;
      }
    }
  }
  return 0;
}


static gint check_bucket_freeobjects(void* db, void* area_header, gint bucketindex) {
  db_area_header* areah;
  gint freelist;
  gint size;
  gint nextptr;
  gint prevptr;
  gint prevfreelist;
  gint tmp;

  areah=(db_area_header*)area_header;
  freelist=(areah->freebuckets)[bucketindex];
  prevfreelist=ptrtooffset(db,&((areah->freebuckets)[bucketindex]));
  while(freelist!=0) {
    if (!isfreeobject(dbfetch(db,freelist))) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d has size gint %d which is not marked free\n",
              (int) freelist, (int) dbfetch(db,freelist));
      return 1;
    }
    size=getfreeobjectsize(dbfetch(db,freelist));
    if (bucketindex!=wg_freebuckets_index(db,size)) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d with size %d is in wrong bucket %d instead of right %d\n",
              (int) freelist, (int) size, (int) bucketindex,
              (int) wg_freebuckets_index(db,size));
      return 2;
    }
    if (getfreeobjectsize(dbfetch(db,freelist+size-sizeof(gint)))!=size) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d has wrong end size %d which is not same as start size %d\n",
              (int) freelist, (int) dbfetch(db,freelist+size-sizeof(gint)), (int) size);
      return 3;
    }
    nextptr=dbfetch(db,freelist+sizeof(gint));
    prevptr=dbfetch(db,freelist+2*sizeof(gint));
    if (prevptr!=prevfreelist) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d has a wrong prevptr: %d instead of %d\n",
              (int) freelist, (int) prevptr, (int) prevfreelist);
      return 4;
    }
    tmp=check_object_in_areabounds(db,area_header,freelist,size);
    if (tmp) {
      printf("varlen freelist object error:\n");
      if (tmp==1) {
        printf("object at offset %d does not start in the area bounds\n",
              (int) freelist);
        return 5;
      } else {
        printf("object at offset %d does not end (%d) in the same area it starts\n",
              (int) freelist, (int) (freelist+size));
        return 6;
      }
    }
    //printf("    ok freeobject offset %d end %d size %d nextptr %d prevptr %d \n",
    //        freelist,freelist+size,size,nextptr,prevptr);
    prevfreelist=freelist;
    freelist=nextptr;
  }
  return 0;
}


static gint check_varlen_area_markers(void* db, void* area_header) {
  /*db_subarea_header* arrayadr;*/
  db_area_header* areah;
  gint last_subarea_index;
  gint i;
  gint size;
  gint subareastart;
  /*gint subareaend;*/
  gint offset;
  gint head;

  areah=(db_area_header*)area_header;
  /*arrayadr=(areah->subarea_array);*/
  last_subarea_index=areah->last_subarea_index;
  for(i=0;(i<=last_subarea_index)&&(i<SUBAREA_ARRAY_SIZE);i++) {

    size=((areah->subarea_array)[i]).alignedsize;
    subareastart=((areah->subarea_array)[i]).alignedoffset;
    /*subareaend=(((areah->subarea_array)[i]).alignedoffset)+size;*/

    // start marker
    offset=subareastart;
    head=dbfetch(db,offset);
    if (!isspecialusedobject(head)) {
      printf("start marker at offset %d has head %d which is not specialusedobject\n",
              (int) offset, (int) head);
      return 21;
    }
    if (getspecialusedobjectsize(head)!=MIN_VARLENOBJ_SIZE) {
      printf("start marker at offset %d has size %d which is not MIN_VARLENOBJ_SIZE %d\n",
              (int) offset, (int) getspecialusedobjectsize(head), (int) MIN_VARLENOBJ_SIZE);
      return 22;
    }
    if (dbfetch(db,offset+sizeof(gint))!=SPECIALGINT1START) {
      printf("start marker at offset %d has second gint %d which is not SPECIALGINT1START %d\n",
              (int) offset, (int) dbfetch(db,offset+sizeof(gint)), SPECIALGINT1START );
      return 23;
    }

    //end marker
    offset=offset+size-MIN_VARLENOBJ_SIZE;
    head=dbfetch(db,offset);
    if (!isspecialusedobject(head)) {
      printf("end marker at offset %d has head %d which is not specialusedobject\n",
              (int) offset, (int) head);
      return 21;
    }
    if (getspecialusedobjectsize(head)!=MIN_VARLENOBJ_SIZE) {
      printf("end marker at offset %d has size %d which is not MIN_VARLENOBJ_SIZE %d\n",
              (int) offset, (int) getspecialusedobjectsize(head), (int) MIN_VARLENOBJ_SIZE);
      return 22;
    }
    if (dbfetch(db,offset+sizeof(gint))!=SPECIALGINT1END) {
      printf("end marker at offset %d has second gint %d which is not SPECIALGINT1END %d\n",
              (int) offset, (int) dbfetch(db,offset+sizeof(gint)), SPECIALGINT1END );
      return 23;
    }
  }
  return 0;
}


static gint check_varlen_area_dv(void* db, void* area_header) {
  db_area_header* areah;
  gint dv;
  gint tmp;

  areah=(db_area_header*)area_header;
  dv=(areah->freebuckets)[DVBUCKET];
  if (dv!=0) {
    printf("checking dv: bucket nr %d at offset %d \ncontains dv at offset %d with size %d(%d) and end %d \n",
          DVBUCKET, (int) dbaddr(db,&(areah->freebuckets)[DVBUCKET]),
          (int) dv,
          (int) ((areah->freebuckets)[DVSIZEBUCKET]>0 ? dbfetch(db,(areah->freebuckets)[DVBUCKET]) : -1),
          (int) (areah->freebuckets)[DVSIZEBUCKET],
          (int) ((areah->freebuckets)[DVBUCKET]+(areah->freebuckets)[DVSIZEBUCKET]));
    if (!isspecialusedobject(dbfetch(db,dv))) {
      printf("dv at offset %d has head %d which is not marked specialusedobject\n",
              (int) dv, (int) dbfetch(db,dv));
      return 10;
    }
    if ((areah->freebuckets)[DVSIZEBUCKET]!=getspecialusedobjectsize(dbfetch(db,dv))) {
      printf("dv at offset %d has head %d with size %d which is different from freebuckets[DVSIZE] %d\n",
              (int) dv, (int) dbfetch(db,dv),
              (int) getspecialusedobjectsize(dbfetch(db,dv)),
              (int) (areah->freebuckets)[DVSIZEBUCKET]);
      return 11;
    }
    if (getspecialusedobjectsize(dbfetch(db,dv))<MIN_VARLENOBJ_SIZE) {
      printf("dv at offset %d has size %d which is smaller than MIN_VARLENOBJ_SIZE %d\n",
              (int) dv, (int) getspecialusedobjectsize(dbfetch(db,dv)), (int) MIN_VARLENOBJ_SIZE);
      return 12;
    }
    if (SPECIALGINT1DV!=dbfetch(db,dv+sizeof(gint))) {
      printf("dv at offset %d has second gint %d which is not SPECIALGINT1DV %d\n",
              (int) dv, (int) dbfetch(db,dv+sizeof(gint)), SPECIALGINT1DV);
      return 12;
    }
    tmp=check_object_in_areabounds(db,area_header,dv,getspecialusedobjectsize(dbfetch(db,dv)));
    if (tmp) {
      printf("dv error:\n");
      if (tmp==1) {
        printf("dv at offset %d does not start in the area bounds\n",
              (int) dv);
        return 13;
      } else {
        printf("dv at offset %d does not end (%d) in the same area it starts\n",
              (int) dv, (int) (dv+getspecialusedobjectsize(dbfetch(db,dv))));
        return 14;
      }
    }
  }
  return 0;
}

static gint check_object_in_areabounds(void* db,void* area_header,gint offset,gint size) {
  db_subarea_header* arrayadr;
  db_area_header* areah;
  gint last_subarea_index;
  gint found;
  gint i;
  gint subareastart;
  gint subareaend;

  areah=(db_area_header*)area_header;
  arrayadr=(areah->subarea_array);
  last_subarea_index=areah->last_subarea_index;
  found=0;
  for(i=0;(i<=last_subarea_index)&&(i<SUBAREA_ARRAY_SIZE);i++) {
    subareastart=((arrayadr[i]).alignedoffset);
    subareaend=((arrayadr[i]).alignedoffset)+((arrayadr[i]).alignedsize);
    if (offset>=subareastart && offset<subareaend) {
        if (offset+size<subareastart || offset+size>subareaend) {
          return 1;
        }
      found=1;
      break;
    }
  }
  if (!found) {
    return 2;
  } else {
    return 0;
  }
}


static gint check_varlen_area_scan(void* db, void* area_header) {
  db_area_header* areah;
  gint dv;
  gint tmp;
  /*db_subarea_header* arrayadr;*/
  gint firstoffset;

  gint curoffset;
  gint head;
  gint last_subarea_index;
  gint i;
  gint subareastart;
  gint subareaend;
  gint freemarker;
  gint dvmarker;

  gint usedcount=0;
  gint usedbytesrealcount=0;
  gint usedbyteswantedcount=0;
  gint freecount=0;
  gint freebytescount=0;
  gint dvcount=0;
  gint dvbytescount=0;
  gint size;
  /*gint offset;*/

  areah=(db_area_header*)area_header;
  /*arrayadr=(areah->subarea_array);*/
  last_subarea_index=areah->last_subarea_index;
  dv=(areah->freebuckets)[DVBUCKET];

  for(i=0;(i<=last_subarea_index)&&(i<SUBAREA_ARRAY_SIZE);i++) {

    size=((areah->subarea_array)[i]).alignedsize;
    subareastart=((areah->subarea_array)[i]).alignedoffset;
    subareaend=(((areah->subarea_array)[i]).alignedoffset)+size;

    // start marker
    /*offset=subareastart;      */
    firstoffset=subareastart; // do not skip initial "used" marker

    curoffset=firstoffset;
    //printf("curroffset %d record %x\n",curoffset,(uint)record);
    freemarker=0; //assume first object is a special in-use marker
    dvmarker=0; // assume first object is not dv
    head=dbfetch(db,curoffset);
    while(1) {
      // increase offset to next memory block
      curoffset=curoffset+(freemarker ? getfreeobjectsize(head) : getusedobjectsize(head));
      if (curoffset>=(subareastart+size)) {
        printf("object areanr %d offset %d size %d starts at or after area end %d\n",
                (int) i, (int) curoffset,
                (int) getusedobjectsize(head), (int) (subareastart+size));
           return 32;
      }
      head=dbfetch(db,curoffset);
      //printf("new curoffset %d head %d isnormaluseobject %d isfreeobject %d \n",
      //       curoffset,head,isnormalusedobject(head),isfreeobject(head));
      // check if found a normal used object
      if (isnormalusedobject(head)) {
        if (freemarker && !isnormalusedobjectprevfree(head)) {
           printf("inuse normal object areanr %d offset %d size %d follows free but is not marked to follow free\n",
                (int) i, (int) curoffset, (int) getusedobjectsize(head));
           return 31;
        } else if (!freemarker &&  !isnormalusedobjectprevused(head)) {
           printf("inuse normal object areanr %d offset %d size %d follows used but is not marked to follow used\n",
                (int) i, (int) curoffset, (int) getusedobjectsize(head));
           return 32;
        }
        tmp=check_varlen_object_infreelist(db,area_header,curoffset,0);
        if (tmp!=0) return tmp;
        freemarker=0;
        dvmarker=0;
        usedcount++;
        usedbytesrealcount+=getusedobjectsize(head);
        usedbyteswantedcount+=getfreeobjectsize(head); // just remove two lowest bits
      } else  if (isfreeobject(head)) {
        if (freemarker) {
           printf("free object areanr %d offset %d size %d follows free\n",
                (int) i, (int) curoffset, (int) getfreeobjectsize(head));
           return 33;
        }
        if (dvmarker) {
           printf("free object areanr %d offset %d size %d follows dv\n",
                (int) i, (int) curoffset, (int) getfreeobjectsize(head));
           return 34;
        }
        tmp=check_varlen_object_infreelist(db,area_header,curoffset,1);
        if (tmp!=1) {
           printf("free object areanr %d offset %d size %d not found in freelist\n",
                (int) i, (int) curoffset, (int) getfreeobjectsize(head));
           return 55;
        }
        freemarker=1;
        dvmarker=0;
        freecount++;
        freebytescount+=getfreeobjectsize(head);
        // loop start leads us to next object
      } else {
        // found a special object (dv or end marker)
        if (dbfetch(db,curoffset+sizeof(gint))==SPECIALGINT1DV) {
          // we have reached a dv object
          if (curoffset!=dv) {
            printf("dv object found areanr %d offset %d size %d not marked as in[DVBUCKET] %d\n",
                (int) i, (int) curoffset, (int) getspecialusedobjectsize(head), (int) dv);
            return 35;
          }
          if (dvcount!=0) {
            printf("second dv object found areanr %d offset %d size %d\n",
                (int) i, (int) curoffset, (int) getspecialusedobjectsize(head));
            return 36;
          }
          if (getspecialusedobjectsize(head)<MIN_VARLENOBJ_SIZE) {
            printf("second dv object found areanr %d offset %d size %d is smaller than MIN_VARLENOBJ_SIZE %d\n",
                (int) i, (int) curoffset,
                (int) getspecialusedobjectsize(head), (int) MIN_VARLENOBJ_SIZE);
            return 37;
          }
          if (freemarker) {
            printf("dv object areanr %d offset %d size %d follows free\n",
                (int) i, (int) curoffset, (int) getspecialusedobjectsize(head));
            return 38;
          }
          tmp=check_varlen_object_infreelist(db,area_header,curoffset,1);
          if (tmp!=0) return tmp;
          dvcount++;
          dvbytescount+=getspecialusedobjectsize(head);
          freemarker=0;
          dvmarker=1;
          // loop start leads us to next object
        } else {
          if (curoffset!=subareaend-MIN_VARLENOBJ_SIZE) {
            printf("special object found areanr %d offset %d size %d not dv and not area last obj %d\n",
                (int) i, (int) curoffset,
                (int) getspecialusedobjectsize(head),(int) (subareaend-MIN_VARLENOBJ_SIZE));
            return 39;
          }
          // we have reached an ok end marker, break while loop
          break;
        }
      }
    }
  }
  printf("usedcount %d\n", (int) usedcount);
  printf("usedbytesrealcount %d\n", (int) usedbytesrealcount);
  printf("usedbyteswantedcount %d\n", (int) usedbyteswantedcount);
  printf("freecount %d\n", (int) freecount);
  printf("freebytescount %d\n", (int) freebytescount);
  printf("dvcount %d\n", (int) dvcount);
  printf("dvbytescount %d\n", (int) dvbytescount);
  return 0;
}

static gint check_varlen_object_infreelist(void* db, void* area_header, gint offset, gint isfree) {
  gint head;
  db_area_header* areah;
  gint freelist;
  gint size;
  /*gint prevfreelist;*/
  gint bucketindex;
  gint objsize;

  head=dbfetch(db,offset);
  size=getfreeobjectsize(head);
  bucketindex=wg_freebuckets_index(db,size);
  areah=(db_area_header*)area_header;
  freelist=(areah->freebuckets)[bucketindex];
  /*prevfreelist=0;*/
  while(freelist!=0) {
    objsize=getfreeobjectsize(dbfetch(db,freelist));
    if (isfree) {
      if (offset==freelist) return 1;
    } else {
      if (offset==freelist) {
        printf("used object at offset %d in freelist for bucket %d\n",
                (int) offset, (int) bucketindex);
        return 51;
      }
      if (offset>freelist && freelist+objsize>offset) {
        printf("used object at offset %d inside freelist object at %d size %d for bucket %d\n",
                (int) offset, (int) freelist, (int) objsize, (int) bucketindex);
        return 52;
      }
    }
    freelist=dbfetch(db,freelist+sizeof(gint));
  }
  return 0;
}


/* --------------------- index testing ------------------------ */


/** Test data inserting with indexed column
 *
 */
static gint wg_test_index1(void *db, int magnitude, int printlevel) {
  const int dbsize = 50*magnitude, rand_updates = magnitude;
  int i, j;
  void *start = NULL, *rec = NULL;
  gint oldv, newv;
  db_memsegment_header* dbh = dbmemsegh(db);

#ifdef _WIN32
  srand(102435356);
#else
  srandom(102435356); /* fixed seed for repeatable sequences */
#endif

  if(wg_column_to_index_id(db, 0, WG_INDEX_TYPE_TTREE, NULL, 0) == -1) {
    if(printlevel > 1)
      printf("no index found on column 0, creating.\n");
    if(wg_create_index(db, 0, WG_INDEX_TYPE_TTREE, NULL, 0)) {
      if(printlevel)
        fprintf(stderr, "index creation failed, aborting.\n");
      return -3;
    }
  }

  if(printlevel > 1) {
    printf("------- tnode_area stats before insert --------\n");
    wg_show_db_area_header(db,&(dbh->tnode_area_header));
  }

  /* 1st loop: insert data in set 1 */
  for(i=0; i<dbsize; i++) {
    rec = wg_create_record(db, 1);
    if(!i)
      start = rec;
#ifdef _WIN32
    newv = rand()>>4;
#else
    newv = random()>>4;
#endif
    if(wg_set_field(db, rec, 0, wg_encode_int(db, newv))) {
      if(printlevel)
        fprintf(stderr, "insert error, aborting.\n");
      return -1;
    }
  }
  if(validate_index(db, start, dbsize, 0, printlevel)) {
    if(printlevel)
      fprintf(stderr, "index validation failed after insert.\n");
    return -2;
  }

  if(printlevel > 1) {
    printf("------- tnode_area stats after insert --------\n");
    wg_show_db_area_header(db,&(dbh->tnode_area_header));
  }

  /* 2nd loop: keep updating with random data */
  for(j=0; j<rand_updates; j++) {
    for(i=0; i<dbsize; i++) {
      if(!i)
        rec = start;
      else
        rec = wg_get_next_record(db, rec);
      oldv = wg_decode_int(db, wg_get_field(db, rec, 0));
#ifdef _WIN32
      newv = rand()>>4;
#else
      newv = random()>>4;
#endif
      if(wg_set_field(db, rec, 0, wg_encode_int(db, newv))) {
        if(printlevel) {
          printf("loop: %d row: %d old: %d new: %d\n",
            j, i, (int) oldv, (int) newv);
          fprintf(stderr, "insert error, aborting.\n");
        }
        return -2;
      }
      if(validate_index(db, start, dbsize, 0, printlevel)) {
        if(printlevel) {
          printf("loop: %d row: %d old: %d new: %d\n",
            j, i, (int) oldv, (int) newv);
          fprintf(stderr, "index validation failed after update.\n");
        }
        return -2;
      }
    }
  }

  if(printlevel > 1) {
    printf("------- tnode_area stats after update --------\n");
    wg_show_db_area_header(db,&(dbh->tnode_area_header));
  }

  return 0;
}

/** Quick index test to check basic behaviour
 *  indexes existing data in database and validates the resulting index
 */
static gint wg_test_index2(void *db, int printlevel) {
  int i, dbsize;
  void *rec, *start;
  if (printlevel>1)
    printf("********* testing T-tree index ********** \n");

  for(i=0; i<10; i++) {
    if(wg_column_to_index_id(db, i, WG_INDEX_TYPE_TTREE, NULL, 0) == -1) {
      if(wg_create_index(db, i, WG_INDEX_TYPE_TTREE, NULL, 0)) {
        if (printlevel)
          printf("index creation failed, aborting.\n");
        return -3;
      }
    }
  }

  start = rec = wg_get_first_record(db);
  dbsize = 0;

  /* Get the number of records in database */
  while(rec) {
    dbsize++;
    rec = wg_get_next_record(db, rec);
  }

  if(!dbsize)
    return 0; /* no data, so nothing more to do */

  for(i=0; i<10; i++) {
    if(validate_index(db, start, dbsize, i, printlevel)) {
      if (printlevel)
        printf("index validation failed.\n");
      return -2;
    }
  }

  if (printlevel>1)
    printf("********* index test successful ********** \n");
  return 0;
}

/** Test data inserting with multi-column hash indexes
 *
 */
static gint wg_test_index3(void *db, int magnitude, int printlevel) {
  const int dbsize = 10*magnitude, rand_updates = magnitude;
  int i, j, k;
  void *start = NULL, *rec = NULL;
  long int newv, rnddata;
  gint index1, index2;
  gint columns[2];

#ifdef _WIN32
  srand(102435356);
#else
  srandom(102435356); /* fixed seed for repeatable sequences */
#endif

  /* index the typical key/value columns */
  columns[0] = 1;
  columns[1] = 2;

  if(printlevel > 1) {
    printf("------- hash index test: inserting data --------\n");
  }

  /* 1st loop: insert data */
  for(i=0; i<dbsize; i++) {
    gint enc;
    rec = wg_create_record(db, 3);
    if(!i)
      start = rec;
    for(j=0; j<2; j++) {
#ifdef _WIN32
      rnddata = rand();
#else
      rnddata = random();
#endif
      newv = rnddata>>4;

      if(rnddata & 1) {
        enc = wg_encode_int(db, newv);
      } else {
        char buf[30];
        snprintf(buf, 29, "%ld", newv);
        buf[29] = '\0';
        enc = wg_encode_str(db, buf, NULL);
      }

      if(wg_set_field(db, rec, columns[j], enc)) {
        if(printlevel)
          fprintf(stderr, "insert error, aborting.\n");
        return -1;
      }
    }
  }

  if(printlevel > 1) {
    printf("------- hash index test: creating indexes --------\n");
  }

  /* Create indexes with data in db */
  if(wg_create_multi_index(db, columns, 2, WG_INDEX_TYPE_HASH, NULL, 0)) {
    if(printlevel)
      fprintf(stderr, "index creation failed, aborting.\n");
    return -3;
  }
  if((index1 = wg_multi_column_to_index_id(db,
    columns, 2, WG_INDEX_TYPE_HASH, NULL, 0)) == -1) {
    if(printlevel)
      fprintf(stderr, "index not found after creation.\n");
    return -3;
  }

  /* Create indexes with data in db */
  if(wg_create_multi_index(db, columns, 2, WG_INDEX_TYPE_HASH_JSON, NULL, 0)) {
    if(printlevel)
      fprintf(stderr, "index creation failed, aborting.\n");
    return -3;
  }
  if((index2 = wg_multi_column_to_index_id(db,
    columns, 2, WG_INDEX_TYPE_HASH_JSON, NULL, 0)) == -1) {
    if(printlevel)
      fprintf(stderr, "index not found after creation.\n");
    return -3;
  }

  if(printlevel > 1) {
    printf("------- hash index test: validating indexes --------\n");
  }

  if(validate_mc_index(db, start, dbsize, index1, columns, 2, printlevel)) {
    if(printlevel)
      fprintf(stderr, "index1 validation failed after insert.\n");
    return -2;
  }

  if(validate_mc_index(db, start, dbsize, index2, columns, 2, printlevel)) {
    if(printlevel)
      fprintf(stderr, "index2 validation failed after insert.\n");
    return -2;
  }

  if(printlevel > 1) {
    printf("------- hash index test: updating data --------\n");
  }

  /* 2nd loop: keep updating with random data */
  for(k=0; k<rand_updates; k++) {
    for(i=0; i<dbsize; i++) {
      gint enc, oldenc;
      if(!i)
        rec = start;
      else
        rec = wg_get_next_record(db, rec);

      for(j=0; j<2; j++) {
#ifdef _WIN32
        rnddata = rand();
#else
        rnddata = random();
#endif
        newv = rnddata>>4;

        if(rnddata & 1) {
          enc = wg_encode_int(db, newv);
        } else {
          char buf[30];
          snprintf(buf, 29, "%ld", newv);
          buf[29] = '\0';
          enc = wg_encode_str(db, buf, NULL);
        }

        oldenc = wg_get_field(db, rec, columns[j]);
        if(wg_set_field(db, rec, columns[j], enc)) {
          if(printlevel) {
            printf("loop: %d row: %d old (encoded): %d new (encoded): %d\n",
              k, i, (int) oldenc, (int) enc);
            fprintf(stderr, "insert error, aborting.\n");
          }
          return -1;
        }
      }

      if(validate_mc_index(db,
        start, dbsize, index1, columns, 2, printlevel)) {
        if(printlevel) {
          printf("loop: %d row: %d\n", k, i);
          fprintf(stderr, "index1 validation failed after update.\n");
        }
        return -2;
      }
      if(validate_mc_index(db,
        start, dbsize, index2, columns, 2, printlevel)) {
        if(printlevel) {
          printf("loop: %d row: %d\n", k, i);
          fprintf(stderr, "index2 validation failed after update.\n");
        }
        return -2;
      }
    }
  }

  if(printlevel > 1) {
    printf("------- hash index test: no errors found --------\n");
  }

  return 0;
}


/** Validate a T-tree index
 *  1. validates a set of rows starting from *rec.
 *  2. checks tree balance
 *  3. checks tree min/max values
 *  returns 0 if no errors found
 *  returns -1 if value was not indexed
 *  returns -2 if there was another error
 */
static int validate_index(void *db, void *rec, int rows, int column,
  int printlevel) {
  gint index_id = wg_column_to_index_id(db, column,
    WG_INDEX_TYPE_TTREE, NULL, 0);
  gint tnode_offset;
  wg_index_header *hdr;

  if(index_id == -1)
    return -2;

  /* Check if all values are indexed */
  while(rec && rows) {
    if(wg_get_record_len(db, rec) > column) {
      gint val = wg_get_field(db, rec, column);
      if(wg_search_ttree_index(db, index_id, val) < 1) {
        if(printlevel) {
          printf("missing: %d\n", (int) val);
        }
        return -1;
      }
    }
    rec = wg_get_next_record(db, rec);
    rows--;
  }

  hdr = (wg_index_header *) offsettoptr(db, index_id);

  if(((struct wg_tnode *)(offsettoptr(db,
    TTREE_ROOT_NODE(hdr))))->parent_offset != 0) {
    if(printlevel)
      printf("root node parent offset is not 0\n");
    return -2;
  }
#ifdef TTREE_CHAINED_NODES
  if(TTREE_MIN_NODE(hdr) == 0) {
    if(printlevel)
      printf("min node offset is 0\n");
    return -2;
  }
  if(TTREE_MAX_NODE(hdr) == 0) {
    if(printlevel)
      printf("max node offset is 0\n");
    return -2;
  }
#endif

#ifdef TTREE_CHAINED_NODES
  tnode_offset = TTREE_MIN_NODE(hdr);
#else
  tnode_offset = wg_ttree_find_lub_node(db, TTREE_ROOT_NODE(hdr));
#endif
  while(tnode_offset) {
    int diff;
    gint minval, maxval;
    struct wg_tnode *node = (struct wg_tnode *) offsettoptr(db, tnode_offset);

    /* Check index tree balance */
    diff = node->left_subtree_height - node->right_subtree_height;
    if(diff < -1 || diff > 1)
      return -2;

    /* Check min/max values */
    minval = wg_get_field(db,
      offsettoptr(db, node->array_of_values[0]), column);
    maxval = wg_get_field(db,
      offsettoptr(db, node->array_of_values[node->number_of_elements - 1]),
      column);
    if(minval != node->current_min) {
      if(printlevel) {
        printf("current_min invalid: %d is: %d should be: %d\n",
          (int) tnode_offset, (int) node->current_min,
          (int) minval);
      }
      return -2;
    }
    if(maxval != node->current_max) {
      if(printlevel) {
        printf("current_max invalid: %d is: %d should be: %d\n",
          (int) tnode_offset, (int) node->current_max,
          (int) maxval);
      }
      return -2;
    }

    tnode_offset = TNODE_SUCCESSOR(db, node);
  }

  return 0;
}

/** Validate a multi-column index
 *  validates a set of rows starting from *rec.
 *  uses the index_id provided (to facilitate separate testing of
 *   multiple indexes on the same column set).
 *
 *  returns 0 if no errors found
 *  returns -1 if value was not indexed or was indexed and shouldn't have been
 *  returns -2 if there was another error
 */
static int validate_mc_index(void *db, void *rec, size_t rows, gint index_id,
  gint *columns, size_t col_count, int printlevel) {
  wg_index_header *hdr = (wg_index_header *) offsettoptr(db, index_id);
  gint max_col = -1;
  size_t i;

  for(i=0; i<col_count; i++) {
    if(columns[i] > max_col) {
      max_col = columns[i];
    }
  }

  if(hdr->type != WG_INDEX_TYPE_HASH_JSON &&
    hdr->type != WG_INDEX_TYPE_HASH) {
  }

  /* Check if all values are indexed */
  while(rec && rows) {
    if(wg_get_record_len(db, rec) > max_col) {
      gint values[MAX_INDEX_FIELDS];
      gint reclist_offset;
      int found = 0;
      for(i=0; i<col_count; i++) {
        values[i] = wg_get_field(db, rec, columns[i]);
      }

      reclist_offset = wg_search_hash(db, index_id, values, col_count);
      /* Check that our original record was among the matched.
       * also check that the other records have correct values.
       */
      if(reclist_offset > 0) {
        gint *nextoffset = &reclist_offset;
        while(*nextoffset) {
          gcell *rec_cell = (gcell *) offsettoptr(db, *nextoffset);
          void *match = offsettoptr(db, rec_cell->car);
          if(match == rec) {
            found = 1;
          } else {
            for(i=0; i<col_count; i++) {
              if(wg_get_field(db, match, columns[i]) != values[i]) {
                if(printlevel) {
                  printf("invalid value in matched record: %p col %d\n",
                    match, (int) columns[i]);
                }
                return -1;
              }
            }
          }

          if(hdr->type == WG_INDEX_TYPE_HASH_JSON && \
            !is_plain_record(match)) {
            if(printlevel) {
              printf("record %p shouldn't be indexed\n", rec);
            }
            return -1;
          }
          nextoffset = &(rec_cell->cdr);
        }
      }

      /* check if the record was supposed to have been indexed */
      if(hdr->type == WG_INDEX_TYPE_HASH || is_plain_record(rec)) {
        if(!found) {
          if(printlevel) {
            printf("missing: record %p\n", rec);
          }
          return -1;
        }
      }
    }
    rec = wg_get_next_record(db, rec);
    rows--;
  }

  return 0;
}


/* -------------------- child db testing ------------------------ */

#ifdef USE_CHILD_DB
static int childdb_mkindex(void *db, int cnt) {
  int i;
  for(i=0; i<cnt; i++) {
    if(wg_column_to_index_id(db, i, WG_INDEX_TYPE_TTREE, NULL, 0) == -1) {
      if(wg_create_index(db, i, WG_INDEX_TYPE_TTREE, NULL, 0)) {
        return 0;
      }
    }
  }
  return 1;
}

static int childdb_ckindex(void *db, int cnt, int printlevel) {
  void *start, *rec;
  int i, dbsize;

  start = rec = (void *) wg_get_first_record(db);
  dbsize = 0;

  /* Get the number of records in database */
  while(rec) {
    dbsize++;
    rec = wg_get_next_record(db, rec);
  }

  for(i=0; i<cnt; i++) {
    if(printlevel > 1)
      printf("checking (%p %d).\n", dbmemseg(db), i);
    if(validate_index(db, start, dbsize, i, printlevel)) {
      if(printlevel)
        printf("index validation failed (%p %d).\n", dbmemseg(db), i);
      return 0;
    }
  }
  return 1;
}

static int childdb_dropindex(void *db, int cnt) {
  int i;
  for(i=0; i<cnt; i++) {
    gint index_id;
    if((index_id = \
      wg_column_to_index_id(db, i, WG_INDEX_TYPE_TTREE, NULL, 0)) != -1) {
      if(wg_drop_index(db, index_id)) {
        return 0;
      }
    }
  }
  return 1;
}
#endif

static gint wg_check_childdb(void* db, int printlevel) {
#ifdef USE_CHILD_DB
  void *foo;
  void *rec1, *rec2, *foorec1, *foorec2, *foorec3, *foorec4;
  gint tmp, str1, str2;

  if(printlevel>1) {
    printf("********* testing child database ********** \n");
  }

  foo = wg_attach_local_database(500000);

  if(foo) {
    if(printlevel>1) {
#ifndef _WIN32
      printf("Parent: %p free %td.\nChild: %p free %td extdbs %td size %td\n",
#else
      printf("Parent: %p free %Id.\nChild: %p free %Id extdbs %Id size %Id\n",
#endif
        dbmemseg(db),
        dbmemsegh(db)->free,
        dbmemseg(foo),
        dbmemsegh(foo)->free,
        dbmemsegh(foo)->extdbs.count,
        dbmemsegh(foo)->size);
    }
  } else {
    printf("Failed to attach to local database.\n");
    return 1;
  }

  if(dbmemsegh(db)->key != 0) {
    /* Test invalid registering */
    if(!wg_register_external_db(db, foo)) {
      if(printlevel)
        printf("Registering the local db in a shared db succeeded, should have failed\n");
      wg_delete_local_database(foo);
      return 1;
    }
  }

  /* Records in parent db */
  rec1 = (void *) wg_create_raw_record(db, 3);
  rec2 = (void *) wg_create_raw_record(db, 3);

  str1 = wg_encode_str(db, "hello", NULL);
  wg_set_new_field(db, rec1, 0, str1);
  wg_set_new_field(db, rec1, 1, wg_encode_str(db, "world", NULL));
  wg_set_new_field(db, rec1, 2, wg_encode_double(db, 1.234));
  wg_set_new_field(db, rec2, 0, wg_encode_record(db, rec1));
  str2 = wg_encode_str(db, "bar", NULL);
  wg_set_new_field(db, rec2, 1, str2);

  /* Records in child db */
  foorec1 = (void *) wg_create_raw_record(foo, 3);
  foorec2 = (void *) wg_create_raw_record(foo, 3);

  tmp = wg_encode_external_data(foo, db, str1);

  /* Try storing external data */
  if(printlevel>1) {
    printf("Expecting an error: \"wg data handling error: "\
      "External reference not recognized\".\n");
  }
  if(!wg_set_new_field(foo, foorec1, 0, tmp)) {
    if(printlevel)
      printf("Storing external data succeeded, should have failed\n");
    wg_delete_local_database(foo);
    return 1;
  }

  /* Test indexes */
  if(printlevel>1) {
    printf("Testing child database index.\n");
  }
  if(!childdb_mkindex(foo, 3)) {
    if(printlevel)
      printf("Child database index creation failed\n");
    wg_delete_local_database(foo);
    return 1;
  }
  if(!childdb_ckindex(foo, 3, printlevel)) {
    if(printlevel)
      printf("Child database index test failed\n");
    wg_delete_local_database(foo);
    return 1;
  }

  /* Test registering (should fail, as we have indexes) */
  if(printlevel>1) {
    printf("Expecting an error: \"db memory allocation error: "\
      "Database has indexes, external references not allowed\".\n");
  }
  if(!wg_register_external_db(foo, db)) {
    if(printlevel)
      printf("Registering the external db succeeded, but we have indexes\n");
    wg_delete_local_database(foo);
    return 1;
  }

  if(!childdb_dropindex(foo, 3)) {
    if(printlevel)
      printf("Dropping indexes failed\n");
    wg_delete_local_database(foo);
    return 1;
  }

  /* Test registering again */
  if(wg_register_external_db(foo, db)) {
    if(printlevel)
      printf("Registering the shared db in local db failed, should have succeeded\n");
    wg_delete_local_database(foo);
    return 1;
  }
  if(printlevel>1) {
    printf("Expecting an error: \"index error: "\
      "Database has external data, indexes disabled\".\n");
  }
  if(childdb_mkindex(foo, 1)) {
    if(printlevel)
      printf("Child database index creation succeeded (should have failed)\n");
    wg_delete_local_database(foo);
    return 1;
  }

  /* Storing external data should now work */
  if(wg_set_new_field(foo, foorec1, 0, tmp)) {
    if(printlevel)
      printf("Storing external data failed, should have succeeded\n");
    wg_delete_local_database(foo);
    return 1;
  }

  wg_set_new_field(foo, foorec1, 1, wg_encode_str(foo, "local data", NULL));

  tmp = wg_encode_external_data(foo, db, wg_encode_record(db, rec1));
  wg_set_new_field(foo, foorec2, 0, tmp);
  wg_set_new_field(foo, foorec2, 1, wg_encode_str(foo, "more local data", NULL));
  tmp = wg_encode_external_data(foo, db, str2);
  wg_set_new_field(foo, foorec2, 2, tmp);

  if(printlevel>1) {
    printf("Testing data comparing.\n");
  }

  /* Test comparing */
  foorec3 = (void *) wg_create_raw_record(foo, 3);
  foorec4 = (void *) wg_create_raw_record(foo, 3);

  wg_set_new_field(foo, foorec3, 0, wg_encode_str(foo, "hello", NULL));
  wg_set_new_field(foo, foorec3, 1, wg_encode_str(foo, "world", NULL));
  wg_set_new_field(foo, foorec3, 2, wg_encode_double(foo, 1.234));

  wg_set_new_field(foo, foorec4, 0, wg_encode_record(foo, foorec3));
  wg_set_new_field(foo, foorec4, 1, wg_encode_str(foo, "more local data", NULL));
  tmp = wg_encode_external_data(foo, db, str2);
  wg_set_new_field(foo, foorec4, 2, tmp);

#if WG_COMPARE_REC_DEPTH > 2
  /* foorec2 and foorec4 should be equal */
  if(WG_COMPARE(foo,
    wg_encode_record(foo, foorec2),
    wg_encode_record(foo, foorec4)) != WG_EQUAL) {
    if(printlevel)
      printf("foorec2 and foorec4 were not equal, but should be.\n");
    wg_delete_local_database(foo);
    return 1;
  }

  /* rec1 and foorec3 should be equal */
  if(WG_COMPARE(foo,
    wg_encode_external_data(foo, db, wg_encode_record(db, rec1)),
    wg_encode_record(foo, foorec3)) != WG_EQUAL) {
    if(printlevel)
      printf("rec1 and foorec3 were not equal, but should be.\n");
    wg_delete_local_database(foo);
    return 1;
  }
#endif

  /* sanity check: foorec3 and foorec4 should not be equal */
  if(WG_COMPARE(foo,
    wg_encode_record(foo, foorec3),
    wg_encode_record(foo, foorec4)) == WG_EQUAL) {
    if(printlevel)
      printf("foorec3 and foorec4 were equal, but should not be.\n");
    wg_delete_local_database(foo);
    return 1;
  }

#ifdef USE_BACKLINKING
  /* Test deleting */
  if(wg_delete_record(db, rec1) != -1) {
    if(printlevel)
      printf("Deleting referenced parent rec1 succeeded (should have failed)\n");
    wg_delete_local_database(foo);
    return 1;
  }
#else
  if(wg_delete_record(db, rec1) != 0) {
    if(printlevel)
      printf("Deleting parent rec1 failed (should have succeeded)\n");
    wg_delete_local_database(foo);
    return 1;
  }
#endif
  if(wg_delete_record(db, rec2) != 0) {
    if(printlevel)
      printf("Deleting non-referenced parent rec2 failed (should have succeeded)\n");
    wg_delete_local_database(foo);
    return 1;
  }
  if(wg_delete_record(foo, foorec2) != 0) {
    if(printlevel)
      printf("Deleting child foorec2 failed (should have succeeded)\n");
    wg_delete_local_database(foo);
    return 1;
  }

  /* right now string refcounts are a bit fishy... skip this */
  /* wg_set_field(foo, foorec4, 2, tmp); */

  /* this should fail, but we don't want to interact with the
   * filesystem in these automated tests
  wg_dump(foo, "invalid.bin");*/

  wg_delete_local_database(foo);

  if(printlevel>1)
    printf("********* child database test successful ********** \n");
#else
  printf("child databases disabled, skipping checks\n");
#endif
  return 0;
}

/* ---------------- schema/JSON related tests ----------------- */

/*
 * Run this on a dedicated database to check the effects
 * of param bits and deleting.
 */
static gint wg_check_schema(void* db, int printlevel) {
  void *rec, *arec, *orec, *trec;
  gint *gptr;
  gint tmp1, tmp2, tmp3;

  if(printlevel>1) {
    printf("********* testing schema functions ********** \n");
  }

  tmp1 = wg_encode_int(db, 99);
  tmp2 = wg_encode_int(db, 98);
  tmp3 = wg_encode_int(db, 97);

  /* Triple */
  rec = wg_create_triple(db, tmp1, tmp2, tmp3, 0);

  /* Check the record (fields and meta bits).
   * it is not a param.
   */
  gptr = ((gint *) rec + RECORD_META_POS);
  if(*gptr) {
    if(printlevel) {
      printf("plain triple is expected to have no meta bits\n");
    }
    return 1;
  }
  gptr = ((gint *) rec + RECORD_HEADER_GINTS + WG_SCHEMA_TRIPLE_OFFSET);
  if(*gptr != tmp1) {
    if(printlevel)
      printf("triple field 1 does not match\n");
    return 1;
  }
  if(*(gptr+1) != tmp2) {
    if(printlevel)
      printf("triple field 2 does not match\n");
    return 1;
  }
  if(*(gptr+2) != tmp3) {
    if(printlevel)
      printf("triple field 3 does not match\n");
    return 1;
  }

  /* the next triple is a param.
   */
  rec = wg_create_triple(db, tmp1, tmp2, tmp3, 1);
  gptr = ((gint *) rec + RECORD_META_POS);
  if(*gptr != (RECORD_META_NOTDATA|RECORD_META_MATCH)) {
    if(printlevel) {
#ifndef _WIN32
      printf("param triple had invalid meta bits (%td)\n", *gptr);
#else
      printf("param triple had invalid meta bits (%Id)\n", *gptr);
#endif
    }
    return 1;
  }

  /* kv-pair */
  rec = wg_create_kvpair(db, tmp2, tmp3, 1);

  /* Check the record (fields and meta bits).
   * it is a param.
   */
  gptr = ((gint *) rec + RECORD_META_POS);
  if(*gptr != (RECORD_META_NOTDATA|RECORD_META_MATCH)) {
    if(printlevel) {
#ifndef _WIN32
      printf("param kv-pair had invalid meta bits (%td)\n", *gptr);
#else
      printf("param kv-pair had invalid meta bits (%Id)\n", *gptr);
#endif
    }
    return 1;
  }
  gptr = ((gint *) rec + RECORD_HEADER_GINTS + WG_SCHEMA_TRIPLE_OFFSET);
  if(*gptr != 0) {
    if(printlevel)
      printf("kv-pair prefix is not NULL\n");
    return 1;
  }
  if(*(gptr+1) != tmp2) {
    if(printlevel)
      printf("kv-pair key does not match\n");
    return 1;
  }
  if(*(gptr+2) != tmp3) {
    if(printlevel)
      printf("kv-pair value does not match\n");
    return 1;
  }

  /* this is not a param.
   */
  rec = wg_create_triple(db, tmp1, tmp2, tmp3, 0);
  gptr = ((gint *) rec + RECORD_META_POS);
  if(*gptr) {
    if(printlevel) {
      printf("plain kv-pair is expected to have no meta bits\n");
    }
    return 1;
  }

  /* params should be invisible */
  if(check_db_rows(db, 2, printlevel)) {
    if(printlevel)
      printf("row count check failed (should have 2 non-param rows).\n");
    return 1;
  }

  /* Object */
  orec = wg_create_object(db, 1, 0, 0);
  if(wg_get_record_len(db, orec) != 1) {
    if(printlevel) {
      printf("object had invalid length\n");
    }
    return 1;
  }

  gptr = ((gint *) orec + RECORD_META_POS);
  if(*gptr != RECORD_META_OBJECT) {
    if(printlevel) {
#ifndef _WIN32
      printf("object (nonparam) had invalid meta bits (%td)\n", *gptr);
#else
      printf("object (nonparam) had invalid meta bits (%Id)\n", *gptr);
#endif
    }
    return 1;
  }

  wg_set_field(db, orec, 0, wg_encode_record(db, rec));

  /* Array. It has the document bit set.
   */
  arec = wg_create_array(db, 4, 1, 0);
  if(wg_get_record_len(db, arec) != 4) {
    if(printlevel) {
      printf("array had invalid length\n");
    }
    return 1;
  }

  gptr = ((gint *) arec + RECORD_META_POS);
  if(*gptr != (RECORD_META_ARRAY|RECORD_META_DOC)) {
    if(printlevel) {
#ifndef _WIN32
      printf("array (doc, nonparam) had invalid meta bits (%td)\n", *gptr);
#else
      printf("array (doc, nonparam) had invalid meta bits (%Id)\n", *gptr);
#endif
    }
    return 1;
  }

  /* Form the document.
   */
  wg_set_field(db, arec, 0, tmp3);
  wg_set_field(db, arec, 1, tmp2);
  wg_set_field(db, arec, 2, tmp1);
  wg_set_field(db, arec, 3, wg_encode_record(db, orec));

#ifdef USE_BACKLINKING
  /* Locate the document through an element.
   */
  trec = wg_find_document(db, rec);
  if(trec != arec) {
    if(printlevel) {
      printf("wg_find_document() failed\n");
    }
    return 1;
  }
#endif

  if(wg_delete_document(db, arec)) {
    if(printlevel) {
      printf("wg_delete_document() failed\n");
    }
    return 1;
  }

  /* of the two rows in db earlier, one was included in the
   * deleted document. One should be remaining.
   */
  if(check_db_rows(db, 1, printlevel)) {
    if(printlevel)
      printf("Invalid number of remaining rows after deleting.\n");
    return 1;
  }

  /* Check the param bits of object and array.
   */
  orec = wg_create_object(db, 5, 0, 1);
  gptr = ((gint *) orec + RECORD_META_POS);
  if(*gptr != (RECORD_META_OBJECT|RECORD_META_NOTDATA|RECORD_META_MATCH)) {
    if(printlevel) {
#ifndef _WIN32
      printf("object (param) had invalid meta bits (%td)\n", *gptr);
#else
      printf("object (param) had invalid meta bits (%Id)\n", *gptr);
#endif
    }
    return 1;
  }

  arec = wg_create_array(db, 6, 0, 1);
  gptr = ((gint *) arec + RECORD_META_POS);
  if(*gptr != (RECORD_META_ARRAY|RECORD_META_NOTDATA|RECORD_META_MATCH)) {
    if(printlevel) {
#ifndef _WIN32
      printf("array (param) had invalid meta bits (%td)\n", *gptr);
#else
      printf("array (param) had invalid meta bits (%Id)\n", *gptr);
#endif
    }
    return 1;
  }

  /* we added params, row count should not increase. */
  if(check_db_rows(db, 1, printlevel)) {
    if(printlevel)
      printf("Invalid number of remaining rows after deleting.\n");
    return 1;
  }

  if(printlevel>1)
    printf("********* schema test successful ********** \n");

  return 0;
}

/*
 * Test JSON parsing. This produces some errors in stderr
 * which is expected (rely on the return value to check for success).
 */
static gint wg_check_json_parsing(void* db, int printlevel) {
  void *doc, *rec;
  gint enc;

  char *json1 = "[7,8,9]"; /* ok */
  char *json2 = "{ \"a\":{\n\"b\": 55.0\n}, \"c\"\n:\"hello\","\
                    "\"d\"\t:[\n]}"; /* ok */
  char *json3 = "25"; /* fail */
  char *json4 = "{ \"a\":{\"b\": 55.0}, \"c\":\"hello\""; /* fail */

  if(printlevel>1) {
    printf("********* testing JSON parsing functions ********** \n");
  }

  /* parse input buf. */
  doc = NULL;
  if(wg_parse_json_document(db, json1, &doc)) {
    if(printlevel)
      printf("Parsing a valid document failed.\n");
    return 1;
  }
  if(!doc) {
    if(printlevel)
      printf("JSON parser did not return a document.\n");
    return 1;
  }

  /* examine structure
   */
  if(wg_get_record_len(db, doc) != 3) {
    if(printlevel)
      printf("Document structure error: bad object length.\n");
    return 1;
  }
  if(is_special_record(doc) || !is_schema_document(doc) ||\
   !is_schema_array(doc)) {
    if(printlevel) {
      printf("Document structure error: invalid meta type\n");
    }
    return 1;
  }

  /* field contents */
  enc = wg_get_field(db, doc, 0);
  if(wg_get_encoded_type(db, enc) != WG_INTTYPE) {
    if(printlevel)
      printf("Document structure error: bad array element(0).\n");
    return 1;
  }
  if(wg_decode_int(db, enc) != 7) {
    if(printlevel)
      printf("Document structure error: bad array element value(0).\n");
    return 1;
  }

  enc = wg_get_field(db, doc, 1);
  if(wg_get_encoded_type(db, enc) != WG_INTTYPE) {
    if(printlevel)
      printf("Document structure error: bad array element(1).\n");
    return 1;
  }
  if(wg_decode_int(db, enc) != 8) {
    if(printlevel)
      printf("Document structure error: bad array element value(1).\n");
    return 1;
  }

  enc = wg_get_field(db, doc, 2);
  if(wg_get_encoded_type(db, enc) != WG_INTTYPE) {
    if(printlevel)
      printf("Document structure error: bad array element(2).\n");
    return 1;
  }
  if(wg_decode_int(db, enc) != 9) {
    if(printlevel)
      printf("Document structure error: bad array element value(2).\n");
    return 1;
  }

  /* Use the param parser to get direct access to the
   * document structure. */
  doc = NULL;
  if(wg_parse_json_param(db, json2, &doc)) {
    if(printlevel)
      printf("Parsing a valid document failed.\n");
    return 1;
  }

  if(!doc) {
    if(printlevel)
      printf("Param parser did not return a document.\n");
    return 1;
  }

  /* examine structure
   */
  if(wg_get_record_len(db, doc) != 3) {
    if(printlevel)
      printf("Document structure error: bad object length.\n");
    return 1;
  }

  if(!is_special_record(doc) || !is_schema_document(doc) ||\
   !is_schema_object(doc)) {
    if(printlevel) {
      printf("Document structure error: invalid meta type\n");
    }
    return 1;
  }

  /* first kv-pair */
  enc = wg_get_field(db, doc, 0);
  if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
    if(printlevel)
      printf("Document structure error: bad object element(0).\n");
    return 1;
  }
  rec = wg_decode_record(db, enc);

  enc = wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_STRTYPE) {
    if(printlevel)
      printf("Document structure error: bad key type.\n");
    return 1;
  }
  if(strncmp("a", wg_decode_str(db, enc), 1)) {
    if(printlevel)
      printf("Document structure error: bad key string.\n");
    return 1;
  }

  enc = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
    if(printlevel)
      printf("Document structure error: bad value type.\n");
    return 1;
  }
  rec = wg_decode_record(db, enc);
  if(wg_get_record_len(db, rec) != 1) {
    if(printlevel)
      printf("Document structure error: bad sub-object length.\n");
    return 1;
  }
  if(!is_schema_object(rec)) {
    if(printlevel) {
      printf("Document structure error: sub-object has invalid meta type\n");
    }
  }

  enc = wg_get_field(db, rec, 0);
  if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
    if(printlevel)
      printf("Document structure error: bad sub-object element(0).\n");
    return 1;
  }
  rec = wg_decode_record(db, enc);

  enc = wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_STRTYPE) {
    if(printlevel)
      printf("Document structure error: bad subobj key type.\n");
    return 1;
  }
  if(strncmp("b", wg_decode_str(db, enc), 1)) {
    if(printlevel)
      printf("Document structure error: bad subobj key string.\n");
    return 1;
  }

  enc = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_DOUBLETYPE) {
    if(printlevel)
      printf("Document structure error: bad subobj value type.\n");
    return 1;
  }

  if(wg_decode_double(db, enc) >= 55.1 ||\
   wg_decode_double(db, enc) <= 54.9) {
    if(printlevel)
      printf("Document structure error: bad subobj value.\n");
    return 1;
  }

  /* second kv-pair */
  enc = wg_get_field(db, doc, 1);
  if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
    if(printlevel)
      printf("Document structure error: bad object element(1).\n");
    return 1;
  }
  rec = wg_decode_record(db, enc);

  enc = wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_STRTYPE) {
    if(printlevel)
      printf("Document structure error: bad key type.\n");
    return 1;
  }
  if(strncmp("c", wg_decode_str(db, enc), 1)) {
    if(printlevel)
      printf("Document structure error: bad key string.\n");
    return 1;
  }

  enc = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_STRTYPE) {
    if(printlevel)
      printf("Document structure error: value type.\n");
    return 1;
  }
  if(strncmp("hello", wg_decode_str(db, enc), 5)) {
    if(printlevel)
      printf("Document structure error: bad value.\n");
    return 1;
  }

  /* third kv-pair */
  enc = wg_get_field(db, doc, 2);
  if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
    if(printlevel)
      printf("Document structure error: bad object element(0).\n");
    return 1;
  }
  rec = wg_decode_record(db, enc);

  enc = wg_get_field(db, rec, WG_SCHEMA_KEY_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_STRTYPE) {
    if(printlevel)
      printf("Document structure error: bad key type.\n");
    return 1;
  }
  if(strncmp("d", wg_decode_str(db, enc), 1)) {
    if(printlevel)
      printf("Document structure error: bad key string.\n");
    return 1;
  }

  enc = wg_get_field(db, rec, WG_SCHEMA_VALUE_OFFSET);
  if(wg_get_encoded_type(db, enc) != WG_RECORDTYPE) {
    if(printlevel)
      printf("Document structure error: bad value type.\n");
    return 1;
  }
  rec = wg_decode_record(db, enc);
  if(!is_schema_array(rec)) {
    if(printlevel) {
      printf("Document structure error: bad value (array expected)\n");
    }
  }
  if(wg_get_record_len(db, rec) != 0) {
    if(printlevel)
      printf("Document structure error: bad array length.\n");
    return 1;
  }

  /* Invalid documents, expect a failure.
   */
  if(printlevel>1)
    printf("testing invalid documents, the following errors are expected.\n");

  if(!wg_check_json(db, NULL)) {
    if(printlevel)
      printf("Checking an invalid document succeeded (expected to fail).\n");
    return 1;
  }
  if(!wg_check_json(db, json3)) {
    if(printlevel)
      printf("Checking an invalid document succeeded (expected to fail).\n");
    return 1;
  }
  if(!wg_parse_json_document(db, json3, NULL)) {
    if(printlevel)
      printf("Parsing an invalid document succeeded.\n");
    return 1;
  }

  if(!wg_check_json(db, json4)) {
    if(printlevel)
      printf("Checking an invalid document succeeded (expected to fail).\n");
    return 1;
  }
  if(!wg_parse_json_param(db, json4, &doc)) {
    if(printlevel)
      printf("Parsing an invalid document succeeded.\n");
    return 1;
  }

  if(printlevel>1)
    printf("********* JSON parsing test successful ********** \n");

  return 0;
}

/*
 * Returns 1 if the offset is in list.
 * Returns 0 otherwise.
 */
static int is_offset_in_list(void *db, gint reclist_offset, gint offset) {
  if(reclist_offset > 0) {
    gint *nextoffset = &reclist_offset;
    while(*nextoffset) {
      gcell *rec_cell = (gcell *) offsettoptr(db, *nextoffset);
      if(rec_cell->car == offset)
        return 1;
      nextoffset = &(rec_cell->cdr);
    }
  }
  return 0;
}

/*
 * Test index hash (low-level functions)
 */
static gint wg_check_idxhash(void* db, int printlevel) {
  db_hash_area_header ha;
  struct {
    char *data;
    gint offsets[10];
    int delidx;
  } rowdata[] = {
    { "0iQ1vMvGX5wfsjLTssyx", { 5709281, 5769186, 0,
                                0, 0, 0, 0, 0, 0, 0 }, 1 },
    { "1jP3hJxO61QVscBEKu9", { 3510018, 8944261, 8172536,
                                4346587, 0, 0, 0, 0, 0, 0 }, 2 },
    { "yLMt2eSQuIi3ChQlI0", { 6587099, 6385516, 0,
                                0, 0, 0, 0, 0, 0, 0 }, 1 },
    { "ZlGS9cVX7fE1v7H6m", { 2059694, 1981000, 8360987,
                             752526, 6435820, 240982,
                             323628, 8875951, 0, 0 }, 1 },
    { "duflillyRviJ1ZvH", { 6711262, 9685175, 4070003,
                            5977585, 9671591, 5321015,
                            7499127, 9101853, 0, 0 }, 2 },
    { "USLP83gH6f4pNYJ", { 8759349, 436333, 0,
                           0, 0, 0, 0, 0, 0, 0 }, 1 },
    { "yHIDgxlEA7RLAx", { 7613500, 534106, 4361094,
                          1506219, 0, 0, 0, 0, 0, 0 }, 1 },
    { "             ", { 6588510, 6253610, 9020726,
                         8514572, 9378303, 1100373, 0, 0, 0, 0 }, 2 },
    { "            ", { 8185484, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0 },
    { "yHIDgxlEA7R", { 2542797, 6481658, 214793,
                       943434, 2934816, 9503963,
                       1374313, 0, 0, 0 }, 4 },
    { NULL, { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, -1 }
  };
  int i;

  if(printlevel>1) {
    printf("********* testing index hash functions ********** \n");
  }

  /* Create a tiny hash table to allow hash chains to be created. */
  if(wg_create_hash(db, &ha, 4)) {
    if(printlevel)
      printf("Failed to create the hash table.\n");
    return 1;
  }

  /* Insert rows in order of columns */
  for(i=0; i<10; i++) {
    int j;
    for(j=0; rowdata[j].data; j++) {
      if(rowdata[j].offsets[i]) {
        if(wg_idxhash_store(db, &ha, rowdata[j].data,
         strlen(rowdata[j].data), rowdata[j].offsets[i])) {
          if(printlevel)
            printf("Hash table insertion failed (j=%d i=%d).\n", j, i);
          return 1;
        }
      }
    }
  }

  /* Check that each offset is present */
  for(i=0; rowdata[i].data; i++) {
    int j;
    gint list = wg_idxhash_find(db, &ha, rowdata[i].data,
      strlen(rowdata[i].data));
    for(j=0; j<10 && rowdata[i].offsets[j]; j++) {
      if(!is_offset_in_list(db, list, rowdata[i].offsets[j])) {
        if(printlevel)
          printf("Offset missing in hash table (i=%d j=%d).\n", i, j);
        return 1;
      }
    }
  }

  /* Delete the rows designated by delidx */
  for(i=0; rowdata[i].data; i++) {
    if(wg_idxhash_remove(db, &ha, rowdata[i].data,
     strlen(rowdata[i].data), rowdata[i].offsets[rowdata[i].delidx])) {
      if(printlevel)
        printf("Hash table deletion failed (i=%d delidx=%d).\n",
          i, rowdata[i].delidx);
      return 1;
    }
  }

  /* Check that the deleted row is not present and that all the others are */
  for(i=0; rowdata[i].data; i++) {
    int j;
    gint list = wg_idxhash_find(db, &ha, rowdata[i].data,
      strlen(rowdata[i].data));
    for(j=0; j<10 && rowdata[i].offsets[j]; j++) {
      if(j == rowdata[i].delidx) {
        /* Should be missing */
        if(is_offset_in_list(db, list, rowdata[i].offsets[j])) {
          if(printlevel)
            printf("Offset not correctly deleted (i=%d delidx=%d).\n",
              i, rowdata[i].delidx);
          return 1;
        }
      } else {
        /* Should be present */
        if(!is_offset_in_list(db, list, rowdata[i].offsets[j])) {
          if(printlevel)
            printf("Offset missing in hash table (i=%d j=%d).\n", i, j);
          return 1;
        }
      }
    }
  }

  if(printlevel>1)
    printf("********* index hash test successful ********** \n");

  return 0;
}

/* --------------------- query testing ------------------------ */

/**
 * Fetch all rows where "col" "cond" "val" is true
 *   (where cond is a comparison operator - equal, less than etc)
 * Check that the val matches the field value in returned records.
 * Check that the number of rows matches the expected value
 */
static int check_matching_rows(void *db, int col, int cond,
 void *val, gint type, int expected, int printlevel) {
  void *rec = NULL;
  wg_query *query = NULL;
  wg_query_arg arglist;
  int cnt;

  arglist.column = col;
  arglist.cond = cond;
  switch(type) {
    case WG_INTTYPE:
      arglist.value = wg_encode_query_param_int(db, *((gint *) val));
      break;
    case WG_DOUBLETYPE:
      arglist.value = wg_encode_query_param_double(db, *((double *) val));
      break;
    case WG_STRTYPE:
      arglist.value = wg_encode_query_param_str(db, (char *) val, NULL);
      break;
    default:
      return -1;
  }

  query = wg_make_query(db, NULL, 0, &arglist, 1);
  if(!query) {
    return -2;
  }

  if(query->res_count != expected) {
    if(printlevel)
      printf("check_matching_rows: res_count mismatch (%d != %d)\n",
        (int) query->res_count, expected);
    return -3;
  }

  cnt = 0;
  while((rec = wg_fetch(db, query))) {
    gint enc = wg_get_field(db, rec, col);
    if(cond == WG_COND_EQUAL) {
      switch(type) {
        case WG_INTTYPE:
          if(wg_decode_int(db, enc) != *((int *) val)) {
            if(printlevel)
              printf("check_matching_rows: int value mismatch\n");
            return -4;
          }
          break;
        case WG_DOUBLETYPE:
          if(wg_decode_double(db, enc) != *((double *) val)) {
            if(printlevel)
              printf("check_matching_rows: double value mismatch\n");
            return -4;
          }
          break;
        case WG_STRTYPE:
          if(strcmp(wg_decode_str(db, enc), (char *) val)) {
            if(printlevel)
              printf("check_matching_rows: string value mismatch\n");
            return -4;
          }
          break;
        default:
          break;
      }
    }
    cnt++;
  }

  if(cnt != expected) {
    if(printlevel)
      printf("check_matching_rows: actual count mismatch (%d != %d)\n",
        cnt, expected);
    return -5;
  }

  wg_free_query(db, query);
  wg_free_query_param(db, arglist.value);
  return 0;
}

/**
 * version of check_matching_rows() using wg_find_record_*()
 */
static int check_matching_rows_find(void *db, int col, int cond,
 void *val, gint type, int expected, int printlevel) {
  void *rec = NULL;
  int cnt = 0;

  for(;;) {
    switch(type) {
      case WG_INTTYPE:
        rec = wg_find_record_int(db, col, cond, *((int *) val), rec);
        break;
      case WG_DOUBLETYPE:
        rec = wg_find_record_double(db, col, cond, *((double *) val), rec);
        break;
      case WG_STRTYPE:
        rec = wg_find_record_str(db, col, cond, (char *) val, rec);
        break;
      default:
        break;
    }
    if(!rec)
      break;
    cnt++;
  }

  if(cnt != expected) {
    if(printlevel)
      printf("check_matching_rows_find: actual count mismatch (%d != %d)\n",
        cnt, expected);
    return -5;
  }

  return 0;
}


/**
 * Count db rows
 */
static int check_db_rows(void *db, int expected, int printlevel) {
  void *rec = NULL;
  int cnt;

  rec = wg_get_first_record(db);
  cnt = 0;
  while(rec) {
    cnt++;
    rec = wg_get_next_record(db, rec);
  }

  if(cnt != expected) {
    if(printlevel)
      printf("check_db_rows: actual count mismatch (%d != %d)\n",
        cnt, expected);
    return -1;
  }

  return 0;
}

/**
 * Basic query tests
 */
static gint wg_test_query(void *db, int magnitude, int printlevel) {
  const int dbsize = 50*magnitude;
  int i, j, k;
  void *rec = NULL;

  if(wg_column_to_index_id(db, 0, WG_INDEX_TYPE_TTREE, NULL, 0) == -1) {
    if(printlevel > 1)
      printf("no index found on column 0, creating.\n");
    if(wg_create_index(db, 0, WG_INDEX_TYPE_TTREE, NULL, 0)) {
      if(printlevel)
        printf("index creation failed, aborting.\n");
      return -3;
    }
  }

  if(printlevel > 1)
    printf("------- Inserting test data --------\n");

  /* Create predictable data */
  for(i=0; i<dbsize; i++) {
    for(j=0; j<50; j++) {
      for(k=0; k<50; k++) {
        char c1[20];
        gint c2 = 100 * j;
        double c3 = 10 * k;
        snprintf(c1, 19, "%d", 1000 * i);
        c1[19] = '\0';

        rec = wg_create_record(db, 3);
        if(!rec) {
          if(printlevel)
            printf("insert error, aborting.\n");
          return -1;
        }
        if(wg_set_field(db, rec, 0, wg_encode_str(db, c1, NULL))) {
          if(printlevel)
            printf("insert error, aborting.\n");
          return -1;
        }
        if(wg_set_field(db, rec, 1, wg_encode_int(db, c2))) {
          if(printlevel)
            printf("insert error, aborting.\n");
          return -1;
        }
        if(wg_set_field(db, rec, 2, wg_encode_double(db, c3))) {
          if(printlevel)
            printf("insert error, aborting.\n");
          return -1;
        }
      }
    }
  }

  if(wg_column_to_index_id(db, 2, WG_INDEX_TYPE_TTREE, NULL, 0) == -1) {
    if(printlevel > 1)
      printf("no index found on column 2, creating.\n");
    if(wg_create_index(db, 2, WG_INDEX_TYPE_TTREE, NULL, 0)) {
      if(printlevel)
        printf("index creation failed, aborting.\n");
      return -3;
    }
  }

  if(printlevel > 1)
    printf("------- Running read query tests --------\n");

  /* Content check read queries */
  for(i=0; i<dbsize; i++) {
    char buf[20];
    snprintf(buf, 19, "%d", 1000 * i);
    buf[19] = '\0';

    if(check_matching_rows(db, 0, WG_COND_EQUAL, (void *) buf,
     WG_STRTYPE, 50*50, printlevel)) {
      if(printlevel)
        printf("content check col=0, i=%d failed.\n", i);
      return -2;
    }
  }

  for(i=0; i<50; i++) {
    gint val = 100 * i;
    if(check_matching_rows(db, 1, WG_COND_EQUAL, (void *) &val,
     WG_INTTYPE, dbsize*50, printlevel)) {
      if(printlevel)
        printf("content check col=1, i=%d failed.\n", i);
      return -2;
    }
  }

  for(i=0; i<50; i++) {
    double val = 10 * i;
    if(check_matching_rows(db, 2, WG_COND_EQUAL, (void *) &val,
     WG_DOUBLETYPE, dbsize*50, printlevel)) {
      if(printlevel)
        printf("content check col=2, i=%d failed.\n", i);
      return -2;
    }
  }

  if(printlevel > 1)
    printf("------- Running find tests --------\n");

  /* Content check read queries */
  for(i=0; i<dbsize; i++) {
    char buf[20];
    snprintf(buf, 19, "%d", 1000 * i);
    buf[19] = '\0';

    if(check_matching_rows_find(db, 0, WG_COND_EQUAL, (void *) buf,
     WG_STRTYPE, 50*50, printlevel)) {
      if(printlevel)
        printf("find check col=0, i=%d failed.\n", i);
      return -8;
    }
  }

  for(i=0; i<4; i++) {
    gint val = 100 * i;
    if(check_matching_rows_find(db, 1, WG_COND_LESSTHAN, (void *) &val,
     WG_INTTYPE, dbsize*50*i, printlevel)) {
      if(printlevel)
        printf("find check col=1, i=%d failed.\n", i);
      return -8;
    }
  }

  for(i=47; i<50; i++) {
    double val = 10 * i;
    if(check_matching_rows_find(db, 2, WG_COND_GTEQUAL, (void *) &val,
     WG_DOUBLETYPE, dbsize*50*(50-i), printlevel)) {
      if(printlevel)
        printf("find check col=2, i=%d failed.\n", i);
      return -8;
    }
  }

  if(printlevel > 1)
    printf("------- Updating test data --------\n");

  /* Update queries */
  for(i=0; i<dbsize; i++) {
    wg_query *query;
    wg_query_arg arg;
    char c1[20];
    snprintf(c1, 19, "%d", 1000 * i);
    c1[19] = '\0';

    arg.column = 0;
    arg.cond = WG_COND_EQUAL;
    arg.value = wg_encode_query_param_str(db, c1, NULL);

    query = wg_make_query(db, NULL, 0, &arg, 1);
    if(!query) {
      if(printlevel)
        printf("wg_make_query() failed\n");
      wg_free_query_param(db, arg.value);
      return -4;
    }

    while((rec = wg_fetch(db, query))) {
      gint c2 = wg_decode_int(db, wg_get_field(db, rec, 1));
      if(wg_set_field(db, rec, 1, wg_encode_int(db, c2 + 21))) {
        if(printlevel)
          printf("update error, aborting.\n");
        wg_free_query_param(db, arg.value);
        return -5;
      }
    }

    wg_free_query_param(db, arg.value);
  }

  for(i=0; i<50; i++) {
    gint c2 = 100 * i + 21;
    wg_query *query;
    wg_query_arg arg;

    arg.column = 1;
    arg.cond = WG_COND_EQUAL;
    arg.value = wg_encode_query_param_int(db, c2);

    query = wg_make_query(db, NULL, 0, &arg, 1);
    if(!query) {
      if(printlevel)
        printf("wg_make_query() failed\n");
      wg_free_query_param(db, arg.value);
      return -4;
    }

    while((rec = wg_fetch(db, query))) {
      double c3 = wg_decode_double(db, wg_get_field(db, rec, 2));
      if(wg_set_field(db, rec, 2, wg_encode_double(db, c3 - 77.42))) {
        if(printlevel)
          printf("update error, aborting.\n");
        wg_free_query_param(db, arg.value);
        return -5;
      }
    }

    wg_free_query_param(db, arg.value);
  }

  for(i=0; i<50; i++) {
    double c3 = 10 * i - 77.42;
    wg_query *query;
    wg_query_arg arg;

    arg.column = 2;
    arg.cond = WG_COND_EQUAL;
    arg.value = wg_encode_query_param_double(db, c3);

    query = wg_make_query(db, NULL, 0, &arg, 1);
    if(!query) {
      if(printlevel)
        printf("wg_make_query() failed\n");
      wg_free_query_param(db, arg.value);
      return -4;
    }

    while((rec = wg_fetch(db, query))) {
      char c1[20];
      int c1val = atol(wg_decode_str(db, wg_get_field(db, rec, 0)));

      snprintf(c1, 19, "%d", c1val * 7);
      c1[19] = '\0';

      if(wg_set_field(db, rec, 0, wg_encode_str(db, c1, NULL))) {
        if(printlevel)
          printf("update error, aborting.\n");
        wg_free_query_param(db, arg.value);
        return -5;
      }
    }

    wg_free_query_param(db, arg.value);
  }

  if(printlevel > 1)
    printf("------- Running read query tests --------\n");

  /* Content check read queries, iteration 2 */
  for(i=0; i<dbsize; i++) {
    char buf[20];
    snprintf(buf, 19, "%d", 1000 * i * 7);
    buf[19] = '\0';

    if(check_matching_rows(db, 0, WG_COND_EQUAL, (void *) buf,
     WG_STRTYPE, 50*50, printlevel)) {
      if(printlevel)
        printf("content check col=0, i=%d failed.\n", i);
      return -2;
    }
  }

  for(i=0; i<50; i++) {
    gint val = 100 * i + 21;
    if(check_matching_rows(db, 1, WG_COND_EQUAL, (void *) &val,
     WG_INTTYPE, dbsize*50, printlevel)) {
      if(printlevel)
        printf("content check col=1, i=%d failed.\n", i);
      return -2;
    }
  }

  for(i=0; i<50; i++) {
    double val = 10 * i - 77.42;
    if(check_matching_rows(db, 2, WG_COND_EQUAL, (void *) &val,
     WG_DOUBLETYPE, dbsize*50, printlevel)) {
      if(printlevel)
        printf("content check col=2, i=%d failed.\n", i);
      return -2;
    }
  }

  if(printlevel > 1)
    printf("------- Running delete queries --------\n");

  /* Delete query */
  for(i=0; i<dbsize; i++) {
    wg_query *query;
    wg_query_arg arglist[3];
    char c1[20];
    snprintf(c1, 19, "%d", 1000 * i * 7);
    c1[19] = '\0';

    arglist[0].column = 0;
    arglist[0].cond = WG_COND_EQUAL;
    arglist[0].value = wg_encode_query_param_str(db, c1, NULL);
    arglist[1].column = 1;
    arglist[1].cond = WG_COND_LESSTHAN;
    arglist[1].value = wg_encode_query_param_int(db, 2021); /* 20 matching */
    arglist[2].column = 2;
    arglist[2].cond = WG_COND_GREATER;
    arglist[2].value = wg_encode_query_param_double(db, 112.58); /* 30 matching */

    query = wg_make_query(db, NULL, 0, arglist, 3);
    if(!query) {
      if(printlevel)
        printf("wg_make_query() failed\n");
      wg_free_query_param(db, arglist[0].value);
      wg_free_query_param(db, arglist[1].value);
      wg_free_query_param(db, arglist[2].value);
      return -4;
    }

    while((rec = wg_fetch(db, query))) {
      if(wg_delete_record(db, rec)) {
        if(printlevel)
          printf("delete failed, aborting.\n");
        wg_free_query_param(db, arglist[0].value);
        wg_free_query_param(db, arglist[1].value);
        wg_free_query_param(db, arglist[2].value);
        return -6;
      }
    }

    wg_free_query_param(db, arglist[0].value);
    wg_free_query_param(db, arglist[1].value);
    wg_free_query_param(db, arglist[2].value);
  }

  if(printlevel > 1)
    printf("------- Checking row count --------\n");

  /* Database scan */
  if(check_db_rows(db, dbsize * (50 * 50 - 30 * 20), printlevel)) {
    if(printlevel)
      printf("row count check failed.\n");
    return -7;
  }

  return 0;
}

/* ------------------------- log testing ------------------------ */

#ifndef _WIN32
#define LOG_TESTFILE  "/tmp/wgdb.logtest"
#else
#define LOG_TESTFILE  "c:\\windows\\temp\\wgdb.logtest"
#endif

static gint wg_check_log(void* db, int printlevel) {
#if defined(USE_DBLOG)
  db_memsegment_header* dbh = dbmemsegh(db);
  db_handle_logdata *ld = ((db_handle *) db)->logdata;
  void *clonedb;
  void *rec1, *rec2;
  gint tmp, str1, str2;
  char logfn[100];
  int i, err, pid;
  int fd;

  if(printlevel>1) {
    printf("********* testing journal logging ********** \n");
  }

  /* Set up the temporary log. We don't use the standard method as
   * that might interfere with real database logs. Also, normally
   * local databases are not logged.
   */
#ifndef _WIN32
  pid = getpid();
#else
  pid = _getpid();
#endif
  snprintf(logfn, 99, "%s.%d", LOG_TESTFILE, pid);
  logfn[99] = '\0';
#ifdef _WIN32
  if(_sopen_s(&fd, logfn, _O_CREAT|_O_APPEND|_O_BINARY|_O_RDWR, _SH_DENYNO,
    _S_IREAD|_S_IWRITE)) {
#else
  if((fd = open(logfn, O_CREAT|O_APPEND|O_RDWR,
    S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)) == -1) {
#endif
    if(printlevel)
      printf("Failed to open the test journal\n");
    return 1;
  }

#ifndef _WIN32
  if(write(fd, WG_JOURNAL_MAGIC, WG_JOURNAL_MAGIC_BYTES) != \
                                          WG_JOURNAL_MAGIC_BYTES) {
    if(printlevel)
      printf("Failed to initialize the test journal\n");
    close(fd);
    return 1;
  }
#else
  if(_write(fd, WG_JOURNAL_MAGIC, WG_JOURNAL_MAGIC_BYTES) != \
                                          WG_JOURNAL_MAGIC_BYTES) {
    if(printlevel)
      printf("Failed to initialize the test journal\n");
    _close(fd);
    return 1;
  }
#endif

  ld->fd = fd;
  ld->serial = dbh->logging.serial;
  dbh->logging.active = 1;

  /* Do various operations in the database:
   * Encode short/long strings, doubles, ints
   * Create records (also with different meta bits)
   * Delete records
   * Set fields
   */
  str1 = wg_encode_str(db, "0000000001000000000200000000030000000004", NULL);
  str2 = wg_encode_str(db, "00000000010000000002", NULL);
  tmp = wg_encode_double(db, -6543.3412);
  rec1 = wg_create_record(db, 7);
  wg_set_field(db, rec1, 4, str1);
  wg_set_field(db, rec1, 5, str2);
  wg_set_field(db, rec1, 6, tmp);

  if(printlevel)
    printf("Expecting a field index error:\n");
  wg_set_field(db, rec1, 7, 0); /* Failed operation, shouldn't be logged */

  rec2 = wg_create_record(db, 6);
  wg_set_field(db, rec2, 1, str1);
  wg_set_field(db, rec2, 3, str2);
  wg_set_field(db, rec2, 5, tmp);

  wg_delete_record(db, rec1);

  rec1 = wg_create_record(db, 10);
  for(i=0; i<10; i++)
    wg_set_field(db, rec1, i, wg_encode_int(db, (~((gint) 0))-i));

  rec1 = wg_create_object(db, 1, 0, 0);
  rec1 = wg_create_array(db, 4, 1, 0);

#ifndef _WIN32
  close(ld->fd);
#else
  _close(ld->fd);
#endif
  ld->fd = -1;

  /* Replay the log in a clone database.
   * Note that replay normally restarts logging using the
   * standard configuration, but here this is not the case as
   * the logging.active flag is not set in the local database.
   */

  clonedb = wg_attach_local_database(800000);
  if(!clonedb) {
    if(printlevel)
      printf("Failed to create a second memory database\n");
    remove(logfn);
    return 1;
  }

  if(wg_replay_log(clonedb, logfn)) {
    if(printlevel)
      printf("Failed to replay the journal\n");
    wg_delete_local_database(clonedb);
    remove(logfn);
    return 1;
  }

  err = 0;

  /* Compare the databases */
  rec1 = wg_get_first_record(db);
  rec2 = wg_get_first_record(clonedb);
  while(rec1) {
    int len1, len2;
    gint meta1, meta2;

    if(!rec2) {
      if(printlevel)
        printf("Error: clone database had fewer records\n");
      err = 1;
      break;
    }

    len1 = wg_get_record_len(db, rec1);
    len2 = wg_get_record_len(clonedb, rec2);
    if(len1 != len2) {
      if(printlevel)
        printf("Error: records had different lengths\n");
      err = 1;
      break;
    }

    meta1 = *((gint *) rec1 + RECORD_META_POS);
    meta2 = *((gint *) rec2 + RECORD_META_POS);
    if(meta1 != meta2) {
      if(printlevel)
        printf("Error: records had different metadata\n");
      err = 1;
      break;
    }

    for(i=0; i<len1; i++) {
      gint type1, type2;
      int intdata1, intdata2;
      double doubledata1, doubledata2;
      char *strdata1, *strdata2;

      type1 = wg_get_field_type(db, rec1, i);
      type2 = wg_get_field_type(clonedb, rec2, i);

      if(type1 != type2) {
        if(printlevel)
          printf("Error: fields had different type\n");
        err = 1;
        goto done;
      }

      switch(type1) {
        case WG_NULLTYPE:
          break;
        case WG_INTTYPE:
          intdata1 = wg_decode_int(db, wg_get_field(db, rec1, i));
          intdata2 = wg_decode_int(db, wg_get_field(clonedb, rec2, i));
          if(intdata1 != intdata2) {
            if(printlevel)
              printf("Error: fields had different value\n");
            err = 1;
            goto done;
          }
          break;
        case WG_DOUBLETYPE:
          doubledata1 = wg_decode_double(db, wg_get_field(db, rec1, i));
          doubledata2 = wg_decode_double(db, wg_get_field(clonedb, rec2, i));
          if(doubledata1 != doubledata2) {
            if(printlevel)
              printf("Error: fields had different value\n");
            err = 1;
            goto done;
          }
          break;
        case WG_STRTYPE:
          strdata1 = wg_decode_str(db, wg_get_field(db, rec1, i));
          strdata2 = wg_decode_str(db, wg_get_field(clonedb, rec2, i));
          if(strcmp(strdata1, strdata2)) {
            if(printlevel)
              printf("Error: fields had different value\n");
            err = 1;
            goto done;
          }
          break;
        default:
          if(printlevel)
            printf("Error: unexpected type\n");
          err = 1;
          goto done;
      }
    }
    rec1 = wg_get_next_record(db, rec1);
    rec2 = wg_get_next_record(clonedb, rec2);
  }
  if(rec2) {
    if(printlevel)
      printf("Error: clone database had more records\n");
  }

done:
  wg_delete_local_database(clonedb);
  remove(logfn);
  if(err)
    return err;

  if(printlevel>1)
    printf("********* journal logging test successful ********** \n");
  return 0;
#else
  printf("logging disabled, skipping checks\n");
  return 77;
#endif
}

/* ------------------ bulk testdata generation ---------------- */

/* Asc/desc/mix integer data functions originally written by Enar Reilent.
 * these functions will generate integer data of given
 * record size into database.
 */

/** Generate integer data with ascending values
 *
 */
int wg_genintdata_asc(void *db, int databasesize, int recordsize){

  int i, j, tmp;
  void *rec;
  wg_int value = 0;
  int increment = 1;
  int incrementincrement = 17;
  int k = 0;
  for (i=0;i<databasesize;i++) {
    rec=wg_create_record(db,recordsize);
    if (rec==NULL) {
      printf("rec creation error\n");
      continue;
    }

    for(j=0;j<recordsize;j++){
      tmp=wg_set_int_field(db,rec,j,value+j);
      if (tmp!=0) {
        printf("int storage error\n");
      }
    }
    value += increment;
    if(k % 2 == 0)increment += 2;
    else increment -= 1;
    k++;
    if(k == incrementincrement) {increment = 1; k = 0;}
  }

  return 0;
}

/** Generate integer data with descending values
 *
 */
int wg_genintdata_desc(void *db, int databasesize, int recordsize){

  int i, j, tmp;
  void *rec;
  wg_int value = 100000;
  int increment = -1;
  int incrementincrement = 17;
  int k = 0;
  for (i=0;i<databasesize;i++) {
    rec=wg_create_record(db,recordsize);
    if (rec==NULL) {
      printf("rec creation error\n");
      continue;
    }

    for(j=0;j<recordsize;j++){
      tmp=wg_set_int_field(db,rec,j,value+j);
      if (tmp!=0) {
        printf("int storage error\n");
      }
    }
    value += increment;
    if(k % 2 == 0)increment -= 2;
    else increment += 1;
    k++;
    if(k == incrementincrement) {increment = -1; k = 0;}
  }

  return 0;
}

/** Generate integer data with mixed values
 *
 */
int wg_genintdata_mix(void *db, int databasesize, int recordsize){

  int i, j, tmp;
  void *rec;
  wg_int value = 1;
  int increment = 1;
  int incrementincrement = 18;
  int k = 0;
  for (i=0;i<databasesize;i++) {
    rec=wg_create_record(db,recordsize);
    if (rec==NULL) {
      printf("rec creation error\n");
      continue;
    }
    if(k % 2)value = 10000 - value;
    for(j=0;j<recordsize;j++){
      tmp=wg_set_int_field(db,rec,j,value+j);
      if (tmp!=0) {
        printf("int storage error\n");
      }
    }
    if(k % 2)value = 10000 - value;
    value += increment;


    if(k % 2 == 0)increment += 2;
    else increment -= 1;
    k++;
    if(k == incrementincrement) {increment = 1; k = 0;}
  }

  return 0;
}


void wg_debug_print_value(void *db, gint data) {
  gint ptrdata;
  int intdata;
  char *strdata, *exdata;
  double doubledata;
  char strbuf[1024];
  char buf[1024];
  int buflen=1023;
  /*gint fieldoffset;
  gint tmp;*/
  gint enc=data;
  gint offset;
  gint refc;
  gint type;

  type=wg_get_encoded_type(db, enc);
  switch(type) {
    case WG_NULLTYPE:
      snprintf(buf, buflen, "null:NULL");
      break;
    case WG_RECORDTYPE:
      ptrdata = (gint) wg_decode_record(db, enc);
      snprintf(buf, buflen, "record:<record at %x>", (int) ptrdata);
      //len = strlen(buf);
      //if(buflen - len > 1)
      //  snprint_record(db, (wg_int*)ptrdata, buf+len, buflen-len);
      break;
    case WG_INTTYPE:
      intdata = wg_decode_int(db, enc);
      if (issmallint(enc))
        snprintf(buf, buflen, "smallint:%d", intdata);
      else
        snprintf(buf, buflen, "longint:%d", intdata);
      break;
    case WG_DOUBLETYPE:
      doubledata = wg_decode_double(db, enc);
      snprintf(buf, buflen, "double:%f", doubledata);
      break;
    case WG_STRTYPE:
      strdata = wg_decode_str(db, enc);
      if ((enc&NORMALPTRMASK)==LONGSTRBITS) {
        /*fieldoffset=decode_longstr_offset(enc)+LONGSTR_META_POS*sizeof(gint);*/
        //printf("fieldoffset %d\n",fieldoffset);
        /*tmp=dbfetch(db,fieldoffset); */
        offset=decode_longstr_offset(enc);
        refc=dbfetch(db,offset+LONGSTR_REFCOUNT_POS*sizeof(gint));
        if (1) { //(tmp&LONGSTR_META_TYPEMASK)==WG_STRTYPE) {
          snprintf(buf, buflen, "longstr: len %d refcount %d str \"%s\" extrastr \"%s\"",
             (int) wg_decode_unistr_len(db,enc,type),
             (int) refc,
             wg_decode_unistr(db,enc,type),
             wg_decode_unistr_lang(db,enc,type));
        }
        /*
        } else if ((tmp&LONGSTR_META_TYPEMASK)==WG_URITYPE) {
          snprintf(buf, buflen, "uri:\"%s\"", strdata);
        } else if ((tmp&LONGSTR_META_TYPEMASK)==WG_XMLLITERALTYPE) {
          snprintf(buf, buflen, "xmlliteral:\"%s\"", strdata);
        } else {
          snprintf(buf, buflen, "unknown_str_subtype %d",tmp&LONGSTR_META_TYPEMASK);
        }
        */
      } else {
        snprintf(buf, buflen, "shortstr: len %d str \"%s\"",
          (int) wg_decode_str_len(db,enc),
          wg_decode_str(db,enc));
      }
      break;
    case WG_URITYPE:
      strdata = wg_decode_uri(db, enc);
      exdata = wg_decode_uri_prefix(db, enc);
      snprintf(buf, buflen, "uri:\"%s%s\"", exdata, strdata);
      break;
    case WG_XMLLITERALTYPE:
      strdata = wg_decode_xmlliteral(db, enc);
      exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      snprintf(buf, buflen, "xmlliteral:\"<xsdtype %s>%s\"", exdata, strdata);
      break;
    case WG_BLOBTYPE:
      //strdata = wg_decode_blob(db, enc);
      //exdata = wg_decode_xmlliteral_xsdtype(db, enc);
      snprintf(buf, buflen, "blob: len %d extralen %d",
         (int) wg_decode_blob_len(db,enc),
         (int) wg_decode_blob_type_len(db,enc));
      break;
    case WG_CHARTYPE:
      intdata = wg_decode_char(db, enc);
      snprintf(buf, buflen, "char:%c", (char) intdata);
      break;
    case WG_DATETYPE:
      intdata = wg_decode_date(db, enc);
      wg_strf_iso_datetime(db,intdata,0,strbuf);
      strbuf[10]=0;
      snprintf(buf, buflen, "date:<raw date %d>%s", intdata,strbuf);
      break;
    case WG_TIMETYPE:
      intdata = wg_decode_time(db, enc);
      wg_strf_iso_datetime(db,1,intdata,strbuf);
      snprintf(buf, buflen, "time:<raw time %d>%s",intdata,strbuf+11);
      break;
    default:
      snprintf(buf, buflen, "<unsupported type>");
      break;
  }
  printf("enc %d %s", (int) enc, buf);
}

/**
 * General sanity checks
 */
static int check_sanity(void *db) {
#ifdef HAVE_64BIT_GINT
  if(sizeof(gint) != 8) {
    printf("gint size sanity check failed\n");
    return 1;
  }
#else
  if(sizeof(gint) != 4) {
    printf("gint size sanity check failed\n");
    return 1;
  }
#endif
  return 0;
}

#ifdef __cplusplus
}
#endif
