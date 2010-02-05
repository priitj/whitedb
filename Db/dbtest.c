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

 /** @file dbtest.c
 *  Database testing, checking and report procedures
 *
 */

/* ====== Includes =============== */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbdata.h"
#include "dbhash.h"
#include "dbtest.h"

/* ====== Private headers and defs ======== */

#ifdef _WIN32
#define snprintf sprintf_s 
#endif

/* ======= Private protos ================ */

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


/* ====== Functions ============== */

int wg_run_tests(void* db, int printlevel) {
  int tmp;
  
  wg_show_db_memsegment_header(db);
  tmp=wg_check_db(db);  
  if (tmp==0) tmp=wg_check_datatype_writeread(db,printlevel);
  if (tmp==0) tmp=wg_check_db(db);
  if (tmp==0) tmp=wg_check_strhash(db,printlevel);
  if (tmp==0) {
    printf("\n***** all tests passed ******\n");
    return 0;
  } else {
    printf("\n***** test failed ******\n");
    return tmp;
  }    
}  

/* ---------------- overviews, statistics ---------------------- */

/** print an overview of full memsegment memory  usage and addresses
*
*
*/


void wg_show_db_memsegment_header(void* db) {
  db_memsegment_header* dbh;
  
  dbh=(db_memsegment_header*) db;
  
  printf("\nShowing db segment information\n");
  printf("==============================\n");
  printf("mark %d\n",dbh->mark);
  printf("size %d\n",dbh->size);
  printf("free %d\n",dbh->free);
  printf("initialadr %x\n",dbh->initialadr);
  printf("key  %d\n",dbh->key);  
  printf("segment header size %d\n",sizeof(db_memsegment_header));
  printf("subarea  array size %d\n",SUBAREA_ARRAY_SIZE);
  
  printf("\ndatarec_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->datarec_area_header));
  printf("\nlongstr_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->longstr_area_header));
  printf("\nlistcell_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->listcell_area_header));
  printf("\nshortstr_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->shortstr_area_header));
  printf("\nword_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->word_area_header));
  printf("\ndoubleword_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->doubleword_area_header));
  printf("\ntnode_area\n");
  printf("-------------\n");  
  wg_show_db_area_header(dbh,&(dbh->tnode_area_header));
}

/** print an overview of a single area memory usage and addresses
*
*
*/

void wg_show_db_area_header(void* db, void* area_header) {
  db_area_header* areah;  
  gint i;
  
  areah=(db_area_header*)area_header; 
  if (areah->fixedlength) {  
    printf("fixedlength with objlength %d bytes\n",areah->objlength);
    printf("freelist %d\n",areah->freelist);
    printf("freelist len %d\n",wg_count_freelist(db,areah->freelist));
  } else {
    printf("varlength\n");
  }    
  printf("last_subarea_index %d\n",areah->last_subarea_index);  
  for (i=0;i<=(areah->last_subarea_index);i++) {
    printf("subarea nr %d \n",i);
    printf("  size     %d\n",((areah->subarea_array)[i]).size);
    printf("  offset        %d\n",((areah->subarea_array)[i]).offset);    
    printf("  alignedsize   %d\n",((areah->subarea_array)[i]).alignedsize);
    printf("  alignedoffset %d\n",((areah->subarea_array)[i]).alignedoffset);
  }  
  for (i=0;i<EXACTBUCKETS_NR+VARBUCKETS_NR;i++) {
    if ((areah->freebuckets)[i]!=0) {
      printf("bucket nr %d \n",i);
      if (i<EXACTBUCKETS_NR) {
        printf(" is exactbucket at offset %d\n",dbaddr(db,&(areah->freebuckets)[i])); 
        wg_show_bucket_freeobjects(db,(areah->freebuckets)[i]);
      } else {
        printf(" is varbucket at offset %d \n",dbaddr(db,&(areah->freebuckets)[i]));          
        wg_show_bucket_freeobjects(db,(areah->freebuckets)[i]);
      }              
    }  
  }
  if ((areah->freebuckets)[DVBUCKET]!=0) {
    printf("bucket nr %d at offset %d \n contains dv at offset %d with size %d(%d) and end %d \n",           
          DVBUCKET,dbaddr(db,&(areah->freebuckets)[DVBUCKET]),
          (areah->freebuckets)[DVBUCKET],
          ((areah->freebuckets)[DVSIZEBUCKET]>0 ? dbfetch(db,(areah->freebuckets)[DVBUCKET]) : -1),
          (areah->freebuckets)[DVSIZEBUCKET],
          (areah->freebuckets)[DVBUCKET]+(areah->freebuckets)[DVSIZEBUCKET]);
  }  
}



/** show a list of free objects in a bucket
*
*/

void wg_show_bucket_freeobjects(void* db, gint freelist) {
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
            freelist,freelist+size,freebits,size,nextptr,prevptr);
    freelist=nextptr;
  }  
}  




/** count elements in a freelist
*
*/

gint wg_count_freelist(void* db, gint freelist) {
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

gint wg_check_datatype_writeread(void* db, gint printlevel) {  
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
  int intdata_nr=4;
  int doubledata_nr=4;
  int fixpointdata_nr=5;
  int datedata_nr=4;  
  int timedata_nr=4;
  int datevecdata_nr=4;  
  int datevecbad_nr=2;  
  int timevecdata_nr=4;  
  int timevecbad_nr=4; 
  int strdata_nr=4;
  int xmlliteraldata_nr=2;
  int uridata_nr=2;
  int blobdata_nr=3;  
  int recdata_nr=10;
  
  // tested data buffers
  char* nulldata[10];
  char chardata[10];
  int intdata[10]; 
  double doubledata[10];
  double fixpointdata[10];
  int timedata[10];
  int datedata[10];
  char* datetimedata[10];  
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
  datetimedata[0]="asasas";
  datetimedata[1]="asasas";
  datetimedata[2]="asasas";
  datetimedata[3]="asasas";
  
  fixpointdata[0]=0;
  fixpointdata[1]=1.23;
  fixpointdata[2]=790.3456;
  fixpointdata[3]=-799.7891;
  fixpointdata[4]=0.002345678;
  
  strdata[0]="abc";
  strdata[1]="abcdefghijklmnop";
  strdata[2]="1234567890123456789012345678901234567890";
  strdata[3]="";
  strextradata[0]=NULL;
  strextradata[1]=NULL;
  strextradata[2]="op12345";
  strextradata[3]="asdasdasdsd";
  xmlliteraldata[0]="ffoo";
  xmlliteraldata[1]="ffooASASASasaasweerrtttyyuuu";
  xmlliteralextradata[0]="bar:we";
  xmlliteralextradata[1]="bar:weasdasdasdasdasdasdasdasdasdasdasdasdasddas";
  uridata[0]="dasdasdasd";
  uridata[1]="dasdasdasd12345678901234567890";
  uriextradata[0]="";
  uriextradata[1]="fofofofof";
 
  blobdata[0]=malloc(10);
  for(i=0;i<10;i++) *(blobdata[0]+i)=i+65;
  blobextradata[0]="type1";
  bloblendata[0]=10;

  blobdata[1]=malloc(1000);
  for(i=0;i<1000;i++) *(blobdata[1]+i)=(i%10)+65;   //i%256;
  blobextradata[1]="type2";
  bloblendata[1]=200;
  
  blobdata[2]=malloc(10);
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
      rec=wg_create_record(db,recdata[j]);
      if (rec==NULL) {
        if (p) printf("check_datatype_writeread gave error: creating record for j %d len %d failed\n",
                      j,recdata[j]); 
        return 1;
      }
      if (wg_get_encoded_type(db,(gint)rec)!=WG_RECORDTYPE) {
        if (p) printf("check_datatype_writeread gave error: created record not right type for j %d len %d\n",
                      j,recdata[j]); 
        return 1;
      }
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
    
    // rec test ended
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


/* --------------- allocation, storage, updafe and deallocation tests ---------- */
/*
gint wg_check_allocation_deallocation(void* db, gint printlevel) { 
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

gint wg_check_strhash(void* db, gint printlevel) {
  int p;
  int i,j;
  char* lang;
  gint tmp;
  gint strs[100];
  char instrbuf[200];
  gint enc;
  gint* rec;
  int records=3;
  int flds=2;
  
  p=printlevel;
  if (p>1) printf("********* testing strhash ********** \n");
  for(i=0;i<100;i++) strs[i]=0;
  
  if (p>1) printf("---------- initial hashtable -----------\n");
  if (p>1) wg_show_strhash(db);  
  
  for (i=0;i<records;i++) {    
    rec=wg_create_record(db,flds);
    if (rec==NULL) { 
      if (p) printf("rec creation error in wg_check_strhash i %d\n",i);
      return 1;    
    }
    for(j=0;j<flds;j++) {
      snprintf(instrbuf,100,"%da1234567890123456789012345678901234567890",i);
      lang="en";        
      enc=wg_encode_str(db,instrbuf,lang);       
      strs[i]=enc;  
      if (p>1) printf("wg_set_field rec %d fld %d str '%s' lang '%s' encoded %d\n",
                     (int)i,(int)j,instrbuf,lang,(int)enc);      
    }      
    tmp=wg_set_field(db,rec,j,enc);      
  }        
  
  if (p>1) printf("---------- hashtable after str adding -----------\n");
  if (p>1) wg_show_strhash(db);  
  
  /*
  if (p>1) printf("---------- testing str removals --------- \n");
 
  for(i=9;i>=0;i--) {
    if (p>1) printf("removing str nr %d \n",i);
    if (strs[i]==0) break;  
    j=wg_remove_from_strhash(db,strs[i]);          
    if (p>1) printf("removal result %d\n",j);
    wg_show_strhash(db);       
  }    
  if (p>1) printf("---------- ending str removals ----------\n");
  
  if (p>1) printf("---------- hashtable after str removals -----------\n");
  if (p>1) wg_show_strhash(db); 
  */
  if (p>1)printf("********* strhash testing ended without errors ********** \n");
  return 0;  
}  


void wg_show_strhash(void* db) {
  db_memsegment_header* dbh;
  gint i;
  gint hashchain;
  gint lasthashchain;
  gint type; 
  gint offset;
  gint refc;
  int encoffset;
    
  dbh=(db_memsegment_header*) db;
  printf("\nshowing strhash table and buckets\n"); 
  printf("-----------------------------------\n");
  printf("INITIAL_STRHASH_LENGTH %d\n",INITIAL_STRHASH_LENGTH);
  printf("size %d\n",(dbh->strhash_area_header).size);
  printf("offset %d\n",(dbh->strhash_area_header).offset);
  printf("arraystart %d\n",(dbh->strhash_area_header).arraystart);
  printf("arraylength %d\n",(dbh->strhash_area_header).arraylength);
  printf("nonempty hash buckets:\n");
  for(i=0;i<(dbh->strhash_area_header).arraylength;i++) {
    hashchain=dbfetch(db,(dbh->strhash_area_header).arraystart+(sizeof(gint)*i));
    lasthashchain=hashchain;    
    if (hashchain!=0) {
      printf("%d: contains %d encoded offset to chain\n",i,hashchain);      
      for(;hashchain!=0;             
          hashchain=dbfetch(db,decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint))) {          
          //printf("hashchain %d decode_longstr_offset(hashchain) %d fulladr %d contents %d\n",
          //       hashchain,
          //       decode_longstr_offset(hashchain),
          //       (decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint)),
          //       dbfetch(db,decode_longstr_offset(hashchain)+LONGSTR_HASHCHAIN_POS*sizeof(gint)));    
          type=wg_get_encoded_type(db,hashchain);
          printf("  type %s",wg_get_type_name(db,type));
          if (type==WG_BLOBTYPE) {
            printf(" len %d\n",wg_decode_str_len(db,hashchain)); 
          } else if (type==WG_STRTYPE || type==WG_XMLLITERALTYPE || 
                     type==WG_URITYPE || type== WG_ANONCONSTTYPE) {
            offset=decode_longstr_offset(hashchain);      
            refc=dbfetch(db,offset+LONGSTR_REFCOUNT_POS);           
            printf(" refcount %d len %d str %s extra %s\n",
                    refc,   
                    wg_decode_unistr_len(db,hashchain,type),                    
                    wg_decode_unistr(db,hashchain,type),
                    wg_decode_unistr_lang(db,hashchain,type));
          } else {
            printf("ERROR: wrong type in strhash bucket\n");
            exit(0);
          }
          lasthashchain=hashchain;
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


gint wg_check_db(void* db) {
  gint res;
  db_memsegment_header* dbh;
  
  dbh=(db_memsegment_header*) db;
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
              freelist,dbfetch(db,freelist));
      return 1;
    }  
    size=getfreeobjectsize(dbfetch(db,freelist)); 
    if (bucketindex!=wg_freebuckets_index(db,size)) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d with size %d is in wrong bucket %d instead of right %d\n",
              freelist,size,bucketindex,wg_freebuckets_index(db,size));
      return 2;
    }      
    if (getfreeobjectsize(dbfetch(db,freelist+size-sizeof(gint)))!=size) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d has wrong end size %d which is not same as start size %d\n",
              freelist,dbfetch(db,freelist+size-sizeof(gint)),size);
      return 3;
    }  
    nextptr=dbfetch(db,freelist+sizeof(gint));
    prevptr=dbfetch(db,freelist+2*sizeof(gint));
    if (prevptr!=prevfreelist) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d has a wrong prevptr: %d instead of %d\n",
              freelist,prevptr,prevfreelist);
      return 4;
    }   
    tmp=check_object_in_areabounds(db,area_header,freelist,size);    
    if (tmp) {
      printf("varlen freelist object error:\n");
      if (tmp==1) {
        printf("object at offset %d does not start in the area bounds\n",
              freelist);
        return 5;
      } else {
        printf("object at offset %d does not end (%d) in the same area it starts\n",
              freelist,freelist+size);
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
  db_subarea_header* arrayadr;
  db_area_header* areah;      
  gint last_subarea_index;
  gint i;
  gint size;
  gint subareastart;
  gint subareaend;
  gint offset;
  gint head;
  
  areah=(db_area_header*)area_header;
  arrayadr=(areah->subarea_array);
  last_subarea_index=areah->last_subarea_index;
  for(i=0;(i<=last_subarea_index)&&(i<SUBAREA_ARRAY_SIZE);i++) {
    
    size=((areah->subarea_array)[i]).alignedsize;
    subareastart=((areah->subarea_array)[i]).alignedoffset;
    subareaend=(((areah->subarea_array)[i]).alignedoffset)+size;
    
    // start marker
    offset=subareastart;
    head=dbfetch(db,offset);
    if (!isspecialusedobject(head)) {
      printf("start marker at offset %d has head %d which is not specialusedobject\n",
              offset,head);
      return 21; 
    }  
    if (getspecialusedobjectsize(head)!=MIN_VARLENOBJ_SIZE) {
      printf("start marker at offset %d has size %d which is not MIN_VARLENOBJ_SIZE %d\n",
              offset,getspecialusedobjectsize(head),MIN_VARLENOBJ_SIZE);
      return 22; 
    } 
    if (dbfetch(db,offset+sizeof(gint))!=SPECIALGINT1START) {
      printf("start marker at offset %d has second gint %d which is not SPECIALGINT1START %d\n",
              offset,dbfetch(db,offset+sizeof(gint)),SPECIALGINT1START );
      return 23; 
    }
    
    //end marker
    offset=offset+size-MIN_VARLENOBJ_SIZE;
    head=dbfetch(db,offset);
    if (!isspecialusedobject(head)) {
      printf("end marker at offset %d has head %d which is not specialusedobject\n",
              offset,head);
      return 21; 
    }  
    if (getspecialusedobjectsize(head)!=MIN_VARLENOBJ_SIZE) {
      printf("end marker at offset %d has size %d which is not MIN_VARLENOBJ_SIZE %d\n",
              offset,getspecialusedobjectsize(head),MIN_VARLENOBJ_SIZE);
      return 22; 
    } 
    if (dbfetch(db,offset+sizeof(gint))!=SPECIALGINT1END) {
      printf("end marker at offset %d has second gint %d which is not SPECIALGINT1END %d\n",
              offset,dbfetch(db,offset+sizeof(gint)),SPECIALGINT1END );
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
          DVBUCKET,dbaddr(db,&(areah->freebuckets)[DVBUCKET]),
          dv,
          ((areah->freebuckets)[DVSIZEBUCKET]>0 ? dbfetch(db,(areah->freebuckets)[DVBUCKET]) : -1),
          (areah->freebuckets)[DVSIZEBUCKET],
          (areah->freebuckets)[DVBUCKET]+(areah->freebuckets)[DVSIZEBUCKET]);
    if (!isspecialusedobject(dbfetch(db,dv))) {
      printf("dv at offset %d has head %d which is not marked specialusedobject\n",
              dv,dbfetch(db,dv));
      return 10;      
    } 
    if ((areah->freebuckets)[DVSIZEBUCKET]!=getspecialusedobjectsize(dbfetch(db,dv))) {
      printf("dv at offset %d has head %d with size %d which is different from freebuckets[DVSIZE] %d\n",
              dv,dbfetch(db,dv),getspecialusedobjectsize(dbfetch(db,dv)),(areah->freebuckets)[DVSIZEBUCKET]);
      return 11;  
    }  
    if (getspecialusedobjectsize(dbfetch(db,dv))<MIN_VARLENOBJ_SIZE) {
      printf("dv at offset %d has size %d which is smaller than MIN_VARLENOBJ_SIZE %d\n",
              dv,getspecialusedobjectsize(dbfetch(db,dv)),MIN_VARLENOBJ_SIZE);
      return 12;      
    }
    if (SPECIALGINT1DV!=dbfetch(db,dv+sizeof(gint))) {
      printf("dv at offset %d has second gint %d which is not SPECIALGINT1DV %d\n",
              dv,dbfetch(db,dv+sizeof(gint)),SPECIALGINT1DV);
      return 12;      
    }
    tmp=check_object_in_areabounds(db,area_header,dv,getspecialusedobjectsize(dbfetch(db,dv)));    
    if (tmp) {
      printf("dv error:\n");
      if (tmp==1) {
        printf("dv at offset %d does not start in the area bounds\n",
              dv);
        return 13;
      } else {
        printf("dv at offset %d does not end (%d) in the same area it starts\n",
              dv,dv+getspecialusedobjectsize(dbfetch(db,dv)));
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
  db_subarea_header* arrayadr;
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
  gint offset;

  areah=(db_area_header*)area_header;    
  arrayadr=(areah->subarea_array);
  last_subarea_index=areah->last_subarea_index;
  dv=(areah->freebuckets)[DVBUCKET]; 
  
  for(i=0;(i<=last_subarea_index)&&(i<SUBAREA_ARRAY_SIZE);i++) {
    
    size=((areah->subarea_array)[i]).alignedsize;
    subareastart=((areah->subarea_array)[i]).alignedoffset;
    subareaend=(((areah->subarea_array)[i]).alignedoffset)+size;
    
    // start marker
    offset=subareastart;      
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
                i,curoffset,getusedobjectsize(head),subareastart+size); 
           return 32;  
      }        
      head=dbfetch(db,curoffset);
      //printf("new curoffset %d head %d isnormaluseobject %d isfreeobject %d \n",
      //       curoffset,head,isnormalusedobject(head),isfreeobject(head));
      // check if found a normal used object
      if (isnormalusedobject(head)) {
        if (freemarker && !isnormalusedobjectprevfree(head)) {
           printf("inuse normal object areanr %d offset %d size %d follows free but is not marked to follow free\n",
                i,curoffset,getusedobjectsize(head)); 
           return 31;
        } else if (!freemarker &&  !isnormalusedobjectprevused(head)) {
           printf("inuse normal object areanr %d offset %d size %d follows used but is not marked to follow used\n",
                i,curoffset,getusedobjectsize(head)); 
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
                i,curoffset,getfreeobjectsize(head)); 
           return 33;
        }  
        if (dvmarker) {
           printf("free object areanr %d offset %d size %d follows dv\n",
                i,curoffset,getfreeobjectsize(head)); 
           return 34;
        }
        tmp=check_varlen_object_infreelist(db,area_header,curoffset,1);
        if (tmp!=1) {
           printf("free object areanr %d offset %d size %d not found in freelist\n",
                i,curoffset,getfreeobjectsize(head)); 
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
                i,curoffset,getspecialusedobjectsize(head),dv); 
            return 35;
          }  
          if (dvcount!=0) {
            printf("second dv object found areanr %d offset %d size %d\n",
                i,curoffset,getspecialusedobjectsize(head)); 
            return 36;
          }  
          if (getspecialusedobjectsize(head)<MIN_VARLENOBJ_SIZE) {
            printf("second dv object found areanr %d offset %d size %d is smaller than MIN_VARLENOBJ_SIZE %d\n",
                i,curoffset,getspecialusedobjectsize(head),MIN_VARLENOBJ_SIZE); 
            return 37;
          } 
          if (freemarker) {
            printf("dv object areanr %d offset %d size %d follows free\n",
                i,curoffset,getspecialusedobjectsize(head)); 
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
                i,curoffset,getspecialusedobjectsize(head),subareaend-MIN_VARLENOBJ_SIZE); 
            return 39; 
          }  
          // we have reached an ok end marker, break while loop
          break;
        }        
      }
    }
  }
  printf("usedcount %d\n",usedcount);
  printf("usedbytesrealcount %d\n",usedbytesrealcount);
  printf("usedbyteswantedcount %d\n",usedbyteswantedcount);
  printf("freecount %d\n",freecount);
  printf("freebytescount %d\n",freebytescount);
  printf("dvcount %d\n",dvcount);
  printf("dvbytescount %d\n",dvbytescount);
  return 0;
}  

static gint check_varlen_object_infreelist(void* db, void* area_header, gint offset, gint isfree) {    
  gint head;     
  db_area_header* areah;
  gint freelist;
  gint size; 
  gint prevfreelist;
  gint bucketindex;
  gint objsize;
  
  head=dbfetch(db,offset);
  size=getfreeobjectsize(head);   
  bucketindex=wg_freebuckets_index(db,size);
  areah=(db_area_header*)area_header;
  freelist=(areah->freebuckets)[bucketindex];
  prevfreelist=0;
  while(freelist!=0) {
    objsize=getfreeobjectsize(dbfetch(db,freelist));
    if (isfree) {
      if (offset==freelist) return 1; 
    } else {
      if (offset==freelist) {
        printf("used object at offset %d in freelist for bucket %d\n",
                offset,bucketindex); 
        return 51; 
      }        
      if (offset>freelist && freelist+objsize>offset) {
        printf("used object at offset %d inside freelist object at %d size %d for bucket %d\n",
                offset,freelist,objsize,bucketindex); 
        return 52; 
      } 
    }         
    freelist=dbfetch(db,freelist+sizeof(gint));  
  }
  return 0;  
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
