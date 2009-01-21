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

#include "../config.h"
#include "dballoc.h"
#include "dbtest.h"

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */




/* ====== Functions ============== */



/* ---------------- overviews, statistics ---------------------- */

/** print an overview of full memsegment memory  usage and addresses
*
*
*/


void show_db_memsegment_header(void* db) {
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
  show_db_area_header(dbh,&(dbh->datarec_area_header));
  printf("\nlongstr_area\n");
  printf("-------------\n");  
  show_db_area_header(dbh,&(dbh->longstr_area_header));
  printf("\nlistcell_area\n");
  printf("-------------\n");  
  show_db_area_header(dbh,&(dbh->listcell_area_header));
  printf("\nshortstr_area\n");
  printf("-------------\n");  
  show_db_area_header(dbh,&(dbh->shortstr_area_header));
  printf("\nword_area\n");
  printf("-------------\n");  
  show_db_area_header(dbh,&(dbh->word_area_header));
  printf("\ndoubleword_area\n");
  printf("-------------\n");  
  show_db_area_header(dbh,&(dbh->doubleword_area_header));
  
  
  
}

/** print an overview of a single area memory usage and addresses
*
*
*/

void show_db_area_header(void* db, void* area_header) {
  db_area_header* areah;  
  gint i;
  
  areah=(db_area_header*)area_header; 
  if (areah->fixedlength) {  
    printf("fixedlength with objlength %d bytes\n",areah->objlength);
    printf("freelist %d\n",areah->freelist);
    printf("freelist len %d\n",count_freelist(db,areah->freelist));
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
        show_bucket_freeobjects(db,(areah->freebuckets)[i]);
      } else {
        printf(" is varbucket at offset %d \n",dbaddr(db,&(areah->freebuckets)[i]));          
        show_bucket_freeobjects(db,(areah->freebuckets)[i]);
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

void show_bucket_freeobjects(void* db, gint freelist) {
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

gint count_freelist(void* db, gint freelist) {
  gint i;
  //printf("freelist %d dbfetch(db,freelist) %d\n",freelist,dbfetch(db,freelist));
  
  for(i=0;freelist; freelist=dbfetch(db,freelist)) {
    //printf("i %d freelist %u\n",i,(uint)freelist);
    i++;
  }  
  return i;
}  

/* --------------- checking and testing ------------------------------*/

/** check if varlen freelist is ok
* 
* return 0 if ok, error nr if wrong
* in case of error an errmsg is printed and function returns immediately
*
*/


gint check_db(void* db) {
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

gint check_varlen_area(void* db, void* area_header) { 
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

gint check_varlen_area_freelist(void* db, void* area_header) {
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


gint check_bucket_freeobjects(void* db, void* area_header, gint bucketindex) {
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
    if (bucketindex!=freebuckets_index(db,size)) {
      printf("varlen freelist object error:\n");
      printf("object at offset %d with size %d is in wrong bucket %d instead of right %d\n",
              freelist,size,bucketindex,freebuckets_index(db,size));
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


gint check_varlen_area_markers(void* db, void* area_header) {
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


gint check_varlen_area_dv(void* db, void* area_header) {
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

gint check_object_in_areabounds(void* db,void* area_header,gint offset,gint size) {
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


gint check_varlen_area_scan(void* db, void* area_header) {
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

gint check_varlen_object_infreelist(void* db, void* area_header, gint offset, gint isfree) {    
  gint head;     
  db_area_header* areah;
  gint freelist;
  gint size; 
  gint prevfreelist;
  gint bucketindex;
  gint objsize;
  
  head=dbfetch(db,offset);
  size=getfreeobjectsize(head);   
  bucketindex=freebuckets_index(db,size);
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
  
  
