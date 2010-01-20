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

 /** @file dbmem.c
 *  Allocating/detaching system memory: shared memory and allocated ordinary memory
 *
 */

/* ====== Includes =============== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/shm.h>
#include <sys/errno.h>
#endif
#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbmem.h"

/* ====== Private headers and defs ======== */

/* ======= Private protos ================ */

static void* link_shared_memory(int key);
static void* create_shared_memory(int key,int size);
static int free_shared_memory(int key);

static int detach_shared_memory(void* shmptr);


/* ====== Functions ============== */


/* ----------- dbase creation and deletion api funs ------------------ */

/*
 returns a pointer to the database, NULL if failure
*/

void* wg_attach_database(char* dbasename, int size){
  
  void* shm;
  int tmp;
  int key=0;
  
  // default args handling
  if (dbasename!=NULL) key=strtol(dbasename,NULL,10);
  if (key<=0 || key==LONG_MIN || key==LONG_MAX) key=DEFAULT_MEMDBASE_KEY;
  if (size<=0) size=DEFAULT_MEMDBASE_SIZE;
  
  // first try to link to already existing block with this key
  shm=link_shared_memory(key);
  if (shm!=NULL) {
    // managed to link to already existing shared memory block        
    //printf("successfully linked to existing shared memory block %d with ptr %x\n",key,(gint)shm);       
    return shm;
  } else { 
    // linking to already existing block failed: create a new block
    //printf("no shared memory block with key %d found, creating new\n",key);  
    shm = create_shared_memory(key,size);
    if (shm==NULL) {
      printf("create_shared_memory gave error\n");    
      return NULL;
    } else {
      tmp=wg_init_db_memsegment(shm,key,size);
      if (tmp) {
        printf("wg_init_db_memsegment gave error\n");    
        return NULL; 
      }  
    }
    return shm;
  }   
}

/*

 detaches a database: returns 0 if OK

*/

int wg_detach_database(void* dbase) {
  return detach_shared_memory(dbase);    
}  

/*

 deletes a database: returns 0 if OK

*/


int wg_delete_database(char* dbasename) {
  int key=0;
  
  // default args handling
  if (dbasename!=NULL) key=strtol(dbasename,NULL,10);
  if (key<=0 || key==LONG_MIN || key==LONG_MAX) key=DEFAULT_MEMDBASE_KEY;
  return free_shared_memory(key);  
}



/* --------- local memory db creation and deleting ---------- */
/* XXX: names of API calls under construction */

/** Create a database in local memory
 * returns a pointer to the database, NULL if failure.
 */

void* wg_attach_local(int size) {
  void* shm;
  
  if (size<=0) size=DEFAULT_MEMDBASE_SIZE;
  
  shm = (void *) malloc(size);
  if (shm==NULL) {
    printf("malloc error in wg_attach_local\n");  /* XXX: error funcs? */
    return NULL;
  } else {
    /* key=0 - no shared memory associated */
    if (wg_init_db_memsegment(shm, 0, size)) {
      printf("wg_init_db_memsegment gave error\n");    
      return NULL; 
    }  
  }
  return shm;
}

/** Free a database in local memory
 * frees the allocated memory.
 */

void wg_detach_local(void* dbase) {
  if(dbase) free(dbase);
}  


/* --------------- dbase create/delete ops not in api ----------------- */


static void* link_shared_memory(int key) {  
  void *shm;
      
#ifdef _WIN32 
  char fname[MAX_FILENAME_SIZE];
  HANDLE hmapfile;
    
  sprintf_s(fname,MAX_FILENAME_SIZE-1,"%d",key);  
  hmapfile = OpenFileMapping(
                   FILE_MAP_ALL_ACCESS,   // read/write access
                   FALSE,                 // do not inherit the name
                   fname);               // name of mapping object   
  errno = 0;  
  if (hmapfile == NULL) {
      /* this is an expected error, message in most cases not wanted --p
      printf("Could not open file mapping object (%d).\n",GetLastError());*/
      return NULL;
   }
   shm = (void*) MapViewOfFile(hmapfile,   // handle to map object
                        FILE_MAP_ALL_ACCESS, // read/write permission
                        0,                   
                        0,                   
                        0);   // size of mapping        
   if (shm == NULL)  { 
      printf("Could not map view of file (%d).\n", GetLastError()); 
      CloseHandle(hmapfile);
      return NULL;
   }  
   return shm;
#else       
  int size=0;
  int shmflg; /* shmflg to be passed to shmget() */ 
  int shmid; /* return value from shmget() */ 
   
  errno = 0;  
  // Link to existing segment
  shmflg=0666;
  shmid=shmget((key_t)key,size,shmflg);
  if (shmid < 0) {  	
    //printf("linking to created shared memory segment failed\n");
    return NULL;
  }
  // Attach the segment to our data space
  shm=shmat(shmid,NULL,0);
  if (shm==(char *) -1) {
    //printf("attaching already created and successfully linked shared memory segment failed\n");
    return NULL;     
  }
  return (void*) shm;
#endif  
}



static void* create_shared_memory(int key,int size) { 
  void *shm;  
    
#ifdef _WIN32     
  char fname[MAX_FILENAME_SIZE];
  HANDLE hmapfile;
    
  sprintf_s(fname,MAX_FILENAME_SIZE-1,"%d",key);   

  hmapfile = CreateFileMapping(
                 INVALID_HANDLE_VALUE,    // use paging file
                 NULL,                    // default security 
                 PAGE_READWRITE,          // read/write access
                 0,                       // max. object size 
                 size,                   // buffer size  
                 fname);                 // name of mapping object
  errno = 0;  
  if (hmapfile == NULL) {
      printf("Could not create file mapping object (%d).\n",GetLastError());
      return NULL;
   }
   shm = (void*) MapViewOfFile(hmapfile,   // handle to map object
                        FILE_MAP_ALL_ACCESS, // read/write permission
                        0,                   
                        0,                   
                        size);           
   if (shm == NULL)  { 
      printf("Could not map view of file (%d).\n", GetLastError()); 
      CloseHandle(hmapfile);
      return NULL;
   }  
   return shm;
#else    
  int shmflg; /* shmflg to be passed to shmget() */ 
  int shmid; /* return value from shmget() */  
   
  // Create the segment
  shmflg=IPC_CREAT | 0666;
  shmid=shmget((key_t)key,size,shmflg);
  if (shmid < 0) {  	
    printf("creating shared memory segment failed");
    return NULL;
  }
  // Attach the segment to our data space
  shm=shmat(shmid,NULL,0);
  if (shm==(char *) -1) {
    printf("attaching shared memory segment failed");
    return NULL;     
  }
  return (void*) shm;
#endif  
}



static int free_shared_memory(int key) {    
#ifdef _WIN32
  return 0;  
#else    
  int size=0;
  int shmflg; /* shmflg to be passed to shmget() */ 
  int shmid; /* return value from shmget() */ 
  int tmp;
    
  errno = 0;  
   // Link to existing segment
  shmflg=0666;
  shmid=shmget((key_t)key,size,shmflg);
  if (shmid < 0) {  	
    printf("linking to created shared memory segment (for freeing) failed");
    return -1;
  }
  // Free the segment
  tmp=shmctl(shmid, IPC_RMID, NULL);
  if (tmp==-1) {
    printf("freeing already created and successfully linked shared memory segment failed");
    return -2;     
  }
  return 0;
#endif  
}



static int detach_shared_memory(void* shmptr) {
#ifdef _WIN32
  return 0;  
#else     
  int tmp;
  
  // detach the segment
  tmp=shmdt(shmptr);
  if (tmp==-1) {
    printf("detaching already created and successfully linked shared memory segment failed");
    return -2;     
  }
  return 0;
#endif  
}

