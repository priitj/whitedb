/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009
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

 /** @file dbdump.c
 *  DB dumping support for wgandalf memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/shm.h>
#include <sys/errno.h>
#include <iostream>
#include <fstream>
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dbmem.h"

/* ====== Private headers and defs ======== */

#include "dbdump.h"



/* ======= Private protos ================ */




/* ====== Functions ============== */


/* ----------- read and write transaction support ----------- */


/** Start write transaction
 *   dump shared memory to the disk
 */

gint wg_dump(void * db,char fileName[]) {
    #ifdef _WIN32
    void *hviewfile;
    HANDLE hmapfile,hfile;
    
    hfile = CreateFile(fileName,       // lpFileName
                GENERIC_READ | GENERIC_WRITE , // dwDesiredAccess
                FILE_SHARE_READ,              // dwShareMode
                NULL,           // lpSecurityAttributes
                CREATE_ALWAYS,  // dwCreationDisposition
                FILE_ATTRIBUTE_NORMAL, // dwFlagsAndAttributes
                NULL            // hTemplateFile
              );
    
    hmapfile = CreateFileMapping(
                 hfile,    // use paging file
                 NULL,                    // default security 
                 PAGE_READWRITE,          // read/write access
                 0,                       // max. object size 
                 DEFAULT_MEMDBASE_SIZE,                   // buffer size  
                 NULL);                 // name of mapping object

    if(hmapfile==NULL)
    {
        printf("Error opening file mapping\n");
        CloseHandle(hmapfile);
    }
    else
    {
        hviewfile = (void*)MapViewOfFile( hmapfile,FILE_MAP_ALL_ACCESS,0,0,0);
        if(hviewfile==NULL)
        {
            printf("mapviewopenERR\n");
            UnmapViewOfFile(hviewfile);
        }
        else
        {

            CopyMemory(hviewfile,db,DEFAULT_MEMDBASE_SIZE);
            if(!FlushViewOfFile (hviewfile,0))
                printf("viewERR\n");
            else
                printf("viewOK\n");
            
            if(!FlushFileBuffers(hfile))
                printf("filebufferERR\n");
            else
                printf("filebufferOK\n");
        }
    }
    CloseHandle(hfile);
    return 1;
    #else
    ifstream file (fileName, ios::in|ios::binary|ios::ate);
    if (file.is_open())
    {
        size = file.tellg();
        file.seekg (0, ios::beg);
        file.write (db,DEFAULT_MEMDBASE_SIZE);
        file.close();
    }
    return 2;
    #endif
}


gint wg_import_dump(void * db,char fileName[]) {
    #ifdef _WIN32
   
    void *hviewfile;
    HANDLE hmapfile,hfile;
    hfile = CreateFile(fileName,       // lpFileName
                GENERIC_READ | GENERIC_WRITE , // dwDesiredAccess
                FILE_SHARE_READ,              // dwShareMode
                NULL,           // lpSecurityAttributes
                OPEN_EXISTING,  // dwCreationDisposition
                FILE_ATTRIBUTE_NORMAL, // dwFlagsAndAttributes
                NULL            // hTemplateFile
              );
    
    if(GetLastError()==2)
        printf("File not found\n");
    else
    {
        printf("File exists, size: %d\n",GetFileSize(hfile,0));
        
        hmapfile = CreateFileMapping(
                 hfile,    // use paging file
                 NULL,                    // default security 
                 PAGE_READWRITE,          // read/write access
                 0,                       // max. object size 
                 DEFAULT_MEMDBASE_SIZE,   // buffer size  
                 NULL);                 // name of mapping object

        if(hmapfile==NULL)
        {
            printf("Error opening file mapping\n");
            CloseHandle(hmapfile);
        }
        else
        {
            hviewfile = (void*)MapViewOfFile(hmapfile,FILE_MAP_ALL_ACCESS,0,0,0);
            if(hviewfile==NULL)
            {
                printf("mapviewopenERR\n");
                
            }
            else
            {
                printf("copy memory\n");
                CopyMemory(db,hviewfile,DEFAULT_MEMDBASE_SIZE);
            }
        }
        CloseHandle(hfile);
        UnmapViewOfFile(hviewfile);
    }
    printf("returnimport\n");
    return 1;
    #else
    ifstream file (fileName, ios::in|ios::binary|ios::ate);
    if (file.is_open())
    {
        size = file.tellg();
        file.seekg (0, ios::beg);
        file.read (db,DEFAULT_MEMDBASE_SIZE);
        file.close();
    }
    return 2;
    #endif
}
