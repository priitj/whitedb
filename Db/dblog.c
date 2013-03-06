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

 /** @file dblog.c
 *  DB logging support for wgandalf memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/shm.h>
#include <sys/errno.h>
//#include <iostream>
//#include <fstream>
#endif

#ifdef __cplusplus
extern "C" {
#endif

//#include "../config.h"
#include "dballoc.h"
#include "dbmem.h"
#include "dbdata.h"

/* ====== Private headers and defs ======== */

#include "dblog.h"



/* ======= Private protos ================ */


/* ====== Functions ============== */


gint wg_log_record(void * db,wg_int record,wg_int length)
{   
    db_memsegment_header* dbh = dbmemsegh(db);
    //only write log if in mode 1
    if(dbh->logging.writelog==1)
    {
        printf("logoffset pointer2: %d\n", (int) dbh->logging.logoffset);
        
        dbstore(db,dbh->logging.logoffset,2); //length of the log about record
        dbstore(db,dbh->logging.logoffset+sizeof(gint),record); //record offset we are using
        dbstore(db,dbh->logging.logoffset+2*sizeof(gint),dbh->logging.counter); //counter of record
        dbstore(db,dbh->logging.logoffset+3*sizeof(gint),length); //lenght of the record
        
        dbh->logging.counter++;
        
        printf("dbfetch: %d, %d, %d, %d\n",
          (int) dbfetch(db,dbh->logging.logoffset+3*sizeof(gint)),
          (int) dbfetch(db,dbh->logging.logoffset+2*sizeof(gint)),
          (int) dbfetch(db,dbh->logging.logoffset+sizeof(gint)),
          (int) dbfetch(db,dbh->logging.logoffset));
        dbh->logging.logoffset+=4*sizeof(gint); //lift pointer
        return 0; 
    }
    else
        return 1;        
}

gint wg_get_log_offset(void * db)
{
    db_memsegment_header* dbh = dbmemsegh(db);
    return dbh->logging.logoffset;   
}

gint wg_log_int(void * db,void * record, wg_int fieldnr, gint data)
{
    db_memsegment_header* dbh = dbmemsegh(db);
    if(dbh->logging.writelog==1)
    {
        printf("logoffset pointer3: %d\n", (int) dbh->logging.logoffset);
        
        dbstore(db,dbh->logging.logoffset,4); //data length
        dbstore(db,dbh->logging.logoffset+sizeof(gint),encode_fullint_offset(ptrtooffset(db,record))); //save record and field type
        dbstore(db,dbh->logging.logoffset+2*sizeof(gint),fieldnr); //save fieldnr 
        dbstore(db,dbh->logging.logoffset+3*sizeof(gint),data); //save data
        
        printf("logint: length(%d),record(%d),data(%d)\n",
          (int) dbfetch(db,dbh->logging.logoffset+3*sizeof(gint)),
          (int) decode_fullint_offset(dbfetch(db,dbh->logging.logoffset+2*sizeof(gint))),
          (int) dbfetch(db,dbh->logging.logoffset));
        dbh->logging.logoffset+=4*sizeof(gint); //lift pointer
        return 0;
    }
    else
        return 1;
}


gint wg_print_log(void * db)
{
    db_memsegment_header* dbh = dbmemsegh(db);
    gint i=0;
    gint data;
    
    printf("LOG OUTPUT, start %d\n", (int) dbh->logging.firstoffset);
    while(1)
    {
        data=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i);
        if(data==WG_MAGIC_RECORD) //get record data (always 4 bytes)
        {
            printf("record: %d, offset: %d, recordsize: %d\n",
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*2),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3));
            i+=4;   
        }
        else if(data>2) //get data (always more than 2 bytes 4+)
        {
            printf("\tdata: %d, record: %d\n",
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3),
              (int) decode_fullint_offset(dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint))));
            i+=data;
        }
        else
            break;
    }
    return dbh->logging.firstoffset;   
}

gint wg_dump_log(void *db,char fileName[])
{
    #ifdef _WIN32
    db_memsegment_header* dbh = dbmemsegh(db);
    //if file not open, open it and leave it open
    if(dbh->logging.fileopen==0)
    {
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

                CopyMemory(hviewfile,offsettoptr(db,dbh->logging.firstoffset),INITIAL_SUBAREA_SIZE);
                //no need for manual flushing, let windows do it's job, just in case
               /* if(!FlushViewOfFile (hviewfile,0))
                    printf("viewERR\n");
                else
                    printf("viewOK\n");
                
                if(!FlushFileBuffers(hfile))
                    printf("filebufferERR\n");
                else
                    printf("filebufferOK\n");*/
            }
        }
        //CloseHandle(hfile); //we dont need to close it... keep it open for further writing
        dbh->logging.fileopen=1;
        dbh->logging.filepointer=hviewfile;
    }
    //file already open, just copy and leave other for windows
    else
    {
        CopyMemory(dbh->logging.filepointer,offsettoptr(db,dbh->logging.firstoffset),INITIAL_SUBAREA_SIZE);
    }
    
    return 1;
    #else
    return 0;
    #endif
    
}

gint wg_import_log(void * db,char fileName[])
{
    db_memsegment_header* dbh = dbmemsegh(db);
    gint i=0;
    gint read;
    gint data;
    gint datatype;
    void * record;
    gint fieldnr;
    
    #ifdef _WIN32
    void *hviewfile;
    HANDLE hmapfile,hfile;
    
    printf("logoffset pointer4: %d\n",dbh->logging.logoffset);
    
    
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
                CopyMemory(offsettoptr(db,dbh->logging.firstoffset),hviewfile,INITIAL_SUBAREA_SIZE);
            }
        }
        CloseHandle(hfile);
        UnmapViewOfFile(hviewfile);
    }
    printf("returnlogimport\n");
    
    #else

    #endif
    
    //do not allow to write log if recovering
    dbh->logging.writelog=0;
    
    printf("LOG OUTPUT, start %d\n", (int) dbh->logging.firstoffset);
    while(1)
    {
        //read length (or record type)
        read=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i);
        if(read==WG_MAGIC_RECORD) //get record data (always 4 bytes)
        {
            printf("record2: %d, offset: %d, recordsize: %d\n",
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*2),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3));
            wg_create_record(db,dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3));
            i+=4;   
        }
        else if(read>2) //get int data (always 4 bytes)
        {
            printf("\tdata2: %d, record: %d, fieldnr: %d\n",
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3),
              (int) decode_fullint_offset(dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint))),
              (int) dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+2*sizeof(gint)));
            record=offsettoptr(db,decode_fullint_offset(dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint))));
            fieldnr=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+2*sizeof(gint));
            data=dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)*3);
            //datatype encoded in record
            datatype=wg_get_encoded_type(db,dbfetch(db,dbh->logging.firstoffset+sizeof(gint)*i+sizeof(gint)));
            if(datatype==WG_INTTYPE)
                wg_set_int_field(db,record,fieldnr,data);
            i+=read;
        }
        else 
            break;
    }
    //enable logging
    dbh->logging.writelog=1;
    return 1;
}

#ifdef __cplusplus
}
#endif
