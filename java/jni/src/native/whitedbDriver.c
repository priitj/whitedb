/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andres Puusepp 2009
* Copyright (c) Priit Järv 2013
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

 /** @file whitedbDriver.c
 *  JNI native methods for WhiteDB.
 *
 */

#include "whitedb_driver_WhiteDB.h"
#include "../../../../Db/dballoc.h"
#include "../../../../Db/dbmem.h"
#include "../../../../Db/dbdata.h"
#include "../../../../Db/dbquery.h"

#ifdef _WIN32
#include "../../config-w32.h"
#else
#include "../../config.h"
#endif

#include <stdlib.h>

#if 0
void* get_database_from_java_object(JNIEnv *env, jobject database) {
    jclass clazz;
    jfieldID fieldID;
    jlong pointer;

    clazz = (*env)->FindClass(env, "whitedb/holder/Database");
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "J");
    pointer = (*env)->GetLongField(env, database, fieldID);

    return (void*)pointer;
}

void* get_record_from_java_object(JNIEnv *env, jobject record) {
    jclass clazz;
    jfieldID fieldID;
    jlong pointer;

    clazz = (*env)->FindClass(env, "whitedb/holder/Record");
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "J");
    pointer = (*env)->GetLongField(env, record, fieldID);

    return (void*)pointer;
}
#endif

jobject create_database_record_for_java(JNIEnv *env, void* recordPointer) {
    jclass clazz;
    jmethodID methodID;
    jobject item;
    jfieldID fieldID;

    clazz = (*env)->FindClass(env, "whitedb/holder/Record");
    methodID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    item = (*env)->NewObject(env, clazz,  methodID, NULL);
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "J");
    (*env)->SetLongField(env, item, fieldID, (jlong)recordPointer);

    return item;
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getDatabase(JNIEnv *env,
  jobject obj, jstring shmname, jint size, jboolean local) {
    jclass clazz;
    jmethodID methodID;
    jobject item;
    jfieldID fieldID;
    jlong shmptr;
    const char *shmnamep = NULL; /* JNI wants const here */

    if(local) {
        shmptr = (jlong) wg_attach_local_database((int) size);
    } else {
        if(shmname)
            shmnamep = (*env)->GetStringUTFChars(env, shmname, 0);
        shmptr = (jlong) wg_attach_database((char *) shmnamep, (int) size);
    }

    clazz = (*env)->FindClass(env, "whitedb/holder/Database");
    methodID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    item = (*env)->NewObject(env, clazz,  methodID, NULL);
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "J");
    (*env)->SetLongField(env, item, fieldID, shmptr);

    if(shmnamep)
        (*env)->ReleaseStringUTFChars(env, shmname, shmnamep);

    return item;
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_deleteDatabase(JNIEnv *env,
  jobject obj, jstring shmname) {
    jboolean ret;
    const char *shmnamep = NULL;
    if(shmname)
        shmnamep = (*env)->GetStringUTFChars(env, shmname, 0);
    ret = wg_delete_database((char *) shmnamep);
    if(shmnamep)
        (*env)->ReleaseStringUTFChars(env, shmname, shmnamep);
    return ret;
}

JNIEXPORT void JNICALL Java_whitedb_driver_WhiteDB_deleteLocalDatabase(JNIEnv *env,
  jobject obj, jlong dbptr) {
    wg_delete_local_database((void *) dbptr);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_detachDatabase(JNIEnv *env,
  jobject obj, jlong dbptr ) {
    return wg_detach_database((void *) dbptr);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_createRecord (JNIEnv *env, jobject obj, jlong dbptr, jint fieldcount) {
    void* record;

    record = wg_create_record((void *) dbptr, (int)fieldcount);

    return create_database_record_for_java(env, record);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getFirstRecord (JNIEnv *env, jobject obj, jlong dbptr) {
    void* record;

    record = wg_get_first_record((void *) dbptr);

    return create_database_record_for_java(env, record);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getNextRecord (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr) {
    void* record;

    record = wg_get_next_record((void *) dbptr, (void *) rptr);
    if(record == NULL) {
        return NULL;
    }
    return create_database_record_for_java(env, record);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_deleteRecord (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr) {

    return wg_delete_record((void *) dbptr, (void *) rptr);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_getRecordLength (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr) {

    return wg_get_record_len((void *) dbptr, (void *) rptr);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_setRecordIntField (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr, jint field, jint value) {

    return wg_set_int_field((void *) dbptr, (void *) rptr, (int)field, (int)value);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_getIntFieldValue (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr, jint field) {
    void* database;

    database = (void *) dbptr;
    return wg_decode_int(database, wg_get_field(database, (void *) rptr, (int)field));
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_setRecordStringField (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr, jint field, jstring value) {
    int result;
    const char *valuep = NULL;

    if(value)
        valuep = (*env)->GetStringUTFChars(env, value, 0);
    if(!valuep)
        return -1;

    result = wg_set_str_field((void *) dbptr, (void *) rptr, (int)field, (char *)valuep);

    (*env)->ReleaseStringUTFChars(env, value, valuep);
    return result;
}

JNIEXPORT jstring JNICALL Java_whitedb_driver_WhiteDB_getStringFieldValue (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr, jint field) {
    void* database;
    gint enc;
    char* str = NULL;

    database = (void *) dbptr;
    enc = wg_get_field(database, (void *) rptr, (int)field);
    if(enc != WG_ILLEGAL) {
        str = wg_decode_str(database, enc);
    }
    if(str) {
        return (*env)->NewStringUTF(env, (const char *) str);
    } else {
        return NULL;
    }
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_setRecordBlobField (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr, jint field, jbyteArray value) {
    void* database;
    size_t arraylen, result;
    gint enc;
    jbyte *valuep = NULL;

    if(value)
        valuep = (*env)->GetByteArrayElements(env, value, 0);
    if(!valuep)
        return -1;

    database = (void *) dbptr;

    arraylen = (*env)->GetArrayLength(env, value);
    enc = wg_encode_blob(database, (char *) valuep, NULL, arraylen);
    if(enc != WG_ILLEGAL) {
        result = wg_set_field(database, (void *) rptr, (int)field, enc);
    } else {
        result = -1;
    }

    (*env)->ReleaseByteArrayElements(env, value, valuep, 0);
    return result;
}

JNIEXPORT jbyteArray JNICALL Java_whitedb_driver_WhiteDB_getBlobFieldValue (JNIEnv *env, jobject obj, jlong dbptr, jlong rptr, jint field) {
    void* database;
    size_t arraylen = 0;
    gint enc;
    char* str = NULL;
    jbyteArray result;

    database = (void *) dbptr;
    enc = wg_get_field(database, (void *) rptr, (int)field);
    if(enc != WG_ILLEGAL) {
        str = wg_decode_blob(database, enc);
        arraylen = wg_decode_blob_len(database, enc);
    }
    if(str) {
        result = (*env)->NewByteArray(env, arraylen);
        if(result)
            (*env)->SetByteArrayRegion(env, result, 0, arraylen,
              (const jbyte *) str);
        return result;
    } else {
        return NULL;
    }
}

gint map_cond(jint cond) {
    /* Robust method of mapping constants. This way redefining
     * something on either side doesn't break. */
    switch(cond) {
        case whitedb_driver_WhiteDB_COND_EQUAL:
            return WG_COND_EQUAL;
        case whitedb_driver_WhiteDB_COND_NOT_EQUAL:
            return WG_COND_NOT_EQUAL;
        case whitedb_driver_WhiteDB_COND_LESSTHAN:
            return WG_COND_LESSTHAN;
        case whitedb_driver_WhiteDB_COND_GREATER:
            return WG_COND_GREATER;
        case whitedb_driver_WhiteDB_COND_LTEQUAL:
            return WG_COND_LTEQUAL;
        case whitedb_driver_WhiteDB_COND_GTEQUAL:
            return WG_COND_GTEQUAL;
        default:
            break;
    }
    return -1;
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_makeQuery(JNIEnv *env,
  jobject obj, jlong dbptr,
  jlong matchrecptr, jobjectArray arglistobj, jlong rowlimit) {
    jclass clazz;
    jmethodID methodID;
    jfieldID fieldID;
    jobject item = NULL;

    void *database;
    wg_query *query;
    void *matchrec = NULL;
    wg_query_arg *argv = NULL;
    int argc = 0, i;

    database = (void *) dbptr;

    if(matchrecptr) {
        matchrec = (void *) matchrecptr;
    } else if(arglistobj) {
        jfieldID column_id, cond_id, value_id;
        argc = (*env)->GetArrayLength(env, arglistobj);
        argv = malloc(sizeof(wg_query_arg) * argc);
        if(!argv) {
            return NULL;
        }

        clazz = (*env)->FindClass(env, "whitedb/util/ArgListEntry");
        column_id = (*env)->GetFieldID(env, clazz, "column", "I");
        cond_id = (*env)->GetFieldID(env, clazz, "cond", "I");
        value_id = (*env)->GetFieldID(env, clazz, "value", "I");
        for(i=0; i<argc; i++) {
            jobject argobj = (*env)->GetObjectArrayElement(env, arglistobj, i);
            argv[i].column = \
                (gint) (*env)->GetIntField(env, argobj, column_id);
            argv[i].cond = map_cond(
                (*env)->GetIntField(env, argobj, cond_id));
            argv[i].value = wg_encode_query_param_int(database,
                (gint) (*env)->GetIntField(env, argobj, value_id));
        }
    }

    if(rowlimit > 0)
	query = wg_make_query_rc(database, matchrec, 0, argv, argc, rowlimit);
    else
	query = wg_make_query(database, matchrec, 0, argv, argc);

    if(query) {
        clazz = (*env)->FindClass(env, "whitedb/holder/Query");
        methodID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
        item = (*env)->NewObject(env, clazz,  methodID, NULL);

        fieldID = (*env)->GetFieldID(env, clazz, "query", "J");
        (*env)->SetLongField(env, item, fieldID, (jlong)query);
        fieldID = (*env)->GetFieldID(env, clazz, "arglist", "J");
        (*env)->SetLongField(env, item, fieldID, (jlong)argv);
        fieldID = (*env)->GetFieldID(env, clazz, "argc", "I");
        (*env)->SetIntField(env, item, fieldID, argc);
    }

    return item;
}

JNIEXPORT void JNICALL Java_whitedb_driver_WhiteDB_freeQuery(JNIEnv *env,
  jobject obj, jlong dbptr, jobject queryobj) {
    jclass clazz;
    jfieldID fieldID;
    jlong pointer;

    void *database;
    wg_query *query = NULL;
    wg_query_arg *arglist = NULL;
    int argc = 0, i;

    database = (void *) dbptr;

    clazz = (*env)->FindClass(env, "whitedb/holder/Query");
    fieldID = (*env)->GetFieldID(env, clazz, "arglist", "J");
    pointer = (*env)->GetLongField(env, queryobj, fieldID);
    if(pointer) {
        arglist = (wg_query_arg *) pointer;
        fieldID = (*env)->GetFieldID(env, clazz, "argc", "I");
        argc = (*env)->GetIntField(env, queryobj, fieldID);
        for(i=0; i<argc; i++) {
            wg_free_query_param(database, arglist[i].value);
        }
        free(arglist);
    }
    fieldID = (*env)->GetFieldID(env, clazz, "query", "J");
    pointer = (*env)->GetLongField(env, queryobj, fieldID);
    query = (wg_query *) pointer;
    if(query)
        wg_free_query(database, query);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_fetchQuery(JNIEnv *env,
  jobject obj, jlong dbptr, jlong queryptr) {

    wg_query *query;
    void *rec = NULL;

    query = (wg_query *) queryptr;
    if(!query)
        return NULL;

    rec = wg_fetch((void *) dbptr, query);
    if(!rec)
        return NULL;
    return create_database_record_for_java(env, rec);
}

JNIEXPORT jlong JNICALL Java_whitedb_driver_WhiteDB_startRead(JNIEnv *env,
  jobject obj, jlong dbptr) {

    return (jlong) wg_start_read((void *) dbptr);
}

JNIEXPORT jlong JNICALL Java_whitedb_driver_WhiteDB_endRead(JNIEnv *env,
  jobject obj, jlong dbptr, jlong lock) {

    return (jlong) wg_end_read((void *) dbptr, lock);
}

JNIEXPORT jlong JNICALL Java_whitedb_driver_WhiteDB_startWrite(JNIEnv *env,
  jobject obj, jlong dbptr) {

    return (jlong) wg_start_write((void *) dbptr);
}

JNIEXPORT jlong JNICALL Java_whitedb_driver_WhiteDB_endWrite(JNIEnv *env,
  jobject obj, jlong dbptr, jlong lock) {

    return (jlong) wg_end_write((void *) dbptr, lock);
}
