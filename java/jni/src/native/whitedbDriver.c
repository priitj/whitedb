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

#ifdef _WIN32
#include "../../config-w32.h"
#else
#include "../../config.h"
#endif

void* get_database_from_java_object(JNIEnv *env, jobject database) {
    jclass clazz;
    jfieldID fieldID;
    jint pointer;

    clazz = (*env)->FindClass(env, "whitedb/holder/Database");
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "I");
    pointer = (*env)->GetIntField(env, database, fieldID);

    return (void*)pointer;
}

void* get_record_from_java_object(JNIEnv *env, jobject record) {
    jclass clazz;
    jfieldID fieldID;
    jint pointer;

    clazz = (*env)->FindClass(env, "whitedb/holder/Record");
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "I");
    pointer = (*env)->GetIntField(env, record, fieldID);

    return (void*)pointer;
}

jobject create_database_record_for_java(JNIEnv *env, void* recordPointer) {
    jclass clazz;
    jmethodID methodID;
    jobject item;
    jfieldID fieldID;

    clazz = (*env)->FindClass(env, "whitedb/holder/Record");
    methodID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    item = (*env)->NewObject(env, clazz,  methodID, NULL);
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "I");
    (*env)->SetIntField(env, item, fieldID, (int)recordPointer);

    return item;
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getDatabase(JNIEnv *env,
  jobject obj, jstring shmname, jint size, jboolean local) {
    jclass clazz;
    jmethodID methodID;
    jobject item;
    jfieldID fieldID;
    int shmptr;
    const char *shmnamep = NULL; /* JNI wants const here */

    if(local) {
        shmptr = (int) wg_attach_local_database((int) size);
    } else {
        if(shmname)
            shmnamep = (*env)->GetStringUTFChars(env, shmname, 0);
        shmptr = (int) wg_attach_database((char *) shmnamep, (int) size);
    }

    clazz = (*env)->FindClass(env, "whitedb/holder/Database");
    methodID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    item = (*env)->NewObject(env, clazz,  methodID, NULL);
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "I");
    (*env)->SetIntField(env, item, fieldID, (int)shmptr);

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
  jobject obj, jobject database) {
    void *db = get_database_from_java_object(env, database);
    wg_delete_local_database(db);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_detachDatabase(JNIEnv *env,
  jobject obj, jobject database) {
    void *db = get_database_from_java_object(env, database);
    return wg_detach_database(db);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_createRecord (JNIEnv *env, jobject obj, jobject database, jint fieldcount) {
    void* pointer;
    void* record;

    pointer = get_database_from_java_object(env, database);
    record = wg_create_record(pointer, (int)fieldcount);

    return create_database_record_for_java(env, record);
}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_setRecordIntField (JNIEnv *env, jobject obj, jobject databaseObj, jobject recordObj, jint field, jint value) {
    void* database;
    void* record;

    database = get_database_from_java_object(env, databaseObj);
    record = get_record_from_java_object(env, recordObj);
    return wg_set_int_field(database, record, (int)field, (int)value);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getFirstRecord (JNIEnv *env, jobject obj, jobject databaseObj) {
    void* database;
    void* record;

    database = get_database_from_java_object(env, databaseObj);
    record = wg_get_first_record(database);

    return create_database_record_for_java(env, record);
}

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getNextRecord (JNIEnv *env, jobject obj, jobject databaseObj, jobject recordObj) {
    void* database;
    void* record;

    database = get_database_from_java_object(env, databaseObj);
    record = get_record_from_java_object(env, recordObj);

    record = wg_get_next_record(database, record);

    if(record == NULL) {
        return NULL;
    }

    return create_database_record_for_java(env, record);

}

JNIEXPORT jint JNICALL Java_whitedb_driver_WhiteDB_getIntFieldValue (JNIEnv *env, jobject obj, jobject databaseObj, jobject recordObj, jint field) {
    void* database;
    void* record;
    int result;

    database = get_database_from_java_object(env, databaseObj);
    record = get_record_from_java_object(env, recordObj);

    result = wg_decode_int(database, wg_get_field(database, record, (int)field));

    return result;
}
