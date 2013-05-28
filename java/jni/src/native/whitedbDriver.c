#include "whitedb_driver_WhiteDB.h"
#include "../../../../Db/dballoc.h"
#include "../../../../Db/dbmem.h"
#include "../../../../Db/dbdata.h"

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

JNIEXPORT jobject JNICALL Java_whitedb_driver_WhiteDB_getDatabase(JNIEnv *env, jobject obj) {
    jclass clazz;
    jmethodID methodID;
    jobject item;
    jfieldID fieldID;
    int shmptr;

    clazz = (*env)->FindClass(env, "whitedb/holder/Database");
    methodID = (*env)->GetMethodID(env, clazz, "<init>", "()V");
    item = (*env)->NewObject(env, clazz,  methodID, NULL);
    fieldID = (*env)->GetFieldID(env, clazz, "pointer", "I");

    shmptr=(int)wg_attach_database(NULL, 0);
    (*env)->SetIntField(env, item, fieldID, (int)shmptr);

    return item;
}

JNIEXPORT jboolean JNICALL Java_whitedb_driver_WhiteDB_deleteDatabase(JNIEnv *env, jobject obj) {
    return wg_delete_database(NULL);
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
