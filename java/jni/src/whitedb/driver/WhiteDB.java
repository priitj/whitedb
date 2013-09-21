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

 /** @file WhiteDB.java
 *  Java API for WhiteDB.
 *
 */

package whitedb.driver;

import whitedb.holder.Database;
import whitedb.holder.Record;
import whitedb.holder.Query;
import whitedb.util.FieldComparator;
import whitedb.util.ArgListEntry;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Arrays;

public class WhiteDB {

    /**************************** Constants  ****************************/

    public static final int COND_EQUAL = 1;
    public static final int COND_NOT_EQUAL = 2;
    public static final int COND_LESSTHAN = 4;
    public static final int COND_GREATER = 8;
    public static final int COND_LTEQUAL = 16;
    public static final int COND_GTEQUAL  = 32;

    /************************** Native methods **************************/

    /*
     * Db connection: encapsulate in class
     */
    private native Database getDatabase(String shmname, int size, boolean local);
    private native void deleteLocalDatabase(long dbptr);
    private native int detachDatabase(long dbptr);

    /*
     * Db management: public
     */
    public native int deleteDatabase(String shmname);

    /*
     * Record handling: wrapped in Java functions
     */
    private native Record createRecord(long dbptr, int fieldCount);
    private native Record getFirstRecord(long dbptr);
    private native Record getNextRecord(long dbptr, long rptr);
    private native int deleteRecord(long dbptr, long rptr);
    private native int getRecordLength(long dbptr, long rptr);

    /*
     * Read/write field data: wrapped in Java functions
     */
    private native int setRecordIntField(long dbptr, long rptr, int field, int value);
    private native int getIntFieldValue(long dbptr, long rptr, int field);
    private native int setRecordStringField(long dbptr, long rptr, int field, String value);
    private native String getStringFieldValue(long dbptr, long rptr, int field);
    private native int setRecordBlobField(long dbptr, long rptr, int field, byte[] value);
    private native byte[] getBlobFieldValue(long dbptr, long rptr, int field);

    /*
     * Query functions: wrapped.
     */
    private native Query makeQuery(long dbptr, long matchrecptr,
        ArgListEntry[] arglist, long rowlimit);
    private native void freeQuery(long dbptr, Query query);
    private native Record fetchQuery(long dbptr, long queryptr);

    /*
     * Locking functions: wrapped.
     */
    private native long startRead(long dbptr);
    private native long endRead(long dbptr, long lock);
    private native long startWrite(long dbptr);
    private native long endWrite(long dbptr, long lock);

    static {
        System.loadLibrary("whitedbDriver");
    }

    /*********************** Connection state ***************************/

    private Database database;
    private boolean local;

    /****************** Class constructor: connect to db ****************/

    public WhiteDB() {
        this.local = false;
        this.database = getDatabase(null, 0, false);
    }

    public WhiteDB(int size) {
        this.local = false;
        this.database = getDatabase(null, size, false);
    }

    public WhiteDB(String shmname) {
        this.local = false;
        this.database = getDatabase(shmname, 0, false);
    }

    public WhiteDB(String shmname, int size) {
        this.local = false;
        this.database = getDatabase(shmname, size, false);
    }

    public WhiteDB(int size, boolean local) {
        this.local = local;
        this.database = getDatabase(null, size, local);
    }

    public WhiteDB(boolean local) {
        this.local = local;
        this.database = getDatabase(null, 0, local);
    }

    public void close() {
        if(local) {
            deleteLocalDatabase(database.pointer);
        } else {
            detachDatabase(database.pointer);
        }
    }

    /******************** Wrappers for native methods *******************/

    public Record createRecord(int fieldCount) {
        return createRecord(database.pointer, fieldCount);
    }

    public Record getFirstRecord() {
        return getFirstRecord(database.pointer);
    }

    public Record getNextRecord(Record record) {
        return getNextRecord(database.pointer, record.pointer);
    }

    public int deleteRecord(Record record) {
        return deleteRecord(database.pointer, record.pointer);
    }

    public int getRecordLength(Record record) {
        return getRecordLength(database.pointer, record.pointer);
    }

    public int setRecordIntField(Record record, int field, int value) {
        return setRecordIntField(database.pointer, record.pointer, field, value);
    }

    public int getIntFieldValue(Record record, int field) {
        return getIntFieldValue(database.pointer, record.pointer, field);
    }

    public int setRecordStringField(Record record, int field, String value) {
        return setRecordStringField(database.pointer, record.pointer, field, value);
    }

    public String getStringFieldValue(Record record, int field) {
        return getStringFieldValue(database.pointer, record.pointer, field);
    }

    public int setRecordBlobField(Record record, int field, byte[] value) {
        return setRecordBlobField(database.pointer, record.pointer, field, value);
    }

    public byte[] getBlobFieldValue(Record record, int field) {
        return getBlobFieldValue(database.pointer, record.pointer, field);
    }

    /****************** Wrappers for query functions ********************/

    public Query makeQuery(Record record) {
        return makeQuery(database.pointer, record.pointer, null, 0);
    }

    public Query makeQuery(ArgListEntry[] arglist) {
        return makeQuery(database.pointer, 0, arglist, 0);
    }

    public Query makeQuery(Record record, long rowlimit) {
        return makeQuery(database.pointer, record.pointer, null, rowlimit);
    }

    public Query makeQuery(ArgListEntry[] arglist, long rowlimit) {
        return makeQuery(database.pointer, 0, arglist, rowlimit);
    }

    public void freeQuery(Query query) {
        freeQuery(database.pointer, query);
    }

    public Record fetchQuery(Query query) {
        return fetchQuery(database.pointer, query.query);
    }

    /**************** Wrappers for locking functions ********************/

    public long startRead() {
        return startRead(database.pointer);
    }

    public long endRead(long lock) {
        return endRead(database.pointer, lock);
    }

    public long startWrite() {
        return startWrite(database.pointer);
    }

    public long endWrite(long lock) {
        return endWrite(database.pointer, lock);
    }

    /********************* ORM support functions ************************/

    public void writeObjectToDatabase(Object object) throws IllegalAccessException {
        Field[] declaredFields = object.getClass().getDeclaredFields();
        Arrays.sort(declaredFields, new FieldComparator()); //Performance issue, cache sorted fields
        Record record = createRecord(database.pointer, declaredFields.length);

        for (int i = 0; i < declaredFields.length; i++) {
            Integer value = getFieldValue(object, declaredFields[i]);
            System.out.println("Writing field: [" + declaredFields[i].getName() + "] with value: " + value);
            setRecordIntField(database.pointer, record.pointer, i, value);
        }
    }

    public <T> T readObjectFromDatabase(Class<T> objecClass, Record record) throws IllegalAccessException, InstantiationException {
        Field[] declaredFields = objecClass.getDeclaredFields();
        Arrays.sort(declaredFields, new FieldComparator()); //Performance issue, cache sorted fields
        T object = objecClass.newInstance();

        for (int i = 0; i < declaredFields.length; i++) {
            int value = getIntFieldValue(database.pointer, record.pointer, i);
            System.out.println("Reading field: [" + declaredFields[i].getName() + "] with value: " + value);
            setFieldValue(object, declaredFields[i], value);
        }

        return object;
    }

    public void setFieldValue(Object object, Field field, int value) throws IllegalAccessException {
        field.set(object, value);
    }

    public Integer getFieldValue(Object object, Field field) throws IllegalAccessException {
        return (Integer) field.get(object);
    }
}
