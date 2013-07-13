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
    private native void deleteLocalDatabase(Database database);
    private native int detachDatabase(Database database);

    /*
     * Db management: public
     */
    public native int deleteDatabase(String shmname);

    /*
     * Record handling: wrapped in Java functions
     */
    private native Record createRecord(Database database, int fieldCount);
    private native Record getFirstRecord(Database database);
    private native Record getNextRecord(Database database, Record record);
    private native int deleteRecord(Database database, Record record);
    private native int getRecordLength(Database database, Record record);

    /*
     * Read/write field data: wrapped in Java functions
     */
    private native int setRecordIntField(Database database, Record record, int field, int value);
    private native int getIntFieldValue(Database database, Record record, int field);
    private native int setRecordStringField(Database database, Record record, int field, String value);
    private native String getStringFieldValue(Database database, Record record, int field);
    private native int setRecordBlobField(Database database, Record record, int field, byte[] value);
    private native byte[] getBlobFieldValue(Database database, Record record, int field);

    static {
        System.loadLibrary("whitedbDriver");
    }

    /*
     * Query functions: wrapped.
     */
    private native Query makeQuery(Database database, Record matchrec,
        ArgListEntry[] arglist);
    private native void freeQuery(Database database, Query query);
    private native Record fetchQuery(Database database, Query query);

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
            deleteLocalDatabase(database);
        } else {
            detachDatabase(database);
        }
    }

    /******************** Wrappers for native methods *******************/

    public Record createRecord(int fieldCount) {
        return createRecord(database, fieldCount);
    }

    public Record getFirstRecord() {
        return getFirstRecord(database);
    }

    public Record getNextRecord(Record record) {
        return getNextRecord(database, record);
    }

    public int deleteRecord(Record record) {
        return deleteRecord(database, record);
    }

    public int getRecordLength(Record record) {
        return getRecordLength(database, record);
    }

    public int setRecordIntField(Record record, int field, int value) {
        return setRecordIntField(database, record, field, value);
    }

    public int getIntFieldValue(Record record, int field) {
        return getIntFieldValue(database, record, field);
    }

    public int setRecordStringField(Record record, int field, String value) {
        return setRecordStringField(database, record, field, value);
    }

    public String getStringFieldValue(Record record, int field) {
        return getStringFieldValue(database, record, field);
    }

    public int setRecordBlobField(Record record, int field, byte[] value) {
        return setRecordBlobField(database, record, field, value);
    }

    public byte[] getBlobFieldValue(Record record, int field) {
        return getBlobFieldValue(database, record, field);
    }

    /****************** Wrappers for query functions ********************/

    public Query makeQuery(Record record) {
        return makeQuery(database, record, null);
    }

    public Query makeQuery(ArgListEntry[] arglist) {
        return makeQuery(database, null, arglist);
    }

    public void freeQuery(Query query) {
        freeQuery(database, query);
    }

    public Record fetchQuery(Query query) {
        return fetchQuery(database, query);
    }

    /********************* ORM support functions ************************/

    public void writeObjectToDatabase(Object object) throws IllegalAccessException {
        Field[] declaredFields = object.getClass().getDeclaredFields();
        Arrays.sort(declaredFields, new FieldComparator()); //Performance issue, cache sorted fields
        Record record = createRecord(database, declaredFields.length);

        for (int i = 0; i < declaredFields.length; i++) {
            Integer value = getFieldValue(object, declaredFields[i]);
            System.out.println("Writing field: [" + declaredFields[i].getName() + "] with value: " + value);
            setRecordIntField(database, record, i, value);
        }
    }

    public <T> T readObjectFromDatabase(Class<T> objecClass, Record record) throws IllegalAccessException, InstantiationException {
        Field[] declaredFields = objecClass.getDeclaredFields();
        Arrays.sort(declaredFields, new FieldComparator()); //Performance issue, cache sorted fields
        T object = objecClass.newInstance();

        for (int i = 0; i < declaredFields.length; i++) {
            int value = getIntFieldValue(database, record, i);
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
