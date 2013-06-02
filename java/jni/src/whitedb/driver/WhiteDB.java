package whitedb.driver;

import whitedb.holder.Database;
import whitedb.holder.Record;
import whitedb.util.FieldComparator;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Arrays;

public class WhiteDB {
    public native Database getDatabase(String shmname, int size, boolean local);

    public native int deleteDatabase(String shmname);

    public native void deleteLocalDatabase(Database database);

    public native int detachDatabase(Database database);

    public native Record createRecord(Database database, int fieldCount);

    public native int setRecordIntField(Database database, Record record, int field, int value);

    public native Record getFirstRecord(Database database);

    public native Record getNextRecord(Database database, Record record);

    public native int getIntFieldValue(Database database, Record record, int field);

    public Database database;
    private boolean local;

    static {
        System.loadLibrary("whitedbDriver");
    }

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
