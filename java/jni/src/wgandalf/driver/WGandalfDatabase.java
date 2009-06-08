package wgandalf.driver;

import wgandalf.holder.Database;
import wgandalf.holder.Record;
import wgandalf.holder.SampleObject;
import wgandalf.util.FieldComparator;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Arrays;

public class WGandalfDatabase {
    public native Database getDatabase();

    public native boolean deleteDatabase();

    public native Record createRecord(Database database, int fieldCount);

    public native int setRecordIntField(Database database, Record record, int field, int value);

    public native Record getFirstRecord(Database database);

    public native Record getNextRecord(Database database, Record record);

    public native int getIntFieldValue(Database database, Record record, int field);

    public Database database;

    static {
        System.loadLibrary("wgandalfDriver");
    }

    public WGandalfDatabase() {
        database = getDatabase();
    }

    public void close() {
        deleteDatabase();
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        ormDatabaseExample();
        //basicDatabaseExample();
    }

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

    /*
     * Prototype only works with int fields.
     */
    public static void ormDatabaseExample() throws IllegalAccessException, InstantiationException {
        SampleObject sampleObject = new SampleObject();
        sampleObject.age = 25;
        sampleObject.weight = 100;

        WGandalfDatabase db = new WGandalfDatabase();
        db.writeObjectToDatabase(sampleObject);

        sampleObject = null;

        Record record = db.getFirstRecord(db.database);
        sampleObject = db.readObjectFromDatabase(SampleObject.class, record);

        System.out.println("Object read from database: " + sampleObject);
        db.close();
    }

    /**
     * Basic example of native database methods usage
     */
    public static void basicDatabaseExample() {
        WGandalfDatabase wGandalfDatabase = new WGandalfDatabase();
        Database database = wGandalfDatabase.getDatabase();
        System.out.println("Database pointer: " + database.pointer);


        Record record = wGandalfDatabase.createRecord(database, 1);
        System.out.println("Create record 1: " + record.pointer);

        int result = wGandalfDatabase.setRecordIntField(database, record, 0, 108);
        System.out.println("Inserted record 1 value, result was: " + result);

        record = wGandalfDatabase.createRecord(database, 1);
        System.out.println("Create record 2: " + record.pointer);

        result = wGandalfDatabase.setRecordIntField(database, record, 0, 666);
        System.out.println("Inserted record 2 value, result was: " + result);

        record = wGandalfDatabase.getFirstRecord(database);
        System.out.println("First record pointer: " + record.pointer);
        System.out.println("Get record value: " + wGandalfDatabase.getIntFieldValue(database, record, 0));

        record = wGandalfDatabase.getNextRecord(database, record);
        System.out.println("Next record pointer: " + record.pointer);
        System.out.println("Get record value: " + wGandalfDatabase.getIntFieldValue(database, record, 0));

        record = wGandalfDatabase.getNextRecord(database, record);
        System.out.println("Next record pointer: " + record);
    }
}
