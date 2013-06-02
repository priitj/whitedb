package whitedb.driver;

import whitedb.driver.WhiteDB;
import whitedb.holder.Database;
import whitedb.holder.Record;
import whitedb.holder.SampleObject;

public class tests {
    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        ormDatabaseExample();
        basicDatabaseExample();
    }

    /*
     * Prototype only works with int fields.
     */
    public static void ormDatabaseExample() throws IllegalAccessException, InstantiationException {
        SampleObject sampleObject = new SampleObject();
        sampleObject.age = 25;
        sampleObject.weight = 100;

        WhiteDB db = new WhiteDB(500000, true);
        db.writeObjectToDatabase(sampleObject);

        sampleObject = null;

        Record record = db.getFirstRecord(db.database);
        sampleObject = db.readObjectFromDatabase(SampleObject.class, record);

        System.out.println("Object read from database: " + sampleObject);
        db.close(); 
    }

    /*
     * Basic example of native database methods usage
     */
    public static void basicDatabaseExample() {
        WhiteDB db = new WhiteDB(500000, true); /* local db, 500k */
        System.out.println("db.database pointer: " + db.database.pointer);

        Record record = db.createRecord(db.database, 1);
        System.out.println("Create record 1: " + record.pointer);

        int result = db.setRecordIntField(db.database, record, 0, 108);
        System.out.println("Inserted record 1 value, result was: " + result);

        record = db.createRecord(db.database, 1);
        System.out.println("Create record 2: " + record.pointer);

        result = db.setRecordIntField(db.database, record, 0, 666);
        System.out.println("Inserted record 2 value, result was: " + result);

        record = db.getFirstRecord(db.database);
        System.out.println("First record pointer: " + record.pointer);
        System.out.println("Get record value: " + db.getIntFieldValue(db.database, record, 0));

        record = db.getNextRecord(db.database, record);
        System.out.println("Next record pointer: " + record.pointer);
        System.out.println("Get record value: " + db.getIntFieldValue(db.database, record, 0));

        record = db.getNextRecord(db.database, record);
        System.out.println("Next record pointer: " + record);
        
        db.close();
    }
}
