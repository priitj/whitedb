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

 /** @file tests.java
 *  Java API tests and demos.
 *
 */

package whitedb.driver;

import whitedb.driver.WhiteDB;
import whitedb.holder.Record;
import whitedb.holder.SampleObject;
import whitedb.holder.Query;
import whitedb.util.ArgListEntry;

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

        Record record = db.getFirstRecord();
        sampleObject = db.readObjectFromDatabase(SampleObject.class, record);

        System.out.println("Object read from database: " + sampleObject);
        db.close(); 
    }

    /*
     * Basic example of native database methods usage
     */
    public static void basicDatabaseExample() {
        WhiteDB db = new WhiteDB(500000, true); /* local db, 500k */
        /* System.out.println("db.database pointer: " + db.database.pointer); */

        Record record = db.createRecord(1);
        System.out.println("Create record 1: " + record.pointer);

        int result = db.setRecordIntField(record, 0, 108);
        System.out.println("Inserted record 1 value, result was: " + result);

        record = db.createRecord(3);
        System.out.println("Create record 2: " + record.pointer);

        result = db.setRecordIntField(record, 0, 666);
        System.out.println("Inserted record 2 field 0, result was: " + result);
        result = db.setRecordStringField(record, 1, "testval");
        System.out.println("Inserted record 2 field 1, result was: " + result);
        result = db.setRecordBlobField(record, 2, new byte[] {65, 0, -7, 44});
        System.out.println("Inserted record 2 field 2, result was: " + result);
        result = db.getRecordLength(record);
        System.out.println("Record 2 length reported: " + result);

        record = db.getFirstRecord();
        System.out.println("First record pointer: " + record.pointer);
        System.out.println("Get field 0 value: " + db.getIntFieldValue(record, 0));

        record = db.getNextRecord(record);
        System.out.println("Next record pointer: " + record.pointer);
        System.out.println("Get field 0 value: " + db.getIntFieldValue(record, 0));
        System.out.println("Get field 1 value: " + db.getStringFieldValue(record, 1));

        byte[] val = db.getBlobFieldValue(record, 2);
        System.out.print("Get field 2 value: ");
        for(int i=0; i<val.length; i++)
            System.out.printf("%02X", val[i]);
        System.out.println();

        record = db.getNextRecord(record);
        System.out.println("Next record pointer: " + record);

        ArgListEntry[] arglist = new ArgListEntry[1];
        arglist[0] = new ArgListEntry(0, db.COND_GREATER, 50);
        Query query = db.makeQuery(arglist);
        record = db.fetchQuery(query);
        while(record != null) {
            System.out.println("Fetched record: " + record.pointer);
            System.out.println("Get field 0 value: " + db.getIntFieldValue(record, 0));
            record = db.fetchQuery(query);
        }
        db.freeQuery(query);

        db.close();
    }
}
