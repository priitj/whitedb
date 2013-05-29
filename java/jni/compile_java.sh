#!/bin/sh

javac -cp ./src -d classes src/whitedb/driver/WhiteDB.java
javac -cp ./src -d classes src/whitedb/driver/tests.java

javah -classpath ./classes -d src/native -jni whitedb.driver.WhiteDB
