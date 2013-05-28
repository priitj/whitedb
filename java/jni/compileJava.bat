javac -cp ./src -d classes src\whitedb\driver\WhiteDB.java
javah -classpath ./classes -d src/native -jni whitedb.driver.WhiteDB
