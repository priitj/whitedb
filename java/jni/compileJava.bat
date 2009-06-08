javac -cp ./src -d classes src\wgandalf\driver\WGandalfDatabase.java
javah -classpath ./classes -d src/native -jni wgandalf.driver.WGandalfDatabase