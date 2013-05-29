#!/bin/sh

java -classpath ./classes -Djava.library.path=./library \
  whitedb.driver.tests
