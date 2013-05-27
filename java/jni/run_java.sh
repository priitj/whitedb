#!/bin/sh

java -classpath ./classes \
  -Djava.library.path=./library \
  wgandalf.driver.WGandalfDatabase
