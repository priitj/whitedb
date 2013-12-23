# Makefile for building dserve and nsmeasure
#
# dserve: database http tool as a standalone server or cgi
# nsmeasure: utility for speed testing the server over http
#
# Alternatively compile them directly as:
# 
# gcc dserve.c dserve_util.c dserve_net.c  -o dserve -O2 -lwgdb -lpthread
# gcc nsmeasure.c -o nsmeasure -O2 -lpthread
#
# dserve can be also compiled to work as a cgi or command line tool only
# without using pthreads by:
# - removing #define SERVEROPTION from dserve.h
# - compiling by gcc dserve.c dserve_util.c -o dserve -O2 -lwgdb

CFLAGS = -O2 -c
CC = gcc

all: dserve nsmeasure

dserve: dserve.o dserve_util.o dserve_net.o 
	$(CC) dserve.o dserve_net.o dserve_util.o -o dserve -lpthread -lwgdb
  
nsmeasure: nsmeasure.o
	$(CC) nsmeasure.o -o nsmeasure -lpthread
  
dserve.o: dserve.h dserve.c
	$(CC) $(CFLAGS) dserve.c
  
dserve_net.o: dserve.h dserve_net.c
	$(CC) $(CFLAGS) dserve_net.c
  
dserve_util.o: dserve.h dserve_util.c
	$(CC) $(CFLAGS) dserve_util.c  
  
nsmeasure.o: nsmeasure.c
	$(CC) $(CFLAGS) nsmeasure.c
  
clean: nsmeasure.c
	rm *.o dserve nsmeasure

