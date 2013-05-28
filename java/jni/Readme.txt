WhiteDB JNI wrapper
===================

The JNI wrapper provides the WhiteDB bindings for Java. It is still under
development and not usable for general application.

Compiling under Windows environment
-----------------------------------

Scripts should be executed in the order given here. Please run each script in the
directory they are located. You may need to edit the scripts to adjust the paths
to your environment; specific instructions below.

`setEnv.bat`

This file sets all needed paths and variables for compiling and running all jni classes.
Line 1: `javac`, `javah` and `java` location in your computer will be added to path.
Line 2: you will be directed to your working directory.
Line 3: path to jni library (in windows, dll file) will be added to your path variable.
Line 4: 'vcvarsall.bat' run. Its needed to use Visual Studios c compiler 'cl.exe'.

`compileJava.bat`

This script file compiles 'WhiteDB.java' source file and generates .h file for jni from compiled class file.

`compileBridge.bat`

This script will compile c source code ('cl.exe' flag '/LD') into dll file including java sdk's jni needed 
files ('cl.exe' flag '/I'). Change path of 'cl.exe' and java sdk includes to paths where they can be found in
your computer. Copy 'config-w32.h' to 'java\jni' directory before running this script.

`runJava.bat`
This script runs WhiteDB class file. 

Compiling under Unix-like environment
-------------------------------------

Scripts should be executed in the order given here. Please run each script in the
directory they are located. Set JAVA_HOME before executing the scripts.

`compile_java.sh`

This script file compiles 'WhiteDB.java' source file and generates the header file
for jni from compiled class file.

`compile_bridge.sh`

Compile the shared library that wraps the WhiteDB functions for Java.

`run_java.sh`

Runs the tests in the WhiteDB class.
