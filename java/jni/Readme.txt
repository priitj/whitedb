setEnv.bat 
This file sets all needed paths and variables for compiling and running all jni classes.
Line 1: javac, javah and java location in your computer will be added to path.
Line 2: you will be directed to your working directory.
Line 3: path to jni library (in windows, dll file) will be added to your path variable.
Line 4: vcvarsall.bat run. Its needed to use Visual Studios c compiler cl.exe.

Change directories in your computer.

compileJava.bat
This script file compiles WGandalfDatabase.java source file and generates .h file for jni from compiled class file.

compileBridge.bat
This script will compile c source code (cl.exe flag /LD) into dll file including java sdk's jni needed 
files (cl.exe flag /I). Change path of cl.exe and java sdk includes to paths where they can be found in
your computer.

runJava.bat
This script runs WGandalfDatabase class file. 



NB!!!
All script files must be executed in the directory where they are located.

