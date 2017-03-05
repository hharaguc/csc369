#!/bin/bash

hadoop com.sun.tools.javac.Main multiInMR.java

jar cvf multiInMR.jar multiInMR*class

hadoop jar multiInMR.jar multiInMR -libjars ../json-simple-1.1.1.jar 
