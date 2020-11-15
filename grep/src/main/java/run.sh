rm -rf out1
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main com/ds/Distributed.java
jar cf run.jar com/ds/*.class
/usr/local/hadoop/bin/hadoop jar run.jar com.ds.Distributed input.txt out1 abc
