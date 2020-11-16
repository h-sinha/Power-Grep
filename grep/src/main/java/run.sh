rm -rf out1
rm com/ds/*.class
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main com/ds/Distributed.java
jar cf run.jar com/ds/*.class
time /usr/local/hadoop/bin/hadoop jar run.jar com.ds.Distributed input out1 india
cat out1/part*
rm com/ds/*.class
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main com/ds/Sequential.java
jar cf run.jar com/ds/*.class
time /usr/local/hadoop/bin/hadoop jar run.jar com.ds.Sequential input out1 india
